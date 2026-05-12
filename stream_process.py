import asyncio
import aiohttp
import os
import time
from pyrogram import Client

class StreamProcessor:
    """
    Zero-storage streaming pipeline:
    Download chunks → FFmpeg pipe → Upload chunks (nothing written to disk)
    """
    
    def __init__(self, client: Client, bot_token: str, dump_channel: int):
        self.client = client
        self.bot_token = bot_token
        self.dump_channel = dump_channel
        self.chunk_size = 256 * 1024  # 256 KB

    async def download_stream(self, message):
        """Async generator yielding chunks from Telegram download stream."""
        async for chunk in self.client.stream_media(message, limit=self.chunk_size):
            yield chunk

    async def process_and_upload(self, message, metadata, progress_callback=None):
        """
        Process video via FFmpeg with pipe I/O and upload directly to dump channel.
        
        Args:
            message: Pyrogram message containing video/document
            metadata: dict with 'title', 'filename', 'caption'
            progress_callback: optional async function for progress updates
            
        Returns:
            file_id of uploaded video or None if failed
        """
        title = metadata.get('title', 'Unknown')
        new_file_name = metadata.get('filename', 'output.mkv')
        caption = metadata.get('caption', '')

        # FFmpeg command with pipe I/O
        cmd = [
            "ffmpeg",
            "-y",                    # Overwrite output
            "-i", "pipe:0",          # Input from stdin
            "-c", "copy",            # Stream copy (no re-encode, fast!)
            "-metadata", f"title={title}",
            "-f", "matroska",        # Output format MKV
            "pipe:1"                 # Output to stdout
        ]

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        # Task 1: Feed download chunks to FFmpeg stdin
        async def feed_stdin():
            try:
                async for chunk in self.download_stream(message):
                    proc.stdin.write(chunk)
                    await proc.stdin.drain()
                proc.stdin.close()
            except Exception as e:
                try:
                    proc.stdin.close()
                except:
                    pass
                raise

        # Task 2: Read from FFmpeg stdout and upload in chunks
        upload_url = f"https://api.telegram.org/bot{self.bot_token}/sendVideo"
        
        async with aiohttp.ClientSession() as session:
            async def upload_from_stdout():
                buffer = bytearray()
                file_id = None
                start_time = time.time()
                total_size = 0

                while True:
                    chunk = await proc.stdout.read(self.chunk_size)
                    if not chunk:
                        break
                    
                    buffer.extend(chunk)
                    total_size += len(chunk)

                    # Upload when buffer >= 5MB or stream ended (last chunk)
                    if len(buffer) >= 5 * 1024 * 1024 or not chunk:
                        # Prepare multipart form data
                        form = aiohttp.FormData()
                        form.add_field('chat_id', str(self.dump_channel))
                        form.add_field('video', bytes(buffer),
                                       filename=new_file_name,
                                       content_type='video/x-matroska')
                        form.add_field('caption', caption)
                        form.add_field('supports_streaming', 'true')

                        async with session.post(upload_url, data=form) as resp:
                            result = await resp.json()
                            if not result.get('ok'):
                                error = result.get('description', 'Unknown error')
                                raise Exception(f"Upload failed: {error}")
                            
                            # If this is the final chunk, get the file_id
                            if not chunk:
                                try:
                                    file_id = result['result']['video']['file_id']
                                except (KeyError, TypeError):
                                    pass

                        # Clear buffer for next chunk
                        buffer = bytearray()

                if progress_callback:
                    elapsed = time.time() - start_time
                    await progress_callback(f"Upload completed in {elapsed:.1f}s ({total_size/1024/1024:.1f} MB)")
                
                return file_id

            # Run both tasks concurrently
            stdin_task = asyncio.create_task(feed_stdin())
            upload_task = asyncio.create_task(upload_from_stdout())

            await asyncio.gather(stdin_task, upload_task)

            # Check FFmpeg exit status
            await proc.wait()
            if proc.returncode != 0:
                stderr = (await proc.stderr.read()).decode()[:300]
                raise Exception(f"FFmpeg error: {stderr}")

            return upload_task.result()
