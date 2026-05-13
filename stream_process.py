import asyncio
import aiohttp
import time

class StreamProcessor:
    """Zero-storage streaming pipeline: Download -> FFmpeg pipe -> Upload directly."""
    
    def __init__(self, client, bot_token, dump_channel):
        self.client = client
        self.bot_token = bot_token
        self.dump_channel = dump_channel
        self.chunk_size = 256 * 1024  # 256 KB chunks

    async def process_and_upload(self, message, metadata, progress_callback=None):
        """
        Args:
            message: Pyrogram message (video/document)
            metadata: dict with keys 'title', 'filename', 'caption'
            progress_callback: optional async function(current_bytes, total_bytes)
        Returns:
            file_id of uploaded video or None if failed
        """
        title = metadata.get('title', 'Unknown')
        file_name = metadata.get('filename', 'output.mkv')
        caption = metadata.get('caption', '')
        total_size = metadata.get('file_size', 0)

        # FFmpeg command with pipe I/O
        cmd = [
            "ffmpeg", "-y",
            "-i", "pipe:0",          # Input from stdin
            "-c", "copy",            # Stream copy (fast, no re-encode)
            "-metadata", f"title={title}",
            "-movflags", "+faststart",
            "-f", "matroska",        # Output as MKV
            "pipe:1"                 # Output to stdout
        ]

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        # Feed download chunks to stdin
        async def feed_stdin():
            try:
                async for chunk in self.client.stream_media(message, limit=self.chunk_size):
                    proc.stdin.write(chunk)
                    await proc.stdin.drain()
                proc.stdin.close()
            except Exception:
                proc.stdin.close()
                raise

        # Upload from stdout in chunks
        upload_url = f"https://api.telegram.org/bot{self.bot_token}/sendVideo"
        async with aiohttp.ClientSession() as session:
            async def upload_stdout():
                buffer = bytearray()
                file_id = None
                uploaded = 0
                start_time = time.time()

                while True:
                    chunk = await proc.stdout.read(self.chunk_size)
                    if not chunk:
                        break
                    buffer.extend(chunk)
                    uploaded += len(chunk)

                    # Upload when buffer reaches 5MB or stream finished
                    if len(buffer) >= 5 * 1024 * 1024 or not chunk:
                        form = aiohttp.FormData()
                        form.add_field('chat_id', str(self.dump_channel))
                        form.add_field('video', bytes(buffer),
                                       filename=file_name,
                                       content_type='video/x-matroska')
                        form.add_field('caption', caption)
                        form.add_field('supports_streaming', 'true')

                        async with session.post(upload_url, data=form) as resp:
                            result = await resp.json()
                            if not result.get('ok'):
                                error = result.get('description', 'Unknown error')
                                raise Exception(f"Upload failed: {error}")
                            # Final chunk returns the file_id
                            if not chunk:
                                try:
                                    file_id = result['result']['video']['file_id']
                                except (KeyError, TypeError):
                                    pass
                        buffer = bytearray()

                        if progress_callback and total_size:
                            await progress_callback(uploaded, total_size)

                elapsed = time.time() - start_time
                print(f"Upload completed in {elapsed:.1f}s, size={uploaded/1024/1024:.1f}MB")
                return file_id

            # Run both tasks concurrently
            stdin_task = asyncio.create_task(feed_stdin())
            upload_task = asyncio.create_task(upload_stdout())

            await asyncio.gather(stdin_task, upload_task)

            # Check FFmpeg exit status
            await proc.wait()
            if proc.returncode != 0:
                stderr = (await proc.stderr.read()).decode()[:300]
                raise Exception(f"FFmpeg error: {stderr}")

            return upload_task.result()
