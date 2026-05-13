import re
import os

def safe_filename(name):
    name = re.sub(r'[\\/:*?"<>|]', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name[:240]

def parse_info(caption, filename):

    info = {
        "season": "01",
        "episode": "01",
        "quality": "720p",
        "audio": "Multi",
        "name": "Anime",
        "video_length": ""
    }

    text = f"{caption}\n{filename}"

    s = re.search(r'(?:S|Season)\s?(\d+)', text, re.I)
    e = re.search(r'(?:E|EP|Episode)\s?(\d+)', text, re.I)
    q = re.search(r'(360p|480p|720p|1080p|2160p)', text, re.I)

    if s:
        info["season"] = s.group(1).zfill(2)

    if e:
        info["episode"] = e.group(1).zfill(2)

    if q:
        info["quality"] = q.group(1)

    name = re.sub(r'[\[\(\{].*?[\]\)\}]', '', filename)
    name = os.path.splitext(name)[0]
    name = re.sub(r'(360p|480p|720p|1080p|2160p)', '', name, flags=re.I)
    name = re.sub(r'(S\d+E\d+)', '', name, flags=re.I)
    name = re.sub(r'[_\.]', ' ', name)

    info["name"] = safe_filename(name.strip())

    return info

def new_filename(info):

    return safe_filename(
        f"{info['name']} "
        f"S{info['season']}E{info['episode']} "
        f"[{info['quality']}] "
        f"[{info['audio']}].mkv"
    )
