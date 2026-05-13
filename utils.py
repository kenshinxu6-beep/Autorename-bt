import re
import os

def parse_info(caption, filename):
    info = {
        "season": "01",
        "episode": "01",
        "quality": "720p",
        "audio": "",
        "name": "Unknown",
        "video_length": ""
    }
    cap = caption or ""
    
    # Extract from caption
    s_match = re.search(r"(?:Season|S)\s*[:\-]?\s*(\d+)", cap, re.I)
    if s_match: info["season"] = s_match.group(1).zfill(2)
    
    e_match = re.search(r"(?:Episode|Ep|E)\s*[:\-]?\s*(\d+)", cap, re.I)
    if e_match: info["episode"] = e_match.group(1).zfill(2)
    
    q_match = re.search(r"(\d{3,4}p)", cap)
    if q_match: info["quality"] = q_match.group(1)
    
    audio_match = re.search(r"Audio\s*:\s*\[([^\]]+)\]", cap, re.I)
    if audio_match:
        info["audio"] = audio_match.group(1).strip()
    else:
        audio_match2 = re.search(r"Language\s*-\s*(\w+)", cap, re.I)
        if audio_match2:
            info["audio"] = audio_match2.group(1).strip()
    
    name_match = re.search(r"(?:ᴀɴɪᴍᴇ|Anime|Name)\s*:\s*(.+)", cap, re.I)
    if name_match:
        info["name"] = name_match.group(1).strip()
    else:
        lines = [l.strip() for l in cap.strip().split('\n') if l.strip()]
        if lines:
            info["name"] = re.sub(r'[^\w\s\-\[\]\(\)]', '', lines[0]).strip()
    
    # Override from filename if not found
    fname = os.path.splitext(filename)[0]
    
    fn_s = re.search(r"season\s*(\d+)", fname, re.I)
    if not s_match and fn_s:
        info["season"] = fn_s.group(1).zfill(2)
    
    fn_e = re.search(r"(?:episode|ep|e)\s*(\d+)", fname, re.I)
    if not e_match and fn_e:
        info["episode"] = fn_e.group(1).zfill(2)
    
    fn_q = re.search(r"(\d{3,4}p)", fname)
    if not q_match and fn_q:
        info["quality"] = fn_q.group(1)
    
    return info


def new_filename(info, template=None):
    if not template:
        template = "{name} S{season}E{episode} [{audio}] [{quality}]"
    
    result = template
    for key, val in info.items():
        result = result.replace("{" + key + "}", str(val))
    
    # Remove empty brackets
    result = re.sub(r'\[\s*\]', '', result).strip()
    result = re.sub(r'\s*\[\s*', ' [', result)
    result = re.sub(r'\s*\]\s*', '] ', result).strip()
    
    if not result.lower().endswith('.mkv'):
        result += '.mkv'
    
    return result
