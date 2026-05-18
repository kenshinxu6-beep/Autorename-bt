import time

def humanbytes(size):

    if not size:
        return "0 B"

    power = 1024
    n = 0
    Dic = {
        0: "B",
        1: "KB",
        2: "MB",
        3: "GB",
        4: "TB"
    }

    while size > power:
        size /= power
        n += 1

    return f"{round(size,2)} {Dic[n]}"


def progress_bar(percent):

    filled = int(percent / 10)

    return (
        "▓" * filled +
        "░" * (10 - filled)
    )


async def progress(
    current,
    total,
    msg,
    start,
    action
):

    now = time.time()

    diff = now - start

    if round(diff % 5) == 0:

        percentage = current * 100 / total

        speed = current / diff

        bar = progress_bar(percentage)

        text = f"""
⚡ {action}

[{bar}] {percentage:.2f}%

📦 {humanbytes(current)} / {humanbytes(total)}

🚀 {humanbytes(speed)}/s
"""

        try:
            await msg.edit(text)
        except:
            pass
