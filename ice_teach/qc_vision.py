"""Quality control on the pictures of the ICE Teach QC camera (local stack only, FrostTwin drive mode).

FrostTwin photographs every pallet that stops on the QC platform (bay 4.2) and publishes it on
ice_data_qc_image (frost_twin/FrostTwin/Qc.cs). This saves each picture in ice_teach/qc_images/ and
runs a baseline check: the saturated (coloured) objects on the pallet, their colour and their length
along the pallet, compared with what the twin really drew there (content/color of the message).
On the real line cell 5 does this with its own camera (CheckQualityCovers / CheckQualityLego).

    python3 ice_teach/qc_vision.py                  # with .envrc in the environment
    python3 ice_teach/qc_vision.py --self-test
"""
import asyncio, base64, io, json, os, sys
import numpy as np
from PIL import Image

HERE = os.path.dirname(os.path.abspath(__file__))
TOPIC = "ice_data_qc_image"
MM_PER_PX = 2 * 350 * np.tan(np.radians(20)) / 480  # Qc.cs: 0.35 m above the pallet top, 40 deg over 480 px
PALLET_PX = 400  # the pallet top (about 210 mm) in the middle of the picture: belts and bays outside are ignored
MIN_MM = 20      # smaller coloured spots (holes, the pallet marker ~10 mm) are not objects; Duplo 2x2 = 32 mm
DUPLO = {1: "2x2", 2: "2x4", 3: "2x8"}
PARTS = {10: "board", 11: "top cover", 12: "bottom cover", 13: "gadget", 14: "engraved gadget"}


def colour(hue):
    """PIL hue (0-255 for 0-360 deg) -> name."""
    deg = hue * 360 / 255
    for limit, name in ((15, "red"), (40, "orange"), (75, "yellow"), (165, "green"), (260, "blue"), (330, "purple")):
        if deg < limit:
            return name
    return "red"


def objects(img):
    """Coloured objects on the pallet, left to right (positions 1 -> 3): [(colour, length mm)].
    An object = a run of image columns that contain saturated pixels."""
    w, h = img.size
    img = img.crop(((w - PALLET_PX) // 2, (h - PALLET_PX) // 2, (w + PALLET_PX) // 2, (h + PALLET_PX) // 2))
    # ponytail: fixed pallet window and column runs; touching objects merge. Segment blobs (scipy/OpenCV) if needed
    hsv = np.asarray(img.convert("HSV")).astype(int)
    mask = (hsv[..., 1] > 100) & (hsv[..., 2] > 60)
    cols = mask.sum(axis=0) > 3
    out, x = [], 0
    while x < len(cols):
        if not cols[x]:
            x += 1
            continue
        x0 = x
        while x < len(cols) and cols[x]:
            x += 1
        if (x - x0) * MM_PER_PX >= MIN_MM:
            hues = hsv[..., 0][:, x0:x][mask[:, x0:x]]
            out.append((colour(int(np.median(hues))), round((x - x0) * MM_PER_PX)))
    return out


def truth(msg):
    names = [DUPLO.get(c) or PARTS.get(c) or str(c) for c in msg.get("content") or [] if c]
    return ", ".join(names) + (f" ({msg['color']})" if msg.get("color") and any(c >= 11 for c in msg.get("content") or []) else "")


def check(msg):
    img = Image.open(io.BytesIO(base64.b64decode(msg["jpg"]))).convert("RGB")
    seen = objects(img)
    content, expected = msg.get("content") or [], msg.get("color") or ""
    if any(c >= 11 for c in content):  # covers / gadget: CheckQualityCovers(colour)
        ok = bool(seen) and all(c == expected for c, _ in seen)
    else:  # Duplo: CheckQualityLego(sizes), one object per brick
        ok = len(seen) == len([c for c in content if c])
    return img, seen, ok


async def main():
    from aiokafka import AIOKafkaConsumer, TopicPartition
    out = os.path.join(HERE, "qc_images")
    os.makedirs(out, exist_ok=True)
    c = AIOKafkaConsumer(bootstrap_servers="localhost:9093", security_protocol="SASL_PLAINTEXT", sasl_mechanism="SCRAM-SHA-512",
                         sasl_plain_username=os.environ["KAFKA_CLIENT_USER"], sasl_plain_password=os.environ["KAFKA_CLIENT_PASSWORD"],
                         max_partition_fetch_bytes=4 << 20)
    await c.start()
    tp = TopicPartition(TOPIC, 0)  # no group: from the end
    c.assign([tp])
    await c.seek_to_end(tp)
    print(f"waiting for pictures on {TOPIC}, saved in {out}", flush=True)
    try:
        async for r in c:
            msg = json.loads(r.value)
            img, seen, ok = check(msg)
            path = os.path.join(out, f"{msg['t']:09.1f}_pallet{msg['p']}.jpg")
            img.save(path)
            print(f"pallet {msg['p']} t={msg['t']}: seen {seen or 'nothing'} | on the pallet: {truth(msg) or 'empty'} "
                  f"-> {'OK' if ok else 'FAIL'}  ({os.path.relpath(path)})", flush=True)
    finally:
        await c.stop()


def self_test():
    img = np.full((480, 640, 3), 90, np.uint8)               # grey pallet
    img[200:280, 0:100] = (20, 20, 160)                      # blue belt, outside the pallet window
    img[200:280, 130:260] = (240, 200, 0)                    # yellow, 130 px
    img[210:214, 300:304] = (20, 20, 160)                    # blue hole, too small
    img[200:280, 400:460] = (0, 140, 30)                     # green, 60 px
    jpg = io.BytesIO()
    Image.fromarray(img).save(jpg, "JPEG")
    msg = {"p": 7, "t": 1.0, "jpg": base64.b64encode(jpg.getvalue()).decode(), "content": [0, 11, 12], "color": "yellow"}
    _, seen, ok = check(msg)
    assert [c for c, _ in seen] == ["yellow", "green"] and not ok, seen
    assert abs(seen[0][1] - 130 * MM_PER_PX) <= 1
    assert colour(0) == "red" and colour(42) == "yellow" and colour(170) == "blue"
    print("ok")


if __name__ == "__main__":
    self_test() if sys.argv[1:] == ["--self-test"] else asyncio.run(main())
