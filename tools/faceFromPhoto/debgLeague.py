#!/usr/bin/env python3
"""Remove backgrounds from every player photo in a BBGM league file and re-host
the cutouts, then rewrite the league file to point at the new hosted URLs.

Runs entirely on your machine (the sandbox that generated this can't reach the
image hosts). One command, resumable, automated hosting.

    pip install "rembg[cli]" pillow requests

    # Host on ImgBB (free key from https://api.imgbb.com/ -> Add API key):
    python3 debgLeague.py --league lg3_edited.json --out lg3_nobg.json \
        --host imgbb --imgbb-key YOUR_IMGBB_KEY

    # OR host in your (public) GitHub repo via jsDelivr - no key, no rate limits:
    python3 debgLeague.py --league lg3_edited.json --out lg3_nobg.json \
        --host github --repo-dir /path/to/zengmcloud \
        --repo-slug achum5/zengmcloud --branch master
    # then: git add playerFaces && git commit && git push  (URLs go live on push)

Notes
-----
* Only player `imgURL`s are touched. Team logos are left alone.
* Duplicate URLs are processed once.
* Progress is cached in the --map file, so re-running skips finished images and
  retries only the ones that failed (e.g. a host that rate-limited you). Safe to
  Ctrl-C and resume.
* A photo that can't be downloaded or cut out keeps its ORIGINAL url, so nothing
  is ever lost or blanked.
* `birefnet-portrait` (default) is the sharpest model for headshots;
  `isnet-general-use` is a lighter all-rounder. First run downloads the model
  once. Add --alpha-matting for cleaner edges (hair/outlines) at some speed cost.
* Cutouts are compressed with pngquant if it's installed (apt-get install
  pngquant) — roughly halves the file size with no visible loss at this scale.
"""

import argparse
import base64
import hashlib
import io
import json
import os
import shutil
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


def url_key(url):
    """Deterministic short id for a source URL (stable across runs, so github
    filenames dedupe and resume cleanly)."""
    return hashlib.md5(url.encode("utf-8")).hexdigest()[:16]

# --- lazy heavy imports (so --dry-run works without rembg/PIL/requests) -------

_rembg_session = None
_rembg_lock = threading.Lock()


def get_rembg_session(model):
    global _rembg_session
    if _rembg_session is None:
        from rembg import new_session

        _rembg_session = new_session(model)
    return _rembg_session


# --- image download ----------------------------------------------------------

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
}


def download(url, retries=3):
    import requests

    last = None
    for attempt in range(retries):
        try:
            headers = dict(BROWSER_HEADERS)
            # A referer of the image's own host placates hotlink filters.
            try:
                from urllib.parse import urlsplit

                parts = urlsplit(url)
                headers["Referer"] = f"{parts.scheme}://{parts.netloc}/"
            except Exception:
                pass
            resp = requests.get(url, headers=headers, timeout=30)
            if resp.status_code == 200 and resp.content:
                return resp.content
            last = f"HTTP {resp.status_code}"
        except Exception as exc:  # noqa: BLE001
            last = str(exc)
        time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"download failed ({last})")


# --- background removal + framing -------------------------------------------


def compress_png(png_bytes):
    """Shrink a transparent PNG with pngquant (palette + alpha) if it's on PATH;
    otherwise return the original bytes. Typically cuts size 50-70%."""
    if shutil.which("pngquant") is None:
        return png_bytes
    try:
        proc = subprocess.run(
            ["pngquant", "--quality=50-88", "--strip", "--force", "-"],
            input=png_bytes,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
        if proc.returncode == 0 and proc.stdout:
            return proc.stdout
    except Exception:  # noqa: BLE001
        pass
    return png_bytes


def cutout(raw_bytes, model, size, margin, alpha_matting):
    from rembg import remove
    from PIL import Image

    session = get_rembg_session(model)
    # Alpha matting refines mask edges (hair, jersey outlines) at some cost in
    # speed — the fix for "rough edges" on portraits.
    kwargs = {}
    if alpha_matting:
        kwargs = dict(
            alpha_matting=True,
            alpha_matting_foreground_threshold=240,
            alpha_matting_background_threshold=10,
            alpha_matting_erode_size=10,
        )
    # rembg is CPU/GPU bound; serialize inference, parallelize the I/O around it.
    with _rembg_lock:
        out = remove(raw_bytes, session=session, **kwargs)

    img = Image.open(io.BytesIO(out)).convert("RGBA")

    # Crop to the subject (alpha bounding box), then center on a square,
    # transparent canvas so every player frames identically in the app.
    bbox = img.getbbox()
    if bbox:
        img = img.crop(bbox)

    w, h = img.size
    side = int(max(w, h) * (1 + margin))
    canvas = Image.new("RGBA", (side, side), (0, 0, 0, 0))
    canvas.paste(img, ((side - w) // 2, (side - h) // 2), img)

    if side != size:
        canvas = canvas.resize((size, size), Image.LANCZOS)

    buf = io.BytesIO()
    canvas.save(buf, format="PNG", optimize=True)
    return compress_png(buf.getvalue())


# --- hosting -----------------------------------------------------------------


def save_github(png_bytes, out_dir, slug, branch, subdir, key):
    """Write the cutout into the repo working tree and return its jsDelivr URL.
    You commit + push the folder afterward; the URL is live once GitHub has it."""
    dest_dir = os.path.join(out_dir, subdir)
    os.makedirs(dest_dir, exist_ok=True)
    fname = f"{key}.png"
    with open(os.path.join(dest_dir, fname), "wb") as fh:
        fh.write(png_bytes)
    return f"https://cdn.jsdelivr.net/gh/{slug}@{branch}/{subdir}/{fname}"


def upload_imgbb(png_bytes, key, name, retries=4):
    import requests

    b64 = base64.b64encode(png_bytes).decode("ascii")
    last = None
    for attempt in range(retries):
        try:
            resp = requests.post(
                "https://api.imgbb.com/1/upload",
                data={"key": key, "image": b64, "name": name},
                timeout=60,
            )
            if resp.status_code == 200:
                return resp.json()["data"]["url"]
            last = f"HTTP {resp.status_code}: {resp.text[:200]}"
            # Rate limited / transient — back off and retry.
            if resp.status_code in (429, 500, 502, 503):
                time.sleep(3 * (attempt + 1))
                continue
        except Exception as exc:  # noqa: BLE001
            last = str(exc)
            time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"upload failed ({last})")


# --- map persistence ---------------------------------------------------------

_map_lock = threading.Lock()


def load_map(path):
    if os.path.exists(path):
        with open(path) as fh:
            return json.load(fh)
    return {"done": {}, "failed": {}}


def save_map(path, data):
    with _map_lock:
        tmp = path + ".tmp"
        with open(tmp, "w") as fh:
            json.dump(data, fh)
        os.replace(tmp, path)


# --- main --------------------------------------------------------------------


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--league", required=True, help="input league .json")
    ap.add_argument("--out", required=True, help="output league .json")
    ap.add_argument(
        "--host",
        choices=["imgbb", "github"],
        default="imgbb",
        help="where cutouts are hosted",
    )
    ap.add_argument("--imgbb-key", help="ImgBB API key (host=imgbb)")
    # github host: files land in <repo-dir>/<subdir>/, served via jsDelivr.
    ap.add_argument("--repo-dir", default=".", help="repo working tree (host=github)")
    ap.add_argument(
        "--repo-slug", default="achum5/zengmcloud", help="owner/repo (host=github)"
    )
    ap.add_argument("--branch", default="master", help="repo branch (host=github)")
    ap.add_argument("--subdir", default="playerFaces", help="repo folder (host=github)")
    ap.add_argument("--map", default="url_map.json", help="resume/cache file")
    ap.add_argument("--model", default="birefnet-portrait")
    ap.add_argument(
        "--alpha-matting",
        action="store_true",
        help="refine mask edges (cleaner hair/outlines, slower)",
    )
    ap.add_argument("--size", type=int, default=200, help="output square px")
    ap.add_argument("--margin", type=float, default=0.12, help="padding fraction")
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--limit", type=int, default=0, help="process only N (testing)")
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="no network/model: just report + rewrite from an existing map",
    )
    args = ap.parse_args()

    with open(args.league) as fh:
        league = json.load(fh)
    players = league.get("players", [])

    # Unique player photo URLs.
    urls = []
    seen = set()
    for p in players:
        u = p.get("imgURL")
        if u and not u.startswith("data:") and u not in seen:
            seen.add(u)
            urls.append(u)

    state = load_map(args.map)
    done = state["done"]
    failed = state["failed"]

    todo = [u for u in urls if u not in done]
    if args.limit:
        todo = todo[: args.limit]

    print(f"players with photo : {sum(1 for p in players if p.get('imgURL'))}")
    print(f"unique photo URLs  : {len(urls)}")
    print(f"already hosted     : {len(done)}")
    print(f"to process now     : {len(todo)}")

    if args.dry_run:
        print(f"[dry-run] host={args.host}, skipping download/removal/host")
    else:
        if args.host == "imgbb" and not args.imgbb_key:
            sys.exit("ERROR: host=imgbb needs --imgbb-key (or use --dry-run)")

        counter = {"ok": 0, "fail": 0}
        clock = {"t": time.time()}

        def work(url):
            key = url_key(url)
            raw = download(url)
            png = cutout(raw, args.model, args.size, args.margin, args.alpha_matting)
            if args.host == "github":
                hosted = save_github(
                    png, args.repo_dir, args.repo_slug, args.branch, args.subdir, key
                )
            else:
                hosted = upload_imgbb(png, args.imgbb_key, "p_" + key)
            return url, hosted

        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futures = {ex.submit(work, u): u for u in todo}
            for fut in as_completed(futures):
                url = futures[fut]
                try:
                    _, hosted = fut.result()
                    done[url] = hosted
                    failed.pop(url, None)
                    counter["ok"] += 1
                except Exception as exc:  # noqa: BLE001
                    failed[url] = str(exc)
                    counter["fail"] += 1
                n = counter["ok"] + counter["fail"]
                # Persist progress periodically so a crash never loses work.
                if n % 25 == 0:
                    save_map(args.map, state)
                    rate = n / max(1e-6, time.time() - clock["t"])
                    print(
                        f"  {n}/{len(todo)}  ok={counter['ok']} "
                        f"fail={counter['fail']}  {rate:.1f}/s",
                        flush=True,
                    )

        save_map(args.map, state)
        print(f"done: ok={counter['ok']} fail={counter['fail']}")

    # Rewrite the league file from whatever's in the map.
    rewritten = 0
    for p in players:
        u = p.get("imgURL")
        if u and u in done:
            p["imgURL"] = done[u]
            rewritten += 1

    with open(args.out, "w") as fh:
        json.dump(league, fh)
    print(f"rewrote {rewritten} player photos -> {args.out}")
    if failed:
        print(f"{len(failed)} URLs failed (kept originals); re-run to retry them")
    if args.host == "github" and not args.dry_run:
        print(
            f"\nNEXT: commit + push the cutouts so the jsDelivr URLs go live:\n"
            f"  cd {args.repo_dir} && git add {args.subdir} && "
            f'git commit -m "player face cutouts" && git push'
        )


if __name__ == "__main__":
    main()
