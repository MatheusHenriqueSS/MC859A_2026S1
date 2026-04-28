#!/usr/bin/env python3
"""Extract only the MusicBrainz TSV tables we need from mbdump.tar.bz2.

Avoids the full ~50 GB extraction — pulls only the tables that participate
in the sample/remix/cover graph. Files land in data/tsv/ as tab-separated
PostgreSQL COPY files (NULLs written as \\N).
"""

from __future__ import annotations

import os
import sys
import tarfile

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TARBALL = os.path.join(REPO, "data", "mbdump.tar.bz2")
OUT_DIR = os.path.join(REPO, "data", "tsv")

NEEDED = {
    "mbdump/artist",
    "mbdump/artist_credit",
    "mbdump/artist_credit_name",
    "mbdump/recording",
    "mbdump/link",
    "mbdump/link_type",
    "mbdump/l_recording_recording",
    "mbdump/track",
    # Geography (country aggregation via artist.area).
    "mbdump/area",
    # Temporal (decade aggregation via track -> medium -> release_*).
    "mbdump/medium",
    "mbdump/release_country",
    "mbdump/release_unknown_country",
}


def main() -> None:
    if not os.path.exists(TARBALL):
        sys.exit(f"missing tarball: {TARBALL}")
    os.makedirs(OUT_DIR, exist_ok=True)

    remaining = {n for n in NEEDED
                 if not os.path.exists(os.path.join(OUT_DIR, os.path.basename(n)))}
    if not remaining:
        print("all needed TSVs already present, nothing to do")
    else:
        print(f"need to extract: {sorted(remaining)}")

    with tarfile.open(TARBALL, mode="r:bz2") as tf:
        for m in tf:
            if m.name not in remaining:
                continue
            base = os.path.basename(m.name)
            dst = os.path.join(OUT_DIR, base)
            print(f"extracting {m.name} -> {dst} ({m.size / 1024 / 1024:.1f} MB)", flush=True)
            with tf.extractfile(m) as src, open(dst, "wb") as out:
                while True:
                    chunk = src.read(1024 * 1024)
                    if not chunk:
                        break
                    out.write(chunk)
            remaining.discard(m.name)
            if not remaining:
                print("all needed members extracted, stopping early", flush=True)
                break

    print("done.")
    for name in sorted(os.listdir(OUT_DIR)):
        path = os.path.join(OUT_DIR, name)
        print(f"  {name}: {os.path.getsize(path) / 1024 / 1024:.1f} MB")


if __name__ == "__main__":
    main()
