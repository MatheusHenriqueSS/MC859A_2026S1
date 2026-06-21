#!/usr/bin/env python3
"""Extract only the MusicBrainz TSV tables we need from the public dumps.

Avoids the full ~50 GB extraction — pulls only the tables that participate in the
sample/remix/cover graph. Files land in data/tsv/ as tab-separated PostgreSQL
COPY files (NULLs written as \\N).

Two source tarballs:
  - mbdump.tar.bz2          (core)    — entities, relationships, `genre` vocab.
  - mbdump-derived.tar.bz2  (derived) — folksonomy tags (`tag`, `*_tag`), used
                                        for the F2 genre layer.

NOTE: the core dump used for F1/F2 is export 20260425-002540. That export was
rotated off the mirror, so the tag tables are taken from a later derived dump;
MusicBrainz ids are stable across exports, so the join by recording/artist id is
valid. Download whichever derived export is current and place it next to the core
tarball as data/mbdump-derived.tar.bz2.
"""

from __future__ import annotations

import os
import sys
import tarfile

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TARBALL_CORE = os.path.join(REPO, "data", "mbdump.tar.bz2")
TARBALL_DERIVED = os.path.join(REPO, "data", "mbdump-derived.tar.bz2")
OUT_DIR = os.path.join(REPO, "data", "tsv")

NEEDED_CORE = {
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
    # Genre vocabulary (F2 Phase 2).
    "mbdump/genre",
    # Release-group genre fallback (F2 Phase 2b): recording -> medium -> release
    # -> release_group; `release` provides the release -> release_group link.
    "mbdump/release",
}

NEEDED_DERIVED = {
    # Folksonomy tags — a tag is a genre when its name is in the `genre` vocab.
    "mbdump/tag",
    "mbdump/artist_tag",
    "mbdump/recording_tag",
    # Album-level genre tags (denser than recording/artist tags).
    "mbdump/release_group_tag",
}


def extract_from(tarball: str, needed: set) -> None:
    if not os.path.exists(tarball):
        print(f"  tarball not present, skipping: {tarball}")
        return
    remaining = {n for n in needed
                 if not os.path.exists(os.path.join(OUT_DIR, os.path.basename(n)))}
    if not remaining:
        print(f"  all needed already present from {os.path.basename(tarball)}")
        return
    print(f"  from {os.path.basename(tarball)} need: {sorted(remaining)}")
    with tarfile.open(tarball, mode="r:bz2") as tf:
        for m in tf:
            if m.name not in remaining:
                continue
            dst = os.path.join(OUT_DIR, os.path.basename(m.name))
            print(f"  extracting {m.name} -> {dst} ({m.size / 1024 / 1024:.1f} MB)", flush=True)
            with tf.extractfile(m) as src, open(dst, "wb") as out:
                while True:
                    chunk = src.read(1024 * 1024)
                    if not chunk:
                        break
                    out.write(chunk)
            remaining.discard(m.name)
            if not remaining:
                print(f"  all needed members extracted from {os.path.basename(tarball)}", flush=True)
                break
    if remaining:
        print(f"  WARNING: not found in {os.path.basename(tarball)}: {sorted(remaining)}")


def main() -> None:
    if not os.path.exists(TARBALL_CORE):
        sys.exit(f"missing core tarball: {TARBALL_CORE}")
    os.makedirs(OUT_DIR, exist_ok=True)
    extract_from(TARBALL_CORE, NEEDED_CORE)
    extract_from(TARBALL_DERIVED, NEEDED_DERIVED)
    print("done.")
    for name in sorted(os.listdir(OUT_DIR)):
        path = os.path.join(OUT_DIR, name)
        print(f"  {name}: {os.path.getsize(path) / 1024 / 1024:.1f} MB")


if __name__ == "__main__":
    main()
