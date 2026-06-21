# Data-source exploration (F2 auxiliary — NOT the reproducible pipeline)

These scripts investigated whether external sources could add sampling edges or
close the genre-coverage gap (especially for **Brazil**). They are auxiliary: the
graded deliverable graph stays 100% **MusicBrainz (CC0)**. All read credentials
from **env vars** (no secrets in code); run with `PYTHONPATH=..` from `src/`.

## Findings (2026-06-21)

- **`spotify_probe.py` — Spotify: ✗ unusable.** Under the Client-Credentials flow,
  Spotify now returns stripped objects: artist `genres` field is **gone**
  (deprecated platform-wide), and `popularity`/`followers` are absent too. No
  sampling data exists on Spotify at all. Verdict: dead end.

- **`disc_parse.py` — Discogs: ✗ for sampling.** Streamed the real
  `discogs_20260501_releases.xml.gz` (10.3 GB) bounded to 60k releases. Schema has
  `genres`/`styles`/`country`/`master_id`/`tracklist`/`extraartists` but **no
  structured track→track sample links** — sampling appears only as free-text
  credit roles (`Performer [Sample]`, `Written-By [Sample of 'Escape']`). Rich
  styles, but no MusicBrainz ids (fuzzy matching) + 10.3 GB → not worth it over our
  83.1% genre coverage.

- **`genius_probe.py` / `genius_brazil.py` — Genius: ✓ best for Brazil.** Official
  free API exposes `song_relationships` (samples / sampled_in / interpolates /
  interpolated_by / cover_of / covered_by / remix_of / remixed_by). 400-track
  Brazilian sample: **80.5% matched, 320 derivation edges → ~1,144 extrapolated**
  for all 1,430 BR tracks (vs MusicBrainz's 766 within-BR). Adds **interpolations**
  and **international reuse of Brazilian sources** (e.g. Banda Black Rio → Yasiin
  Bey, Airto Moreira → Bellini "Samba de Janeiro") that MusicBrainz lacks.
  **Caveat:** Genius data is proprietary (non-commercial use OK; lyrics are the
  protected part, which we don't touch) → use as a **cited case study**, do NOT
  redistribute raw edges in the public repo.

## Takeaway

Rich Brazilian *sampling* data lives in proprietary databases (Genius / WhoSampled),
not in open data — a data-availability finding worth stating in the report. Open
CC0 MusicBrainz is reproducible but Anglophone-sparse on Brazil.
