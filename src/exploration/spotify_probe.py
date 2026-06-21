#!/usr/bin/env python3
"""ROI probe: how many currently-uncovered recordings would gain a genre via
their artist's Spotify genres? (Spotify has no track genres — artist-level only.)

Creds from env SPOTIFY_ID / SPOTIFY_SECRET. Run from src/ (imports graphs)."""
import os, sys, time, csv, base64, json, urllib.parse, urllib.request, urllib.error
from collections import OrderedDict

import graphs
import analyze_common as C

CID = os.environ["SPOTIFY_ID"]; SEC = os.environ["SPOTIFY_SECRET"]
MAP = "../analysis_output/recording_genre_map.csv"
N_SAMPLE = 300


def get_token():
    data = urllib.parse.urlencode({"grant_type": "client_credentials"}).encode()
    req = urllib.request.Request("https://accounts.spotify.com/api/token", data=data)
    req.add_header("Authorization", "Basic " + base64.b64encode(f"{CID}:{SEC}".encode()).decode())
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)["access_token"]


TOK = get_token()
print("auth OK (token acquired)")


def api(url):
    global TOK
    for _ in range(6):
        req = urllib.request.Request(url)
        req.add_header("Authorization", "Bearer " + TOK)
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            if e.code == 429:
                time.sleep(int(e.headers.get("Retry-After", "2")) + 1); continue
            if e.code == 401:
                TOK = get_token(); continue
            return None
        except Exception:
            time.sleep(1); continue
    return None


def artist_genres(name):
    q = urllib.parse.quote(name)
    d = api(f"https://api.spotify.com/v1/search?q={q}&type=artist&limit=1")
    items = (d or {}).get("artists", {}).get("items", [])
    if not items:
        return None  # not found on Spotify
    return items[0].get("genres", [])  # possibly empty list


covered = set()
with open(MAP, newline="", encoding="utf-8") as f:
    r = csv.reader(f); next(r)
    for row in r:
        covered.add(row[0])

G = graphs.load("track_v2")
uncov = [(n, G.nodes[n]) for n in G
         if n not in covered and not C.is_placeholder(G.nodes[n].get("artist", ""))
         and (G.nodes[n].get("artist", "") or "").strip()]
uncov.sort(key=lambda x: x[0])
stride = max(1, len(uncov) // N_SAMPLE)
sample = uncov[::stride][:N_SAMPLE]

art_to_recs = OrderedDict()
for n, d in sample:
    art_to_recs.setdefault(d.get("artist", "").strip(), []).append(n)

found = with_genre = recs_with_genre = 0
rows = []
for a, recs in art_to_recs.items():
    g = artist_genres(a)
    if g is None:
        st = "NOT_FOUND"
    elif not g:
        st = "found,no-genre"; found += 1
    else:
        st = "GENRE: " + ", ".join(g[:3]); found += 1; with_genre += 1; recs_with_genre += len(recs)
    rows.append((len(recs), a, st))
    time.sleep(0.05)

print(f"\nsample recordings:           {len(sample)}")
print(f"unique (non-placeholder) artists: {len(art_to_recs)}")
print(f"artists found on Spotify:    {found} ({100*found/max(len(art_to_recs),1):.1f}%)")
print(f"  ...with >=1 Spotify genre: {with_genre} ({100*with_genre/max(len(art_to_recs),1):.1f}%)")
print(f"SAMPLE RECORDINGS THAT WOULD GAIN A GENRE: {recs_with_genre}/{len(sample)} "
      f"({100*recs_with_genre/max(len(sample),1):.1f}%)")
print("\nexamples:")
for nr, a, st in rows[:30]:
    print(f"  x{nr} {a[:38]:38} {st}")
