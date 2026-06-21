#!/usr/bin/env python3
"""Probe Genius API for sampling/derivation relationships, focused on Brazil.
Creds from env GENIUS_ID / GENIUS_SECRET."""
import os, json, time, urllib.parse, urllib.request, urllib.error

CID = os.environ["GENIUS_ID"]; SEC = os.environ["GENIUS_SECRET"]


def get_token():
    data = urllib.parse.urlencode({
        "grant_type": "client_credentials",
        "client_id": CID, "client_secret": SEC,
    }).encode()
    req = urllib.request.Request("https://api.genius.com/oauth/token", data=data)
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.load(r).get("access_token")
    except urllib.error.HTTPError as e:
        print("TOKEN ERROR", e.code, e.read()[:300].decode("utf-8", "replace"))
        return None


TOK = get_token()
if not TOK:
    print("\n>> client_credentials did not yield a token. Need the dashboard "
          "'Client Access Token' instead.")
    raise SystemExit
print("auth OK (token acquired)\n")


def api(path):
    req = urllib.request.Request("https://api.genius.com" + path)
    req.add_header("Authorization", "Bearer " + TOK)
    for _ in range(4):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            if e.code == 429:
                time.sleep(3); continue
            return None
        except Exception:
            time.sleep(1); continue
    return None


REL = ("samples", "sampled_in", "interpolates", "interpolated_by",
       "cover_of", "covered_by", "remix_of", "remixed_by")

CURATED = [
    "Jorge Ben Taj Mahal", "Tim Maia Não Quero Dinheiro", "Sérgio Mendes Mas que Nada",
    "Azymuth Jazz Carnival", "Marcos Valle Estrelar", "Banda Black Rio Maria Fumaça",
    "Antônio Carlos Jobim The Girl from Ipanema", "Gilberto Gil Toda Menina Baiana",
    "Os Mutantes A Minha Menina", "João Donato A Rã",
]

for q in CURATED:
    d = api("/search?q=" + urllib.parse.quote(q))
    hits = (d or {}).get("response", {}).get("hits", [])
    if not hits:
        print(f"[no hit] {q}")
        continue
    h = hits[0]["result"]
    sid, full = h["id"], h.get("full_title", h.get("title"))
    s = api(f"/songs/{sid}?text_format=plain")
    rels = (s or {}).get("response", {}).get("song", {}).get("song_relationships", [])
    summary = []
    for r in rels:
        rt = r.get("relationship_type")
        n = len(r.get("songs", []))
        if rt in REL and n:
            ex = r["songs"][0].get("full_title", "")
            summary.append(f"{rt}={n} (e.g. {ex[:45]})")
    print(f"\n• {full[:55]}")
    print("   " + ("; ".join(summary) if summary else "no sample/cover relationships listed"))
    time.sleep(0.3)
