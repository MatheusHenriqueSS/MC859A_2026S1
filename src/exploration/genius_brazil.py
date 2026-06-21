#!/usr/bin/env python3
"""Measure how many sample/cover/interpolation edges Genius adds for our
Brazilian-artist tracks (vs MusicBrainz's 766 within-Brazil edges).
Creds from env GENIUS_ID / GENIUS_SECRET. Run with PYTHONPATH=src."""
import os, re, json, time, urllib.parse, urllib.request, urllib.error
import graphs, analyze_common as C

CID = os.environ["GENIUS_ID"]; SEC = os.environ["GENIUS_SECRET"]
N_SAMPLE = 400


def get_token():
    data = urllib.parse.urlencode({"grant_type": "client_credentials",
                                   "client_id": CID, "client_secret": SEC}).encode()
    req = urllib.request.Request("https://api.genius.com/oauth/token", data=data)
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)["access_token"]


TOK = get_token()
print("auth OK", flush=True)


def api(path):
    for _ in range(5):
        req = urllib.request.Request("https://api.genius.com" + path)
        req.add_header("Authorization", "Bearer " + TOK)
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            if e.code == 429:
                time.sleep(5); continue
            return None
        except Exception:
            time.sleep(1); continue
    return None


DERIV = ("samples", "sampled_in", "interpolates", "interpolated_by",
         "cover_of", "covered_by", "remix_of", "remixed_by")


def clean(t):
    return re.sub(r"\s*[\(\[].*?(remix|mix|edit|version|ao vivo|live|feat).*?[\)\]]", "",
                  t, flags=re.I).strip()


covered_recs = set()
G = graphs.load("track_v2")
br = [(n, d) for n, d in G.nodes(data=True)
      if (d.get("country", "") or "") == "Brazil"
      and not C.is_placeholder(d.get("artist", "")) and (d.get("title", "") or "")]
br.sort(key=lambda x: x[0])
stride = max(1, len(br) // N_SAMPLE)
sample = br[::stride][:N_SAMPLE]
print(f"Brazilian-artist tracks: {len(br):,}; probing {len(sample)}", flush=True)

matched = with_rel = 0
edges = set()                 # dedup: (our_genius_id, other_id, kind)
by_type = {k: 0 for k in DERIV}
examples = []
for i, (n, d) in enumerate(sample):
    q = f"{d.get('artist','')} {clean(d.get('title',''))}".strip()
    s = api("/search?q=" + urllib.parse.quote(q))
    hits = (s or {}).get("response", {}).get("hits", [])
    if not hits:
        continue
    res = hits[0]["result"]
    matched += 1
    sid = res["id"]
    song = api(f"/songs/{sid}?text_format=plain")
    rels = (song or {}).get("response", {}).get("song", {}).get("song_relationships", [])
    had = False
    for r in rels:
        rt = r.get("relationship_type")
        if rt not in DERIV:
            continue
        for o in r.get("songs", []):
            key = (sid, o.get("id"), rt)
            if o.get("id") and key not in edges:
                edges.add(key); by_type[rt] += 1; had = True
                if len(examples) < 18 and rt in ("samples", "sampled_in", "interpolates", "interpolated_by"):
                    examples.append(f"{res.get('full_title','')[:34]}  --{rt}-->  {o.get('full_title','')[:34]}")
    if had:
        with_rel += 1
    if i % 50 == 0:
        print(f"  {i}/{len(sample)} processed, {matched} matched, {len(edges)} edges", flush=True)
    time.sleep(0.2)

print("\n===== GENIUS BRAZIL ENRICHMENT (sample) =====")
print(f"tracks probed:               {len(sample)}")
print(f"matched on Genius:           {matched} ({100*matched/len(sample):.1f}%)")
print(f"with >=1 deriv relationship: {with_rel} ({100*with_rel/len(sample):.1f}%)")
print(f"unique derivation edges:     {len(edges)}")
print("by type:", {k: v for k, v in by_type.items() if v})
scale = len(br) / len(sample)
print(f"\nextrapolated to all {len(br):,} BR tracks: ~{int(len(edges)*scale):,} edges "
      f"(MusicBrainz has 766 within-BR + ~364 cross-border)")
print("\nexample edges (samples/interpolations):")
for e in examples:
    print("  " + e)
