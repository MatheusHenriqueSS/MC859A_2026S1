#!/usr/bin/env python3
"""Full Genius case study for Brazil — fetch sample/cover/interpolation/remix
relationships for ALL Brazilian-artist recordings in track_v2, then build
rankings + a derivation subgraph.

Genius data is proprietary (non-commercial use OK; lyrics are the protected part,
which we never touch). Outputs go to REPO/private/ which is gitignored — they are a
CITED CASE STUDY, not redistributed raw. The CC0 MusicBrainz graph stays the
graded backbone; this only quantifies what open data misses for Brazil.

Creds from env GENIUS_ID / GENIUS_SECRET (source ~/.genius_creds). Resumable:
re-running skips tracks already in genius_brazil_raw.jsonl. Run with PYTHONPATH=src.
"""
from __future__ import annotations

import csv
import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, defaultdict

import networkx as nx

import graphs
import analyze_common as C

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PRIV = os.path.join(REPO, "private")
os.makedirs(PRIV, exist_ok=True)
RAW = os.path.join(PRIV, "genius_brazil_raw.jsonl")

CID = os.environ["GENIUS_ID"]
SEC = os.environ["GENIUS_SECRET"]

# Genius relationship_type -> (orientation, is_sample_family)
#   orientation "deriv"  : our track derives FROM partner  (our -> partner)
#   orientation "source" : our track is the SOURCE for partner (partner -> our)
REL = {
    "samples": ("deriv", True), "interpolates": ("deriv", True),
    "cover_of": ("deriv", False), "remix_of": ("deriv", False),
    "sampled_in": ("source", True), "interpolated_by": ("source", True),
    "covered_by": ("source", False), "remixed_by": ("source", False),
}


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


def clean(t):
    return re.sub(r"\s*[\(\[].*?(remix|mix|edit|version|ao vivo|live|feat).*?[\)\]]", "",
                  t or "", flags=re.I).strip()


def norm(s):
    return re.sub(r"[^a-z0-9]+", " ", (s or "").lower()).strip()


def first_tokens(s, k=2):
    return set(norm(s).split()[:k])


def fetch():
    G = graphs.load("track_v2")
    br = [(n, d) for n, d in G.nodes(data=True)
          if (d.get("country") or "") == "Brazil"
          and not C.is_placeholder(d.get("artist", "")) and (d.get("title") or "")]
    br.sort(key=lambda x: x[0])
    print(f"Brazilian-artist tracks: {len(br):,}", flush=True)

    done = set()
    if os.path.exists(RAW):
        with open(RAW, encoding="utf-8") as f:
            for line in f:
                try:
                    done.add(json.loads(line)["node"])
                except Exception:
                    pass
        print(f"resuming: {len(done):,} already fetched", flush=True)

    todo = [(n, d) for n, d in br if n not in done]
    out = open(RAW, "a", encoding="utf-8")
    for i, (n, d) in enumerate(todo):
        artist, title = d.get("artist", ""), d.get("title", "")
        rec = {"node": n, "artist": artist, "title": title, "matched": False}
        s = api("/search?q=" + urllib.parse.quote(f"{artist} {clean(title)}".strip()))
        hits = (s or {}).get("response", {}).get("hits", [])
        if hits:
            res = hits[0]["result"]
            pa = res.get("primary_artist", {}).get("name", "")
            verified = bool(first_tokens(artist) & first_tokens(pa)) or \
                bool(norm(clean(title)) and norm(clean(title)) in norm(res.get("title", "")))
            rec.update(matched=True, genius_id=res["id"], full_title=res.get("full_title", ""),
                       primary_artist=pa, verified=verified, rels=[])
            song = api(f"/songs/{res['id']}?text_format=plain")
            rels = (song or {}).get("response", {}).get("song", {}).get("song_relationships", [])
            for r in rels:
                rt = r.get("relationship_type")
                if rt not in REL:
                    continue
                for o in r.get("songs", []):
                    if o.get("id"):
                        rec["rels"].append({
                            "type": rt, "other_id": o["id"],
                            "other_title": o.get("full_title", ""),
                            "other_artist": o.get("primary_artist", {}).get("name", ""),
                        })
        out.write(json.dumps(rec, ensure_ascii=False) + "\n")
        out.flush()
        if i % 50 == 0:
            print(f"  {i}/{len(todo)} processed (+{len(done)} cached)", flush=True)
        time.sleep(0.2)
    out.close()
    print("fetch complete", flush=True)


def analyze():
    """Aggregate the JSONL into rankings, a summary, and a GraphML subgraph."""
    records = []
    with open(RAW, encoding="utf-8") as f:
        for line in f:
            try:
                records.append(json.loads(line))
            except Exception:
                pass
    matched = [r for r in records if r.get("matched")]
    verified = [r for r in matched if r.get("verified")]
    print(f"records {len(records)} | matched {len(matched)} | verified {len(verified)}", flush=True)

    # build edges from VERIFIED matches only (quality), oriented A -> B = "A derives from B"
    Gx = nx.DiGraph()
    edges_csv = []
    reused = Counter()       # BR track full_title -> times it is a source
    samples = Counter()      # partner full_title -> times BR samples it
    by_type = Counter()
    for r in verified:
        ours = (r["full_title"], r["genius_id"], r.get("primary_artist", ""))
        Gx.add_node(r["genius_id"], title=r["full_title"],
                    artist=r.get("primary_artist", ""), is_brazil_seed=1)
        for rel in r["rels"]:
            ot, oid, otitle, oartist = rel["type"], rel["other_id"], rel["other_title"], rel["other_artist"]
            orient, _is_sample = REL[ot]
            by_type[ot] += 1
            if oid not in Gx:
                Gx.add_node(oid, title=otitle, artist=oartist, is_brazil_seed=0)
            if orient == "deriv":          # our -> partner (BR samples partner)
                a, b = r["genius_id"], oid
                samples[f"{oartist} — {otitle}"] += 1
            else:                          # partner -> our (BR is the source)
                a, b = oid, r["genius_id"]
                reused[f"{r.get('primary_artist','')} — {r['full_title']}"] += 1
            Gx.add_edge(a, b, relation=ot)
            edges_csv.append([r["full_title"], ot, otitle, oartist])

    # write outputs (all under private/, gitignored)
    with open(os.path.join(PRIV, "genius_brazil_edges.csv"), "w", newline="", encoding="utf-8") as f:
        wr = csv.writer(f)
        wr.writerow(["brazil_track", "relation", "partner_track", "partner_artist"])
        wr.writerows(edges_csv)

    def dump(counter, path, header):
        with open(path, "w", encoding="utf-8") as f:
            f.write(header + "\n" + "-" * 70 + "\n")
            for name, c in counter.most_common(60):
                f.write(f"{c:4d}  {name}\n")

    dump(reused, os.path.join(PRIV, "genius_br_most_reused.txt"),
         "Brazilian tracks most reused as a SOURCE (Genius)")
    dump(samples, os.path.join(PRIV, "genius_br_top_samples.txt"),
         "Tracks Brazilian songs most SAMPLE/derive from (Genius)")

    nx.write_graphml(Gx, os.path.join(PRIV, "genius_brazil_subgraph.graphml"))

    summary = {
        "records": len(records), "matched": len(matched),
        "match_rate": round(100 * len(matched) / max(1, len(records)), 1),
        "verified": len(verified),
        "verify_rate_of_matched": round(100 * len(verified) / max(1, len(matched)), 1),
        "derivation_edges_verified": len(edges_csv),
        "by_relation_type": dict(by_type.most_common()),
        "graph_nodes": Gx.number_of_nodes(), "graph_edges": Gx.number_of_edges(),
        "musicbrainz_comparison": {"mb_within_br_edges": 766, "mb_cross_border_br_edges": 364},
    }
    with open(os.path.join(PRIV, "genius_brazil_summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    print(json.dumps(summary, indent=2, ensure_ascii=False), flush=True)
    print(f"\nDONE. Outputs in {PRIV} (gitignored).", flush=True)


if __name__ == "__main__":
    import sys
    if "--analyze-only" not in sys.argv:
        fetch()
    analyze()
