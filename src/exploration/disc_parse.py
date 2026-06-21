import sys, xml.etree.ElementTree as ET
from collections import Counter
N = 60000
count = br = 0
fields = set(); styles = Counter(); roles = Counter(); samp_roles = Counter()
example = None
for ev, el in ET.iterparse(sys.stdin.buffer, events=("end",)):
    if el.tag != "release":
        continue
    count += 1
    for c in el:
        fields.add(c.tag)
    is_br = el.findtext("country") == "Brazil"
    if is_br:
        br += 1
        for s in el.findall("./styles/style"):
            if s.text:
                styles[s.text] += 1
    for r in el.findall(".//role"):
        if r.text:
            roles[r.text.strip()] += 1
            if "sampl" in r.text.lower():
                samp_roles[r.text.strip()] += 1
    if example is None and is_br:
        example = ET.tostring(el, encoding="unicode")
    el.clear()
    if count >= N:
        break
print("releases parsed:           ", count)
print("release child fields:      ", sorted(fields))
print("Brazil-country releases:   ", br)
print("top Brazilian styles:      ", styles.most_common(15))
print('roles containing "sampl":  ', dict(samp_roles) or "NONE")
print("top roles overall:         ", [r for r, _ in roles.most_common(12)])
if example:
    print("\n--- example Brazilian release (trimmed) ---")
    print(example[:1600])
