#!/usr/bin/env python3
import sys
current, total = None, 0.0
for line in sys.stdin:
    parts = line.rstrip("\n").split("\t")
    if len(parts) != 2:
        continue
    magasin = parts[0]
    try:
        cout = float(parts[1])
    except ValueError:
        continue
    if magasin == current:
        total += cout
    else:
        if current is not None:
            print("%s\t%.2f" % (current, total))
        current, total = magasin, cout
if current is not None:
    print("%s\t%.2f" % (current, total))
