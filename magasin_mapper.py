#!/usr/bin/env python3
import sys
for line in sys.stdin:
    fields = line.rstrip("\n").split("\t")
    if len(fields) != 6:
        continue
    magasin, cout = fields[2], fields[4]
    try:
        float(cout)
    except ValueError:
        continue
    print("%s\t%s" % (magasin, cout))
