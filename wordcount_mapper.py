#!/usr/bin/env python3
import sys
for line in sys.stdin:
    for word in line.split():
        print("%s\t1" % word)
