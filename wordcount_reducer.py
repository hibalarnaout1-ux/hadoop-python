#!/usr/bin/env python3
import sys
current_word, total = None, 0
for line in sys.stdin:
    parts = line.rstrip("\n").split("\t")
    if len(parts) != 2:
        continue
    word, count = parts
    try:
        count = int(count)
    except ValueError:
        continue
    if word == current_word:
        total += count
    else:
        if current_word is not None:
            print("%s\t%d" % (current_word, total))
        current_word, total = word, count
if current_word is not None:
    print("%s\t%d" % (current_word, total))
