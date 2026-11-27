#!/usr/bin/env python3
# Simule Hadoop Streaming en local : map -> sort -> reduce
import subprocess
import sys

mapper, reducer, fichier = sys.argv[1], sys.argv[2], sys.argv[3]

with open(fichier, "rb") as f:
    mapped = subprocess.run([sys.executable, mapper], stdin=f,
                            stdout=subprocess.PIPE, check=True).stdout

lines = sorted(mapped.decode("utf-8").splitlines(True))   # shuffle & sort
reduced = subprocess.run([sys.executable, reducer],
                         input="".join(lines).encode("utf-8"),
                         stdout=subprocess.PIPE, check=True).stdout

sys.stdout.write(reduced.decode("utf-8"))