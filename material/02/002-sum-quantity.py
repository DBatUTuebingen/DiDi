#!/usr/bin/env python3

import sys

sum = 0

with open(sys.argv[1], 'r') as file:
    for line in file:
        sum = sum + int(line.split('|', 5)[4])

print(sum)
