#!/usr/bin/env python

import sys
import pstats

argv_len = len(sys.argv)

if argv_len > 1:
    p = pstats.Stats(sys.argv[1])
    sorting = sys.argv[2].split(',') if argv_len > 2 else [-1]
    p.strip_dirs().sort_stats(*sorting).print_stats(150)

# pretty print using other python soft_l1
# you need pip install gprof2dot and install graphviz
# gprof2dot.py -f pstats output.pstats | dot -Tpng -o output.png