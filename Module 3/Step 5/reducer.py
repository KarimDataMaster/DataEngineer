#! /usr/bin/env python
"""reducer.py"""

import sys
from collections import defaultdict


def perform_reduce():
    H = defaultdict(list)
    for line in sys.stdin:
        pt, m, ta = line.split('\t')
        key = '%s\t%s' % (pt, m)
        if m == 'tpep_pickup_datetime':
            continue
        elif ta == 'tip_amount':
            continue
        elif pt == 'payment_type':
            continue
        else:
            H[key].append(float(ta.strip()))
    for t in H:
        print('%s\t%s' % (t, (sum(H[t]) // len(H[t]))))
        # print(f'{t}\t{sum(H[t]) // len(H[t])}')


if __name__ == '__main__':
    perform_reduce()
