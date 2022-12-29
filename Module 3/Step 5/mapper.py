#! /usr/bin/env python
"""mapper.py"""

import sys


def perform_map():
    for line in sys.stdin:
        line = line.strip().split(',')
        payment_type = line[9]
        month = line[1][:7]
        tip_amount = line[13]
        print('%s\t%s\t%s' % (payment_type, month, tip_amount))


if __name__ == '__main__':
    perform_map()
