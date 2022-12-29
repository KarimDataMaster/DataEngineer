#! /usr/bin/env python
#!/usr/bin/python3

import sys
# lines = sys.stdin.readlines()
# print("".join(sorted(set(lines))))


def perform_map():
    for line in sys.stdin:
        line = line.strip().split(',')
        payment_type = line[9]
        tip_amount = line[13]
        print('%s\t%s' % (payment_type, tip_amount))


if __name__ == '__main__':
    perform_map()
