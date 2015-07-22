#!/usr/bin/env python
import sys, os, subprocess
from argparse import ArgumentParser

def main():
    parser = ArgumentParser()
    parser.add_argument('-c', '--conf', metavar='File',
                        help='set config file, default to wesqc.conf')
    parser.add_argument('-s', '--step', default="0-3", metavar='STR',
                        help='run the pipeline on which steps')
    parser.add_argument('-j', '--jobs', default=0, metavar='INT',
                        help='the executer number, 0 implies let spark decide')

    args = parser.parse_args()

    if os.path.isfile(args.conf):
        subprocess.call(["", ])


if __name__ == "__main__":
    main()
