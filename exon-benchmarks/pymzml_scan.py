# pip install "pymzml[full]"

import sys

import pymzml


def main():
    mzml_path = sys.argv[1]
    count = 0

    run = pymzml.run.Reader(mzml_path)

    for _ in run:
        count += 1

    print(count)

main()
