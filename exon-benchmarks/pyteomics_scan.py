import sys

from pyteomics.mzml import MzML


def main():
    mzml_path = sys.argv[1]
    count = 0

    mzml = MzML(mzml_path)

    for _ in mzml:
        count += 1

    print(count)

main()
