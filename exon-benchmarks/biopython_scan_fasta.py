# Script that given a path to a FASTA will count the number
# of sequences that do not start with M

import gzip
import sys

from Bio import SeqIO

def main():
    fasta_path = sys.argv[1]
    count = 0

    if fasta_path.endswith('.gz'):
        fasta_path = gzip.open(fasta_path, 'rt')
    else:
        fasta_path = open(fasta_path, 'rt')

    for record in SeqIO.parse(fasta_path, 'fasta'):
        if record.seq[0] != 'M':
            count += 1
    print(count)

main()
