#! /usr/bin/python3
import sys
from pandas import read_csv
import os
from zipfile import ZipFile

target_cols = [
        'FlightDate',
        'UniqueCarrier',
        'FlightNum',
        'Origin',
        'Dest',
        'DepTime',
        'DepDelay',
        'ArrTime',
        'ArrDelay'
        ]

def main():
    input_path = sys.argv[1]
    zip_file = ZipFile(input_path)
    input_file_base = os.path.basename(input_path)
    input_file = os.path.splitext(input_file_base)[0] + ".csv"
    output_path = os.path.splitext(input_path)[0] + ".bz2"
    content = read_csv(zip_file.open(input_file), usecols=target_cols)
    content.to_csv(output_path, compression='bz2', index=False)

if __name__ == '__main__':
    main()
