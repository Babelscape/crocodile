import os, sys
import re
from csv import reader
from tqdm import tqdm
import argparse

import sqlite3

def main(input_file = '/mnt/sda/RE-NLG-Dataset/datasets/wikidata/wikidata-triples.csv', folder_input = 'text', output_file = 'wikidata-triples-ca-subj.csv'):
    wikidata_ids = set()
    pattern = re.compile(r"wikidata='(.*?)'")

    for i,j,y in os.walk(folder_input):
        for file_name in y:
            # print(i + '/' + file_name)
            for k, line in enumerate(open(i + '/' + file_name)):
                for match in re.finditer(pattern, line):
                    wikidata_ids.add(match.group(1))

    # open file in read mode
    with open(output_file,'w') as file:
        with open(input_file, 'r') as read_obj:
            # pass the file object to reader() to get the reader object
            csv_reader = reader(read_obj)
            # Iterate over each row in the csv using reader object
            for i, row in tqdm(enumerate(csv_reader)):
                # row variable is a list that represents a row in csv
                if row[0].split('\t')[0] in wikidata_ids:
                    file.write(row[0])
                    file.write('\n')
def main_db(input_file = '/mnt/sda/RE-NLG-Dataset/datasets/wikidata/wikidata-triples.csv', folder_input = 'text', output_file = 'wikidata-triples-ca-subj.csv'):
    wikidata_ids = set()
    pattern = re.compile(r'wikidata="(.*?)"')

    for i,j,y in os.walk(folder_input):
        for file_name in y:
            # print(i + '/' + file_name)
            for k, line in enumerate(open(i + '/' + file_name)):
                for match in re.finditer(pattern, line):
                    wikidata_ids.add(match.group(1))
    try:
        os.remove(output_file)
    except FileNotFoundError:
        pass
    print(f'Found {len(wikidata_ids)} different entities')
    conn = sqlite3.connect(output_file, isolation_level="EXCLUSIVE")
    with conn:
        conn.execute(
            """CREATE TABLE triplets (
            subject text,
            relation text,
            object text,
            subjobj text)"""
        )

    c = conn.cursor()

    # open file in read mode
    with open(input_file, 'r') as read_obj:
        # pass the file object to reader() to get the reader object
        csv_reader = reader(read_obj)
        # Iterate over each row in the csv using reader object
        for i, row in tqdm(enumerate(csv_reader)):
            # row variable is a list that represents a row in csv
            if row[0].split('\t')[0] in wikidata_ids:
                c.execute(
                    "INSERT INTO triplets (subject, relation, object, subjobj) VALUES (?, ?, ?, ?)",
                    (row[0].split('\t')[0], row[0].split('\t')[1], row[0].split('\t')[2], row[0].split('\t')[0] + '\t' + row[0].split('\t')[2]),
                )
    conn.commit()
    conn.execute("""CREATE INDEX idx_triplet_id ON triplets(subjobj);""")
    conn.commit()
    conn.execute("""CREATE INDEX idx_triplet_trio ON triplets(subject, relation, object);""")
    conn.commit()
if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description=__doc__)
    parser.add_argument("--input", 
                        help="XML wiki dump file")
    parser.add_argument("--output",
                        help="XML wiki dump file")
    parser.add_argument("--input_triples", 
                        help="XML wiki dump file")
    parser.add_argument("--format", 
                        help="format of the output file")
    args = parser.parse_args()
    if args.format == 'db':
        main_db(args.input_triples,args.input, args.output)
    else:
        main(args.input_triples,args.input, args.output)
