import jsonlines
import re
from tqdm import trange, tqdm
import argparse
import os, sys

def prepare_triplet(subject_entity, object_entity, article_text, predicate):
    text_triplet = ''
    text_triplet += article_text[min(subject_entity['boundaries'][1], object_entity['boundaries'][1]):max(subject_entity['boundaries'][0], object_entity['boundaries'][0])]
    return text_triplet.strip('\n')

def main(folder_input = 'out/es/AA/'):
    if folder_input.split("/")[0] == 'out_clean':
        path = f'relations_clean/{folder_input.split("/")[1]}'
    else:
        path = f'relations_inter/{folder_input.split("/")[1]}'
    try:
        os.mkdir(path)
    except OSError:
        print ("Creation of the directory %s failed" % path)
        pass
    else:
        print ("Successfully created the directory %s " % path)
    for k,j,y in os.walk(folder_input):
        for file_name in y:
            with jsonlines.open(k + '/' + file_name) as reader:
                for i, article in tqdm(enumerate(reader)):
                    previous = []
                    for triple in article['triples']:
                        with open(f"{path}/{triple['predicate']['uri']}.txt", "a+") as file:
                            if triple['subject']['boundaries'] != None and triple['object']['boundaries'] != None and (triple['subject']['uri'], triple['object']['uri']) not in previous:
                                previous.append((triple['subject']['uri'], triple['object']['uri']))
                                text = prepare_triplet(triple['subject'], triple['object'], article['text'], triple["predicate"])
                            elif (triple['subject']['uri'], triple['object']['uri']) not in previous:
                                distance = 1000000
                                for entity in article['entities']:
                                    if entity['uri'] == triple['subject']['uri']:
                                        if abs(min(triple['object']['boundaries'])-min(entity['boundaries'])) < distance:
                                            subject_entity = entity
                                            distance = abs(min(triple['object']['boundaries'])-min(entity['boundaries']))
                                triple['subject'] = subject_entity
                                previous.append((triple['subject']['uri'], triple['object']['uri']))
                                text = prepare_triplet(subject_entity, triple['object'], article['text'], triple["predicate"])
                            file.write(text.replace("\n", "\\n").replace("\t", "\\t") + '\n')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description=__doc__)
    parser.add_argument("--folder_input", 
                        help="input file")
    args = parser.parse_args()

    main(args.folder_input)
