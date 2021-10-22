import glob
import os, sys
from datasets import load_dataset
import argparse

def main(input_file = 'text/en1/AA/wiki_00.jsonl', language = 'en'):
    dataset = load_dataset('json', data_files=input_file)
    path = f'relations/{language}/'
    try:
        os.mkdir(path)
    except OSError:
        print ("Creation of the directory %s failed" % path)
        pass
    else:
        print ("Successfully created the directory %s " % path)
    # files = glob.glob(f'{path}/*')
    # for f in files:
    #     os.remove(f)

    for article in dataset['train']:
        previous = []
        for triple in article['triples']:
            if triple['subject']['boundaries'] != None and triple['object']['boundaries'] != None and (triple['subject']['uri'], triple['object']['uri']) not in previous:
                previous.append((triple['subject']['uri'], triple['object']['uri']))
                text_triplet = ''
                with open(f"{path}/{triple['predicate']['uri']}.txt", "a+") as file:
                    text_triplet += article['text'][:min(triple['subject']['boundaries'][0], triple['object']['boundaries'][0])].split('.')[-1]
                    text_triplet += article['text'][min(triple['subject']['boundaries'][0], triple['object']['boundaries'][0]):max(triple['subject']['boundaries'][1], triple['object']['boundaries'][1])]
                    text_triplet += article['text'][max(triple['subject']['boundaries'][1], triple['object']['boundaries'][1]):].split('.')[0]
                    file.write(text_triplet.strip('\n').replace("\n", "\\n").replace("\t", "\\t") + '\t' + triple['subject']['surfaceform'] + '\t' + triple['object']['surfaceform'] + '\t' + triple['predicate']['surfaceform'] + '\n')
if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description=__doc__)
    parser.add_argument("--input", 
                        help="input file")
    parser.add_argument("--lang",
                        help="XML wiki dump file")
    args = parser.parse_args()
    
    main(args.input, args.lang)
