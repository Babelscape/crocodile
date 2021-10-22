import multiprocessing
from pipeline.entitylinker import *
from pipeline.triplealigner import *
from pipeline.datareader import WikiDataAbstractsDataReader
from pipeline.writer import JsonWriter, JsonlWriter, OutputSplitter, NextFile
from pipeline.coreference import *
from utils.triplereader import *
from pipeline.filter import *
from pympler import muppy, summary
import pandas as pd
import argparse
from timeit import default_timer

__START_DOC__ = 0   #start reading from document number
__CORES__ = 7

parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                    formatter_class=argparse.RawDescriptionHelpFormatter,
                                    description=__doc__)
parser.add_argument("--input", default = 'text/zh', 
                    help="XML wiki dump file")
parser.add_argument("--output", default = './out/zh',
                    help="XML wiki dump file")
parser.add_argument("--input_triples", default = 'data/zh/wikidata-triples-zh-subj.csv',
                    help="XML wiki dump file")
parser.add_argument("--language", default = 'zh',
                    help="language to sue")
 
args = parser.parse_args()
# Reading the DBpedia Abstracts Dataset
reader = WikiDataAbstractsDataReader(args.input)
# Loading the WikidataSpotlightEntityLinker ... DBpedia Spotlight with mapping DBpedia URIs to Wikidata
# link = WikidataSpotlightEntityLinker('./datasets/wikidata/dbpedia-wikidata-sameas-dict.csv', support=10, confidence=0.4)
main_ent_lim = MainEntityLimiter()
min_ent_lim = EntityLimiter(2, 100)
min_trip_lim = MinTriplesLimiter(1)
# min_trip_lim = TriplesLimiter(5, 500)

filter_entities = ['Q4167410', 'Q13406463', 'Q18340514', 'Q12308941', 'Q11879590', 'Q101352']
# trip_read = TripleSPARQLReader('./datasets/wikidata/wikidata-triples.csv')
if args.input_triples.endswith('.db'):
    trip_read = TripleDBReader(args.input_triples, args.language)
else: 
    trip_read = TripleCSVReader(args.input_triples, args.language)
Salign = SimpleAligner(trip_read)
#prop = WikidataPropertyLinker('./datasets/wikidata/wikidata-properties.csv')
if args.language == 'zh':
    spacy_model = 'zh_core_web_sm'
elif args.language == 'en':
    spacy_model = 'en_core_web_sm'
elif args.language == 'es' or args.language == 'ca':
    spacy_model = 'es_core_news_sm'
elif args.language == 'it':
    spacy_model = 'it_core_news_sm'
else:
    spacy_model = 'xx_ent_wiki_sm'

# date = DateLinkerSpacy(spacy_model)
date = DateLinkerRegex(args.language)

#SPOalign = SPOAligner(trip_read)
NSalign = NoSubjectAlign(trip_read)
writer = JsonlWriter(args.output, "rebel", filesize=5000, startfile=__START_DOC__)


def reading_documents():
    # reading document and apply all non parallelizable process

    for d in reader.read_documents():
        # d = date.run(d)                     # SU Time is non parallelizable
        yield d

def process_document(d):

    if trip_read.get_exists(d.uri, 'P31', filter_entities):
        return None
    d = date.run(d)
    if not main_ent_lim.run(d):
        return None
    if not min_ent_lim.run(d):
        return None
    d = NSalign.run(d)
    d = Salign.run(d)
    if not min_trip_lim.run(d):
        return None
    writer.run(d)
    del(d)

    # print("error Processing document %s" % d.title)

if __name__ == '__main__':
    interval_start = default_timer()
    for d in reader.read_documents():
        process_document(d)
    print(f'Finished in {(default_timer() - interval_start)}')