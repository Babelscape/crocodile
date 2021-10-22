import multiprocessing
from pipeline.entitylinker import *
from pipeline.triplealigner import *
from pipeline.datareader import WikiDataAbstractsDataReader
from pipeline.writer import JsonWriter, JsonlWriter, OutputSplitter, NextFile
from utils.triplereader import *
from pipeline.filter import *
import argparse
from timeit import default_timer

__START_DOC__ = 0   #start reading from document number
__CORES__ = 7

parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                    formatter_class=argparse.RawDescriptionHelpFormatter,
                                    description=__doc__)
parser.add_argument("--input", default = 'text/ko', 
                    help="XML wiki dump file")
parser.add_argument("--output", default = './out/ko',
                    help="XML wiki dump file")
parser.add_argument("--input_triples", default = 'data/ko/wikidata-triples-ko-subj.db',
                    help="XML wiki dump file")
parser.add_argument("--language", default = 'ko',
                    help="language to use")
 
args = parser.parse_args()
# Reading the Wikipedia Abstracts Dataset
reader = WikiDataAbstractsDataReader(args.input)
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
# writer = JsonlWriter(args.output, "re-nlg", filesize=5000, startfile=__START_DOC__)
nextFile = NextFile(args.output)
output = OutputSplitter(nextFile, 5000, False)

def multhithreadprocess(q, output_queue):
    while True:
        d = q.get()
        if d is None:
            break
        if trip_read.get_exists(d.uri, 'P31', filter_entities):
            continue
        d = date.run(d)
        # d = date.run(d)
        if not main_ent_lim.run(d):
            # output_queue.put('skip')
            continue
        if not min_ent_lim.run(d):
            # output_queue.put('skip')
            continue
        d = NSalign.run(d)
        d = Salign.run(d)
        if not min_trip_lim.run(d):
            # output_queue.put('skip')
            continue
        output_queue.put(d)
        
def reduce_process(output_queue, output):
    """Pull finished article text, write series of files (or stdout)
    :param output_queue: text to be output.
    :param output: file object where to print.
    """
    print('reduce_process')
    period = 5000
    interval_start = default_timer()
    # FIXME: use a heap
    ordering_buffer = {}  # collected pages
    next_ordinal = 0  # sequence number of pages
    while True:
        d = output_queue.get()
        if d is None:
            break
        if d == 'skip':
            continue
        output.run(d)
        next_ordinal += 1
        if next_ordinal % period == 0:
            interval_rate = period / (default_timer() - interval_start)
            print(f"Extracted {next_ordinal} articles ({interval_rate} art/s)")

            interval_start = default_timer()

if __name__ == '__main__':
    # multiprocessing.set_start_method('spawn')
    # output queue
    interval_start = default_timer()
    output_queue = multiprocessing.Queue(maxsize=__CORES__*20)

    # Reduce job that sorts and prints output
    reduce = multiprocessing.Process(target=reduce_process, args=(output_queue, output))
    reduce.start()
    try:
        # __CORES__ = 2
        q = multiprocessing.Queue(maxsize=__CORES__*20)
        # iolock = ctx.Lock()
        # pool = ctx.Pool(__CORES__, initializer=multhithreadprocess, initargs=(q, writer_output))
        workers = []
        for _ in range(max(1, __CORES__)):
            extractor = multiprocessing.Process(target=multhithreadprocess,
                                args=(q, output_queue))
            extractor.daemon = True  # only live while parent process lives
            extractor.start()
            workers.append(extractor)

        for d in reader.read_documents():
            # if trip_read.get_exists(d.uri, 'P31', filter_entities):
            #     continue
            # d = date.run(d)
            q.put(d)  # blocks until q below its max size
        for _ in workers:  # tell workers we're done
            q.put(None)
        # signal termination
        # wait for workers to terminate
        for w in workers:
            w.join()
        # signal end of work to reduce process
        output_queue.put(None)
        # wait for it to finish
        reduce.join()
    finally:
        for _ in workers:  # tell workers we're done
            q.put(None)
        # signal termination
        # wait for workers to terminate
        for w in workers:
            w.join()
        # signal end of work to reduce process
        output_queue.put(None)
        # wait for it to finish
        reduce.join()
    print(f'Finished in {(default_timer() - interval_start)}')
