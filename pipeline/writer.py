from pipeline.pipeline import *
import json
import pickle
import os
from timeit import default_timer


class NextFile():

    """
    Synchronous generation of next available file name.
    """

    filesPerDir = 100

    def __init__(self, path_name):
        self.path_name = path_name
        self.dir_index = -1
        self.file_index = -1

    def next(self):
        self.file_index = (self.file_index + 1) % NextFile.filesPerDir
        if self.file_index == 0:
            self.dir_index += 1
        dirname = self._dirname()
        if not os.path.isdir(dirname):
            os.makedirs(dirname)
        print(self._filepath())
        return self._filepath()

    def _dirname(self):
        char1 = self.dir_index % 26
        char2 = int(self.dir_index / 26) % 26
        return os.path.join(self.path_name, '%c%c' % (ord('A') + char2, ord('A') + char1))

    def _filepath(self):
        return '%s/wiki_%02d.jsonl' % (self._dirname(), self.file_index)


class OutputSplitter():

    """
    File-like object, that splits output to multiple files of a given max size.
    """

    def __init__(self, nextFile, max_file_size=0, compress=True):
        """
        :param nextFile: a NextFile object from which to obtain filenames
            to use.
        :param max_file_size: the maximum size of each file.
        :para compress: whether to write data with bzip compression.
        """
        self.nextFile = nextFile
        self.compress = compress
        self.max_file_size = max_file_size
        self.file = self.open(self.nextFile.next())
        self.counter = 0
    def reserve(self):
        if self.counter + 1 > self.max_file_size:
            self.close()
            self.file = self.open(self.nextFile.next())
            self.counter = 0
    def run(self, data):
        self.reserve()
        self.counter += 1
        self.file.write(json.dumps(data.toJSON()) + "\n")
    def close(self):
        self.file.close()

    def open(self, filename):
        if self.compress:
            return bz2.BZ2File(filename + '.bz2', 'w')
        else:
            f = open(filename, 'w')
            return f


class JsonWriter(BasePipeline):

    def __init__(self, outputfolder, basefilename=None, filesize=10000, startfile=0):
        """
        when attached to the pipeline this file log all json
        :param outputfolder: folder to save output files in
        :param basefilename: filename prefix to add before all file names
        :param filesize:
        """

        self.outputfolder = outputfolder

        if not os.path.exists(outputfolder):
            os.makedirs(outputfolder)

        self.basefilename = basefilename
        self.filesize = filesize
        self.counter = 0 + startfile
        self.buffer = []

    def run(self, document):

        self.counter += 1
        self.buffer.append(document.toJSON())

        if self.counter % self.filesize == 0:
            self.flush()

        return document

    def flush(self):
        
        filename = "%s-%s.json" % (self.counter-self.filesize, self.counter)
        filename = "%s_%s" % (self.basefilename, filename) if self.basefilename is not None else filename
        filename = os.path.join(self.outputfolder, filename)

        with open(filename, 'w') as outfile:
            json.dump(self.buffer, outfile)
            print("Saved file %s" % filename)
            del self.buffer
            self.buffer = []

class JsonlWriter(BasePipeline):

    def __init__(self, outputfolder, basefilename=None, filesize=10000, startfile=0):
        """
        when attached to the pipeline this file log all jsonl
        :param outputfolder: folder to save output files in
        :param basefilename: filename prefix to add before all file names
        :param filesize:
        """

        self.outputfolder = outputfolder

        if not os.path.exists(outputfolder):
            os.makedirs(outputfolder)

        self.basefilename = basefilename
        self.filesize = filesize
        self.counter = 0 + startfile
        self.buffer = []
        self.interval_start = default_timer()

    def run(self, document):
        self.counter += 1
        self.buffer.append(document.toJSON())

        if self.counter % self.filesize == 0:
            self.flush()
            print(default_timer(), self.interval_start)
            interval_rate = self.filesize / (default_timer() - self.interval_start)
            print(f"Extracted {self.counter} articles ({interval_rate} art/s)")
            self.interval_start = default_timer()
        return document

    def flush(self):
        
        filename = "%s-%s.jsonl" % (self.counter-self.filesize, self.counter)
        filename = "%s_%s" % (self.basefilename, filename) if self.basefilename is not None else filename
        filename = os.path.join(self.outputfolder, filename)

        with open(filename, 'w') as outfile:
            for item in self.buffer:
                outfile.write(json.dumps(item) + "\n")
            print("Saved file %s" % filename)
            del self.buffer
            self.buffer = []

class CustomeWriterTriples(JsonWriter):
    def __init__(self, outputfolder, basefilename=None, filesize=10000, startfile=0):
        #super(CostumeWriterTriples, self).__init__(outputfolder, basefilename, filesize, startfile)
        JsonWriter.__init__(self, outputfolder, basefilename, filesize, startfile)
    def run(self, document):
        self.counter += 1
        triples = self.createTriples(document)

        self.buffer.append(triples)

        if self.counter % self.filesize == 0:
            self.flush()

        return document

    def createTriples(self, document):
        triples = {}
        triples['triples'] = []
        triples['additionalTriples'] = []
        triples['summary'] = document.text

        for t in document.triples:
            # check if main enitity of document is subject or object in the triple
            if t.subject.uri == document.docid:
                str_triple = t.subject.uri + ' ' + t.predicate.uri + ' ' + t.object.uri
                triples['triples'].append(str_triple)


            elif t.object.uri == document.docid:
                str_triple = t.subject.uri + ' ' + t.predicate.uri + ' ' + t.object.uri
                triples['additionalTriples'].append(str_triple)

        triples['triples'] = list(set(triples['triples']))
        triples['additionalTriples'] = list(set(triples['additionalTriples']))

        return triples

    def flush(self):
        filename = "%s-%s-triples.pkl" % (self.counter-self.filesize, self.counter)
        filename = "%s_%s" % (self.basefilename, filename) if self.basefilename is not None else filename
        filename = os.path.join(self.outputfolder, filename)

        with open(filename, 'w') as outfile:
            pickle.dump(self.buffer, outfile)
            print("Saved file %s" % filename)
            del self.buffer
            self.buffer = []

class CustomeWriterEntities(JsonWriter):
    def __init__(self, outputfolder, basefilename=None, filesize=10000, startfile=0):
        JsonWriter.__init__(self, outputfolder, basefilename, filesize, startfile)

    def run(self, document):
        self.counter += 1
        entities = self.createEntities(document)

        self.buffer.append(entities)

        if self.counter % self.filesize == 0:
            self.flush()

        return document

    def createEntities(self, document):
        entities = []

        for e in document.entities:
            entity = {}
            entity['URI'] = e.uri
            entity['offset'] = e.boundaries[0]
            entity['surfaceForm'] = e.surfaceform
            # entity['propertyplaceholder'] = e.property_placeholder
            # entity['typeplaceholder'] = e.type_placeholder
            entity['annotator'] = e.annotator
            entities.append(entity)

        entities = sorted(entities, key=lambda x: x['offset'])
        return entities

    def flush(self):
        filename = "%s-%s-entities.pkl" % (self.counter-self.filesize, self.counter)
        filename = "%s_%s" % (self.basefilename, filename) if self.basefilename is not None else filename
        filename = os.path.join(self.outputfolder, filename)

        with open(filename, 'w') as outfile:
            pickle.dump(self.buffer, outfile)
            print("Saved file %s" % filename)
            del self.buffer
            self.buffer = []








