"""
Our annotation pipeline consits of a series of BasePipeline objects.   (EntityLinker, Coreference, triple aligner .. etc)
each BasePipelie class takes a document class and add it's annotation
Each Document with it's annotation when converted into json has the following fields.

  {
        "doc"id:                       Document id     -- Wikipedia document id when dealing with wikipedia dump
        "title":                    title of the wikipedia document
        "uri":                      URI of the item containing the main page
        "text":                     The whole text of the document
        "sentences_boundaries":                start and end offsets of sentences
                                    [(start,end),(start,end)] start/ end are character indices
        "words_boundaries":                                      # list of tuples (start, end) of each word in Wikipedia Article, start/ end are character indices
        "entities":                                             # list of Entities   (Class Entity)
                                    [
                                    {
                                    "uri":
                                    "boundaries": (start,end)   # tuple containing the of the surface form of the entity
                                    "surface-form": ""
                                    "annotator" : ""            # the annotator name used to detect this entity [NER,DBpediaspotlight,coref]
                                    }
                                    ]
        "triples":                  list of triples that occur in the document
                                    We opt of having them exclusive of other fields so they can be self contained and easy to process
                                    [
                                    {
                                    "subject":          class Entity
                                    "predicate":        class Entity
                                    "object":           class Entity
                                    "dependency_path": "lexicalized dependency path between sub and obj if exists" or None (if not existing)
                                    "confidence":      # confidence of annotation if possible
                                    "annotator":       # annotator used to annotate this triple with the sentence
                                    "sentence_id":     # integer shows which sentence does this triple lie in
                                    }
                                    ]
    }
"""

from nltk.tokenize import WordPunctTokenizer
from nltk.tokenize.punkt import PunktSentenceTokenizer, PunktParameters
punkt_param = PunktParameters()
punkt_param.abbrev_types = set(['st', 'dr', 'prof', 'mgr', 'sgt', 'mr', 'mrs', 'inc', 'no', 'etc'])

class Document:

    def __init__(self, docid, title, pageuri, text, sentence_boundaries=None, words_boundaries=None, entities=None, triples=None):
        """

        initalization of document class
        :param id: wikipedia id of each page  # document id if another text dataset is used.
        :param title: title of the page
        :param pageuri: URI of the item containing the main page
        :param text:  "text that is contained in the page"
        :param sentence_boundaries: start and end offsets of sentences
        :param word_boundaries: list of tuples (start, end) of each word in Wikipedia Article, start/ end are character indices
        :param entities: list of Entities in the document
        :param triples:  list of Triples aligned with sentences in the document
        """

        self.docid = docid
        self.title = title
        self.uri = pageuri
        self.text = text
        self.sentences_boundaries = self.__get_setences_boundaries() if sentence_boundaries is None else sentence_boundaries
        self.words_boundaries = self.__get_words_boundaries() if words_boundaries is None else words_boundaries
        self.entities = [] if entities is None else entities
        self.triples = [] if triples is None else triples

    @classmethod
    def fromJSON(cls, j):
        """
        instantiate a document class from existing json file
        :param j: j is a json file containing all fields as described in the begining of the document
        """

        docid = j['docid']
        title = j['title']
        uri = j['uri']
        text = j['text']
        sentences_boundaries = j['sentences_boundaries'] if 'sentences_boundaries' in j else None
        word_boundaries =j['words_boundaries'] if 'words_boundaries' in j else None
        entities = [Entity.fromJSON(ej) for ej in j['entities']] if 'entities' in j else None
        triples = [Triple.fromJSON(tj) for tj in j['triples']] if 'triples' in j else None

        return Document(docid, title, uri, text, sentences_boundaries, word_boundaries, entities, triples)


    def __get_setences_boundaries(self):
        """
        function to tokenize sentences and return
        sentence boundaries of each sentence using a tokenizer.
        :return:
        """

        tokenizer = PunktSentenceTokenizer(punkt_param)
        sentences = list(tokenizer.span_tokenize(self.text))
        return sentences

    def __get_words_boundaries(self):
        """
        function to tokenize words in the document and return words
        boundaries of each sentence using a tokenizer.
        :return:
        """
        tokenizer = WordPunctTokenizer()
        words = list(tokenizer.span_tokenize(self.text))
        return words

    def toJSON(self):
        """
        function to print the annotated document into one json file
        :return:
        """
        j = self.__dict__.copy()
        j['entities'] = [i.toJSON() for i in j['entities']] if 'entities' in j and j['entities'] is not None else []
        j['triples'] = [i.toJSON() for i in j['triples']] if 'triples' in j and j['triples'] is not None else []
        del j['sentences_boundaries']
        del j['words_boundaries']
        return j

    def get_sentences(self):
        """
        :return: get sentences text
        """
        return [self.text[s:e] for s, e in self.sentences_boundaries]


class Entity:
    def __init__(self, uri, boundaries, surfaceform, annotator=None, type_placeholder=None, property_placeholder=None):
        """
        :param uri: entity uri
        :param boundaries: start and end boundaries of the surface form in the sentence
        :param surfaceform: text containing the surface form of the entity
        :param annotator:   annotator used in entity linking
        """
        self.uri = uri
        self.boundaries = boundaries
        self.surfaceform = surfaceform
        self.annotator = annotator
        # self.type_placeholder = type_placeholder
        # self.property_placeholder = property_placeholder

    @classmethod
    def fromJSON(cls, j):
        """
        initialize an entity class using a json object
        :param j: json object of an entity
        :return: Entity instantiated object
        """
        annotator = j['annotator'] if 'annotator' in j else None
        type_placeholder = j['type_placeholder'] if 'type_placeholder' in j else None
        property_placeholder = j['property_placeholder'] if 'property_placeholder' in j else None
        return Entity(j['uri'], j['boundaries'], j['surfaceform'], annotator, type_placeholder, property_placeholder)

    def toJSON(self):

        return self.__dict__.copy()


class Triple:
    def __init__(self, subject, predicate, object, sentence_id, dependency_path=None, confidence=None, annotator=None):
        """
        :param subject: entity class containing the triple subject
        :param predicate: entity class containing the triple predicate
        :param object:    entity class containing the triple object
        :param sentence_id:  integer showing which sentence in the document this (0,1,2,3 .. first , second , third ..etc)
        :param dependency_path: "lexicalized dependency path between sub and obj if exists" or None (if not existing)
        :param confidence: confidence of annotation if possible
        :param annotator:
        """

        self.subject = subject
        self.predicate = predicate
        self.object = object
        self.sentence_id = sentence_id
        self.dependency_path = dependency_path
        self.confidence = confidence
        self.annotator = annotator

    @classmethod
    def fromJSON(cls, j):
        """
        initialize a triple class using a json object
        :param j: json object of an entity
        :return: Triple instantiated object
        """
        subject = Entity.fromJSON(j['subject'])
        predicate = Entity.fromJSON(j['predicate'])
        object = Entity.fromJSON(j['object'])
        sentence_id = j['sentence_id']
        dependency_path = j['dependency_path'] if 'dependency_path' in j else None
        confidence = j['confidence'] if 'confidence' in j else None
        annotator = j['annotator'] if 'annotator' in j else None

        return Triple(subject, predicate, object, sentence_id, dependency_path, confidence, annotator)

    def toJSON(self):
        j = self.__dict__.copy()
        j['subject'] = j['subject'].toJSON()
        j['predicate'] = j['predicate'].toJSON()
        j['object'] = j['object'].toJSON()

        return j


class BasePipeline:

    def run(self, document):
        """
        a basic run function for all pipeline components.
        * To Override in every Pipeline components
        :param j: json file containing all annotations per document as defined in the README file
        :return: the same Json after adding annotations.
        """

        return document





