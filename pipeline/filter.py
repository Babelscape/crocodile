from pipeline import *

class SentenceLimiter:
    """
    Limit the text, word boundaries and 
    sentence boundaries of a given document
    to the number of sentences given
    """
    def run(self, document, number_sentences):
        """
        :param: number_sentences, starts with 0 for the fist sentence
        """
        boundaries = (document.sentences_boundaries[0][0], document.sentences_boundaries[:number_sentences+1][-1][1])
        document.text = document.text[boundaries[0]:boundaries[1]]
        document.sentences_boundaries = self._limitSenteceBoundaries(document.sentences_boundaries, boundaries[1])
        document.words_boundaries = self._limitWordBoundaries(document.words_boundaries, boundaries[1])
        document.entities = self._limitEntities(document.entities, boundaries[1])
        document.triples = self._limitTriples(document.triples, boundaries[1])
        return document

    def _limitSenteceBoundaries(self, sentences_boundaries, maxi):
        sentences_boundaries_new = []
        for sent in sentences_boundaries:
            if sent[1] <= maxi:
                sentences_boundaries_new.append(sent) 
        return sentences_boundaries_new

    def _limitEntities(self, entities, maxi):
        entities_new = []
        for e in entities:
            if e.boundaries[1] <= maxi:
                entities_new.append(e)
        return entities_new

    def _limitTriples(self, triples, maxi):
        triples_new = []
        for t in triples:
            if t.sentence_id == 0:
                triples_new.append(t)
        return triples_new

    def _limitWordBoundaries(self, words_boundaries, maxi):
        words_boundaries_new = []
        for word in words_boundaries:
            if word[1] <= maxi:
                words_boundaries_new.append(word) 
        return words_boundaries_new


class MainEntityLimiter:
    """
    Remove a document's content if the main entity is not aligned
    """
    def run(self, document):
        if not document.uri in [i.uri for i in document.entities]:
            document = None
        return document


class EntityTypeFilter:
    """
    Remove all documents that are of a certain type
    """
    def __init__(self, all_triples, entities):
        """
        :param: input TripleReaderTriples object
        :param: a list of entity that should be filtered
        """
        self.wikidata_triples = all_triples
        self.entities = entities

    def run(self, document):
        # P31: instance of
        prop_id = 'http://www.wikidata.org/prop/direct/P31'
        if any([i for i in self.wikidata_triples.get(document.docid) if i[1] == prop_id and i[2] in self.entities]):
            document = None
        return document

class MinEntityLimiter:
    """
    Remove a document's content if the there aren't enough entities.
    """
    def __init__(self, min_entities):
        """
        :param: Minumum number of entities
        """
        self.min_entities = min_entities
    def run(self, document):
        if len(document.entities) < self.min_entities:
            document = None
        return document

class MinTriplesLimiter:
    """
    Remove a document's content if the there aren't enough entities.
    """
    def __init__(self, min_triples):
        """
        :param: Minumum number of triples
        """
        self.min_triples = min_triples
    def run(self, document):
        if len(document.triples) < self.min_triples:
            document = None
        return document

class TriplesLimiter:
    """
    Remove a document's content if the there aren't enough or too many triplets.
    """
    def __init__(self, min_triples, max_triples):
        """
        :param: Minumum number of triples
        """
        self.min_triples = min_triples
        self.max_triples = max_triples
    def run(self, document):
        if len(document.triples) < self.min_triples or len(document.triples) > self.max_triples:
            document = None
        return document

class EntityLimiter:
    """
    Remove a document's content if the there aren't enough or too many entities.
    """
    def __init__(self, min_entities, max_entities):
        """
        :param: Minumum number of triples
        """
        self.min_entities = min_entities
        self.max_entities = max_entities
    def run(self, document):
        if len(document.entities) < self.min_entities or len(document.entities) > self.max_entities:
            document = None
        return document