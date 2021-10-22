from pipeline.pipeline import *
from utils import triplereader
import itertools

BASEURIOBJ = "http://www.wikidata.org/entity/"
class NoSubjectAlign(BasePipeline):
    """
    Following the assumption in NoSUB  [1] and [2] that sentences in one paragraph all share the same subject.
    [1] Augenstein, Isabelle, Diana Maynard, and Fabio Ciravegna. "Distantly supervised web relation extraction for knowledge base population." Semantic Web 7.4 (2016): 335-349.
    [2] WikiReading: A Novel Large-scale Language Understanding Task over Wikipedia Hewlett et al. 2016
    """
    def __init__(self, triples_reference):
        self.annotator_name = "NoSubject-Triple-aligner"

        # pd.read_csv(triples_file, sep="\t", names=["subject", "predicate", "object"]).set_index(['subject', 'object'])

        self.wikidata_triples = triples_reference


    def run(self, document):
        """
        :param: input document to align its sentences with triples
        :return:
        """
        for sid, (start, end) in enumerate(document.sentences_boundaries):

            # Getting sentence subject
            # Every sentence has main entity as subject

            # if subject already tagged use it if not use only the URI
            # entities in sentence
            es = [j for j in document.entities if j.boundaries[0] >= start and j.boundaries[1] <= end]
            e_sub = [j for j in es if j.uri == document.uri]
            if len(e_sub) > 0:
                subject = e_sub[0]
            else:
                subject = Entity(document.uri,
                                 boundaries=None,
                                 surfaceform=document.title,
                                 annotator=self.annotator_name)

            for o in es:
                if subject.uri == o.uri:
                    continue
                # if o.annotator == 'Date_Linker':
                #     subj = 'wd:' + subject.uri.strip().replace(BASEURIOBJ, "")
                #     obj = o.uri
                # elif subject.annotator == 'Date_Linker':
                #     subj = subject.uri
                #     obj = 'wd:' + o.uri.strip().replace(BASEURIOBJ, "")       
                # else:
                #     obj = 'wd:' + o.uri.strip().replace(BASEURIOBJ, "")       
                #     subj = 'wd:' + subject.uri.strip().replace(BASEURIOBJ, "")

                #predicates = self.wikidata_triples["%s\t%s" % (subject.uri, o.uri)]
                predicates = self.wikidata_triples.get(subject, o)

                for pred in predicates:
                    pred = Entity(pred,
                                  boundaries=None,
                                  surfaceform=self.wikidata_triples.get_label(pred),
                                  annotator=self.annotator_name)

                    triple = Triple(subject=subject,
                                    predicate=pred,
                                    object=o,
                                    sentence_id=sid,
                                    annotator=self.annotator_name
                                    )

                    document.triples.append(triple)

        return document


class SimpleAligner(BasePipeline):
    """
    Take a document with tagged entities and match them with one another.
    Example : If we have three entities Q1, Q2 and Q3, it will try to find a
    property binding Q1 with Q2, Q2 with Q1, Q2 with Q3 etc...
    It won't match Q1 with itself, but if Q1 == Q2, it will try to find a
    property between them
    """
    def __init__(self, triples_reference):
        """
        :param: input document containing the triples (two entities and
        the property that bind them together)
        """
        self.annotator_name = "Simple-Aligner"

        self.wikidata_triples = triples_reference

    def run(self, document):
        """
        :param: input document to align its sentences with triples
        :return:
        """
        for sid, (start, end) in enumerate(document.sentences_boundaries):
            es = [j for j in document.entities if j.boundaries[0] >= start and j.boundaries[1] <= end]
            # We use permutations to match every entity with all the others
            for o in itertools.permutations(es, 2):
                if o[0].uri == o[1].uri:
                    continue

                predicates = self.wikidata_triples.get(o[0], o[1])

                # And create the triples
                for pred in predicates:
                    pred = Entity(pred,
                                  boundaries=None,
                                  surfaceform=self.wikidata_triples.get_label(pred),
                                  annotator=self.annotator_name)

                    triple = Triple(subject=o[0],
                                    predicate=pred,
                                    object=o[1],
                                    sentence_id=sid,
                                    annotator=self.annotator_name
                                    )

                    document.triples.append(triple)

        return document


class SPOAligner(BasePipeline):

    def __init__(self, triples_reference):
        self.annotator_name = "SPOAligner"
        # Add here the name of the annotators creating entities with something else than properties
        self.annotator_list = ["Wikidata_Spotlight_Entity_Linker", "Me","Simple_Coreference", "Date_Linker"]

        self.wikidata_triples = triples_reference

    def run(self, document):
        for sid, (start, end) in enumerate(document.sentences_boundaries):

            # Entities created by the Entity linkers and the Coreference
            es = [j for j in document.entities if j.boundaries[0] >= start
                                                and j.boundaries[1] <= end
                                                and j.annotator in self.annotator_list]

            # Entities created by the Property Linker
            p = [j for j in document.entities if j.boundaries[0] >= start
                                                and j.boundaries[1] <= end
                                                and j.annotator == 'Wikidata_Property_Linker']

            for o in itertools.permutations(es, 2):
                if o[0].uri == o[1].uri:
                    continue

                predicates = self.wikidata_triples.get(o[0], o[1])
                #predicates = self.wikidata_triples["%s\t%s" % (o[0].uri, o[1].uri)]

                # And create the triples
                for kbpred in predicates:
                    for spred in p:
                        if kbpred == spred.uri:
                            triple = Triple(subject=o[0],
                                            predicate=spred,
                                            object=o[1],
                                            sentence_id=sid,
                                            annotator=self.annotator_name
                                            )

                            document.triples.append(triple)

        return document

class NoAligner(BasePipeline):
    """
    Take a document with tagged entities and add the triples that are not 
    in the document, without alignment in the text.
    """
    def __init__(self, all_triples):
        """
        :param: input document containing the triples (two entities and
        the property that bind them together)
        """
        self.annotator_name = "No-Aligner"

        self.wikidata_triples = all_triples


    def makeTriple(self, s, p, o):
        subj = Entity(s,
            boundaries=None,
            surfaceform=None,
            annotator=self.annotator_name)

        pred = Entity(p,
            boundaries=None,
            surfaceform=None,
            annotator=self.annotator_name)

        obj = Entity(o,
            boundaries=None,
            surfaceform=None,
            annotator=self.annotator_name)

        triple = Triple(subject=subj,
            predicate=pred,
            object=obj,
            sentence_id=None,
            annotator=self.annotator_name)
        return triple

    def run(self, document):

        tall = []
        tagged_entities = set([e.uri for e in document.entities])

        for t in self.wikidata_triples.get(document.docid):
            # noagliner aligns triples only:
                # document.uri is the subject of the triples
                # document.uri is the object of the triple and its subject is linked by entity linker

            if t[0] not in tagged_entities and t[2] == document.uri:
                continue

            tall.append(t[0]+"\t"+t[1]+"\t"+t[2])

        tall = set(tall)

        tdoc = set([t.subject.uri+"\t"+t.predicate.uri+"\t"+t.object.uri for t in document.triples])



        tadd = tall - tdoc
        for t in tadd:
            triple = self.makeTriple(*t.split("\t"))
            document.triples.append(triple)

        return document

class NoAlignerLimitedProperties(BasePipeline):
    """
    Take a document with tagged entities and add the triples that are not 
    in the document, without alignment in the text.
    Limit the missing entities to the entities with properties
    that appear in the first sentence.
    """
    def __init__(self, all_triples):
        """
        :param: input document containing the triples (two entities and
        the property that bind them together)
        """
        self.annotator_name = "No-Aligner"

        self.wikidata_triples = all_triples

    def makeTriple(self, s, p, o):
        subj = Entity(s,
            boundaries=None,
            surfaceform=None,
            annotator=self.annotator_name)

        pred = Entity(p,
            boundaries=None,
            surfaceform=None,
            annotator=self.annotator_name)

        obj = Entity(o,
            boundaries=None,
            surfaceform=None,
            annotator=self.annotator_name)

        triple = Triple(subject=subj,
            predicate=pred,
            object=obj,
            sentence_id=None,
            annotator=self.annotator_name)
        return triple

    #get all properties that are used in the first sentence
    def getAllowedProperties(self, triples):
        allowed_properties = []
        for t in triples:
            if t.sentence_id == 0:
                allowed_properties.append(t.predicate.uri)
        return allowed_properties

    def run(self, document):
        allowed_properties = self.getAllowedProperties(document.triples)

        for t in self.wikidata_triples.get(document.docid):
            if not allowed_properties or not t[1] in allowed_properties:
                continue
            # TODO: Better comparison
            exists = False
            for doc_t in document.triples:
                if doc_t.subject.uri == t[0] and doc_t.predicate.uri == t[1] and doc_t.object.uri == t[2]:
                    exists = True
            if not exists:
                triple = self.makeTriple(t[0], t[1], t[2])
                document.triples.append(triple)

        return document
