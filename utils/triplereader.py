from collections import defaultdict
import sys
from SPARQLWrapper import SPARQLWrapper, JSON
from csv import reader
import pandas as pd
import sqlite3

class TripleReader:

    def __init__(self, triples_file):

        self.baseuripred = "http://www.wikidata.org/prop/direct/"
        self.baseuriobj = "http://www.wikidata.org/entity/"

        self.d = defaultdict(list)
        with open(triples_file) as f:
            for l in f:
                tmp = l.split("\t")
                if len(tmp) == 3:
                    self.d["%s%s" %
                           (tmp[0].strip().replace(self.baseuriobj, ""),
                            tmp[2].strip().replace(self.baseuriobj, ""))].append(tmp[1].strip().replace(self.baseuripred, ""))

    def get(self, suri, objuri):
        p = self.d["%s%s" % (suri.strip().replace(self.baseuriobj, ""), objuri.strip().replace(self.baseuriobj, ""))]
        return ["%s%s" % (self.baseuripred, i) for i in p]

class TripleCSVReader:

    def __init__(self, triples_file, language):
        self.d = defaultdict(list)
        with open(triples_file, 'r') as read_obj:
            # pass the file object to reader() to get the reader object
            csv_reader = reader(read_obj)
            # Iterate over each row in the csv using reader object
            for i, row in enumerate(csv_reader):
                row = row[0].split('\t')
                self.d[(row[0], row[2])].append(row[1])
        self.d_properties = defaultdict(list)
        endpoint_url = "https://query.wikidata.org/sparql"
        user_agent = "WDQS-example Python/%s.%s" % (sys.version_info[0], sys.version_info[1])
        sparql = SPARQLWrapper(endpoint_url, agent=user_agent)
        query = f'SELECT (STRAFTER(STR(?property), "entity/") AS ?pName) ?propertyLabel ?propertyDescription ?propertyAltLabel WHERE {{?property wikibase:propertyType ?propertyType. SERVICE wikibase:label {{ bd:serviceParam wikibase:language \"{language}\". }}}}'
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        for result in results["results"]["bindings"]:
            self.d_properties[result['pName']['value']] = result['propertyLabel']['value']
    def get(self, suri, objuri):
        p = self.d[(suri.uri, objuri.uri)]
        return p
    def get_uri(self, suri, objuri):
        p = self.d[(suri, objuri)]
        return p
    def get_label(self, p):
        p = self.d_properties[p]
        return p
    def get_exists(self, suri, rel ,objuri):
        # with sqlite3.connect(self._path_to_db) as conn:
        for entity in objuri:
            p = self.d[(suri, entity)]
            if rel in p:
                return True
        return False

class TripleDBReader:

    def __init__(self, triples_file, language):
        self.d_properties = defaultdict(list)
        endpoint_url = "https://query.wikidata.org/sparql"
        user_agent = "WDQS-example Python/%s.%s" % (sys.version_info[0], sys.version_info[1])
        sparql = SPARQLWrapper(endpoint_url, agent=user_agent)
        query = f'SELECT (STRAFTER(STR(?property), "entity/") AS ?pName) ?propertyLabel ?propertyDescription ?propertyAltLabel WHERE {{?property wikibase:propertyType ?propertyType. SERVICE wikibase:label {{ bd:serviceParam wikibase:language \"{language}\". }}}}'
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        try:
            # print(query)
            results = sparql.query().convert()
        except FileNotFoundError:
            print(query)
        for result in results["results"]["bindings"]:
            self.d_properties[result['pName']['value']] = result['propertyLabel']['value']
        self._path_to_db = triples_file

    def get(self, suri, objuri):
        # with sqlite3.connect(self._path_to_db) as conn:
        with sqlite3.connect(self._path_to_db) as conn:
            c = conn.cursor()
            c.execute("SELECT relation FROM triplets WHERE subjobj=?", (suri.uri+'\t'+objuri.uri,))
            results = c.fetchall()
        if len(results) > 0:
            return [result[0] for result in results]
        else:
            return []

    def get_label(self, p):
        p = self.d_properties[p]
        return p

    def get_exists(self, suri, rel, objuri):
        # with sqlite3.connect(self._path_to_db) as conn:
        with sqlite3.connect(self._path_to_db) as conn:
            c = conn.cursor()
            c.execute(f"SELECT relation FROM triplets WHERE subject=? and relation=? and object IN ({','.join(['?']*len(objuri))})", (suri,rel,*objuri,))
            results = c.fetchone()
        if results == None:
            return False
        else:
            return True

class TripleSPARQLReader:

    def __init__(self, triples_file):
        self.baseuripred = "http://www.wikidata.org/prop/direct/"
        self.baseuriobj = "http://www.wikidata.org/entity/"
        self.endpoint_url = "https://query.wikidata.org/sparql"
        self.user_agent = "WDQS-example Python/%s.%s" % (sys.version_info[0], sys.version_info[1])
        # TODO adjust user agent; see https://w.wiki/CX6
        self.sparql = SPARQLWrapper(self.endpoint_url, agent=self.user_agent)

    def get_results(self, query):

        self.sparql.setQuery(query)
        self.sparql.setReturnFormat(JSON)
        return self.sparql.query().convert()

    def get(self, suri, objuri):
        if suri.annotator == 'Date_Linker' or suri.annotator == 'Value_Linker':
            subj = suri.uri
        else:
            subj = 'wd:' + suri.uri.strip().replace(self.baseuriobj, "")       

        if objuri.annotator == 'Date_Linker' or suri.annotator == 'Value_Linker':
            obj = objuri.uri
        else:
            obj = 'wd:' + objuri.uri.strip().replace(self.baseuriobj, "")   
        query = """SELECT ?item ?propLabel{
        %s  ?item  %s . 
            ?prop wikibase:directClaim ?item. 
        SERVICE wikibase:label { 
            bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". 
            ?prop      rdfs:label ?propLabel .
        }
        }""" % (subj, obj)
        # print(query)
        results = self.get_results(query)

        # for result in results["results"]["bindings"]:
        #     print(result)
        return [i["item"]["value"] for i in results["results"]["bindings"]]