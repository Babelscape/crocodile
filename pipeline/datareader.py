from pipeline.pipeline import *
import csv
import glob
import os
import json
import pandas as pd
# import xml.etree.ElementTree as ET
from lxml import etree as ET

class WikiDataAbstractsDataReader:
    """
    class with a default read_documents functions that yields Document iterator
    """
    def __init__(self, dataset_file, skip=0):
        """

        :param dataset_file: path of the dataset file
        :param db_wd_mapping: if given the page-uri will be changed from the one in the dataset
        :param skip: skip the first n documents
        to be mapped using the mappings file given.
        """

        self.dataset_file = dataset_file
        self.skip = skip

    def read_documents(self):
        """
        function that yields iterator of documents
        the URI of each document is the Knowledge base URI after being mapped
        """
        for i,j,y in os.walk(self.dataset_file):
            for file_name in y:
                iter_doc = ET.iterparse(i + '/' + file_name, events=('end',), tag='doc')
                for _, elem in iter_doc:
                    document = Document(
                        docid=elem.get('id'),
                        pageuri=elem.get('wikidata'),
                        title=elem.get('title'),
                        text=elem.findtext('text')#.decode('utf-8')
                    )
                    for elem_child in elem.iter():
                        if elem_child.tag == 'link':
                            entity = Entity(elem_child.get('wikidata'),
                                            boundaries=(int(elem_child.get('start')), int(elem_child.get('end'))),
                                            surfaceform=elem_child.get('label'),
                                            annotator='Me')
                            document.entities.append(entity)
                            del entity
                            elem_child.clear()
                    elem.clear()
                    yield document
                    del document
                    # while elem.getprevious() is not None:
                    #     del elem.getparent()[0]
                elem.clear()
                del(iter_doc)