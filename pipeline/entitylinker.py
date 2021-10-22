# -*- coding: utf-8 -*-

from pipeline.pipeline import *
# import spotlight
import csv
import json
import os
# from sutime import SUTime
# from date_extractor import extract_dates
import spacy, re, dateparser
from spacy.matcher import Matcher
from spacy.tokenizer import Tokenizer
from spacy import displacy
import datetime
spacy.prefer_gpu()

english_months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August','September', 'October', 'November', 'December', 
                  'january', 'february', 'march', 'april', 'may', 'june', 'july', 'august' 'september', 'october', 'november', 'december',
                  'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec',                   
                  'jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']

deutsche_months = ['Januar', 'Februar', 'März', 'April', 'Mai', 'Juni', 'Juli', 'August','September', 'Oktober', 'November', 'Dezember', 
                  'januar', 'februar', 'märz', 'april', 'mai', 'juni', 'juli', 'august' 'september', 'oktober', 'november', 'dezember',
                  'Jan', 'Feb', 'Mär', 'Apr', 'Mai', 'Jun', 'Jul', 'Aug', 'Sep', 'Okt', 'Nov', 'Dez',                   
                  'jan', 'feb', 'mär', 'apr', 'mai', 'jun', 'jul', 'aug', 'sep', 'okt', 'nov', 'dez']
russian_months = ['января','февраля','марта','апреля','мая','июня','июля','августа','сентября','октября','ноября','декабря','декабря']
hindi_months = ['जनवरी','फ़रवरी','मार्च','अप्रैल','मई','जून','जुलाई','अगस्त','सितम्बर','अक्टूबर','नवंबर','दिसंबर']
arabic_months = ['يناير','فبراير','مارس','إبريل','أبريل','مايو','يونية','يونيو','يولية','يوليو','أغسطس','سبتمبر','أكتوبر','نوفمبر','ديسمبر']
greek_months = ['Ιανουάριος','Φεβρουάριος','Μάρτιος','Απρίλιος','Μάιος','Ιούνιος','Ιούλιος','Αύγουστος','Σεπτέμβριος','Οκτώβριος','Νοέμβριος','Δεκέμβριος',
                'Ιανουαρίου','Φεβρουαρίου','Μαρτίου','Απριλίου','Μαΐου','Ιουνίου','Ιουλίου','Αυγούστου','Σεπτεμβρίου','Οκτωβρίου','Νοεμβρίου','Δεκεμβρίου']
swedish_months = ['januari','februari','mars','april','maj','juni','juli','augusti','september','oktober','november','december']
dutch_months = ['januari', 'jan.','februari', 'febr.','maart', 'mrt.','april', 'apr','mei','juni','juli','augustus', 'aug.','september', 'sep.','oktober', 'okt.','november','nov.','december','dec.']
polish_months = ['styczeń', 'stycznia','luty', 'lutego','marzec', 'marca','kwiecień', 'kwietnia','maj','maja','czerwiec','czerwca', 'lipiec','lipca', 'sierpień','sierpnia', 'wrzesień','września','październik','października','listopad', 'listopada', 'grudzień', 'grudnia']
spanish_months = ['enero', 'febrero','marzo', 'abril','mayo', 'junio','julio', 'agosto','septiembre','octubre','noviembre','diciembre']
catalan_months = ['gener', 'febrer','març', 'abril','maig', 'juny','juliol', 'agost','setembre','octubre','novembre','desembre']
portuguese_months = ['janeiro', 'fevereiro','março', 'abril','maio', 'junho','julho', 'agosto','setembro','outubro','novembro','dezembro']
italian_months = ['gennaio', 'febbraio','marzo', 'aprile','maggio', 'giugno','luglio', 'agosto','settembre','ottobre','novembre','dicembre']
french_months = ['janvier', 'février','mars', 'avril','mai', 'juin','juillet', 'août','septembre','octobre','novembre','décembre']

class WikidataSpotlightEntityLinker(BasePipeline):

    def __init__(self, db_wd_mapping, spotlight_url='http://localhost:2222/rest/annotate', confidence=0.2, support=1):
        """
        :param db_wd_mapping: csv file name containing mappings between DBpedia URIS and Wikdiata URIS
        :param spotlight_url: url of the dbpedia spotlight service
        :param confidence: min confidence
        :param support:  min supporting document
        """
        self.annotator_name = 'Wikidata_Spotlight_Entity_Linker'
        self.spotlight_url = spotlight_url
        self.confidence = confidence
        self.support = support

        self.mappings = {}
        with open(db_wd_mapping) as f:
            for l in f.readlines():
                tmp = l.split("\t")
                self.mappings[tmp[0].strip()] = tmp[1].strip()

    def run(self, document):
        """
        :param document: Document object
        :return: Document after being annotated
        """

        #document.entities = []

        for sid, (start, end) in enumerate(document.sentences_boundaries):

            try:
                annotations = spotlight.annotate(self.spotlight_url,
                                                 document.text[start:end],
                                                 self.confidence,
                                                 self.support)

            except Exception as e:
                annotations = []

            for ann in annotations:

                e_start = document.sentences_boundaries[sid][0] + ann['offset']

                if type(ann['surfaceForm']) not in [str, unicode]:
                    ann['surfaceForm'] = str(ann['surfaceForm'])

                e_end = e_start + len(ann['surfaceForm'])

                # change DBpedia URI to Wikidata URI
                if ann['URI'] in self.mappings:
                    ann['URI'] = self.mappings[ann['URI']]
                else:
                    continue

                entity = Entity(ann['URI'],
                                boundaries=(e_start, e_end),
                                surfaceform=ann['surfaceForm'],
                                annotator=self.annotator_name)

                document.entities.append(entity)

        return document

class DateLinker(BasePipeline):

    def __init__(self, resource_folder=None):
        self.annotator_name = 'Date_Linker'
        if resource_folder is None:
            self.resource_folder = os.path.join(os.path.dirname(__file__), '../resources/sutime/')
        self.sutime = SUTime(jars=self.resource_folder)

    def run(self, document):

        dates = self.sutime.parse(document.text)

        pattern = re.compile(r"^-*\d*-*\d*-*\d*-*$")

        for date in dates:
            try:
                if date["type"] == "DATE" and pattern.match(date["value"]):
                    val = date["value"]
                    if val[0] == '-':
                        if len(val[1:]) == 4:
                            stdform = '"' + val + '-00-00T00:00:00Z"^^xsd:dateTime'
                        elif len(val[1:]) == 7:
                            stdform = '"' + val + '-00T00:00:00Z"^^xsd:dateTime'
                        elif len(val[1:]) == 10:
                            stdform = '"' + val + 'T00:00:00Z"^^xsd:dateTime'
                        else:
                            stdform = '"' + val + '"^^http://www.w3.org/2001/XMLSchema#dateTime'

                    else:
                        if len(val) == 4:
                            stdform = '"' + val + '-00-00T00:00:00Z"^^xsd:dateTime'
                        elif len(val) == 7:
                            stdform = '"' + val + '-00T00:00:00Z"^^xsd:dateTime'
                        elif len(val) == 10:
                            stdform = '"' + val + 'T00:00:00Z"^^xsd:dateTime'
                        else:
                            stdform = '"' + val + '"^^http://www.w3.org/2001/XMLSchema#dateTime'

                    start = date["start"]
                    end = date["end"]

                    entity = Entity(uri=stdform,
                                    boundaries=(start, end),
                                    surfaceform=document.text[start:end],
                                    annotator=self.annotator_name)

                    document.entities.append(entity)
            except Exception as e:
                    continue
        return document


class DateLinkerSpacy(BasePipeline):

    def __init__(self, model = "es_core_news_sm"):
        self.nlp = spacy.load(model)
        self.annotator_name = 'Date_Linker'

        infix_re = re.compile(r'''[.\,\-\/]''')
        prefix_re = re.compile(r'''[\(]''')
        suffix_re = re.compile(r'''[\)]''')
        if model != 'zh_core_web_sm':
            self.nlp.tokenizer = Tokenizer(self.nlp.vocab, infix_finditer = infix_re.finditer, suffix_search = suffix_re.search, prefix_search = prefix_re.search)
        DATE = self.nlp.vocab.strings['DATE']

        # for the token pattern 1st, 22nd, 15th etc
        # IS_REGEX_MATCH = add_regex_flag(self.nlp.vocab, '\d{1,2}(?:[stndrh]){2}?')

        pattern_1 = [{'IS_DIGIT': True}, {'ORTH': '/'}, {'IS_DIGIT': True}, {'ORTH': '/'}, {'IS_DIGIT': True}]
        # MM-DD-YYYY and YYYY-MM-DD
        pattern_2 = [{'IS_DIGIT': True}, {'ORTH': '-'}, {'IS_DIGIT': True}, {'ORTH': '-'}, {'IS_DIGIT': True}]
        # dates of the form 10-Aug-2018
        pattern_3 = [{'IS_DIGIT': True}, {'ORTH': '-'}, {'is_alpha': True}, {'ORTH': '-'}, {'IS_DIGIT': True}]
        # dates of the form Aug-10-2018
        pattern_4 = [{'is_alpha': True}, {'ORTH': '-'}, {'IS_DIGIT': True}, {'ORTH': '-'}, {'IS_DIGIT': True}]
        # dates of the form 10th August, 2018
        pattern_5 = [{"TEXT": {"REGEX": "\d{1,2}(?:[stndrh]){2}?"}}, {'is_alpha': True}, {'ORTH': ',', 'OP': '?'}, {'IS_DIGIT': True}]
        # dates of the form August 10th, 2018
        pattern_6 = [{'is_alpha': True}, {"TEXT": {"REGEX": "\d{1,2}(?:[stndrh]){2}?"}}, {'ORTH': ',', 'OP': '?'}, {'IS_DIGIT': True}]
        pattern_7 = [{'IS_DIGIT': True}, {'TEXT': 'de', 'OP': '?'}, {'is_alpha': True}, {'TEXT': {"REGEX": "del?"}, 'OP': '?'}, {'IS_DIGIT': True}]
        # pattern_8 = [{"TEXT": {"REGEX": "\-?\d+$"}},{'ORTH': '.', 'OP': '?'}, {'IS_DIGIT': True, 'OP': '?'}]
        # pattern_9 = [{"TEXT": {"REGEX": "\+?\d+$"}},{'ORTH': '.', 'OP': '?'}, {'IS_DIGIT': True, 'OP': '?'}]
        self.matcher = Matcher(self.nlp.vocab)
        self.matcher.add('Dates and Values', [pattern_1, pattern_2, pattern_3, pattern_4, 
                            pattern_5, pattern_6, pattern_7 # , pattern_8, pattern_9
                            ])
        self.regex = r"\d{2,4}年(\d{1,2}月)?(\d{1,2}日?)|[\-\+]?\d+([.,]\d{3})*([\,\.]\d+)?"

    def run(self, document):
        doc = self.nlp(document.text, disable=['tok2vec', 'morphologizer', 'parser', 'ner', 'attribute_ruler', 'lemmatizer'])
        matches = self.matcher(doc)
        spans = [doc[start:end] for _, start, end in matches]

        matches = re.finditer(self.regex, document.text, re.MULTILINE)
        for matchNum, match in enumerate(matches, start=1):
            if doc.char_span(match.start(), match.end()):
                spans.append(doc.char_span(match.start(), match.end()))

        for span in spacy.util.filter_spans(spans):
            try:
                date = dateparser.parse(str(span), languages=['es','en','ca','zh','it'], settings={'PREFER_DAY_OF_MONTH': 'first', 'RELATIVE_BASE': datetime.datetime(2020, 1, 1)})
                if date:
                    annotator = self.annotator_name
                    stdform = date.strftime("%Y-%m-%dT00:00:00Z^^http://www.w3.org/2001/XMLSchema#dateTime")
                    entity = Entity(uri=stdform,
                        boundaries=(span.start_char, span.end_char),
                        surfaceform=str(span),
                        annotator=annotator)

                    document.entities.append(entity)
                if str(span).replace('+','',1).replace(',','',1).replace('.','',1).replace('-','',1).isdigit():
                    value = str(span).replace(",",".")
                    value = value.replace(".", "", value.count(".") -1)
                    annotator = 'Value_Linker'
                    if float(value)>0:
                        stdform = f'+{value.replace("+","",1)}^^http://www.w3.org/2001/XMLSchema#decimal'
                    else:
                        stdform = f'{value}^^http://www.w3.org/2001/XMLSchema#decimal'     

                    entity = Entity(uri=stdform,
                                    boundaries=(span.start_char, span.end_char),
                                    surfaceform=str(span),
                                    annotator=annotator)

                    document.entities.append(entity)
            except Exception as e:
                continue

        return document

class DateLinkerRegex(BasePipeline):

    def __init__(self, language = "en"):
        self.annotator_name = 'Date_Linker'
        self.regex = {}
        self.regex['zh'] = r"(\d{2,4}年)?(\d{1,2}月)?(\d{1,2}日?)"
        self.regex_values = r'[\-\+]?\d+([.,]\d{3})*([\,\.]\d+)?'
        self.regex_dates = r'[a-zA-ZÀ-ú]{1,10}\-\d{1,2}\-\d{2,4}|\d{1,2}\-[a-zA-ZÀ-ú]{1,10}\-\d{2,4}|\d{1,4}\-\d{1,2}\-\d{2,4}|\d{1,4}\/\d{1,2}\/\d{2,4}'
        # self.regex_dates = r'[a-zA-ZÀ-ú]{1,10}\-\d{1,2}\-\d{2,4}|\d{1,2}\-[a-zA-ZÀ-ú]{1,10}\-\d{2,4}(\/|.)d{1,4}\-\d{1,2}\-\d{2,4}|\d{1,4}(\/|.)\d{1,2}(\/|.)\d{2,4}'
        # self.regex_en = r'\d{1,2}(?:[stndrh]){0,2}? [a-zA-Z]{1,10},? ?\d{2,4}|[a-zA-ZÀ-ú]{1,10} \d{1,2}(?:[stndrh]){0,2}?,? ?\d{2,4}'
        self.regex['en'] = fr'\d{{1,2}}(?:[stndrh]){{0,2}}? ?(of)? ?({"|".join(english_months)}),? ?(of)? ?\d{{2,4}}|({"|".join(english_months)})? ?(of)? \d{{1,2}}(?:[stndrh]){{0,2}}?,? ?(of)? ?\d{{2,4}}'
        # self.regex['es'] = r'(\d{1,2}|[a-zA-ZÀ-ú]{1,10})? ?[del]{0,3} (\d{1,2}|[a-zA-ZÀ-ú]{1,10}) ?[del]{0,3} \d{2,4}'
        # self.regex['ca'] = r"(\d{1,2}|[a-zA-ZÀ-ú']{3,10})? ?[del]{0,3} (\d{1,2}|[a-zA-ZÀ-ú']{3,10}) ?[del]{0,3} \d{2,4}"
        # self.regex['it'] = r'(\d{1,2}|[a-zA-ZÀ-ú]{1,10})? ?[dei]{0,3} (\d{1,2}|[a-zA-ZÀ-ú]{1,10}) ?[dei]{0,3} \d{2,4}'
        # self.regex['fr'] = r'(\d{1,2}|premier)? (\d{1,2}|[a-zA-ZÀ-ú]{1,10}) \d{2,4}'
        self.regex['de'] = fr'((\d{{1,2}}.?)|[a-zA-ZÀ-ú]{{1,30}})? ?(\d{{1,2}}|{"|".join(deutsche_months)}) ?\d{{2,4}}'
        self.regex['ru'] = fr'((\d{{1,2}}.?)|[ЁёА-я]{{1,20}})? ?(\d{{1,2}}|{"|".join(russian_months)}) ?\d{{2,4}}'
        self.regex['hi'] = fr'(\d{{1,2}}.?)? ?(\d{{1,2}}|{"|".join(hindi_months)})? ?\d{{2,4}}'
        self.regex['ar'] = fr'((\d{{2,4}}.?))? ?(\d{{2,4}}|{"|".join(arabic_months)})? ?\d{{2,4}}م?' #fr'((\d{{1,2}}.?)|[ЁёА-я]{{1,20}}) ?(\d{{1,2}}|{"|".join(arabic_months)}) ?\d{{2,4}}'
        self.regex['ko'] = r'(\d{2,4}년)? ?(\d{1,2}월)? ?(\d{1,2}일?)'
        self.regex['el'] = fr'((\d{{1,2}}.?)|[α-ωΑ-Ω]{{1,20}})? ?(\d{{1,2}}|{"|".join(greek_months)}) ?[τουης]{{0,3}} ?\d{{2,4}}'
        self.regex['sv'] = fr'((\d{{1,2}}.?)|[a-zA-ZÀ-ú]{{3,20}})? ?(\d{{1,2}}|{"|".join(swedish_months)}) ?\d{{2,4}}'
        self.regex['nl'] = fr'((\d{{1,2}}.?)|[a-zA-ZÀ-ú]{{3,20}})? ?(\d{{1,2}}|{"|".join(dutch_months)}) ?\d{{2,4}}'
        self.regex['pl'] = fr'((\d{{1,2}}.?)|[AaĄąBbCcĆćDdEeĘęFfGgHhIiJjKkLlŁłMmNnŃńOoÓóPpRrSsŚśTtUuWwYyZzŹźŻż]{{3,20}})? ?(\d{{1,2}}|{"|".join(polish_months)}) ?\d{{2,4}}r?'
        self.regex['pt'] = fr'(\d{{1,2}}|[a-zA-ZÀ-ú]{{3,10}})? ?[del]{{0,3}} (\d{{1,2}}|{"|".join(portuguese_months)}) ?[del]{{0,3}} \d{{2,4}}'
        self.regex['ca'] = fr"(\d{{1,2}}|[a-zA-ZÀ-ú]{{3,10}})? ?[del']{{0,3}} ?(\d{{1,2}}|{'|'.join(catalan_months)}) ?[del]{{0,3}} \d{{2,4}}"
        self.regex['es'] = fr'(\d{{1,2}}|[a-zA-ZÀ-ú]{{3,10}})? ?[del]{{0,3}} (\d{{1,2}}|{"|".join(spanish_months)}) ?[del]{{0,3}} \d{{2,4}}'
        self.regex['ja'] = r'(\d{2,4}年)? ?(\d{1,2}月)? ?(\d{1,2}日?)'
        self.regex['it'] = fr'(\d{{1,2}}|[a-zA-ZÀ-ú]{{3,10}})? ?[dei]{{0,3}} (\d{{1,2}}|{"|".join(italian_months)}) ?[dei]{{0,3}} \d{{2,4}}'
        # self.regex['fr'] = r'(\d{1,2}|premier)? (\d{1,2}|[a-zA-ZÀ-ú]{1,10}) \d{2,4}'
        self.regex['fr'] = fr'(\d{{1,2}}|premier)? (\d{{1,2}}|{"|".join(french_months)}) \d{{2,4}}'
        self.regex['vi'] = r'(\d{1,2})?(tháng) ?(\d{1,2})? ?(năm) ?(\d{2,4})'
        # for the token pattern 1st, 22nd, 15th etc
        # IS_REGEX_MATCH = add_regex_flag(self.nlp.vocab, '\d{1,2}(?:[stndrh]){2}?')
        self.language = language

    def filter_spans(self, spans):
        """Filter a sequence of spans and remove duplicates or overlaps. Useful for
        creating named entities (where one token can only be part of one entity) or
        when merging spans with `Retokenizer.merge`. When spans overlap, the (first)
        longest span is preferred over shorter spans.
        spans (iterable): The spans to filter.
        RETURNS (list): The filtered spans.
        """
        get_sort_key = lambda span: (span.end() - span.start(), -span.start())
        sorted_spans = sorted(spans, key=get_sort_key, reverse=True)
        result = []
        seen_tokens = set()
        for span in sorted_spans:
            # Check for end - 1 here because boundaries are inclusive
            if span.start() not in seen_tokens and span.end() - 1 not in seen_tokens:
                result.append(span)
                seen_tokens.update(range(span.start(), span.end()))
        result = sorted(result, key=lambda span: span.start())

        return result

    def run(self, document):
        matches_values = list(re.finditer(self.regex_values, document.text, re.MULTILINE))
        matches_dates = list(re.finditer(self.regex_dates, document.text, re.MULTILINE))
        # filtered_spans = filter_spans(matches_values+matches_dates)
        # matches_values = [match for match in matches_values if match in filtered_spans]
        if self.language in self.regex:
            matches_dates += list(re.finditer(self.regex[self.language], document.text, re.MULTILINE|re.IGNORECASE))
        # if self.language == 'en':
        #     matches_dates += list(re.finditer(self.regex_en, document.text, re.MULTILINE))
        # elif self.language == 'es' or self.language == 'ca':
        #     matches_dates += list(re.finditer(self.regex_es, document.text, re.MULTILINE))
        # elif self.language == 'zh':
        #     matches_dates += list(re.finditer(self.regex_zh, document.text, re.MULTILINE))
        # elif self.language == 'it':
        #     matches_dates += list(re.finditer(self.regex_it, document.text, re.MULTILINE))
        # matches_dates = [match for match in matches_dates if dateparser.parse(str(match.group()), languages=[self.language], settings={'PREFER_DAY_OF_MONTH': 'first', 'RELATIVE_BASE': datetime.datetime(2020, 1, 1)})]
        matches_dates = self.filter_spans(matches_dates)
        filtered_spans = self.filter_spans(matches_values+matches_dates)
        matches_values = [match for match in matches_values if match in filtered_spans]
        # print(matches_dates)
        for span in matches_dates:
            try:
                if self.language == 'ko':
                    date = dateparser.parse(str(span.group()).replace('년', '/').replace('월', '/').replace('일', '/'), languages=[self.language], settings={'PREFER_DAY_OF_MONTH': 'first', 'RELATIVE_BASE': datetime.datetime(2020, 1, 1)})
                else:
                    date = dateparser.parse(str(span.group()), languages=[self.language], settings={'PREFER_DAY_OF_MONTH': 'first', 'RELATIVE_BASE': datetime.datetime(2020, 1, 1)})
                annotator = self.annotator_name
                stdform = date.strftime("%Y-%m-%dT00:00:00Z^^http://www.w3.org/2001/XMLSchema#dateTime")
                entity = Entity(uri=stdform,
                    boundaries=(span.end()-len(span.group().lstrip(' of ')), span.end()),
                    surfaceform=str(span.group().lstrip(' of ')),
                    annotator=annotator)

                document.entities.append(entity)
            except Exception as e:
                continue
        for span in matches_values:
            try:
                if str(span.group()).replace('+','',1).replace(',','',1).replace('.','',1).replace('-','',1).isdigit():
                    value = str(span.group()).replace(",",".")
                    value = value.replace(".", "", value.count(".") -1)
                    annotator = 'Value_Linker'
                    if float(value)>0:
                        stdform = f'+{value}^^http://www.w3.org/2001/XMLSchema#decimal'
                    else:
                        stdform = f'{value}^^http://www.w3.org/2001/XMLSchema#decimal'     

                    entity = Entity(uri=stdform,
                                    boundaries=(span.start(), span.end()),
                                    surfaceform=str(span.group()),
                                    annotator=annotator)

                    document.entities.append(entity)
            except Exception as e:
                continue

        return document
