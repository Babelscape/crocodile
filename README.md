# CROCODILE: automatiC RelatiOn extraCtiOn Dataset wIth nLi filtEring.

This repository contains the extraction framework for the REBEL Dataset as seen in the paper [REBEL: Relation Extraction By End-to-end Language generation](https://github.com/Babelscape/rebel).

    @inproceedings{huguet-cabot-navigli-2021-rebel-relation,
        title = "{REBEL}: Relation Extraction By End-to-end Language generation",
        author = "Huguet Cabot, Pere-Llu{\'\i}s  and
          Navigli, Roberto",
        booktitle = "Findings of the Association for Computational Linguistics: EMNLP 2021",
        month = nov,
        year = "2021",
        address = "Punta Cana, Dominican Republic",
        publisher = "Association for Computational Linguistics",
        url = "https://aclanthology.org/2021.findings-emnlp.204",
        pages = "2370--2381",
        abstract = "Extracting relation triplets from raw text is a crucial task in Information Extraction, enabling multiple applications such as populating or validating knowledge bases, factchecking, and other downstream tasks. However, it usually involves multiple-step pipelines that propagate errors or are limited to a small number of relation types. To overcome these issues, we propose the use of autoregressive seq2seq models. Such models have previously been shown to perform well not only in language generation, but also in NLU tasks such as Entity Linking, thanks to their framing as seq2seq tasks. In this paper, we show how Relation Extraction can be simplified by expressing triplets as a sequence of text and we present REBEL, a seq2seq model based on BART that performs end-to-end relation extraction for more than 200 different relation types. We show our model{'}s flexibility by fine-tuning it on an array of Relation Extraction and Relation Classification benchmarks, with it attaining state-of-the-art performance in most of them.",
    }

This repository/project is a re-implementation of [T-REx Pipeline](https://github.com/hadyelsahar/RE-NLG-Dataset)(MIT License), more details found at: [T-REx Website](https://hadyelsahar.github.io/t-rex/), in order to work with any wikipedia dump in any language (with minor tweaks) as well as enabling a set of efficiency changes (multi-core extraction, sql usage) for the triplet alignments that dramatically increase speed and a filtering process based on Natural Language Inference. The original project was based on dbpedia abstracts, while in this one we use the paragraphs before the table of contents in any wikipedia article, or the whole wikipedia article if desired. To identify entities we use wikipedia hyperlinks and a date and value parser.

We want to thank the creators of the T-REx dataset, since this project wouldn't be possible without theirs.

### Setup 

To download the wikidata triplets run `startup.sh` 

For `./wikiextractor` we use a submodule which is a fork of the original [wikiextractor](https://github.com/attardi/wikiextractor) that implements [wikimapper](https://pypi.org/project/wikimapper/) to extract the Wikidata entities. You can find the fork [here](https://github.com/LittlePea13/wikiextractor), and clone it to the corresponding folder.

# Text Dumps

### Wikipedia Articles dump
go to `./wikiextractor` and follow the documentation there to extract wikipedia dumps with wikidata entity linkings. (You need to manually download the wikipedia dump.)

        python -m wikiextractor.wikiextractor.WikiExtractor data/en/enwiki-latest-pages-articles-multistream.xml.bz2 --links --language en --output text/en --templates templates

### Create wikidata triplet csv
In order to create a "trimmed" version of the wikidata-triples.csv that the `startup.sh` generated run the following command:

    python wikidata-triplets.py --input text/it --output data/it/wikidata-triples-it-subj.csv --input_triples wikidata/wikidata-triples.csv

This will save to `data` a version of the `wikidata-triples.csv` file containing only entites present as subjects in the extracted dump from `wikiextractor` provided in the `input` parameter. If instead you want to use a db version, using sqlite, run the following command. Be aware that this may slow down the extraction, but consumes less memory and allows for multiprocessing, so it is the adviced way:

    python wikidata-triplets.py --input text/it/ --output data/it/wikidata-triples-it-subj.db --input_triples wikidata/wikidata-triples.csv --format db

# Pipeline to create a REBEL dataset for "any" language:

Instead of running each command, one can run the pipeline script that given a lang code from wikipedia will create the dataset for that given language. Be aware that the triplet filtering uses RoBERTa XLM NLI, which albeit multilingual, doesn't include all languages. Similarly, the extractor for dates and values is regex based, which may not work for certain languages.

That said, if one wants to create the dataset for english:

    sh extract_lan.sh en

will run all necessary steps to download the latest wikipedia dump, filter the wikidata triplets, output the dataset in out/en and the cleaned up version using RoBERTa in out_clean/en. To use the filtering step, one needs torch and transformers:

    conda install pytorch torchvision torchaudio cudatoolkit=11.1 -c pytorch -c conda-forge
    pip install transformers sentencepiece protobuf

## Output Format :
All of the modules in the pipeline take the a single json file [as described below]
 and outputs the same file after filling in some of its attributes.
```
  {
        "docid":                   Document id     -- Wikipedia document id when dealing with wikipedia dump
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
```

# License 
CROCODILE is licensed under the CC BY-SA-NC 4.0 license. The text of the license can be found [here](https://github.com/Babelscape/crocodile/blob/master/LICENSE.md).
