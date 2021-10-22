#!/usr/bin/env bash
# this is a bash script to bootstart the project including downloading of datasets - setup of additional tools.
# pass a language code as a variable to install a certain language. Defaults to English if no language code given.

################################
# Download Data #
###############################
echo "downloading wikipedia and wikidata dumps..."
mkdir data/$1
wikimapper download $1wiki-latest --dir data/$1/
echo "Create wikidata database"
wikimapper create $1wiki-latest --dumpdir data/$1/ --target data/$1/index_$1wiki-latest.db
echo "Extract abstracts"
python -m wikiextractor.wikiextractor.WikiExtractor data/$1/$1wiki-latest-pages-articles-multistream.xml.bz2 --links --language $1 --output text/$1 --templates data/$1/templates.txt
echo "Fix first and last file: "
echo `ls -1 text/$1/**/* | tail -1`
echo "</data>" >> `ls -1 text/$1/**/* | tail -1`
sed -i '$ d' text/$1/AA/wiki_00
echo "</data>" >> text/$1/AA/wiki_00
echo "Create triplets db"
python wikidata-triplets.py --input text/$1/ --output data/$1/wikidata-triples-$1-subj.db --input_triples wikidata/wikidata-triples.csv --format db
echo "Extract triplets to out/$1"
python multicore_run.py --input text/$1/ --output ./out/$1/ --input_triples data/$1/wikidata-triples-$1-subj.db --language $1
echo "Clean triplets to out_clean/$1"
# python filter_relations.py --folder_input out/$1
python add_filter_relations.py --folder_input out/$1