#!/usr/bin/env bash
# this is a bash script to bootstart the project including downloading of datasets - setup of additional tools.
# pass a language code as a variable to install a certain language. Defaults to English if no language code given.

################################
# Downloading Wikidata Triples #
################################
echo "downloading wikidata dumps..."
mkdir wikidata
cd wikidata

# triples
echo "download wikidata facts triples statements from wikidata truthy dump .."
wget https://dumps.wikimedia.org/wikidatawiki/entities/latest-truthy.nt.bz2
echo "make csv file out of nt .."
## skipping labels and meta information and keep only wikidata props
# sed -i -e 's/\(http:\/\/www.wikidata.org\/prop\/direct\/\|http:\/\/www.wikidata.org\/entity\/\)//g' wikidata-triples-ca.csv 
bzcat latest-truthy.nt.bz2 | grep "/prop/direct/P" | sed -E 's/[<>"]//g'| sed -E 's/@.+//g' | cut -d" " -f1-3 | sed -E 's/\s/\t/g' |  sed -e 's/\(http:\/\/www.wikidata.org\/prop\/direct\/\|http:\/\/www.wikidata.org\/entity\/\)//g' > wikidata-triples.csv
echo "make csv file for labels out of nt .."
