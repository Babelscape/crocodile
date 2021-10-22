# pipeline - Classes

The automatic alignment and extraction of the process consists of the following steps

* Reading input sentences
* Sentence Chunking
* Coreference Resolution
* Entity Linking (DBpedia Spotlight)
* Date and Number Detection  # 2nd Phase.
* Mapping Triples to each sentence
    - Mapping Triple to Sentences.
    - Alignment of the Property if exists (simple matching using property labels).
    - Check if Dependency path between Subject and Object words. (Boolean Value).