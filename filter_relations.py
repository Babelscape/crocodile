import jsonlines
import re
import transformers
import torch
from tqdm import trange, tqdm
import argparse
import os, sys

def get_case_insensitive_key_value(input_dict, key):
    return next((value for dict_key, value in input_dict.items() if dict_key.lower() == key.lower()), None)

def filter_triples(model, tokenizer, texts):
    if max([len(text) for text in texts])>256:
        range_length = 12
    else:
        range_length = 64
    result = []
    for batch in range(0,len(texts),range_length):
        encoded_input = tokenizer(
                [ex[0] for ex in texts[batch: batch + range_length]], [ex[1] for ex in texts[batch: batch + range_length]],
                return_tensors="pt",
                add_special_tokens=True,
                max_length=256,
                padding='longest',
                return_token_type_ids=False,
                truncation_strategy='only_first')
        for tensor in encoded_input:
            encoded_input[tensor] = encoded_input[tensor].cuda()
        with torch.no_grad():  # remove this if you need gradients.
            outputs = model(**encoded_input, return_dict=True, output_attentions=False, output_hidden_states = False)
        result.append(outputs['logits'].softmax(dim=1))
        del outputs
    logits = torch.cat(result)
    if language == 'ko':
        return logits.argmax(1) == get_case_insensitive_key_value(model.config.label2id, 'entailment')# [:,get_case_insensitive_key_value(model.config.label2id, 'entailment')]>0.75
    return logits[:,get_case_insensitive_key_value(model.config.label2id, 'entailment')]>0.75

def prepare_triplet(subject_entity, object_entity, article_text, predicate):
    text_triplet = ''
    text_triplet += re.compile("(?<!\d)\.(?!\d)").split(article_text[:min(subject_entity['boundaries'][0], object_entity['boundaries'][0])])[-1]
    text_triplet += article_text[min(subject_entity['boundaries'][0], object_entity['boundaries'][0]):max(subject_entity['boundaries'][1], object_entity['boundaries'][1])]
    text_triplet += re.compile("(?<!\d)\.(?!\d)").split(article_text[max(subject_entity['boundaries'][1], object_entity['boundaries'][1]):])[0]
    return (text_triplet.strip('\n'), ' '.join([str(subject_entity['surfaceform']), str(predicate['surfaceform']), str(object_entity['surfaceform'])]))

def main(folder_input = 'out/ko'):
    global language 
    language = folder_input.split('/')[1]
    if folder_input.split('/')[1] == 'ko':
        model_name_or_path = '/home/huguetcabot/sentence_transformers/test-glue/checkpoint-1564'
    else:
        model_name_or_path = 'joeddav/xlm-roberta-large-xnli'

    tokenizer = transformers.AutoTokenizer.from_pretrained(
            model_name_or_path)
    model_config = transformers.AutoConfig.from_pretrained(
        model_name_or_path,
        # num_labels=2,
        output_hidden_states=True,
        output_attentions=True,
    )
    model = transformers.AutoModelForSequenceClassification.from_pretrained(model_name_or_path, config = model_config)
    model.cuda()
    model.eval()
    model.half()
    with jsonlines.open(f'out_clean/{"/".join(folder_input.split("/")[1:])}.jsonl', mode='w') as writer:
        for k,j,y in os.walk(folder_input):
            for file_name in y:
                with jsonlines.open(k + '/' + file_name) as reader:
                    for i, article in tqdm(enumerate(reader)):
                        previous = []
                        triples_list = []
                        texts = []
                        for triple in article['triples']:
                            if triple['subject']['boundaries'] != None and triple['object']['boundaries'] != None and (triple['subject']['boundaries'], triple['object']['boundaries']) not in previous:
                                previous.append((triple['subject']['boundaries'], triple['object']['boundaries']))
                                triples_list.append(triple)
                                texts.append(prepare_triplet(triple['subject'], triple['object'], article['text'], triple["predicate"]))
                            elif (triple['subject']['boundaries'], triple['object']['boundaries']) not in previous:
                                distance = 1000000
                                for entity in article['entities']:
                                    if entity['uri'] == triple['subject']['uri']:
                                        if abs(min(triple['object']['boundaries'])-min(entity['boundaries'])) < distance:
                                            subject_entity = entity
                                            distance = abs(min(triple['object']['boundaries'])-min(entity['boundaries']))
                                triple['subject'] = subject_entity
                                previous.append((triple['subject']['boundaries'], triple['object']['boundaries']))
                                triples_list.append(triple)
                                texts.append(prepare_triplet(subject_entity, triple['object'], article['text'], triple["predicate"]))
                        indexes = filter_triples(model, tokenizer, texts)
                        if len(indexes) == 0:
                            continue
                        article['triples'] = [x for i,x in zip(indexes, triples_list) if (i == True) or x["predicate"]["uri"] in ["P569", "P570"]]
                        writer.write(article)
if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description=__doc__)
    parser.add_argument("--folder_input", 
                        help="input file")
    args = parser.parse_args()

    main(args.folder_input)
