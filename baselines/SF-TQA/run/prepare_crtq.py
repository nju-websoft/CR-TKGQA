import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from tqdm import tqdm

from optparse import OptionParser
from utils.dataset_utils import dump_json_dataset, load_json_dataset
from utils.sutime_utils import annotate_datetime

def parse_args():
    parser = OptionParser()
    parser.add_option('-i',dest="input_path",type=str,help="input path")
    parser.add_option('-o',dest="output_path",type=str,help="output path")
    parser.add_option('-e',dest="el_type_use_golden",type=str,help="entity linking type, 't' for using golden, 'f' for not using golden", default='t')
    opt,args = parser.parse_args()
    return opt,args

def parse_entity_linking(example:dict, el_type_use_golden=True):
    entity_linking = dict()
    if el_type_use_golden:
        for qid in example['topic_entity_label_map'].keys():
            label = example['topic_entity_label_map'][qid]
            entity_linking[label] = dict()
            entity_linking[label]['mention'] = label
            entity_linking[label]['score'] = 0
            entity_linking[label]['source'] = 'golden'
            entity_linking[label]['value'] = f"http://www.wikidata.org/entity/{qid}"
    else:
        return example['entity_linking']
    return entity_linking

# CResTQA的答案是二维的，这里要展成一维的
def format_answer(answers: list):
    format_answer = list()
    for answer in answers:
        if isinstance(answer, list):
            format_answer += answer
        else:
            format_answer.append(answer)            
    return format_answer

def parse_temporal_relations(annotate):
    temporal_relations = []
    if "relations" in annotate.keys():
        id2label = {}
        for label in annotate["labels"]:
            id2label[label["id"]] = label
        for relation in annotate["relations"]:
            temporal_relation = {}
            if "signal" in relation.keys():
                temporal_relation["signal"] = id2label[relation["signal"]]
            temporal_relation["target"] = id2label[relation["target"]]
            temporal_relation["related_to"] = id2label[relation["relatedTo"]]
            if temporal_relation["related_to"]["label"] == "T1" and temporal_relation["related_to"]["interval"] is None:
                continue
            temporal_relation["type"] = relation["type"]
            temporal_relation["type"] = relation["type"]
            temporal_relation["rel_type"] = relation["relType"]
            temporal_relations.append(temporal_relation)
    return temporal_relations

if __name__ == '__main__':
    opt,_ = parse_args()
    dataset = load_json_dataset(opt.input_path)
    # 这里annotate和数据是在一起的
    format_dataset = []
    for sample in tqdm(dataset):
        format_sample = {'id': sample['id'], 'question': sample['question']}
        format_sample['answers'] = format_answer(sample['answer'])
        format_sample['entity_linking'] = parse_entity_linking(sample, True if opt.el_type_use_golden == 't' else False)
        format_sample["labels"] = list()
        format_sample["temporal_relations"] = list()
        time_annotates = annotate_datetime(sample['question'], ref_date="2024-12-31")
        if 'labels' in sample.keys() and sample['labels'] != None:
            for label in sample['labels']:
                if label["label"] == "T1":
                    label["interval"] = None
                    for time_annotate in time_annotates:
                        if label["mention"] in time_annotate["text"] or time_annotate["text"] in label["mention"]:
                            label["interval"] = time_annotate["interval"]
                format_sample['labels'].append(label)
        if 'relations' in sample.keys() and sample['relations'] != None:
            format_sample['temporal_relations'] = parse_temporal_relations(sample)
        format_dataset.append(format_sample)
    print(len(format_dataset))
    dump_json_dataset(format_dataset,opt.output_path)
