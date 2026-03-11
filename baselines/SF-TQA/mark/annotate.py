import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from mark.mark_train import classify
from postprocess_dataset import group_labels_mark, group_labels, merge_continuous_same_label, split_by_signal1, relations
from utils.dataset_utils import dump_json_dataset, load_json_dataset
from run.annotate_post_process import enrich_temporal_relations

best_model_path = 'mark/data/model/best/checkpoint-4856'

def mark_pipeline(data):
    data = classify(data, best_model_path)
    for example in data:
        group_labels_mark(example)
        group_labels(example)
        merge_continuous_same_label(example)
        example['grouped_mark_labels'] = split_by_signal1(example['merged_mark_labels'])
        relations(example)
        if 'labels' in example.keys():
            example.pop('labels')
        if 'relations' in example.keys():
            example.pop('relations')
        if 'temporal_relations' in example.keys():
            example.pop('temporal_relations')
        example['labels'] = example['merged_mark_labels']
        example['relations'] = example['mark_relations']
        example.pop('merged_mark_labels')
        example.pop('mark_relations')
    enrich_temporal_relations(data)
    return data

def main():
    load_dir = 'data/dataset/ResTQA/raw'
    save_dir = 'data/dataset/ResTQA/run/annotate'
    files = os.listdir(load_dir)
    json_files = list()
    for file in files:
        file_dir = os.path.join(load_dir, file)
        if os.path.isfile(file_dir) and file_dir[-5:] == '.json':
            json_files.append(file_dir)
    for json_file in json_files:
        data = load_json_dataset(json_file)
        save_file = save_dir + '/' + json_file.split('/')[-1]
        data = mark_pipeline(data)
        dump_json_dataset(data, save_file)

if __name__ == '__main__':
    main()