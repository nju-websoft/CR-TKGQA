import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import json
from tqdm import tqdm

from utils.dataset_utils import *
from run.annotate_post_process import annotate_temporal_relations

position_list = ['B', 'I']
label_list = ['T1', 'T2', 'E1', 'E2', 'S1', 'S2', 'P']

trigger2relation = {
    "start":"START",
    "start time is":"START",
    "start time of":"START",
    "from":"START",
    "since":"START",
    "earliest": "START",
    "end": "END",
    "until":"END",
    "til":"END",
    "end time is":"END",
    "end time of":"END",
    "up to":"END",
    "latest": "END",
    "last": "END",
    "after":"AFTER",
    "before":"BEFORE",
    "prior": "BEFORE",
    "prior to": "BEFORE",
    "in":"INCLUDE",
    "at":"INCLUDE",
    "on":"INCLUDE",
    "during":"INCLUDE",
    "time": "INCLUDE",
    "between": "INCLUDE",
    "while":"SIMULTANEOUS",
    "when":"SIMULTANEOUS",
    "point in time": "SIMULTANEOUS",
    "at point in time of":"SIMULTANEOUS",
    "point in time is":"SIMULTANEOUS"
}

data_dir = 'mark/data/dataset/post_process'
data_sub_dirs = ['0_group',
                 '1_labels',
                 '2_labels_group',
                 '3_split',
                 '4_relations'
                ]
check_dirs = [data_dir]
check_dirs += [data_dir + '/' + str(data_sub_dir) for data_sub_dir in data_sub_dirs]

for check_dir in check_dirs:
    if not os.path.exists(check_dir):
        os.makedirs(check_dir)

def group_labels_mark(example):
    example['grouped_labels_mark'] = list()
    if len(example['labels_mark']) == 0:
        return
    for label_mark in example['labels_mark']:
        entity = label_mark['entity'].split('-')
        assert(len(entity) == 2 and entity[0] in position_list and entity[1] in label_list)
        if entity[0] == 'B':
            example['grouped_labels_mark'].append([label_mark])
        else:
            # assert(entity[1] == example['grouped_labels_mark'][-1][0]['entity'].split('-')[1])
            if len(example['grouped_labels_mark']) == 0:
                continue
            prev_label_mark = example['grouped_labels_mark'][-1][-1]
            if label_mark['index'] != prev_label_mark['index'] + 1:
                continue
            example['grouped_labels_mark'][-1].append(label_mark)

def group_labels_mark_main():
    step = 0
    load_dir = 'mark/data/dataset/inference'
    files = os.listdir(load_dir)
    json_files = list()
    for file in files:
        file_dir = os.path.join(load_dir, file)
        if os.path.isfile(file_dir) and file_dir[-5:] == '.json' and file[:12] == 'labels_mark_':
            json_files.append(file_dir)
    save_dir = f'{data_dir}/{data_sub_dirs[step]}'
    for json_file in json_files:
        data = json.load(open(json_file, 'r', encoding='utf-8'))
        save_file = save_dir + '/' + json_file.split('/')[-1].replace('labels_mark_', '')
        for example in tqdm(data):
            group_labels_mark(example)
        with open(save_file, 'w', encoding='utf-8') as w:
            json.dump(data, w, indent=4)

def group_labels(example):
    example['mark_labels'] = list()
    for i, group in enumerate(example['grouped_labels_mark']):
        group_label = dict()
        label_vote = dict() # 统计一下组里所有label的数量，取最多的最为组的label
        for mark in group:
            entity = mark['entity'].split('-')[1]
            if entity not in label_vote.keys():
                label_vote[entity] = 0
            label_vote[entity] += 1
        max_label = None
        max_label_count = 0
        for key in label_vote.keys():
            if label_vote[key] > max_label_count:
                max_label_count = label_vote[key]
                max_label = key
        assert(max_label != None and max_label_count > 0)
        group_label['label'] = max_label
        group_label['start'] = group[0]['start']
        group_label['end'] = group[-1]['end']
        group_label['mention'] = example['question'][group_label['start']:group_label['end']]
        group_label['id'] = i
        group_label['start_index'] = group[0]['index']
        group_label['end_index'] = group[-1]['index'] + 1
        example['mark_labels'].append(group_label)
    # labels = example['mark_labels']
    # example['mark_labels'] = list()
    # for label in labels:
    #     if label['label'] == 'S1' and mention2relType(label['mention']) == None:
    #         continue
    #     example['mark_labels'].append(label)
    example.pop('labels_mark')
    example.pop('grouped_labels_mark')

def group_labels_main():
    step = 1
    load_dir = f'{data_dir}/{data_sub_dirs[step - 1]}'
    files = os.listdir(load_dir)
    json_files = list()
    for file in files:
        file_dir = os.path.join(load_dir, file)
        if os.path.isfile(file_dir) and file_dir[-5:] == '.json':
            json_files.append(file_dir)
    save_dir = f'{data_dir}/{data_sub_dirs[step]}'
    for json_file in json_files:
        data = json.load(open(json_file, 'r', encoding='utf-8'))
        save_file = save_dir + '/' + json_file.split('/')[-1]
        for example in tqdm(data):
            group_labels(example)
        with open(save_file, 'w', encoding='utf-8') as w:
            json.dump(data, w, indent=4)

# def annotate_temporal_relations_main():
#     step = 2
#     load_dir = f'{data_dir}/{data_sub_dirs[step - 1]}'
#     files = os.listdir(load_dir)
#     json_files = list()
#     for file in files:
#         file_dir = os.path.join(load_dir, file)
#         if os.path.isfile(file_dir) and file_dir[-5:] == '.json':
#             json_files.append(file_dir)
#     save_dir = f'{data_dir}/{data_sub_dirs[step]}'
#     for json_file in json_files:
#         data = json.load(open(json_file, 'r', encoding='utf-8'))
#         save_file = save_dir + '/' + json_file.split('/')[-1]
#         annotate_temporal_relations(json_file, save_file)
#         with open(save_file, 'w', encoding='utf-8') as w:
#             json.dump(data, w, indent=4)

def merge_continuous_same_label(example):
    example['merged_mark_labels'] = list()
    for label in example['mark_labels']:
        if len(example['merged_mark_labels']) == 0:
            example['merged_mark_labels'].append(label)
            continue
        if label['label'] == example['merged_mark_labels'][-1]['label'] and label['start_index'] == example['merged_mark_labels'][-1]['end_index']:
            example['merged_mark_labels'][-1]['end'] = label['end']
            example['merged_mark_labels'][-1]['end_index'] = label['end_index']
        else:
            example['merged_mark_labels'].append(label)
    for i in range(len(example['merged_mark_labels'])):
        example['merged_mark_labels'][i]['mention'] = example['question'][example['merged_mark_labels'][i]['start']:example['merged_mark_labels'][i]['end']]
        example['merged_mark_labels'][i]['id'] = i
        example['merged_mark_labels'][i].pop('start_index')
        example['merged_mark_labels'][i].pop('end_index')
    example.pop('mark_labels')

def merge_continuous_same_label_main():
    step = 2
    load_dir = f'{data_dir}/{data_sub_dirs[step - 1]}'
    files = os.listdir(load_dir)
    json_files = list()
    for file in files:
        file_dir = os.path.join(load_dir, file)
        if os.path.isfile(file_dir) and file_dir[-5:] == '.json':
            json_files.append(file_dir)
    save_dir = f'{data_dir}/{data_sub_dirs[step]}'
    for json_file in json_files:
        data = json.load(open(json_file, 'r', encoding='utf-8'))
        save_file = save_dir + '/' + json_file.split('/')[-1]
        for example in tqdm(data):
            merge_continuous_same_label(example)
        with open(save_file, 'w', encoding='utf-8') as w:
            json.dump(data, w, indent=4)



def split_by_signal1(labels):
    labels_group = list()
    for label in labels:
        if label['label'] == 'S1':
            if len(labels_group) == 0 or labels_group[-1][0]['label'] != 'S1':
                labels_group.append([label])
            else:
                labels_group[-1].append(label)
        else:
            if len(labels_group) == 0:
                labels_group.append([label])
            else:
                if labels_group[-1][0]['label'] == 'S1':
                    labels_group.append([label])
                else:
                    labels_group[-1].append(label)
    return labels_group

def split_by_signal1_main():
    step = 3
    load_dir = f'{data_dir}/{data_sub_dirs[step - 1]}'
    files = os.listdir(load_dir)
    json_files = list()
    for file in files:
        file_dir = os.path.join(load_dir, file)
        if os.path.isfile(file_dir) and file_dir[-5:] == '.json':
            json_files.append(file_dir)
    save_dir = f'{data_dir}/{data_sub_dirs[step]}'
    for json_file in json_files:
        data = json.load(open(json_file, 'r', encoding='utf-8'))
        save_file = save_dir + '/' + json_file.split('/')[-1]
        for example in tqdm(data):
            example['grouped_mark_labels'] = split_by_signal1(example['merged_mark_labels'])
        with open(save_file, 'w', encoding='utf-8') as w:
            json.dump(data, w, indent=4)

# def test():
#     step = 4
#     load_dir = f'{data_dir}/{data_sub_dirs[step - 1]}'
#     files = os.listdir(load_dir)
#     json_files = list()
#     for file in files:
#         file_dir = os.path.join(load_dir, file)
#         if os.path.isfile(file_dir) and file_dir[-5:] == '.json':
#             json_files.append(file_dir)
#     for json_file in json_files:
#         data = json.load(open(json_file, 'r', encoding='utf-8'))
#         for example in data:
#             for i in range(len(example['grouped_mark_labels']) - 1):
#                 if example['grouped_mark_labels'][i + 1][0]['label'] == 'S1' and example['grouped_mark_labels'][i][0]['label'] == 'S1':
#                     print(example['id'])

def format_mention(mention):
    for tk in [',', '.', '<', '>', '?', '/', ';', ':', '\'', '"', '[', ']', '{', '}', '|', '\\', '`', '~', '!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '-', '_', '=', '+']:
        mention = mention.replace(tk, '')
    while '  ' in mention:
        mention = mention.replace('  ', ' ')
    return mention

def mention2relType(mention):
    mention = format_mention(mention)
    if mention in trigger2relation.keys():
        return trigger2relation[mention]
    for key in trigger2relation.keys():
        if key in mention or mention in key:
            return trigger2relation[key]
    return None
    

def relations(example):
    example['mark_relations'] = list()
    for i, group in enumerate(example['grouped_mark_labels']):
        if group[0]['label'] == 'S1' and i > 0 and i < len(example['grouped_mark_labels']) - 1:
            prev_group = example['grouped_mark_labels'][i - 1]
            next_group = example['grouped_mark_labels'][i + 1]
            for label in group:
                for prev in prev_group:
                    for next in next_group:
                        if (prev['label'], label['label'], next['label']) in [('E1', 'S1', 'E1'), ('E2', 'S1', 'E1'), ('E2', 'S1', 'E2'), ('E2', 'S1', 'S2'), ('E2', 'S1', 'T1')]:
                            relType = mention2relType(label['mention'])
                            if relType == None:
                                continue
                            relation = dict()
                            relation['type'] = 'TLINK'
                            relation['target'] = prev['id']
                            relation['signal'] = label['id']
                            relation['relatedTo'] = next['id']
                            relation['relType'] = relType
                            example['mark_relations'].append(relation)
                        elif (prev['label'], label['label'], next['label']) == ('P', 'S1', 'T1'):
                            relType = mention2relType(label['mention'])
                            if relType == None:
                                continue
                            relation = dict()
                            relation['type'] = 'TREL'
                            relation['property'] = prev['id']
                            relation['signal'] = label['id']
                            relation['relatedTo'] = next['id']
                            relation['relType'] = relType
                            example['mark_relations'].append(relation)
        else:
            for j in range(len(group)):
                for k in range(j + 1, len(group)):
                    if (group[j]['label'], group[k]['label']) in [('E1', 'T1'), ('E2', 'E1'), ('E2', 'T1')]:
                        relation = dict()
                        relation['type'] = 'TLINK'
                        relation['target'] = group[j]['id']
                        relation['relatedTo'] = group[k]['id']
                        relation['relType'] = 'INCLUDE'
                        example['mark_relations'].append(relation)
                    elif (group[j]['label'], group[k]['label']) == ('P', 'T1'):
                        relation = dict()
                        relation['type'] = 'TREL'
                        relation['property'] = group[j]['id']
                        relation['relatedTo'] = group[k]['id']
                        relation['relType'] = 'INCLUDE'
                        example['mark_relations'].append(relation)
    example.pop('grouped_mark_labels')

def relations_main():
    step = 4
    load_dir = f'{data_dir}/{data_sub_dirs[step - 1]}'
    files = os.listdir(load_dir)
    json_files = list()
    for file in files:
        file_dir = os.path.join(load_dir, file)
        if os.path.isfile(file_dir) and file_dir[-5:] == '.json':
            json_files.append(file_dir)
    save_dir = f'{data_dir}/{data_sub_dirs[step]}'
    for json_file in json_files:
        data = json.load(open(json_file, 'r', encoding='utf-8'))
        save_file = save_dir + '/' + json_file.split('/')[-1]
        for example in tqdm(data):
            relations(example)
        with open(save_file, 'w', encoding='utf-8') as w:
            json.dump(data, w, indent=4)

if __name__ == '__main__':
    # group_labels_main()
    # merge_continuous_same_label_main()
    # split_by_signal1_main()
    relations_main()