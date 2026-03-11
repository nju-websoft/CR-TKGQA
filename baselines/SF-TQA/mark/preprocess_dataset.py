import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from transformers import AutoTokenizer

from utils.dataset_utils import *

label2id = {
    "O": 0,
    "B-T1": 1,
    "I-T1": 2,
    "B-T2": 3,
    "I-T2": 4,
    "B-E1": 5,
    "I-E1": 6,
    "B-E2": 7,
    "I-E2": 8,
    "B-S1": 9,
    "I-S1": 10,
    "B-S2": 11,
    "I-S2": 12,
    "B-P": 13,
    "I-P": 14
}
label_list = list(label2id.keys())
id2label = {
    0: "O",
    1: "B-T1",
    2: "I-T1",
    3: "B-T2",
    4: "I-T2",
    5: "B-E1",
    6: "I-E1",
    7: "B-E2",
    8: "I-E2",
    9: "B-S1",
    10: "I-S1",
    11: "B-S2",
    12: "I-S2",
    13: "B-P",
    14: "I-P"
}

def preprocess_labels(text, entity_labels=None, tokenizer=AutoTokenizer.from_pretrained("distilbert/distilbert-base-uncased")):
    """
    将基于字符偏移的实体标签转换为 BIO 格式的序列标签。
    
    :param text: 原始文本字符串。
    :param entity_labels: 实体标签，包含字符偏移和类别信息。
    :param tokenizer: 分词器，用于将文本分词为 token。
    :return: BIO 格式的标签列表。
    """
    # 分词并获取 word_ids 和 token offsets
    tokenized_inputs = tokenizer(
        text, 
        return_offsets_mapping=True, 
        is_split_into_words=False, 
        truncation=True
    )
    offsets = tokenized_inputs["offset_mapping"][1:-1]
    tokens = list()
    for offset in offsets:
        tokens.append(text[offset[0]:offset[1]])
    # 初始化标签为 "O"
    labels = ["O"] * len(offsets)
    # 遍历实体标签，将其映射到 token
    if entity_labels != None:
        for entity in entity_labels:
            entity_label = entity["label"]
            entity_start = entity["start"]
            entity_end = entity["end"]
            for idx, (start, end) in enumerate(offsets):
                if start is None or end is None:  # 忽略特殊 token
                    continue
                if start >= entity_start and end <= entity_end:  # Token 在实体范围内
                    if start == entity_start:  # 实体的起始 token
                        labels[idx] = f"B-{entity_label}"
                    else:  # 实体的内部 token
                        labels[idx] = f"I-{entity_label}"
    ner_tags = [label2id[label] for label in labels]
    assert(len(tokens) == len(ner_tags))
    return tokens, ner_tags


if __name__ == '__main__':
    data = load_json_dataset('mark/data/dataset/tlink_timequestion_test_result.json')
    for example in data:
        if 'labels' not in example.keys():
            example['labels'] = None
        example['tokens'], example['ner_tags'] = preprocess_labels(example['question'], example['labels'])
    dump_json_dataset(data, 'mark/data/dataset/test.json')