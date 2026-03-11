from global_utils.file_util import *
from global_utils.component import query_label, load_label_dict


def format_cres_answer_for_exaqt(answer, answer_type):
    Answer = []
    for ans in answer:
        for i, ele in enumerate(ans): # 压缩到一维
            ele_type = answer_type[i]
            if ele_type == 'Entity':
                item = {
                    "AnswerType": 'Entity',
                    "WikidataQid": ele,
                    "WikidataLabel": query_label(ele),
                    "WikipediaURL": None
                }
            elif ele_type == 'Time':
                item = {
                    "AnswerType": "Value",
                    "AnswerArgument": ele + "T00:00:00Z"
                }
            elif ele_type == 'Number':
                item = {
                    "AnswerType": "Value",
                    "AnswerArgument": ele
                }
            elif ele_type == 'Boolean': # exaqt 之前没有处理
                item = {
                    "AnswerType": "Value",
                    "AnswerArgument": ele
                }
            else:
                raise Exception
            Answer.append(item)
    return Answer


def process_cresqa():
    origin_dataset_dir = '../../dataset/CResQA'
    output_dir = '_benchmarks/CResQA'
    for filename in ['train', 'dev', 'test']:
        input_path = os.path.join(origin_dataset_dir, filename)
        data = read_json_file(input_path)
        new_data = []
        for i, item in enumerate(data):
            new_item = {}
            new_item['Id'] = item['id']
            new_item['Question'] = item['question']
            new_item['Answer'] = format_cres_answer_for_exaqt(item['answer'], item['answer_type'])
            new_item["Temporal signal"] = ["No signal"] # 设置默认
            new_item["Temporal question type"] = ["Explicit"] # 设置默认
            new_item['topic_entity_label_map'] = item['topic_entity_label_map']
            new_item['gold_entity_label_map'] = item['gold_entity_label_map']
            new_item['gold_relation_label_map'] = item['gold_relation_label_map']
            new_data.append(new_item)
        output_path = os.path.join(output_dir, filename)
        write_json_file(output_path, new_data)


def process_nerd():
    input_nerd_dir = '../../dataset/CResQA'
    output_nerd_dir = '_intermediate_representations/CResQA'
    for filename in ['train-nerd', 'dev-nerd', 'test-nerd']:
        input_path = os.path.join(input_nerd_dir, filename)
        data = read_json_file(input_path)
        new_data = []
        for i, item in enumerate(data):
            new_item = {}
            new_item['Id'] = item['id']
            new_item['Question'] = item['question']
            new_item['Answer'] = format_cres_answer_for_exaqt(item['answer'], item['answer_type'])
            new_item["Temporal signal"] = ["No signal"] # 设置默认
            new_item["Temporal question type"] = ["Explicit"] # 设置默认
            new_item['topic_entity_label_map'] = item['topic_entity_label_map']
            new_item['gold_entity_label_map'] = item['gold_entity_label_map']
            new_item['gold_relation_label_map'] = item['gold_relation_label_map']
            new_item['tagme'] = item['tagme']
            new_item['elq'] = item['elq']
            new_item['wat'] = item['wat']
            new_data.append(new_item)
        output_path = os.path.join(output_nerd_dir, filename)
        write_json_file(output_path, new_data)


if __name__ == '__main__':
    load_label_dict()
    
    process_nerd()
    process_cresqa()