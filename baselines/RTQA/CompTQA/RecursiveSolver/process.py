import json

def readjson(path):
    '''读取json,json_list就返回list,json就返回dict'''
    with open(path, mode='r', encoding='utf-8') as load_f:
        data_ = json.load(load_f)
    return data_

def savejson(file_name, json_info, indent=4):
    '''json list保存为json'''
    with open('{}.json'.format(file_name), mode='w', encoding='utf-8') as fp:
        json.dump(json_info, fp, indent=indent, sort_keys=False, ensure_ascii=False)


raw_data = readjson("../../datasets/CompTQA/test_sample_1000.json")
tree = readjson("trees_10.json")

question_qid_map = {}

for item1, item2 in zip(raw_data, tree):
    qid = item1['id']
    question_qid_map[item2[-1]['question_text']] = qid

# path of result file
data = readjson("xxx")

for it in data:
    for ele in it:
        if "gold_answer" in ele:
            ele['question_id'] = question_qid_map[ele['question_text']]

savejson("processed_results", data)