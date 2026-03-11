import json

def read_all_data():
    file_dir = 'mark/data/annotate/'
    files = ['dev', 'test', 'train']
    data = list()
    for file in files:
        data += json.load(open(f"{file_dir}tlink_timequestion_{file}_result.txt", 'r', encoding='utf-8'))
    return data

def id2label(example):
    if 'labels' not in example.keys():
        return
    label_list = list()
    for i in range(len(example['labels'])):
        label_list.append(None)
    for label in example['labels']:
        assert(label_list[label['id']] == None)
        label_list[label['id']] = label
    example['labels'] = label_list

def relations_str(example):
    ans = list()
    if 'relations' in example.keys():
        for relation in example['relations']:
            ans.append(f"({example['labels'][relation['property']]['label'] if 'property' in relation.keys() else example['labels'][relation['target']]['label']}) -> [{example['labels'][relation['signal']]['label'] if 'signal' in relation.keys() else '_'}] -> ({example['labels'][relation['relatedTo']]['label']}) : {relation['type']} [{relation['relType']}]")
    return ans

def main():
    data = read_all_data()
    rules = dict()
    for example in data:
        id2label(example)
        for rule in relations_str(example):
            if rule not in rules.keys():
                rules[rule] = 0
            rules[rule] += 1
    # rule_list = [rules[key] for key in rules.keys()]
    # rule_list = list(set(rule_list))
    # rule_list.sort(reverse=True)
    # for rule in rule_list:
    #     for key in rules.keys():
    #         if rule == rules[key]:
    #             print(f"{rules[key]}: {key}")
    rule_list = list(rules.keys())
    rule_list.sort()
    for rule in rule_list:
        print(f"{rule} ::::: {rules[rule]}")

if __name__ == '__main__':
    main()
