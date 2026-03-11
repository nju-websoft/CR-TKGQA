from tqdm import tqdm

from exaqt.answer_predict.script_listscore import *
from global_utils.file_util import *


def eval_res(kb_pred_file, threshold, target_level=None, target_tag=None, fp=None):
    gold_data_dict = read_json_as_dict('../../dataset/CResQA/test.json', 'id')

    kb_only_recall, kb_only_precision, kb_only_f1, kb_only_hits = [], [], [], []
    kb_only_mrr = []
    kb_only_hits5 = []

    with open(kb_pred_file) as f_kb:
        count = 0
        for line_kb in tqdm(zip(f_kb)):
            line_kb = json.loads(line_kb[0])
            id = line_kb['id']

            level = gold_data_dict[id]['comp_level']
            tag_list = gold_data_dict[id]['tags']
            if target_level is not None and level != target_level:
                continue
            if target_tag is not None and target_tag not in tag_list:
                continue
                
            answers = set([answer for answer in line_kb['answers']])
            dist_kb = line_kb['dist']
            kb_entities = set(dist_kb.keys())

            p, r, f1, hits1, best_entity = get_one_f1(kb_entities, dist_kb, threshold, answers)
            kb_only_precision.append(p)
            kb_only_recall.append(r)
            kb_only_f1.append(f1)
            kb_only_hits.append(hits1)

            mmr = get_mmr_metric(kb_entities, dist_kb, 5, answers)
            kb_only_mrr.append(mmr)

            hits5, best_entities = get_hitsmetric(kb_entities, dist_kb, 5, answers)
            kb_only_hits5.append(hits5)

            # result = "{0}${1}${2}${3}${4}".format( str(id), str(hits1), str(hits5), str(mmr), ";".join(best_entities))
            # print(result)
            count += 1

    print("line count |" + str(count))
    print('Average hits1: ' , str(sum(kb_only_hits) / len(kb_only_hits)))
    print('Average hits5: ' , str(sum(kb_only_hits5) / len(kb_only_hits5)))
    print('Average mmr: ' , str(sum(kb_only_mrr) / len(kb_only_mrr)))
    print('precision: ' , str(sum(kb_only_precision) / len(kb_only_precision)))
    print('recall: ' , str(sum(kb_only_recall) / len(kb_only_recall)))
    print('f1: ' , str(sum(kb_only_f1) / len(kb_only_f1)))

pred_path = '_intermediate_representations/CResQA/elq/answer_predict/model/pred_entqtkg'
# Overall
print("#################### Overall ####################")
eval_res(pred_path, threshold=0.1)


# level
print("#################### level ####################")
for level in ['iid', 'compositional', 'zero-shot']:
    print('--------------------------------------')
    print(level)
    eval_res(pred_path, target_level=level, threshold=0.1)

# tag
print("#################### tag ####################")
for tag in ['O', 'C', 'C-M', 'M', 'M-C', 'C and M']:
    print('--------------------------------------')
    print(tag)
    eval_res(pred_path, target_level='iid', target_tag=tag, threshold=0.1)
    