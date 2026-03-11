from tqdm import tqdm

from global_utils.file_util import *
from global_utils.metrics import Metrics, get_precision, get_recall, get_f1_score_by_pr


def eval_res(res_path, target_level=None, target_tag=None):
    gold_data_dict = read_json_as_dict('../../dataset/CResQA/test.json', 'question')
    results = read_json_file(res_path)
    
    metrics = Metrics()
    for result in results:
        question = result['question']
        if question not in gold_data_dict:
            # print(question) # 20个问题有修改
            continue
        level = gold_data_dict[question]['comp_level']
        tag_list = gold_data_dict[question]['tags']
        if target_level is not None and level != target_level:
            continue
        if target_tag is not None and target_tag not in tag_list:
            continue
        
        precision, recall, f1 = result['res']['P'], result['res']['R'], result['res']['F1']
        metrics.add_metric('P', precision)
        metrics.add_metric('R', recall)
        metrics.add_metric('F1', f1)
        metrics.count()

    print('-----------------------------------')
    print(metrics.get_metrics(['P', 'R', 'F1']))



res_path = '_results/res.json'
# Overall
print("#################### Overall ####################")
eval_res(res_path)

# level
print("#################### level ####################")
for level in ['iid', 'compositional', 'zero-shot']:
    print('--------------------------------------')
    print(level)
    eval_res(res_path, target_level=level)

# tag
print("#################### tag ####################")
for tag in ['O', 'C', 'C-M', 'M', 'M-C', 'C and M']:
    print('--------------------------------------')
    print(tag)
    eval_res(res_path, target_level='iid', target_tag=tag)
    