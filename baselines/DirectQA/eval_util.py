import re
from collections import Counter
from itertools import chain

from global_utils.file_util import *
from global_utils.metrics import Metrics, get_precision, get_recall, get_f1_score_by_pr
from baseline.directQA.utils.verify_result import compare_a_pred_golden_pair
"""
Since direct QA answers the label of entities, we need to re-implement the metrics.

"""

def eval_cres_result_bak_directqa(pred_path, gold_path, eval_res_path, stat_exec_cnt=True):
    print('**********************************************************')
    print(f'Start evaluating {pred_path}, golden: {gold_path}')
    pred_data = read_json_file(pred_path)
    gold_data_dict = read_json_as_dict(gold_path, 'id')
    eval_total = {}

    # total
    metrics = Metrics()
    executable_cnt = 0
    executable_idx_list = []
    # three_level
    three_level_metrics = [Metrics(), Metrics(), Metrics()]
    level_idx_map = {
        "iid": 0,
        "compositional": 1,
        "zero-shot": 2
    }
    idx_level_map = {v: k for k, v in level_idx_map.items()}
    level_sample_cnt = [0] * 3
    # tag
    tag_metrics = [Metrics(), Metrics(), Metrics(), Metrics(), Metrics(), Metrics()]
    tag_idx_map = {
        # 第一类
        "multiple_temporal_facts": 0,
        "non_temporal_multihop": 1,
        # 第二类
        "temporal_comparison": 2,
        "temporal_calculation": 3,
        "composited_temporal_calculation": 4,
        # 其他
        "implicit": 5,
    }
    idx_tag_map = {v: k for k, v in tag_idx_map.items()}

    for idx, item in enumerate(pred_data):
        qid = item['id']

        if stat_exec_cnt and item['executable_idx'] is not None:
            executable_cnt += 1
            executable_idx_list.append(item['executable_idx'])

        pred_answer = item['answer']
        gold_answer = gold_data_dict[qid]['answer']
        # print(pred_answer, gold_answer)

        pred_data[idx]['p'] = precision = get_precision(gold_answer, pred_answer)
        pred_data[idx]['r'] = recall = get_recall(gold_answer, pred_answer)
        pred_data[idx]['f1'] = f1 = get_f1_score_by_pr(precision, recall)

        metrics.add_metric('total P', precision)
        metrics.add_metric('total R', recall)
        metrics.add_metric('total F1', f1)
        metrics.count()

        # ----------------------three_level----------------------------
        level = gold_data_dict[qid]['comp_level']
        level_idx = level_idx_map[level]
        level_sample_cnt[level_idx] += 1
        three_level_metrics[level_idx].add_metric(f'{level} P', precision)
        three_level_metrics[level_idx].add_metric(f'{level} R', recall)
        three_level_metrics[level_idx].add_metric(f'{level} F1', f1)
        three_level_metrics[level_idx].count()

        # ----------------------tag----------------------------
        for tag in gold_data_dict[qid]['tags']:
            if tag not in tag_idx_map:
                continue
            tag_idx = tag_idx_map[tag]
            tag_metrics[tag_idx].add_metric(f'{tag} P', precision)
            tag_metrics[tag_idx].add_metric(f'{tag} R', recall)
            tag_metrics[tag_idx].add_metric(f'{tag} F1', f1)
            tag_metrics[tag_idx].count()

    if stat_exec_cnt:
        print('-----------------------------------')
        eval_total['executable_cnt'] = executable_cnt
        eval_total['executable_rate'] = executable_cnt / len(pred_data)
        print(f'executable_cnt: {executable_cnt}, executable_rate: {executable_cnt / len(pred_data)}')

        shot_idx_frequency = Counter(executable_idx_list)
        print(shot_idx_frequency)
        shot_idx_total_count = sum(shot_idx_frequency.values())
        frequency_percentage = {key: (count / shot_idx_total_count) * 100 for key, count in shot_idx_frequency.items()}
        for key in sorted(frequency_percentage.keys()):
            percentage = frequency_percentage[key]
            print(f"executable_idx = {key}: {percentage:.2f}%")

    print('-----------------------------------')
    print(metrics.get_metrics(['total P', 'total R', 'total F1']))
    eval_total['total'] = metrics.get_metrics(['total P', 'total R', 'total F1'])
    
    print('-----------------------------------')
    for i in range(len(three_level_metrics)):
        # print(level_sample_cnt[i])
        level = list(level_idx_map.keys())[i]
        print(three_level_metrics[i].get_metrics([f'{level} P', f'{level} R', f'{level} F1']))
        eval_total[level] = three_level_metrics[i].get_metrics([f'{level} P', f'{level} R', f'{level} F1'])
    
    print('-----------------------------------')
    for i in range(len(tag_metrics)):
        tag = list(tag_idx_map.keys())[i]
        print(tag_metrics[i].get_metrics([f'{tag} P', f'{tag} R', f'{tag} F1']))
        eval_total[tag] = tag_metrics[i].get_metrics([f'{tag} P', f'{tag} R', f'{tag} F1'])
    
    # 每个例子的结果
    write_json_file(eval_res_path, pred_data)

    # 整体结果
    eval_total_path = eval_res_path.replace('_eval', '_eval_total')
    write_json_file(eval_total_path, eval_total)


def eval_cres_result_once_directqa(pred_path, eval_res_path, stat_exec_cnt=True, stat_feedback=False, target_level=None, target_tag=None, dump_results=False):
    pred_data = read_json_file(pred_path)
    eval_res = {}

    if stat_feedback:
        feedback_count = [len(item['res'])-1 for item in pred_data]
        feedback_dist = Counter(feedback_count)
        eval_res['feedback_dist'] = feedback_dist
        ##################################
        # 反馈次数（每个问题可重复计算）
        prefix_list = []
        for item in pred_data:
            if len(item['msgs']) <= 2:
                continue
            for feedback_idx in range(2, len(item['msgs']), 2):
                feedback_msg = item['msgs'][feedback_idx]
                prefix = feedback_msg.split('Error details:')[0]
                prefix_list.append(prefix)
        prefix_dist = Counter(prefix_list)
        eval_res['prefix_dist'] = prefix_dist
        ##################################
        # 反馈次数（每个问题最多算一次）
        prefix_list_by_questoin = []
        for item in pred_data:
            if len(item['msgs']) <= 2:
                continue
            tag_list = item['tags']
            # if "C + M" not in tag_list:
            #     continue
            prefix_set = set()
            for feedback_idx in range(2, len(item['msgs']), 2):
                feedback_msg = item['msgs'][feedback_idx]
                prefix = feedback_msg.split('Error details:')[0]
                prefix_set.add(prefix)
            prefix_list_by_questoin.extend(list(prefix_set))
        prefix_dist = Counter(prefix_list_by_questoin)
        eval_res['prefix_dist_by_question'] = prefix_dist

    metrics = Metrics()
    executable_cnt = 0
    executable_idx_list = []

    for idx, item in enumerate(pred_data):
        qid = item['id']
        level = item['comp_level']
        tag_list = item['tags']
        if target_level is not None and level != target_level:
            continue
        if target_tag is not None:
            if target_tag not in tag_list:
                continue


        pred_answer = item['llm_answer_parsed']
        gold_answer = item['answer']
        #格式转换一下，要求的格式是：都是二维列表[[]]，我们把pred列表里，不是list的元素都套一层list
        pred_answer_new = []
        for ans in pred_answer:
            if not isinstance(ans, list):
                ans = [ans]
            pred_answer_new.append(ans)
        pred_answer = pred_answer_new
        pred_data[idx]['p'] = precision = get_precision(gold_answer, pred_answer, comparator=compare_a_pred_golden_pair) 
        pred_data[idx]['r'] = recall = get_recall(gold_answer, pred_answer, comparator=compare_a_pred_golden_pair)
        pred_data[idx]['f1'] = f1 = get_f1_score_by_pr(precision, recall)
        metrics.add_metric('P', precision)
        metrics.add_metric('R', recall)
        metrics.add_metric('F1', f1)
        metrics.count()

    if stat_exec_cnt:
        print('-----------------------------------')
        eval_res['executable_cnt'] = executable_cnt
        eval_res['executable_rate'] = executable_cnt / len(pred_data)
        print(f'executable_cnt: {executable_cnt}, executable_rate: {executable_cnt / len(pred_data)}')

        shot_idx_frequency = Counter(executable_idx_list)
        print(shot_idx_frequency)
        shot_idx_total_count = sum(shot_idx_frequency.values())
        frequency_percentage = {key: (count / shot_idx_total_count) * 100 for key, count in shot_idx_frequency.items()}
        for key in sorted(frequency_percentage.keys()):
            percentage = frequency_percentage[key]
            print(f"executable_idx = {key}: {percentage:.2f}%")

    print(metrics.get_metrics(['P', 'R', 'F1']))
    eval_res['res'] = metrics.get_metrics(['P', 'R', 'F1'])
    
    # 每个例子的结果
    if dump_results:
        write_json_file(eval_res_path, pred_data)
    return eval_res


def eval_cres_result_directqa(pred_path, eval_res_path, stat_exec_cnt=True):
    print('**********************************************************')
    print(f'Start evaluating {pred_path}')
    eval_total = {}
    eval_total['total'] = eval_cres_result_once_directqa(pred_path, eval_res_path, stat_exec_cnt=stat_exec_cnt, dump_results=True)    
    print('-----------------------------------')
    all_levels = ["iid", "compositional", "zero-shot"]
    for level in all_levels:
        print(f'[{level}]')
        eval_total[level] = eval_cres_result_once_directqa(pred_path, eval_res_path, target_level=level, stat_exec_cnt=False)
    print('-----------------------------------')
    gold_data = read_json_file(pred_path)
    all_tags = set(chain.from_iterable(item['tags'] for item in gold_data))
    for tag in list(all_tags):
        print(f'[{tag}]')
        eval_total[tag] = eval_cres_result_once_directqa(pred_path, pred_path, eval_res_path, target_level='iid', target_tag=tag, stat_exec_cnt=False)
    # 整体结果
    eval_total_path = eval_res_path.replace('eval.json', 'eval_total.json')
    write_json_file(eval_total_path, eval_total)