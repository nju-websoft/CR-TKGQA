import json

def load_json(path: str):
    return json.load(open(path, 'r'))

def main(method: str):
    data = load_json('analysis_results/CR-TKGQA/complexity_taxonomy.json')
    methods = load_json('baselines/results/collect.json')
    id2methods = {example['id'] : example for example in methods}

    w_comp2id = dict()
    wo_comp2id = dict()
    w_comp2id['temporal_fact_fusion'] = list()
    wo_comp2id['temporal_fact_fusion'] = list()
    for example in data:
        if 'temporal_fact_fusion' not in example['analysis'].keys():
            wo_comp2id['temporal_fact_fusion'].append(example['id'])
        example['analysis']['granularity_conversion'] = set(example['analysis']['granularity_conversion']).difference(set(example['analysis']['timepoint_comparision']))
        for key in example['analysis'].keys():
            if key == 'fact_counting':
                continue
            elif key == 'temporal_fact_fusion':
                w_comp2id[key].append(example['id'])
            elif key == 'multi_hop_reasoning':
                if key not in w_comp2id.keys():
                    w_comp2id[key] = list()
                    wo_comp2id[key] = list()
                if example['analysis'][key][0] == 0:
                    wo_comp2id[key].append(example['id'])
                else:
                    w_comp2id[key].append(example['id'])
            else:
                if key not in w_comp2id.keys():
                    w_comp2id[key] = list()
                    wo_comp2id[key] = list()
                if len(example['analysis'][key]) > 0:
                    w_comp2id[key].append(example['id'])
                else:
                    wo_comp2id[key].append(example['id'])

    w_f1 = dict()
    w_count = dict()
    wo_f1 = dict()
    wo_count = dict()

    for key in w_comp2id.keys():
        if key not in w_f1.keys():
            w_f1[key] = 0.0
            w_count[key] =  0
        for id in w_comp2id[key]:
            if id not in id2methods.keys():
                continue
            example = id2methods[id]
            w_f1[key] += example['methods'][method]['evaluate']['f1']
            w_count[key] += 1
    for key in wo_comp2id.keys():
        if key not in wo_f1.keys():
            wo_f1[key] = 0.0
            wo_count[key] = 0
        for id in wo_comp2id[key]:
            if id not in id2methods.keys():
                continue
            example = id2methods[id]
            wo_f1[key] += example['methods'][method]['evaluate']['f1']
            wo_count[key] += 1

    w_f1['temporal_statistic'] += w_f1['frequency']
    w_count['temporal_statistic'] += w_count['frequency']
    wo_f1['temporal_statistic'] += wo_f1['frequency']
    wo_count['temporal_statistic'] += wo_count['frequency']
    w_f1.pop('frequency')
    w_count.pop('frequency')
    wo_f1.pop('frequency')
    wo_count.pop('frequency')
    w_comp2id.pop('frequency')
    wo_comp2id.pop('frequency')

    for key in ['temporal_fact_fusion','multi_hop_reasoning','timepoint_comparision','duration_comparison','duration_derivation','duration_calculation','timepoint_shift','granularity_conversion','timepoint_ordinal','duration_ordinal','temporal_statistic']:
        print(f'w: {round(w_f1[key] / w_count[key] if w_count[key] > 0 else 0.0, 4)}\t{w_count[key]}\t\two: {round(wo_f1[key] / wo_count[key] if wo_count[key] > 0 else 0.0, 4)}\t{wo_count[key]}\t\t{key}')

if __name__ == "__main__":
    main('SPINACH')
    print("\n\n\n\n\n\n")
    main('DirectQA')
    print("\n\n\n\n\n\n")
    main('RTQA')