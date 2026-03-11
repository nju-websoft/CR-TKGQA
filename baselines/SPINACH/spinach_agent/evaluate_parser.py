import argparse
import asyncio
import re
import csv
import json
import sys
from multiprocessing import pool
from typing import List, Dict
import debugpy

from chainlite import write_prompt_logs_to_file, get_logger, get_total_cost, llm_generation_chain
from tqdm import tqdm
from collections import OrderedDict

from eval import parallel_f1
from spinach_agent.part_to_whole_parser import PartToWholeParser

sys.path.insert(0, "./")
from spinach_agent.parser_state import SparqlQuery, state_to_dict
from config import USE_LOCAL_ENDPOINT
if not USE_LOCAL_ENDPOINT:
    from wikidata_utils import execute_sparql
else:
    from wikidata_utils_local import execute_sparql, wikidata_local_endpoint

logger = get_logger(__name__)

def calculate_metrics(
    results: List[Dict],
    use_existing_predictions = True,
    gold_sparqls = []
):
    count = 0
    strict_em_count = 0
    f1_score = 0.0
    none_count = 0
    all_outputs = []

    all_predicted_answers = []
    all_gold_answers = []
    
    for idx, entry in tqdm(enumerate(
        results
    ), "prepare evaluation"):
        gold_answer = entry["gold_answer_tuple"]
        predicted_sparql = entry["predicted_sparql"]
        question = entry["question"]
        
        output = {}
        predicted_answer = None
        if use_existing_predictions and set(["predicted_sparql", "predicted_answer_tuple"]).issubset(entry.keys()):
            predicted_answer = entry["predicted_answer_tuple"]
            output["predicted_sparql"] = entry["predicted_sparql"]
            output["predicted_answer_tuple"] = entry["predicted_answer_tuple"]
        
        else:
            if predicted_sparql is None:
                output["predicted_sparql"], output["predicted_answer_tuple"] = None, []
            else:
                if type(predicted_sparql) == str:
                    predicted_sparql = SparqlQuery(predicted_sparql)
                if predicted_sparql.execution_result is None:
                    predicted_sparql.execute()
                predicted_answer = predicted_sparql.execution_result
                output["predicted_sparql"] = predicted_sparql.sparql
                output["predicted_answer_tuple"] = predicted_sparql.execution_result
        
        if output["predicted_sparql"] is None:
            print("Warning: a SPARQL query is recorded as None")

        if not predicted_answer:
            none_count += 1

        all_predicted_answers.append(predicted_answer)
        all_gold_answers.append(gold_answer)

        output["gold_answer_tuple"] = gold_answer
        output["question"] = question
        if gold_sparqls:
            output["gold_sparql"] = gold_sparqls[idx]
        all_outputs.append(output)

        count += 1
            
    # calculate F1s using multiprocessing because of how computation-intensive it is
    all_f1s = parallel_f1(all_predicted_answers, all_gold_answers)
    for output, f1 in zip(all_outputs, all_f1s):
        output["f1"] = f1
        f1_score += f1
        
        if f1 == 1:
            strict_em_count += 1
            

    print(f"\tTotal number of examples with results and hereby evaluated = {len(results)}")
    print(f"\trow-major F1 = {f1_score *100 / count:0.2f}%")
    print(f"\t% of F1 == 1 (row-major EM) = {strict_em_count * 100 / count:.2f}%")
    print(f'\t"None" answers: {none_count * 100 / count:.2f}%')
    print("\tTotal LLM cost: $%.2f" % get_total_cost())

    return all_outputs

 
def _modify_query(query):
    pattern = re.compile(r'SELECT\s+DISTINCT\s+(.*?)\s+WHERE', re.DOTALL)
    match = pattern.search(query)
    if match:
        select_clause = match.group(1)
        labels = re.findall(r'\?(\w+Label)', select_clause)
        needed_vars = []
        for label in labels:
            base_var = '?' + label[:-5]
            if base_var not in select_clause.split():
                needed_vars.append(base_var)
        if needed_vars:
            new_select_clause = ' '.join(needed_vars) + ' ' + select_clause
            query = query.replace(select_clause, new_select_clause)
    return query


async def post_processing(
    model, 
    results,
    regex_use_select_distinct_and_id_not_label = False,
    llm_extract_prediction_if_null = False
):
    extract_sparql = llm_generation_chain(
        template_file="extract_final_sparql.prompt",
        engine=model,
        max_tokens=500,
    )
    for i in tqdm(range(len(results)), desc="post processing"):
        if "final_sparql" in results[i]:
            results[i]['predicted_sparql'] = results[i]["final_sparql"].sparql
        elif len(results[i]["generated_sparqls"]) > 0:
            # if there are generated sparqls, select the last one with results
            past_sparqls_w_results = list(filter(lambda x: x.execution_result is not None, results[i]["generated_sparqls"]))
            if past_sparqls_w_results:
                results[i]['predicted_sparql'] = past_sparqls_w_results[-1].sparql
            else:
                results[i]['predicted_sparql'] = results[i]["generated_sparqls"][-1].sparql
        else:
            results[i]['predicted_sparql'] = None
        
        # LLM extracts a prediction
        if llm_extract_prediction_if_null:
            cond = None
            if not USE_LOCAL_ENDPOINT:
                cond = results[i]['predicted_sparql'] is None or execute_sparql(results[i]['predicted_sparql']) == []
            else:
                cond = results[i]['predicted_sparql'] is None or execute_sparql(results[i]['predicted_sparql'], wikidata_local_endpoint) == []
            if cond:
                final_sparql = await extract_sparql.ainvoke(
                    {
                        "question": results[i]["question"],
                        "trace": results[i]["actions"]
                    }
                )
                final_sparql = final_sparql.strip().replace('```sparql', '')
                final_sparql = final_sparql.replace('```', '')
                results[i]['predicted_sparql'] = final_sparql

        if regex_use_select_distinct_and_id_not_label:
            if results[i]['predicted_sparql'] and 'SELECT DISTINCT' not in results[i]['predicted_sparql'] :
                results[i]['predicted_sparql'] = results[i]['predicted_sparql'].replace('SELECT', 'SELECT DISTINCT')
            
            if results[i]['predicted_sparql']:
                results[i]['predicted_sparql'] = _modify_query(results[i]['predicted_sparql'])

    return results

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=str, required=True)
    parser.add_argument("--output_file", type=str, required=True)
    parser.add_argument(
        "--subsample",
        type=int,
        required=True,
        help="Set to -1 to evaluate the full dataset.",
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Where in the dataset start subsampling.",
    )
    #Modified
    parser.add_argument(
        "--n_threads",
        default=1,
        help="Number of threads.",
    )    
    parser.add_argument("--engine", type=str, required=True)
    parser.add_argument(
        "--parser_type",
        type=str,
        required=True,
        choices=["part_to_whole", "graph_explorer", "fine_tuned"],
    )
    parser.add_argument(
        "--regex_use_select_distinct_and_id_not_label",
        action='store_true',
        help="If enabled, attempts to use regex to use `SELECT DISTINCT` instead of `SELECT`, and try to always include the variable QID instead of the label (i.e., use `x` instead of `xLabel`).",
    )
    parser.add_argument(
        "--llm_extract_prediction_if_null",
        action='store_true',
        help="If enabled, if a reasoning chain ended without any predicted SPARQL, asks a LLM to return a SPARQL. This part is implemented inside `extract_sparql.ainvoke`. This is helpful because for simple queries, LLMs could just use ``get_wikidata_entry'' to get results instead of ever writing a SPARQL.",
    )
    parser.add_argument(
        "--write_csv",
        action='store_true',
        help="If enabled, write a CSV file for the JSON output at the same directory as output_file",
    )    

    parser.add_argument(
        "--external_entity_linking_path",
        type=str,
        default = "None",
        help="If enabled, use the external entity results in the path; 'golden' will use the golden entity linking.",
    )    

    args = parser.parse_args()

    with open(args.dataset) as input_file:
        dataset = json.load(input_file)
    questions = []
    gold_answers = []
    gold_sparqls = []
    # debugpy.listen(5678)  # 默认端口 5678
    # print("等待 VS Code 调试器附加...")
    # debugpy.wait_for_client()  # 暂停执行直到调试器连接
    # print("调试器已附加，开始执行...")
    # #我们修改，使得可以从外部载入实体连接结果
    use_external_el = False
    if args.external_entity_linking_path != "None":
        if args.external_entity_linking_path.lower() == "golden":
            print("使用golden链接结果")
            use_external_el = True
            use_golden_el = True
        else:
            print("使用外部实体链接结果", args.external_entity_linking_path)
            use_external_el = True
            use_golden_el = False
            el_results = json.load(open(args.external_entity_linking_path))
        #整理为：label：entity：desc模式，附加在问题后
    #Modification：we use random to sample.
    if args.subsample >= 0:
        import random
        random.seed(1)
        selected_dataset = dataset[args.offset : ]
        selected_dataset = random.sample(selected_dataset, args.subsample)
    else:
        selected_dataset = dataset[args.offset : ]
    # load the dataset and execute all sparqls
    # if args.subsample >= 0:
    #     selected_dataset = dataset[args.offset : args.offset + args.subsample]
    # else:
    #     selected_dataset = dataset[args.offset : ]
    # Modified
    for d in tqdm(selected_dataset, desc="Loading dataset and executing its gold SPARQLs", position=0, leave=True):
        if not USE_LOCAL_ENDPOINT:
            results = execute_sparql(d["sparql"])
        else:
            results = execute_sparql(d["sparql"], wikidata_local_endpoint)
        if results == []:
            print(f"[WARN] {d['sparql']} has an empty result", 'USING LOCAL ENDPOINT?', USE_LOCAL_ENDPOINT)
            # continue
        #我们采用直接在questions后面加entity link结果的办法吧
        if use_external_el:
            if not use_golden_el:
            #我们默认用问题的id field
                el_result = el_results[d['id']]['llm_entity_selected']
                el_result_str = ",".join([f"{item['qid']} ({item['mention']})" for item in el_result])
                questions.append({"question": d["question"] + " | Entity linking results: " +el_result_str, "conversation_history": []})
            else:
                #直接从数据集读取golden 实体
                el_result = [{"qid": k, "mention": v} for k, v in d['gold_entity_label_map'].items()]
                el_result_str = ",".join([f"{item['qid']} ({item['mention']})" for item in el_result])
                questions.append({"question": d["question"] + " | Entity linking results: " +el_result_str, "conversation_history": []})
        else:
            questions.append({"question": d["question"], "conversation_history": []})
        gold_answers.append(
            results
        ) # we do not convert to string here
        gold_sparqls.append(d["sparql"])
    semantic_parser_class = None
    if args.parser_type == "part_to_whole":
        semantic_parser_class = GraphExplorerParser = PartToWholeParser
    elif args.parser_type == "graph_explorer":
        semantic_parser_class = GraphExplorerParser
    elif args.parser_type == "fine_tuned":
        pass  # TODO add LLaMA baseline
    else:
        raise ValueError("Unknown --args.parser_type", args.parser_type)
    semantic_parser_class.initialize(engine=args.engine, use_external_el=use_external_el)
    try:
        chain_output = semantic_parser_class.run_batch(
            questions,
        )
    finally:
        write_prompt_logs_to_file ()
    sparql_query_results = []
    with open(args.output_file.replace(".json", ".log"), "w") as log_file:
        all_logs = [state_to_dict(qr) for qr in chain_output]
        json.dump(all_logs, log_file, indent=2, ensure_ascii=False, default=vars)
        
        for qr in chain_output:
            sparql_query_results.append(qr.get("final_sparql", None))
    # extract_sparql = llm_generation_chain(
    #     template_file="extract_final_sparql.prompt",
    #     engine=args.engine,
    #     max_tokens=500,
    # )
    results = asyncio.run(post_processing(
        args.engine,
        chain_output,
        regex_use_select_distinct_and_id_not_label=args.regex_use_select_distinct_and_id_not_label,
        llm_extract_prediction_if_null=args.llm_extract_prediction_if_null
    ))

    for i in range(len(gold_answers)):
        results[i].update({
            "gold_answer_tuple": gold_answers[i],
        })
    
    results = calculate_metrics(results)
    
    # sort the result dict
    all_outputs = []
    for i in range(len(gold_sparqls)):
        results[i].update({
            "gold_sparql": gold_sparqls[i],
        })
        all_outputs.append(OrderedDict((key, results[i][key]) for key in [
            "question",
            "predicted_sparql",
            "gold_sparql",
            "f1",
            "predicted_answer_tuple",
            "gold_answer_tuple",
        ]))

    with open(args.output_file, "w") as output_file:
        json.dump(all_outputs, output_file, indent=2, ensure_ascii=False)

    if args.write_csv:
        # Prepare the CSV output
        csv_columns = [
            "question",
            "gold_answer_set",
            "gold_answer_tuple",
            "predicted_sparql",
            "predicted_preverbalized_sparql",
            "predicted_answer_set",
            "predicted_answer_tuple",
            "f1",
            "em",
            "superset",
            "tuple_superset",
            "subset",
            "superset_or_subset",
        ]

        if gold_sparqls:
            csv_columns.insert(1, "gold_sparql")

        with open(args.output_file.replace(".json", ".csv"), "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()

            for output in all_outputs:
                # Write the row to the CSV file
                writer.writerow(output)
