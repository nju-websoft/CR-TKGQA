import sys 
sys.path.append("/home5/yhbao/git/ComplexTKBQA")
from global_tools.SPARQL_utils import WDExecutor
from global_utils.log import setup_custom_logger
from global_utils.file_util import readjson, savejson
from global_utils.llm import LLMRequestor
import logging
import random
import argparse
import os
from datetime import datetime
import ast

prompt_with_kbinfo = """
You are an expert in answering time-related questions using Wikidata. Given a question, find the answer based on Wikidata KB.
Answer types:
- Entity: Provide as a Python string (e.g., entity label).
- Number: Provide as a Python string, whose value is an int or float.
- Time-point: Provide as a string in "YYYY-MM-DD" format.
- Boolean: Provide as "TRUE" and "FALSE".
Output format:
Use a list for answers. Each element represents one answer.
If an answer has multiple dimensions (e.g., describing different aspects), each element must be a list of those dimensions.
Crucially, your final output must contain exactly one block enclosed by *answer* and *answer*. This block will contain the answer list described above.

You may use Chain of Thought (COT) reasoning in your response. 
However, regardless of the length or complexity of your reasoning, 
the *answer* ... *answer* block must appear exactly once and should contain only the final answer list.

Below are some examples.


Input: (This is a question with a single entity as the answer)
Question: Which motion picture was the first to be nominated for Academy Award for Best Motion Picture after Cries and Whispers?
Entities: Q102427|Academy Award for Best Motion Picture, Q11424|motion picture, Q830874|Cries and Whispers
Relations: P1411|nominated for, P585|point in time
Output:
..............
*answer* [ ["The_Godfather_Part_II"] ] *answer*


Input: (This is a question with a single number as the answer)
How many years combined did Ronaldo Nazário and Miguel Angel Portugal play for RMCF 1902?
Entities: Q2704604|Miguel Angel Portugal, Q529207|Ronaldo Nazário, Q8682|RMCF 1902
Relations: P54|member of sports team, P580|start time, P582|end time, P585|point in time
Output:
..............
*answer* [ ["9"] ] *answer*

Input: (This is a question with a single time point as the answer)
When did Mary Harewood forfeit her noble title as British Princess?
Entities: Q233913|Mary Harewood, Q579431|British Princess
Relations: P582|end time, P585|point in time, P97|noble title
Output:
..............
*answer* [ ["1910-05-06"] ] *answer*

Input: (This is a question with a single boolean as the answer)
Question: In 1896, did Mitchell Hurwitz enroll at the the employer of Herbert Wechsler and the location of Three Way Piece No. 1: Points?
Entities: Q15516042|Herbert Wechsler, Q19759459|Three Way Piece No. 1: Points, Q4992236|Mitchell Hurwitz
Relations: P108|employer, P276|location, P580|start time, P585|point in time, P69|educated at
Output:
..............
*answer* [ ["FALSE"] ] *answer*

Input:  (This is a question with a more than one entity as the answers)
Question: During Horst Sindermann's teenager period, who was the head of government of the states (countries) that follows GB?
Entities: Q174193|GB, Q5|human, Q57896|Horst Sindermann, Q6256|states (countries)
Relations: P155|follows， P569|date of birth， P580|start time， P582|end time，P585|point in time, P6|head of government
Output:
..............
*answer* [ ["Joseph_Lyons"] , ["James_Scullin"] , ["Stanley_Bruce"] ] *answer*


Input: (This is a question with three dimension of answers: entity, time-point, and time-point)
Which ex-the position previously held by John Parker has the longest lifespan after their presidency, and when did they leave office and pass away?
Entities: Q328135|John Parker
Relations: P39|position held， P569|date of birth， P570|date of death， P582|end time， P585|point in time
Output:
..............
*answer* [ ["Jimmie_Davis" , "1948-05-11", "2000-11-05"] ] *answer*
"""

prompt_with_kbinfo_eonly = """
You are an expert in answering time-related questions using Wikidata. Given a question, find the answer based on Wikidata KB.
Answer types:
- Entity: Provide as a Python string (e.g., entity label).
- Number: Provide as a Python string, whose value is an int or float.
- Time-point: Provide as a string in "YYYY-MM-DD" format.
- Boolean: Provide as "TRUE" and "FALSE".
Output format:
Use a list for answers. Each element represents one answer.
If an answer has multiple dimensions (e.g., describing different aspects), each element must be a list of those dimensions.
Crucially, your final output must contain exactly one block enclosed by *answer* and *answer*. This block will contain the answer list described above.

You may use Chain of Thought (COT) reasoning in your response. 
However, regardless of the length or complexity of your reasoning, 
the *answer* ... *answer* block must appear exactly once and should contain only the final answer list.

Below are some examples.


Input: (This is a question with a single entity as the answer)
Question: Which motion picture was the first to be nominated for Academy Award for Best Motion Picture after Cries and Whispers?
Entities: Q102427|Academy Award for Best Motion Picture, Q11424|motion picture, Q830874|Cries and Whispers
Output:
..............
*answer* [ ["The_Godfather_Part_II"] ] *answer*


Input: (This is a question with a single number as the answer)
How many years combined did Ronaldo Nazário and Miguel Angel Portugal play for RMCF 1902?
Entities: Q2704604|Miguel Angel Portugal, Q529207|Ronaldo Nazário, Q8682|RMCF 1902
Relations: P54|member of sports team, P580|start time, P582|end time, P585|point in time
Output:
..............
*answer* [ ["9"] ] *answer*

Input: (This is a question with a single time point as the answer)
When did Mary Harewood forfeit her noble title as British Princess?
Entities: Q233913|Mary Harewood, Q579431|British Princess
Output:
..............
*answer* [ ["1910-05-06"] ] *answer*

Input: (This is a question with a single boolean as the answer)
Question: In 1896, did Mitchell Hurwitz enroll at the the employer of Herbert Wechsler and the location of Three Way Piece No. 1: Points?
Entities: Q15516042|Herbert Wechsler, Q19759459|Three Way Piece No. 1: Points, Q4992236|Mitchell Hurwitz
Output:
..............
*answer* [ ["FALSE"] ] *answer*

Input:  (This is a question with a more than one entity as the answers)
Question: During Horst Sindermann's teenager period, who was the head of government of the states (countries) that follows GB?
Entities: Q174193|GB, Q5|human, Q57896|Horst Sindermann, Q6256|states (countries)
Output:
..............
*answer* [ ["Joseph_Lyons"] , ["James_Scullin"] , ["Stanley_Bruce"] ] *answer*


Input: (This is a question with three dimension of answers: entity, time-point, and time-point)
Which ex-the position previously held by John Parker has the longest lifespan after their presidency, and when did they leave office and pass away?
Entities: Q328135|John Parker
Output:
..............
*answer* [ ["Jimmie_Davis" , "1948-05-11", "2000-11-05"] ] *answer*
"""

prompt_wo_kbinfo = """
You are an expert in answering time-related questions using Wikidata. Given a question, find the answer based on Wikidata KB.
Answer types:
- Entity: Provide as a Python string (e.g., entity label).
- Number: Provide as a Python string, whose value is an int or float.
- Time-point: Provide as a string in "YYYY-MM-DD" format.
- Boolean: Provide as "TRUE" and "FALSE".
Output format:
Use a list for answers. Each element is also a list, representing one answer.
If an answer has multiple fields (e.g., describing different aspects), each element must be a list of those fields.
Crucially, your final output must contain exactly one block enclosed by *answer* and *answer*. This block will contain the answer list described above.

You may use Chain of Thought (COT) reasoning in your response. 
However, regardless of the length or complexity of your reasoning, 
the *answer* ... *answer* block must appear exactly once and should contain only the final answer list.

Below are some examples.


Input: (This is a question with a single entity as the answer)
Question: Which motion picture was the first to be nominated for Academy Award for Best Motion Picture after Cries and Whispers?
Output:
..............
*answer* [ ["The_Godfather_Part_II"] ] *answer*


Input: (This is a question with a single number as the answer)
Question: How many years combined did Ronaldo Nazário and Miguel Angel Portugal play for RMCF 1902?
Output:
..............
*answer* [ ["9"] ] *answer*

Input: (This is a question with a single time point as the answer)
Question: When did Mary Harewood forfeit her noble title as British Princess?
Output:
..............
*answer* [ ["1910-05-06"] ] *answer*

Input: (This is a question with a single boolean as the answer)
Question: In 1896, did Mitchell Hurwitz enroll at the the employer of Herbert Wechsler and the location of Three Way Piece No. 1: Points?
Output:
..............
*answer* [ ["FALSE"] ] *answer*

Input: (This is a question with a more than one entity as the answers)
Question: During Horst Sindermann's teenager period, who was the head of government of the states (countries) that follows GB?
Output:
..............
*answer* [ ["Joseph_Lyons"] , ["James_Scullin"] , ["Stanley_Bruce"] ] *answer*


Input: (This is a question with three dimension of answers: entity, time-point, and time-point)
Question: Which ex-the position previously held by John Parker has the longest lifespan after their presidency, and when did they leave office and pass away?
Output:
..............
*answer* [ ["Jimmie_Davis" , "1948-05-11", "2000-11-05"] ] *answer*
"""



class direct_LLMQA:
    def __init__(self, llm_api, n_threads, ER_setting, llm_param=None, logger=None) -> None:
        self.llm_requestor = LLMRequestor(llm_api, llm_param, n_threads, logger=logger)
        if ER_setting not in ["goldER", "candER", "goldEcandR", "candEgoldR", "noER", 'goldE']:
            raise Exception("unknown ER setting")
        else:
            self.ER_setting = ER_setting
            
            
    def answer_questions(self, question_dict, output_path):
        #call LLM to directly answer a question_list
        def prepare_llm_input(question_dict, ER_setting):
            if ER_setting == "noER":
                base_prompt = prompt_wo_kbinfo
            elif ER_setting == 'goldE':
                base_prompt = prompt_with_kbinfo_eonly
            else:
                base_prompt = prompt_with_kbinfo
            qid2msg_dict = {}
            for k, v in question_dict.items():
                question = v['question']
                entity = None
                relation = None
                if ER_setting == "noER":
                    input = f"\nInput:\nQuestion: {question}"
                else:
                    if ER_setting == "goldER" or ER_setting == "goldEcandR" or ER_setting == 'goldE':
                        entity = v['gold_entity_label_map']
                    else:
                        entity = v['linked_entity_label_map']
                    entity_str = ", ".join([f"{k}|{v}" for k, v in entity.items()])
                    if ER_setting == "goldER" or ER_setting == "candEgoldR":
                        relation = v['gold_relation_label_map']
                    elif ER_setting != 'goldE':
                        relation = v['linked_relation_label_map']
                    if ER_setting == 'goldE':
                        input = f"\nInput:\nQuestion: {question}\nEntities: {entity_str}"
                    else:
                        relation_str = ", ".join([f"{k}|{v}" for k, v in relation.items()])
                        input = f"\nInput:\nQuestion: {question}\nEntities: {entity_str}\nRelations: {relation_str}"
                msgs = [
                    {'role': 'system', 'content': base_prompt},
                    {'role': 'user', 'content': input},
                    ]
                qid2msg_dict[k] = msgs
            return qid2msg_dict
        
        #注意到有的时候会抽风，输出""
        #修改：可以重复运行。每次检测输出为""的，重新跑
        qid2msg_dict = prepare_llm_input(question_dict, self.ER_setting)
        if os.path.isfile(output_path):
            print(f"[INFO] Output file: {output_path} has already existed.")
            completed_data = readjson(output_path)
            blank_results = {k: v for k, v in completed_data.items() if len(v) == 0}
            if len(blank_results) == 0:
                return
            else:
                print(f"[INFO] Find {len(blank_results)} blank results, rerun these questions.")
                qid2msg_dict = {k:v for k, v in qid2msg_dict.items() if k in blank_results}
                messages_list = list(qid2msg_dict.values())
        else:
            messages_list = list(qid2msg_dict.values())
        output = self.llm_requestor.concurrent_chat(messages_list)
        #map output_back_to_qid
        qid2input_dict = {k:v[1]['content'] for k, v in qid2msg_dict.items()}
        input2output_dict = {output_item[1][1]['content']:output_item[0] for output_item in output}
        qid2output_dict = {k:input2output_dict[v] for k,v in qid2input_dict.items()}
        if not os.path.isfile(output_path):
            #还没有输出文件，直接保存输出
            savejson(output_path, qid2output_dict)
        else:
            completed_data = readjson(output_path)
            completed_data.update(qid2output_dict)
            savejson(output_path, completed_data)
         
    

    def parse_outputs(self, question_dict, output_dict):
        """
        extract answers, parse llm output into list
        """
        def extract_between(s, x):
            first_occurrence = s.find(x)
            if first_occurrence == -1:
                return None  # x 不在 s 中
            second_occurrence = s.find(x, first_occurrence + len(x))
            if second_occurrence == -1:
                return None  # x 只出现一次
            return s[first_occurrence + len(x):second_occurrence]
        for k in question_dict:
            question_dict[k]['llm_output'] = output_dict[k]
        for k in question_dict:
            try:
                answer_line = extract_between(question_dict[k]['llm_output'], "*answer*").strip()
                answer_list = ast.literal_eval(answer_line)
                question_dict[k]['llm_answer_parsed'] = answer_list
            except:
                question_dict[k]['llm_answer_parsed'] = ["ERROR"]
        return question_dict
    
    
def get_sampled_data_dict(dataset_path, num, random_seed = 1, key = "id"):
    """
    sample a subset of dataset, and convert to dict
    """
    data = readjson(dataset_path)
    random.seed(random_seed)
    sampled_data = random.sample(data, num)
    sampled_dict = {item[key]:item for item in sampled_data}
    return sampled_dict

def get_data_dict(dataset_path, key, sample_num, random_seed = 1):
    
    """
    sample a subset of dataset, and convert to dict
    """
    data = readjson(dataset_path)
    if sample_num != -1:
        random.seed(random_seed)
        data = random.sample(data, sample_num)
    data_dict = {item[key]:item for item in data}
    return data_dict

def run_directQA_experiment(data_path, output_dir, prefix, ER_setting, llm_api, n_threads = 5, sample_num = -1):
    def clean_data(obj):
        if isinstance(obj, dict):
            return {k: clean_data(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [clean_data(item) for item in obj]
        elif obj is Ellipsis:
            return None 
        else:
            return obj

    result_dir = output_dir + "/" + prefix
    if not os.path.isdir(result_dir):
        os.mkdir(result_dir)
    result_file_path = f"{result_dir}/{llm_api}_{ER_setting}_results.json"
    # print(f"[INFO]Running directQA using {ER_setting}, {llm_api} for {data_path}")
    output_parsed_path = result_file_path.split(".")[0] + "_parsed.json"
    data_dict = get_data_dict(data_path, key = "id", sample_num = sample_num)
    directQA = direct_LLMQA(llm_api=llm_api, n_threads = n_threads, ER_setting=ER_setting, logger=None)
    # savejson(path=output_parsed_path, data = [])    #总是先测试能不能打开路径
    # directQA.answer_questions(data_dict, result_file_path)
    output_dict = readjson(result_file_path)
    #final output needs to be a list (according to the implementation of the metric function)
    parsed_output_dict = list(directQA.parse_outputs(data_dict, output_dict).values())
    parsed_output_dict = clean_data(parsed_output_dict)
    savejson(path=output_parsed_path, data = parsed_output_dict)
    


if __name__ == "__main__":
    pass
    # sample_num = 200
    # llm_api = "gpt4omini"
    # for setting in ['noER']:
    #     ER_setting = setting
    #     for dataset_split in ["dataset/CResQA/seeds.json", "dataset/CResQA/generalizations.json", "dataset/CResQA/nontempcomp.json", "dataset/CResQA/tempcomp.json"]:
    #         set_name = dataset_split.split("/")[-1].split(".")[0]
    #         data_path = dataset_split
    #         output_path = f"output/directqa/{set_name}_{str(sample_num)}_{llm_api}_{setting}.json"
    #         run_directQA_experiment(data_path, output_path, ER_setting, llm_api=llm_api, n_threads=10, sample_num=sample_num)