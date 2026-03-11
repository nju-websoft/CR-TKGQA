import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import json
from optparse import OptionParser
from utils.sutime_utils import annotate_datetime
from utils.dataset_utils import dump_json_dataset, load_json_dataset

def parse_args():
    parser = OptionParser()
    parser.add_option('-r',dest="ref_date",type=str,default="2024-12-20",help="reference date")
    parser.add_option('-d',dest="dataset_path",type=str,help="dataset path")
    parser.add_option('-o',dest="output_path",type=str,help="output path")
    opt,args = parser.parse_args()
    return opt,args

if __name__ == "__main__":
    opt,args = parse_args()
    dataset = load_json_dataset(opt.dataset_path)
    for sample in dataset:
        question = sample["question"]
        time_annotates = annotate_datetime(question,opt.ref_date)
        sample["time_annnotates"] = time_annotates
    dump_json_dataset(dataset,opt.output_path)
    
