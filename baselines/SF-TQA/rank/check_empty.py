import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.dataset_utils import load_pickle_dataset

cache_dir = "data/dataset/ResTQA/run/input/case_cache/graph_rank/dev"
if not os.path.exists(cache_dir):
    raise Exception("cache dir does not exist")
sample_files = []
for root,dirs,files in os.walk(cache_dir):
    for file in files:
        if file.endswith(".pkl"):
            sample_files.append(file)
sample_files.sort(key=lambda f:int(f[:-4]))
dataset = []
for file in sample_files:
    file_path = os.path.join(cache_dir,file)
    print(file)
    sample = load_pickle_dataset(file_path)