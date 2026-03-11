import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.dataset_utils import dump_json_dataset, load_json_dataset
import random
import pickle

def sample():
    data = load_json_dataset('data/dataset/ResTQA/raw/train.json')
    data = random.sample(data, 70)
    dump_json_dataset(data, 'data/dataset/ResTQA/raw/train.json')
    data = load_json_dataset('data/dataset/ResTQA/raw/dev.json')
    data = random.sample(data, 20)
    dump_json_dataset(data, 'data/dataset/ResTQA/raw/dev.json')
    data = load_json_dataset('data/dataset/ResTQA/raw/test.json')
    data = random.sample(data, 10)
    dump_json_dataset(data, 'data/dataset/ResTQA/raw/test.json')

def id_int():
    data = load_json_dataset('data/dataset/ResTQA/raw/train.json')
    start = 0
    for example in data:
        example['id'] = start
        start += 1
    dump_json_dataset(data, 'data/dataset/ResTQA/raw/train.json')
    data = load_json_dataset('data/dataset/ResTQA/raw/dev.json')
    for example in data:
        example['id'] = start
        start += 1
    dump_json_dataset(data, 'data/dataset/ResTQA/raw/dev.json')
    data = load_json_dataset('data/dataset/ResTQA/raw/test.json')
    for example in data:
        example['id'] = start
        start += 1
    dump_json_dataset(data, 'data/dataset/ResTQA/raw/test.json')

def read_pkl():
    loaded_data = pickle.load(open('data/dataset/ResTQA/run/input/case_cache/path_rank/train/659.pkl', 'rb'))
    print(loaded_data)

def find_0_len():
    load_dir = 'data/dataset/ResTQA/run/input/case_cache/path_rank/train'
    files = os.listdir(load_dir)
    json_files = list()
    for file in files:
        file_dir = os.path.join(load_dir, file)
        if os.path.isfile(file_dir) and file_dir[-4:] == '.pkl':
            json_files.append(file_dir)
    for json_file in json_files:
        data = pickle.load(open(json_file, 'rb'))
        if len(data['graphs_with_score']) == 0:
            print(json_file.split('/')[-1].replace('.pkl', ''))

if __name__ == '__main__':
    # sample()
    # id_int()
    # read_pkl()
    find_0_len()
    