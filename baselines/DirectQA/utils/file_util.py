import json
import os

def load_json(file_path):
    return json.load(open(file_path))

def dump_json(data, file_path):
    json.dump(data, open(file_path, "w"), ensure_ascii=False, indent=4)

def make_dir(dir):
    if not os.path.exists(dir):
        os.makedirs(dir)

def get_immediate_subdirs(path):
    return [os.path.join(path, name) for name in os.listdir(path) if os.path.isdir(os.path.join(path, name))]

def dump_txt(data, file_path):
    with open(file_path, "w") as f:
        f.write("\n".join(data) + "\n")
