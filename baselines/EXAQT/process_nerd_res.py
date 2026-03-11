from global_utils.file_util import *
from global_utils.metrics import *

train_1_nerd_path = '_intermediate_representations/CResQA/train_1-nerd.json'
train_2_nerd_path = '_intermediate_representations/CResQA/train_2-nerd.json'
train_nerd_path = '_intermediate_representations/CResQA/train-nerd.json'
dev_nerd_path = '_intermediate_representations/CResQA/dev-nerd.json'
test_nerd_path = '_intermediate_representations/CResQA/test-nerd.json'
nerd_clean_dir = '../../dataset/CResQA'


def merge_train_and_filter():
    if not check_file_exists(train_nerd_path):
        data = read_json_file(train_1_nerd_path) + read_json_file(train_2_nerd_path)
        write_json_file(train_nerd_path, data)
    for path in [train_nerd_path, dev_nerd_path, test_nerd_path]:
        data = read_json_file(path)
        new_data = []
        for item in data:
            new_item = item.copy()
            del new_item['Question']
            new_data.append(new_item)
        output_path = os.path.join(nerd_clean_dir, get_file_name(path))
        write_json_file(output_path, new_data)


if __name__ == '__main__':
    # train_1 train_2 合并 复制一份到公共目录
    merge_train_and_filter()
