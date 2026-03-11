import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import torch
from transformers import AutoModelForTokenClassification, AutoTokenizer

def load_model():
    model_path = 'mark/data/model/timequestions_ner.pkl'
    model = torch.load(model_path)
    # 加载分词器，这里假设你的分词器是基于transformers库的
    tokenizer_path = '/home2/hyli/models/bert-base-uncased'  # 替换为你的分词器路径
    tokenizer = AutoTokenizer.from_pretrained(tokenizer_path)
    return model, tokenizer

def predict():
    model, tokenizer = load_model()
    example = {
    "question": "Hugging Face is a company based in New York."
    }
    # 对输入文本进行分词和编码
    inputs = tokenizer(example['question'], return_tensors='pt')
    # 确保模型在正确的设备上
    # device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    device = 'cpu'
    model = model.to(device)
    inputs = inputs.to(device)

    # 设置为评估模式
    model.eval()

    # 不计算梯度
    with torch.no_grad():
        outputs = model(**inputs)

    # 获取预测结果
    predictions = torch.argmax(outputs.logits, dim=2)

    # 解析预测结果
    labels = predictions.cpu().numpy()
    entities = []
    for i, label in enumerate(labels):
        if label != -100:  # -100 是 tokenizer.pad_token_id，表示忽略的标记
            entity = {
                'word': inputs.tokens[i],
                'score': torch.nn.functional.softmax(outputs.logits[i], dim=-1)[label].item(),
                'entity': model.config.id2label[label]
            }
            entities.append(entity)

    # 将解析后的实体添加到example字典中
    example['labels_mark'] = entities
    print(example['labels_mark'])

if __name__ == '__main__':
    # predict()
    torch.cuda.empty_cache()

