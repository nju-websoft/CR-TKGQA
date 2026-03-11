import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from transformers import AutoTokenizer, DataCollatorForTokenClassification, AutoModelForTokenClassification, TrainingArguments, Trainer, pipeline, BertTokenizerFast, BertForTokenClassification
from datasets import load_dataset
import evaluate
import numpy
from tqdm import tqdm

from utils.dataset_utils import *
from mark.preprocess_dataset import label2id, label_list, id2label

model_path = 'distilbert/distilbert-base-uncased'
# model_path = '/home2/hyli/models/bert-base-uncased'
tokenizer = AutoTokenizer.from_pretrained(model_path)
data_collator = DataCollatorForTokenClassification(tokenizer=tokenizer)
seqeval = evaluate.load("seqeval")

def tokenize_and_align_labels(examples):
    tokenized_inputs = tokenizer(examples["tokens"], truncation=True, is_split_into_words=True)

    labels = []
    for i, label in enumerate(examples[f"ner_tags"]):
        word_ids = tokenized_inputs.word_ids(batch_index=i)  # Map tokens to their respective word.
        previous_word_idx = None
        label_ids = []
        for word_idx in word_ids:  # Set the special tokens to -100.
            if word_idx is None:
                label_ids.append(-100)
            elif word_idx != previous_word_idx:  # Only label the first token of a given word.
                label_ids.append(label[word_idx])
            else:
                label_ids.append(-100)
            previous_word_idx = word_idx
        labels.append(label_ids)

    tokenized_inputs["labels"] = labels
    return tokenized_inputs

def compute_metrics(p):
    predictions, labels = p
    predictions = numpy.argmax(predictions, axis=2)

    true_predictions = [
        [label_list[p] for (p, l) in zip(prediction, label) if l != -100]
        for prediction, label in zip(predictions, labels)
    ]
    true_labels = [
        [label_list[l] for (p, l) in zip(prediction, label) if l != -100]
        for prediction, label in zip(predictions, labels)
    ]

    results = seqeval.compute(predictions=true_predictions, references=true_labels)
    return {
        "precision": results["overall_precision"],
        "recall": results["overall_recall"],
        "f1": results["overall_f1"],
        "accuracy": results["overall_accuracy"],
    }


def train():
    model = AutoModelForTokenClassification.from_pretrained(
        model_path, num_labels=len(label_list), id2label=id2label, label2id=label2id
    )
    data = load_dataset('json', data_files={'train': 'mark/data/dataset/train/train.json', 'test': 'mark/data/dataset/train/test.json'})
    tokenized_data = data.map(tokenize_and_align_labels, batched=True)
    training_args = TrainingArguments(
        output_dir="mark/data/model",
        learning_rate=2e-5,
        per_device_train_batch_size=256,
        per_device_eval_batch_size=256,
        num_train_epochs=15,
        weight_decay=0.01,
        evaluation_strategy="epoch",
        save_strategy="epoch",
        load_best_model_at_end=True
    )
    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=tokenized_data["train"],
        eval_dataset=tokenized_data["test"],
        # processing_class=tokenizer,
        data_collator=data_collator,
        compute_metrics=compute_metrics
    )
    trainer.train()

def inference(data, classifier):
    for example in tqdm(data):
        example['labels_mark'] = classifier(example['question'])
        for label in example['labels_mark']:
            label['entity'] = str(label['entity'])
            label['score'] = float(label['score'])
            label['index'] = int(label['index'])
            label['word'] = str(label['word'])
            label['start'] = int(label['start'])
            label['end'] = int(label['end'])

def mark(data_paths, best_model_path):
    classifier = pipeline("ner", model=best_model_path, tokenizer=tokenizer)
    for data_path in data_paths:
        data = load_json_dataset(data_path)
        inference(data, classifier)
        dirs = data_path.split('/')
        save_path = '/'.join(dirs[:-1]) + '/labels_mark_large_' + dirs[-1]
        dump_json_dataset(data, save_path)

def mark_main():
    data_paths = [
        'mark/data/dataset/inference/dev.json',
        'mark/data/dataset/inference/test.json',
        'mark/data/dataset/inference/train.json'
    ]
    # mark(data_paths, 'mark/data/model/checkpoint-2128')
    mark(data_paths, 'mark/data/model/timequestions_ner.pkl')

def classify(data: list, best_model_path: str):
    classifier = pipeline("ner", model=best_model_path, tokenizer=tokenizer)
    inference(data, classifier)
    return data

if __name__ == '__main__':
    # mark_main()
    train()