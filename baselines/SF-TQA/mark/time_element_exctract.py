import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import json
import torch
import random
from tqdm import tqdm
from transformers import BertTokenizer, BertModel, BertConfig
import numpy as np
from transformers import BertTokenizerFast
import os
from transformers import BertForTokenClassification

def get_match_span(tokens,raw):
    res = []
    tokens_keep = ""
    label = raw[0][1]
    for index,i in enumerate(raw):
        token = i[0]
        if(tokens_keep == tokens):
            return res,label
        if(token in tokens):
            res.append(index)
            tokens_keep += token
        else:
            return res,label
    return res,label

def match_tokens_with_tokens(res,question_tokens):
    new_res = []
    for i in question_tokens:
        try:
            span,label = get_match_span(i,res)
        except:
            print(question_tokens)
            break
        tokens = ""
        for j in span:
            tokens += res[j][0]
        if(tokens==i):
            for j in span:
                del res[0]
        new_res.append([tokens,label])
    return new_res



GPU_NUM = 0
special_char = [',' ,'.' ,'\'','#']
class DataGen(torch.utils.data.Dataset):
    def __init__(self,encoding_question):
        self.encoding = encoding_question

class ner_model:
    def __init__(self):
        # self.model_name = 'bert-base-uncased'
        self.model_name = '/home2/hyli/models/bert-base-uncased'
        # self.save_dir = './model/cronquestions_ner.pkl'
        #self.save_dir = './model/NEW_timequestions_ner.pkl'
        self.save_dir = 'mark/data/model/timequestions_ner.pkl'
        # self.vocabulary = {'B': 0, 'I': 1, 'O': 2}
        self.conjunc2label = {'O': 0, 'B-T1': 1, 'B-T2': 2, 'B-E1': 3, 'I-E1': 4, 'B-E2': 5, 'I-E2': 6, 'B-S1': 7, 'I-S1': 8,
                              'B-S2': 9, 'I-T2':10, 'I-T1':11, 'I-S2':12, "B-P":13, "I-P":14}
        #self.conjunc2label = {'O': 0, 'B-T1': 1, 'B-T2': 2, 'B-E1': 3, 'I-E1': 3, 'B-E2': 4, 'I-E2': 4, 'B-S1': 5,
        #                      'I-S1': 5,
        #                     'B-S2': 6, 'I-T2': 2, 'I-T1': 1, 'I-S2': 6, "B-P": 7, "I-P": 7}
        self.n_class = len(self.conjunc2label)
        self.label2conjunc = {v: k for k, v in self.conjunc2label.items()}
        #self.label2conjunc = {0 : 'O', 1 : 'T1', 2: 'T2', 3: 'E1', 4 : 'E2', 5: 'S1',
        #                     6 : 'S2', 7 : "P"}
        self.tokenizer = BertTokenizerFast.from_pretrained(self.model_name)
        self.bert_config = BertConfig.from_pretrained(self.model_name)
        self.model = BertForTokenClassification.from_pretrained(self.model_name, num_labels=self.n_class)
        # self.model = lightseq.Transformer('../../model/conjunction.pb',self.n_class)
        # self.device = torch.device("cuda:" + str(GPU_NUM)) if torch.cuda.is_available() else 'cpu'
        self.device = torch.device("cuda") if torch.cuda.is_available() else 'cpu'
        print('time element extract model device', self.device)
        self.GPU_NUM = GPU_NUM
        os.environ["CUDA_VISIBLE_DEVICES"] = "0"

        self.max_length = 64
        self.batch_size = 256
        self.num_train_epochs = 1
        self.get_dataset()
        self.load_model()
        # self.train_model()
        # self.get_predict_for_test()
        # self.eval("./CronQuestions/test_bio.txt")

    def get_span_label(self,span_info,labels):
        start = span_info.start
        if(labels[start]!= ' '):
            return labels[start]
        else:
            return labels[start+1]

    def get_label(self, encoding, char_labels):
        bio = [self.conjunc2label['O']] * self.max_length
        tokens = encoding.encodings[0].tokens
        l = len([i for i in tokens if i not in ['[CLS]','[SEP]','[PAD]']])
        for i in range(1,l+1):
            bio[i] = self.get_span_label(encoding.token_to_chars(i), char_labels)
        return bio

    def get_encoding(self, tokens, char_labels = None,is_test=False):
        question = " ".join(tokens)
        encoding = self.tokenizer(question, max_length=self.max_length, padding='max_length',
                                  truncation=True)
        if is_test != True:
            encoding['labels'] = torch.tensor(self.get_label(encoding, char_labels)).long()
        for key in encoding.keys():
            encoding[key] = torch.tensor(encoding[key]).long()
        return encoding

    def get_data(self,path):
        res = []
        with open(path,"r",encoding='utf-8') as f:
            tokens,labels,char_labels = [],[],[]
            for line in f.readlines():
                if(len(line.strip().split("\t"))<2):
                    encoding = self.get_encoding(tokens,char_labels)
                    res.append(encoding)
                    tokens,labels,char_labels = [],[],[]
                else:
                    token = line.strip().split("\t")[0]
                    tokens.append(token)
                    label = self.conjunc2label[line.strip().split("\t")[1]]
                    for i in list(token):
                        char_labels.append(label)
                    char_labels.append(' ')
                    labels.append(label)
        return res

    def get_dataset(self):
        # TODO 检查即可
        # 读取标注文件，train test split
        print('Load training data...')

        # self.train_encoding = self.get_data("./CronQuestions/dev_bio.txt")
        self.train_encoding = self.get_data("mark/data/annotate/tlink_timequestion_train_result.txt")
        # self.train_dataset = DataGen(self.train_encoding)
        self.train_dataloader = torch.utils.data.DataLoader(self.train_encoding, batch_size=self.batch_size)
        # self.dev_encoding = self.get_data("./CronQuestions/dev_1_bio.txt")
        self.dev_encoding = self.get_data("mark/data/annotate/tlink_timequestion_dev_result.txt")
        # self.dev_dataset = DataGen(self.dev_encoding)
        self.dev_dataloader = torch.utils.data.DataLoader(self.dev_encoding, batch_size=self.batch_size)
        # self.test_encoding = self.get_data("./CronQuestions/dev_2_bio.txt")
        self.test_encoding = self.get_data("mark/data/annotate/tlink_timequestion_test_result.txt")
        # self.test_dataset = DataGen(self.test_encoding)
        self.test_dataloader = torch.utils.data.DataLoader(self.test_encoding, batch_size=self.batch_size)

        print('Data completed ! ')

    def do_eval(self, eval_dataloader):
        eval_loss_sum = 0.0
        eval_accu = 0
        eval_num = 0
        self.model.eval()
        for eval_step, eval_encoding in enumerate(eval_dataloader):
            eval_token_ids = eval_encoding['input_ids'].to(self.device)
            eval_labels = eval_encoding['labels'].to(self.device)
            eval_attention_mask = eval_encoding['attention_mask'].to(self.device)
            with torch.no_grad():
                eval_out = self.model(input_ids=eval_token_ids, attention_mask=eval_attention_mask,
                                      labels=eval_labels)  # output
                eval_logits = eval_out.logits
                eval_loss = eval_out.loss
                eval_loss_sum += eval_loss.cpu().data.numpy()

                # calculate active accuracy
                eval_active_loss = eval_attention_mask.view(-1) == 1
                eval_labels = eval_labels.view(-1)[eval_active_loss]
                eval_logits = eval_logits.view(-1, self.n_class)[eval_active_loss]
                eval_num += eval_labels.shape[0]
                # calculate after mask
                eval_accu += (eval_logits.argmax(1) ==
                              eval_labels).sum().cpu().data.numpy()

        eval_accu = eval_accu / eval_num
        eval_loss = eval_loss / eval_num
        return {'loss': eval_loss, 'acc': eval_accu}

    def train_model(self, num_epoch=-1):
        if num_epoch != -1:
            self.num_train_epochs = num_epoch
        self.dev_best_matrix = self.do_eval(self.dev_dataloader)
        print('last best:', self.dev_best_matrix)
        print('Start training')
        optimizer = torch.optim.Adam(self.model.parameters(), lr=1e-5, betas=(0.5, 0.999), weight_decay=0.01)
        # optimizer = torch.optim.Adam(self.model.parameters(), lr=1e-4, betas=(0.5, 0.999), weight_decay=0.01)
        # optimizer = torch.optim.SGD(self.model.parameters(), lr=0.001, momentum=0.9, weight_decay=1e-4)

        for epoch in range(self.num_train_epochs):
            loss_sum = 0.0
            accu = 0
            train_num = 0
            self.model.train()
            for step, encoding in enumerate(self.train_dataloader):
                token_ids = encoding['input_ids'].to(self.device)
                labels = encoding['labels'].to(self.device)
                attention_mask = encoding['attention_mask'].to(self.device)
                out = self.model(input_ids=token_ids, attention_mask=attention_mask,labels=labels)  # output
                logits = out.logits
                loss = out.loss
                optimizer.zero_grad()
                loss.backward()
                optimizer.step()
                loss_sum += loss.cpu().data.numpy()

                # calculate active accuracy
                active_loss = attention_mask.view(-1) == 1
                labels = labels.view(-1)[active_loss]
                logits = logits.view(-1, self.n_class)[active_loss]
                train_num += labels.shape[0]
                # calculate after mask
                accu += (logits.argmax(1) == labels).sum().cpu().data.numpy()
            loss_sum = loss_sum / train_num
            accu = accu / train_num
            dev_matrix = self.do_eval(self.dev_dataloader)
            test_matrix = self.do_eval(self.test_dataloader)
            print("epoch % d,train loss:%f,acc:%f,dev loss:%f, acc:%f,test loss:%f, acc:%f" % (
                epoch, loss_sum, accu, dev_matrix['loss'], dev_matrix['acc'], test_matrix['loss'], test_matrix['acc']))

            if dev_matrix['acc'] > self.dev_best_matrix['acc']:
                torch.save(self.model.state_dict(), self.save_dir)
                print('Best model on dev set saved! acc increase:', dev_matrix['acc'] - self.dev_best_matrix['acc'])
                self.dev_best_matrix = dev_matrix

    def predict(self,question):
        question = question.lower().strip()
        encoding = self.get_encoding(tokens=question.split(" "), is_test=True)
        out = self.model(input_ids=encoding['input_ids'].unsqueeze(0).to(self.device),
                         attention_mask=encoding['attention_mask'].unsqueeze(0).to(self.device))

        pred = out[0][0].argmax(1)
        attention_mask = encoding['attention_mask']
        active = attention_mask.view(-1) == 1
        pred = pred.view(-1)[active]
        pred = list(pred.cpu().data.numpy())  # 不要CLS和SEP

        res = []
        # res = []
        question = question.split(" ")
        for token_index, pre_pos in enumerate(pred):
            if token_index == 0:
                continue
            elif token_index == len(pred) - 1:
                break
            token = " ".join(question)[encoding.token_to_chars(token_index)[0]:encoding.token_to_chars(token_index)[1]]
            res.append([token, self.label2conjunc[pre_pos]])
        res = match_tokens_with_tokens(res, question)
        # for token_index,pre_pos in enumerate(pred):
        #     if token_index==0:
        #         continue
        #     elif token_index == len(pred)-1:
        #         break
        #     token = question[encoding.token_to_chars(token_index)[0]:encoding.token_to_chars(token_index)[1]]
        #     res.append([token,self.label2conjunc[pre_pos]])
        return res

    def load_model(self):
        if os.path.exists(self.save_dir):
            self.model.load_state_dict(torch.load(self.save_dir))
            self.model.to(self.device)
            print('Successfully load exist model! ', self.save_dir)
        else:
            print('No model exist, training new model!')
            self.model.to(self.device)
            self.train_model(25)

    def eval(self,path):
        encoding = self.get_data(path)
        dataloader = torch.utils.data.DataLoader(encoding, batch_size=self.batch_size)
        matrix = self.do_eval(dataloader)
        print("test loss:%f, acc:%f" % (
            matrix['loss'], matrix['acc']))


    def get_predict_for_test(self):
        pred_res = []
        question_list = []
        with open("TimeQuestions/bio_test_for_train.txt", "r", encoding="utf-8") as f:
            tokens = []
            for line in f.readlines():
                if (len(line.strip().split("\t")) < 2):
                    question_list.append(tokens)
                    tokens = []
                else:
                    token = line.strip().split("\t")[0]
                    tokens.append(token)
        for question,encoding in zip(question_list,self.test_encoding):
            out = self.model(input_ids=encoding['input_ids'].unsqueeze(0).to(self.device),
                             attention_mask=encoding['attention_mask'].unsqueeze(0).to(self.device))
            pred = out[0][0].argmax(1)
            attention_mask = encoding['attention_mask']
            active = attention_mask.view(-1) == 1
            pred = pred.view(-1)[active]
            pred = list(pred.cpu().data.numpy())  # 不要CLS和SEP

            res = []
            for token_index, pre_pos in enumerate(pred):
                if token_index == 0:
                    continue
                elif token_index == len(pred) - 1:
                    break
                token = " ".join(question)[encoding.token_to_chars(token_index)[0]:encoding.token_to_chars(token_index)[1]]
                res.append([token, self.label2conjunc[pre_pos]])
            res = match_tokens_with_tokens(res,question)
            pred_res.append(res)
        with open("./res/bio_test_pred.txt","w+",encoding="utf-8") as f:
            for item in pred_res:
                for tuple in item:
                    f.write(tuple[0]+"\t"+tuple[1]+"\n")
                f.write("\n")

if __name__ == "__main__":
    a = ner_model()
    question = "what was disneys first color movie"
    print(a.predict(question))
    # question_list = []
    # pred_res = []
    # path = "./WebQSP/WebQSP.train.json"
    # json_list = []
    # with open(path,"r",encoding="utf-8") as f:
    #     data = json.load(f)
    # for i in data["Questions"]:
    #     question_list.append(i["ProcessedQuestion"])
    # for i in question_list:
    #     pred_res.append(a.predict(i))
    # with open("./res/bio_webqsp_train_res.txt", "w+", encoding="utf-8") as f:
    #     for item in pred_res:
    #         for tuple in item:
    #             f.write(tuple[0] + "\t" + tuple[1] + "\n")
    #         f.write("\n")