import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from optparse import OptionParser
import random
from tqdm import tqdm
from utils.dataset_utils import DATASET, dump_json_dataset, load_query_caches
from utils.freebase_utils import freebase_init
from utils.wikidata_utils import wikidata_init
from rank.wikidata_tag import WikidataTagger,WikidataPathRankTagger
from rank.freebase_tag import FreebaseTagger,FreebasePathRankTagger


def prepare_path_rank(dataset,ground_kb,neg_rate):
    train_dataset = []
    if ground_kb == "FREEBASE":
        tagger = FreebasePathRankTagger()
    else:
        tagger = WikidataPathRankTagger()
    for sample in tqdm(dataset):
        question_label = sample["question"]
        graphs_with_score = [(graph_with_score["graph"], graph_with_score["f1"]) for graph_with_score in sample["graphs_with_score"]]
        if len(graphs_with_score) > 0 and graphs_with_score[0][1] > 0:
            positive_num = 0
            for graph,score in graphs_with_score:
                if score > 0:
                    train_dataset.append({"question":question_label,"graph_label":tagger.tag(graph),"label":1})
                    positive_num += 1
            neg_graphs = [graph for graph,score in graphs_with_score if score < 1e-6]
            max_neg_samples = positive_num * neg_rate
            random.shuffle(neg_graphs)
            for i,neg_graph in enumerate(neg_graphs):
                if neg_rate > 0 and i >= max_neg_samples:
                    break
                train_dataset.append({"question":question_label,"graph_label":tagger.tag(neg_graph),"label":0})
    return train_dataset

def prepare_graph_rank(dataset,ground_kb,neg_rate):
    train_dataset = []
    hard_rate = neg_rate // 2

    if ground_kb == "FREEBASE":
        tagger = FreebaseTagger()
    else:
        tagger = WikidataTagger()
        
    for sample in tqdm(dataset):
        question_label = sample["question"]
        graphs_with_score = [(graph_with_score["graph"], graph_with_score["f1"]) for graph_with_score in sample["graphs_with_score"]]
        if len(graphs_with_score) > 0 and graphs_with_score[0][1] > 0:
            pos_score = graphs_with_score[0][1]
            neg_graphs = [graph for graph, score in graphs_with_score if score < 1e-6]
            if len(neg_graphs) == 0:
                continue
            for i, graph_with_score in enumerate(graphs_with_score):
                graph, score = graph_with_score
                if abs(score - pos_score) < 1e-6:
                    train_sample = {"question": question_label, "positive": tagger.tag(graph), "negatives": []}
                    hard_cnt = 0
                    for j in range(i + 1, len(graphs_with_score)):
                        graph2, score2 = graphs_with_score[j]
                        if score2 < score:
                            train_sample["negatives"].append(tagger.tag(graph2))
                            hard_cnt += 1
                            if hard_cnt == hard_rate:
                                break
                    rest_neg_cnt = neg_rate - hard_cnt
                    random.shuffle(neg_graphs)
                    while rest_neg_cnt > 0:
                        for neg_graph in neg_graphs:
                            train_sample["negatives"].append(tagger.tag(neg_graph))
                            rest_neg_cnt -= 1
                            if rest_neg_cnt == 0:
                                break
                    train_dataset.append(train_sample)
    return train_dataset

def parse_args():
    parser = OptionParser()
    parser.add_option("-i","--input_path",action='store',type='string',dest='input_path',default="data/dataset/ResTQA/run/input/case_cache/graph_rank/dev")
    parser.add_option("-o","--output_path",action="store",type="string",dest="output_path",default="data/dataset/ResTQA/run/input/train/graph_rank/dev.json")
    parser.add_option("-t",action="store",type="string",dest="task",default="graph_rank")
    parser.add_option("-g", "--ground_kb", action="store", type="string", dest="ground_kb", default="WIKIDATA")
    parser.add_option("-n",action="store",type="int",dest="neg_rate",default=25)
    opt,args = parser.parse_args()
    return opt,args

if __name__ == "__main__":

    random.seed(2518)

    opt,_ = parse_args()
    dataset = load_query_caches(opt.input_path)

    if opt.ground_kb == "FREEBASE":
        freebase_init(DATASET.TEQ)
    elif opt.ground_kb == "WIKIDATA":
        wikidata_init(DATASET.TQ)

    if opt.task == "path_rank":
        dataset = prepare_path_rank(dataset,opt.ground_kb,opt.neg_rate)
        dump_json_dataset(dataset,opt.output_path)
    elif opt.task == "graph_rank":
        dataset = prepare_graph_rank(dataset,opt.ground_kb,opt.neg_rate)
        dump_json_dataset(dataset,opt.output_path)
