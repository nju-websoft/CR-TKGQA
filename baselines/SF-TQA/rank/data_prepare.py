from eval.evaluate import Evaluator
from utils.dataset_utils import *
from solve.generate import BasicQueryGenerator, TemporalQueryGenerator
from optparse import OptionParser

from utils.wikidata_utils import wikidata_init

def prepare_main_path_rank(dataset,neg_num):
    generator = BasicQueryGenerator()
    rank_dataset = []
    avg_max_f1 = 0.0
    for i,parsed_question in enumerate(dataset):
        print("question {}:{}".format(i,parsed_question.question))
        graphs = generator.generate_onehop_bone(parsed_question)
        for graph in graphs:
            predicts = graph.get_answers()
            evaluator = Evaluator(predicts,parsed_question.answers)
            graph.score = evaluator.f1
        graphs.sort(key=lambda g:g.score,reverse=True)
        if len(graphs) > 0:
            print("max F1:{}".format(graphs[0].score))
            print("query graph:{}".format(graphs[0].to_label_seq()))
            print("sparql:{}".format(graphs[0].to_sparql()))
            avg_max_f1 += graphs[0].score
        if len(graphs) > 0 and graphs[0].score > 0:
            rank_sample = {"question":parsed_question.question,"positive":(graphs[0].to_label_seq(),graphs[0].score),"negatives":[]}
            neg_cnt = 0
            for i,graph in enumerate(graphs):
                if i > 1 and graph.score < 0.1:
                    rank_sample["negatives"].append((graph.to_label_seq(),graph.score))
                    neg_cnt += 1
                    if neg_cnt == neg_num:
                        break
            rank_dataset.append(rank_sample)
        print()
    if len(dataset) > 0:
        avg_max_f1 /= len(dataset)
    print("avg max F1:{:.4f}".format(avg_max_f1))
    return rank_dataset

def prepare_graph_rank(dataset,neg_num):
    generator = TemporalQueryGenerator()
    rank_dataset = []
    avg_max_f1 = 0.0
    start_no = 0
    for i,parsed_question in enumerate(dataset):
        if i < start_no:
            continue
        print("question {}:{}".format(i,parsed_question.question))
        try:
            graphs = generator.generate(parsed_question)
            for graph in graphs:
                predicts = graph.get_answers()
                evaluator = Evaluator(predicts,parsed_question.answers)
                graph.score = evaluator.f1
            graphs.sort(key=lambda g:g.score,reverse=True)
            if len(graphs) > 0:
                print("max F1:{}".format(graphs[0].score))
                print("query graph:{}".format(graphs[0].to_label_seq()))
                print("sparql:{}".format(graphs[0].to_sparql()))
                avg_max_f1 += graphs[0].score
            if len(graphs) > 0 and graphs[0].score > 0:
                rank_sample = {"question":parsed_question.question,"positive":(graphs[0].to_label_seq(),graphs[0].score),"negatives":[]}
                neg_cnt = 0
                for i,graph in enumerate(graphs):
                    if i > 0 and graph.score < 0.1:
                        rank_sample["negatives"].append((graph.to_label_seq(),graph.score))
                        neg_cnt += 1
                        if neg_cnt == neg_num:
                            break
                rank_dataset.append(rank_sample)
        except Exception as err:
            print(err)
        print()
    if len(dataset) > 0:
        avg_max_f1 /= len(dataset)
    print("avg max F1:{:.4f}".format(avg_max_f1))
    return rank_dataset

def parse_args():
    parser = OptionParser()
    parser.add_option("-t","--task",action='store',type='string',dest='task')
    parser.add_option("-i","--input_path",action='store',type='string',dest='input_path')
    parser.add_option("-o","--output_path",action="store",type="string",dest="output_path")
    parser.add_option("-d","--dataset",action='store',type='string',dest='dataset')
    parser.add_option("-n","--neg_num",action="store",type="int",dest="neg_num",default=9)
    opt,args = parser.parse_args()
    return opt,args

if __name__ == "__main__":
    opt,_ = parse_args()
    wikidata_init(DATASET[opt.dataset])
    if opt.task == "main_path_rank":
       dataset = load_parsed_questions(opt.input_path,DATASET[opt.dataset])
       rank_dataset = prepare_main_path_rank(dataset,opt.neg_num)
       dump_json_dataset(rank_dataset,opt.output_path)
    elif opt.task == "graph_rank":
        dataset = load_parsed_questions(opt.input_path,DATASET[opt.dataset])
        rank_dataset = prepare_graph_rank(dataset,opt.neg_num)
        dump_json_dataset(rank_dataset,opt.output_path)