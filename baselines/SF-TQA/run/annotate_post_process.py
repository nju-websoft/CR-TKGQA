import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.dataset_utils import dump_json_dataset, load_json_dataset

# input_path = "data/dataset/TempQuestions/annotation/all_v_0_2.json"
# output_path = "data/dataset/TempQuestions/annotation/all_v_0_3.json"


inverse_relation = {
    "INCLUDES":"IS_INCLUDED",
    "BEGINS":"BEGUN_BY",
    "ENDS":"ENDED_BY",
    "SIMULTANEOUS":"SIMULTANEOUS",
    "AFTER":"BEFORE",
    "BEFORE":"AFTER"
}

def update_rel_types(sample,id2labels):
    simutaneous_words = ["when","during"]
    end_words = ["until","till"]
    if "relations" in sample:
        relations = sample["relations"]
        for relation in relations:
            if relation["relType"] == "INCLUDE":
                relation["relType"] = "INCLUDES"
                if "signal" in relation and (id2labels[relation["signal"]]["mention"].lower() in simutaneous_words):
                    relation["relType"] = "SIMULTANEOUS"
            elif relation["relType"] == "START":
                relation["relType"] = "BEGINS"
                if "signal" in relation and (id2labels[relation["signal"]]["mention"].lower() in end_words):
                    relation["relType"] = "ENDS"
            elif relation["relType"] == "END":
                relation["relType"] = "ENDS"
        
        for relation in relations:
            relation["type"]="TLINK"
            if "property" in relation:
                relation["target"] = relation["property"]
                del relation["property"]
            if id2labels[relation["target"]]["label"] == "E1" and id2labels[relation["relatedTo"]]["label"] == "T1":
                id2labels[relation["target"]]["label"] = "E2"
            if id2labels[relation["relatedTo"]]["start"] < id2labels[relation["target"]]["start"]:
                tmp = relation["target"]
                relation["target"] = relation["relatedTo"]
                relation["relatedTo"] = tmp
                relation["relType"] = inverse_relation[relation["relType"]]
                #print(sample)



def enrich_relations4T2(sample,id2labels,labels):
    question = sample["question"]
    reltype_dict = {
        ("INCLUDES","IS_INCLUDED"):["at","in","on"],
        ("SIMULTANEOUS","SIMULTANEOUS"):["during"],
        ("BEGINS","BEGUN_BY"):["since","from"],
        ("ENDS","ENDED_BY"):["until","till"]
    }

    label_id = len(labels)

    if "relations" in sample:
        relations = sample["relations"]
    else:
        relations = []

    T2_label = None
    for label in labels:
        if label["label"] == "T2":
            T2_label = label
            break
    if T2_label is None:
        # print("T2_label is None")
        return
    
    E_label = None
    for label in labels:
        if label["label"].startswith("E"):
            E_label = label
            break
    if E_label is None:
        # print("E_label is None")
        return

    for label in labels:
        if label["label"] == "T2":
            T2_label = label
            prefix = question[:T2_label["start"]].lower()

            for reltypes,triggers in reltype_dict.items():
                for trigger in triggers:
                    if prefix.endswith(" " + trigger + " ") or prefix == trigger + " ":
                        signal_start = T2_label["start"]-len(trigger)-1
                        signal_end = T2_label["start"]-1
                        signal_label = None
                        for label in labels:
                            if label["start"] == signal_start and label["end"] == signal_end:
                                signal_label = label
                        if signal_label is None:
                            signal_label = {"id":label_id,"label":"S1","start":T2_label["start"]-len(trigger)-1,"end":T2_label["start"]-1,"mention":""}    
                            signal_label["mention"] = question[signal_label["start"]:signal_label["end"]]
                            labels.append(signal_label)

                        if E_label["start"] < T2_label["start"]:
                            target = E_label["id"]
                            related = T2_label["id"]
                            reltype = reltypes[0]
                        else:
                            target = T2_label["id"]
                            related = E_label["id"]
                            reltype = reltypes[1]

                        relation = {"type":"TLINK","target":target,"signal":signal_label["id"],"relatedTo":related,"relType":reltype}
                        relations.append(relation)
                        sample["relations"] = relations
                        # print(sample)

def annotate_IAFTER_IBEFORE(sample,id2labels):
    question = sample["question"]
    single_words = ["was","is"]
    if "relations" in sample:
        relations = sample["relations"]
        for relation in relations:
            if relation["relType"] == "AFTER" or relation["relType"] == "BEFORE":
                signal_label = id2labels[relation["signal"]]
                question = question[:signal_label["start"]]
                for single_word in single_words:
                    if " " + single_word + " " in question:
                        if relation["relType"] == "AFTER":
                            relation["relType"] = "IAFTER"
                        else:
                            relation["relType"] = "IBEFORE"
                            #print(sample)
                        break

def enrich_temporal_relations(data):
    for sample in data:
        if "labels" in sample:
            labels = sample["labels"]
            id2labels = {label["id"]:label for label in labels}
            update_rel_types(sample,id2labels)
            enrich_relations4T2(sample,id2labels,labels)
            annotate_IAFTER_IBEFORE(sample,id2labels)

def annotate_temporal_relations(input_path, output_path):
    dataset = load_json_dataset(input_path)
    for sample in dataset:
        if "labels" in sample:
            labels = sample["labels"]
            id2labels = {label["id"]:label for label in labels}
            update_rel_types(sample,id2labels)
            enrich_relations4T2(sample,id2labels,labels)
            annotate_IAFTER_IBEFORE(sample,id2labels)
    dump_json_dataset(dataset,output_path)
