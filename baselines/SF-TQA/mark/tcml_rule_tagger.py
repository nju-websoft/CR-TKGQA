import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import re
from utils.nlp_utils import nlp
from utils.sutime_utils import annotate_datetime

wh_triggers = ["what day and year","what day of the week","what day",\
    "which year","which years","which date","which dates","when","what year",\
        "what years","what date","what dates","which period","how many years","what other years",\
        "what time"]

wh_time_words = ["time","date","last year","year","day","period","period of time","dates","days","years"]

wh_begin_relation_words = ["since","from","after"]
wh_end_relation_words = ["until","till","to","before","up to"]
wh_simul_relation_words = ["on","at","in","during"]
wh_relation2triggers = {
    "BEGIN":["since","from","after"],
    "END":["until","till","to","before","up to"],
    "SIMULTANEOUS":["on","at","in","during"]
}

relation2triggers = {
    "BEGIN":["start time is","from","since"],
    "END":["until","till","end time is","up to","end time of"],
    "AFTER":["after"],
    "BEFORE":["before"],
    "INCLUDE":["in","at","on","during"],
    "SIMULTANEOUS":["while","when","point in time is"]    
}

trigger2relation = {
    "start time is":"BEGIN",
    "start time of":"BEGIN",
    "from":"BEGIN",
    "since":"BEGIN",
    "until":"END",
    "til":"END",
    "end time is":"END",
    "end time of":"END",
    "up to":"END",
    "after":"AFTER",
    "before":"BEFORE",
    "in":"INCLUDE",
    "at":"INCLUDE",
    "on":"INCLUDE",
    "during":"INCLUDE",
    "while":"SIMULTANEOUS",
    "when":"SIMULTANEOUS",
    "at point in time of":"SIMULTANEOUS",
    "point in time is":"SIMULTANEOUS"
}

wh_trigger2relation = {
    "from":"BEGIN",
    "since":"BEGIN",
    "start":"BEGIN",
    "until":"END",
    "til":"END",
    "end time is":"END",
    "up to":"END",
    "end":"END",
    "in":"SIMULTANEOUS",
    "at":"SIMULTANEOUS",
    "on":"SIMULTANEOUS",
    "during":"SIMULTANEOUS",
    "when":"SIMULTANEOUS",
    "while":"SIMULTANEOUS"
}

certainly_relation_triggers = ["since","until","till","during","while","when"]

filter_events = ["series ordinal","statement"]

es_labels = ["population","rate","gdp"]

current_words = ["now","right now","present","presently","current","currently"]

def annotate_events(question,parsed_question,entities,events):
    event_labels = []
    #找所有谓词作为事件
    i = 0
    aux_events = []
    while i < len(parsed_question):
        token = parsed_question[i]
        if token.pos_ == "AUX":
            j = i+1
            if j == len(parsed_question):
                i = j
                continue
            event_start = token.idx
            event_end = token.idx + len(token)
            if token.text in ["did","do","does"]:
                i = j
                continue
            while j < len(parsed_question):
                if parsed_question[j].pos_ == "ADJ":
                    event_start = parsed_question[j].idx
                    event_end = parsed_question[j].idx + len(parsed_question[j])
                elif parsed_question[j].pos_ == "VERB":
                    event_start = parsed_question[j].idx
                    event_end = parsed_question[j].idx + len(parsed_question[j])
                    j += 1
                    break
                elif parsed_question[j].pos_ == "NOUN":
                    event_start = parsed_question[j].idx
                    event_end = parsed_question[j].idx + len(parsed_question[j])
                    j += 1
                    k = j
                    is_find_word = False
                    while k < len(parsed_question):
                        if parsed_question[k].pos_ == "NOUN":
                            event_end = parsed_question[k].idx + len(parsed_question[k])
                            is_find_word = True
                            k += 1
                        elif parsed_question[k].pos_ == "VERB":
                            event_start = parsed_question[k].idx
                            event_end = parsed_question[k].idx + len(parsed_question[k])
                            is_find_word = True
                            k+= 1
                            break
                        else:
                            break
                    if is_find_word:
                        j = k
                    break
                if parsed_question[j].text in ["after","before","during","when","to"]:
                    j += 1
                    break
                j+=1
            if j == i + 1:
                continue
            event_label = {"id":-1,"label":"EO","start":event_start,"end":event_end,"mention":question[event_start:event_end]}
            event_labels.append(event_label)
            if parsed_question[i].text in ["has","had","have","having",\
                "is","was","were"]:
                aux_events.append(event_label)
            i=j
        elif token.pos_ == "VERB":
            if i == 0 and token.text in ["tell","give","list"]:
                i += 1
                continue
            event_start = token.idx
            event_end = token.idx + len(token)
            if token.text in ["has","had","have","having",\
                "is","was","were","did","does","do"]:
                j = i + 1
                while j < len(parsed_question):
                    if parsed_question[j].pos_ == "ADJ":
                        event_start = parsed_question[j].idx
                        event_end = parsed_question[j].idx + len(parsed_question[j])
                    elif parsed_question[j].pos_ == "VERB":
                        event_start = parsed_question[j].idx
                        event_end = parsed_question[j].idx + len(parsed_question[j])
                        j+=1
                        break
                    elif parsed_question[j].pos_ == "NOUN":
                        event_start = parsed_question[j].idx
                        event_end = parsed_question[j].idx + len(parsed_question[j])
                        j += 1
                        k = j
                        is_find_word = False
                        while k < len(parsed_question):
                            if parsed_question[k].pos_ == "NOUN":
                                event_end = parsed_question[k].idx + len(parsed_question[k])
                                is_find_word = True
                                k += 1
                            elif parsed_question[k].pos_ == "VERB":
                                event_start = parsed_question[k].idx
                                event_end = parsed_question[k].idx + len(parsed_question[k])
                                is_find_word = True
                                k += 1
                                break
                            else:
                                break
                        if is_find_word:
                            j = k
                        break
                    if parsed_question[j].text in ["after","before","during","when","to"]:
                        j += 1
                        break
                    j += 1
                i = j
            else:
                i += 1
                if i < len(parsed_question) and parsed_question[i].pos_ == "VERB":
                    i += 1
            event_label = {"id":-1,"label":"EO","start":event_start,"end":event_end,"mention":question[event_start:event_end]}
            if (event_label["mention"] == "start" or event_label["mention"] == "end") and question.find(event_label["mention"] + " time") != -1:
                continue
            event_labels.append(event_label)
        else:
            i += 1
    
    #基于NER识别命名事件
    for event in events:
        start = question.find(event)
        end = start + len(event)
        event_label = {"id":-1,"label":"EO","start":start,"end":end,"mention":event}
        event_labels.append(event_label)

    #名词性非命名事件识别
    if len(event_labels) == 0:
        for token in parsed_question:
            if token.pos_ == "NOUN":
                event_start = token.idx
                event_end = event_start + len(token)
                for token2 in parsed_question:
                    if token2.idx >= token.idx:
                        if token2.pos_ == "NOUN":
                            event_end = token2.idx + len(token2)
                        else:
                            break
                event_label = {"id":-1,"label":"EO","start":event_start,"end":event_end,"mention":question[event_start:event_end]}
                event_labels.append(event_label)
                break    
    remove_labels = []
    
    #过滤错误识别的时间
    for label in event_labels:
        for wh_time_word in wh_time_words:
            regex = r"\b(is|was|were)( the)?( (?P<filter_event>start|end|point|last))?( in)?( " + wh_time_word + ")?"
            match = re.search(regex,question)
            if match and label["start"] == match.start("filter_event") and label["end"] == match.end("filter_event") and match.group() not in ["is","was","were"]:
                remove_labels.append(label)
                break
            elif label["mention"] == wh_time_word:
                remove_labels.append(label)
            elif label["mention"].endswith(" " + wh_time_word):
                label["end"] -= len(wh_time_word) + 1
                label["mention"] = question[label["start"]:label["end"]]
    
    for label in event_labels:
        if label["mention"] in filter_events:
            remove_labels.append(label)
        for event in events:
            if event in label["mention"] and event != label["mention"]:
                remove_labels.append(label)
                break
    
    for label1 in event_labels:
        for label2 in event_labels:
            if label1 not in remove_labels and label2 not in remove_labels:
                if label1 != label2 and label1["start"] >= label2["start"] \
                    and label1["end"] <= label2["end"]:
                    remove_labels.append(label1)

    for label in remove_labels:
        if label in event_labels:
            event_labels.remove(label)
    
    #更正事件类型
    for label in event_labels:
        for trigger in es_labels:
            regex = r"\b"+trigger+r"\b"
            if re.search(regex,label["mention"]):
                label["label"]="ES"

    return event_labels,aux_events

def annotate_ordinal_labels(question,entities,parsed_questions):
    labels = []
    for entity in parsed_questions.ents:
        if entity.label_ == "ORDINAL":
            label = {}
            label["id"] = -1
            label["label"] = "SO"
            label["start"] = entity.start_char
            label["end"] = entity.end_char
            label["mention"] = question[label["start"]:label["end"]]
            '''
            keep = True
            for entity2 in entities:
                if label["mention"] in entity2:
                    keep = False
            if keep:
                labels.append(label)
            '''
            labels.append(label)
    #特定触发词
    question_tokens = question.split()
    first_words = ["first","firstly","earliest","youngest"]
    for first_word in first_words:
        if first_word in question_tokens:
            start = question.find(first_word)
            end = start + len(first_word)
            is_added = False
            for label in labels:
                if label["start"] == start and label["end"] == end:
                    is_added = True
                    break
            if not is_added:
                label = {"id":-1,"label":"SO","start":start,"end":end,"mention":question[start:end]}
                labels.append(label)
    
    last_words = ["last","final","finally","latest","oldest","eldest"]
    for last_word in last_words:
        if last_word in question_tokens:
            start = question.find(last_word)
            end = start + len(last_word)
            is_added = False
            for label in labels:
                if label["start"] == start and label["end"] == end:
                    is_added = True
                    break
            if not is_added:
                label = {"id":-1,"label":"SO","start":start,"end":end,"mention":question[start:end]}
                labels.append(label)

    #特定短语匹配
    regex = "series ordinal is (?P<ordinal>[0-9]+)"
    match = re.search(regex,question)
    if match:
        ordinal = match.group("ordinal") 
        label = {"id":-1,"label":"SO","start":match.start("ordinal"),"end":match.end("ordinal"),"mention":ordinal}
        print(ordinal)
        labels.append(label)

    return labels

def annotate_unknown_time(question):
    labels = []
    
    filter_start_when = False
    #what's | what is开头
    for wh_time_word in wh_time_words:
        #what is the date of death of aretha franklin
        regex1 = r"(what is|what's|what was|what were) (the )?(?P<tu>" + wh_time_word + ")"
        #print(regex1)
        match_result = re.match(regex1,question)
        if match_result:
            tu_label = {"id":-1,"label":"TU","start":match_result.start("tu"),"end":match_result.end("tu"),"mention":wh_time_word}
            labels.append(tu_label)

        #what is the end time for the daily show as rachael harris has cast member
        regex2 = r"^(what is|what's|what was|what were|when is|when was|when were) (the point |the |point )?(?P<prefix>\w+) (?P<tu>" + wh_time_word + ")"
        match_result = re.match(regex2,question)
        if match_result and match_result.group("prefix") not in ["the","last","first"]:
            filter_start_when = True
            tu_label = {"id":-1,"label":"TU","start":match_result.start("tu"),"end":match_result.end("tu"),"mention":wh_time_word}
            labels.append(tu_label)
        
        regex3 = r"and (the point |the |point )?(?P<tu>" + wh_time_word + ")"
        match_result = re.search(regex3,question)
        if match_result:            
            tu_label = {"id":-1,"label":"TU","start":match_result.start("tu"),"end":match_result.end("tu"),"mention":wh_time_word}
            labels.append(tu_label)        

        #what is the start time and end time of barnaul which is located in the administrative territorial entity as west sibirean krai
        regex4 = r"and (the )?(?P<prefix>\w+) (?P<tu>" + wh_time_word + ")"
        match_result = re.search(regex4,question)
        if match_result and match_result.group("prefix") != "the":
            tu_label = {"id":-1,"label":"TU","start":match_result.start("tu"),"end":match_result.end("tu"),"mention":wh_time_word}
            labels.append(tu_label)   

    #when,what years等
    for wh_trigger in wh_triggers:
        if wh_trigger == "when":
            if not filter_start_when and question.startswith("when"):
                tu_label = {"id":-1,"label":"TU","start":0,"end":4,"mention":wh_trigger}
                labels.append(tu_label)
            else:
                regex = r"\band when\b"
                match = re.search(regex,question)
                if match:
                    start = match.start() + 4
                    tu_label = {"id":-1,"label":"TU","start":start,"end":start+4,"mention":wh_trigger}
                    labels.append(tu_label)
                else:                       
                    for trigger in wh_trigger2relation.keys():
                        regex = r"\b" + trigger + r" when\b"
                        match = re.search(regex,question)
                        if match:
                            start = match.start() + 1 + len(trigger)
                            tu_label = {"id":-1,"label":"TU","start":start,"end":start+4,"mention":wh_trigger}
                            labels.append(tu_label)                          
        else:
            regex = r"\b" + wh_trigger + r"\b"
            matches = re.finditer(regex,question)
            for match in matches:
                tu_label = {"id":-1,"label":"TU","start":match.start(),"end":match.end(),"mention":wh_trigger}
                labels.append(tu_label)
    
    remove_labels = []
    for label1 in labels:
        for label2 in labels:
            if label1 != label2 and label1["start"] >= label2["start"] and label1["end"] <= label2["end"]:
                remove_labels.append(label1)

    for label in remove_labels:
        if label in labels:
            labels.remove(label)
    return labels

def annotate_explicit_time(question,entities):
    labels = annotate_datetime(question,"2020-04-01")
    filter_labels = []
    for label in labels:
        keep = True
        for entity in entities:
            if label["mention"] in entity and label["mention"] != entity:
                keep = False
                break
        if keep:
            filter_labels.append(label)
        if label["mention"].startswith("the year "):
            label["start"] += len("the year ")
            label["mention"] = question[label["start"]:label["end"]]
    return filter_labels

def annotate_tr_signals(question):
    labels = []
    for trigger in trigger2relation.keys():
        matches = re.finditer(r"\b" + trigger + r"\b",question)
        for match in matches:
            if trigger == "end time of" or trigger == "start time of":
                postfix = question[match.end():].strip()
                if not (postfix[0].isdigit() or postfix[1].isdigit()):
                    continue
            sr_label = {"id":-1,"label":"SR","start":match.start(),"end":match.end(),"mention":trigger}
            labels.append(sr_label)

    #what's | what is开头抽取start和end触发词
    for wh_time_word in wh_time_words:
        #what is the end time for the daily show as rachael harris has cast member
        regex1 = r"^(what is|what's|what was|what were|when is|when was|when were) (the point |the |point )?(?P<prefix>\w+) (?P<tu>" + wh_time_word + ")"
        match_result = re.match(regex1,question)
        if match_result and match_result.group("prefix") != "the":
            prefix = match_result.group("prefix")
            if prefix == "start" or prefix == "end":
                label = {"id":-1,"label":"SR","start":match_result.start("prefix"),"end":match_result.end("prefix"),"mention":prefix}
                labels.append(label)

        #what is the start time and end time of barnaul which is located in the administrative territorial entity as west sibirean krai
        regex2 = r"and (the )?(?P<prefix>\w+) (?P<tu>" + wh_time_word + ")"
        match_result = re.search(regex2,question)
        if match_result and match_result.group("prefix") != "the":
            prefix = match_result.group("prefix")
            if prefix == "start" or prefix == "end":
                label = {"id":-1,"label":"SR","start":match_result.start("prefix"),"end":match_result.end("prefix"),"mention":prefix}
                labels.append(label)


    #过滤被其他signal包含的signal
    remove_labels = []
    for label1 in labels:
        for label2 in labels:
            if label1 != label2 and label1["start"] >= label2["start"] and label1["end"] <= label2["end"]:
                remove_labels.append(label1)
    for label in remove_labels:
        if label in labels:
            labels.remove(label)
    return labels

def annotate_elements(question,parsed_question,entities,events):
    labels,aux_events = annotate_events(question,parsed_question,entities,events)
    labels += annotate_unknown_time(question)
    labels += annotate_explicit_time(question,entities)
    labels += annotate_ordinal_labels(question,entities,parsed_question)
    labels += annotate_tr_signals(question)
    labels.sort(key=lambda l:l["start"])

    remove_labels = []

    #过滤被时序关系触发词包含的事件
    for label1 in labels:
        if label1["label"].startswith("E"):
            for label2 in labels:
                if label2["label"] == "SR" and label1["start"] >= label2["start"] \
                    and label1["end"] <= label2["end"]:
                    remove_labels.append(label1)
                    break

    #过滤时序关系信号when
    for label1 in labels:
        if label1["label"] == "SR" and label1["mention"] == "when":
            for label2 in labels:
                if label2["label"] == "TU" and label2["mention"] == "when" \
                    and label2["start"] == label1["start"]:
                    remove_labels.append(label1)
                    break
    
    #过滤be+时间
    for label1 in labels:
        for label2 in labels:
            if label1["label"].startswith("E") and label2["label"] == "TK":
                regex = "(is|were|was) " + label2["mention"]
                if re.match(regex,label1["mention"]):
                    remove_labels.append(label1)

    #过滤重复标签
    for i,label1 in enumerate(labels):
        for j,label2 in enumerate(labels):
            if j < i and label2["start"] == label1["start"] and label2["end"] == label1["end"] \
                and label2["label"] == label1["label"]:
                remove_labels.append(label1)
                break
    
    #去掉在命名事件中的序数
    for label in labels:
        if label["label"] == "SO" or label["label"] == "TK":
            for event in events:
                if label["mention"] in event:
                    remove_labels.append(label)
                    break
    
    #去掉命名实体中的时间
    for label in labels:
        if label["label"] == "TK":
            for entity in entities:
                if label["mention"] in entity:
                    remove_labels.append(label)
                    break

    #去掉event mention中的序数和时间表达式
    for label1 in labels:
        if label1["label"].startswith("E"):
            for label2 in labels:
                if label2["start"] >= label1["start"] and label2["end"] < label1["end"] \
                    and label2["label"] == "SO" and label2 not in remove_labels:
                    label1["start"] = label2["end"] + 1
                    label1["mention"] = question[label1["start"]:label1["end"]]
                if label2["start"] >= label1["start"] and label2["end"] < label1["end"] \
                    and label2["label"] == "TK": 
                    label1["start"] = label2["end"] + 1
                    label1["mention"] = question[label1["start"]:label1["end"]]

    #过滤现在
    for label1 in labels:
        if label1["label"] == "TK" and label1["mention"] in ["year","the year"]:
            remove_labels.append(label1)
        if label1["label"] == "TK" and label1["mention"] in current_words:
            for label2 in labels:
                if label2["label"] == "TK" and label2["mention"] not in current_words:
                    remove_labels.append(label1)
                    break

    #更新point in time的边界
    for label in labels:
        if label["mention"].startswith("point in time"):
            label["start"] += 6
            label["end"] = label["start"] + 2
            label["mention"] = question[label["start"]:label["end"]]
        if label["label"].startswith("E") and label["mention"].startswith("to "):
            label["start"] += 3
            label["mention"] = question[label["start"]:label["end"]]

    for label in remove_labels:
        if label in labels:
            labels.remove(label)

    for i,label in enumerate(labels):
        label["id"] = i
         
    return labels,aux_events

def find_noun_event_in_span(question,parsed_question,entities,start,end):
    for wh_time_word in wh_time_words:
        filter_regex = r"^(what is|what's|what was|what were|when is|when was|when were)((( the)? (\w+ )?)| )" + wh_time_word \
            + r"( and((( the)? (\w+ )?)| )" + wh_time_word + ")?"
        match_result = re.match(filter_regex,question)
        if match_result:
            start = max(start,match_result.end())
    event_label = None
    for token in parsed_question:
        if token.idx >= start and token.idx + len(token) <= end:
            if token.pos_ == "NOUN":
                event_label = {"id":-1,"label":"EO","start":token.idx,"end":token.idx + len(token),"mention":question[token.idx:token.idx + len(token)]}
                break
    return event_label
            
def annotate_temporal_relations(question,parsed_question,entities,labels):

    remove_labels = []
    relations = []
    #以时序关系触发词生成关系
    enrich_labels = []
    for i,label in enumerate(labels):
        if label["label"] == "SR":
            is_find_relation = False
            if i == len(labels)-1 or labels[i+1]["label"] == "SR":
                #如果该类触发词必定指示时序关系且事件必然出现在触发词右边
                if label["mention"] in certainly_relation_triggers:
                    if i > 0 and labels[i-1]["label"].startswith("E"):
                        target = labels[i-1]
                    else:
                        target = find_noun_event_in_span(question,parsed_question,entities,0,label["start"])
                    related = find_noun_event_in_span(question,parsed_question,entities,label["end"],len(question))
                    if target and related:
                        is_find_relation = True
                        enrich_labels.append(related)
                        if i == 0 or i > 0 and labels[i-1] != target:
                            enrich_labels.append(target)
                        relation = {"id":-1,"type":"TRLINK","target":target,"related_to":related,"signal":label,"rel_type":trigger2relation[label["mention"]]}
                        relations.append(relation)
                elif label["mention"] == "before" or label["mention"] == "after":
                    if i > 0 and labels[i-1]["label"].startswith("E"):
                        is_find_relation = True
                        relation = {"id":-1,"type":"TRLINK","target":labels[i-1],"related_to":labels[i-1],"signal":label,"rel_type":trigger2relation[label["mention"]]}
                        relations.append(relation)
                    else:
                        event_label = find_noun_event_in_span(question,parsed_question,entities,0,label["start"])
                        if event_label:
                            is_find_relation = True
                            enrich_labels.append(event_label)
                            relation = {"id":-1,"type":"TRLINK","target":event_label,"related_to":labels[i-1],"signal":label,"rel_type":trigger2relation[label["mention"]]}
                            relations.append(relation)
            else:
                if labels[i+1]["label"] == "TK" or labels[i+1]["label"] == "TU":
                    # E S T
                    j = i-1
                    while j >= 0:
                        if labels[j]["label"].startswith("E"):
                            is_find_relation = True
                            if labels[i+1]["label"] == "TU":
                                relation = {"id":-1,"type":"TRLINK","target":labels[i+1],"related_to":labels[j],"signal":label,"rel_type":wh_trigger2relation[label["mention"]]}
                            else:
                                relation = {"id":-1,"type":"TRLINK","target":labels[j],"related_to":labels[i+1],"signal":label,"rel_type":trigger2relation[label["mention"]]}
                            relations.append(relation)
                            break
                        j-=1
                    # S T E
                    if not is_find_relation:
                        j = i + 2
                        while j < len(labels):
                            if labels[j]["label"].startswith("E"):
                                is_find_relation = True
                                if labels[i+1]["label"] == "TU":
                                    relation = {"id":-1,"type":"TRLINK","target":labels[i+1],"related_to":labels[j],"signal":label,"rel_type":wh_trigger2relation[label["mention"]]}
                                else:
                                    relation = {"id":-1,"type":"TRLINK","target":labels[j],"related_to":labels[i+1],"signal":label,"rel_type":trigger2relation[label["mention"]]}
                                relations.append(relation)
                                break
                            j+=1
                    #没有已抽取的事件
                    if not is_find_relation:
                        event_label = find_noun_event_in_span(question,parsed_question,entities,0,label["start"])
                        if not event_label:
                            event_label = find_noun_event_in_span(question,parsed_question,entities,label["end"],len(question))
                        if event_label:
                            is_find_relation = True
                            enrich_labels.append(event_label)
                            if labels[i+1]["label"] == "TU":
                                relation = {"id":-1,"type":"TRLINK","target":labels[i+1],"related_to":event_label,"signal":label,"rel_type":wh_trigger2relation[label["mention"]]}
                            else:
                                relation = {"id":-1,"type":"TRLINK","target":event_label,"related_to":labels[i+1],"signal":label,"rel_type":trigger2relation[label["mention"]]}
                            relations.append(relation)
                elif labels[i+1]["label"].startswith("E"):
                    if label["mention"] in certainly_relation_triggers:
                        #E S E
                        j = i-1
                        while j >= 0:
                            if labels[j]["label"].startswith("E"):
                                is_find_relation = True
                                relation = {"id":-1,"type":"TRLINK","target":labels[j],"related_to":labels[i+1],"signal":label,"rel_type":trigger2relation[label["mention"]]}
                                relations.append(relation)
                                break
                            j-=1
                        if not is_find_relation:
                            #S E E
                            j = i + 2
                            while j < len(labels):
                                if labels[j]["label"].startswith("E"):
                                    is_find_relation = True
                                    relation = {"id":-1,"type":"TRLINK","target":labels[j],"related_to":labels[i+1],"signal":label,"rel_type":trigger2relation[label["mention"]]}
                                    relations.append(relation)
                                    break
                                j+=1
                        #缺少事件label
                        if not is_find_relation:
                            #抽取 E S E
                            target = find_noun_event_in_span(question,parsed_question,entities,0,label["start"])
                            if target:
                                is_find_relation = True
                                enrich_labels.append(target)
                                relation = {"id":-1,"type":"TRLINK","target":target,"related_to":labels[i+1],"signal":label,"rel_type":trigger2relation[label["mention"]]}
                        if not is_find_relation:
                            #抽取S E E
                            related = find_noun_event_in_span(question,parsed_question,entities,label["end"],len(question))
                            if target:
                                is_find_relation = True
                                enrich_labels.append(target)
                                relation = {"id":-1,"type":"TRLINK","target":labels[i+1],"related_to":related,"signal":label,"rel_type":trigger2relation[label["mention"]]}                                                   
                    else:
                        #E S E
                        if i > 0 and labels[i-1]["label"].startswith("E"): 
                            is_find_relation = True
                            relation = {"id":-1,"type":"TRLINK","target":labels[i-1],"related_to":labels[i+1],"signal":label,"rel_type":trigger2relation[label["mention"]]}
                            relations.append(relation)
                        #S E E
                        elif i+2 < len(labels) and labels[i+2]["label"].startswith("E"):
                            is_find_relation = True
                            relation = {"id":-1,"type":"TRLINK","target":labels[i+2],"related_to":labels[i+1],"signal":label,"rel_type":trigger2relation[label["mention"]]}
                            relations.append(relation)
                                                                                           
            if not is_find_relation:
                remove_labels.append(label)

    
    #无时序关系信号的情况
    #用TK或TU触发时序关系
    for label in labels:
        if label["label"] == "TK" or label["label"] == "TU":
            in_relation = False
            for relation in relations:
                if relation["related_to"] == label or relation["target"] == label:
                    in_relation = True
                    break
            if not in_relation:
                min_dist = len(question)
                event_label = None
                for e_label in labels:
                    if e_label["label"].startswith("E"):
                        cur_dist = abs(label["start"]-e_label["start"])
                        if cur_dist < min_dist:
                            min_dist = cur_dist
                            event_label = e_label
                if event_label:
                    if label["label"] == "TK":
                        relation = {"id":-1,"type":"TRLINK","target":event_label,"related_to":label,"signal":None,"rel_type":"INCLUDE"}
                        relations.append(relation)
                    else:
                        relation = {"id":-1,"type":"TRLINK","target":label,"related_to":event_label,"signal":None,"rel_type":"SIMULTANEOUS"}
                        relations.append(relation)                 
    #过滤触发词
    for remove_label in remove_labels:
        labels.remove(remove_label)
    
    if "is subject of" in question or "is the subject of" in question:
        target = None
        for label in labels:
            if label["label"] == "EO":
                target = label
                break
        related=None
        if target:
            for label in labels:
                if label["label"] == "EO" and label["start"] > target["start"]:
                    related = label
        
        if target and related:
            relation = {"id":-1,"type":"TRLINK","target":target,"related_to":related,"signal":None,"rel_type":"INCLUDE"}
            relations.append(relation)

    labels += enrich_labels
    return labels,relations

def annotate_temporal_ordinals(question,parsed_question,entities,labels):
    relations = []
    verb_tokens = []
    for token in parsed_question:
        if token.pos_ == "VERB" or token.pos_ == "AUX":
            verb_tokens.append(token.text)

    for label1 in labels:
        if label1["label"] == "SO":
            min_dist = max(label1["start"],abs(len(question)-label1["start"]))
            event_label = None
            for label2 in labels:
                if label2["label"].startswith("E"):
                    event_tokens = label2["mention"].split()
                    for verb_token in verb_tokens:
                        if verb_token in event_tokens:                          
                            cur_dist = abs(label2["start"] - label1["start"])
                            if cur_dist < min_dist:
                                min_dist = cur_dist
                                event_label = label2
                            break
            if not event_label:
                min_dist = max(label1["start"],abs(len(question)-label1["start"]))
                for label2 in labels:
                    if label2["label"].startswith("E"):
                        cur_dist = abs(label2["start"] - label1["start"])
                        if cur_dist < min_dist:
                            min_dist = cur_dist
                            event_label = label2
                        break                
            if event_label:
                relation = {"id":-1,"type":"TOLINK","event":event_label,"ordinal":label1}
                relations.append(relation)
    return labels,relations

def annotate_question(question,entities,events):
    parsed_question = nlp(question)
    labels,aux_events = annotate_elements(question,parsed_question,entities,events)
    labels_wo_so = [label for label in labels if label["label"] != "SO"]
    labels_so = [label for label in labels if label["label"] == "SO"]

    labels_wo_so,trlinks = annotate_temporal_relations(question,parsed_question,entities,labels_wo_so)

    labels = labels_wo_so + labels_so
    labels.sort(key=lambda l:l["start"])
    labels,tolinks = annotate_temporal_ordinals(question,parsed_question,entities,labels)

    for event in aux_events:
        is_in_rel = False
        for trlink in trlinks:
            if trlink["target"] == event or trlink["related_to"] == event:
                is_in_rel = True
                break
        for tolink in tolinks:
            if tolink["event"] == event:
                is_in_rel = True
                break
        if not is_in_rel and event in labels:
            labels.remove(event)
    return labels,trlinks,tolinks


if __name__ == "__main__":
    #"what is award received of michael moorcock that is point in time is 1976-0-0"
    #"when is end time and start time of guglielmo marconi whose spouse as beatrice o'brien"
    question = "which album did dave vanian and the phantom chords release in 1995?"
    entities = ["chief justice","supreme court of india"]
    events = []
    parsed_question = nlp(question)

    for token in parsed_question:
        print(token.text,token.pos_)
    for entity in parsed_question.ents:
        print(entity.text,entity.label_)
    for chunk in parsed_question.noun_chunks:
        print(chunk.text)
    labels,aux_events = annotate_elements(question,parsed_question,entities,events)
    labels_wo_so = [label for label in labels if label["label"] != "SO"]
    labels_so = [label for label in labels if label["label"] == "SO"]

    labels_wo_so,trlinks = annotate_temporal_relations(question,parsed_question,entities,labels_wo_so)

    labels = labels_wo_so + labels_so
    labels.sort(key=lambda l:l["start"])
    labels,tolinks = annotate_temporal_ordinals(question,parsed_question,entities,labels)
    print()