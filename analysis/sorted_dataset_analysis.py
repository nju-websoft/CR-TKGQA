import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import copy
import json
from analysis.sparql_ply import parse_sparql
from analysis.sparql_ply.components import TriplesPath, Union, Query, GraphPattern, QueryComponent, NodeTerm
from analysis.sparql_ply.util import serialize, deserialize, traverse, expand_syntax_form, get_free_varibles
from analysis.fact_graph import FactNode, FactGraph, FactGraphTraverser

import networkx as nx
from networkx import has_path
import re
import itertools
import random 
import math
from tqdm import tqdm
import csv
import pickle
from tabulate import tabulate

def csv_file_to_dict_list(file_path: str):
    result = []
    
    with open(file_path, 'r', encoding='utf-8-sig') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            result.append(dict(row))

def read_json(file_path):
    return json.load(open(file_path, 'r'))

def save_json(data, file_path, indent = 2):
    with open(file_path, 'w', encoding = 'utf-8') as f:
        json.dump(data, f, indent=indent)

from sparql_ply.util import serialize, deserialize, traverse, expand_syntax_form, get_free_varibles


def read_json(file_path):
    return json.load(open(file_path, 'r'))

wikidata_prefix = {
    "bd": "http://www.bigdata.com/rdf#",
    "bds": "http://www.bigdata.com/rdf/search#",
    "cc": "http://creativecommons.org/ns#",
    "dct": "http://purl.org/dc/terms/",
    "gas": "http://www.bigdata.com/rdf/gas#",
    "geo": "http://www.opengis.net/ont/geosparql#",
    "url":  "http://schema.org/",
    "geof": "http://www.opengis.net/def/geosparql/function/",
    "hint": "http://www.bigdata.com/queryHints#",
    "ontolex": "http://www.w3.org/ns/lemon/ontolex#",
    "owl": "http://www.w3.org/2002/07/owl#",
    "p": "http://www.wikidata.org/prop/",
    "pq": "http://www.wikidata.org/prop/qualifier/",
    "pqn": "http://www.wikidata.org/prop/qualifier/value-normalized/",
    "pqv": "http://www.wikidata.org/prop/qualifier/value/",
    "pr": "http://www.wikidata.org/prop/reference/",
    "prn": "http://www.wikidata.org/prop/reference/value-normalized/",
    "prov": "http://www.w3.org/ns/prov#",
    "prv": "http://www.wikidata.org/prop/reference/value/",
    "ps": "http://www.wikidata.org/prop/statement/",
    "psn": "http://www.wikidata.org/prop/statement/value-normalized/",
    "psv": "http://www.wikidata.org/prop/statement/value/",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "schema": "http://schema.org/",
    "skos": "http://www.w3.org/2004/02/skos/core#",
    "wd": "http://www.wikidata.org/entity/",
    "wde":  "http://www.wikidata.org/entity/",
    "wdata": "http://www.wikidata.org/wiki/Special:EntityData/",
    "wdno": "http://www.wikidata.org/prop/novalue/",
    "wdref": "http://www.wikidata.org/reference/",
    "wds": "http://www.wikidata.org/entity/statement/",
    "wdt": "http://www.wikidata.org/prop/direct/",
    "wdtn": "http://www.wikidata.org/prop/direct-normalized/",
    "wdv": "http://www.wikidata.org/value/",
    "wikibase": "http://wikiba.se/ontology#",
    "xsd": "http://www.w3.org/2001/XMLSchema#"
}

wikidata_prefix_list_long_to_short = [(k, v) for k, v in wikidata_prefix.items()]
wikidata_prefix_list_long_to_short.sort(key = lambda i: i[1], reverse=True)

wikidata_properties = {item['ID']:item for item in json.load(open('../resources/all_wikidata_property.json'))}


def replace_xsd_iri_with_prefix(sparql_expression):
    xsd_mappings = {
        r'<http://www\.w3\.org/2001/XMLSchema#string>': 'xsd:string',
        r'<http://www\.w3\.org/2001/XMLSchema#integer>': 'xsd:integer',
        r'<http://www\.w3\.org/2001/XMLSchema#decimal>': 'xsd:decimal',
        r'<http://www\.w3\.org/2001/XMLSchema#float>': 'xsd:float',
        r'<http://www\.w3\.org/2001/XMLSchema#double>': 'xsd:double',
        r'<http://www\.w3\.org/2001/XMLSchema#boolean>': 'xsd:boolean',
        r'<http://www\.w3\.org/2001/XMLSchema#date>': 'xsd:date',
        r'<http://www\.w3\.org/2001/XMLSchema#dateTime>': 'xsd:dateTime',
        r'<http://www\.w3\.org/2001/XMLSchema#time>': 'xsd:time',
        r'<http://www\.w3\.org/2001/XMLSchema#gYear>': 'xsd:gYear',
        r'<http://www\.w3\.org/2001/XMLSchema#gYearMonth>': 'xsd:gYearMonth',
        r'<http://www\.w3\.org/2001/XMLSchema#duration>': 'xsd:duration',
        r'<http://www\.w3\.org/2001/XMLSchema#anyURI>': 'xsd:anyURI'
    }
    general_pattern = r'<http://www\.w3\.org/2001/XMLSchema#(\w+)>'
    
    def replace_match(match):
        xsd_type = match.group(1) 
        return f'xsd:{xsd_type}'
    result = re.sub(general_pattern, replace_match, sparql_expression)
    
    return result


def analysis_temporal_taxonomy_sparql(sparql):

    def convert_full_iri_to_short(str):
        for (short, long) in wikidata_prefix_list_long_to_short:
            str = str.replace(long, short+':')
        str = str.replace('<','').replace('>','')
        return str
    
    def convert_triple_into_nary_facts(triples):
        temp = []
        for triple in triples:
            subject = convert_full_iri_to_short(triple.subj.value)
            predicate = convert_full_iri_to_short(str(triple.pred_obj_list[0][0]))
            object = convert_full_iri_to_short(triple.pred_obj_list[0][1][0].value)
            temp.append([subject,predicate,object])
        triples = temp

        stmt_nodes = []
        for triple in triples:
            if triple[1].startswith('p:'):
                stmt_nodes.append(triple[2])
        used_triples = []
        facts = []
        for stmt_node in stmt_nodes:
            nary_fact = {'stmt_node':stmt_node, 'triples':[], 'external_variable':[], 
                         'number_of_temporal_external_variables':0}
            for triple in triples:
                if triple[0] == stmt_node or triple[2] == stmt_node:
                    if triple[0].startswith('?') and triple[0] != stmt_node:
                        nary_fact['external_variable'].append({'variable': triple[0], 'temporal_value':False})
                    if triple[2].startswith('?') and triple[2] != stmt_node: 
                        if wikidata_properties.get(triple[1].split(':')[1], {'datatype':None})['datatype'] == "T":   
                            nary_fact['external_variable'].append({'variable': triple[2], 
                                                                   'temporal_property':triple[1],
                                                                   'temporal_value':True,
                                                                   'temporal_type':'time_point'})
                            nary_fact['number_of_temporal_external_variables'] += 1
                        elif triple[1].split(':')[1] in ['P2047', 'P2097']:
                            nary_fact['external_variable'].append({'variable': triple[2], 
                                                                   'temporal_property':triple[1],
                                                                   'temporal_value':True,
                                                                   'temporal_type':'duration'})
                            nary_fact['number_of_temporal_external_variables'] += 1
                        else:
                            nary_fact['external_variable'].append({'variable': triple[2], 'temporal_value':False})
                    nary_fact['triples'].append(triple)
                    used_triples.append(triple)
            facts.append(nary_fact)
        for triple in triples:
            if triple not in used_triples:
                binary_fact = {'stmt_node':None, 'triples':[triple], 'external_variable':[],
                               'number_of_temporal_external_variables':0}
                if triple[0].startswith('?'):
                    binary_fact['external_variable'].append({'variable': triple[0], 'temporal_value':False})
                if triple[2].startswith('?'): 
                    if wikidata_properties.get(triple[1].split(':')[1], {'datatype':None})['datatype'] == "T":    
                        binary_fact['external_variable'].append({'variable': triple[2], 
                                                                 'temporal_property':triple[1],
                                                                 'temporal_value':True,
                                                                 'temporal_type':'time_point'})
                        binary_fact['number_of_temporal_external_variables'] += 1
                    elif triple[1].split(':')[1] in ['P2047', 'P2097']:
                        binary_fact['external_variable'].append({'variable': triple[2], 
                                                                'temporal_property':triple[1],
                                                                'temporal_value':True,
                                                                'temporal_type':'duration'})
                        binary_fact['number_of_temporal_external_variables'] += 1
                    else:
                        binary_fact['external_variable'].append({'variable': triple[2], 'temporal_value':False})
                facts.append(binary_fact)
        return facts

    def has_type(component, types):
        if isinstance(types, list):
            for ty in types:
                if component.type & ty:
                    return True
            return False
        else:
            return component.type & types
    
    def get_elements(component:QueryComponent):
        nonlocal elements
        if has_type(component, [GraphPattern.FILTER, GraphPattern.BIND, TriplesPath.TYPE]):
            elements.append(component)

    def analysis_filter(element: GraphPattern):
        def convert_node_to_dict(node, node_dict: dict, father: str):
            node_str = replace_xsd_iri_with_prefix(str(node))
            node_dict[node_str] = {
                'type': str(type(node)).replace("<class 'sparql_ply.components.", "").replace("'>", "")
            }
            if father != None:
                node_dict[node_str]['father'] = father
            if 'operator' in node.__dict__.keys() and node.operator != None:
                node_dict[node_str]['operator'] = node.operator
            if 'value' in node.__dict__.keys():
                node_dict[node_str]['value'] = node.value
            if 'children' in node.__dict__.keys():
                node_dict[node_str]['children'] = list()
                for child in node.children:
                    node_dict[node_str]['children'].append(replace_xsd_iri_with_prefix(str(child)))
                    convert_node_to_dict(child, node_dict, node_str)
        node_dict = dict()
        convert_node_to_dict(element, node_dict, None)
        return node_dict

    try:
        sparql_expanded = expand_syntax_form(sparql, wikidata_prefix)
        parse_tree = parse_sparql(sparql_expanded)
    except:
        import traceback
        traceback.print_exc()
        parse_tree = None
        return None
    if parse_tree is not None:
        elements = []
        triples = []
        filters = []
        filter_contents = dict()
        bind_contents = dict()
        binds = []
        solution_modifier_triggers = []
        traverse(parse_tree, before=get_elements)
        for element in elements:
            if element.type & TriplesPath.TYPE:
                triples.append(element)
            elif element.type & GraphPattern.FILTER:
                element_str = replace_xsd_iri_with_prefix(str(element))
                filters.append(element_str)
                filter_contents[element_str] = analysis_filter(element)
            elif element.type & GraphPattern.BIND:
                element_str = replace_xsd_iri_with_prefix(str(element))
                binds.append(element_str)
                bind_contents[element_str] = analysis_filter(element)
        facts = convert_triple_into_nary_facts(triples)
        sparql_wo_space = sparql.replace(' ', '').lower()
        for trigger in [r'orderby', r'count\(', r"avg\("]:
            num_occurances = len(re.findall(trigger, sparql_wo_space))
            for i in range(0, num_occurances):
                    if trigger == "orderby":
                        solution_modifier_triggers.append('ordinal_rank')
                    else:
                        solution_modifier_triggers.append(trigger.strip('\('))
        extracted_features = {
            'facts':facts,
            'filters':filters, 
            'binds':binds, 
            'solution_modifier_triggers':solution_modifier_triggers,
            'filter_contents': filter_contents,
            'bind_contents': bind_contents
        }
        return extracted_features




def analysis_structural_complexity(data_item, extracted_features):
    structural_complexity = []
    temporal_variables = []
    multi_hop_length = 10
    G_sparql = nx.Graph()
    facts = extracted_features['facts']
    for fact in facts:
        if 'wdt:P31' in fact['triples'][0][1]:
            continue
        elif 'wikibase:' in fact['triples'][0][1]:
            continue
        temporal_variables += ([var for var in fact['external_variable'] if var['temporal_value']])
        for triple in fact['triples']:
            s, p, o = triple[0], triple[1], triple[2]
            if triple[1].startswith('p:'):
                s_type, o_type = ['var' if s.startswith("?") else 'uri'], 'stmt'
            elif triple[1].startswith('ps:') or triple[1].startswith('pq:'):
                s_type, o_type = 'stmt', ('var' if s.startswith("?") else 'uri')
            else:
                s_type, o_type = ['var' if s.startswith("?") else 'uri'], ['var' if o.startswith("?") else 'uri']
            if s not in G_sparql:
                G_sparql.add_nodes_from([(s, {'type':s_type})])
            if o not in G_sparql:
                G_sparql.add_nodes_from([(o, {'type':o_type})])
            G_sparql.add_edges_from([(s, o, {"property": p})])
    G_sparql_fact_only = copy.deepcopy(G_sparql) 
    for bind in extracted_features['binds']:
        AS_left, AS_right = bind[0:bind.index('AS')], bind[bind.index('AS'):]
        left_variables, right_variable = list(re.findall(r"\?\w+", AS_left)), re.findall(r"\?\w+", AS_right)[0]
        G_sparql.add_nodes_from([(right_variable, {'type':'var'})])
        for var in left_variables:
            if var not in G_sparql:
                G_sparql.add_nodes_from([(var, {'type':'var'})])
            G_sparql.add_edges_from([(var, right_variable, {"property": "BIND"})])
    for idx, filter in enumerate(extracted_features['filters']):
        G_sparql.add_nodes_from([(f'FILTER_{idx}', {'type':'FILTER'})])
        variables = list(re.findall(r'\?\w+', filter))
        for var in variables:
            if var not in G_sparql:
                G_sparql.add_nodes_from([(var, {'type':'var'})])
            G_sparql.add_edges_from([(var, f'FILTER_{idx}', {"property": "FILTER"})])
    temporal_external_variables = []
    non_temporal_external_variables = []
    constants = []
    for fact_idx, fact in enumerate(facts):
        for triple in fact['triples']:
            s, p, o = triple[0], triple[1], triple[2]
            if not s.startswith('?'):
                constants.append(s)
            if not o.startswith('?'):
                constants.append(o)
        for var in fact['external_variable']:
            if var['temporal_value']:
                temporal_external_variables.append(var['variable'])
            else:
                non_temporal_external_variables.append(var['variable'])
    strong_components = list(nx.connected_components(G_sparql_fact_only))
    for component in strong_components:
        external_vars_in_component = []
        url_in_component = []
        for var in temporal_external_variables + non_temporal_external_variables:
            if var in component:
                external_vars_in_component.append(var)
        for url in constants:
            if url in component:
                url_in_component.append(url)
        for var, url in itertools.product(external_vars_in_component, url_in_component):
            path = nx.shortest_path(G_sparql_fact_only, source=var, target=url)
            has_entity = False
            for i in range(len(path) - 1):
                if path[i].startswith('wd:'):
                    has_entity = True
            if has_entity:
                continue
            edges = [G_sparql_fact_only[path[i]][path[i+1]] for i in range(0, len(path)-1)]
            if len(edges) >= 3:
                structural_complexity.append('multi_hop_reasoning')
                break
            elif len(edges) == 2:
                wdt_num = len([edge for edge in edges if 'wdt:' in edge['property']])
                if wdt_num > 0:
                    structural_complexity.append('multi_hop_reasoning')
                    break
            wdt_ps_pq_num = len([edge for edge in edges if edge['property'].startswith('ps:') or edge['property'].startswith('pq:') or edge['property'].startswith('wdt:')])
            if wdt_ps_pq_num < multi_hop_length:
                multi_hop_length = wdt_ps_pq_num
    nodes_to_vars = {}
    for idx, fact in enumerate(facts):
        temporal_external_variables = [var['variable'] for var in fact['external_variable'] if var['temporal_value']]
        nodes_to_vars[f"fact_{idx}"] = temporal_external_variables
    nodes_to_temp_var_noempty = {k:v for k,v in nodes_to_vars.items() if len(v) > 0}
    if len(nodes_to_temp_var_noempty) >= 2:
        structural_complexity.append("temporal_fact_fusion")
    return structural_complexity, G_sparql, multi_hop_length, len(nodes_to_temp_var_noempty)


def collect_var(extracted_features):
    time_point_vars = set()
    duration_vars = set()
    nontime_vars = set()
    for fact in extracted_features['facts']:
        for external_variable in fact['external_variable']:
            if external_variable['temporal_value']:
                if external_variable['temporal_type'] == "time_point":
                    time_point_vars.add(external_variable['variable'])
                else:
                    duration_vars.add(external_variable['variable'])
            else:
                nontime_vars.add(external_variable['variable'])
    return list(time_point_vars), list(duration_vars), list(nontime_vars)

def get_multi_hop_length_by_fact_traverse(facts: list):
    nodes = [FactNode(fact) for fact in facts]
    graph = FactGraph()
    graph.add_nodes(nodes)
    traverser = FactGraphTraverser(graph)
    traverser.start_traverse()
    traverse_result = traverser.time_vars
    max_depth = 0
    for var in traverse_result.keys():
        max_depth = max(max_depth, traverse_result[var])
    return max_depth

def analysis_temporal_taxonomy(dataset):
    if dataset == "CR-TKGQA":
        extraction_results = []
        dataset_dir = "../dataset/CR-TKGQA"
        result_dir = "../analysis_results/CR-TKGQA"
        if not os.path.isdir(result_dir):
            os.makedirs(result_dir)
        total_data = []
        for split in ['test', 'dev', 'train']:
            total_data += json.load(open(f'{dataset_dir}/{split}.json', 'r'))
        for item in tqdm(total_data):
            extracted_features = analysis_temporal_taxonomy_sparql(item['sparql'])
            time_point_vars, duration_vars, nontime_vars = collect_var(extracted_features)
            extraction_result = {
                'id':item['id'],
                'question':item['question'],
                'question_tagged':item['question_tagged'],
                'sparql':item['sparql'],
                'extracted_features':extracted_features,
                'vars': {
                    'time_point': time_point_vars,
                    'duration': duration_vars,
                    'nontime': nontime_vars
                },
                'filter': extracted_features['filter_contents'],
                'bind': extracted_features['bind_contents'],
            }
            extraction_result['analysis'] = analysis_filter_bind(extraction_result['vars'], extraction_result['filter'], extraction_result['bind'])
            structural_complexity, G_sparql, multi_hop_length, temporal_composition_count  = analysis_structural_complexity(item, extracted_features)
            extraction_result['sparql_skeleton_graph'] = nx.node_link_data(G_sparql)
            if 'temporal_fact_fusion' in structural_complexity:
                extraction_result['analysis']['temporal_fact_fusion'] = [temporal_composition_count]
            extraction_result['analysis']['multi_hop_reasoning'] = [get_multi_hop_length_by_fact_traverse(extracted_features['facts'])]
            analysis_aggregation(extraction_result['analysis'], extraction_result['sparql'], extraction_result['vars'])
            extracted_features.pop('filter_contents')
            extracted_features.pop('bind_contents')
            extraction_results.append(extraction_result)
        json.dump(extraction_results, open(f'{result_dir}/complexity_taxonomy.json', 'w'), indent=4)
    elif dataset == "TempQA-WD":
        dataset_dir = "../dataset/tempqa_wd"
        result_dir = "../analysis_results/TempQA-WD"
        if not os.path.isdir(result_dir):
            os.makedirs(result_dir)
        extraction_results = []
        total_data = []
        for split in ['test', 'dev']:
            total_data += json.load(open(f'{dataset_dir}/{split}.json', 'r'))
        for item in tqdm(total_data):
            extracted_features = analysis_temporal_taxonomy_sparql(item['SPARQL'])
            time_point_vars, duration_vars, nontime_vars = collect_var(extracted_features)
            extraction_result = {
                'id':item['ID'],
                'question':item['TEXT'],
                'sparql':item['SPARQL'],
                'extracted_features':extracted_features,
                'vars': {
                    'time_point': time_point_vars,
                    'duration': duration_vars,
                    'nontime': nontime_vars
                },
                'filter': extracted_features['filter_contents'],
                'bind': extracted_features['bind_contents']
            }
            extraction_result['analysis'] = analysis_filter_bind(extraction_result['vars'], extraction_result['filter'], extraction_result['bind'])
            structural_complexity, G_sparql, multi_hop_length, temporal_composition_count  = analysis_structural_complexity(item, extracted_features)
            extraction_result['sparql_skeleton_graph'] = nx.node_link_data(G_sparql)
            if 'temporal_fact_fusion' in structural_complexity:
                extraction_result['analysis']['temporal_fact_fusion'] = [temporal_composition_count]
            extraction_result['analysis']['multi_hop_reasoning'] = [get_multi_hop_length_by_fact_traverse(extracted_features['facts'])]
            analysis_aggregation(extraction_result['analysis'], extraction_result['sparql'], extraction_result['vars'])
            extracted_features.pop('filter_contents')
            extracted_features.pop('bind_contents')
            extraction_results.append(extraction_result)
        json.dump(extraction_results, open(f'{result_dir}/complexity_taxonomy.json', 'w'), indent=4)


def convert_format():
    for file in ['train', 'valid', 'test']:
        pickle_data = pickle.load(open(f'pickle/{file}.pickle', 'rb'))
        json_data = list()
        for example in pickle_data:
            ans = dict()
            ans['question'] = example['question']
            ans['answers'] = list(example['answers'])
            ans['answer_type'] = example['answer_type']
            ans['template'] = example['template']
            ans['entities'] = list(example['entities'])
            ans['times'] = list(example['times'])
            ans['relations'] = list(example['relations'])
            ans['type'] = example['type']
            ans['annotation'] = example['annotation']
            ans['uniq_id'] = example['uniq_id']
            ans['paraphrases'] = example['paraphrases']
            json_data.append(ans)
        with open(f'json/{file}.json', 'w', encoding='utf-8') as w:
                json.dump(json_data, w, indent=4, ensure_ascii=False)


def analysis_aggregation(analysis_result: dict, sparql, vars):
    def select_content(sparql):
        pattern = r'SELECT(.*?)\{'
        matches = re.findall(pattern, sparql)
        return [match.strip() for match in matches]
    def order_by_content(sparql):
        prev = 0
        order_bys = list()
        for i, c in enumerate(sparql):
            if c == 'O':
                if len(sparql) > i + len('ORDER BY') and sparql[i : i + len('ORDER BY')] == 'ORDER BY':
                    prev = i + len('ORDER BY')
            elif c == 'L':
                if len(sparql) > i + len('LIMIT') and sparql[i : i + len('LIMIT')] == 'LIMIT':
                    order_bys.append(sparql[prev : i])
        return [match.strip() for match in order_bys]
    analysis_result['timepoint_ordinal'] = set()
    analysis_result['duration_ordinal'] = set()
    analysis_result['temporal_statistic'] = set()
    analysis_result['fact_counting'] = set()
    selects = select_content(sparql)
    order_bys = order_by_content(sparql)
    for select in selects:
        if 'COUNT' in select:
            analysis_result['fact_counting'].add(select)
        if 'SUM' in select or 'AVG' in select:
            analysis_result['temporal_statistic'].add(select)
        if 'MAX' in select or 'MIN' in select:
            if '*' in select or '/' in select:
                analysis_result['duration_ordinal'].add(select)
                if '365' not in select and select not in analysis_result['duration_calculation']:
                    analysis_result['duration_calculation'].append(select)
            has_time_point_var = False
            has_duration_var = False
            for var in vars['time_point']:
                if f'{var} ' in select or f'{var})' in select or select.endswith(var):
                    has_time_point_var = True
                    break
            for var in vars['duration'] + vars['derived_duration']:
                if f'{var} ' in select or f'{var})' in select or select.endswith(var):
                    has_duration_var = True
                    break
            if '+' in select:
                if has_duration_var and has_time_point_var:
                    analysis_result['timepoint_ordinal'].add(select)
                    if select not in analysis_result['timepoint_shift']:
                        analysis_result['timepoint_shift'].append(select)
                elif has_duration_var:
                    analysis_result['duration_ordinal'].add(select)
                    if select not in analysis_result['duration_calculation']:
                        analysis_result['duration_calculation'].append(select)
            if '-' in select:
                analysis_result['duration_ordinal'].add(select)
                if has_time_point_var and select not in analysis_result['duration_derivation']:
                    analysis_result['duration_derivation'].append(select)
                if has_duration_var and select not in analysis_result['duration_calculation']:
                    analysis_result['duration_calculation'].append(select)
            if '+' not in select and '-' not in select and '*' not in select and '/' not in select:
                if has_time_point_var:
                    analysis_result['timepoint_ordinal'].add(select)
                if has_duration_var:
                    analysis_result['duration_ordinal'].add(select)
    for order_by in order_bys:
        has_time_point_var = False
        has_duration_var = False
        if '*' in order_by or '/' in order_by:
            analysis_result['duration_ordinal'].add(order_by)
            if '365' not in order_by and order_by not in analysis_result['duration_calculation']:
                analysis_result['duration_calculation'].append(order_by)
        for var in vars['time_point']:
            if f'{var} ' in order_by or f'{var})' in order_by or order_by.endswith(var):
                has_time_point_var = True
                break
        for var in vars['duration'] + vars['derived_duration']:
            if f'{var} ' in order_by or f'{var})' in order_by or order_by.endswith(var):
                has_duration_var = True
                break
        if '+' in order_by:
            if has_duration_var and has_time_point_var:
                analysis_result['timepoint_ordinal'].add(order_by)
                if order_by not in analysis_result['timepoint_shift']:
                    analysis_result['timepoint_shift'].append(order_by)
            elif has_duration_var:
                analysis_result['duration_ordinal'].add(order_by)
                if order_by not in analysis_result['duration_calculation']:
                    analysis_result['duration_calculation'].append(order_by)
        if '-' in order_by:
            analysis_result['duration_ordinal'].add(order_by)
            if has_time_point_var and order_by not in analysis_result['duration_derivation']:
                analysis_result['duration_derivation'].append(order_by)
            if has_duration_var and order_by not in analysis_result['duration_calculation']:
                analysis_result['duration_calculation'].append(order_by)
        if '+' not in order_by and '-' not in order_by and '*' not in order_by and '/' not in order_by:
            if has_time_point_var:
                analysis_result['timepoint_ordinal'].add(order_by)
            if has_duration_var:
                analysis_result['duration_ordinal'].add(order_by)
    analysis_result['timepoint_ordinal'] = list(analysis_result['timepoint_ordinal'])
    analysis_result['duration_ordinal'] = list(analysis_result['duration_ordinal'])
    analysis_result['temporal_statistic'] = list(analysis_result['temporal_statistic'])
    analysis_result['fact_counting'] = list(analysis_result['fact_counting'])

def analysis_filter_bind(vars, filters, binds):
    analysis_result = {
        'timepoint_comparision': set(),
        'duration_comparison': set(),
        'duration_calculation': set(),
        'timepoint_shift': set(),
        'duration_derivation': set(),
        'granularity_conversion': set(),
        'frequency': set()
    }
    vars['derived_duration'] = list()
    for var in vars['time_point']:
        for bind in binds.keys():
            is_time_point = True
            new_var = binds[bind][bind]['children'][1]
            assert f'AS {new_var}' in bind
            for content in binds[bind].keys():
                if content == var:
                    while 'father' in binds[bind][content].keys() and binds[bind][content]['father'] != None:
                        content = binds[bind][content]['father']
                        if 'operator' in binds[bind][content].keys():
                            oper = binds[bind][content]['operator']
                            if oper in ['YEAR', 'MONTH', "DAY"]:
                                analysis_result['granularity_conversion'].add(bind)
                            elif oper == '-':
                                if is_time_point:
                                    analysis_result['duration_derivation'].add(bind)
                                    is_time_point = False
                                else:
                                    analysis_result['duration_calculation'].add(bind)
                            elif oper == '+':
                                if is_time_point:
                                    analysis_result['timepoint_shift'].add(bind)
                                else:
                                    analysis_result['duration_calculation'].add(bind)
                            elif oper in ['*', '/']:
                                if '1' in binds[bind][content]['children'] or '365' in binds[bind][content]['children']:
                                    continue
                                if oper == '/':
                                    analysis_result['frequency'].add(bind)
                                else:
                                    analysis_result['duration_calculation'].add(bind)
            if not is_time_point:
                vars['derived_duration'].append(new_var)
        for filter in filters.keys():
            is_time_point = True
            for content in filters[filter].keys():
                if content == var:
                    while 'father' in filters[filter][content].keys() and filters[filter][content]['father'] != None:
                        content = filters[filter][content]['father']
                        if 'operator' in filters[filter][content].keys():
                            oper = filters[filter][content]['operator']
                            if oper in ['YEAR', 'MONTH', "DAY"]:
                                analysis_result['granularity_conversion'].add(filter)
                            elif oper == '-':
                                if is_time_point:
                                    analysis_result['duration_derivation'].add(filter)
                                    is_time_point = False
                                else:
                                    analysis_result['duration_calculation'].add(filter)
                            elif oper == '+':
                                if is_time_point:
                                    analysis_result['timepoint_shift'].add(filter)
                                else:
                                    analysis_result['duration_calculation'].add(filter)
                            elif oper in ['*', '/']:
                                if '1' in filters[filter][content]['children'] or '365' in filters[filter][content]['children']:
                                    continue
                                if oper == '/':
                                    analysis_result['frequency'].add(filter)
                                else:
                                    analysis_result['duration_calculation'].add(filter)
                            elif oper in ['<', '>', '=', '<=', '>=']:
                                if is_time_point:
                                    analysis_result['timepoint_comparision'].add(filter)
                                else:
                                    analysis_result['duration_comparison'].add(filter)
    new_vars = list()
    for var in vars['derived_duration'] + vars['duration']:
        for bind in binds.keys():
            new_var = binds[bind][bind]['children'][1]
            assert f'AS {new_var}' in bind
            for content in binds[bind].keys():
                if content == var:
                    while 'father' in binds[bind][content].keys() and binds[bind][content]['father'] != None:
                        content = binds[bind][content]['father']
                        if 'operator' in binds[bind][content].keys():
                            oper = binds[bind][content]['operator']
                            if oper == '-':
                                analysis_result['duration_calculation'].add(bind)
                            elif oper == '+':
                                analysis_result['duration_calculation'].add(bind)
                            elif oper in ['*', '/']:
                                if '1' in binds[bind][content]['children'] or '365' in binds[bind][content]['children']:
                                    continue
                                if oper == '/':
                                    analysis_result['frequency'].add(bind)
                                else:
                                    analysis_result['duration_calculation'].add(bind)
                new_vars.append(new_var)
    vars['derived_duration'].extend(new_vars)
    for var in vars['derived_duration'] + vars['duration']:
        for filter in filters.keys():
            for content in filters[filter].keys():
                if content == var:
                    while 'father' in filters[filter][content].keys() and filters[filter][content]['father'] != None:
                        content = filters[filter][content]['father']
                        if 'operator' in filters[filter][content].keys():
                            oper = filters[filter][content]['operator']
                            if oper == '-':
                                analysis_result['duration_calculation'].add(filter)
                            elif oper == '+':
                                analysis_result['duration_calculation'].add(filter)
                            elif oper in ['*', '/']:
                                if '1' in filters[filter][content]['children'] or '365' in filters[filter][content]['children']:
                                    continue
                                if oper == '/':
                                    analysis_result['frequency'].add(filter)
                                else:
                                    analysis_result['duration_calculation'].add(filter)
                            elif oper in ['<', '>', '=', '<=', '>=']:
                                analysis_result['duration_comparison'].add(filter)
    for key in analysis_result.keys():
        analysis_result[key] = list(analysis_result[key])
    return analysis_result


def graph_invariants(graph):
    invariants = {}
    
    invariants['num_nodes'] = graph.number_of_nodes()
    invariants['num_edges'] = graph.number_of_edges()
    
    if invariants['num_nodes'] == 0:
        invariants['degree_sequence'] = tuple()
        invariants['connected_components'] = 0
        invariants['is_connected'] = False
        return invariants
    
    degree_sequence = sorted([d for n, d in graph.degree()])
    invariants['degree_sequence'] = tuple(degree_sequence)
    
    invariants['connected_components'] = nx.number_connected_components(graph)
    invariants['is_connected'] = invariants['connected_components'] == 1
    
    if invariants['is_connected']:
        try:
            invariants['diameter'] = nx.diameter(graph)
        except:
            invariants['diameter'] = 0
    else:
        invariants['diameter'] = float('inf')
    
    return invariants


def get_isomorphism_skeletons(graphs, abstract_relation=False):
    invariant_groups = {}
    
    for graph in graphs:
        inv_key = tuple(sorted(graph_invariants(graph).items()))
        if inv_key not in invariant_groups:
            invariant_groups[inv_key] = []
        invariant_groups[inv_key].append(graph)
    
    classes = []
    
    for inv_key, graph_group in tqdm(invariant_groups.items()):
        if len(graph_group) == 1:
            classes.append(graph_group)
            continue
            
        group_classes = []
        for graph in graph_group:
            found_class = False
            for i in range(len(group_classes)):
                if abstract_relation:
                    result = nx.is_isomorphic(graph, group_classes[i][0], 
                                            node_match=lambda n1, n2: n1.get("type") == n2.get("type"))
                else:
                    result = nx.is_isomorphic(graph, group_classes[i][0],
                                            node_match=lambda n1, n2: n1.get("type") == n2.get("type"),
                                            edge_match=lambda e1, e2: e1.get("property") == e2.get('property'))
                if result:
                    group_classes[i].append(graph)
                    found_class = True
                    break
            
            if not found_class:
                group_classes.append([graph])
        
        classes.extend(group_classes)
    
    return classes


def analysis_split_complexity(data_dir, extraction_data_path, table_output_path):
    if 'tempqa' in data_dir:
        result_table = {'all':{}}
        complexity_dict = { 
            "temporal_fact_fusion": [],
            "multi_hop_reasoning": [],
            "timepoint_comparision": [],
            "duration_comparison": [],
            "duration_calculation": [],
            "timepoint_shift": [],
            "duration_derivation": [],
            "granularity_conversion": [],
            "timepoint_ordinal": [],
            "duration_ordinal": [],
            "statistical":[],
        }
        extraction_results = read_json(extraction_data_path)
        for item in tqdm(extraction_results):
            for aspect in item['analysis']:
                if len(item['analysis'][aspect]) > 0:
                    if aspect == 'temporal_statistic' or aspect == 'frequency':
                        complexity_dict['statistical'].append(item['id'])
                    elif aspect == "multi_hop_reasoning":
                        if item['analysis'][aspect][0] >= 1:
                            complexity_dict[aspect].append(item['id'])
                    else:
                        if aspect not in complexity_dict:
                            continue
                        complexity_dict[aspect].append(item['id']) 
        for k, v in complexity_dict.items():
            num_complexity = len(v)
            print(k, num_complexity, 100*(num_complexity/len(extraction_results)))
            result_table['all'][k] = {'number': num_complexity, 'percentage':100*(num_complexity/len(extraction_results))}
        sparql_structures = [nx.node_link_graph(item['sparql_skeleton_graph']) for item in extraction_results]
        number_Canonical_LF = len(get_isomorphism_skeletons(sparql_structures, abstract_relation=False))
        number_Struct = len(get_isomorphism_skeletons(sparql_structures, abstract_relation=True))
        print('number_Canonical_LF',number_Canonical_LF ,'number_Struct',number_Struct)
        result_table['all']['number_of_skeletons'] = {'number_Canonical_LF': number_Canonical_LF, 'number_Struct':number_Struct}
        save_json(result_table, table_output_path)
    else:
        result_table = {'all':{}, 'seed':{}, 'generalized':{}, 'augmented':{}}
        elements_all, elements_seed, elements_generalizations, elements_compositional = [], [], [], []
        seed_ids = []
        generalized_ids = []
        SEA_ids = []
        ETA_ids = []
        TEA_ids = []
        for split in ['test', 'train', 'dev']:
            data_path = f'{data_dir}/{split}.json'
            split_data = read_json(data_path)
            question_dict = {item['id']:item for item in split_data}
            id2questiontype = {item['id']:item['origin'] for item in split_data}
            total_ids = list(question_dict.keys())
            for id in total_ids:
                if id2questiontype[id] == 'Seed':
                    seed_ids.append(id)
                elif id2questiontype[id] == 'Generalization':
                    generalized_ids.append(id)
                elif id2questiontype[id] == 'Static Entity Augmentation':
                    SEA_ids.append(id)
                elif id2questiontype[id] == 'Temporal Entity Augmentation':
                    TEA_ids.append(id)
                elif id2questiontype[id] == 'Event Time Augmentation':
                    ETA_ids.append(id)
                else: 
                    print(id2questiontype[id])
                    assert(0)
        compositional_ids = SEA_ids + ETA_ids + TEA_ids
        for item in json.load(open(extraction_data_path)):
            elements_all.append(item)
            if item['id'] in seed_ids:
                elements_seed.append(item)
            if item['id'] in generalized_ids:
                elements_generalizations.append(item)
            if item['id'] in compositional_ids:
                elements_compositional.append(item)
        for idx, split in enumerate([elements_all, elements_seed, elements_generalizations, elements_compositional]):
            print(['elements_all', 'elements_seed', 'elements_generalizations', 'elements_compositional'][idx])
            result_table_key = ['all', 'seed', 'generalized', 'augmented'][idx]
            result_table[result_table_key] = {}
            complexity_dict = { 
                "temporal_fact_fusion": [],
                "multi_hop_reasoning": [],
                "timepoint_comparision": [],
                "duration_comparison": [],
                "duration_calculation": [],
                "timepoint_shift": [],
                "duration_derivation": [],
                "granularity_conversion": [],
                "timepoint_ordinal": [],
                "duration_ordinal": [],
                "statistical":[],
            }
            for item in split:
                for aspect in item['analysis']:
                    if len(item['analysis'][aspect]) > 0:
                        if aspect == 'temporal_statistic' or aspect == 'frequency':
                            complexity_dict['statistical'].append(item['id'])
                        elif aspect == "multi_hop_reasoning":
                            if item['analysis'][aspect][0] >= 1:
                                complexity_dict[aspect].append(item['id'])
                        else:
                            if aspect not in complexity_dict:
                                continue
                            complexity_dict[aspect].append(item['id']) 
            for k, v in complexity_dict.items():
                num_complexity = len(v)
                print(k, num_complexity, 100*(num_complexity/len(split)))
                result_table[result_table_key][k] = {'number':num_complexity, 'percentage':100*(num_complexity/len(split))}
            sparql_structures = [nx.node_link_graph(item['sparql_skeleton_graph']) for item in split]
            number_Canonical_LF = len(get_isomorphism_skeletons(sparql_structures, abstract_relation=False))
            number_Struct = len(get_isomorphism_skeletons(sparql_structures, abstract_relation=True))
            print('number_Canonical_LF',number_Canonical_LF ,'number_Struct',number_Struct)
            result_table[result_table_key]['number_of_skeletons'] = {'number_Canonical_LF': number_Canonical_LF, 'number_Struct':number_Struct}
        save_json(result_table, table_output_path)


def calculate_splits_statics(data_dir):
    total_seeds, total_generalized, total_literal_comp, total_entity_comp, total_temp_comp = 0 ,0 ,0 ,0 ,0
    for split in ['train', 'dev', 'test']:
        data_path = f'{data_dir}/{split}.json'
        split_data = read_json(data_path)
        question_dict = {item['id']:item for item in split_data}  
        id2questiontype = {item['id']:item['origin'] for item in split_data}
        total_ids = list(question_dict.keys())
        seed_ids = []
        generalized_ids = []
        SEA_ids = []
        ETA_ids = []
        TEA_ids = []
        for id in total_ids:
            if id2questiontype[id] == 'Seed':
                seed_ids.append(id)
            elif id2questiontype[id] == 'Generalization':
                generalized_ids.append(id)
            elif id2questiontype[id] == 'Static Entity Augmentation':
                SEA_ids.append(id)
            elif id2questiontype[id] == 'Temporal Entity Augmentation':
                TEA_ids.append(id)
            elif id2questiontype[id] == 'Event Time Augmentation':
                ETA_ids.append(id)
            else: 
                print(id2questiontype[id])
                assert(0)
        print("------------------------------------------------------------------------")
        print(split, 
            "seed", len(seed_ids),
            "generalized", len(generalized_ids),
            "static entity augmentation", len(SEA_ids),
            "temporal entity augmentation", len(TEA_ids),
            "event time augmentation", len(ETA_ids), 
            "total_questions", len(generalized_ids) + len(SEA_ids) + len(TEA_ids) + len(ETA_ids) + len (seed_ids)
        )
        total_seeds += len(seed_ids)
        total_generalized += len(generalized_ids)
        total_literal_comp += len(ETA_ids)
        total_entity_comp += len(SEA_ids)
        total_temp_comp += len(TEA_ids)
    print("------------------------------------------------------------------------")
    print('total', 
        "seed", total_seeds,
        "generalized", total_generalized, 
        "static entity augmentation", total_entity_comp,        
        "temporal entity augmentation", total_temp_comp,
        "event time augmentation", total_literal_comp,
        "total_questions", total_seeds + total_generalized + total_temp_comp + total_entity_comp + total_literal_comp 
    )


def result_analysis(extraction_path, result_path, test_path):
    def avg(l):
        if len(l) == 0:
            return 0
        else:
            return sum(l)/len(l)
    structural_none = []
    structural_tc = []
    structural_m = []
    structural_tc_m = []
    operational_none = []
    operational_cmp = []
    operational_arith = []
    operational_aggr = []
    operational_cmp_arith = []
    operational_cmp_aggr = []
    operational_arith_aggr = []
    operational_full = []
    id2question = {item['id']:item['question'] for item in read_json(test_path)}
    id2complevel = {item['id']:item['comp_level'] for item in read_json(test_path)}
    data = read_json(extraction_path)
    id2aspect = {}
    level1_cmp = []
    level1_art = []
    level1_agg = []
    for item in data:
        id = item['id']
        complexity_aspects = [aspect for aspect in item['analysis'] if len(item['analysis'][aspect]) > 0]
        id2aspect[id] =complexity_aspects
        structural = [aspect for aspect in complexity_aspects if aspect in ['temporal_fact_fusion', 'multi_hop_reasoning']]
        operational = [aspect for aspect in complexity_aspects if aspect not in ['temporal_fact_fusion', 'multi_hop_reasoning']]
        if len(structural) == 0:
            structural_none.append(id)
        if 'temporal_fact_fusion' in structural and 'multi_hop_reasoning' not in structural:
            structural_tc.append(id)
        if 'temporal_fact_fusion' not in structural and 'multi_hop_reasoning' in structural:
            structural_m.append(id)
        if 'temporal_fact_fusion' in structural and 'multi_hop_reasoning' in structural:
            structural_tc_m.append(id)
        has_cmp = False
        has_arith = False
        has_aggr = False
        for aspect in operational:
            if aspect in ['timepoint_comparision', 'duration_comparison']:
                has_cmp = True
            elif aspect in ['granularity_conversion', 'duration_derivation', 'duration_calculation', 'timepoint_shift']:
                has_arith = True
            elif aspect in ['timepoint_ordinal', 'duration_ordinal', 'temporal_statistic', 'frequency']:
                has_aggr = True
            elif aspect in ['fact_counting']:
                continue
            else:
                print(aspect)
                assert(0)
        if not has_cmp and not has_arith and not has_aggr:
            operational_none.append(id)
        elif  has_cmp and not has_arith and not has_aggr:
            operational_cmp.append(id)
        elif not has_cmp and  has_arith and not has_aggr:
            operational_arith.append(id)
        elif not has_cmp and not has_arith and has_aggr:
            operational_aggr.append(id)
        elif has_cmp and has_arith and not has_aggr:
            operational_cmp_arith.append(id)
        elif has_cmp and not has_arith and  has_aggr:
            operational_cmp_aggr.append(id)
        elif not has_cmp and has_arith and has_aggr:
            operational_arith_aggr.append(id)
        elif has_cmp and has_arith and has_aggr:
            operational_full.append(id)
    method_f1s = {}
    for item in read_json(result_path):
        qid = item['id']
        for method, feature in item['methods'].items():
            f1 = feature['evaluate']['f1']
            if method not in method_f1s:
                method_f1s[method] = {}
            method_f1s[method][qid] = f1
    for method, result in method_f1s.items():
        print('--------------------------------------------------------------------------------------------\n\n\n')
        print(method)
        print('avg f1', avg(list(result.values())))
        print('iid f1', avg([f1 for qid, f1 in result.items() if id2complevel[qid] == 'iid']))
        print('compositional f1', avg([f1 for qid, f1 in result.items() if id2complevel[qid] == 'compositional']))
        print('zero-shot f1', avg([f1 for qid, f1 in result.items() if id2complevel[qid] == 'zero-shot']))


def draw_complexity_combination_table(extraction_path):
    print('Analyzing complexity combination of extraction results:', extraction_path)
    data = read_json(extraction_path)
    total_len = len(data)
    structural_m = []
    structural_tc = []
    for item in data:
        id = item['id']
        complexity_aspects = [aspect for aspect in item['analysis'] if len(item['analysis'][aspect]) > 0]
        if 'multi_hop_reasoning' in complexity_aspects and item['analysis']['multi_hop_reasoning'][0] >= 1:
            structural_m.append(id)
        if 'temporal_fact_fusion' in complexity_aspects:
            structural_tc.append(id)
    structural_tc_m = set(structural_m).intersection(structural_tc)
    print('Questions containing both temporal fact fusion and multi hop reasoning')
    print(len(structural_tc_m), ':', 100*len(structural_tc_m)/total_len)

def random_sample_datasets_for_sutime_nuance_check(datasets, population):
    random.seed(1)
    datasets_data = {}
    for dataset in datasets:
        datasets_data[dataset] = []
        if dataset == "C2-TKGQA":
            dataset_dir = "dataset/C2-TKGQA"
            for split in ['test', 'dev', 'train']:
                datasets_data[dataset] += json.load(open(f'{dataset_dir}/{split}.json', 'r'))
        elif dataset == "ETRQA":
            dataset_dir = "dataset/ETRQA"
            datasets_data[dataset] = list(json.load(open(f'{dataset_dir}/extracted_output.json', 'r')).values())
        elif dataset == "MultiTQ":
            dataset_dir = "dataset/MultiTQ/data/MultiTQ/questions"
            for split in ['test', 'dev', 'train']:
                datasets_data[dataset] += json.load(open(f'{dataset_dir}/{split}.json', 'r'))
        elif dataset == "TempQA_WD":
            dataset_dir = "dataset/tempqa-wd/data"
            for split in ['test', 'dev']:
                datasets_data[dataset] += json.load(open(f'{dataset_dir}/{split}.json', 'r'))
    for k in datasets_data:
        random.seed(1)
        datasets_data[k] = random.sample(datasets_data[k], min(population, len(datasets_data[k])))
    return datasets_data


from tqdm import tqdm
if __name__ == "__main__":
    # uncomment to run corresponding analysis
    #-------------------------------- Calculation of Dataset Staticstics  ---------------------------------------------------------
    calculate_splits_statics('../dataset/CR-TKGQA')

    # ------------------------------------ Analysis of Complexity -----------------------------------------------------------------
    analysis_temporal_taxonomy('CR-TKGQA')
    analysis_split_complexity('../dataset/CR-TKGQA', '../analysis_results/CR-TKGQA/complexity_taxonomy.json', '../analysis_results/CR-TKGQA/table_cr_tkgqa_sparql.json')

    analysis_temporal_taxonomy('TempQA-WD')
    analysis_split_complexity('../dataset/tempqa-wd', '../analysis_results/TempQA-WD/complexity_taxonomy.json', '../analysis_results/TempQA-WD/table_tempqa_wd_sparql.json')

    draw_complexity_combination_table('../analysis_results/CR-TKGQA/complexity_taxonomy.json')
    draw_complexity_combination_table('../analysis_results/TempQA-WD/complexity_taxonomy.json') 

    #----------------------------------- Analysis of Experimental Results --------------------------------------------------------
    result_analysis('../analysis_results/CR-TKGQA/complexity_taxonomy.json', '../baselines/results/collect.json', '../dataset/CR-TKGQA/test.json')