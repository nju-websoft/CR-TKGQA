import requests
from urllib.parse import quote, unquote
import json
import time
import logging
logging.basicConfig(
    filename="app.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

OFFICIAL_SLEEP_TIME = 3
TIME_OUT = 6000000
USE_DEFAULT_TIME_OUT = True

def get_endpoint(type='official'):
    wikidata_endpoint = {
        "official": "https://query.wikidata.org/sparql",
        "local": "http://114.212.81.217:8895/sparql"
    }
    return wikidata_endpoint[type]

def add_prefix(sparql: str):
    prefix = '''PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX wikibase: <http://wikiba.se/ontology#>
PREFIX p: <http://www.wikidata.org/prop/>
PREFIX ps: <http://www.wikidata.org/prop/statement/>
PREFIX pq: <http://www.wikidata.org/prop/qualifier/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX bd: <http://www.bigdata.com/rdf#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
'''
    return prefix + sparql

def query(sparql: str, endpoint='official'):
    headers = {'Accept':'application/sparql-results+json'}
    if endpoint == 'local':
        sparql = add_prefix(sparql)
    url = f'{get_endpoint(endpoint)}?query={quote(sparql)}'
    try:
        query_request = requests.get(url, headers=headers)
        if endpoint == 'official':
            time.sleep(OFFICIAL_SLEEP_TIME)
        if query_request.status_code != 200:
            print(query_request.text)
            print(f"Endpoint {endpoint.upper()} returns error!")
            if endpoint == 'official':
                print(f"Now use endpoint Local!")
                return query(sparql, endpoint='local')
            return None, None
        response = json.loads(query_request.text)
        vars = response['head']['vars']
        assert(len(vars) > 0)
        if len(response['results']['bindings']) == 0:
            return None, None
        query_results = list()
        for binding in response['results']['bindings']:
            query_result = dict()
            for var in vars:
                if var not in binding.keys():
                    print(vars)
                    print(binding)
                    return None, None
                if binding[var]['type'] == 'uri':
                    query_result[var] = binding[var]['value'].split('/')[-1]
                else:
                    query_result[var] = binding[var]['value']
            query_results.append(query_result)
        type_dict = dict()
        binding = response['results']['bindings'][0]
        for var in vars:
            assert(binding[var]['type'] in ['uri', 'typed-literal', 'literal'])
            type_dict[var] = binding[var]['type']
            if type_dict[var] == 'typed-literal':
                data_type = binding[var]['datatype'].split('#')[-1]
                type_dict[var] = type_dict[var] + '#' + data_type
        return type_dict, query_results
    except:
        print(f"Endpoint {endpoint.upper()} returns error!")
        if endpoint == 'official':
            print(f"Now use endpoint Local!")
            return query(sparql, endpoint='local')
        return None, None

def query_label(id: str):
    sparql = f'''
SELECT DISTINCT ?label WHERE {{
    wd:{id} rdfs:label ?label .
    FILTER(LANG(?label) = "en")
}}
LIMIT 1
'''
    print(sparql)
    _, query_results = query(sparql)
    if query_results is None:
        return "NOLABEL"
    return query_results[0]['label']

def query_alias(id: str):
    sparql = f'''
SELECT DISTINCT ?alias WHERE {{
    wd:{id} skos:altLabel ?alias .
    FILTER(LANG(?alias) = "en")
}}
'''
    print(sparql)
    _, query_results = query(sparql)
    if query_results == None:
        return []
    alias = [result['alias'] for result in query_results]
    return alias



# def query(sparql: str, endpoint='local'):
#     headers = {'Accept':'application/sparql-results+json'}
#     if endpoint == 'local':
#         sparql = add_prefix(sparql)
#     url = f'{get_endpoint(endpoint)}?query={quote(sparql)}'
#     try:
#         query_request = requests.get(url, headers=headers)
#     except:
#         print(f"Endpoint {endpoint.upper()} returns exception!")
#         return None, None
#     if query_request.status_code != 200:
#         assert query_request.status_code in [500, 502]
#         if query_request.status_code == 500:
#             print(query_request.status_code)
#             print(query_request.text)
#             print(f"Endpoint {endpoint.upper()} returns error!")
#             return None, None
#         else:
#             print(f"Endpoint {endpoint.upper()} shut down!")
#             exit()
#     response = json.loads(query_request.text)
#     vars = response['head']['vars']
#     assert(len(vars) > 0)
#     if len(response['results']['bindings']) == 0:
#         return None, None
#     query_results = list()
#     for binding in response['results']['bindings']:
#         query_result = dict()
#         for var in vars:
#             if var not in binding.keys():
#                 print(vars)
#                 print(binding)
#                 return None, None
#             if binding[var]['type'] == 'uri':
#                 query_result[var] = binding[var]['value'].split('/')[-1]
#             else:
#                 query_result[var] = binding[var]['value']
#         query_results.append(query_result)
#     type_dict = dict()
#     binding = response['results']['bindings'][0]
#     for var in vars:
#         assert(binding[var]['type'] in ['uri', 'typed-literal', 'literal'])
#         type_dict[var] = binding[var]['type']
#         if type_dict[var] == 'typed-literal':
#             data_type = binding[var]['datatype'].split('#')[-1]
#             type_dict[var] = type_dict[var] + '#' + data_type
#     return type_dict, query_results
