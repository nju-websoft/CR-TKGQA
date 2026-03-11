
PROXIES = {
    "http": "http://114.212.86.184:6789",  # HTTP 代理
    "https": "http://114.212.86.184:6789", # HTTPS 代理（注意协议可能仍是http）
}
WIKIDATA_OFFICIAL_ENDPOINT = "https://query.wikidata.org/sparql"
LOCAL_WIKIDATA2023_ENDPOINT =  "http://114.212.81.217:8895/sparql"


WIKIDATA_STD_PREFIX = """
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX wikibase: <http://wikiba.se/ontology#>
PREFIX p: <http://www.wikidata.org/prop/>
PREFIX ps: <http://www.wikidata.org/prop/statement/>
PREFIX pq: <http://www.wikidata.org/prop/qualifier/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX bd: <http://www.bigdata.com/rdf#>
"""

USE_LOCAL_ENDPOINT = False

WIKIDATA_FULL_PREFIX = """

"""