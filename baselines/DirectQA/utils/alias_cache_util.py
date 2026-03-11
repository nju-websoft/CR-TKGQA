import json
import os

from utils.wikidata_util import *

cache_path = 'baseline/directQA/cache/alias.jsonl'
cache = dict()
for line in open(cache_path, 'r', encoding='utf-8'):
    line = line.strip()
    if line:
        try:
            content = json.loads(line)
            assert isinstance(content, dict) and len(content.keys()) == 1
            key = list(content.keys())[0]
            value = content[key]
            cache[key] = value
        except json.JSONDecodeError:
            print(print(f"解析失败的行内容: {line}"))

def dump_cache(id: str, alias: list):
    data = {id : alias}
    with open(cache_path, 'a', encoding='utf-8') as f:
        json_line = json.dumps(data, ensure_ascii=False)
        f.write(json_line + '\n')

def get_alias_cache(id: str):
    if id in cache.keys():
        return cache[id]
    alias = query_alias(id)
    if not isinstance(alias, list):
        alias = [alias]
    cache[id] = alias
    dump_cache(id, alias)
    return alias
