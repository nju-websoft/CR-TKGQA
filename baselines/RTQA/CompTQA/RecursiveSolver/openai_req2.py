# We will rewrite the `OpenaiReq` class to use asyncio and aiohttp for asynchronous requests

import asyncio
import aiohttp
import os
import json
import random
from openai import OpenAI

def read_txt(file_path):
    lines = open(file_path, 'r', encoding='utf-8').readlines()
    lines = [line.strip() for line in lines]
    return [line for line in lines if line != '']

class AsyncOpenaiReq:
    def __init__(self, api_key, base_url):
        self.api_key = api_key
        self.base_url = base_url
        self.cache = {}
        self.cache_path = "./cache.jsonl"
        # put your llm keys here
        self.api_keys = []
        if os.path.exists(self.cache_path):
            with open(self.cache_path, "r") as f:
                for line in f:
                    datum = json.loads(line.strip())
                    self.cache[tuple(datum["input"])] = datum["response"]

    async def req2deepseekr1(self, prompt, model="deepseek-chat", temperature=0.0, max_tokens=1024, stop=None, use_cache=False):
        assert isinstance(prompt, str)
        api_key = random.choice(self.api_keys)
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }

        data = {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": temperature,
            "max_tokens": max_tokens,
            "stop": stop
        }

        for attempt in range(3):
            try:
                async with aiohttp.ClientSession(trust_env=True) as session:
                    async with session.post(f"https://api.deepseek.com/chat/completions", headers=headers, json=data) as resp:
                        if resp.status != 200:
                            raise Exception(f"API returned status code {resp.status}")
                        response = await resp.json()
                        break
            except Exception as e:
                print(f"[Attempt {attempt+1}] OpenAI API Error:", e)
                await asyncio.sleep(1)
        else:
            return ['openai error'], False

        try:
            choice = response['choices'][0]
            response_dict = {
                "finish_reason": choice["finish_reason"],
                "content": choice["message"]["content"]
            }
            
        except Exception as e:
            print("Parsing OpenAI Response Error:", e)
            return ['openai error'], False

        return [response_dict], True

    async def req2openai(self, prompt, model="gpt-4o-mini", temperature=0.0, max_tokens=1024, stop=None, logprobs=True, use_cache=False):
        assert isinstance(prompt, str)
        input_key = (prompt, model, max_tokens, stop, logprobs)
        if use_cache and temperature == 0 and input_key in self.cache:
            return self.cache[input_key], True

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        data = {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": temperature,
            "max_tokens": max_tokens,
            "stop": stop,
            "logprobs": logprobs
        }

        for attempt in range(3):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(f"{self.base_url}/chat/completions", headers=headers, json=data) as resp:
                        if resp.status != 200:
                            raise Exception(f"API returned status code {resp.status}")
                        response = await resp.json()
                        break
            except Exception as e:
                print(f"[Attempt {attempt+1}] OpenAI API Error:", e)
                await asyncio.sleep(1)
        else:
            return ['openai error'], False

        try:
            choice = response['choices'][0]
            response_dict = {
                "finish_reason": choice["finish_reason"],
                "content": choice["message"]["content"],
                "logprobs": {
                    "tokens": [t["token"] for t in choice.get("logprobs", {}).get("content", [])],
                    "token_logprobs": [t["logprob"] for t in choice.get("logprobs", {}).get("content", [])]
                }
            }
        except Exception as e:
            print("Parsing OpenAI Response Error:", e)
            return ['openai error'], False

        if temperature == 0:
            if input_key not in self.cache:
                self.cache[input_key] = [response_dict]
                with open(self.cache_path, "a") as f:
                    f.write(f"{json.dumps({'input': input_key, 'response': [response_dict]})}\n")

        return [response_dict], True



