from typing import Dict, List, OrderedDict
import json
import time
import logging
import asyncio
import pymongo.errors
import httpx
import pymongo
import pymongo.errors

db = pymongo.MongoClient().get_database("tenhou")

async def fetch(username):
    async with httpx.AsyncClient() as sess:
        resp = await sess.get(f"http://127.0.0.1:7235/rank",
                                params={"username": username},
                                timeout=7)
        data = resp.content
        data = json.loads(data)
        return data


_cached_gradechanges: OrderedDict[str, Dict] = OrderedDict()


def get_record_key(r):
    return f'{r["starttime"]}.{r["username"]}'


async def update_gradechanges():
    curtime = time.time()
    try:
        async with httpx.AsyncClient() as sess:
            resp = await sess.get(f"https://nodocchi.moe/api/gradechanges.php")
            result = resp.json()

        # Clean cached_gradechanges
        while len(_cached_gradechanges) > len(result) + 500:
            _cached_gradechanges.popitem(False)

        keys = set(map(get_record_key, result))
        new_keys = keys - set(_cached_gradechanges.keys())
        result: List[Dict] = list(
            filter(lambda r: get_record_key(r) in new_keys, result))

        print(f"Checking grade changes appending: {len(result)}")
        for res in result:
            try:
                key = get_record_key(res)
                res["starttime"] = int(res["starttime"])
                await fetch(res["username"])
                _cached_gradechanges[key] = res
            except Exception as e:
                print(f'Failed at {res["username"]} {repr(e)}')

    except Exception as e:
        print(f'{e}')
    print(f'Finished. cost {time.time() - curtime} s')

try:
    asyncio.run(update_gradechanges())
except KeyboardInterrupt:
    pass
