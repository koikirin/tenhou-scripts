import asyncio
import datetime
import gzip
import sys
import time
from typing import List

import httpx
import pymongo

db = pymongo.MongoClient().get_database("tenhou")


def convert_starttime(timepat: str, minute: str) -> int:
    birthday = f"{timepat[:8]}{minute}"
    date_time = datetime.datetime.strptime(birthday, '%Y%m%d%H:%M')
    utc_timestamp = date_time.replace(
        tzinfo=datetime.timezone(datetime.timedelta(hours=+9))).timestamp()
    return int(utc_timestamp)


def convert_playtype(playtype: str) -> dict:
    ret = {}
    if "三" in playtype:
        ret["playernum"] = 3
    elif "四" in playtype:
        ret["playernum"] = 4
    if "特" in playtype:
        ret["playerlevel"] = 2
    elif "鳳" in playtype:
        ret["playerlevel"] = 3
    if "東" in playtype:
        ret["playlength"] = 1
    elif "南" in playtype:
        ret["playlength"] = 2
    if "喰" in playtype:
        ret["kuitanari"] = 1
    if "赤" in playtype:
        ret["akaari"] = 1
    if "速" in playtype:
        ret["rapid"] = 1
    return ret


class TenhouLog:
    _sctype: str

    async def _fetch(self, timepat: str):
        raise NotImplementedError

    async def fetch(self, timep: datetime.datetime):
        timepat = f"{timep.year}{timep.month:02}{timep.day:02}{timep.hour:02}"
        # Check if already record, exit
        if db["synclog"].find_one({"_id": f"{self._sctype}{timepat}"}):
            print(f"[{type(self).__name__}] Already synced {timep}.")
            return True
        try:
            documents = await self._fetch(timepat)
            db["matches"].insert_many(documents, ordered=False)
        except Exception as e:
            print("Error: ", repr(e))
            print(f"[{type(self).__name__}] Failed to fetch {timep}.")
            return False
        db["synclog"].insert_one({
            "_id": f"{self._sctype}{timepat}",
            "sctype": self._sctype,
            "sync_time": int(time.time())
        })
        print(
            f"[{type(self).__name__}] Successfully sync {timep} with {len(documents)} documents"
        )
        return True


class TenhouSCCLog(TenhouLog):
    def __init__(self) -> None:
        self._sctype = "c"

    async def _fetch(self, timepat: str):
        url = f"https://tenhou.net/sc/raw/dat/scc{timepat}.html.gz"
        headers = {
            "Referer":
            "https://tenhou.net/sc/raw/",
            "User-Agent":
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
        }
        async with httpx.AsyncClient(headers=headers) as client:
            resp = await client.get(url, headers=headers)
            resp.raise_for_status()
            data = resp.read()
            data = gzip.decompress(data)
            data = data.decode("utf-8")
        documents = []
        for line in data.split("\n"):
            if not line.strip():
                continue
            starttime, during, playtype, paipu, info = line.strip().split(
                "|", maxsplit=4)
            d = {
                "starttime": convert_starttime(timepat, starttime.strip()),
                "sctype": self._sctype,
                "during": during.strip(),
                "url": paipu.split('"')[1],
                "players": [],
                "points": [],
                **convert_playtype(playtype)
            }
            for sinfo in info.split():
                player = sinfo[:sinfo.find("(")].strip()
                point = sinfo[sinfo.find("(") + 1:sinfo.find(")")].strip()
                d["players"].append(player)
                d["points"].append(point)
            assert d["playernum"] == len(d["players"])
            documents.append(d)
        return documents


class TenhouSCBLog(TenhouLog):
    def __init__(self) -> None:
        self._current_version = datetime.datetime(2022, 12, 10, 0)
        self._sctype = "b"

    async def _fetch(self, timepat: str):
        url = f"https://tenhou.net/sc/raw/dat/scb{timepat}.log.gz"
        headers = {
            "Referer":
            "https://tenhou.net/sc/raw/",
            "User-Agent":
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
        }
        async with httpx.AsyncClient(headers=headers) as client:
            resp = await client.get(url, headers=headers)
            resp.raise_for_status()
            data = resp.read()
            data = gzip.decompress(data)
            data = data.decode("utf-8")

        documents = []
        for line in data.split("\n"):
            if not line.strip():
                continue
            starttime, during, playtype, info = line.strip().split("|",
                                                                   maxsplit=3)
            d = {
                "starttime": convert_starttime(timepat, starttime.strip()),
                "sctype": self._sctype,
                "during": int(during.strip()),
                "players": [],
                "points": [],
                **convert_playtype(playtype)
            }
            for sinfo in info.split():
                player = sinfo[:sinfo.find("(")].strip()
                point = sinfo[sinfo.find("(") + 1:sinfo.find(")")].strip()
                d["players"].append(player)
                d["points"].append(point)
            assert d["playernum"] == len(d["players"])
            documents.append(d)
        return documents


ts: List[TenhouLog] = [TenhouSCBLog(), TenhouSCCLog()]


async def main():
    print("Start syncing")
    curdate = datetime.datetime.now()
    for i in range(12):
        for t in ts:
            await t.fetch(curdate - datetime.timedelta(hours=i))


async def sync_old():
    print("Start sync old from last week")
    curdate = datetime.datetime.now()
    for i in range(7 * 24):
        for t in ts:
            await t.fetch(curdate - datetime.timedelta(hours=i))


if len(sys.argv) > 1 and sys.argv[1] == "old":
    asyncio.run(sync_old())
else:
    asyncio.run(main())
