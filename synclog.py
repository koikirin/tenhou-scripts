import asyncio
import datetime
import gzip
import json
import sys
from typing import List

import httpx
import pymongo
import pymongo.errors

with open("config.json") as f:
    config = json.load(f)

db = pymongo.MongoClient(config["database"]).get_database("tenhou")

import zoneinfo

_tz9 = zoneinfo.ZoneInfo("Asia/Tokyo")
_tz8 = zoneinfo.ZoneInfo("Asia/Shanghai")


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
    if "般" in playtype:
        ret["playerlevel"] = 0
    elif "上" in playtype:
        ret["playerlevel"] = 1
    elif "特" in playtype:
        ret["playerlevel"] = 2
    elif "鳳" in playtype:
        ret["playerlevel"] = 3
    elif "Ｗ" in playtype:
        ret["playerlevel"] = 9
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
    if "祝" in playtype:
        ret["shuugi"] = 1
    return ret


class TenhouLog:
    _sctype: str

    async def _fetch(self, timepat: str, new: bool):
        raise NotImplementedError

    async def fetch(self, timep: datetime.datetime, new: bool = False):
        timepat = f"{timep.year}{timep.month:02}{timep.day:02}{timep.hour:02}"
        try:
            documents = await self._fetch(timepat, new=new)
            if documents:
                db["matches"].insert_many(documents, ordered=False)
        except pymongo.errors.BulkWriteError as e:
            assert isinstance(e.details, dict)
            errd = {}
            for key in e.details:
                if key.startswith("n"):
                    errd[key] = e.details[key]
            print(
                f"[{type(self).__name__}] Result of bulk insert {timep}:{new}: {errd}."
            )
            return True
        except Exception as e:
            raise e
            print(repr(e))
            print(f"[{type(self).__name__}] Failed to fetch {timep}:{new}.")
            return False
        print(
            f"[{type(self).__name__}] Successfully sync {timep}:{new} with {len(documents)} documents"
        )
        return True

    async def sync(self, start: int, end: int = 0) -> datetime.datetime:
        raise NotImplemented


# class TenhouSCCLog(TenhouLog):
#     def __init__(self) -> None:
#         self._sctype = "c"

#     async def _fetch(self, timepat: str):
#         url = f"https://tenhou.net/sc/raw/dat/scc{timepat}.html.gz"
#         headers = {
#             "Referer":
#             "https://tenhou.net/sc/raw/",
#             "User-Agent":
#             "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
#         }
#         async with httpx.AsyncClient(headers=headers) as client:
#             resp = await client.get(url, headers=headers)
#             resp.raise_for_status()
#             data = resp.read()
#             data = gzip.decompress(data)
#             data = data.decode("utf-8")
#         documents = []
#         for line in data.split("\n"):
#             if not line.strip():
#                 continue
#             starttime, during, playtype, paipu, info = line.strip().split(
#                 "|", maxsplit=4)
#             d = {
#                 "starttime": convert_starttime(timepat, starttime.strip()),
#                 "sctype": self._sctype,
#                 "during": during.strip(),
#                 "url": paipu.split('"')[1],
#                 "players": [],
#                 "points": [],
#                 **convert_playtype(playtype)
#             }
#             for sinfo in info.split():
#                 player = sinfo[:sinfo.find("(")].strip()
#                 point = sinfo[sinfo.find("(") + 1:sinfo.find(")")].strip()
#                 d["players"].append(player)
#                 d["points"].append(point)
#             assert d["playernum"] == len(d["players"])
#             d["_id"] = f"{self._sctype}.{d['starttime']}.{d['players'][0]}"
#             documents.append(d)
#         return documents


class TenhouSCBLog(TenhouLog):
    def __init__(self) -> None:
        self._sctype = "b"

    def _fetch(self, timepat: str, new: bool):
        if new:
            return self._fetch_new(timepat)
        else:
            return self._fetch_old(timepat)

    async def _fetch_new(self, timepat: str):
        url = f"https://tenhou.net/sc/raw/dat/scb{timepat}.log.gz"
        print(url)
        headers = {
            "Referer":
            "https://tenhou.net/sc/raw/",
            "User-Agent":
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
        }
        async with httpx.AsyncClient(headers=headers) as client:
            resp = await client.get(url, headers=headers)
            # Raise if the timepat too old ?
            if resp.status_code == 404:
                return []
            resp.raise_for_status()
            data = resp.read()
            data = gzip.decompress(data)
            data = data.decode("utf-8")
        return self._process(timepat, data)

    async def _fetch_old(self, timepat: str):
        timepat = timepat[:8]
        url = f"https://tenhou.net/sc/raw/dat/{timepat[:4]}/scb{timepat[:8]}.log.gz"
        print(url)
        headers = {
            "Referer":
            "https://tenhou.net/sc/raw/",
            "User-Agent":
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
        }
        async with httpx.AsyncClient(headers=headers) as client:
            resp = await client.get(url, headers=headers)
            if resp.status_code == 404:
                print("Switch to fetch recent logs")
                ret = []
                for i in range(24):
                    tpat = f"{timepat}{i:02}"
                    ret.extend(await self._fetch_new(tpat))
                return ret
            resp.raise_for_status()
            data = resp.read()
            data = gzip.decompress(data)
            data = data.decode("utf-8")
        return self._process(timepat, data)

    def _process(self, timepat: str, data: str):
        documents = []
        dd = {}
        dup = False
        for line in data.split("\n"):
            if not line.strip():
                continue
            starttime, during, playtype, info = line.strip().split("|",
                                                                   maxsplit=3)
            starttime = convert_starttime(timepat, starttime.strip())
            during = int(during.strip())
            d = {
                "starttime": starttime,
                "endtime": starttime + 60 * during,
                "sctype": self._sctype,
                "during": during,
                "players": [],
                "points": [],
                **convert_playtype(playtype)
            }
            d_player = ""
            for sinfo in info.split():
                player = sinfo[:sinfo.find("(")].strip()
                point = sinfo[sinfo.find("(") + 1:sinfo.find(")")].strip()
                d["players"].append(player)
                d["points"].append(point)
                if not d_player and player != "NoName":
                    d_player = player
            assert d["playernum"] == len(d["players"])

            d["_id"] = f"{self._sctype}.{d['starttime']}.{d['during']}.{d_player}"
            if d["_id"] in dd:
                # print("Dup key", d["_id"])
                dup = True
            else:
                dd[d["_id"]] = 1
            documents.append(d)
        if dup: print('Duplicate Key!')
        return documents
    
    async def sync(self, start: int, end: int = 0):
        curdate = datetime.datetime.now(_tz9) - datetime.timedelta(hours=1)
        # If today, calc by hour, otherwise, calc by day
        startdate = datetime.datetime.fromtimestamp(start, tz=_tz9) - datetime.timedelta(hours=1)
        enddate = datetime.datetime.fromtimestamp(end, tz=_tz9) if end else curdate
        while startdate < enddate:
            if curdate.date() == startdate.date():
                while startdate < curdate:
                    if not await self.fetch(startdate, new=True):
                        return startdate - datetime.timedelta(hours=1)
                    startdate += datetime.timedelta(hours=1)
                await self.fetch(startdate, new=True)
                break
            else:
                if not await self.fetch(startdate, new=False):
                    return False

                startdate += datetime.timedelta(days=1)
                startdate = datetime.datetime(year=startdate.year,
                                            month=startdate.month,
                                            day=startdate.day,
                                            tzinfo=_tz9)
        return enddate
    

class TenhouSCALog(TenhouLog):
    def __init__(self) -> None:
        self._sctype = "a"

    def _fetch(self, timepat: str, new: bool):
        if new:
            return self._fetch_new(timepat)
        else:
            return self._fetch_old(timepat)

    async def _fetch_new(self, timepat: str):
        url = f"https://tenhou.net/sc/raw/dat/sca{timepat[:8]}.log.gz"
        print(url)
        headers = {
            "Referer":
            "https://tenhou.net/sc/raw/",
            "User-Agent":
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
        }
        async with httpx.AsyncClient(headers=headers) as client:
            resp = await client.get(url, headers=headers)
            # Raise if the timepat too old ?
            if resp.status_code == 404:
                return []
            resp.raise_for_status()
            data = resp.read()
            data = gzip.decompress(data)
            data = data.decode("utf-8")
        return self._process(timepat, data)

    async def _fetch_old(self, timepat: str):
        timepat = timepat[:8]
        url = f"https://tenhou.net/sc/raw/dat/{timepat[:4]}/sca{timepat[:8]}.log.gz"
        print(url)
        headers = {
            "Referer":
            "https://tenhou.net/sc/raw/",
            "User-Agent":
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
        }
        async with httpx.AsyncClient(headers=headers) as client:
            resp = await client.get(url, headers=headers)
            if resp.status_code == 404:
                print("Switch to fetch recent logs")
                return await self._fetch_new(timepat)
            resp.raise_for_status()
            data = resp.read()
            data = gzip.decompress(data)
            data = data.decode("utf-8")
        return self._process(timepat, data)

    def _process(self, timepat: str, data: str):
        documents = []
        dd = {}
        dup = False
        for line in data.split("\n"):
            if not line.strip():
                continue
            lobby, starttime, playtype, info = line.strip().split("|",
                                                                   maxsplit=3)
            lobby = lobby.strip()
            starttime = convert_starttime(timepat, starttime.strip())
            d = {
                "starttime": starttime,
                "sctype": self._sctype,
                "lobby": int(lobby[1:]),
                "rawlobby": lobby,
                "players": [],
                "points": [],
                **convert_playtype(playtype)
            }
            d_player, d_point = "", ""
            for sinfo in info.split():
                player = sinfo[:sinfo.find("(")].strip()
                point = sinfo[sinfo.find("(") + 1:sinfo.find(")")].strip()
                shuugi = None
                if "," in point:
                    point, shuugi = point.split(",")
                d["players"].append(player)
                d["points"].append(point)
                if shuugi:
                    d.setdefault("shuugis", []).append(shuugi[:-1])
                if not d_player and player != "NoName":
                    d_player = player
                elif not d_point:
                    d_point = point
            assert d["playernum"] == len(d["players"])

            d["_id"] = f"{self._sctype}.{d['starttime']}.{d['rawlobby']}.{d_player if d_player else d_point}"
            if d["_id"] in dd:
                # print("Dup key", d["_id"])
                dup = True
            else:
                dd[d["_id"]] = 1
            documents.append(d)
        if dup: print('Duplicate Key!')
        return documents

    async def sync(self, start: int, end: int = 0):
        curdate = datetime.datetime.now(_tz9) - datetime.timedelta(hours=1)
        # If today, calc by hour, otherwise, calc by day
        startdate = datetime.datetime.fromtimestamp(start, tz=_tz9) - datetime.timedelta(hours=1)
        enddate = datetime.datetime.fromtimestamp(end, tz=_tz9) if end else curdate
        while startdate < enddate:
            if curdate.date() == startdate.date():
                await self.fetch(startdate, new=True)
                break
            else:
                if not await self.fetch(startdate, new=False):
                    return False

                startdate += datetime.timedelta(days=1)
                startdate = datetime.datetime(year=startdate.year,
                                            month=startdate.month,
                                            day=startdate.day,
                                            tzinfo=_tz9)
        return enddate
    

providers: List[TenhouLog] = [TenhouSCBLog(), TenhouSCALog()]

async def main():
    curdate = datetime.datetime.now(_tz9)
    print(f"[Start] {curdate}")
    for t in providers:
        cursor = db["synclog"].find({
            "sctype": t._sctype,
        }).sort("$natural", pymongo.DESCENDING).limit(1)

        for synclog in cursor:
            break
        else:
            ts = int((curdate - datetime.timedelta(days=3)).timestamp())
            synclog = {"start": ts, "end": ts}
        enddate = await t.sync(synclog["end"])

        if enddate:
            db["synclog"].insert_one({
                "start": synclog["start"],
                "end": int(enddate.timestamp()),
                "sctype": t._sctype,
            })
            print("[Finish]", t._sctype)
        else:
            print("[Fail]", t._sctype)

if len(sys.argv) >= 3 and sys.argv[-3] == "sync":
    asyncio.run(providers[int(sys.argv[-2])].sync(int(sys.argv[-1])))
else:
    asyncio.run(main())
    