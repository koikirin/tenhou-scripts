import asyncio
import datetime
import gzip
import json

import httpx
import pymongo
import pymongo.errors

with open("config.json") as f:
    config = json.load(f)

db = pymongo.MongoClient(config["database"]).get_database("tenhou")


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
            print(repr(e))
            print(f"[{type(self).__name__}] Failed to fetch {timep}:{new}.")
            return False
        print(
            f"[{type(self).__name__}] Successfully sync {timep}:{new} with {len(documents)} documents"
        )
        return True


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
        self._current_version = datetime.datetime(2022, 12, 10, 0)
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
                print("Dup key", d["_id"])
            else:
                dd[d["_id"]] = 1
            documents.append(d)
        return documents


# ts: List[TenhouLog] = [TenhouSCBLog()]
t = TenhouSCBLog()


async def main():
    curdate = datetime.datetime.now(_tz9)
    print(f"[Start] {curdate}")
    cursor = db["synclog"].find().sort("$natural", pymongo.DESCENDING).limit(1)

    for synclog in cursor:
        break
    else:
        ts = int((curdate - datetime.timedelta(days=3)).timestamp())
        synclog = {"start": ts, "end": ts}
    enddate = await sync_time_region(synclog["end"])

    if enddate:
        db["synclog"].insert_one({
            "start": synclog["start"],
            "end": int(enddate.timestamp())
        })
        print("[Finish]")
    else:
        print("[Fail]")


import zoneinfo

_tz9 = zoneinfo.ZoneInfo("Asia/Tokyo")
_tz8 = zoneinfo.ZoneInfo("Asia/Shanghai")


async def sync_time_region(start: int, end: int = 0):
    curdate = datetime.datetime.now(_tz9) - datetime.timedelta(hours=1)
    # If today, calc by hour, otherwise, calc by day
    startdate = datetime.datetime.fromtimestamp(start, tz=_tz9)
    enddate = datetime.datetime.fromtimestamp(end, tz=_tz9) if end else curdate
    while startdate < enddate:
        if curdate.date() == startdate.date():
            while startdate < curdate:
                if not await t.fetch(startdate, new=True):
                    return startdate - datetime.timedelta(hours=1)
                startdate += datetime.timedelta(hours=1)
            await t.fetch(startdate, new=True)
            break
        else:
            if not await t.fetch(startdate, new=False):
                return False

            startdate += datetime.timedelta(days=1)
            startdate = datetime.datetime(year=startdate.year,
                                          month=startdate.month,
                                          day=startdate.day,
                                          tzinfo=_tz9)
    return enddate


asyncio.run(main())
