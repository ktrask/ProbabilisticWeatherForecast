import asyncio

import aiofiles
from aiohttp import ClientSession


async def make_request(session, url):
    response = await session.request(method="GET", url=url)
    filename = url.split('/')[-1]
    async for data in response.content.iter_chunked(1024):
        async with aiofiles.open(filename, "ba") as f:
            await f.write(data)
    return filename


BASEURL = "https://data.ecmwf.int/forecasts"

# Naming convention [ROOT]/[yyyymmdd]/[HH]z/[model]/[resol]/[stream]/[yyyymmdd][HH]0000-[step][U]-[stream]-[type].[format]
# https://confluence.ecmwf.int/display/DAC/ECMWF+open+data%3A+real-time+forecasts+from+IFS+and+AIFS

from datetime import datetime, timedelta
yesterday = datetime.now() - timedelta(1)


modeldate = datetime.strftime(yesterday, '%Y%m%d')

steps = range(0,361,6)
modeltime = "12" #00 or 12
#latest model from yester should always be available

filetypes = ["grib2", "index"]


async def main():
    urls = []
    for step in steps:
        for filetype in filetypes:
            url = f"{BASEURL}/{modeldate}/{modeltime}z/ifs/0p25/enfo/{modeldate}{modeltime}0000-{step}h-enfo-ef.{filetype}"
            urls.append(url)
    print(len(urls))
    while len(urls) > 0:
        myChunk = []
        if len(urls) > 10:
            for i in range(10):
                myChunk.append(urls.pop())
        else:
            for i in range(len(urls)):
                myChunk.append(urls.pop())
        print(myChunk)
        async with ClientSession() as session:
            coros = [make_request(session, url) for url in myChunk[0:2]]
            result_files = await asyncio.gather(*coros)
        print(result_files)


asyncio.run(main())
