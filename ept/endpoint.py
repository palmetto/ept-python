#
# Endpoint module
#
import json

import aiohttp
import aiofiles
import asyncio
from aiobotocore.session import get_session
from urllib.parse import urlparse

from .pool import TaskPool


class Driver(object):
    def __init__(self, root, concurrency=1):
        self.root = root
        self.parts = []
        self.concurrency = concurrency

class S3Driver(Driver):
    def __init__(self, root):
        super().__init__(root)

    async def get(self, part, client=None):

        url = self.root + part
        o = urlparse(url , allow_fragments=False)

        if client is not None:
            response = await client.get_object(Bucket=o.netloc, Key=o.path.lstrip('/'))
            async with response['Body'] as stream:
                return await stream.read()

        session = get_session()
        async with session.create_client('s3') as client:
            response = await client.get_object(Bucket=o.netloc, Key=o.path.lstrip('/'))
            async with response['Body'] as stream:
                return await stream.read()


class Http(Driver):
    def __init__(self, root, query=None):
        super(Http, self).__init__(root)
        self.query = query

    async def download(self, session, url):
        async with session.get(url) as response:
            return await response.read()

    async def get(self, part, session=None, tpool=None):
        url = self.root + part
        if self.query is not None:
            url += '?' + self.query
        if tpool:
            return await tpool.put(self.download(session, url))
        if session:
            async with session.get(url) as response:
                return await response.read()
        else:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    return await response.read()

    def stage(self, part):
        self.parts.append(part)

    async def bulk(self):
        connector = aiohttp.TCPConnector(limit=None)
        async with aiohttp.ClientSession(connector=connector) as session, TaskPool(
            self.concurrency
        ) as tasks:
            for part in self.parts:
                await tasks.put(self.download(session, self.root + part))

        return tasks


class File(Driver):
    def __init__(self, root):
        super(File, self).__init__(root)

    async def get(self, part, session=None, tpool=None):
        url = self.root
        if part:
            url = url + part

        async with aiofiles.open(url, "rb") as d:
            return await d.read()


class Endpoint(object):
    def __init__(self, root, query=None):
        self.root = root
        self.query = query

        if root.startswith("s3://") or root.startswith("s3://"):
            self.remote = True
            self.driver = S3Driver(root)
        else:
            self.remote = False
            self.driver = File(root)

    def get(self, part):
        loop = asyncio.get_event_loop()
        o = loop.run_until_complete(self.driver.get(part))
        return o

    async def aget(self, part=None, session=None, tpool=None):
        return await self.driver.get(part, session)
