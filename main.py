import os
from typing import Optional

from aiohttp import web
import datetime
import math
import redis.asyncio as redis
import logging
from elasticsearch import AsyncElasticsearch


redis_url = os.getenv('REDIS_URL')
if not redis_url:
    raise Exception('REDIS_URL environment variable not set')

elastic_url = os.getenv('ELASTIC_URL')

pool = redis.connection.ConnectionPool.from_url(url=redis_url)
es = AsyncElasticsearch(hosts=[elastic_url])


async def handle(request):
    name = request.match_info.get('name', "Anonymous")
    text = "Hello, " + name
    return web.Response(text=text)


def get_next_list(tenant_id: str, timestamp: Optional[float] = None) -> str:
    minutes = math.floor(timestamp or datetime.datetime.now().timestamp() / 60)
    return f'{tenant_id}_{minutes}'


async def update_visit_expire(request):
    tenant_id = request.match_info.get('tenantId')
    browser_id = request.match_info.get('browserId')
    next_list = get_next_list(tenant_id)

    command = """
    local visitor_time = redis.call('hget', KEYS[1]..'_'..KEYS[2] ,'tt')
        if (visitor_time==false) then
            return 0
        end
        redis.call('zadd', KEYS[3], visitor_time, KEYS[2])
        redis.call('expire', KEYS[3], 120)
        redis.call('expire', KEYS[1]..'_'..KEYS[2], 45);
    """

    with await pool.get_connection("update_visit_expire") as conn:
        result = await conn.execute('EVAL', command, 3, tenant_id, browser_id, next_list)
        logging.error("result: %s", result)
    return web.Response(text="")

async def track_button_impression(request):
    time = await get_time()
    command = """
    redis.call('hincrby', KEYS[1], 'c', 1)
    redis.call('hmset', KEYS[1], 'u', ARGV[1], 't', ARGV[2])
    redis.call('expire', KEYS[1], 5400)
    """
    url_param = request.query.get('p')

    with await pool.get_connection("track_button_impression") as conn:
        result = await conn.execute("eval", command, 1, "la-hosted_" + request.query.get("i"), url_param, time)
        logging.error("result: %s", result)
    return web.Response(text="")


async def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


async def track_visit(request):
    time = datetime.datetime.now().timestamp()
    next_list = get_next_list("la-hosted", time)
    current_list = get_next_list("la-hosted", time - 60)
    command = """
    redis.call('hmset', KEYS[1], 's', ARGV[1], 'dlv', ARGV[2], 'tt', ARGV[3], 'u', ARGV[4], 'r', ARGV[5], 'i', ARGV[6], 'ua', ARGV[7], 'sc', ARGV[8], 'ud', ARGV[9], 'vn', ARGV[10])
                redis.call('hsetnx', KEYS[1], 'dfv', ARGV[2])
                redis.call('expire', KEYS[1], 45)
                redis.call('zadd', KEYS[2], ARGV[3], ARGV[11])
                redis.call('expire', KEYS[2], 70)
                redis.call('zadd', KEYS[3], ARGV[3], ARGV[11])
                redis.call('expire', KEYS[3], 140)
    """

    with await pool.get_connection("track_visit") as conn:
        browser = request.query.get("b")
        session = request.query.get("s")
        page_title = request.query.get("pt")
        page_url = request.query.get("url")
        page_ref = request.query.get("ref")
        screen = request.query.get("sr")
        user_details = request.query.get("ud")
        now = await get_time()
        visitor_new = request.query.get("vn")
        ip = request.remote
        jsTrack = request.query.get("jstk")
        result = await conn.execute("eval", command, 3, "la-hosted_"+ browser, current_list, next_list, session, now, datetime.datetime.now().timestamp(), page_url, page_ref, ip, request.headers["user_agent"], screen, user_details, visitor_new, browser)
        logging.error("result: %s", result)


        index = "la_perf_pagevisit_v1.1_" + datetime.date.today().strftime("%Y_%m_%d")

        result = await es.index(index=index, document={"b":browser, "dv":now, "t":page_title, "u":page_url, "r":page_ref, "tenant_id":"la-hosted"})
        logging.error("result: %s", result)


    return web.Response(text="")



app = web.Application()
app.add_routes([web.get('/', handle),
                web.get('/update_visit_expire/{session}/{tenantId}/{browserId}', update_visit_expire),
                web.get('/track_button_impression', track_button_impression),
                web.get('/track_visit', track_visit)
                ])

if __name__ == '__main__':
    web.run_app(app)
