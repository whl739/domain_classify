# -*- coding: utf-8 -*-

import time, random
import redis

rds = redis.StrictRedis(host='localhost', port=6379, db=0)

_KEY_REQUEST_ID = 'request_id'
_KEY_RID_STATUS = 'rid_status'
_KEY_DOMAIN_CATEGORY = 'domain'

def gen_request_id():
    rid = rds.incr(_KEY_REQUEST_ID)
    return '%d%05d%d' % (time.time(), random.randint(0, 99999), rid)

def update_rid_status(rid, status):
    return rds.hset(_KEY_RID_STATUS, rid, status)

def get_rid_status(rid):
    return rds.hget(_KEY_RID_STATUS, rid)

def set_domain_category(domain_category):
    pipe = rds.pipeline(transaction=False)
    for domain, category in domain_category.items():
        pipe.hset(_KEY_DOMAIN_CATEGORY, domain, category)
    pipe.execute()

def get_domain_category(domains):
    return rds.hmget(_KEY_DOMAIN_CATEGORY, *domains)

def set_category(rid, category):
    for cate, count in category.items():
        rds.zadd(rid, count, cate)

def get_category(rid):
    res = rds.zrange(rid, 0, -1, withscores=True, score_cast_func=int)
    if not res:
        return None

    category = {r[0]:r[1] for r in res}
    return category

class RdsQueue(object):
    def __init__(self, key, redis_connection, timeout=0):
        self.key = key
        self.redis_connection = redis_connection
        self.timeout = timeout

    def send_msg(self, msg):
        self.redis_connection.rpush(self.key, msg)

    def deal_msg(self, callback):
        while True:
            try:
                _, msg = self.redis_connection.blpop(self.key, timeout=self.timeout)
                if msg:
                    callback(msg)
            except Exception as e:
                print e

domain_crawler_mq = RdsQueue('domain_crawler_mq', rds)
