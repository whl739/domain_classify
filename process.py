# -*- coding: utf-8 -*-

import json
import multiprocessing

from collections import defaultdict

from utils.crawler import crawl
from utils.rds import update_rid_status, get_domain_category,\
set_category, domain_crawler_mq

def scan_domain(rid, domains):
    update_rid_status(rid, 'RUNNING')

    results = get_domain_category(domains)
    domain_results = dict(zip(domains, results))
    need_scan_domains = [domain for domain in domain_results if domain_results[domain] is None]
    if need_scan_domains:
        scan_results = crawl(need_scan_domains)
        domain_results.update(scan_results)

    category_count = defaultdict(int)
    for category in domain_results.values():
        category_count[category] += 1
    set_category(rid, category_count)

    update_rid_status(rid, 'OK')

def deal_with_mq_msg(msg):
    try:
        msg = json.loads(msg)
        rid = msg['rid']
        domains = msg['domains']
        scan_domain(rid, domains)
    except Exception, e:
        print e

if __name__ == "__main__":
    pool = multiprocessing.Pool(maxtasksperchild=1)
    def deal(msg):
        pool.apply_async(deal_with_mq_msg, (msg,))
    domain_crawler_mq.deal_msg(deal)
