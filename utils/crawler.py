# -*- coding: utf-8 -*-

import requests
from requests.exceptions import ConnectionError
import jieba
from lxml import etree
import chardet

import Queue
from threadpool import WorkerManager
from classify import classify
from rds import set_domain_category

import sys
reload(sys)
sys.setdefaultencoding("utf-8")


headers = {
    "User-agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:14.0) Gecko/20100101 Firefox/14.0.1",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-cn,zh;q=0.8,en-us;q=0.5,en;q=0.3",
    "Accept-Encoding": "gzip, deflate"
}

title_xpath = '/html/head/title/text()'
keywords_xpath = '''
    /html/head/meta[translate(@name, "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
    "abcdefghijklmnopqrstuvwxyz")="keywords"]/@content
'''
des_xpath = '''
    /html/head/meta[translate(@name, "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
    "abcdefghijklmnopqrstuvwxyz")="description"]/@content
'''

def cut_web_content(domain, web_content):
    if len(web_content) < 10:
        print '%s content too short:%s' % (domain, web_content)
        return []
    try:
        web_content = web_content.partition('/head>')
        web_content = ''.join(web_content[:2]) + '</html>'

        if not isinstance(web_content, unicode):
            try:
                charset_dec = chardet.detect(web_content)
                encoding = charset_dec['encoding']
                if charset_dec['confidence'] > 0.8:
                    web_content = web_content.decode(encoding)
                else:
                    web_content = web_content.decode('utf-8')
            except UnicodeDecodeError:
                web_content = web_content.decode('gb18030','ignore')

        tree = etree.HTML(web_content)
        title = tree.xpath(title_xpath)
        keywords = tree.xpath(keywords_xpath)
        description = tree.xpath(des_xpath)

        keyword_list = title + keywords + description
        keyword_list = [keyword for keyword in keyword_list if keyword]
        keyword_list = '|'.join(keyword_list)

        keyword_list = jieba.cut(keyword_list)
        keyword_list = [keyword.strip() for keyword in keyword_list if len(keyword) > 1]
    except Exception, e:
        print '%s cut web content error:%s' % (domain, e)
        return []

    return keyword_list

def fetch(domain):
    keywords = None
    category = None
    try:
        url = domain
        if not url.startswith('http'):
            url = ''.join(["http://www.", domain])
        r = requests.get(url, allow_redirects=True, headers=headers,
            timeout=30)
        keywords = cut_web_content(domain, r.content)
        if len(keywords) < 3:
            category = '关键词太少'
        keywords = '|'.join(keywords)
    except ConnectionError, e:
        if '[Errno -' in str(e.args[0]):
            category = "无法解析"
        else:
            category = "无法访问"
    except Exception, e:
        category = '无法访问'

    print '%s:%s:%s' %(domain, keywords, category)
    return domain, keywords, category

def crawl(domains):
    pool = WorkerManager(1000)
    for domain in domains:
        pool.add_job(fetch, domain)
    pool.wait_for_complete()

    domain_results = {}
    keywords_list = []
    need_classify_domains = []
    while True:
        try:
            domain, keywords, category = pool.get_result(block=False)
            if category is None:
                need_classify_domains.append(domain)
                keywords_list.append(keywords)
            else:
                domain_results[domain] = category
        except Queue.Empty:
            break

    pred = classify(keywords_list)
    classify_results = dict(zip(need_classify_domains, pred))
    domain_results.update(classify_results)

    set_domain_category(domain_results)
    return domain_results
