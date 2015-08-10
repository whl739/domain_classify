# -*- coding: utf-8 -*-

import json
import web

#from tasks import scan_domain
from utils.rds import gen_request_id, get_category, get_rid_status, domain_crawler_mq

web.config.debug = True

urls = (
    '/', 'app.Index',
    '/result', 'app.Result',
)

app = web.application(urls)

class Index(object):
    def POST(self):
        form_data = web.input()
        domains = form_data.get('domains', '')
        try:
            domains = json.loads(domains)
        except Exception:
            return output_json({'status':'ERROR', 'message':'params domains error'})
        if not domains:
            return output_json({'status':'ERROR', 'message':'none domains'})
        rid = gen_request_id()
        msg = json.dumps({'rid':rid, 'domains':domains})
        domain_crawler_mq.send_msg(msg)
        return output_json({'status':'OK', 'rid':rid})

class Result(object):
    def POST(self):
        form_data = web.input()
        rid = form_data.get('rid', '')
        status = get_rid_status(rid)
        if status is None:
           return output_json({'status':'ERROR', 'message':'no such rid'})
        ret = get_category(rid)
        return output_json({'status':status, 'result':ret})

def output_json(result):
    web.header('Content-Type', 'application/json')
    return json.dumps(result)

if __name__ == '__main__':
    app.run()
