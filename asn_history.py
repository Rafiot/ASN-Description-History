#!/usr/bin/python

import re
import dateutil.parser
import datetime
import urllib
import os
import redis
import time
from pubsublogger import publisher

redis_host = '127.0.0.1'
redis_db = 0
redis_port = 6389

sleep_timer = 36000



if __name__ == '__main__':
    publisher.port = redis_port
    publisher.channel = 'ASN_History'
    time.sleep(5)
    publisher.info('Importer started.')
    while True:
        today = datetime.date.today().strftime("%Y%m%d")
        autnums_dir = 'autnums'
        current_file = os.path.join(autnums_dir, today)

        if os.path.exists(current_file):
            time.sleep(sleep_timer)
            continue
        publisher.info('New day.')
        urllib.urlretrieve('http://www.cidr-report.org/as2.0/autnums.html', current_file)

        f = open(current_file).read()
        data = re.findall('as=AS(.*)&.*</a> (.*)\n', f)
        update_raw = re.sub('[\n()]', '',
                re.findall('File last modified at (.*)</I>', f , re.S)[0])

        update = dateutil.parser.parse(update_raw)


        r = redis.Redis(host = redis_host, port=redis_port, db=redis_db)

        last_update = r.get('last_update')
        if last_update != update.isoformat():
            msg = 'File has been updated. Was {old} is {new}.'.format(
                    old=last_update, new = update.isoformat())
            publisher.info(msg)
            p = r.pipeline(transaction=False)
            p.set('last_update', update.isoformat())
            for asn, descr in data:
                p.hset(asn, update.isoformat(), descr)
            p.execute()
            publisher.info('Import finished.')
        time.sleep(sleep_timer)
