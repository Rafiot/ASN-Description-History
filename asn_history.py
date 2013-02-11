#!/usr/bin/python

import re
import dateutil.parser
import datetime
import urllib
import os
import redis
import time
from pubsublogger import publisher

import constraint as c

sleep_timer = 36000

if __name__ == '__main__':
    publisher.port = c.redis_port
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


        r = redis.Redis(host = c.redis_host, port=c.redis_port, db=c.redis_db)

        last_update = r.get('last_update')
        if last_update != update.isoformat():
            msg = 'File has been updated. Was {old} is {new}.'.format(
                    old=last_update, new = update.isoformat())
            publisher.info(msg)
            p = r.pipeline(transaction=False)
            p.set('last_update', update.isoformat())
            for asn, descr in data:
                last_descr = None
                all_descrs = r.hgetall(asn)
                if len(all_descrs) != 0:
                    dates = all_descrs.keys()
                    dates.sort()
                    last_descr = all_descrs[dates[-1]]
                if last_descr is None or descr != last_descr:
                    p.hset(asn, update.isoformat(), descr)
                    msg = 'New description for {asn}. Was {old}, is {new}'.\
                            format(asn = asn, old = last_descr, new = descr)
                    publisher.info(msg)
            p.execute()
            publisher.info('Import finished.')
        time.sleep(sleep_timer)
