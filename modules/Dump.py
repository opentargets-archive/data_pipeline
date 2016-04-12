import json
import ujson

import datetime

import sys

from common import Actions
from common.ElasticsearchQuery import ESQuery
import gzip, time, logging

from settings import Config
import grequests, requests

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s',
                    # filename='/tmp/myapp.log',
                    # filemode='w',
                    )


class DumpActions(Actions):
    DUMP='dump'


class DumpGenerator(object):

    def __init__(self, es, api_url = 'https://alpha.targetvalidation.org'):

        self.es = es
        self.esquery = ESQuery(es)
        self.api_url = api_url


    def dump(self):


        '''dump evidence data'''
        logging.info('Dumping evidence data')
        with gzip.open(Config.DUMP_FILE_EVIDENCE, 'wb') as dump_file:
            for row in self.get_data('/api/latest/public/evidence/filter'):
                dump_file.write(row+'\n')


        '''dump association data'''
        logging.info('Dumping association data')
        with gzip.open(Config.DUMP_FILE_ASSOCIATION, 'wb') as dump_file:
            for row in self.get_data('/api/latest/public/association/filter'):
                dump_file.write(row + '\n')

        '''dump search objects???'''



    def get_data(self, query):


        # call='''http://localhost:7999/api/latest/public/evidence/filter?datasource=gwas_catalog&direct=false&size
        # =1000&from=%i'''
        # call='''http://192.168.99.100:7997/api/latest/public/evidence/filter?datasource=gwas_catalog&direct=false
        # &size=1000&from=%i'''
        i=0
        c=0
        start_time = time.time()

        max_page = int(1e7)
        batch_size=20
        page_size=1000
        downloaded_data = 0
        is_done = False
        total = 0
        query = self.api_url + query + '?size='+str(page_size)+'&from=%i'
        for i in range(0,max_page,batch_size):
            auth_token = self._get_auth_token()
            headers = {'Auth-token': auth_token}
            rs = (grequests.get(u, headers = headers, timeout = 60, stream = True) for u in (query % (int(i * page_size)) for i in range(i,i+batch_size)))
            for r in grequests.map(rs):
                if r is not None:
                    # r = requests.get(query % (int(i * 1000)), headers = headers)
                    # print r.status_code, r.headers['x-ratelimit-remaining-10s'], r.headers['x-ratelimit-remaining-1h'], int(r.headers['x-api-took']) / 1000., round(time.time() - start_time, 3), r.headers['x-ratelimit-reset-10s']
                    while r.status_code == 429:
                        time_wait = float(r.headers['x-usage-limit-wait'])/1000
                        logging.warn('Usage Limit exceeded!!!! sleeping for %fs | 10s limit %s | 1h limit %s'%(time_wait, r.headers['x-usage-remaining-10s'], r.headers['x-usage-remaining-1h']))
                        time.sleep(time_wait+2)#TODO check wait time is computed properly in the backend
                        r = requests.get(r.url, headers = headers, timeout = 60)
                    if r.status_code != 200:
                        logging.error('invalid status code in request:%s | trying again in 3 seconds...' % r.url)
                        time.sleep(3)
                        r = requests.get(r.url, headers=headers, timeout=60)
                        if r.status_code != 200:
                            logging.error('invalid status code in request:%s | trying again in 30 seconds...' % r.url)
                            time.sleep(30)
                            r = requests.get(r.url, headers=headers, timeout=60)
                            if r.status_code != 200:
                                logging.error('invalid status code in request:%s | giving up' % r.url)
                                raise Exception("error in response", r.status_code)
                        if r.status_code == 200:
                            logging.error('invalid status code in request:%s | fixed' % r.url)

                    if r.status_code == 200:
                        try:
                            data = r.json()['data']
                        except Exception,e:
                            logging.error('cannot parse json in requests:%s'%r.url)
                            raise Exception(e)

                        if not total:
                            total = int(r.json()['total'])
                        if not data:
                            logging.info('getting data completed')
                            is_done = True
                        if r.status_code == 200:
                            for row in data:
                                yield json.dumps(row, separators=(',', ':'))
                            i += 1
                            c += len(data)
                            downloaded_data += int(r.headers['content-length'])


            et = int(time.time()-start_time)
            etd =datetime.timedelta(seconds=et)
            speed = round(c/(et),0)
            data_rate = downloaded_data/1048576./et
            eta = datetime.timedelta(seconds=int((total-c)/speed))

            logging.info("retrieved %i of %i (%.2f%%) | elapsed time %s| speed %i datapoints/sec | data rate %.2fMbps | downloaded data %.2fMb | ETA %s"%(c,total,float(c)/total*100, etd, speed,data_rate, downloaded_data/1048576., eta))
            if is_done:
                break

    def _get_auth_token(self):
        r= requests.get(self.api_url+'/api/latest/public/auth/request_token?secret=32rZb5EqAm3QsC509uO8Oe53X4l5jC46&app_name=load-test')#&expiry=%i'%60*60)
        return r.json()['token']

