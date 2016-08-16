import json
import os
import ujson
import requests
import datetime

import sys
from Queue import Queue, Empty
from threading import Thread

from multiprocessing import Process, JoinableQueue

from common import Actions
from common.ElasticsearchQuery import ESQuery
import gzip, time, logging

from settings import Config


class DumpActions(Actions):
    DUMP='dump'


class DumpGenerator(object):

    def __init__(self, es, api_url = 'https://alpha.targetvalidation.org'):

        self.es = es
        self.esquery = ESQuery(es)
        self.api_url = api_url


    def dump(self):


        # '''dump evidence data'''
        # logging.info('Dumping evidence data')
        # # self.get_data('/api/latest/public/evidence/filter', Config.DUMP_FILE_EVIDENCE)
        # self.put_files_together(Config.DUMP_FILE_EVIDENCE)
        # # with gzip.open(Config.DUMP_FILE_EVIDENCE, 'wb') as dump_file:
        #     # for row in self.get_data_concurrently('/api/latest/public/evidence/filter'):
        #     #     dump_file.write(row+'\n')
        #

        '''dump association data'''
        logging.info('Dumping association data')
        self.get_data('/api/latest/public/association/filter', Config.DUMP_FILE_ASSOCIATION)
        self.put_files_together(Config.DUMP_FILE_ASSOCIATION)

        # with gzip.open(Config.DUMP_FILE_ASSOCIATION, 'wb') as dump_file:
        #     for row in self.get_data('/api/latest/public/association/filter'):
                # dump_file.write(row + '\n')

        '''dump search objects???'''



    def get_data(self, query, filename = Config.DUMP_FILE_EVIDENCE, max_retry = 5):
        import grequests


        c=0
        start_time = time.time()

        max_page = int(1e7)
        batch_size = Config.DUMP_BATCH_SIZE
        page_size=Config.DUMP_PAGE_SIZE
        downloaded_data = 0
        is_done = False
        total= int(requests.get(self.api_url + query + '?size=0').json()['total'])
        logging.info('%i datapoints to download' % total)
        query = self.api_url + query + '?size='+str(page_size)+'&from=%i'
        for i in range(0,max_page,batch_size):
            auth_token = self._get_auth_token()
            headers = {'Auth-token': auth_token}
            urls = [u for u in (query % (int(i * page_size)) for i in range(i,i+batch_size))]
            urls_to_get = [u for u in urls if not os.path.exists(self.get_file_path(u, filename))]
            c+=(len(urls) - len(urls_to_get))*page_size
            rs = (grequests.get(u, headers = headers, timeout = 60) for u in urls_to_get)
            for r in grequests.map(rs):
                if r is not None:
                    logging.debug('Request %s, status %s, took %s, size %iKb'%(r.url, r.status_code, r.elapsed,float(r.headers.get('content-length', 0))/1024.))
                    # r = requests.get(query % (int(i * 1000)), headers = headers)
                    # print r.status_code, r.headers['x-ratelimit-remaining-10s'], r.headers['x-ratelimit-remaining-1h'], int(r.headers['x-api-took']) / 1000., round(time.time() - start_time, 3), r.headers['x-ratelimit-reset-10s']
                    while r.status_code == 429:
                        time_wait = float(r.headers['retry-after'])
                        logging.warn('Usage Limit exceeded!!!! sleeping for %fs | 10s limit %s | 1h limit %s'%(time_wait, r.headers['x-usage-remaining-10s'], r.headers['x-usage-remaining-1h']))
                        time.sleep(time_wait+2)#TODO check wait time is computed properly in the backend
                        r = requests.get(r.url, headers = headers, timeout = 120)
                    if r.status_code != 200:
                        i=0
                        while (r.status_code != 200) and (i<max_retry):
                            wait=30*i
                            logging.error('invalid status code %s in request:%s | trying again in %i seconds...' % (r.status_code,r.url, wait))
                            time.sleep(wait)
                            r = requests.get(r.url, headers=headers, timeout=120)
                            if r.status_code == 200:
                                logging.debug('invalid status code in request:%s | fixed' % r.url)
                                break
                            i+=1

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
                            file_path = self.get_file_path(r.url, filename)

                            if not os.path.exists(file_path):
                                with gzip.open(file_path, 'wb') as dump_file:
                                    for row in data:
                                        dump_file.write(json.dumps(row, separators=(',', ':')) + '\n')
                            # for row in data:
                            #     yield json.dumps(row, separators=(',', ':'))
                            i += 1
                            c += len(data)
                            if data:
                                downloaded_data += int(r.headers['content-length'])


            et = int(time.time()-start_time)
            if et:
                etd =datetime.timedelta(seconds=et)
                speed = round(c/(et),0)
                data_rate = downloaded_data/1048576./et
                eta = datetime.timedelta(seconds=int((total-c)/speed))

                logging.info("retrieved %i of %i (%.2f%%) | elapsed time %s| speed %i datapoints/sec | data rate %.2fMbps | downloaded data %.2fMb | ETA %s"%(c,total,float(c)/total*100, etd, speed,data_rate, downloaded_data/1048576., eta))
                if is_done:
                    break

    def _get_auth_token(self):
        r= requests.get(self.api_url+'/api/latest/public/auth/request_token?secret=1RT6L519zkcTH9i3F99OjeYn13k79Wep&app_name=load-test')#&expiry=%i'%60*60)
        return r.json()['token']


    def get_data_concurrently(self, query):
        concurrent = 5

        def doWork():
            r_token = requests.get(
                self.api_url + '/api/latest/public/auth/request_token?secret=32rZb5EqAm3QsC509uO8Oe53X4l5jC46'
                               '&app_name=load-test')
            auth_token = r_token.json()['token']
            # print "worker started with token", auth_token
            while True:
                try:
                    url = q_url.get()
                    headers = {'Auth-token': auth_token}
                    r = requests.get(url, headers=headers, timeout=60, stream=False)
                    q_results.put(r)
                    q_url.task_done()
                except Empty:
                    print 'no url to get'
                    pass

        def generate(query=None, page_size=None, total=None):
            total = int(total)
            for i, url in enumerate((u for u in (query % (int(i * page_size)) for i in range(total / page_size + 1)))):
                q_url.put(url)
                if (i+1)%10 == 0:
                    logging.debug('generated %i urls out of %i'%(i+1, total / page_size))
            q_url.join()


        q_url = JoinableQueue(concurrent*2)
        q_results = JoinableQueue(concurrent)

        max_page = int(1e7)
        batch_size = 100
        page_size = 500
        downloaded_data = 0
        is_done = False
        total_to_download = int(requests.get(self.api_url + query + '?size=0').json()['total'])
        logging.info('%i datapoints to download'%total_to_download)
        query = self.api_url + query + '?size=' + str(page_size) + '&from=%i'
        t2 = Process(target=generate, kwargs=dict(query = query, page_size = page_size, total = total_to_download))
        t2.start()
        for i in range(concurrent):
            t = Process(target=doWork)
            t.start()


        downloaded_data = 0
        start_time = time.time()
        res_count = 0
        is_done = False
        c = 0

        while True:
            try:
                r = q_results.get_nowait()
                c += 1
                res_count = c * page_size
                if r:

                    if r.status_code == 200:
                        try:
                            data = r.json()['data']
                        except Exception, e:
                            logging.error('cannot parse json in requests:%s' % r.url)
                            raise Exception(e)
                        if r.status_code == 200:
                            file_path = os.path.join(Config.DUMP_FILE_FOLDER,
                                                     Config.DUMP_FILE_EVIDENCE + '_' + r.url.split('?')[1])

                            if not os.path.exists(file_path):
                                with gzip.open(file_path, 'wb') as dump_file:
                                    for row in data:
                                      dump_file.write(json.dumps(row, separators=(',', ':')) + '\n')
                        # print url, r.status_code, int(r.headers['content-length'])
                        logging.debug(str(r.status_code) + ' ' + r.url)
                    downloaded_data += int(r.headers['content-length'])
                    if res_count % 10000 == 0:
                        try:
                            et = int(time.time() - start_time)
                            if et:

                                etd = datetime.timedelta(seconds=et)
                                speed = round(res_count / et)
                                # print speed
                                data_rate = downloaded_data / 1048576. / et
                                eta = datetime.timedelta(seconds=int((total_to_download - res_count) / speed))
                                # eta = datetime.timedelta(seconds=1)
                                # print res_count, total_to_download, float(res_count) / total_to_download * 100,
                                #  etd, speed, data_rate, downloaded_data / 1048576., eta


                                logging.info(
                                    "retrieved %i of %i (%.2f%%) | elapsed time %s| speed %i datapoints/sec | "
                                    "data rate %.2fMbps (%.2fMbps uncompressed) | downloaded data %.2fMb (%.2fMb "
                                    "uncompressed) | ETA %s" % (
                                        res_count, total_to_download, float(res_count) / total_to_download * 100, etd,
                                        speed, data_rate, data_rate * 8.4, downloaded_data / 1048576.,
                                        downloaded_data / 1048576. * 8.4, eta))
                        except:
                            pass
                # print c
                # if r:
                #     if r.status_code == 200:
                #         try:
                #             data = r.json()['data']
                #         except Exception, e:
                #             logging.error('cannot parse json in requests:%s' % r.url)
                #             raise Exception(e)
                #
                #         if not data:
                #             logging.info('getting data completed')
                #             is_done = True
                #         if r.status_code == 200:
                #             for row in data:
                #                 yield json.dumps(row, separators=(',', ':'))
                #                 # pass
                #             res_count += len(data)
                #             if 'content-length' not in r.headers:
                #                 print len(data), r.headers.keys()
                #             downloaded_data += int(r.headers['content-length'])
                #         if res_count%10000 ==0:
                #             try:
                #                 et = int(time.time() - start_time)
                #                 if et:
                #                     etd = datetime.timedelta(seconds=et)
                #                     speed = round(res_count / et)
                #                     # print speed
                #                     data_rate = downloaded_data / 1048576. / et
                #                     eta = datetime.timedelta(seconds=int((total_to_download - res_count) / speed))
                #                     # eta = datetime.timedelta(seconds=1)
                #                     # print res_count, total_to_download, float(res_count) / total_to_download * 100,
                #                     #  etd, speed, data_rate, downloaded_data / 1048576., eta
                #
                #
                #                     logging.info(
                #                         "retrieved %i of %i (%.2f%%) | elapsed time %s| speed %i datapoints/sec | "
                #                         "data rate %.2fMbps (%.2fMbps uncompressed) | downloaded data %.2fMb (%.2fMb uncompressed) | ETA %s" % (
                #                         res_count, total_to_download, float(res_count) / total_to_download * 100, etd,
                #                         speed, data_rate, data_rate*8.4, downloaded_data / 1048576., downloaded_data / 1048576.*8.4, eta))
                #                     # logging.info('%s %s %s %s %s %s'%(str(q_results.qsize()),str(q_results.empty()), str(q_results.full()),
                #                     #                                   str(q_url.qsize()), str(q_url.empty()),
                #                     #                                   str(q_url.full())))
                #             except Exception, e:
                #                 print e
                #                 pass
                q_results.task_done()
            except Empty:
                logging.debug('waiting for results to write')
                time.sleep(0.5)
                if is_done:
                    print 'done'
                    break

    def get_file_path(self, url, filename = Config.DUMP_FILE_EVIDENCE):
        file_path = os.path.join(Config.DUMP_FILE_FOLDER,
                                 filename + '_' + url.split('?')[1])
        return file_path

    def put_files_together(self, filename_pattern):
        joined_output_path = os.path.join(Config.DUMP_FILE_FOLDER, filename_pattern)
        c=0
        with gzip.open(joined_output_path, 'wb') as joined_output:
            for f in os.listdir(Config.DUMP_FILE_FOLDER):
                if f.startswith(filename_pattern) and not f==filename_pattern:
                    with gzip.open(os.path.join(Config.DUMP_FILE_FOLDER, f), 'rb') as f_unit:
                        data = f_unit.readlines()
                        c+=len(data)
                        joined_output.writelines(data)
                    logging.debug("compacted %s"%f)

        logging.info('Compacted %i lines in a file at this path: %s'%(c, joined_output_path))

