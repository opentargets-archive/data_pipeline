import unittest
import uuid

from mrtarget.common.Redis import RedisQueue, WhiteCollarWorker, RedisQueueWorkerThread
from mrtarget.common.connection import PipelineConnectors


class ProxyWorker(RedisQueueWorkerThread):
    '''reads from the input queue and push it to the output queue'''

    def process(self, data):
        return data


class ConsumerWorker(RedisQueueWorkerThread):
    '''reads from the input queue do nothing with it'''

    def process(self, data):
        print data
        return


class TestRedisWorker(unittest.TestCase):
    UNIQUE_RUN_ID = str(uuid.uuid4()).replace('-', '')[:16]
    queue_name_test = UNIQUE_RUN_ID+'|test_queue'

    def test_WhiteCollar(self):
        connectors = PipelineConnectors()
        connectors.init_services_connections("localhost",35000, False)
        
        queue1 = RedisQueue(queue_id='test1' + '_1',
                                max_size=10,
                                job_timeout=120,
                                batch_size=1,
                                r_server=connectors.r_server,
                                serialiser='pickle')

        queue2 = RedisQueue(queue_id='test2' + '_2',
                                max_size=10,
                                job_timeout=120,
                                batch_size=1,
                                r_server=connectors.r_server,
                                serialiser='pickle')

        test_workers = WhiteCollarWorker(target=ProxyWorker,
                                         pool_size=1,
                                         queue_in=queue1,
                                         redis_path=None,
                                         queue_out=queue2,

                                         )

        test_consumer = WhiteCollarWorker(target=ConsumerWorker,
                                          pool_size=1,
                                          queue_in=queue2,
                                          redis_path=None,
                                          )

        test_workers.start()
        test_consumer.start()

        iterations = 100
        for i in range(iterations):
            queue1.put(i)
        queue1.set_submission_finished()

        test_workers.join(timeout=10)
        test_consumer.join(timeout=10)

        queue1_status = queue1.get_status()

        self.assertEquals(queue1_status['submitted_counter'], iterations)
        self.assertEquals(queue1_status['processed_counter'], iterations)
        self.assertTrue(queue1.is_done())
        self.assertTrue(queue2.is_done())
        self.assertFalse(test_workers.is_alive())
        self.assertFalse(test_consumer.is_alive())
        
        queue1.close()
        queue2.close()
 
        connectors.close()
