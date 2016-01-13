import time
from redislite import Redis
from common.Redis import RedisQueue


r_server = Redis()

q= RedisQueue(r_server=r_server, max_size=100)

print q.get_status()

'''submit jobs'''
for i in range(10):
    q.put(i)
print q.get_status()
q.set_submission_finished()

'''get a job'''
key, value = q.get()
q.done(key)
print q.get_status(), value, 'done'

'''get a job and signal as error'''
key, value  = q.get()
q.done(key, error=True)
print q.get_status(), value, 'done'

'''get a job, timeout, and put it back '''
key, value  = q.get()
time.sleep(7)
print q.get_status(), value, 'timed out'
q.put_back_timedout_jobs()
print q.get_status()

'''get more jobs than available'''
for i in range(10):
    data = q.get(timeout=5)
    if data:
        key, value = data
        q.done(key)
        print value, 'done'

print q.get_status()
q.close()
print q.get_status()