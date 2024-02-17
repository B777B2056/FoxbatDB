# -*- coding: utf-8 -*-
import json
import random
import time
import redis
from locust import User, events, task, tag

DBHost = "localhost"
DBPort = 7698

'''
    Run the following command: locust -f stress.py --headless -u 100 -r 1 -t 5m
    For parameter meanings, see https://docs.locust.io/en/stable/configuration.html.
'''


class RedisClient(object):
    def __init__(self, host=DBHost, port=DBPort):
        self.rc = redis.StrictRedis(host=host, port=port)

    def get_query_string(self, key, command='GET'):
        result = None
        start_time = time.time()
        try:
            result = self.rc.get(key)
            if not result:
                result = ''
        except Exception as e:
            total_time = int((time.time() - start_time) * 1000)
            events.fire(request_type=command, name=key, response_time=total_time, exception=e)
        else:
            total_time = int((time.time() - start_time) * 1000)
            length = len(str(result))
            events.request.fire(request_type=command, name=key, response_time=total_time,
                                response_length=length)
        return result

    def set_query_string(self, key, command='SET'):
        result = None
        bid_price = random.randint(47238, 57238)
        redis_response = {'bids': bid_price}
        start_time = time.time()
        try:
            result = self.rc.set(key, json.dumps(redis_response))
            if not result:
                result = ''
        except Exception as e:
            total_time = int((time.time() - start_time) * 1000)
            events.fire(request_type=command, name=key, response_time=total_time, exception=e)
        else:
            total_time = int((time.time() - start_time) * 1000)
            length = len(str(result))
            events.request.fire(request_type=command, name=key, response_time=total_time,
                                response_length=length)
        return result


class RedisLocust(User):
    def __init__(self, *args, **kwargs):
        super(RedisLocust, self).__init__(*args, **kwargs)
        self.client = RedisClient()

    @task
    @tag("set")
    def set_operations(self):
        self.client.set_query_string("string_set_operation")

    @task
    @tag("get")
    def get_operations(self):
        self.client.get_query_string("string_get_operation")
