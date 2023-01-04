from redis import Redis
from collections.abc import Callable
from typing import Any
import logging
import time

logging.basicConfig(format='%(asctime)s | %(levelname)s | %(message)s', level=logging.INFO)

class Queue:
    def __init__(
        self,
        user_function:Callable,
        redis_client:Redis = Redis(),
        queue_name:str = 'queue',
        success_queue_name:str = None,
        error_queue_name:str = None,
        sleep_time:int = 30
        ) -> None:

        self.user_function = user_function
        self.redis_client = redis_client
        self.queue_name = queue_name
        self.success_queue_name = success_queue_name or f'{self.queue_name}:success'
        self.error_queue_name = error_queue_name or f'{self.queue_name}:error'
        self.sleep_time = sleep_time

        redis_client.connection_pool.connection_kwargs['decode_responses'] = True

    def test_redis_connection(self) -> bool:
        try:
            self.redis_client.ping()
            logging.info('Successfully connected to Redis.')
            return True
        except Exception as e:
            logging.error(f'Unable to connect to redis. {e}')
            return False

    def run_user_function(self, parameter:Any) -> None:
        try:
            self.user_function(parameter)
            self.redis_client.rpush(self.success_queue_name, parameter)
            logging.info(f'User function successfully executed with parameter "{parameter}"')
        except Exception as e:
            self.redis_client.rpush(self.error_queue_name, parameter)
            logging.error(f'Failed to execute user function with parameter "{parameter}". {e}')

    def start(self) -> None:
        if not self.test_redis_connection():
            return None
        while True:
            queue = self.redis_client.lpop(self.queue_name, count=1) or []
            if len(queue) > 0:
                self.run_user_function(queue[0])
            else:
                logging.info(f'The Redis queue "{self.queue_name}" is empty, sleeping for {self.sleep_time} second(s).')
                time.sleep(self.sleep_time)
