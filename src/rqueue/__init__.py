from redis import Redis
from collections.abc import Callable
from typing import Any
import logging
import time
from threading import Thread

logging.basicConfig(format='%(asctime)s | %(levelname)s | %(message)s', level=logging.INFO)

class QueueExecutor:
    def __init__(
        self,
        function:Callable,
        queue_name:str,
        retry:bool = False,
        threadings:int = 1,
        redis_client:Redis = Redis(),
        success_queue_name:str = None,
        error_queue_name:str = None,
        sleep_time:int = 30
        ) -> None:

        self.function = function
        self.queue_name = queue_name
        self.retry = retry
        self.threadings = threadings
        self.redis_client = redis_client
        self.success_queue_name = success_queue_name or f'{self.queue_name}:success'
        self.error_queue_name = error_queue_name or f'{self.queue_name}:error'
        self.sleep_time = sleep_time

        if retry:
            self.error_queue_name = queue_name

        redis_client.connection_pool.connection_kwargs['decode_responses'] = True

    def test_redis_connection(self) -> bool:
        try:
            self.redis_client.ping()
            logging.info('Successfully connected to Redis.')
            return True
        except Exception as e:
            logging.error(f'Unable to connect to redis. {e}')
            return False

    def run_function(self, parameter:Any) -> None:
        try:
            self.function(parameter)
            self.redis_client.rpush(self.success_queue_name, parameter)
            logging.info(f'User function successfully executed with parameter "{parameter}"')
        except Exception as e:
            self.redis_client.rpush(self.error_queue_name, parameter)
            logging.error(f'Failed to execute user function with parameter "{parameter}". {e}')

    def run_function_threading(self, parameters:list) -> None:
        threads = []
        for parameter in parameters:
            thread = Thread(target=self.run_function, args=(parameter,))
            threads.append(thread)
            thread.start()
        for thread in threads:
            thread.join()

    def execute(self) -> None:
        if not self.test_redis_connection():
            return None
        while True:
            queue = self.redis_client.lpop(self.queue_name, count=self.threadings) or []
            if len(queue) > 0:
                self.run_function_threading(queue)
            else:
                logging.info(f'The Redis queue "{self.queue_name}" is empty, sleeping for {self.sleep_time} second(s).')
                time.sleep(self.sleep_time)

class QueueFiller:
    def __init__(
        self,
        queue_name:str,
        elements:list,
        redis_client:Redis = Redis()
        ) -> None:

        self.queue_name = queue_name
        self.elements = elements
        self.redis_client = redis_client

        redis_client.connection_pool.connection_kwargs['decode_responses'] = True

    def test_redis_connection(self) -> bool:
        try:
            self.redis_client.ping()
            logging.info('Successfully connected to Redis.')
            return True
        except Exception as e:
            logging.error(f'Unable to connect to redis. {e}')
            return False
    
    def fill(self) -> None:
        if not self.test_redis_connection():
            return None
        try:
            pushed = self.redis_client.rpush(self.queue_name, *self.elements)
            logging.info(f'The Redis queue "{self.queue_name}" was successfuly filled with {pushed} elements.')
        except Exception as e:
            logging.error(f'Failed to fill "{self.queue_name}" queue. {e}')
