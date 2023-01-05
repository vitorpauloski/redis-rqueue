from redis import Redis
from collections.abc import Callable
from typing import Any
import logging
import time
from threading import Thread

logging.basicConfig(format='%(asctime)s | %(levelname)s | %(message)s', level=logging.INFO)

class Queue:
    def __init__(self, queue_name:str, redis_host:str='localhost', redis_port:int=6379, redis_db:int=0) -> None:
        self.queue_name = queue_name
        self.redis_client = Redis(host=redis_host, port=redis_port, db=redis_db, decode_responses=True)

        self.test_redis_connection()

    def test_redis_connection(self) -> bool:
        try:
            self.redis_client.ping()
            logging.info('Successfully connected to Redis.')
            return True
        except Exception as e:
            logging.error(f'Unable to connect to redis. {e}')
            return False

class QueueFiller(Queue):
    def __init__(self, queue_name:str, elements:list, redis_host:str='localhost', redis_port:int=6379, redis_db:int=0) -> None:
        super().__init__(queue_name, redis_host=redis_host, redis_port=redis_port, redis_db=redis_db)
        self.elements = elements

    def fill(self) -> None:
        try:
            self.redis_client.rpush(self.queue_name, *self.elements)
            logging.info(f'The Redis queue "{self.queue_name}" was successfuly filled with {len(self.elements)} elements.')
        except Exception as e:
            logging.error(f'Failed to fill "{self.queue_name}" queue. {e}')

class QueueExecutor(Queue):
    def __init__(
        self,
        queue_name:str,
        function:Callable,
        redis_host:str = 'localhost',
        redis_port:int = 6379,
        redis_db:int = 0,
        retry:bool = False,
        threadings:int = 1,
        success_queue_name:str = None,
        error_queue_name:str = None,
        sleep_time:int = 30
        ) -> None:

        super().__init__(queue_name, redis_host=redis_host, redis_port=redis_port, redis_db=redis_db)
        self.function = function
        self.retry = retry
        self.threadings = threadings
        self.success_queue_name = success_queue_name or f'{self.queue_name}:success'
        self.error_queue_name = error_queue_name or f'{self.queue_name}:error'
        self.sleep_time = sleep_time

        if retry:
            self.error_queue_name = queue_name

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
        while True:
            queue = self.redis_client.lpop(self.queue_name, count=self.threadings) or []
            if len(queue) > 0:
                self.run_function_threading(queue)
            else:
                logging.info(f'The Redis queue "{self.queue_name}" is empty, sleeping for {self.sleep_time} second(s).')
                time.sleep(self.sleep_time)
