from redis import Redis
from collections.abc import Callable
from typing import Any, List
import logging
import time
from threading import Thread
import csv

logging.basicConfig(format='%(asctime)s | %(levelname)s | %(message)s', level=logging.INFO)

class Queue:
    def __init__(
        self,
        queue_name:str,
        success_queue:str|List[str] = None,
        error_queue:str|List[str] = None,
        redis_host:str = 'localhost',
        redis_port:int = 6379,
        redis_db:int = 0
        ) -> None:

        self.queue_name = queue_name
        self.redis_client = Redis(host=redis_host, port=redis_port, db=redis_db, decode_responses=True)

        if success_queue:
            self.success_queue = [success_queue] if type(success_queue) == str else success_queue
        else:
            self.success_queue = [f'{self.queue_name}:success']

        if error_queue:
            self.error_queue = [error_queue] if type(error_queue) == str else error_queue
        else:
            self.error_queue = [f'{self.queue_name}:error']

        self.redis_client.ping()
        logging.info('Successfully connected to Redis.')

    def _run_user_function(self, function:Callable, parameter:Any) -> None:
        try:
            function(parameter)
            for queue in self.success_queue:
                self.redis_client.rpush(queue, parameter)
            logging.info(f'User function successfully executed with parameter "{parameter}"')
        except Exception as e:
            for queue in self.error_queue:
                self.redis_client.rpush(queue, parameter)
            logging.error(f'Failed to execute user function with parameter "{parameter}". {e}')

    def _run_user_function_threading(self, function:Callable, parameters:list) -> None:
        threads = []
        for parameter in parameters:
            thread = Thread(target=self._run_user_function, args=(function, parameter))
            threads.append(thread)
            thread.start()
        for thread in threads:
            thread.join()

    def fill_from_list(self, elements:list, flush:bool=True) -> None:
        if flush:
            self.redis_client.delete(self.queue_name)
            logging.info(f'The Redis queue "{self.queue_name}" was deleted.')
        self.redis_client.rpush(self.queue_name, *elements)
        logging.info(f'The Redis queue "{self.queue_name}" was filled with {len(elements)} elements.')

    def fill_from_csv(self, csv_path:str, flush:bool=True) -> None:
        with open(csv_path) as f:
            elements = [element[0] for element in list(csv.reader(f))]
        self.fill_from_list(elements, flush=flush)

    def execute(self, function:Callable, threadings:int=2, retry:bool=True, sleep_time:int=30) -> None:
        if retry:
            self.error_queue = [self.queue_name]

        while True:
            parameters = self.redis_client.lpop(self.queue_name, count=threadings) or []
            if parameters:
                self._run_user_function_threading(function, parameters)
            else:
                logging.info(f'The Redis queue "{self.queue_name}" is empty, sleeping for {sleep_time} second(s).')
                time.sleep(sleep_time)
