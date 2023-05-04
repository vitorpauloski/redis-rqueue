import logging
import time
from collections.abc import Callable

import redis

logging.basicConfig(
    format='%(asctime)s | %(levelname)s | %(message)s', level=logging.INFO)


class QueueExecutor:
    def __init__(
        self,
        redis_client: redis.Redis,
        user_function: Callable[[str], None],
        queue: str,
        doing_queue: str,
        success_queue: str,
        error_queue: str,
        maximum_attempts: int,
        sleep_time: int
    ) -> None:

        self.redis_client = redis_client
        self.user_function = user_function
        self.queue = queue
        self.doing_queue = doing_queue
        self.success_queue = success_queue
        self.error_queue = error_queue
        self.maximum_attempts = maximum_attempts
        self.sleep_time = sleep_time

        self.redis_client.connection_pool.connection_kwargs['decode_responses'] = True
        self.redis_client.ping()

        self.attempt = 1
        self.current_queue = queue

    def execute(self):
        while True:
            if parameter := self.redis_client.lpop(self.current_queue):
                self.redis_client.rpush(self.doing_queue, parameter)
                try:
                    self.user_function(parameter)
                    self.redis_client.lrem(self.doing_queue, 0, parameter)
                    self.redis_client.rpush(self.success_queue, parameter)
                    logging.info(
                        f'User function successfully executed with parameter "{parameter}". (attempt {self.attempt}).')
                except Exception as e:
                    self.redis_client.lrem(self.doing_queue, 0, parameter)
                    if self.attempt >= self.maximum_attempts:
                        self.redis_client.rpush(self.error_queue, parameter)
                    else:
                        self.redis_client.rpush(
                            f'{self.queue}:attempt:{self.attempt + 1}', parameter)
                    logging.error(
                        f'Failed to execute user function with parameter "{parameter}". (attempt {self.attempt}). {e}')
            elif self.attempt < self.maximum_attempts:
                current_queue = self.current_queue
                self.attempt += 1
                self.current_queue = f'{self.queue}:attempt:{self.attempt}'
                logging.info(
                    f'The Redis queue "{current_queue}" is empty, changing to queue "{self.current_queue}".')
            else:
                current_queue = self.current_queue
                self.attempt = 1
                self.current_queue = self.queue
                logging.info(
                    f'The Redis queue "{current_queue}" is empty, restarting.')
                logging.info(f'Waiting {self.sleep_time} seconds.')
                time.sleep(self.sleep_time)
