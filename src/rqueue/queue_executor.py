import logging
import signal
import time
from collections.abc import Callable

import redis

logging.basicConfig(
    format='%(asctime)s | %(levelname)s | %(message)s', level=logging.INFO)


class TimeoutException(Exception):
    pass


class QueueExecutor:
    def __init__(
        self,
        redis_client: redis.Redis,
        user_function: Callable[[str], None],
        function_timeout: int,
        queue: str,
        doing_queue: str,
        success_queue: str,
        error_queue: str,
        maximum_attempts: int,
        sleep_time: int,
        execute_on_error: Callable[[str], None] = None
    ) -> None:

        self.redis_client = redis_client
        self.user_function = user_function
        self.function_timeout = function_timeout
        self.queue = queue
        self.doing_queue = doing_queue
        self.success_queue = success_queue
        self.error_queue = error_queue
        self.maximum_attempts = maximum_attempts
        self.sleep_time = sleep_time
        self.execute_on_error = execute_on_error

        self.redis_client.connection_pool.connection_kwargs['decode_responses'] = True
        self.redis_client.ping()

        self.attempt = 1
        self.current_queue = queue

    def _timeout_handler(self, signum, frame):
        raise TimeoutException(
            f'Function timed out ({self.function_timeout}s).')

    def execute(self):
        signal.signal(signal.SIGALRM, self._timeout_handler)
        while True:
            if parameter := self.redis_client.lpop(self.current_queue):
                self.redis_client.rpush(self.doing_queue, parameter)
                signal.alarm(self.function_timeout)
                try:
                    self.user_function(parameter)
                    self.redis_client.lrem(self.doing_queue, 0, parameter)
                    self.redis_client.rpush(self.success_queue, parameter)
                    logging.info(
                        f'User function successfully executed with parameter "{parameter}" (attempt {self.attempt}).')
                except Exception as e:
                    self.redis_client.lrem(self.doing_queue, 0, parameter)
                    if self.attempt >= self.maximum_attempts:
                        if self.execute_on_error:
                            self.execute_on_error(parameter)
                        self.redis_client.rpush(self.error_queue, parameter)
                    else:
                        self.redis_client.rpush(
                            f'{self.queue}:attempt:{self.attempt + 1}', parameter)
                    logging.error(
                        f'Failed to execute user function with parameter "{parameter}" (attempt {self.attempt}). {e}')
                finally:
                    signal.alarm(0)
            elif self.attempt < self.maximum_attempts:
                current_queue = self.current_queue
                self.attempt += 1
                self.current_queue = f'{self.queue}:attempt:{self.attempt}'
                logging.info(
                    f'The Redis queue "{current_queue}" is empty, changing to queue "{self.current_queue}".')
            else:
                if doing := self.redis_client.lrange('doing_queue', 0, -1):
                    self.redis_client.rpush(self.queue, *doing)
                    self.redis_client.delete('doing_queue')
                current_queue = self.current_queue
                self.attempt = 1
                self.current_queue = self.queue
                logging.info(
                    f'The Redis queue "{current_queue}" is empty, restarting.')
                logging.info(f'Sleeping for {self.sleep_time}s.')
                time.sleep(self.sleep_time)
