# redis-rqueue

## Installation

### With pip

```console
pip install --upgrade pip setuptools
pip install redis-rqueue
```

### from source

```console
pip install --upgrade pip setuptools
pip install -e .
```

## Usage

```python
from rqueue import Queue, QueueExecutor
from . import my_function

queue = Queue('queue')
queue.fill_from_list([0, 1, 2, 3], flush=True)
queue.fill_from_csv('file.csv', flush=True)

executor = QueueExecutor('queue', my_function, retry=True, threadings=2)
executor.execute()
```
