# rqueue

## Installation

```console
pip install --upgrade pip setuptools
pip install -e .
```

## Usage

```python
from rqueue import Queue
from . import my_function

queue = Queue(my_function, queue_name='queue', retry=True, threadings=4)
queue.start()
```
