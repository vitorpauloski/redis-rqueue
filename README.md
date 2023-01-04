# rqueue

## Installation

```console
pip install --upgrade pip setuptools
pip install -e .
```

## Usage

```python
from rqueue import QueueExecutor
from . import my_function

queue = QueueExecutor(my_function, queue_name='queue', retry=True, threadings=4)
queue.execute()
```
