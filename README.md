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
from rqueue import Queue
from . import my_function

queue = Queue('queue')
queue.fill_from_list([0, 1, 2, 3])
queue.fill_from_csv('file.csv')

queue.execute(my_function)
```
