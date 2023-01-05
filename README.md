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
from rqueue import QueueFiller, QueueExecutor
from . import my_function

ELEMENTS = [0, 1, 2, 3]

filler = QueueFiller('queue', ELEMENTS)
filler.fill()

executor = QueueExecutor('queue', my_function, retry=True, threadings=4)
executor.execute()
```
