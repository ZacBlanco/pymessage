# pymessage

A simple message-based communication interface.

```python
from pymessage import TCPCommunicator
comm = TCPCommunicator(8080)
comm.listen() # Begins listening for messages in the background
comm.send('127.0.0.1',
          'Hello, World!'.encode('utf-8'),
          'msg1'.encode('utf-8'))

print(comm.get('msg1'.encode('utf-8'))) # "Hello, World!"
```

## Develop

**Pre-Requisites**: `pip` and `python`, and `virtualenv` installed

    git clone https://github.com/zacblanco/pymessage.git
    cd pymessage
    make test

