'''Communicator which handles UDP/TCP communication

'''
import inspect
import json
import logging
import math
import socket
import struct
import threading
import collections
import time


TAG_SIZE = 4
SEQ_SIZE = 2
UDP = 10
TCP = 20

socket.setdefaulttimeout(1.5)
logger = logging.getLogger(__name__)

class BaseCommunicator(object):
    '''Communicators send and receive data with a specific "tag" and store it until a user
    retrieves it.

    Lifetime: listen -> send -> get -> close

    Possible to simply just send -> get, however you won't be able to receive connection
    attempts from other hosts

    '''

    def __init__(self, port):
        self.connections = {}
        self.data_store = {}
        self.listen_thread = None
        self.listen_sock = None
        self.port = check_port(port)

        self.is_listening = False
        self.recv_callback = None

        self.conn_lock = threading.Lock()
        self.data_lock = threading.Lock()

    def send(self, addr, data, tag):
        '''Sends a message of bytes to addr with a tag identifier'''
        raise NotImplementedError("Can't use base communicator object. Use UDP or TCP")

    def listen(self, addr='0.0.0.0'):
        '''Listen for incoming messages or connection requests

            Args:
                addr (str): The IP address to listen for requests on. Default is '0.0.0.0'

        '''
        raise NotImplementedError("Can't use BaseCommunicator. Use UDP or TCP.")

    def _receive(self, data, addr):
        '''Take a piece of data which was received and process it into a message'''
        raise NotImplementedError()

    def close(self):
        '''Close all the communicator sockets'''
        raise NotImplementedError("Can't use BaseCommunicator. Use UDP or TCP.")

    def get(self, ip_addr, tag):
        '''Get a key/tag value from the data store.

        The data is only going to be located in the data store if every single packet for the given
        tag was received and able to be reassembled. Otherwise the incomplete data will reside
        in ``self.tmp_data``.

        The ``self.data_store`` object has the following structure:

        .. code-block:: javascript

            {
                ip_address: {
                    tag_1: data,
                    tag_2: data2
                }
            }


        Args:
            ip (str): The ip address of the host we wish get data from
            tag (bytes/bytearray): The data tag for the message which is being received

        Returns:
            bytes: ``None`` if complete data is not found, Otherwise if found will return the data

        '''
        data = None
        tg_int = int.from_bytes(tag[:TAG_SIZE], byteorder='little')
        if ip_addr not in self.data_store:
            data = None
        elif tg_int not in self.data_store[ip_addr]:
            data = None
        else:
            self.data_lock.acquire()
            data = self.data_store[ip_addr][tg_int]
            self.data_store[ip_addr][tg_int] = None
            self.data_lock.release()
        return data

    def register_recv_callback(self, callback):
        '''Allows one to register a callback function which is executed whenever a
        full message is received and decoded.

        The callback must have the signature of ``(str, bytes, bytes)`` where ``str`` is the sender
        address. The first ``bytes`` is the tag. The second ``bytes`` is the data.

        Args:
            callback (func): A function with arguments of (sender, tag, data)

        Returns:
            N/A
        '''
        if isinstance(callback, collections.Callable) is not True:
            raise TypeError("Callback must be a function")
        fullspec = inspect.getfullargspec(callback)
        n_args = len(fullspec[0])
        if n_args != 3:
            raise ValueError("Callback function did not have 3 arguments.")
        self.recv_callback = callback


class TCPCommunicator(BaseCommunicator):
    '''Communicators send and receive data with a specific "tag" and store it until a user
    retrieves it.

    Lifetime: listen -> send -> get -> close

    Possible to simply just send -> get, however you won't be able to receive connection
    attempts from other hosts

    '''

    def __init__(self, port):
        super().__init__(port)

    def connect(self, ip_addr, timeout=None):
        '''Connect to a TCP socket at ip_addr using the send_port.

        The connection of addr will be put into the internal connections dictionary. When the next
        tcp_send call is initiated a new thread will start reading messages from the connection

        The call will block until timeout seconds have passed. A socket.TimeoutError will be
        raised if the connection is not successful. Otherwise if timeout is None the call
        will block indefinitely until a connection has been made.

        Args:
            ip_addr (str): The IP to connect to on self.send_port
            timeout (float): Number of seconds to wait before timing out the connection.

        Returns: N/A
        '''
        conn = None
        # Work until we hit the timeout
        self.conn_lock.acquire()
        if ip_addr in self.connections:
            self.conn_lock.release()
            logger.warning('Attempting to make a connection to %s which already exists.', ip_addr)
            return
        self.conn_lock.release()

        msg = None
        if timeout is None:
            while conn is None:
                try:
                    conn = socket.create_connection((ip_addr, self.port))
                except socket.timeout as err:
                    pass
                except OSError as err:
                    msg = "Got {} while trying to connect() to {}".format(err, ip_addr)
        else:
            time_start = time.time()
            while time.time() - time_start < timeout and conn is None:
                try:
                    conn = socket.create_connection((ip_addr, self.port), timeout=timeout)
                except OSError as err:
                    msg = str(err)

        if msg is not None:
            logger.info("Exception trying to connect to %s with err %s", ip_addr, msg)

        self.conn_lock.acquire()
        if conn is not None and ip_addr not in self.connections: # Brand new
            self.connections[ip_addr] = conn
            conn_thread = threading.Thread(target=self._run_connect, args=(conn, ip_addr))
            conn_thread.start()
        elif conn is not None and ip_addr in self.connections: # Already existing
            logger.warning("Connect to ip %s requested but connection already existed", ip_addr)
            conn.close()
        else:
            self.conn_lock.release() # Release here because we raise an exception
            raise ConnectionError('Unable to connect to {}'.format(ip_addr))
        self.conn_lock.release()

    def send(self, addr, data, tag):
        '''Sends data to specified hosts

        Args:
            addrs (str): IPv4 Address to send to
            data (bytes): Data to send
            tag (bytes): Message identifier. Will take up to first TAG_SIZE bytes

        '''
        # Create the packet with tag at the top
        msg = bytearray()
        msg += tag[:TAG_SIZE]
        msg += data
        mlen = len(msg) # msg length
        msg = struct.pack('!I', mlen) + msg # prepend message length to data
        self._attempt_send_data(addr, msg, timeout=5)

    def _attempt_send_data(self, addr, msg, timeout=None):
        with self.conn_lock:
            if addr in self.connections:
                self.connections[addr].sendall(msg)
                return

        try:
            self.connect(addr, timeout=timeout)
            # addr should now be in connections if connect() was successful
            with self.conn_lock:
                conn_sock = self.connections[addr]
                conn_sock.sendall(msg)
            logger.debug('Successfully transmitted data to %s', addr)
        except OSError as err:
            # Log message about how unable to send
            exception = 'Unable to send data to {}. Error: {}'.format(addr, err)
            logger.error(exception)
            raise RuntimeError(exception)

    def listen(self, addr='0.0.0.0'):
        '''Start listening on port ``self.port``. Creates a new thread where the socket will
        listen for incoming connections

        Args:
            addr (str): The address to accept new connections on
        '''
        if self.is_listening is True:
            raise RuntimeError('Cannot listen again. Socket already listening.')

        if self.listen_thread is None:  # Create thread if not already created
            self.listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.listen_thread = threading.Thread(target=self._run_tcp,
                                                  args=(self.listen_sock, addr,
                                                        self.port))

            self.is_listening = True
            self.listen_thread.start()

    def close(self):
        logger.debug('Close requested on communicator %s', self)
        if self.listen_thread != None:
            self.is_listening = False
            self.listen_thread.join()
            self.listen_thread = None
        try:
            self.listen_sock.close()
        except BaseException as err:
            logger.debug('exception closing listening socket: %s', err)

        self.conn_lock.acquire()
        for addr in self.connections:
            try:
                self.connections[addr].close()
            except BaseException as err:
                logger.debug('Closing %s, Error: %s', addr, err)
        self.connections = {}
        self.conn_lock.release()
        # raise NotImplementedError("Can't use BaseCommunicator. Use UDP or TCP.")

    def _run_tcp(self, _sock, host, port):
        '''Method which accepts TCP connections and passes them off to run_connect

        Args:
            _sock (sockets.socket): A socket object to bind to
            host (str): The hostname/IP that we should bind the socket to. Use empty string '' for
            senders who wish to contact you outside the network.
            port (int): The port as an integer
        '''
        _sock.settimeout(1.5)
        _sock.bind((host, port))
        # Accept maximum of three un-accepted conns before refusing
        _sock.listen(3)
        while self.is_listening:
            try:
                conn, addr = _sock.accept()
                #logger.info('Accepted new socket connection to %s', str(addr[0]))
                self.conn_lock.acquire()
                if addr[0] not in self.connections:
                    self.connections[addr[0]] = conn
                    thd = threading.Thread(target=self._run_connect,
                                           args=(conn, addr[0]))
                    thd.start()
                else:
                    logger.debug('Got new connection which was already present from %s', addr)
                    conn.close()
                self.conn_lock.release()
            except socket.timeout:
                pass
                # logger.debug('__run_tcp__, connection accept timeout: %s', err)
        logger.debug('Listening thread exiting')

    def _run_connect(self, connection, addr):
        '''Worker methods which receive data from TCP connections.

        Must be able to convert TCP byte streams into individual messages. In order to accomplish
        this we used a message-length prefixing strategy. Every message sent from the other end of
        the wire is required to prefix a 4-byte message length to the very front of every message.
        This allows us to 'chunk' the messages and read only the necessary bytes. If an invalid
        message length arrives then we close the socket connection and log an error.

        Args:
            connection (connection): A TCP connection from socket.accept()
            addr (str): The Inet(6) address representing the address of the client
        '''

        while self.is_listening:
            with self.conn_lock:
                if addr not in self.connections:
                    logger.warning("Run TCP on connection not in dict %s", addr)
                    connection.close()
                    return
            try:
                # Get message length
                len_b = recv_n_bytes(connection, TAG_SIZE)
                if len_b is None:
                    break
                m_len = struct.unpack('!I', len_b)[0]
                if m_len < 0:
                    # Log an error, close connection
                    logger.warning("msg len < 0")
                    break
                msg_data = recv_n_bytes(connection, m_len)
                if msg_data is not None:
                    self._receive(msg_data, addr)
                else:
                    logger.warning("msgdata is None")
                    break # Close connection if returned None

            except socket.timeout as err:
                pass
            except OSError as err:
                logger.warning('OSError on _run_tcp: %s', err)
                break

        self.conn_lock.acquire()
        self.connections.pop(addr, None)  # Remove the connection
        logger.debug("Popped connection with addr %s", addr)
        self.conn_lock.release()
        try:
            connection.close()
        except OSError as err:
            logger.debug('_run_connect, error closing TCP socket: %s', err)
        return

    def _receive(self, data, addr):
        '''Stores the packet data received into the data store

        Args:
            data (bytes) : The data to store.
            addr (str): The ip address of the node.

        '''
        if len(data) < TAG_SIZE:
            # Log error on data
            return
        data_tag = int.from_bytes(data[:TAG_SIZE], byteorder='little')
        dat = data[TAG_SIZE:]

        self.data_lock.acquire()
        if addr not in self.data_store:
            self.data_store[addr] = {}
        self.data_store[addr][data_tag] = dat
        self.data_lock.release()
        logger.debug('Stored data at [%s][%s]', addr, data_tag)
        if self.recv_callback != None:
            # run a callback on the newly collected data.
            self.recv_callback(addr, data_tag, dat)
        return

class UDPCommunicator(BaseCommunicator):
    '''This is a threaded class interface designed to send and receive messages
     'asynchronously' via python's threading interface. It was designed mainly
      designed for use in communication for the algorithm termed 'Cloud K-SVD'.

    This class provides the following methods for users

    - listen: Starts the threaded listener
    - send: sends data via the open socket
    - get: retrieve data from the data-store
    - stop: stop listening on the thread

    The typical sequence will be something like the following:

    1. Take the object you wish to send. Encode it to bytes.
     i.e. ``my_bytes = str([1, 2, 3, 4, 5]).encode('utf-8')``
    2. After encoding to bytes and creating a communicator,
     use ``send()`` in order to send it to the listening host.
     The methods here will take care of packet fragmenting and
     makes sure messages are reassembled correctly. You
     must also add a 'tag' to the data. It should be a TAG_SIZE-byte
     long identifier.

      - ``comm.send('IP_ADDRESS', my_bytes, 'tag1'.encode('utf-8'))``


    3. After sending, there's nothing else for the client to do'
    4. When the packet reaches the other end, each packet is received and
     catalogged. Once all of the pieces of a message are received,
    the message is transferred as a whole to the data store where it can
     be retrieved
    5. Use ``get()`` to retrieve the message from the sender and by tag. ``comm.get('ip', 'tag1')``

    As simple as that!

    Notes:

    - A limitation (dependent upon python implementation) is that there may only be a single python
    thread running at one time due to GIL (Global Interpreter Lock)

    - There is an intermediate step between receiving data and making it
     available to the user. The object must receive all packets in order to
     reconstruct the data into its original form in bytes. This is performed
     by the ``receive`` method.

    - Data segments which have not been reconstructed lie within
     ``self.tmp_data``. Reconstructed data is within ``self.data_store``

    Constructor Docs

    Args:
        protocol (str): A string. One of 'UDP' or 'TCP' (case insensistive)
        listen_port(int): A port between 0 and 65535
        send_port(int): (Optional) Defaults to value set for listen_port, otherwise
         must be set to a valid port number.

    '''

    def __init__(self, port):
        '''Constructor calls BaseCommunicator constructor and sets up tmp data store'''
        self.tmp_data = {}
        self.send_sock = None
        super().__init__(port)

    def close(self):
        '''Closes both listening sockets and the sending sockets

        The sockets may only be closed once. After closing a new object must be created.

        Args:
            N/A

        Returns:
            N/A

        '''
        logger.debug('Close requested on communicator %s', self)
        if self.is_listening is True:
            if self.listen_thread != None:
                self.is_listening = False
                self.listen_thread.join()
                self.listen_thread = None
        # On a close, we can still send afterwards, but we never receive anything
        try:
            self.listen_sock.close()
        except BaseException as err:
            logger.debug('exception closing listening socket: %s', err)

        try:
            self.send_sock.close()
        except BaseException as err:
            logger.debug('exception closing sending socket: %s', err)

        self.send_sock = None
        self.listen_sock = None

    def listen(self, addr='0.0.0.0'):
        '''Start listening on port ``self.port``. Creates a new thread where the socket will
        listen for incoming data

        Args:
            N/A

        Returns:
            N/A

        '''
        if self.listen_thread is None:  # Create thread if not already created
            self.listen_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            # Set to nonblocking in order to get our threaded server to work :) (We
            # should investigate the performance impact of this)
            self.listen_sock.setblocking(False)
            self.listen_thread = threading.Thread(target=self._run_listen,
                                                  args=(self.listen_sock, addr,
                                                        self.port))

            self.is_listening = True
            self.listen_thread.start()

    def _run_listen(self, _sock, host, port):
        '''Worker method for the threaded listener in order to retrieve incoming messages

        Messages should be kept under 2048 bytes, undefined behavior will occur with larger
        messages. A future version may fix this restriction.

        Args:
            _sock (sockets.socket): A socket object to bind to
            host (str): The hostname/IP that we should bind the socket to. Use empty string '' for
            senders who wish to contact you outside the network.
            port (int): The port as an integer

        Returns:
            N/A

        '''
        try:
            _sock.bind((host, port))
        except BaseException as err:
            logger.warning("Could not bind to %s:%s, Got %s", host, port, err)
            _sock.close()
            self.close()
            return

        while self.is_listening:
            try:
                data, addr = _sock.recvfrom(2048)  # Receive at max 2048 bytes
                # logger.debug('Received data from address {}'.format(addr))
                self._receive(data, addr[0])
            except BlockingIOError:
                pass

        _sock.close()

    def send(self, ip_addr, data, tag):
        '''Send a chunk of data with a specific tag to an ip address. The packet will be
        automatically chunked into N packets where N = ceil(bytes/(MTU-68))

        Args:
            ip (str): The hostname/ip to send to
            data (bytes): The bytes of data which will be sent to the other host.
            tag (bytes): An identifier for the message

        Returns:
            bool: True if all packets were created and sent successfully.
        '''
        if self.send_sock is None:
            self.send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # As simple as just creating the packets and sending each one
        # individually
        ret = True
        packets = create_packets(data, tag)
        # logger.debug("Sending {} packet(s) to {}".format(len(packets), ip))
        for packet in packets:
            # logger.debug('Sending packet to IP: {} on port {}'.format(ip, self.send_port))
            try:
                if self.send_sock.sendto(packet, (ip_addr, self.port)) < 0:
                    logger.debug("Some packets were not sent successfully")
                    ret = False

            except OSError as err:
                ret = False
                logger.warning(str(err))
                break
        return ret

    def _receive(self, data, addr):
        '''Take a piece of data received over the socket and processes the data and attempt to
        combine packet sequences together, passing them to the data store when ready.

        ``self.tmp_data`` is an object with the structure

        .. code-block:: javascript

            {
                ip_address: {
                    tag_1: {
                        'seq_total': Max_num_packets,
                        'packets' = {
                            1: packet_data_1,
                            2: packet_data_2,
                            ...
                            ...
                        }
                    },
                    tag_2: {
                        ...
                    }
                },
                ip_address_2 : {
                    ...
                }
            }

        Args:
            data (bytes): a packet of data received over the socket to process
            addr (str): The ip address or hostname of the sending host

        Returns:
            N/A
        '''

        if addr not in self.tmp_data:
            self.tmp_data[addr] = {}

        # disassemble the packet
        seq_total = struct.unpack('H', data[0:2])[0]
        seq_num = struct.unpack('H', data[2:4])[0]
        data_tag = int.from_bytes(data[4:4+TAG_SIZE], byteorder='little')
        dat = data[4+TAG_SIZE:]

        # Create an entry for data_tag
        if data_tag not in self.tmp_data[addr]:
            self.tmp_data[addr][data_tag] = {}
            self.tmp_data[addr][data_tag]['packets'] = {}
            self.tmp_data[addr][data_tag]['seq_total'] = seq_total

        if (seq_total != self.tmp_data[addr][data_tag]['seq_total']
                or self.tmp_data[addr][data_tag]['seq_total'] is None):
            # If the tag existed, make sure the sequence total is equal to the
            # current, otherwise throw away any packets we've already collected
            self.tmp_data[addr][data_tag]['seq_total'] = seq_total
            self.tmp_data[addr][data_tag]['packets'] = {}

        self.tmp_data[addr][data_tag]['packets'][seq_num] = dat

        num_packets = len(self.tmp_data[addr][data_tag]['packets'])
        # seq_total is max index of 0-index based list.
        if num_packets == seq_total + 1:
            # Reassmble the packets in order
            reassembled = bytes()
            for i in range(num_packets):
                reassembled += self.tmp_data[addr][data_tag]['packets'][i]
            if addr not in self.data_store:
                self.data_store[addr] = {}
            logger.debug("Adding reassmbled packet to data store with IP %s and tag %s",
                         addr, data_tag)
            self.data_lock.acquire()
            self.data_store[addr][data_tag] = reassembled
            self.data_lock.release()
            if self.recv_callback != None:
                # run a callback on the newly collected packets.
                self.recv_callback(addr, data_tag, reassembled)
            self.tmp_data[addr][data_tag]['packets'] = {}
            self.tmp_data[addr][data_tag]['seq_total'] = {}

def get_mtu():
    '''Attempts to return the MTU for the network by finding the min of the
    first hop MTU and 576 bytes. i.e min(MTU_fh, 576)

    Note that the 576 byte sized MTU does not account for the checksum/ip headers so
    when sending data we need to take the IP/protocol headers into account.

    The current implementation just assumes the default minimum of 576.
    We should try to implement something to actually calculate min(MTU_fh, 576)

    Returns:
        int: 576
    '''
    return 576


def check_port(port):
    '''Checks if a port is valid.

    A port is restricted to be a 16 bit integer which is in the range 0 < port < 65535.
    Typically most applications will use ports > ~5000

    Args:
        port (int): The port number. ValueError raised if it is not an int.

    Returns:
        int: returns the port number if valid, otherwise an error is raised

    '''
    if isinstance(port, int) is not True:  # Ensure we're dealing with real ports
        raise TypeError("port must be an integer")
    elif port < 0 or port > 65535:
        raise ValueError("port must be between 0 and 65535")
    else:
        return port

def create_packets(data, tag):
    '''Segments a chunk of data (payload) into separate packets in order to send in sequence to
    the desired address.

    The messages sent using this class are simple byte arrays, which include metadata, so in
    order to segment into the correct number of packets we also need to calculate the size of
    the packet overhead (metadata) as well as subtract the IP headers in order to find the
    maximal amount of payload data we can send in a single packet.

    We need to use the MTU in this case which we'll take as typically a minimum of 576 bytes.
    According to RFC 791 the maximum IP header size is 60 bytes (typically 20), and according
    to RFC 768, the UDP header size is 8 bytes. This leaves the bare minimum payload size to be
    508 bytes. (576 - 60 - 8).

    We will structure packets as such (not including UDP/IP headers)

    +---------------------+--------------------+----------------------+
    | Seq.Total (2 bytes) | Seq. Num (2 bytes) | Tag (TAG_SIZE bytes) |
    +---------------------+--------------------+----------------------+
    |                            Data (500 Bytes)                     |
    +-----------------------------------------------------------------+

    A limitation is that we can only sequence a total of 2^16 packets which, given a max data
    size of 500 bytes gives us a maximum data transmission of (2^16)*500 ~= 33MB for a single
    request.

    Also note that the Seq Num. is zero-indexed so that the maximum sequence number (and the
    sequence total) will go up to ``len(packets) - 1``. Or in other words, 1 less than the
    number of packets.

    Args:
        data (bytes): The data as a string which is meant to be sent to its destination
        tag (bytes): A tag. Only the first TAG_SIZE bytes are added as the tag.

    Returns
        list: A list containing the payload for the packets which should be sent to the
        destination.
    '''
    packets = []
    max_payload = get_mtu() - 68  # conservative estimate to prevent IP fragmenting on UDP
    metadata_size = 8  # 8 bytes
    data_size = len(data)
    max_data = max_payload - metadata_size

    # TWO CASES
    # - We can send everything in 1 packet
    # - We must break into multiple packets which will require sequencing
    if data_size <= max_data:
        # Assemble the packet, no sequencing
        payload = build_meta_packet(0, 0, tag)
        payload += data
        packets.append(payload)
    else:
        total_packets = math.floor(data_size / max_payload)
        for i in range(total_packets):  # [0, total_packets-1]
            pkt1 = build_meta_packet(i, total_packets, tag)

            # Slice data into ~500 byte packs
            dat1 = data[i * max_data:(i + 1) * max_data]
            pkt1 += dat1
            packets.append(pkt1)
        # Build the  final packet
        pkt1 = build_meta_packet(total_packets, total_packets, tag)
        min_bound = (total_packets) * max_data
        dat1 = data[min_bound:]
        pkt1 += dat1
        packets.append(pkt1)

    return packets

def get_payload(payload):
    '''Take data payload and return a byte array of the object. Should be
    structured as a dict/list object. Note this method is slightly expensive
    because it encodes a dictionary as a JSON string in order to get the bytes

    We also set the separators to exclude spaces in the interest of saving
     data due to spaces being unnecessary. This is a primitive way to convert
    data into bytes and you can load it.

    Args:
        payload(obj): A JSON serializable object representing the payload data

    Returns:
        bytes: The object as a utf-8 encoded string
    '''
    data = json.dumps(payload, separators=[':', ',']).encode('utf-8')
    return data


def decode_payload(payload):
    '''Takes a byte array and converts it into an object using ``json.loads``

    Args:
        payload (bytearray): An array of bytes to decode and convert into an object.

    Returns:
        dict/list: A dictionary or list object depending on the JSON that was encoded
    '''

    data = json.loads(payload.decode('utf-8'), separator=([':', ',']))
    return data


def build_meta_packet(seq_num, seq_total, tag):
    '''Create a bytearray which returns a sequence of bytes based on the metadata

    Args:
        seq_num(int): The packet's sequence number
        seq_total(int): The total number of sequence packets to be sent
        tag (bytes): A string or bytes object to encode as the data tag

    Returns:
        bytearray: A bytearray with the metadata
    '''
    if isinstance(seq_total, int) is False or isinstance(seq_num, int) is False:
        raise TypeError("Sequence number and total must be integer")
    packet = bytearray()
    packet += struct.pack('H', seq_total)
    packet += struct.pack('H', seq_num)
    packet += tag[0:4]
    return packet

def recv_n_bytes(conn, num):
    '''Get a set number of bytes from a TCP connection

    Args:
        conn (socket): A connected TCP socket
        num (int): Number of bytes to read

    Returns:
        bytes: a bytes object containing the data read. None if num < 0
    '''
    if num < 0:
        raise ValueError("conn {} request {} bytes. Bytes must be > 0".format(conn, num))
    bytes_left = num
    msg_b = b''
    while len(msg_b) < num:
        try:
            bts = conn.recv(bytes_left)
            if len(bts) == 0:
                return None
                # break  # Empty str means closed socket
            msg_b += bts
            bytes_left -= len(bts)
        except socket.timeout as err:
            # Socket timed out waiting for bytes
            pass
        except OSError as err:
            # Socket timed out - log an error eventually
            logger.info('recv_n_bytes: %s', str(err))
            return None
    return msg_b
