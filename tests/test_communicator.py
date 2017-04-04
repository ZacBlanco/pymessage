import json
import random
import struct
import time
import unittest
import threading
import socket
import struct
from unittest.mock import MagicMock, patch
import sys
import os
sys.path.append(os.getcwd())
import pymessage as comm
from pymessage import UDPCommunicator, TCPCommunicator


class TestCommModuleMethods(unittest.TestCase):

    def test_get_mtu(self):
        mtu1 = comm.get_mtu()
        self.assertEqual(mtu1, 576)

    def test_port_str(self):
        prt1 = ''
        with self.assertRaises(TypeError):
            comm.check_port(prt1)

    def test_port_ltz(self):
        prt1 = -1
        with self.assertRaises(ValueError):
            comm.check_port(prt1)

    def test_port_gtsf(self):
        prt1 = 65536
        with self.assertRaises(ValueError):
            comm.check_port(prt1)

    def test_port_valid(self):
        self.assertEqual(comm.check_port(8080), 8080)
        self.assertEqual(comm.check_port(10000), 10000)


class TestUDPCommunicator(unittest.TestCase):

    def test_bad_init(self):
        '''Test constructors with bad parameters
        Don't need to close b/c we never actually create an object.
        '''
        with self.assertRaises(ValueError):
            UDPCommunicator(999090)

        with self.assertRaises(ValueError):
            UDPCommunicator(90000)

        with self.assertRaises(TypeError):
            UDPCommunicator('123')

        with self.assertRaises(ValueError):
            UDPCommunicator(-1)


    def test_init(self):
        comm1 = UDPCommunicator(9887)
        self.assertEqual(comm1.port, 9887)
        self.assertEqual(comm1.is_listening, False)
        self.assertEqual(comm1.listen_thread, None)
        comm1.close()

    def test_close(self):
        comm1 = UDPCommunicator(9887)
        self.assertEqual(comm1.is_listening, False)

        comm1.close()
        self.assertEqual(comm1.is_listening, False)
        self.assertEqual(comm1.listen_sock, None)
        self.assertEqual(comm1.send_sock, None)


    def test_double_close(self):
        comm1 = UDPCommunicator(9090)
        comm1.listen()
        comm1.close()
        comm1.close()

    def test_payload(self):
        l = []
        for i in range(10):
            l.append(i)
        data = {'hello': 'world',
                'test': l}
        data1 = json.dumps(data, separators=[':', ',']).encode('utf-8')
        self.assertEqual(data1, comm.get_payload(data))

    def test_packet_create_single(self):
        l = []
        for i in range(100):
            l.append(i)
        comm1 = UDPCommunicator(9090)
        l = str(l).encode('utf-8')
        p1 = comm.create_packets(l, '9012'.encode('utf-8'))
        self.assertEqual(len(p1), 1)
        p1 = p1[0]
        comm1.close()

    def test_large_packet(self):
        comm1 = UDPCommunicator(10001)
        d = []
        for i in range(1000):
            d.append(random.random())
        d = str(d).encode('utf-8')
        packs = comm.create_packets(d, 'tag1'.encode('utf-8'))
        r = bytes() # total data bytes
        t = bytes()
        for packet in packs:
            r += packet[8:]
            t += packet

        self.assertEqual(len(d), len(r))
        self.assertEqual(len(d), len(t) - 8*len(packs))
        for i in range(len(packs)):
            seq = struct.unpack('H', packs[i][2:4])[0]
            t = struct.unpack('H', packs[i][0:2])[0]
            self.assertEqual(seq, i)
            self.assertEqual(t, len(packs) - 1)
        comm1.close()

    def test_single_packet(self):
        comm1 = UDPCommunicator(8080)
        d = []
        for i in range(122): # Exactly 500 bytes
            d.append(i)
        d = str(d).encode('utf-8')
        d = comm.create_packets(d, '1111'.encode('utf-8'))
        self.assertEqual(len(d), 1, 'Should have only created a single packet')
        self.assertEqual('1111'.encode('utf-8'), d[0][4:8])
        self.assertEqual(0, struct.unpack('H', d[0][0:2])[0])
        self.assertEqual(0, struct.unpack('H', d[0][2:4])[0])
        comm1.close()

    def test_close_ops(self):
        comm1 = UDPCommunicator(80)
        comm1.close()
        comm1.close()

    @patch('socket.socket.sendto', side_effect=[1, -1, 5, -1])
    def test_mocked_send(self, mock1):
        comm1 = UDPCommunicator(10001)
        msg = 'ayyyy'.encode('utf-8')
        self.assertEqual(comm1.send('192.168.1.1', msg, 'noice'.encode('utf-8')), True)
        self.assertEqual(comm1.send('192.168.1.1', msg, 'noice'.encode('utf-8')), False)
        self.assertEqual(comm1.send('192.168.1.1', msg, 'noice'.encode('utf-8')), True)
        self.assertEqual(comm1.send('192.168.1.1', msg, 'noice'.encode('utf-8')), False)
        comm1.close()

    @patch('socket.socket.sendto', return_value=5)
    def test_big_mock_send(self, mock1):
        list_bytes = str(list(range(1000))).encode('utf-8')
        comm1 = UDPCommunicator(10001)
        self.assertEqual(comm1.send('abcomm1213', list_bytes, 'big_'.encode('utf-8')), True)
        mock1.return_value = -1
        self.assertEqual(comm1.send('abcomm1213', list_bytes, 'big_'.encode('utf-8')), False)
        comm1.close()


    def test_get(self):
        '''Encodes and decodes a single packet.'''
        comm1 = UDPCommunicator(10001)
        l = str(list(range(100))).encode('utf-8')
        for packet in comm.create_packets(l, '_get'.encode('utf-8')):
            comm1._receive(packet, 'test')
        r = comm1.get('test', '_get'.encode('utf-8'))
        self.assertNotEqual(r, None)
        self.assertEqual(l, r, 'Reassembled bytes should be the same.')
        comm1.close()

    def test_create_large(self):
        '''This test helped to fix a bug where we were accidentally appending an extra blank packet
        when creating packets'''
        comm1 = UDPCommunicator(10001)
        l = str(list(range(550))).encode('utf-8')
        packets = comm.create_packets(l, '_get'.encode('utf-8'))
        self.assertEqual(len(packets), 6)

        comm1.close()

    def test_large_get(self):
        '''This test assures that packets split into multiple pieces and received are able to be
        reassembled corectly.'''
        comm1 = UDPCommunicator(10001)
        l = str(list(range(1000))).encode('utf-8')
        packets = comm.create_packets(l, '_get'.encode('utf-8'))
        for packet in packets:
            comm1._receive(packet, 'test')
        r = comm1.get('test', '_get'.encode('utf-8'))
        self.assertNotEqual(r, None)
        self.assertEqual(l, r, 'Reassembled bytes should be the same.')
        comm1.close()

    @patch('socket.socket.recvfrom')
    def test_mock_listen(self, mock1):
        l = str(list(range(20))).encode('utf-8')
        d = struct.pack('H', 0)
        d += d
        d += 'test'.encode('utf-8')
        d += l
        mock1.return_value = (d, ('127.0.0.1', 9071))
        comm1 = UDPCommunicator(9071)
        comm1.listen()
        self.assertNotEqual(comm1.listen_thread, None)
        self.assertEqual(comm1.is_listening, True)
        comm1._receive = MagicMock()
        comm1.send('127.0.0.1', l, 'test'.encode('utf-8'))

        # Give some time for the other thread to run before checking conditions
        ctr = 0
        while mock1.called != True and comm1._receive.called != True and ctr < 20:
            time.sleep(0.1)
            ctr += 1

        mock1.assert_called_with(2048)
        comm1.close()
        comm1._receive.assert_called_with(d, '127.0.0.1')

    def test_same_tag_send(self):


        comm1 = UDPCommunicator(9071)
        for i in range(10):
            msg = 'Iteration: {}'.format(i).encode('utf-8')
            packets = comm.create_packets(msg, 'test'.encode('utf-8'))
            for packet in packets:
                comm1._receive(packet, 'local')
            self.assertEqual(comm1.get('local', 'test'.encode('utf-8')).decode('utf-8'),
                             msg.decode('utf-8'))

        comm1.close()

    def test_multi_get(self):

        comm1 = UDPCommunicator(9071)
        s = bytes(str(range(1000)).encode('utf-8'))
        pkts = comm.create_packets(s, 'tg11'.encode('utf-8'))
        for pkt in pkts:
            comm1._receive(pkt, 'local')
        s2 = comm1.get('local', 'tg11'.encode('utf-8'))
        self.assertEqual(s, s2, "Bytes should be able to be retrieved")
        s2 = comm1.get('local', 'tg11'.encode('utf-8'))
        self.assertNotEqual(s2, s, "Should not be able to retrieve the same data again")
        comm1.close()

    def test_register_callback(self):
        def cbk(a, b, c):
            return "callback"
        def bck(a, b):
            return "bad"
        comm1 = UDPCommunicator(9071)
        comm1.register_recv_callback(cbk)

        # Should raise error on non-function
        with self.assertRaises(TypeError):
            comm1.register_recv_callback("a")

        # Should raise error on bad function signature
        with self.assertRaises(ValueError):
            comm1.register_recv_callback(bck)

        comm1.close()

class TCPCommTest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        self.comm1 = None
        super().__init__(*args, **kwargs)

    def setUp(self):
        if self.comm1 is None:
            self.comm1 = TCPCommunicator(9090)
        else:
            self.comm1.close()
            self.comm1 = TCPCommunicator(9090)

    def tearDown(self):
        if self.comm1 is not None:
            self.comm1.close()

    def test_multiple_connections(self):
        '''Try connecting to the same host multiple times'''
        self.comm1 = TCPCommunicator(9090)
        self.comm1.listen()
        self.comm1.connect('localhost')
        self.comm1.send('localhost', b'12345', b'1234')
        time.sleep(0.2)
        data = self.comm1.get('127.0.0.1', b'1234')
        self.assertEqual(data, b'12345')
        self.comm1.close()

    def test_send_no_connect(self):
        '''Send a message before connecting'''
        self.comm1 = TCPCommunicator(7777)
        self.comm1.listen()
        self.comm1.send('localhost', b'ABC1234', b'99999')
        time.sleep(0.1)
        data = self.comm1.get('127.0.0.1', b'9999')
        self.assertEqual(data, b'ABC1234')


    def test_constructor(self):
        '''Make sure we can create a TCP object'''
        self.comm1 = TCPCommunicator(8998)
        self.assertNotEqual(self.comm1, None)
        self.comm1.close()
        self.comm1 = TCPCommunicator(8998)
        self.assertNotEqual(self.comm1, None)
        self.comm1.close()


    def test_listen(self):
        '''Make sure the listening thread gets created'''
        self.comm1 = TCPCommunicator(8998)
        self.comm1.listen()
        self.assertEqual(self.comm1.is_listening, True)
        self.assertNotEqual(self.comm1.listen_thread, None)
        self.comm1.close()
        self.assertNotEqual(self.comm1.is_listening, True)
        self.assertEqual(self.comm1.listen_thread, None)

    @patch('socket.socket.accept', return_value=(MagicMock(), ('127.0.0.1', 1234)))
    @patch('pymessage.TCPCommunicator._run_connect', return_value=-1)
    def test_run_tcp(self, mock_conn, mock_sock):
        '''Make sure that we create all of our threads'''
        self.comm1 = TCPCommunicator(8998)
        self.comm1.is_listening = True
        self.comm1.listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        thd = threading.Thread(target=self.comm1._run_tcp,
                               args=(self.comm1.listen_sock, '0.0.0.0', self.comm1.port))
        thd.start()
        time.sleep(0.1)
        self.comm1.is_listening = False
        thd.join()
        self.assertTrue(mock_sock.called)
        self.assertTrue(mock_conn.called)
        self.assertTrue(isinstance(self.comm1.connections['127.0.0.1'], MagicMock))
        self.comm1.close()

    @patch('socket.socket.recv',
           side_effect=[struct.pack('!I', 5), 'hello world'.encode('utf-8'), b''])
    def test_run_connect(self, mock_recv):
        '''Make sure that we can receive data'''
        self.comm1 = TCPCommunicator(8998)
        self.comm1.connections['127.0.0.1'] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.comm1.is_listening = True
        self.comm1._receive = MagicMock()
        self.comm1._receive.return_value = -1
        thd = threading.Thread(target=self.comm1._run_connect,
                               args=(self.comm1.connections['127.0.0.1'], '127.0.0.1'))
        thd.start()
        time.sleep(0.1)
        self.comm1.is_listening = False
        thd.join()
        self.assertTrue(self.comm1._receive.called)
        self.assertTrue(mock_recv.called)
        self.assertTrue('127.0.0.1' not in self.comm1.connections)
        self.comm1.close()

    def test_recv_get_tcp(self):
        '''Ensure we can put a packet into the data store'''
        self.comm1 = TCPCommunicator(8998)
        tag = 'abcd'.encode('utf-8')[0:4]
        data = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'.encode('utf-8')
        pkt = tag + data
        self.comm1._receive(pkt, '127.0.0.1')
        self.assertEqual(self.comm1.get('127.0.0.1', tag), data)
        self.comm1.close()

    def test_recv_n_bytes(self):
        '''Make sure that the function to receive at most "n" bytes works correctly
        Used for message delimiting'''
        with patch('socket.socket.recv', side_effect=[b'1', b'2', b'3', b'4']):
            _sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            msg = comm.recv_n_bytes(_sock, 4)
            self.assertEqual(msg, b'1234')
            _sock.close()

        def recv(n):
            if n < 8:
                return b'tcp_sock'[:n]
            else:
                return b'tcp_sock'
        with patch('socket.socket.recv', side_effect=recv):
            _sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            msg = comm.recv_n_bytes(_sock, 5)
            self.assertEqual(msg, b'tcp_s')
            msg = comm.recv_n_bytes(_sock, 25)
            self.assertEqual(msg, (b'tcp_sock'*5)[:25])
            with self.assertRaises(ValueError):
                msg = comm.recv_n_bytes(_sock, -1)
            _sock.close()

        with patch('socket.socket.recv', return_value=struct.pack('!I', 512)):
            _sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            msg = comm.recv_n_bytes(_sock, 4)
            self.assertEqual(struct.unpack('!I', msg)[0], 512)
            _sock.close()

        with patch('socket.socket.recv', side_effect=[b'\x00', b'\x00', b'\x02', b'\x00']):
            _sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            msg = comm.recv_n_bytes(_sock, 4)
            self.assertEqual(struct.unpack('!I', msg)[0], 512)
            _sock.close()

if __name__ == "__main__":
    unittest.main()
