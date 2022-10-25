import enum
import logging
import llp
import queue
import struct
import threading

class SWPType(enum.IntEnum):
    DATA = ord('D')
    ACK = ord('A')

class SWPPacket:
    _PACK_FORMAT = '!BI'
    _HEADER_SIZE = struct.calcsize(_PACK_FORMAT)
    MAX_DATA_SIZE = 1400 # Leaves plenty of space for IP + UDP + SWP header 

    def __init__(self, type, seq_num, data=b''):
        self._type = type
        self._seq_num = seq_num
        self._data = data

    @property
    def type(self):
        return self._type

    @property
    def seq_num(self):
        return self._seq_num
    
    @property
    def data(self):
        return self._data

    def to_bytes(self):
        header = struct.pack(SWPPacket._PACK_FORMAT, self._type.value, 
                self._seq_num)
        return header + self._data
       
    @classmethod
    def from_bytes(cls, raw):
        header = struct.unpack(SWPPacket._PACK_FORMAT,
                raw[:SWPPacket._HEADER_SIZE])
        type = SWPType(header[0])
        seq_num = header[1]
        data = raw[SWPPacket._HEADER_SIZE:]
        return SWPPacket(type, seq_num, data)

    def __str__(self):
        return "%s %d %s" % (self._type.name, self._seq_num, repr(self._data))

class SWPSender:
    _SEND_WINDOW_SIZE = 5
    _TIMEOUT = 1

    def __init__(self, remote_address, loss_probability=0):
        self._llp_endpoint = llp.LLPEndpoint(remote_address=remote_address,
                loss_probability=loss_probability)

        # Start receive thread
        self._recv_thread = threading.Thread(target=self._recv)
        self._recv_thread.start()

        self.send_window_semaphore = threading.Semaphore(SWPSender._SEND_WINDOW_SIZE)
        self.counter_semaphore = threading.Semaphore(1)
        self.counter = 0
        self.last_ack = 0

        # array of packets and timers
        self.timers = {}
        self.packets = {}



    def send(self, data):
        for i in range(0, len(data), SWPPacket.MAX_DATA_SIZE):
            self._send(data[i:i+SWPPacket.MAX_DATA_SIZE])

    def _send(self, data):
        self.send_window_semaphore.acquire()
        self.counter_semaphore.acquire()
        seq_num = self.counter
        self.counter = self.counter + 1
        self.counter_semaphore.release()

        swp = SWPPacket(SWPType.DATA, seq_num, data)
        self.packets[seq_num] = swp
        self._llp_endpoint.send(swp.to_bytes())
        timer = threading.Timer(SWPSender._TIMEOUT, SWPSender._retransmit, seq_num)
        self.timers[seq_num] = timer
        timer.start()
        return
        
    def _retransmit(self, seq_num):
        swp = SWPPacket(SWPType.DATA, seq_num, self.packets[seq_num])
        self._llp_endpoint.send(swp.to_bytes())
        timer = threading.Timer(SWPSender._TIMEOUT, SWPSender._retransmit, seq_num)
        self.timers[seq_num] = timer
        timer.start()
        return 

    def _recv(self):
        while True:
            # Receive SWP packet
            raw = self._llp_endpoint.recv()
            if raw is None:
                continue
            packet = SWPPacket.from_bytes(raw)
            logging.debug("Received: %s" % packet)
            if not packet._type == SWPType.ACK:
                continue
            
            seq_num = packet._seq_num
            timer = self.timers[seq_num]
            timer.cancel()

            for i in range(self.last_ack + 1, seq_num + 1):
                self.timers.pop(i)
                self.packets.pop(i)

            self.last_ack = seq_num
            self.send_window_semaphore.release()

        return

class SWPReceiver:
    _RECV_WINDOW_SIZE = 5

    def __init__(self, local_address, loss_probability=0):
        self._llp_endpoint = llp.LLPEndpoint(local_address=local_address, 
                loss_probability=loss_probability)

        # Received data waiting for application to consume
        self._ready_data = queue.Queue()

        # Start receive thread
        self._recv_thread = threading.Thread(target=self._recv)
        self._recv_thread.start()
        
        # TODO: Add additional state variables


    def recv(self):
        return self._ready_data.get()

    def _recv(self):
        while True:
            # Receive data packet
            raw = self._llp_endpoint.recv()
            packet = SWPPacket.from_bytes(raw)
            logging.debug("Received: %s" % packet)
            
            # TODO

        return
