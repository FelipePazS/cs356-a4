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
        logging.debug("ACQUIRED")
        seq_num = self.counter
        self.counter = self.counter + 1

        swp = SWPPacket(SWPType.DATA, seq_num, data)
        self.packets[seq_num] = swp
        logging.debug("Sent: %s" % swp)
        self._llp_endpoint.send(swp.to_bytes())
        timer = threading.Timer(SWPSender._TIMEOUT, SWPSender._retransmit, [self, seq_num])
        self.timers[seq_num] = timer
        timer.start()
        return
        
    def _retransmit(self, seq_num):
        swp = self.packets.get(seq_num)
        logging.debug("Sent: %s" % swp)
        self._llp_endpoint.send(swp.to_bytes())
        timer = threading.Timer(SWPSender._TIMEOUT, SWPSender._retransmit, [self, seq_num])
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

            self.send_window_semaphore.release()
            logging.debug("RELEASED")
            self.last_ack = seq_num

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
    
        self.packets = {}

        self.highest_ack_seq_num = 0


    def recv(self):
        return self._ready_data.get()

    def _recv(self):
        while True:
            # Receive data packet
            raw = self._llp_endpoint.recv()
            packet = SWPPacket.from_bytes(raw)
            logging.debug("Received: %s" % packet)
            # retransmit if already acknowledged 
            if packet._type == SWPType.ACK:
                # retransmit
                continue 

            # 1. Check if the chunk of data was already acknowledged and retransmit an SWP ACK containing the highest acknowledged sequence number
            if (packet.seq_num <= self.highest_ack_seq_num and not packet.seq_num == 0):
                swp = SWPPacket(SWPType.ACK, packet.seq_num)
                self._llp_endpoint.send(swp.to_bytes())
                continue

            # if the packet comes with a sequence number greater than the last read + windows size, drop the packet
            

            # 2. Add the chunk of data to a buffer—in case it is out of order.
            # self.packets[packet.seq_num % SWPSender._SEND_WINDOW_SIZE] = packet
            self.packets[packet.seq_num] = packet

            # 3. Traverse the buffer, starting from the first buffered chunk of data, 
            # until reaching a “hole”—i.e., a missing chunk of data. All chunks of data prior to this hole 
            # should be placed in the _ready_data queue, which is where data is read from when an “application” 
            # (e.g., server.py) calls recv, and removed from the buffer.
            if packet.seq_num == 0:
                self._ready_data.put(self.packets.pop(packet.seq_num).data)
            else:
                for i in range(self.highest_ack_seq_num + 1, self.highest_ack_seq_num + 1 + len(self.packets)) :
                    if not i in self.packets:
                        break
                    self.highest_ack_seq_num = i
                    self._ready_data.put(self.packets.pop(i).data)
            # 4. Send an acknowledgement for the highest sequence number for which all data chunks up to and including that sequence number have been received.
            swp = SWPPacket(SWPType.ACK, self.highest_ack_seq_num)
            logging.debug("Sent: %s" % swp)
            self._llp_endpoint.send(swp.to_bytes())

        return
