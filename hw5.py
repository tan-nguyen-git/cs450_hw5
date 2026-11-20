"""
Where solution code to HW5 should be written.  No other files should
be modified.

Protocol (all packets start with a 1 byte type and a 4 byte sequence number):
    DATA   seq, payload   -- chunk number `seq` of the file
    ACK    seq            -- cumulative: every chunk < seq has been received
    FIN    seq            -- sender is done; there are exactly `seq` chunks
    FINACK seq            -- receiver got everything and acknowledges the FIN
"""

import socket
import io
import time
import typing
import struct
import homework5
import homework5.logging

DATA, ACK, FIN, FINACK = 0, 1, 2, 3
HEADER = struct.Struct("!BI")
PAYLOAD = homework5.MAX_PACKET - HEADER.size
WINDOW = 2          # at most 2 packets fit on the wire anyway
MIN_RTO = 0.02
MAX_RTO = 1.0
FIN_TRIES = 8


def _pkt(kind: int, seq: int, payload: bytes = b"") -> bytes:
    return HEADER.pack(kind, seq) + payload


def _parse(raw: bytes):
    if len(raw) < HEADER.size:
        return None
    kind, seq = HEADER.unpack_from(raw)
    return kind, seq, raw[HEADER.size:]


def send(sock: socket.socket, data: bytes):
    """
    Implementation of the sending logic for sending data over a slow,
    lossy, constrained network.

    Args:
        sock -- A socket object, constructed and initialized to communicate
                over a simulated lossy network.
        data -- A bytes object, containing the data to send over the network.
    """
    logger = homework5.logging.get_logger("hw5-sender")
    chunks = [data[i:i + PAYLOAD] for i in range(0, len(data), PAYLOAD)]
    total = len(chunks)

    est_rtt = None
    dev_rtt = 0.0
    rto = 0.5

    def update_rtt(sample):
        nonlocal est_rtt, dev_rtt, rto
        if est_rtt is None:
            est_rtt, dev_rtt = sample, sample / 2
        else:
            dev_rtt = 0.75 * dev_rtt + 0.25 * abs(sample - est_rtt)
            est_rtt = 0.875 * est_rtt + 0.125 * sample
        rto = min(MAX_RTO, max(MIN_RTO, est_rtt + 4 * dev_rtt))

    base = 0            # oldest unacked chunk
    nxt = 0             # next chunk never sent
    sent_at = {}        # seq -> time of (latest) transmission
    retx = set()        # seqs that were retransmitted (Karn's algorithm)

    while base < total:
        while nxt < total and nxt < base + WINDOW:
            sock.send(_pkt(DATA, nxt, chunks[nxt]))
            sent_at[nxt] = time.time()
            nxt += 1

        wait = sent_at[base] + rto - time.time()
        if wait <= 0:
            wait = 0.0001
        sock.settimeout(wait)
        try:
            raw = sock.recv(homework5.MAX_PACKET)
        except socket.timeout:
            logger.info("Timeout on %d, rto=%.3f", base, rto)
            sock.send(_pkt(DATA, base, chunks[base]))
            sent_at[base] = time.time()
            retx.add(base)
            rto = min(MAX_RTO, rto * 2)
            continue
        except ConnectionError:
            continue

        parsed = _parse(raw)
        if parsed is None or parsed[0] != ACK:
            continue
        ack = parsed[1]
        if ack > base:
            last = min(ack, total) - 1
            if last not in retx:
                update_rtt(time.time() - sent_at[last])
            elif est_rtt is not None:
                # Undo exponential backoff once progress is made again.
                rto = min(MAX_RTO, max(MIN_RTO, est_rtt + 4 * dev_rtt))
            for s in range(base, min(ack, total)):
                sent_at.pop(s, None)
                retx.discard(s)
            base = min(ack, total)
            if ack > nxt:
                nxt = base

    # Close down: FIN / FINACK
    tries = 0
    while tries < FIN_TRIES:
        sock.send(_pkt(FIN, total))
        tries += 1
        deadline = time.time() + max(rto, 0.05)
        done = False
        while True:
            left = deadline - time.time()
            if left <= 0:
                break
            sock.settimeout(left)
            try:
                raw = sock.recv(homework5.MAX_PACKET)
            except socket.timeout:
                break
            except ConnectionError:
                break
            parsed = _parse(raw)
            if parsed and parsed[0] == FINACK:
                done = True
                break
        if done:
            break


def recv(sock: socket.socket, dest: io.BufferedIOBase) -> int:
    """
    Implementation of the receiving logic for receiving data over a slow,
    lossy, constrained network.

    Args:
        sock -- A socket object, constructed and initialized to communicate
                over a simulated lossy network.

    Return:
        The number of bytes written to the destination.
    """
    logger = homework5.logging.get_logger("hw5-receiver")
    num_bytes = 0
    expected = 0
    buffered = {}

    sock.settimeout(None)
    while True:
        try:
            raw = sock.recv(homework5.MAX_PACKET)
        except ConnectionError:
            continue
        parsed = _parse(raw)
        if parsed is None:
            continue
        kind, seq, payload = parsed

        if kind == DATA:
            if seq >= expected and seq < expected + 64:
                buffered[seq] = payload
                while expected in buffered:
                    chunk = buffered.pop(expected)
                    dest.write(chunk)
                    num_bytes += len(chunk)
                    expected += 1
                dest.flush()
            sock.send(_pkt(ACK, expected))
        elif kind == FIN:
            if seq == expected:
                break
            # Not everything arrived yet; keep acking so sender resends.
            sock.send(_pkt(ACK, expected))

    # Got FIN with everything received.  Send FINACK, and linger briefly in
    # case it is lost and the sender retransmits the FIN.
    sock.send(_pkt(FINACK, expected))
    linger = 3.0
    while True:
        sock.settimeout(linger)
        try:
            raw = sock.recv(homework5.MAX_PACKET)
        except (socket.timeout, ConnectionError):
            break
        parsed = _parse(raw)
        if parsed and parsed[0] == FIN:
            sock.send(_pkt(FINACK, expected))
        elif parsed and parsed[0] == DATA:
            sock.send(_pkt(ACK, expected))
    dest.flush()
    return num_bytes
