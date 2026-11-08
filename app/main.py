import socket  # noqa: F401
import threading
import time
from collections import deque
import sys

def parse_resp(data):
    if not data or data[0:1] != b"*":
        return None
    pos = data.find(b"\r\n")
    if pos == -1:
        return None
    count = int(data[1:pos])
    pos += 2
    parts = []
    for _ in range(count):
        if pos >= len(data) or data[pos:pos + 1] != b"$":
            return None
        end = data.find(b"\r\n", pos)
        if end == -1:
            return None
        length = int(data[pos + 1:end])
        pos = end + 2
        if pos + length > len(data):
            return None
        parts.append(data[pos:pos + length])
        pos += length + 2
        
    return parts, pos


def encode_bulk(s):
    return b"$" + str(len(s)).encode() + b"\r\n" + s + b"\r\n"


def encode_array(items):
    return b"*" + str(len(items)).encode() + b"\r\n" + b"".join(encode_bulk(x) for x in items)


waiters = {}
config = {"role": "master", "replid": "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb", "offset": 0}
EMPTY_RDB = bytes.fromhex(
    "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040"
    "fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000"
    "fff06e3bfec0ff5aa2"
)
versions = {}
replicas = []
acks = {}
ack_cv = threading.Condition()
WRITE_CMDS = (b"SET", b"INCR", b"RPUSH", b"LPUSH", b"LPOP", b"BLPOP", b"XADD")
stream_cv = threading.Condition()

def read_streams(store, lock, keys, ids):
    out = []
    with lock:
        for k, i in zip(keys, ids):
            after = parse_range_id(i, 0)
            entry = store.get(k)
            entries = entry[1] if entry is not None and entry[0] == b"stream" else []
            res = [
                (eid, f) for eid, f in entries
                if tuple(int(x) for x in eid.split(b"-")) > after
            ]
            if res:
                out.append((k, res))  
    return out

def parse_range_id(s, default_seq):
    if s == b"-":
        return (0, 0)
    if s == b"+":
        return (float("inf"), float("inf"))
    if b"-" in s:
        a, b = s.split(b"-")
        return int(a), int(b)
    return int(s), default_seq


def encode_entries(entries):
    out = b"*" + str(len(entries)).encode() + b"\r\n"
    for eid, fields in entries:
        out += b"*2\r\n" + encode_bulk(eid) + encode_array(fields)
    return out


class Capture:
    def __init__(self):
        self.data = b""
    
    def sendall(self, b):
        self.data += b

def execute(parts, connection, store, lock):
    cmd = parts[0].upper()
    if cmd in WRITE_CMDS and len(parts) >= 2:
        with lock:
            versions[parts[1]] = versions.get(parts[1], 0) + 1
            data = encode_array(parts)
            for r in replicas:
                r.sendall(data)
            config["offset"] += len(data)
    if cmd == b"PING":
        connection.sendall(b"+PONG\r\n")
    elif cmd == b"ECHO" and len(parts) >= 2:
        connection.sendall(encode_bulk(parts[1]))
    elif cmd == b"SET" and len(parts) >= 3:
        key = parts[1]
        value = parts[2]
        expiry = None
        i = 3
        while i < len(parts):
            opt = parts[i].upper()
            if opt in (b"PX", b"EX") and i + 1 < len(parts):
                amount = int(parts[i + 1])
                if opt == b"PX":
                    expiry = time.time() + amount / 1000.0
                else:
                    expiry = time.time() + amount
                i += 2
            else:
                i += 1
        with lock:
            store[key] = (value, expiry)
        connection.sendall(b"+OK\r\n")
    elif cmd == b"GET" and len(parts) >= 2:
        key = parts[1]
        with lock:
            entry = store.get(key)
            if entry is not None:
                value, expiry = entry
                if expiry is not None and time.time() >= expiry:
                    del store[key]
                    entry = None
        if entry is not None:
            connection.sendall(encode_bulk(entry[0]))
        else:
            connection.sendall(b"$-1\r\n")
    elif cmd in (b"RPUSH", b"LPUSH") and len(parts) >= 3:
        key = parts[1]
        with lock:
            entry = store.get(key)
            if entry is not None and entry[0] == b"list":
                lst = entry[1]
                expiry = entry[2]
            else:
                lst = []
                expiry = None
            if cmd == b"RPUSH":
                lst.extend(parts[2:])
            else:
                lst[0:0] = parts[2:][::-1]
            store[key] = (b"list", lst, expiry)
            count = len(lst)
            q = waiters.get(key)
            while q and lst:
                ev, slot = q.popleft()
                slot.append(lst.pop(0))
                ev.set()
        connection.sendall(b":" + str(count).encode() + b"\r\n")
    elif cmd == b"LRANGE" and len(parts) >= 4:
        key = parts[1]
        start = int(parts[2])
        stop = int(parts[3])
        with lock:
            entry = store.get(key)
            if entry is not None and entry[0] == b"list":
                lst = entry[1]
                n = len(lst)
                s = start + n if start < 0 else start
                e = stop + n if stop < 0 else stop
                s = max(s, 0)
                e = max(e, -1)
                items = lst[s:e + 1]
            else:
                items = []
        connection.sendall(encode_array(items))
    elif cmd == b"LLEN" and len(parts) >= 2:
        with lock:
            entry = store.get(parts[1])
            n = len(entry[1]) if entry is not None and entry[0] == b"list" else 0
        connection.sendall(b":" + str(n).encode() + b"\r\n")
    elif cmd == b"LPOP" and len(parts) >= 2:
        count = int(parts[2]) if len(parts) >= 3 else None
        with lock:
            entry = store.get(parts[1])
            lst = entry[1] if entry is not None and entry[0] == b"list" else None
            if lst and count is None:
                item = lst.pop(0)
            elif lst and count is not None:
                item = lst[:count]
                del lst[:count]
            else:
                item = None
        if item is None:
            connection.sendall(b"$-1\r\n")
        elif count is None:
            connection.sendall(encode_bulk(item))
        else:
            connection.sendall(encode_array(item))
    elif cmd == b"BLPOP" and len(parts) >= 3:
        key = parts[1]
        timeout = float(parts[-1])
        ev = threading.Event()
        slot = []
        with lock:
            entry = store.get(key)
            if entry is not None and entry[0] == b"list" and entry[1]:
                slot.append(entry[1].pop(0))
            else:
                waiters.setdefault(key, deque()).append((ev, slot))
        if not slot:
            ev.wait(timeout if timeout > 0 else None)
        with lock:
            if not slot:
                q = waiters.get(key)
                if q and (ev, slot) in q:
                    q.remove((ev, slot))
        if slot:
            connection.sendall(encode_array([key, slot[0]]))
        else:
            connection.sendall(b"*-1\r\n")
    elif cmd == b"TYPE" and len(parts) >= 2:
        key = parts[1]
        with lock:
            entry = store.get(key)
            if entry is not None and len(entry) == 2:
                if entry[1] is not None and time.time() >= entry[1]:
                    del store[key]
                    t = b"none"
                else:
                    t = b"string"
            elif entry is not None:
                t = entry[0]
            else:
                t = b"none"
        connection.sendall(b"+" + t + b"\r\n")
    elif cmd == b"XADD" and len(parts) >= 5:
        key = parts[1]
        entry_id = parts[2]
        fields = parts[3:]
        err = None
        with lock:
            entry = store.get(key)
            if entry is not None and entry[0] == b"stream":
                entries = entry[1]
            else:
                entries = []
            if entry_id == b"*":
                entry_id = str(int(time.time() * 1000)).encode() + b"-*"
            ms_part, seq_part = entry_id.split(b"-")
            ms = int(ms_part)
            if seq_part == b"*":
                last = entries[-1][0].split(b"-") if entries else None
                if last and int(last[0]) == ms:
                    seq = int(last[1]) + 1
                else:
                    seq = 1 if ms == 0 else 0
                entry_id = str(ms).encode() + b"-" + str(seq).encode()
            else:
                seq = int(seq_part)
            if (ms, seq) <= (0, 0):
                err = b"-ERR The ID specified in XADD must be greater than 0-0\r\n"
            elif entries:
                lms, lseq = (int(x) for x in entries[-1][0].split(b"-"))
                if (ms, seq) <= (lms, lseq):
                    err = b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"
            if err is None:
                entries.append((entry_id, fields))
                store[key] = (b"stream", entries, None)
        if err:
            connection.sendall(err)
        else:
            with stream_cv:
                stream_cv.notify_all()
            connection.sendall(encode_bulk(entry_id))
    elif cmd == b"XRANGE" and len(parts) >= 4:
        start = parse_range_id(parts[2], 0)
        end = parse_range_id(parts[3], float("inf"))
        with lock:
            entry = store.get(parts[1])
            entries = entry[1] if entry is not None and entry[0] == b"stream" else []
            result = [
                (eid, f) for eid, f in entries
                if start <= tuple(int(x) for x in eid.split(b"-")) <= end
            ]
        connection.sendall(encode_entries(result))
    elif cmd == b"XREAD":
        upper = [p.upper() for p in parts]
        block = int(parts[upper.index(b"BLOCK") + 1]) if b"BLOCK" in upper else None
        rest = parts[upper.index(b"STREAMS") + 1:]
        half = len(rest) // 2
        keys, ids = rest[:half], rest[half:]
        with lock:
            resolved = []
            for k, i in zip(keys, ids):
                if i == b"$":
                    entry = store.get(k)
                    if entry is not None and entry[0] == b"stream" and entry[1]:
                        i = entry[1][-1][0]
                    else:
                        i = b"0-0"
                resolved.append(i)
            ids = resolved
        if block is None:
            out = read_streams(store, lock, keys, ids)
        else:
            deadline = time.time() + block / 1000.0
            with stream_cv:
                out = read_streams(store, lock, keys, ids)
                while not out:
                    if block == 0:
                        stream_cv.wait()
                    else:
                        remaining = deadline - time.time()
                        if remaining <= 0:
                            break
                        stream_cv.wait(remaining)
                    out = read_streams(store, lock, keys, ids)
        if not out and block is not None:
            connection.sendall(b"*-1\r\n")
        else:
            body = b"*" + str(len(out)).encode() + b"\r\n"
            for k, res in out:
                body += b"*2\r\n" + encode_bulk(k) + encode_entries(res)
            connection.sendall(body)
    elif cmd == b"INCR" and len(parts) >= 2:
        key = parts[1]
        err = False
        with lock:
            entry = store.get(key)
            if entry is not None and len(entry) == 2 and entry[1] is not None and time.time() >= entry[1]:
                entry = None
            if entry is None:
                n = 1
                expiry = None
            else:
                try:
                    n = int(entry[0]) + 1
                    expiry = entry[1]
                except ValueError:
                    err = True
            if not err:
                store[key] = (str(n).encode(), expiry)
        if err:
            connection.sendall(b"-ERR value is not an integer or out of range\r\n")
        else:
            connection.sendall(b":" + str(n).encode() + b"\r\n")
    elif cmd == b"INFO":
        info = "\r\n".join([
            "role:" + config["role"],
            "master_replid:" + config["replid"],
            "master_repl_offset:" + str(config["offset"]),
        ])
        connection.sendall(encode_bulk(info.encode()))
    elif cmd == b"REPLCONF":
        if len(parts) >= 3 and parts[1].upper() == b"ACK":
            with ack_cv:
                acks[connection] = int(parts[2])
                ack_cv.notify_all()
        else:
            connection.sendall(b"+OK\r\n")
    elif cmd == b"PSYNC":
        connection.sendall(b"+FULLRESYNC " + config["replid"].encode() + b" 0\r\n")
        connection.sendall(b"$" + str(len(EMPTY_RDB)).encode() + b"\r\n" + EMPTY_RDB)
        replicas.append(connection)
    elif cmd == b"WAIT":
        need = int(parts[1])
        timeout = int(parts[2])
        target = config["offset"]
        if target == 0:
            n = len(replicas)
        else:
            getack = encode_array([b"REPLCONF", b"GETACK", b"*"])
            with lock:
                for r in replicas:
                    r.sendall(getack)
                config["offset"] += len(getack)
            deadline = time.time() + timeout / 1000.0
            with ack_cv:
                while True:
                    n = sum(1 for v in acks.values() if v >= target)
                    remaining = deadline - time.time()
                    if n >= need or remaining <= 0:
                        break
                    ack_cv.wait(remaining)
        connection.sendall(b":" + str(len(replicas)).encode() + b"\r\n")


def handle_master(conn, store, lock):
    buffer = b""
    while b"\r\n" not in buffer:
        buffer += conn.recv(1024)
    buffer = buffer[buffer.find(b"\r\n") + 2:]
    offset = 0
    while True:
        if buffer.startswith(b"$") and b"\r\n" in buffer:
            end = buffer.find(b"\r\n")
            n = int(buffer[1:end])
            if len(buffer) >= end + 2 + n:
                buffer = buffer[end + 2 + n:]
                break
        buffer += conn.recv(1024)
    while True:
        while True:
            result = parse_resp(buffer)
            if result is None:
                break
            parts, consumed = result
            buffer = buffer[consumed:]
            if parts[0].upper() == b"REPLCONF" and len(parts) >= 2 and parts[1].upper() == b"GETACK":
                conn.sendall(encode_array([b"REPLCONF", b"ACK", str(offset).encode()]))
            else:
                execute(parts, Capture(), store, lock)
            offset += consumed
        data = conn.recv(1024)
        if not data:
            break
        buffer += data
    

def handle_client(connection, store, lock):
    with connection:
        buffer = b""
        in_multi = False
        queue = []
        watched = {}
        while True:
            data = connection.recv(1024)
            if not data:
                break
            buffer += data
            while True:
                result = parse_resp(buffer)
                if result is None:
                    break
                parts, consumed = result
                buffer = buffer[consumed:]
                cmd = parts[0].upper()
                if in_multi and cmd not in (b"EXEC", b"MULTI", b"DISCARD", b"WATCH"):
                    queue.append(parts)
                    connection.sendall(b"+QUEUED\r\n")
                elif cmd == b"MULTI":
                    in_multi = True
                    queue = []
                    connection.sendall(b"+OK\r\n")
                elif cmd == b"EXEC":
                    if not in_multi:
                        connection.sendall(b"-ERR EXEC without MULTI\r\n")
                    else:
                        in_multi = False
                        with lock:
                            dirty = any(versions.get(k, 0) != v for k, v in watched.items())
                        watched = {}
                        if dirty:
                            queue = []
                            connection.sendall(b"*-1\r\n")
                        else:
                            out = b"*" + str(len(queue)).encode() + b"\r\n"
                            for q in queue:
                                cap = Capture()
                                execute(q, cap, store, lock)
                                out += cap.data
                            queue = []
                            connection.sendall(out)
                elif cmd == b"DISCARD":
                    if not in_multi:
                        connection.sendall(b"-ERR DISCARD without MULTI\r\n")
                    else:
                        in_multi = False
                        queue = []
                        watched = {}
                        connection.sendall(b"+OK\r\n")
                elif cmd == b"WATCH":
                    if in_multi:
                        connection.sendall(b"-ERR WATCH inside MULTI is not allowed\r\n")
                    else:
                        with lock:
                            for k in parts[1:]:
                                watched[k] = versions.get(k, 0)
                        connection.sendall(b"+OK\r\n")
                elif cmd == b"UNWATCH":
                    watched = {}
                    connection.sendall(b"+OK\r\n")
                else:
                    execute(parts, connection, store, lock)
                    
def main():
    port = 6379
    if "--port" in sys.argv:
        port = int(sys.argv[sys.argv.index("--port") + 1])
    if "--replicaof" in sys.argv:
        host, mport = sys.argv[sys.argv.index("--replicaof") + 1].split()
        config["role"] = "slave"
        config["master"] = (host, int(mport))
    server_socket = socket.create_server(("localhost", port), reuse_port=True)
    store = {}
    lock = threading.Lock()

    if config["role"] == "slave":
        master_conn  = socket.create_connection(config["master"])
        master_conn.sendall(encode_array([b"PING"]))
        master_conn.recv(1024)
        master_conn.sendall(encode_array([b"REPLCONF", b"listening-port", str(port).encode()]))
        master_conn.recv(1024)
        master_conn.sendall(encode_array([b"REPLCONF", b"capa", b"psync2"]))
        master_conn.recv(1024)
        master_conn.sendall(encode_array([b"PSYNC", b"?", b"-1"]))
        config["master_conn"] = master_conn
        threading.Thread(target=handle_master, args=(config["master_conn"], store, lock), daemon=True).start()
    
    while True:
        connection, _ = server_socket.accept()
        thread = threading.Thread(target=handle_client, args=(connection, store, lock))
        thread.start()


if __name__ == "__main__":
    main()
