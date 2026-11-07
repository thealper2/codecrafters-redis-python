import socket  # noqa: F401
import threading
import time

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


def handle_client(connection, store, lock):
    with connection:
        buffer = b""
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
                elif cmd == b"RPUSH" and len(parts) >= 3:
                    key = parts[1]
                    with lock:
                        entry = store.get(key)
                        if entry is not None and entry[0] == b"list":
                            lst = entry[1]
                            expiry = entry[2]
                        else:
                            lst = []
                            expiry = None
                        lst.extend(parts[2:])
                        store[key] = (b"list", lst, expiry)
                        count = len(lst)
                    connection.sendall(b":" + str(count).encode() + b"\r\n")
                elif cmd == b"LRANGE" and len(parts) >= 4:
                    key = parts[1]
                    start = int(parts[2])
                    stop = int(parts[3])
                    with lock:
                        entry = store.get(key)
                        if entry is not None and entry[0] == b"list":
                            items = entry[1][start:stop + 1]
                        else:
                            items = []
                    connection.sendall(encode_array(items))
    

def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    store = {}
    lock = threading.Lock()
    while True:
        connection, _ = server_socket.accept()
        thread = threading.Thread(target=handle_client, args=(connection, store, lock))
        thread.start()


if __name__ == "__main__":
    main()
