import socket  # noqa: F401
import threading

def parse_resp(data):
    if not data:
        return None
    if data[0:1] != b"*":
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
        
    return parts


def encode_bulk(s):
    return b"$" + str(len(s)).encode() + b"\r\n" + s + b"\r\n"


def handle_client(connection):
    with connection:
        buffer = b""
        while True:
            data = connection.recv(1024)
            if not data:
                break
            buffer += data
            while True:
                parts = parse_resp(buffer)
                if parts is None:
                    break
                consumed = 0
                if parts:
                    pass
                p = buffer.find(b"\r\n")
                count = int(buffer[1:p])
                pos = p + 2
                for _ in range(count):
                    end = buffer.find(b"\r\n", pos)
                    length = int(buffer[pos + 1:end])
                    pos = end + 2 + length + 2
                consumed = pos
                buffer = buffer[consumed:]
                cmd = parts[0].upper()
                if cmd == b"PING":
                    connection.sendall(b"+PONG\r\n")
                elif cmd == b"ECHO" and len(parts) >= 2:
                    connection.sendall(encode_bulk(parts[1]))
    

def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        connection, _ = server_socket.accept()
        thread = threading.Thread(target=handle_client, args=(connection,))
        thread.start()


if __name__ == "__main__":
    main()
