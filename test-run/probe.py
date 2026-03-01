#!/usr/bin/env python3
"""Probe the TypeDB pgwire endpoint with raw PG wire protocol messages."""
import socket, struct, sys

HOST = sys.argv[1] if len(sys.argv) > 1 else "172.18.128.1"
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 15432

def recv_until_ready(s):
    """Read until we see ReadyForQuery ('Z')."""
    buf = b""
    while True:
        try:
            chunk = s.recv(4096)
            if not chunk:
                break
            buf += chunk
            # Z message: 'Z' + int32(5) + byte(status)
            if len(buf) >= 6 and buf[-6] == ord('Z'):
                break
        except socket.timeout:
            break
    return buf

def parse_messages(data):
    """Parse PG wire protocol messages from raw bytes."""
    i = 0
    while i < len(data):
        if i + 5 > len(data):
            print(f"  <incomplete at offset {i}>")
            break
        msg_type = chr(data[i])
        msg_len = struct.unpack("!i", data[i+1:i+5])[0]
        msg_body = data[i+5:i+1+msg_len]
        yield msg_type, msg_len, msg_body
        i += 1 + msg_len

def send_query(s, sql):
    """Send a Simple Query message."""
    sql_bytes = sql.encode("utf-8") + b"\x00"
    msg = b"Q" + struct.pack("!i", len(sql_bytes) + 4) + sql_bytes
    s.sendall(msg)

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(10)
s.connect((HOST, PORT))
print(f"Connected to {HOST}:{PORT}")

# 1. Startup
params = b"user\x00typedb\x00database\x00typedb\x00\x00"
version = struct.pack("!i", 196608)  # 3.0
body = version + params
length = struct.pack("!i", len(body) + 4)
s.sendall(length + body)

startup_resp = recv_until_ready(s)
print(f"\n=== STARTUP ({len(startup_resp)} bytes) ===")
for mt, ml, mb in parse_messages(startup_resp):
    if mt == "R":
        status = struct.unpack("!i", mb[:4])[0]
        print(f"  [{mt}] Auth status={status} (0=Ok)")
    elif mt == "S":
        parts = mb.rstrip(b"\x00").split(b"\x00")
        print(f"  [{mt}] {parts[0].decode()}={parts[1].decode()}")
    elif mt == "K":
        pid, key = struct.unpack("!ii", mb[:8])
        print(f"  [{mt}] BackendKeyData pid={pid} key={key}")
    elif mt == "Z":
        print(f"  [{mt}] ReadyForQuery status={chr(mb[0])}")

# 2. Queries
queries = [
    "SELECT version()",
    "SELECT current_database()",
    "SELECT * FROM information_schema.tables",
    "SELECT * FROM information_schema.columns LIMIT 3",
    "SELECT * FROM pg_catalog.pg_type LIMIT 3",
    "SHOW TABLES",
    "SET client_encoding TO 'UTF8'",
    "BEGIN",
    "COMMIT",
]

for sql in queries:
    print(f"\n=== QUERY: {sql} ===")
    send_query(s, sql)
    resp = recv_until_ready(s)
    for mt, ml, mb in parse_messages(resp):
        if mt == "T":
            # RowDescription
            num_fields = struct.unpack("!h", mb[:2])[0]
            print(f"  [{mt}] RowDescription: {num_fields} field(s)")
        elif mt == "D":
            # DataRow
            num_cols = struct.unpack("!h", mb[:2])[0]
            vals = []
            offset = 2
            for _ in range(num_cols):
                col_len = struct.unpack("!i", mb[offset:offset+4])[0]
                offset += 4
                if col_len == -1:
                    vals.append("NULL")
                else:
                    vals.append(mb[offset:offset+col_len].decode("utf-8", errors="replace"))
                    offset += col_len
            print(f"  [{mt}] DataRow: {vals}")
        elif mt == "C":
            tag = mb.rstrip(b"\x00").decode("utf-8")
            print(f"  [{mt}] CommandComplete: {tag}")
        elif mt == "Z":
            print(f"  [{mt}] ReadyForQuery: {chr(mb[0])}")
        elif mt == "E":
            # Error
            fields = mb.split(b"\x00")
            msg_parts = [f.decode("utf-8", errors="replace") for f in fields if f]
            print(f"  [{mt}] Error: {msg_parts}")

# 3. Terminate
s.sendall(b"X\x00\x00\x00\x04")
s.close()
print("\n=== DONE ===")
