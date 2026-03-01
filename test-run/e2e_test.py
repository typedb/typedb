#!/usr/bin/env python3
"""Full E2E test suite for TypeDB pgwire endpoint using raw PG wire protocol."""
import socket, struct, sys

HOST = sys.argv[1] if len(sys.argv) > 1 else "172.18.128.1"
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 15432

def recv_until_ready(s):
    buf = b""
    while True:
        try:
            chunk = s.recv(8192)
            if not chunk:
                break
            buf += chunk
            # Scan for Z (ReadyForQuery) message
            i = 0
            while i < len(buf):
                if i + 5 > len(buf):
                    break
                if buf[i] == ord('Z'):
                    msg_len = struct.unpack("!i", buf[i+1:i+5])[0]
                    if i + 1 + msg_len <= len(buf):
                        return buf
                i += 1 + struct.unpack("!i", buf[i+1:i+5])[0] if i + 5 <= len(buf) else len(buf)
        except socket.timeout:
            break
    return buf

def parse_messages(data):
    i = 0
    while i < len(data):
        if i + 5 > len(data):
            break
        msg_type = chr(data[i])
        msg_len = struct.unpack("!i", data[i+1:i+5])[0]
        msg_body = data[i+5:i+1+msg_len]
        yield msg_type, msg_body
        i += 1 + msg_len

def decode_row_desc(body):
    num = struct.unpack("!h", body[:2])[0]
    cols = []
    off = 2
    for _ in range(num):
        end = body.index(b"\x00", off)
        name = body[off:end].decode()
        cols.append(name)
        off = end + 1 + 18  # skip table_oid(4) + col_attr(2) + type_oid(4) + type_size(2) + type_mod(4) + format(2)
    return cols

def decode_data_row(body):
    num = struct.unpack("!h", body[:2])[0]
    vals = []
    off = 2
    for _ in range(num):
        clen = struct.unpack("!i", body[off:off+4])[0]
        off += 4
        if clen == -1:
            vals.append("NULL")
        else:
            vals.append(body[off:off+clen].decode("utf-8", errors="replace"))
            off += clen
    return vals

def query(s, sql):
    sql_bytes = sql.encode("utf-8") + b"\x00"
    msg = b"Q" + struct.pack("!i", len(sql_bytes) + 4) + sql_bytes
    s.sendall(msg)
    resp = recv_until_ready(s)
    cols = None
    rows = []
    tag = None
    error = None
    for mt, mb in parse_messages(resp):
        if mt == "T":
            cols = decode_row_desc(mb)
        elif mt == "D":
            rows.append(decode_data_row(mb))
        elif mt == "C":
            tag = mb.rstrip(b"\x00").decode()
        elif mt == "E":
            parts = mb.split(b"\x00")
            error = " | ".join(p.decode("utf-8", errors="replace") for p in parts if p)
    return cols, rows, tag, error

# Connect
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(10)
s.connect((HOST, PORT))

# Startup
params = b"user\x00typedb\x00database\x00typedb\x00\x00"
version = struct.pack("!i", 196608)
body = version + params
length = struct.pack("!i", len(body) + 4)
s.sendall(length + body)
recv_until_ready(s)

passed = 0
failed = 0

tests = [
    ("SELECT version()", None),
    ("SELECT current_database()", None),
    ("SELECT current_schema()", None),
    ("SELECT current_user", None),
    ("SELECT 1 AS test", None),
    ("SELECT 42 AS answer, 'hello' AS greeting", None),
    ("SELECT * FROM pg_catalog.pg_type LIMIT 5", None),
    ("SELECT * FROM pg_catalog.pg_namespace", None),
    ("SELECT * FROM pg_catalog.pg_database", None),
    ("SELECT * FROM pg_catalog.pg_class LIMIT 3", None),
    ("SELECT * FROM pg_catalog.pg_attribute LIMIT 3", None),
    ("SELECT * FROM pg_catalog.pg_am", None),
    ("SELECT * FROM pg_catalog.pg_settings LIMIT 3", None),
    ("SELECT * FROM information_schema.schemata", None),
    ("SELECT * FROM information_schema.tables", None),
    ("SELECT * FROM information_schema.columns LIMIT 3", None),
    ("SHOW TABLES", None),
    ("SET client_encoding TO 'UTF8'", "SET"),
    ("BEGIN", "BEGIN"),
    ("COMMIT", "COMMIT"),
    ("ROLLBACK", "ROLLBACK"),
    ("DISCARD ALL", "DISCARD ALL"),
]

for sql, expected_tag in tests:
    cols, rows, tag, error = query(s, sql)
    if error:
        print(f"FAIL  {sql}")
        print(f"      Error: {error}")
        failed += 1
    else:
        status = "OK"
        if expected_tag and tag != expected_tag:
            status = f"FAIL (expected tag={expected_tag}, got {tag})"
            failed += 1
        else:
            passed += 1
        row_info = f"{len(rows)} row(s)"
        if rows and len(rows) <= 2:
            row_info += f": {rows}"
        elif rows:
            row_info += f": {rows[0]} ..."
        col_info = f"{len(cols)} col(s)" if cols else "no cols"
        print(f"{status:5s} {sql}")
        print(f"      tag={tag} {col_info} {row_info}")

# Terminate
s.sendall(b"X\x00\x00\x00\x04")
s.close()

print(f"\n{'='*60}")
print(f"Results: {passed} passed, {failed} failed out of {passed+failed}")
print(f"{'='*60}")
sys.exit(1 if failed > 0 else 0)
