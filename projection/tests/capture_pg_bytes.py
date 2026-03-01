#!/usr/bin/env python3
"""
Connect to a real Postgres instance via raw TCP, capture wire protocol bytes,
and write them as hex-encoded golden fixtures for Rust integration tests.

Usage (from WSL):
  python3 projection/tests/capture_pg_bytes.py > projection/tests/pg_golden_bytes.txt

Requires: Postgres on 127.0.0.1:5433 (podman container)
"""

import socket
import struct
import sys
import json

PG_HOST = "127.0.0.1"
PG_PORT = 5433
PG_USER = "postgres"
PG_DB = "postgres"
PG_PASS = "test"

def build_startup(user, database):
    """Build a StartupMessage packet."""
    payload = struct.pack("!i", 196608)  # protocol version 3.0
    payload += f"user\x00{user}\x00database\x00{database}\x00\x00".encode()
    length = 4 + len(payload)
    return struct.pack("!i", length) + payload

def build_query(sql):
    """Build a Query ('Q') packet."""
    payload = sql.encode() + b"\x00"
    length = 4 + len(payload)
    return b"Q" + struct.pack("!i", length) + payload

def build_password(password):
    """Build a PasswordMessage ('p') packet."""
    payload = password.encode() + b"\x00"
    length = 4 + len(payload)
    return b"p" + struct.pack("!i", length) + payload

def read_message(sock):
    """Read one wire protocol message. Returns (type_byte, full_message_bytes, payload_bytes)."""
    type_byte = sock.recv(1)
    if not type_byte:
        return None, None, None
    length_bytes = sock.recv(4)
    length = struct.unpack("!i", length_bytes)[0]
    payload_len = length - 4
    payload = b""
    while len(payload) < payload_len:
        chunk = sock.recv(payload_len - len(payload))
        if not chunk:
            break
        payload += chunk
    full = type_byte + length_bytes + payload
    return type_byte[0], full, payload

def capture_session():
    """Capture a full Postgres session and return golden bytes."""
    results = {}

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(10)
    sock.connect((PG_HOST, PG_PORT))

    # 1. Send startup
    startup = build_startup(PG_USER, PG_DB)
    sock.sendall(startup)

    # 2. Read handshake messages
    handshake_types = []
    while True:
        msg_type, full, payload = read_message(sock)
        if msg_type is None:
            break

        handshake_types.append(chr(msg_type))

        if msg_type == ord('R'):  # Authentication
            auth_type = struct.unpack("!i", payload[:4])[0]
            if auth_type == 0:  # AuthenticationOk
                results["auth_ok"] = full.hex()
            elif auth_type == 3:  # CleartextPassword
                results["auth_cleartext_request"] = full.hex()
                sock.sendall(build_password(PG_PASS))
            elif auth_type == 5:  # MD5Password
                results["auth_md5_request"] = full.hex()
                salt = payload[4:8]
                results["auth_md5_salt"] = salt.hex()
                # For simplicity, abort if MD5 — we need trust or password auth
                print("ERROR: MD5 auth not supported, configure pg_hba.conf", file=sys.stderr)
                sock.close()
                sys.exit(1)

        elif msg_type == ord('S'):  # ParameterStatus
            # Parse key\0value\0
            parts = payload.split(b"\x00")
            key = parts[0].decode()
            value = parts[1].decode()
            if "parameter_statuses" not in results:
                results["parameter_statuses"] = []
            results["parameter_statuses"].append({
                "key": key,
                "value": value,
                "hex": full.hex()
            })

        elif msg_type == ord('K'):  # BackendKeyData
            pid, secret = struct.unpack("!ii", payload)
            results["backend_key_data"] = {
                "hex": full.hex(),
                "pid": pid,
                "secret": secret
            }

        elif msg_type == ord('Z'):  # ReadyForQuery
            results["ready_for_query_idle"] = full.hex()
            results["handshake_type_sequence"] = handshake_types
            break

    # 3. Send simple query: SELECT 42 AS answer
    sock.sendall(build_query("SELECT 42 AS answer"))
    query1_messages = []
    while True:
        msg_type, full, payload = read_message(sock)
        if msg_type is None:
            break

        if msg_type == ord('T'):  # RowDescription
            results["row_desc_select_42"] = full.hex()
        elif msg_type == ord('D'):  # DataRow
            results["data_row_select_42"] = full.hex()
        elif msg_type == ord('C'):  # CommandComplete
            results["command_complete_select_1"] = full.hex()
            # Parse tag
            tag = payload.split(b"\x00")[0].decode()
            results["command_complete_tag"] = tag
        elif msg_type == ord('Z'):
            results["ready_for_query_after_select"] = full.hex()
            break

    # 4. Send query with NULL: SELECT NULL::text AS empty
    sock.sendall(build_query("SELECT NULL::text AS empty"))
    while True:
        msg_type, full, payload = read_message(sock)
        if msg_type is None:
            break
        if msg_type == ord('D'):
            results["data_row_null"] = full.hex()
        elif msg_type == ord('Z'):
            break

    # 5. Send multi-row query
    sock.sendall(build_query("SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, NULL)) AS t(id, name)"))
    multi_rows = []
    while True:
        msg_type, full, payload = read_message(sock)
        if msg_type is None:
            break
        if msg_type == ord('T'):
            results["row_desc_multi"] = full.hex()
        elif msg_type == ord('D'):
            multi_rows.append(full.hex())
        elif msg_type == ord('C'):
            results["command_complete_select_3"] = full.hex()
        elif msg_type == ord('Z'):
            break
    results["data_rows_multi"] = multi_rows

    # 6. Send bad query
    sock.sendall(build_query("SELEKT bad_syntax"))
    while True:
        msg_type, full, payload = read_message(sock)
        if msg_type is None:
            break
        if msg_type == ord('E'):
            results["error_response_syntax"] = full.hex()
            # Parse fields
            fields = {}
            pos = 0
            while pos < len(payload) and payload[pos] != 0:
                field_type = chr(payload[pos])
                pos += 1
                end = payload.index(0, pos)
                fields[field_type] = payload[pos:end].decode()
                pos = end + 1
            results["error_response_fields"] = fields
        elif msg_type == ord('Z'):
            break

    # 7. Terminate
    sock.sendall(b"X\x00\x00\x00\x04")
    sock.close()

    return results

if __name__ == "__main__":
    data = capture_session()
    print(json.dumps(data, indent=2))
