#!/usr/bin/env python3
import argparse
import hashlib
import os
import socket
import struct
import sys
import threading

REQ_MAGIC = 0x45515231
RESP_MAGIC = 0x45535031

_model = None
_dim = 512
_mock = False
_sentence_transformer_cls = None


def _normalize(vec):
    s = sum(float(x) * float(x) for x in vec) ** 0.5
    if s <= 0:
        return vec
    return [float(x) / s for x in vec]


def _mock_embed(text: bytes, dim: int):
    out = []
    seed = text
    while len(out) < dim:
        h = hashlib.sha256(seed).digest()
        seed = h
        for i in range(0, len(h), 4):
            if len(out) >= dim:
                break
            v = struct.unpack('<I', h[i:i+4])[0]
            out.append((v / 2147483648.0) - 1.0)
    return _normalize(out)


def _configure_torch_runtime():
    try:
        import torch
        if hasattr(torch.backends, 'mkldnn'):
            torch.backends.mkldnn.enabled = False
    except Exception as e:
        print(f'[embedding_server] warning: failed to configure torch runtime: {e}', file=sys.stderr)


def _check_dependencies():
    global _sentence_transformer_cls
    if _sentence_transformer_cls is not None:
        return
    _configure_torch_runtime()
    try:
        from sentence_transformers import SentenceTransformer
    except ModuleNotFoundError as e:
        if e.name == 'sentence_transformers':
            print('[embedding_server] missing dependency: sentence_transformers', file=sys.stderr)
            print('[embedding_server] install with: python3 -m pip install sentence-transformers', file=sys.stderr)
            print('[embedding_server] for local tests: KVS_EMBEDDING_MOCK=1 ./kvstore kvs.toml', file=sys.stderr)
        raise
    _sentence_transformer_cls = SentenceTransformer


def _load_model(model_name: str):
    global _model
    _check_dependencies()
    _model = _sentence_transformer_cls(model_name, trust_remote_code=True)


def embed(text: bytes):
    if _mock:
        return _mock_embed(text, _dim)
    if _model is None:
        raise RuntimeError('model not loaded')
    s = text.decode('utf-8', errors='replace')
    try:
        vec = _model.encode([s], normalize_embeddings=True, truncate_dim=_dim)[0]
    except TypeError:
        vec = _model.encode([s], normalize_embeddings=True)[0]
        vec = vec[:_dim]
    return [float(x) for x in vec]


def handle(conn: socket.socket):
    with conn:
        while True:
            hdr = conn.recv(12)
            if not hdr:
                return
            while len(hdr) < 12:
                more = conn.recv(12 - len(hdr))
                if not more:
                    return
                hdr += more
            magic, req_id, text_len = struct.unpack('<III', hdr)
            if magic != REQ_MAGIC or text_len > 16 * 1024 * 1024:
                return
            data = b''
            while len(data) < text_len:
                chunk = conn.recv(text_len - len(data))
                if not chunk:
                    return
                data += chunk
            try:
                vec = embed(data)
                payload = struct.pack('<%df' % len(vec), *vec)
                resp = struct.pack('<IIIII', RESP_MAGIC, req_id, 0, len(vec), len(payload))
                conn.sendall(resp + payload)
            except Exception as e:
                msg = str(e).encode()[:256]
                resp = struct.pack('<IIIII', RESP_MAGIC, req_id, 1, 0, 0)
                conn.sendall(resp)


def main():
    global _dim, _mock
    ap = argparse.ArgumentParser()
    ap.add_argument('--socket', required=True)
    ap.add_argument('--model', default='Qwen/Qwen3-Embedding-0.6B')
    ap.add_argument('--dim', type=int, default=512)
    ap.add_argument('--mock', action='store_true')
    args = ap.parse_args()
    _dim = args.dim
    _mock = args.mock or os.environ.get('KVS_EMBEDDING_MOCK') == '1'
    if not _mock:
        try:
            _check_dependencies()
        except ModuleNotFoundError:
            sys.exit(1)
    try:
        os.unlink(args.socket)
    except FileNotFoundError:
        pass
    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    srv.bind(args.socket)
    srv.listen(128)
    # Make the Unix socket visible before loading the model. First model load can be slow;
    # connected workers will block until the accept loop starts.
    if not _mock:
        _load_model(args.model)
    while True:
        conn, _ = srv.accept()
        threading.Thread(target=handle, args=(conn,), daemon=True).start()


if __name__ == '__main__':
    main()
