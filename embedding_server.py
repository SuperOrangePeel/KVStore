#!/usr/bin/env python3
import argparse
import hashlib
import os
import queue
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
_batch_queue = None
_batch_size = 8
_batch_wait_ms = 5
_device = "cpu"


def _log(msg: str):
    print(f'[embedding_server] {msg}', file=sys.stderr, flush=True)


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


def _resolve_device(requested: str):
    requested = requested or 'auto'
    if requested == 'auto':
        try:
            import torch
            return 'cuda' if torch.cuda.is_available() else 'cpu'
        except Exception:
            return 'cpu'
    if requested == 'cuda':
        import torch
        if not torch.cuda.is_available():
            raise RuntimeError('CUDA requested but torch.cuda.is_available() is false')
    return requested


def _load_model(model_name: str, requested_device: str):
    global _model, _device
    _check_dependencies()
    _device = _resolve_device(requested_device)
    _log(f'using device={_device}')
    _model = _sentence_transformer_cls(model_name, trust_remote_code=True, device=_device)


def _warmup_model():
    if _mock or _model is None:
        return
    _log('warming up model')
    embed_one(b'warmup')
    _log('model warmup done')


def embed_many(texts):
    if _mock:
        return [_mock_embed(text, _dim) for text in texts]
    if _model is None:
        raise RuntimeError('model not loaded')
    strings = [text.decode('utf-8', errors='replace') for text in texts]
    try:
        vecs = _model.encode(strings, normalize_embeddings=True, truncate_dim=_dim)
    except TypeError:
        vecs = _model.encode(strings, normalize_embeddings=True)
        vecs = [vec[:_dim] for vec in vecs]
    return [[float(x) for x in vec] for vec in vecs]


def embed_one(text: bytes):
    return embed_many([text])[0]


class EmbedJob:
    __slots__ = ('conn', 'req_id', 'text')

    def __init__(self, conn, req_id, text):
        self.conn = conn
        self.req_id = req_id
        self.text = text


def send_error(conn, req_id, err):
    _log(f'error req_id={req_id}: {err!r}')
    try:
        resp = struct.pack('<IIIII', RESP_MAGIC, req_id, 1, 0, 0)
        conn.sendall(resp)
    except Exception:
        pass


def send_vector(conn, req_id, vec):
    payload = struct.pack('<%df' % len(vec), *vec)
    resp = struct.pack('<IIIII', RESP_MAGIC, req_id, 0, len(vec), len(payload))
    conn.sendall(resp + payload)


def batch_loop():
    global _batch_queue
    while True:
        first = _batch_queue.get()
        jobs = [first]
        deadline = _batch_wait_ms / 1000.0
        while len(jobs) < _batch_size:
            try:
                jobs.append(_batch_queue.get(timeout=deadline))
            except queue.Empty:
                break
        try:
            vecs = embed_many([job.text for job in jobs])
            for job, vec in zip(jobs, vecs):
                try:
                    send_vector(job.conn, job.req_id, vec)
                except Exception:
                    pass
        except Exception as e:
            for job in jobs:
                send_error(job.conn, job.req_id, e)


def handle(conn: socket.socket):
    global _batch_queue
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
            _batch_queue.put(EmbedJob(conn, req_id, data))


def main():
    global _dim, _mock, _batch_queue, _batch_size, _batch_wait_ms
    ap = argparse.ArgumentParser()
    ap.add_argument('--socket', required=True)
    ap.add_argument('--model', default='Qwen/Qwen3-Embedding-0.6B')
    ap.add_argument('--dim', type=int, default=512)
    ap.add_argument('--mock', action='store_true')
    ap.add_argument('--batch-size', type=int, default=8)
    ap.add_argument('--batch-wait-ms', type=int, default=5)
    ap.add_argument('--device', default=os.environ.get('KVS_EMBEDDING_DEVICE', 'auto'))
    args = ap.parse_args()
    _dim = args.dim
    _mock = args.mock or os.environ.get('KVS_EMBEDDING_MOCK') == '1'
    _batch_size = args.batch_size if args.batch_size > 0 else 8
    _batch_wait_ms = args.batch_wait_ms if args.batch_wait_ms >= 0 else 5
    _batch_queue = queue.Queue()
    if not _mock:
        try:
            _check_dependencies()
            _resolve_device(args.device)
        except Exception as e:
            _log(f'preflight failed: {e!r}')
            sys.exit(1)
    try:
        os.unlink(args.socket)
    except FileNotFoundError:
        pass
    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    srv.bind(args.socket)
    srv.listen(128)
    _log(f'listening socket={args.socket} mock={_mock} dim={_dim} batch_size={_batch_size} batch_wait_ms={_batch_wait_ms}')
    # Make the Unix socket visible before loading the model. First model load can be slow;
    # connected workers will block until the accept loop starts.
    if not _mock:
        _log(f'loading model={args.model}')
        try:
            _load_model(args.model, args.device)
        except Exception as e:
            _log(f'failed to load model: {e!r}')
            sys.exit(1)
        _log('model loaded')
        _warmup_model()
    threading.Thread(target=batch_loop, daemon=True).start()
    while True:
        conn, _ = srv.accept()
        threading.Thread(target=handle, args=(conn,), daemon=True).start()


if __name__ == '__main__':
    main()
