import base64
import threading
import time
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import Request, urlopen


class Client:
    def __init__(self, timeout=5):
        self.timeout = timeout

    def request(self, url, method='GET', headers=None, body=None):
        request = Request(url, data=body, headers=headers or {}, method=method)
        try:
            response = urlopen(request, timeout=self.timeout)
        except HTTPError as error:
            response = error
        with response:
            return response.status, dict(response.headers), response.read()


@contextmanager
def replay_server(routes):
    positions = {}
    calls = []
    lock = threading.Lock()
    gates = {}

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format_string, *arguments):
            return None

        def do_GET(self):
            self.respond()

        def do_POST(self):
            self.respond()

        def respond(self):
            length = int(self.headers.get('Content-Length', '0'))
            body = self.rfile.read(length)
            with lock:
                calls.append(dict(method=self.command, path=self.path,
                                  headers=dict(self.headers),
                                  body=body.decode('utf-8', errors='replace')))
                replies = routes.get(self.path, [dict(status=404, body='未找到资源')])
                index = positions.get(self.path, 0)
                reply = replies[min(index, len(replies) - 1)]
                positions[self.path] = index + 1
                gate_name = reply.get('gate')
                if gate_name:
                    event = gates.setdefault((self.path, index, gate_name),
                                             threading.Event())
            # 门闩等待放在锁外，避免阻塞其他路径的请求
            if gate_name:
                event.wait(timeout=10)
            data = base64.b64decode(reply['base64']) if 'base64' in reply \
                else reply.get('body', '').encode('utf-8')
            if reply.get('delay'):
                time.sleep(reply['delay'])
            self.send_response(reply.get('status', 200))
            for name, value in reply.get('headers', {}).items():
                self.send_header(name, value)
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    def release_gate(name):
        for (path, index, gate_name), event in list(gates.items()):
            if gate_name == name:
                event.set()

    try:
        yield f'http://127.0.0.1:{server.server_port}', calls, release_gate
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
