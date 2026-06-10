#!/usr/bin/env python3
"""Local dev server: serves client/ statically, proxies /api/* to production.

Lets the static client run against real data without Render env vars.
Usage: python3 dev-proxy.py [port]
"""
import http.server
import sys
import urllib.request

UPSTREAM = 'https://saigon-soundscape.onrender.com'
PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8901


class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory='client', **kwargs)

    def do_GET(self):
        if self.path.startswith('/api/'):
            try:
                req = urllib.request.Request(UPSTREAM + self.path,
                                             headers={'User-Agent': 'dev-proxy'})
                with urllib.request.urlopen(req, timeout=60) as r:
                    body = r.read()
                    self.send_response(r.status)
                    self.send_header('Content-Type', r.headers.get('Content-Type', 'application/json'))
                    self.send_header('Content-Length', str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
            except Exception as e:
                self.send_error(502, str(e))
        else:
            super().do_GET()


if __name__ == '__main__':
    http.server.ThreadingHTTPServer(('127.0.0.1', PORT), Handler).serve_forever()
