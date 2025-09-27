import argparse
import http.server
import os
import socketserver
from urllib.parse import unquote

def main():
    parser = argparse.ArgumentParser(description="Origin (PFS) demo HTTP server")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--root", type=str, default="./origin_data")
    args = parser.parse_args()

    root = os.path.abspath(args.root)
    os.makedirs(root, exist_ok=True)

    # 仅允许访问 --root 目录下的文件，避免路径穿越
    class Handler(http.server.SimpleHTTPRequestHandler):
        def translate_path(self, path):
            path = unquote(path)
            if path.startswith("/"):
                path = path[1:]
            return os.path.join(root, path)

    with socketserver.ThreadingTCPServer(("0.0.0.0", args.port), Handler) as httpd:
        print(f"[origin] Serving {root} on port {args.port}")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\n[origin] Shutting down")

if __name__ == "__main__":
    main()