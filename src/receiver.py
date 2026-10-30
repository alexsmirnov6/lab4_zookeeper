"""Accept checkpoints and result pointers using an independent ZooKeeper session."""

import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer

from kazoo.exceptions import BadVersionError

from storage import commit_checked, encode, event, get_document, run_path, s3_client, zk_client


class Receiver(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def reply(self, status, document):
        body = encode(document)
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        self.reply(200 if self.path == "/health" and self.server.zk.connected else 503,
                   {"connected": self.server.zk.connected})

    def do_POST(self):
        if self.path not in ("/checkpoint", "/publish"):
            self.reply(404, {"error": "Unknown operation"})
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if not 0 < length <= 65536:
                raise ValueError("Invalid request size")
            request = json.loads(self.rfile.read(length))
            if not request["key"].startswith(os.environ["RUN_ID"] + "/"):
                raise ValueError("The object belongs to another run")
            get_document(self.server.s3, request["key"], request["sha256"])
            zk = self.server.zk
            root = self.server.root
            operation = self.path == "/publish"
            transaction = zk.transaction()
            # Checking the epoch and changing the pointer must be one atomic operation.
            if not operation or self.server.fencing:
                transaction.check(root + "/epoch", version=request["epoch"])
            pointer = root + ("/result" if operation else "/checkpoint")
            transaction.set_data(pointer, encode(request), version=-1)
            if operation:
                transaction.create(root + "/publications/entry-", encode(request), sequence=True)
            committed = commit_checked(transaction)
            stat = next(item for item in committed if hasattr(item, "version"))
            event("receiver", "записал результат" if operation else "checkpoint_saved",
                  coordinator=request["coordinator"], epoch=request["epoch"],
                  version=stat.version, key=request["key"])
            self.reply(200, {"version": stat.version})
        except BadVersionError:
            event("receiver", "publication_rejected" if self.path == "/publish" else "checkpoint_rejected",
                  coordinator=request["coordinator"], epoch=request["epoch"], error="BadVersionError")
            self.reply(409, {"error": "BadVersionError"})
        except Exception as error:
            event("receiver", "request_failed", error=type(error).__name__, message=str(error))
            self.reply(500, {"error": type(error).__name__, "message": str(error)})


def main():
    server = HTTPServer(("0.0.0.0", 8080), Receiver)
    server.zk = zk_client()
    server.zk.start(timeout=30)
    server.s3 = s3_client()
    server.root = run_path()
    server.fencing = os.environ.get("FENCING", "1") == "1"
    event("receiver", "started", fencing=server.fencing)
    try:
        server.serve_forever()
    finally:
        server.zk.stop()
        server.zk.close()
        server.s3.close()


if __name__ == "__main__":
    main()
