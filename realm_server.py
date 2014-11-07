import urlparse
import Cookie
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer

PORT_NUMBER = 8080

first_version = 0
transact_logs = []

class MyHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        global first_version, transact_logs
        url = urlparse.urlsplit(self.path)
        path = url.path
        print "path = '%s'" % (path)
        if path.startswith('/receive/'):
            client_version = int(path.split('/')[2])
            if transact_logs:
                if client_version == 0:
                    client_version = first_version - 1
                print "client_version = '%s'" % (client_version)
                last_version = first_version + (len(transact_logs) - 1)
                if client_version < last_version:
                    next_version = client_version + 1
                    transact_log_ndx = next_version - first_version
                    transact_log = transact_logs[transact_log_ndx]
                    self.send_response(200)
                    self.send_header('Content-type', 'application/octet-stream')
                    self.end_headers()
                    self.wfile.write(transact_log)
                    return
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write("up-to-date")
            return
        if path.startswith('/send/'):
            client_version = int(path.split('/')[2])
            invalid_version = False
            if client_version < 1:
                invalid_version = True
            elif transact_logs:
                print "first_version = %s" % (first_version)
                last_version = first_version + (len(transact_logs) - 1)
                print "last_version = %s" % (last_version)
                if client_version <= last_version:
                    self.send_response(200)
                    self.send_header('Content-type', 'text/plain')
                    self.end_headers()
                    self.wfile.write("conflict")
                    return
                if client_version > last_version + 1:
                    invalid_version = True
            if invalid_version:
                self.send_error(400, 'Bad request: Invalid client version %s' % (client_version))
                return
            body_size = int(self.headers['Content-Length'])
            transact_log = self.rfile.read(body_size)
            print "transact_log: '%s'" % (transact_log)
            first = not transact_logs
            print "first = %s" % (first)
            transact_logs.append(transact_log)
            if first:
                first_version = client_version
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write("ok")
            return
        self.send_error(404, 'File Not Found: %s' % (self.path))

    def get_session(self):
        if "Cookie" in self.headers:
            print self.headers["Cookie"]
            cookies = Cookie.SimpleCookie(self.headers["Cookie"])
            session_id = cookies['session'].value
            session = self.sessions[session_id]
            print "Found session for '%s'" % (session_id)
        else:
            session_id = '%s' % (len(self.sessions))
            print "New session id '%s'" % (session_id)
            self.send_header('Set-Cookie', 'session=%s' % (session_id))
            session = {}
            self.sessions[session_id] = session
        return session

try:
    server = HTTPServer(('', PORT_NUMBER), MyHandler)
    print 'Realm sync server started on port ' , PORT_NUMBER
    server.serve_forever()
except KeyboardInterrupt:
    print '^C received, shutting down the web server'
