core_git_remote = 'git@github.com:realm/realm-core.git'
sync_git_remote = 'git@github.com:realm/realm-sync.git'

import sys
import os
import subprocess
import tempfile
import shutil
import time
import threading

root_dir = os.path.dirname(sys.argv[0])

cmake = os.environ.get('CMAKE', 'cmake')
cmake_generator = os.environ.get('CMAKE_GENERATOR', 'Ninja')
make = os.environ.get('MAKE', 'ninja')
cmake_build_type = os.environ.get('CMAKE_BUILD_TYPE', 'Debug')
build_dir_name = os.environ.get('REALM_BUILD_DIR',
                                {'Release': 'build.release',
                                 'Debug': 'build.debug'}.get(cmake_build_type, 'build'))
cmd_prefix = os.environ.get('CMD_PREFIX', '').split()

print('cmake=%s' % (cmake,))
print('cmake_generator=%s' % (cmake_generator,))
print('make=%s' % (make,))
print('cmake_build_type=%s' % (cmake_build_type,))
print('build_dir_name=%s' % (build_dir_name,))
print('cmd_prefix=%s' % (cmd_prefix,))

def ensure_dir(path):
    if not os.path.isdir(path):
        os.makedirs(path)

class TempDir:
    def __init__(self):
        self._path = tempfile.mkdtemp()
    def __enter__(self):
        return self._path
    def __exit__(self, type, value, traceback):
        shutil.rmtree(self._path)
        return False # Reraise exception

def run_subprocess(args, cwd = None):
    proc = subprocess.Popen(args, cwd = cwd)
    exit_status = proc.wait()
    if exit_status != 0:
        raise Exception('Execution of %s failed with exit status %s' % (args, exit_status))

def fetch(repo_name, git_remote, revision):
    repo_parent_dir = root_dir + '/fetches/' + repo_name
    repo_dir = repo_parent_dir + '/' + revision
    if not os.path.exists(repo_dir):
        print('================> Fetching %s@%s' % (repo_name, revision))
        run_subprocess(['git', 'clone', '--quiet', '--branch', revision, '--recurse-submodules',
                        git_remote, repo_dir])
    return repo_dir

def fetch_core(revision):
    return fetch('core', core_git_remote, revision)

def fetch_sync(revision):
    return fetch('sync', sync_git_remote, revision)

def read_core_revision(repo_dir):
    prefix = 'REALM_CORE_VERSION='
    path = repo_dir + '/dependencies.list'
    file = open(path)
    for line in file:
        if line.startswith(prefix):
            version = line[len(prefix):].strip()
            tag = 'v' + version
            return tag
    raise ValueError('Failed to find Realm Core version in %s' % (path,))

def build_core(revision):
    repo_dir = fetch_core(revision)
    build_dir = repo_dir + '/' + build_dir_name
    print('================> Building core@%s' % (revision,))
    ensure_dir(build_dir)
    run_subprocess(cmd_prefix +
                   [cmake, '-G', cmake_generator,
                    '-D', 'REALM_NO_TESTS=1',
                    '-D', 'CMAKE_BUILD_TYPE=' + cmake_build_type,
                    '..'], cwd = build_dir)
    run_subprocess(cmd_prefix + [make], cwd = build_dir)
    return build_dir

def build_sync(revision):
    if revision:
        repo_dir = fetch_sync(revision)
        core_revision = read_core_revision(repo_dir)
        core_build_dir = build_core(core_revision)
        print('================> Building sync@%s' % (revision,))
    else:
        repo_dir = root_dir + '/../..'
        core_build_dir = os.environ.get('REALM_CORE_BUILDTREE',
                                        repo_dir + '/../realm-core/' + build_dir_name)
        print('================> Building sync')
    openssl_root_dir = os.environ.get('OPENSSL_ROOT_DIR', '/usr')
    build_dir = repo_dir + '/' + build_dir_name
    core_build_dir_2 = os.path.abspath(core_build_dir)
    ensure_dir(build_dir)
    run_subprocess(cmd_prefix +
                   [cmake, '-G', cmake_generator,
                    '-D', 'OPENSSL_ROOT_DIR=' + openssl_root_dir,
                    '-D', 'REALM_CORE_BUILDTREE=' + core_build_dir_2,
                    '-D', 'CMAKE_BUILD_TYPE=' + cmake_build_type,
                    '..'], cwd = build_dir)
    run_subprocess(cmd_prefix + [make], cwd = build_dir)
    return build_dir

def build_server(revision = None):
    build_dir = build_sync(revision)
    suffix = '-dbg' if cmake_build_type == 'Debug' else ''
    return build_dir + '/src/realm/realm-sync-worker' + suffix

def build_client(revision = None):
    build_dir = build_sync(revision)
    return build_dir + '/test/test-client'

class Server:
    def __init__(self, exec_path, server_dir):
        listen_port = 0 # Assign an unused port
        args = [ exec_path,
                 '-r', server_dir,
                 '-k', root_dir + '/../test_pubkey.pem',
                 '--log-level', 'info',
                 '--listen-port', str(listen_port) ]
        self._proc = subprocess.Popen(args, stderr = subprocess.PIPE)
        self._listen_port_semaphore = threading.Semaphore(0)
        self._errors_seen = False
        self._log_scanning_thread = threading.Thread(target = lambda: self._scan_log())
        self._log_scanning_thread.start()
        self._listen_port_semaphore.acquire()
    def __enter__(self):
        return self
    def __exit__(self, type, value, traceback):
        self.kill()
        return False # Reraise exception
    def get_listen_port(self):
        return self._listen_port
    def kill(self):
        self._proc.kill()
        self._proc.wait()
        self._log_scanning_thread.join()
        if self._errors_seen:
            raise Exception('Errors were reported by server')
    def _scan_log(self):
        listening_prefix = 'Listening on '
        error_prefix = 'ERROR: '
        have_port = False
        while True:
            line = self._proc.stderr.readline()
            if not line:
                break
            line_2 = line.decode('utf-8')
            if not have_port:
                if line_2.startswith(listening_prefix):
                    rest = line_2[len(listening_prefix) :]
                    pos = rest.index(' ')
                    endpoint = rest[0 : pos]
                    pos = endpoint.rindex(':')
                    port = int(endpoint[pos + 1 :])
                    self._listen_port = port
                    self._listen_port_semaphore.release()
                    have_port = True
            if line_2.startswith(error_prefix):
                self._errors_seen = True
            sys.stderr.write('Server: ' + line_2)

class Client:
    def __init__(self, exec_path, realm_path, virtual_path, listen_port, extra_args):
        args = [ exec_path,
                 realm_path,
                 'realm://localhost:%s%s' % (listen_port, virtual_path),
                 '--abort-on-error', 'always' ]
        self._proc = subprocess.Popen(args + extra_args, stderr = subprocess.PIPE)
        self._log_scanning_thread = threading.Thread(target = lambda: self._scan_log())
        self._log_scanning_thread.start()
    def __enter__(self):
        return self
    def __exit__(self, type, value, traceback):
        self.wait()
        return False # Reraise exception
    def wait(self):
        exit_status = self._proc.wait()
        self._log_scanning_thread.join()
        if exit_status != 0:
            raise Exception('Execution of client failed with exit status %s' % (exit_status,))
    def _scan_log(self):
        while True:
            line = self._proc.stderr.readline()
            if not line:
                break
            line_2 = line.decode('utf-8')
            sys.stderr.write('Client: ' + line_2)

def run_client(exec_path, realm_path, virtual_path, listen_port, extra_args):
    with Client(exec_path, realm_path, virtual_path, listen_port, extra_args):
        pass

def upload(exec_path, realm_path, virtual_path, listen_port, num_blobs = 1):
    extra_args = [ '--num-transacts', '1',
                   '--transact-period', '0',
                   '--num-blobs', "%d" % num_blobs ]
    run_client(exec_path, realm_path, virtual_path, listen_port, extra_args)

def download(exec_path, realm_path, virtual_path, listen_port):
    extra_args = []
    run_client(exec_path, realm_path, virtual_path, listen_port, extra_args)

# Test a scenario that triggered a bug
# (https://jira.mongodb.org/browse/SYNC-14) in version Sync 4.7.2.
def double_download_with_protocol_28():
    server_exec_path   = build_server() # Local version
    client_exec_path_1 = build_client() # Local version
    client_exec_path_2 = build_client('v4.6.4') # Protocol version 28
    print('================> TEST: double_download_with_protocol_28')
    with TempDir() as server_dir:
        with Server(server_exec_path, server_dir) as server:
            listen_port = server.get_listen_port()
            with TempDir() as client_dir:
                writer_path = os.path.join(client_dir, 'writer.realm')
                reader_path = os.path.join(client_dir, 'reader.realm')
                upload(client_exec_path_1, writer_path, '/test', listen_port)
                download(client_exec_path_2, reader_path, '/test', listen_port)
                upload(client_exec_path_1, writer_path, '/test', listen_port)
                download(client_exec_path_2, reader_path, '/test', listen_port)
double_download_with_protocol_28()

# Test a scenario that triggered a bug
# (https://jira.mongodb.org/browse/SYNC-27) in version Sync 4.7.4.
def double_download_with_sync_4_6_4_then_upgrade_server():
    server_exec_path_1 = build_server('v4.6.4') # Protocol version 28
    server_exec_path_2 = build_server() # Local version
    client_exec_path   = build_client('v4.6.4') # Protocol version 28
    print('================> TEST: double_download_with_sync_4_6_4_then_upgrade_server')
    with TempDir() as client_dir:
        writer_path = os.path.join(client_dir, 'writer.realm')
        reader_path = os.path.join(client_dir, 'reader.realm')
        with TempDir() as server_dir:
            with Server(server_exec_path_1, server_dir) as server:
                listen_port = server.get_listen_port()
                upload(client_exec_path, writer_path, '/test', listen_port)
                download(client_exec_path, reader_path, '/test', listen_port)
                upload(client_exec_path, writer_path, '/test', listen_port)
            with Server(server_exec_path_2, server_dir) as server:
                listen_port = server.get_listen_port()
                download(client_exec_path, reader_path, '/test', listen_port)
double_download_with_sync_4_6_4_then_upgrade_server()

def double_download_with_sync_4_6_4_then_upgrade_both():
    server_exec_path_1 = build_server('v4.6.4') # Protocol version 28
    server_exec_path_2 = build_server() # Local version
    client_exec_path_1 = build_client('v4.6.4') # Protocol version 28
    client_exec_path_2 = build_client() # Local version
    print('================> TEST: double_download_with_sync_4_6_4_then_upgrade_both')
    with TempDir() as client_dir:
        writer_path = os.path.join(client_dir, 'writer.realm')
        reader_path = os.path.join(client_dir, 'reader.realm')
        with TempDir() as server_dir:
            with Server(server_exec_path_1, server_dir) as server:
                listen_port = server.get_listen_port()
                upload(client_exec_path_1, writer_path, '/test', listen_port)
                download(client_exec_path_1, reader_path, '/test', listen_port)
                upload(client_exec_path_1, writer_path, '/test', listen_port)
            with Server(server_exec_path_2, server_dir) as server:
                listen_port = server.get_listen_port()
                download(client_exec_path_2, reader_path, '/test', listen_port)
double_download_with_sync_4_6_4_then_upgrade_both()

print('================> All tests passed')
