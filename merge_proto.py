import random

# Causality
# ---------
#
# Let A and B be two instructions, and let P be the peer on which B
# originates. B is then *causally affected* by A if, and only if A is integrated
# by P before the origination of B.
#
# Instructions A and B are said to be *causally related* if A causally affects
# B, or vice versa. Otherwise they are said to be *causally unrelated*.
#
# Note that if A causally affects B, then it is impossible for B to causally
# affect A.
#
#
# Causal consistency
# ------------------
#
# Causal consisteny means that when assigning a time stamp to changeset L of
# local origin, that time stamp is greater than, or equal to the greatest time
# stamp of all changesets (with local or foreign origin) integrated locally
# before L.
#
# The obvious source of causal inconsistency would be two peers with real-time
# clocks that are not perfectly synchronized. A less obvious cause would be
# nonmonotony (backward adjustments) of the local real-time clock.
#
# It is not yet known whether causal consistency will be necessary, or even
# desirable in the end. It is clear, however, that there are problems where one
# possible solution is, or relies on causal consistency. For an example, see
# "Set/set conflicts".
#
#
# Effective time stamp
# --------------------
#
# The *effective time stamp* of a changeset is the pair `(origin_timestamp,
# origin_peer_id)`. Effective time stamps are compared accoring to their
# lexicographical order, i.e., with `origin_timestamp` as the major criterion,
# and `origin_peer_id` as the minor. The effective time stamp of an operation,
# is the effective time stamp of the changeset to which the instruction belongs.
#
# Two causally unrelated instructions can have equal time stamps, but they
# cannot have equal effective time stamps, since they must originate from
# different peers.
#
#
# Set/set conflicts
# -----------------
#
# Two "set" operations conflict if they are causally unrelated and target the
# same element (same index).
#
# The resolution is to annul the instruction with the lowest effective time
# stamp. Note that they cannot have equal effective time stamps.
#
#
# From the point of view of a particular peer, an incoming "set" instruction A
# specifying index I conflicts with a causally unrelated previously integrated
# "set" instruction B specify index J if, and only if T(I) = U(J) and not
# annulled(B).
#
# Two different solutions: Causal consistency, or marking local instructions as
# deleted. But to be able to regenerate these markings from scratch, deleted
# incoming "set" instruction need to be converted into something else, such as
# an "annulled set" instruction.


class Peer:
    def __init__(self, peer_id):
        self._peer_id = peer_id  # Uniqueness is necessary
        self._version = 0
        self._current_time = 0 # System realtime clock (not necessarily monotonic)
        self._latest_time = 0 # Used to enforce causal consistency (must be persisted)
        self.list = []
        self.history = []

    def _add_changeset(self, changeset, timestamp, remote_version, remote_peer_id):
        assert (timestamp == None) == (remote_version == None)
        assert (remote_version == None) == (remote_peer_id == None)
        new_version = self._version + 1
        self._version = new_version
        if timestamp == None:
            timestamp = self._current_time
            if timestamp < self._latest_time:
                timestamp = self._latest_time
            self._latest_time = timestamp
        for operation in changeset:
            name  = operation['name']
            index = operation['index']
            value = operation['value']
            if name == 'set':
                self.list[index] = value
            elif name == 'insert':
                self.list.insert(index, value)
            else:
                raise Exception("Unknown operation '%s'" % (name))
        self.history.append({
            'changeset': changeset,
            'version': new_version,
            'remote_version': remote_version,
            'remote_peer_id': remote_peer_id,
            'timestamp': timestamp,
            'transformed_indexes': {}
        })

    def set(self, index, value):
        if index < 0 or index >= len(self.list):
            raise Exception('Index out of range')
        changeset = [ { 'name': 'set', 'index': index, 'value': value } ]
        self._add_changeset(changeset, None, None, None)

    def insert(self, index, value):
        if index < 0 or index > len(self.list):
            raise Exception('Index out of range')
        changeset = [ { 'name': 'insert', 'index': index, 'value': value } ]
        self._add_changeset(changeset, None, None, None)

    def integrate_next_changeset_from(self, remote):
        assert self._peer_id != remote._peer_id
        assert self._is_server != remote._is_server # Star shape is a requirement

        # Find the last remote version already integrated
        last_remote_version_integrated = 0
        for i in range(len(self.history), 0, -1):
            entry = self.history[i-1]
            if entry['remote_peer_id'] == remote._peer_id:
                last_remote_version_integrated = entry['remote_version']
                break

        # Find next remote version to integrate
        next_remote_version_to_integrate = last_remote_version_integrated + 1
        while True:
            remote_entry = remote.history[next_remote_version_to_integrate-1]
            if remote_entry['remote_peer_id'] != self._peer_id:
                break
            next_remote_version_to_integrate = next_remote_version_to_integrate + 1

        # Find the last local version already integrated into the next remote
        # version to be integrated
        last_local_version_integrated = 0
        for i in range(next_remote_version_to_integrate-1, 0, -1):
            remote_entry_2 = remote.history[i-1]
            if remote_entry_2['remote_peer_id'] == self._peer_id:
                last_local_version_integrated = remote_entry_2['remote_version']
                break

        # It is necessary to add 1 to the time stamp of `remote_entry` to ensure
        # that the effective time stamp `(self._latest_time, self.__peer_id)` is
        # always greater than, or equal to the effective time stamp of
        # `remote_entry`.
        if self._latest_time < remote_entry['timestamp'] + 1:
            self._latest_time = remote_entry['timestamp'] + 1

        remote_peer_id = remote_entry['remote_peer_id']
        if remote_peer_id == None:
            remote_peer_id = remote._peer_id
        remote_timestamp = (remote_entry['timestamp'], remote_peer_id)

        # Reconstruct transformed indexes
        last_local_version = self._version
        transformed_indexes = {}
        for version_2 in range(last_local_version_integrated+1, last_local_version+1):
            entry_2 = self.history[version_2 - 1]
            received_2_from_remote = (entry_2['remote_peer_id'] == remote._peer_id)
            if not received_2_from_remote:
                indexes_2 = {}
                for i in range(len(entry_2['changeset'])):
                    operation_2 = entry_2['changeset'][i]
                    indexes_2[i] = operation_2['index']
                transformed_indexes[version_2] = indexes_2
                continue
            for operation_2 in entry_2['changeset']:
                if operation_2['name'] == 'set':
                    continue
                index_2 = operation_2['index']
                for version_1 in range(last_local_version_integrated+1, version_2)[::-1]:
                    entry_1 = self.history[version_1 - 1]
                    received_1_from_remote = (entry_1['remote_peer_id'] == remote._peer_id)
                    if received_1_from_remote:
                        continue
                    num_operations = len(entry_1['changeset'])
                    indexes_1 = transformed_indexes[version_1]
                    for i in range(num_operations)[::-1]:
                        operation_1 = entry_1['changeset'][i]
                        index_1 = indexes_1[i]
                        if operation_1['name'] == 'insert' and index_1 < index_2:
                            index_2 -= 1
                        elif operation_2['name'] == 'insert' and index_1 >= index_2:
                            index_1 += 1
                        indexes_1[i] = index_1

        # Check that reconstructed transformed indexes conincide with cached
        # ones
        for version in transformed_indexes:
            indexes = transformed_indexes[version]
            entry = self.history[version-1]
            if remote._peer_id in entry['transformed_indexes']:
                assert indexes == entry['transformed_indexes'][remote._peer_id]
            entry['transformed_indexes'][remote._peer_id] = indexes

        # Transform incoming changeset
        new_changeset = []
        for remote_operation in remote_entry['changeset']:
            remote_index = remote_operation['index']
            discard_operation = False
            for version in range(last_local_version_integrated+1, last_local_version+1):
                entry = self.history[version-1]
                received_from_remote = (entry['remote_peer_id'] == remote._peer_id)
                if received_from_remote:
                    continue
                local_peer_id = entry['remote_peer_id']
                if local_peer_id == None:
                    local_peer_id = self._peer_id
                num_operations = len(entry['changeset'])
                local_indexes = transformed_indexes[version]
                local_timestamp = (entry['timestamp'], local_peer_id)
                assert local_timestamp != remote_timestamp
                for i in range(num_operations):
                    local_operation = entry['changeset'][i]
                    local_index = local_indexes[i]
                    if local_operation['name'] == 'set' and remote_operation['name'] == 'set':
                        if local_index == remote_index and local_timestamp > remote_timestamp:
                            discard_operation = True
                            break
                    elif local_operation['name'] == 'insert':
                        if remote_operation['name'] == 'insert':
                            if  (local_index, local_timestamp) < (remote_index, remote_timestamp):
                                remote_index += 1
                            else:
                                local_index += 1
                        elif local_index <= remote_index:
                            remote_index += 1
                    elif remote_operation['name'] == 'insert':
                        if local_index >= remote_index:
                            local_index += 1
                    local_indexes[i] = local_index
                if discard_operation:
                    break
            if not discard_operation:
                new_changeset.append({
                    'name':  remote_operation['name'],
                    'index': remote_index,
                    'value': remote_operation['value']
                })

        self._add_changeset(new_changeset, remote_entry['timestamp'],
                            remote_entry['version'], remote._peer_id)

    def count_outstanding_remote_changesets(self, remote):
        assert self._peer_id != remote._peer_id

        # Find the last remote version already integrated
        last_remote_version_integrated = 0
        for i in range(len(self.history), 0, -1):
            entry = self.history[i-1]
            if entry['remote_peer_id'] == remote._peer_id:
                last_remote_version_integrated = entry['remote_version']
                break

        # Count subsequent remote versions to integrate
        n = 0
        for i in range(last_remote_version_integrated+1, remote._version+1):
            remote_entry = remote.history[i-1]
            if remote_entry['remote_peer_id'] != self._peer_id:
                n = n + 1
        return n


    def get_peer_id(self):
        return self._peer_id

    def get_version(self):
        return self._version

    def get_time(self):
        return self._current_time

    def set_time(self, time):
        self._current_time = time

    def advance_time(self, time):
        self._current_time = self._current_time + time



class Server(Peer):
    def __init__(self, peer_id):
        Peer.__init__(self, peer_id)
        self._is_server = True



class Client(Peer):
    def __init__(self, peer_id):
        Peer.__init__(self, peer_id)
        self._is_server = False



def dump(server, clients):
    print('Server:  ', server.list)
    for client in clients:
        print('Client_%s:' % (client.get_peer_id()), client.list)
    print('Server:  ', server.history)
    for client in clients:
       print('Client_%s:' % (client.get_peer_id()), client.history)

def check(cond):
    if not cond:
        raise Exception('Check failed')



# Example that sorts a foreign insert in the middle of a sequence of consecutive
# inserts due to timing
def test_merge_inserts_by_time():
    server = Server(peer_id=0)
    client = Client(peer_id=1)

    server.set_time(0)
    server.insert(index=0, value=100)
    server.set_time(1)
    server.insert(index=1, value=101)

    client.set_time(2)
    client.insert(index=0, value=200)

    server.set_time(3)
    server.insert(index=2, value=102)
    server.set_time(4)
    server.insert(index=3, value=103)

    server.integrate_next_changeset_from(client)
    client.integrate_next_changeset_from(server)
    client.integrate_next_changeset_from(server)
    client.integrate_next_changeset_from(server)
    client.integrate_next_changeset_from(server)

    check(server.list == [100, 101, 200, 102, 103])
    check(client.list == server.list)



# Example that demonstrates that non-end insertions are handled correctly, that
# is, intuitively
def test_nonend_insertions():
    server = Server(peer_id=0)
    client = Client(peer_id=1)

    server.set_time(0)
    server.insert(index=0, value=100)
    server.set_time(1)
    server.insert(index=0, value=101)

    client.set_time(2)
    client.insert(index=0, value=200)

    server.set_time(3)
    server.insert(index=0, value=102)
    server.set_time(4)
    server.insert(index=0, value=103)

    server.integrate_next_changeset_from(client)
    client.integrate_next_changeset_from(server)
    client.integrate_next_changeset_from(server)
    client.integrate_next_changeset_from(server)
    client.integrate_next_changeset_from(server)

    check(server.list == [103, 102, 101, 100, 200])
    check(client.list == server.list)



# Example that demonstrates 2-way merge with server in the middle (star-shaped
# topology)
def test_twoway_with_server():
    server   = Server(peer_id=0)
    client_A = Client(peer_id=1)
    client_B = Client(peer_id=2)

    client_A.set_time(0)
    client_A.insert(index=0, value=100)
    client_B.set_time(1)
    client_B.insert(index=0, value=200)

    client_A.set_time(2)
    client_A.insert(index=1, value=101)
    client_B.set_time(3)
    client_B.insert(index=1, value=201)

    server.integrate_next_changeset_from(client_A)
    client_B.integrate_next_changeset_from(server)

    server.integrate_next_changeset_from(client_A)
    client_B.integrate_next_changeset_from(server)

    server.integrate_next_changeset_from(client_B)
    client_A.integrate_next_changeset_from(server)

    server.integrate_next_changeset_from(client_B)
    client_A.integrate_next_changeset_from(server)

    check(server.list == [100, 200, 101, 201])
    check(client_A.list == server.list)
    check(client_B.list == server.list)



# Example that demonstrates 3-way merge with server in the middle (star-shaped
# topology)
def test_threeway_with_server():
    server   = Server(peer_id=0)
    client_A = Client(peer_id=1)
    client_B = Client(peer_id=2)
    client_C = Client(peer_id=3)

    client_A.set_time(0)
    client_A.insert(index=0, value=100)
    client_B.set_time(1)
    client_B.insert(index=0, value=200)

    client_A.set_time(2)
    client_A.insert(index=1, value=101)
    client_B.set_time(3)
    client_B.insert(index=1, value=201)

    server.integrate_next_changeset_from(client_A)

    client_B.integrate_next_changeset_from(server)
    client_C.integrate_next_changeset_from(server)

    client_C.set_time(4)
    client_C.insert(index=0, value=300)

    server.integrate_next_changeset_from(client_A)
    client_B.integrate_next_changeset_from(server)

    server.integrate_next_changeset_from(client_B)
    client_A.integrate_next_changeset_from(server)

    server.integrate_next_changeset_from(client_C)
    client_A.integrate_next_changeset_from(server)
    client_B.integrate_next_changeset_from(server)

    client_A.set_time(5)
    client_A.insert(index=3, value=102)
    server.integrate_next_changeset_from(client_A)
    client_B.integrate_next_changeset_from(server)

    client_C.integrate_next_changeset_from(server)
    client_C.integrate_next_changeset_from(server)

    server.integrate_next_changeset_from(client_B)
    client_A.integrate_next_changeset_from(server)

    client_C.integrate_next_changeset_from(server)
    client_C.integrate_next_changeset_from(server)

    check(server.list == [300, 100, 200, 102, 101, 201])
    check(client_A.list == server.list)
    check(client_B.list == server.list)
    check(client_C.list == server.list)



# Randomized testing
def test_randomized():
    print('Running randomized tests. This may take a while (half a minute).')

    num_clients = 5

    error = False
    for _ in range(10000):
#        seed = random.randint(0, 1000000000)
#        seed = 277992091
#        random.seed(seed)

        server  = Server(peer_id=0)
        clients = [Client(peer_id=1+i) for i in range(0, num_clients)]

#        print('server   = Server(peer_id=%s)' % (server.get_peer_id()))
#        for client in clients:
#            peer_id = client.get_peer_id()
#            print('client_%s = Client(peer_id=%s)' % (peer_id, peer_id))

        current_value = 0
        def next_value():
            nonlocal current_value
            current_value = current_value + 1
            return current_value

        num_remain_set_operations    = 7
        num_remain_insert_operations = 7

        def do_set(client):
            nonlocal num_remain_set_operations
            num_remain_set_operations = num_remain_set_operations - 1
            index = random.randint(0, len(client.list)-1)
            value = next_value()
#            print('client_%s.set(%s, %s)' % (client.get_peer_id(), index, value))
            client.set(index, value)

        def do_insert(client):
            nonlocal num_remain_insert_operations
            num_remain_insert_operations = num_remain_insert_operations - 1
            index = random.randint(0, len(client.list))
            value = next_value()
#            print('client_%s.insert(%s, %s)' % (client.get_peer_id(), index, value))
            client.insert(index, value)

        def do_download(client):
#            print('client_%s.integrate_next_changeset_from(server)' % (client.get_peer_id()))
            client.integrate_next_changeset_from(server)

        def do_upload(client):
#            print('server.integrate_next_changeset_from(client_%s)' % (client.get_peer_id()))
            server.integrate_next_changeset_from(client)

        def add_client_actions(actions, client):
            if num_remain_set_operations > 0:
                actions.append({
                    'name':   'set_%s' % (client.get_peer_id()),
                    'weight': len(client.list),
                    'exec':   (lambda: do_set(client))
                })
            if num_remain_insert_operations > 0:
                actions.append({
                    'name':   'insert_%s' % (client.get_peer_id()),
                    'weight': len(client.list) + 1,
                    'exec':   (lambda: do_insert(client))
                })
            actions.append({
                'name':   'download_%s' % (client.get_peer_id()),
                'weight': client.count_outstanding_remote_changesets(server),
                'exec':   (lambda: do_download(client))
            })
            actions.append({
                'name':   'upload_%s' % (client.get_peer_id()),
                'weight': server.count_outstanding_remote_changesets(client),
                'exec':   (lambda: do_upload(client))
            })

        while True:
            for client in clients:
                if random.randint(0,4) == 0:
#                    print('client_%s.advance_time(1)' % (client.get_peer_id()))
                    client.advance_time(1)
            actions = []
            for client in clients:
                add_client_actions(actions, client)
            total_weight = 0
            for action in actions:
                total_weight = total_weight + action['weight']
            if total_weight == 0:
                break
            i = random.randint(0, total_weight-1)
            found = False
            for action in actions:
                weight = action['weight']
                if i < weight:
                    action['exec']()
                    found = True
                    break
                i = i - weight
            assert found
#            print('# Server:   %s  (version=%s)' % (server.list, server.get_version()))
#            for client in clients:
#                print('# Client_%s: %s  (version=%s, time=%s)' % (client.get_peer_id(), client.list,
#                                                                  client.get_version(),
#                                                                  client.get_time()))

#        print('dump(server, [ %s ])' % (', '.join(["client_%s" % (c.get_peer_id()) for c in clients])))

        for client in clients:
            if client.list != server.list:
               error = True
               break

        if error:
            print('------------------ ERROR ------------------')
#            print('seed = ', seed)
            dump(server, clients)
            break

    check(not error)



test_merge_inserts_by_time()
test_nonend_insertions()
test_twoway_with_server()
test_threeway_with_server()
test_randomized()
print('All tests passed')
