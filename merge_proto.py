import random


def create(peer_id):
    return {
        'peer_id': peer_id,  # Uniqueness is critical
        'version': 0,
        'list': [],
        'history': [],
        'current_time': 0, # System realtime clock (not necessarily monotone)
        'latest_local_time_seen': 0, # Protects against local nonmonotony (must be persisted)
        'latest_remote_time_seen': 0 # Protects against nonlocal temporal inconsistency (must be persisted)
    }

# Note: latest_local_time_seen and latest_remote_time_seen could be combined
# into one thing, but not without increasing the risk of time acceleration,
# which if occuring over a long time, will have unfortunate effects on perceived
# temporal realism.


def add_changeset(_self, changeset, timestamp, remote_version, remote_peer_id):
    assert((timestamp == None) == (remote_version == None))
    assert((remote_version == None) == (remote_peer_id == None))
    new_version = _self['version'] + 1
    _self['version'] = new_version
    if timestamp == None:
        timestamp = _self['current_time']
        if timestamp <= _self['latest_remote_time_seen']:
            timestamp = _self['latest_remote_time_seen'] + 1
        if timestamp < _self['latest_local_time_seen']:
            timestamp = _self['latest_local_time_seen']
        _self['latest_local_time_seen'] = timestamp
    for operation in changeset:
        name  = operation['name']
        index = operation['index']
        value = operation['value']
        if name == 'set':
            _self['list'][index] = value
        elif name == 'insert':
            _self['list'].insert(index, value)
        else:
            raise Exception("Unknown operation '%s'" % (name))
    _self['history'].append({
        'changeset': changeset,
        'version': new_version,
        'remote_version': remote_version,
        'remote_peer_id': remote_peer_id,
        'timestamp': timestamp
    })


def add_remote_changeset(_self, remote):
    assert(_self['peer_id'] != remote['peer_id'])

    # Find the last remote version already integrated
    last_remote_version_integrated = 0
    for i in range(len(_self['history']), 0, -1):
        entry = _self['history'][i-1]
        if entry['remote_peer_id'] == remote['peer_id']:
            last_remote_version_integrated = entry['remote_version']
            break

    # Find next remote version to integrate
    next_remote_version_to_integrate = last_remote_version_integrated + 1
    while True:
        remote_entry = remote['history'][next_remote_version_to_integrate-1]
        if remote_entry['remote_peer_id'] != _self['peer_id']:
            break
        next_remote_version_to_integrate = next_remote_version_to_integrate + 1

    if _self['latest_remote_time_seen'] < remote_entry['timestamp']:
        _self['latest_remote_time_seen'] = remote_entry['timestamp']

    # Find the last local version already integrated into the next remote
    # version to be integrated
    last_local_version_integrated = 0
    for i in range(next_remote_version_to_integrate-1, 0, -1):
        remote_entry_2 = remote['history'][i-1]
        if remote_entry_2['remote_peer_id'] == _self['peer_id']:
            last_local_version_integrated = remote_entry_2['remote_version']
            break

    # Build the operation transformation map. This map expresses the necessary
    # translation of the list element index of the next foreign list modifying
    # operation to be integrated. The index in an incoming 'set' operation will
    # be translated according to this map, but the situation is more complicated
    # when the incoming operation is an 'insert' operation, as it may involve
    # resolving conflicts with insertions already performed locally.
    #
    # The map is represented as an ordered sequence of 4-tuples (begin, diff,
    # timestamp, peer_id) each one representing the next flat piece of the graph
    # of a 'staircase function'. The domain of a particular entry is
    # `begin`..`end-1` where `end` is `begin` of the next entry in the map, or
    # 'infinity' if there is no next entry. In general, some entries in the map
    # have empty domains, in which case they are in the map only for the purpose
    # of resolving conflicts. `diff` is the value to be added to the list
    # element index of the incoming operation, and this value is always strictly
    # greater than `diff` of the preceding map entry, or strictly greater than
    # zero for the first entry in the map. A map with no entries expresses an
    # identity transformation. `timestamp` and `peer_id` specify the timestamp
    # and peer identifier of the locally performed insertion operation that gave
    # rise to the map entry.
    #
    # For a non-insertion operation occurring at list index `i`, the effective
    # change in index is the value of `diff` of the last map entry where `begin
    # <= i`. For *insertion* operations, on the other hand, the relevant map
    # entry is instead the last one that satisfies `begin <= i && (begin < i ||
    # timestamp <= ts && (timestamp < ts || peer_id <= pid))` where `ts` and
    # `pid` are the timestamp and the peer identifier of the incoming insertion
    # operation respectively. Note that the map is layed out precisely such that
    # the process of locating the right map entry **is** the process that
    # resolves conflicts.
    last_local_version = _self['version']
    map = []
    for version in range(last_local_version_integrated+1, last_local_version+1):
        entry = _self['history'][version-1]
        for operation in entry['changeset']:
            index = operation['index']
            i = 0
            while i < len(map):
                begin, diff, _, _ = map[i]
                if index < begin + diff:
                    break
                i = i + 1
            num = 1 # Number of overwritten/inserted list elements
            if operation['name'] == 'set':
                pass
            else:
                if entry['remote_peer_id'] != remote['peer_id']:
                    for j in range(i, len(map)):
                        map[j][1] = map[j][1] + num
                    diff = map[i-1][1] if i > 0 else 0
                    peer_id = entry['remote_peer_id']
                    if peer_id == None:
                        peer_id = _self['peer_id']
                    map.insert(i, [index-diff, diff+num, entry['timestamp'], peer_id])
                else:
                    for j in range(i, len(map)):
                        map[j][0] = map[j][0] + num

    # Use the new map to transform the incoming operations, updating it as
    # necessary along the way.
    new_changeset = []
    remote_entry_peer_id = remote_entry['remote_peer_id']
    if remote_entry_peer_id == None:
        remote_entry_peer_id = remote['peer_id']
    for operation in remote_entry['changeset']:
        i = len(map)
        while i > 0:
            begin, _, _, _ = map[i-1]
            if operation['index'] >= begin:
                break
            i = i - 1
        if operation['name'] == 'insert':
            while i > 0:
                begin, diff, timestamp, peer_id = map[i-1]
                if operation['index'] > begin:
                    break
                if operation['index'] == begin:
                    if remote_entry['timestamp'] > timestamp:
                        break
                    if remote_entry['timestamp'] == timestamp:
                        if remote_entry_peer_id > peer_id:
                            break
                        assert(remote_entry_peer_id < peer_id)
                i = i - 1
            num = 1 # Number of inserted list elements
            for j in range(i, len(map)):
                map[j][0] = map[j][0] + num
        diff = map[i-1][1] if i > 0 else 0
        new_changeset.append({
            'name':  operation['name'],
            'index': operation['index'] + diff,
            'value': operation['value']
        })

    add_changeset(_self, new_changeset, remote_entry['timestamp'],
                  remote_entry['version'], remote['peer_id'])


def set(_self, index, value):
    if index < 0 or index >= len(_self['list']):
        raise Exception("Index out of range")
    changeset = [ { 'name': 'set', 'index': index, 'value': value } ]
    add_changeset(_self, changeset, None, None, None)

def insert(_self, index, value):
    if index < 0 or index > len(_self['list']):
        raise Exception("Index out of range")
    changeset = [ { 'name': 'insert', 'index': index, 'value': value } ]
    add_changeset(_self, changeset, None, None, None)


def count_outstanding_remote_changesets(_self, remote):
    assert(_self['peer_id'] != remote['peer_id'])

    # Find the last remote version already integrated
    last_remote_version_integrated = 0
    for i in range(len(_self['history']), 0, -1):
        entry = _self['history'][i-1]
        if entry['remote_peer_id'] == remote['peer_id']:
            last_remote_version_integrated = entry['remote_version']
            break

    # Count subsequent remote versions to integrate
    n = 0
    for i in range(last_remote_version_integrated+1, remote['version']+1):
        remote_entry = remote['history'][i-1]
        if remote_entry['remote_peer_id'] != _self['peer_id']:
            n = n + 1
    return n


def set_time(_self, time):
    _self['current_time'] = time

def advance_time(_self, time):
    _self['current_time'] = _self['current_time'] + time




# Example that sorts a foreign insert in the middle of a sequence of consecutive
# inserts due to timing

server = create(peer_id=0)
client = create(peer_id=1)

set_time(server, 0)
insert(server, index=0, value=100)
set_time(server, 1)
insert(server, index=1, value=101)

set_time(client, 2)
insert(client, index=0, value=200)

set_time(server, 3)
insert(server, index=2, value=102)
set_time(server, 4)
insert(server, index=3, value=103)

add_remote_changeset(server, client)
add_remote_changeset(client, server)
add_remote_changeset(client, server)
add_remote_changeset(client, server)
add_remote_changeset(client, server)

print '---'
print "Server:", server['list']
print "Client:", client['list']



# Example that demonstrates that non-end insertions are handled correctly, that
# is, intuitively

server = create(peer_id=0)
client = create(peer_id=1)

set_time(server, 0)
insert(server, index=0, value=100)
set_time(server, 1)
insert(server, index=0, value=101)

set_time(client, 2)
insert(client, index=0, value=200)

set_time(server, 3)
insert(server, index=0, value=102)
set_time(server, 4)
insert(server, index=0, value=103)

add_remote_changeset(server, client)
add_remote_changeset(client, server)
add_remote_changeset(client, server)
add_remote_changeset(client, server)
add_remote_changeset(client, server)

print '---'
print "Server:", server['list']
print "Client:", client['list']



# Example that demonstrates 3-way merge with server in the middle (star-shaped
# topology)

server   = create(peer_id=0)
client_A = create(peer_id=1)
client_B = create(peer_id=2)
client_C = create(peer_id=3)

set_time(client_A, 0)
insert(client_A, index=0, value=100)
set_time(client_B, 1)
insert(client_B, index=0, value=200)

set_time(client_A, 2)
insert(client_A, index=1, value=101)
set_time(client_B, 3)
insert(client_B, index=1, value=201)

add_remote_changeset(server, client_A)

add_remote_changeset(client_B, server)
add_remote_changeset(client_C, server)

set_time(client_C, 4)
insert(client_C, index=0, value=300)

add_remote_changeset(server, client_A)
add_remote_changeset(client_B, server)

add_remote_changeset(server, client_B)
add_remote_changeset(client_A, server)

add_remote_changeset(server, client_C)
add_remote_changeset(client_A, server)
add_remote_changeset(client_B, server)

set_time(client_A, 5)
insert(client_A, index=3, value=102)
add_remote_changeset(server, client_A)
add_remote_changeset(client_B, server)

add_remote_changeset(client_C, server)
add_remote_changeset(client_C, server)

add_remote_changeset(server, client_B)
add_remote_changeset(client_A, server)

add_remote_changeset(client_C, server)
add_remote_changeset(client_C, server)

print '---'
print "Server:  ",   server['list']
print "Client-A:", client_A['list']
print "Client-B:", client_B['list']
print "Client-C:", client_C['list']


num_clients = 5

for _ in range(10000000):
    server  = create(peer_id=0)
    clients = [create(peer_id=1+i) for i in range(0, num_clients)]

    current_value = 0
    def next_value():
        global current_value
        current_value = current_value + 1
        return current_value

    num_remain_set_operations    = 0
    num_remain_insert_operations = 7

    def do_set(client):
        global num_remain_set_operations
        num_remain_set_operations = num_remain_set_operations - 1
        index = random.randint(0, len(client['list'])-1)
        value = next_value()
#        print "set(clients[%s], %s, %s)  {time=%s}" % (client['peer_id'], index, value, client['current_time'])
        set(client, index, value)

    def do_insert(client):
        global num_remain_insert_operations
        num_remain_insert_operations = num_remain_insert_operations - 1
        index = random.randint(0, len(client['list']))
        value = next_value()
#        print "insert(clients[%s], %s, %s)  {time=%s}" % (client['peer_id'], index, value, client['current_time'])
        insert(client, index, value)

    def do_download(client):
#        print "add_remote_changeset(clients[%s], server)" % (client['peer_id'])
        add_remote_changeset(client, server)

    def do_upload(client):
#        print "add_remote_changeset(server, clients[%s])" % (client['peer_id'])
        add_remote_changeset(server, client)

    def add_client_actions(actions, client):
        if num_remain_set_operations > 0:
            actions.append({
                'name':   'set[%s]' % (client['peer_id']),
                'weight': len(client['list']),
                'exec':   (lambda: do_set(client))
            })
        if num_remain_insert_operations > 0:
            actions.append({
                'name':   'insert[%s]' % (client['peer_id']),
                'weight': len(client['list']) + 1,
                'exec':   (lambda: do_insert(client))
            })
        actions.append({
            'name':   'download[%s]' % (client['peer_id']),
            'weight': count_outstanding_remote_changesets(client, server),
            'exec':   (lambda: do_download(client))
        })
        actions.append({
            'name':   'upload[%s]' % (client['peer_id']),
            'weight': count_outstanding_remote_changesets(server, client),
            'exec':   (lambda: do_upload(client))
        })

    while True:
        for client in clients:
            if random.randint(0,4) == 0:
                advance_time(client, 1)
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
        assert(found)

    error = False
    for client in clients:
        if client['list'] != server['list']:
            error = True
            break

    if error:
        print '------------------ ERROR ------------------'
        print "Server:   ", server['list']
        for client in clients:
            print "Client[%d]:" % (client['peer_id']), client['list']
        print "Server:   ", server['history']
        for client in clients:
            print "Client[%d]:" % (client['peer_id']), client['history']
        break
