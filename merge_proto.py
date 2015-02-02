

current_timestamp = 0

def next_timestamp():
    global current_timestamp
    current_timestamp = current_timestamp + 1
    return current_timestamp


def add_changeset(_self, changeset, timestamp, remote_version, remote_peer_id):
    assert((remote_version == None) == (remote_peer_id == None))
    new_version = _self['version'] + 1
    _self['version'] = new_version
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
    next_remote_version_to_intergate = last_remote_version_integrated + 1
    while True:
        remote_entry = remote['history'][next_remote_version_to_intergate-1]
        if remote_entry['remote_peer_id'] != _self['peer_id']:
            break
        next_remote_version_to_intergate = next_remote_version_to_intergate + 1

    # Find the last local version already integrated into the next remote
    # version to be intergated
    last_local_version_integrated = 0
    for i in range(next_remote_version_to_intergate-1, 0, -1):
        remote_entry_2 = remote['history'][i-1]
        if remote_entry_2['remote_peer_id'] == _self['peer_id']:
            last_local_version_integrated = remote_entry_2['remote_version']
            break

    # Rebuild the operation transformation map
    last_local_version = _self['version']
    map = []
    for version in range(last_local_version_integrated+1, last_local_version+1):
        entry = _self['history'][version-1]
        for operation in entry['changeset']:
            if operation['name'] != 'insert':
                continue
            index = operation['index']
            i = 0
            while i < len(map):
                begin, diff, _, _ = map[i]
                if index < begin + diff:
                    break
                i = i + 1
            num = 1 # Number of inserted list elements
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

    # Use map to transform incoming operations
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
        diff = map[i-1][1] if i > 0 else 0
        new_changeset.append({
            'name':  operation['name'],
            'index': operation['index'] + diff,
            'value': operation['value']
        })

    add_changeset(_self, new_changeset, remote_entry['timestamp'],
                  remote_entry['version'], remote['peer_id'])


def set(peer, index, value, reuse_prev_timestamp = False):
    if index < 0 or index >= len(peer['list']):
        raise Exception("Index out of range")
    changeset = [ { 'name': 'set', 'index': index, 'value': value } ]
    timestamp = next_timestamp() if not reuse_prev_timestamp else current_timestamp
    add_changeset(peer, changeset, timestamp, None, None)

def insert(peer, index, value, reuse_prev_timestamp = False):
    if index < 0 or index > len(peer['list']):
        raise Exception("Index out of range")
    changeset = [ { 'name': 'insert', 'index': index, 'value': value } ]
    timestamp = next_timestamp() if not reuse_prev_timestamp else current_timestamp
    add_changeset(peer, changeset, timestamp, None, None)



# Example that sorts a foreign insert in the middle of a sequence of consecutive
# inserts due to timing

server = { 'peer_id': 0, 'version': 0, 'list': [], 'history': [] }
client = { 'peer_id': 1, 'version': 0, 'list': [], 'history': [] }

insert(server, index=0, value=100)
insert(server, index=1, value=101)

insert(client, index=0, value=200)

insert(server, index=2, value=102)
insert(server, index=3, value=103)

add_remote_changeset(server, client)
add_remote_changeset(client, server)
add_remote_changeset(client, server)
add_remote_changeset(client, server)
add_remote_changeset(client, server)

print '---'
print "Server:", server['list']
print "Client:", client['list']



# Example that demonstrates that non-end insertions are handled correctly
# (intuitively)

server = { 'peer_id': 0, 'version': 0, 'list': [], 'history': [] }
client = { 'peer_id': 1, 'version': 0, 'list': [], 'history': [] }

insert(server, index=0, value=100)
insert(server, index=0, value=101)

insert(client, index=0, value=200)

insert(server, index=0, value=102)
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

server   = { 'peer_id': 0, 'version': 0, 'list': [], 'history': [] }
client_A = { 'peer_id': 1, 'version': 0, 'list': [], 'history': [] }
client_B = { 'peer_id': 2, 'version': 0, 'list': [], 'history': [] }
client_C = { 'peer_id': 3, 'version': 0, 'list': [], 'history': [] }

insert(client_A, index=0, value=100)
insert(client_B, index=0, value=200)

insert(client_A, index=1, value=101)
insert(client_B, index=1, value=201)

add_remote_changeset(server, client_A)

add_remote_changeset(client_B, server)
add_remote_changeset(client_C, server)

insert(client_C, index=0, value=300)

add_remote_changeset(server, client_A)
add_remote_changeset(client_B, server)

add_remote_changeset(server, client_B)
add_remote_changeset(client_A, server)

add_remote_changeset(server, client_C)
add_remote_changeset(client_A, server)
add_remote_changeset(client_B, server)

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

