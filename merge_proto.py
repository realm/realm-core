

current_timestamp = 0

def next_timestamp():
    global current_timestamp
    current_timestamp = current_timestamp + 1
    return current_timestamp


server   = { 'peer_id': 0, 'version': 0, 'list': [], 'history': [] }
client_A = { 'peer_id': 1, 'version': 0, 'list': [], 'history': [] }
client_B = { 'peer_id': 2, 'version': 0, 'list': [], 'history': [] }
client_C = { 'peer_id': 3, 'version': 0, 'list': [], 'history': [] }


def add_operation(peer, operation, index, value, timestamp, remote_version, peer_id):
    new_version = peer['version'] + 1
    peer['version'] = new_version
    if operation == 'set':
        peer['list'][index] = value
    elif operation == 'insert':
        peer['list'].insert(index, value)
    else:
        raise Exception("Unknown operation")
    assert((remote_version == None) == (peer_id == None))
    if peer_id == None:
        peer_id = peer['peer_id']
    peer['history'].append({
        'operation': operation,
        'index': index,
        'value': value,
        'version': new_version,
        'remote_version': remote_version,
        'timestamp': timestamp,
        'peer_id': peer_id
    })


def sync_one(origin, destination):
    assert(origin['peer_id'] != destination['peer_id'])

    # Find the last origin version already integrated into the destination
    last_origin_version_integrated = 0
    for i in range(len(destination['history']), 0, -1):
        entry = destination['history'][i-1]
        if entry['peer_id'] == origin['peer_id']:
            last_origin_version_integrated = entry['remote_version']
            break

    # Find next origin version to integrate into the destination
    next_origin_version_to_intergate = last_origin_version_integrated + 1
    while True:
        origin_entry = origin['history'][next_origin_version_to_intergate-1]
        if origin_entry['peer_id'] != destination['peer_id']:
            break
        next_origin_version_to_intergate = next_origin_version_to_intergate + 1

    # Find the last destination version already integrated into the next origin
    # version to be intergated into the destination
    last_destination_version_integrated = 0
    for i in range(next_origin_version_to_intergate-1, 0, -1):
        entry = origin['history'][i-1]
        if entry['peer_id'] == destination['peer_id']:
            last_destination_version_integrated = entry['remote_version']
            break

    # Rebuild the operation transformation map
    last_destination_version = destination['version']
    map = []
    for ver in range(last_destination_version_integrated, last_destination_version):
        produced_ver = ver + 1
        entry = destination['history'][produced_ver-1]
        if entry['operation'] != 'insert':
            continue
        index = entry['index']
        i = 0
        while i < len(map):
            begin, diff, _, _ = map[i]
            if index < begin + diff:
                break
            i = i + 1
        num = 1 # Number of inserted list elements
        is_foreign_to_origin = (entry['peer_id'] != origin['peer_id'])
        if is_foreign_to_origin:
            for j in range(i, len(map)):
                map[j][1] = map[j][1] + num
            diff = map[i-1][1] if i > 0 else 0
            map.insert(i, [index-diff, diff+num, entry['timestamp'], entry['peer_id']])
        else:
            for j in range(i, len(map)):
                map[j][0] = map[j][0] + num

    # Use map to transform incoming operation
    i = len(map)
    while i > 0:
        begin, _, _, _ = map[i-1]
        if origin_entry['index'] >= begin:
            break
        i = i - 1
    if origin_entry['operation'] == 'insert':
        while i > 0:
            begin, diff, timestamp, peer_id = map[i-1]
            if origin_entry['index'] > begin:
                break
            if origin_entry['index'] == begin:
                if origin_entry['timestamp'] > timestamp:
                    break
                if origin_entry['timestamp'] == timestamp:
                    if origin_entry['peer_id'] > peer_id:
                        break
                    assert(origin_entry['peer_id'] < peer_id)
            i = i - 1
    diff = map[i-1][1] if i > 0 else 0

    add_operation(destination,
                  origin_entry['operation'],
                  origin_entry['index'] + diff,
                  origin_entry['value'],
                  origin_entry['timestamp'],
                  origin_entry['version'],
                  origin['peer_id'])


def set(peer, index, value, timestamp = None):
    if index < 0 or index >= len(peer['list']):
        raise Exception("Index out of range")
    if timestamp == None:
        timestamp = next_timestamp()
    add_operation(peer, 'set', index, value, timestamp, None, None)

def insert(peer, index, value, timestamp = None):
    if index < 0 or index > len(peer['list']):
        raise Exception("Index out of range")
    if timestamp == None:
        timestamp = next_timestamp()
    add_operation(peer, 'insert', index, value, timestamp, None, None)



insert(client_A, index=0, value=100)
insert(client_B, index=0, value=200)

insert(client_A, index=1, value=101)
insert(client_B, index=1, value=201)

sync_one(client_A, server)
sync_one(server, client_B)
sync_one(server, client_C)

insert(client_C, index=0, value=300)

sync_one(client_A, server)
sync_one(server, client_B)

sync_one(client_B, server)
sync_one(server, client_A)

sync_one(client_C, server)
sync_one(server, client_A)
sync_one(server, client_B)

insert(client_A, index=3, value=102)
sync_one(client_A, server)
sync_one(server, client_B)

sync_one(server, client_C)
sync_one(server, client_C)

sync_one(client_B, server)
sync_one(server, client_A)

sync_one(server, client_C)
sync_one(server, client_C)


#insert(server, index=0, value=100, timestamp=1)
#insert(server, index=0, value=101, timestamp=3)
#insert(server, index=0, value=102, timestamp=5)
#insert(server, index=3, value=103, timestamp=7)

#insert(client, index=0, value=200, timestamp=4)

#sync_one(client, server)
#sync_one(server, client)
#sync_one(server, client)
#sync_one(server, client)
#sync_one(server, client)



print "S:",   server['list']
print "A:", client_A['list']
print "B:", client_B['list']
print "C:", client_C['list']


# Deletions
