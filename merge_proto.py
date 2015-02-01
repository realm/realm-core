

current_timestamp = 0

def next_timestamp():
    global current_timestamp
    current_timestamp = current_timestamp + 1
    return current_timestamp


server = { 'peer_id': 0, 'version': 0, 'list': [], 'history': [] }
client = { 'peer_id': 1, 'version': 0, 'list': [], 'history': [] }


def add_operation(peer, operation, index, value, timestamp, last_remote_version_integrated, peer_id):
    new_version = peer['version'] + 1
    peer['version'] = new_version
    if operation == 'set':
        peer['list'][index] = value
    elif operation == 'insert':
        peer['list'].insert(index, value)
    else:
        raise Exception("Unknown operation")
    assert((last_remote_version_integrated == None) == (peer_id == None))
    if last_remote_version_integrated == None:
        last_remote_version_integrated = 0
        if len(peer['history']) != 0:
            last_remote_version_integrated = peer['history'][-1]['last_remote_version_integrated']
    if peer_id == None:
        peer_id = peer['peer_id']
    peer['history'].append({
        'operation': operation,
        'index': index,
        'value': value,
        'version': new_version,
        'last_remote_version_integrated': last_remote_version_integrated,
        'timestamp': timestamp,
        'peer_id': peer_id
    })


def sync_one(origin, destination):
    # Find next history entry to transfer from origin peer to destination peer
    last_origin_version_integrated = 0
    if len(destination['history']) != 0:
        last_origin_version_integrated = destination['history'][-1]['last_remote_version_integrated']
    while True:
        next_origin_version_to_intergate = last_origin_version_integrated + 1
        origin_entry = origin['history'][next_origin_version_to_intergate-1]
        is_remote = (origin_entry['peer_id'] != origin['peer_id'])
        if not is_remote:
            break
        last_origin_version_integrated = next_origin_version_to_intergate

    # Rebuild the operation transformation map
    ver_1 = origin_entry['last_remote_version_integrated']
    ver_2 = destination['version']
    map = []
    for ver in range(ver_1, ver_2):
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
        is_remote = (entry['peer_id'] != destination['peer_id'])
        if is_remote:
            for j in range(i, len(map)):
                map[j][0] = map[j][0] + num
        else:
            for j in range(i, len(map)):
                map[j][1] = map[j][1] + num
            diff = map[i-1][1] if i > 0 else 0
            map.insert(i, [index-diff, diff+num, entry['timestamp'], entry['peer_id']])

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
                  origin_entry['peer_id'])


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



insert(server, index=0, value=100)
insert(client, index=0, value=200)

insert(server, index=1, value=101)
insert(client, index=1, value=201)

sync_one(server, client)
sync_one(server, client)

sync_one(client, server)

insert(server, index=3, value=102)
sync_one(server, client)

sync_one(client, server)



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



print "Server:", server['list']
print "Client:", client['list']


# Original non-end remote insertions?

# Deletions

# Server in the middle
