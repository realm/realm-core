

server = { 'version': 0, 'list': [], 'history': [], 'map': (lambda(x): x) }
client = { 'version': 0, 'list': [], 'history': [], 'map': (lambda(x): x) }


def insert(participant, ndx, val, last_remote_version_integrated = None):
    new_version = participant['version'] + 1
    participant['version'] = new_version
    participant['list'].insert(ndx, val)
    is_remote = True
    if last_remote_version_integrated == None:
        is_remote = False
        last_remote_version_integrated = 0
        if len(participant['history']) != 0:
            last_remote_version_integrated = participant['history'][-1]['last_remote_version_integrated']
    participant['history'].append({ 'op': 'insert', 'ndx': ndx, 'val': val, 'ver': new_version, 'last_remote_version_integrated': last_remote_version_integrated, 'is_remote': is_remote })


def sync_one(fr, to):
    last_from_version_integrated = 0
    if len(to['history']) != 0:
        last_from_version_integrated = to['history'][-1]['last_remote_version_integrated']
    while True:
        op = fr['history'][last_from_version_integrated]
        if not op['is_remote']:
            break
        last_from_version_integrated = last_from_version_integrated + 1
    insert(to,  to['map'](op['ndx']), op['val'], op['ver'])
#    transformed = { 'op': op['op'], 'ndx': to['map'](op['ndx']), 'val': op['val'], 'ver': new_version, 'last_remote_version_integrated': last_remote_version_integrated }

insert(server, 0, 100)

insert(client, 0, 101)

sync_one(server, client)

insert(server, 0, 100)

sync_one(server, client)


#insert(server, 1, 101)

#insert(client, 1, 103)

#sync_one(server, client)

#insert(client, 3, 103)

print "Server:", server
print "Client:", client







