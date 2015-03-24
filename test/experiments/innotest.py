import realm
import os
import random

# this is just a test, so start over each time
db_path = 'parallel_benchmark.realm'
if os.path.exists(db_path):
    os.remove(db_path)

db = realm.SharedGroup(db_path)

print "Filling db"
with db.write() as group:
    table = group.create_table("test", ["key",   "int",
                                        "value", "string"])
    
    for i in range(1000000):
        table += [i, str(i)]

print "done"
