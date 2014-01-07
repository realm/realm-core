import tightdb
import os
import random

# this is just a test, so start over each time
db_path = 'test2.tightdb'
if os.path.exists(db_path):
    os.remove(db_path)

db = tightdb.SharedGroup(db_path)

print "Filling db"
with db.write() as group:
    table = group.create_table("test", ["key",   "int",
                                        "value", "string"])
    
    for i in range(1000000):
        table += [i, str(i)]

print "Reading random values"
for i in range(1000000):   
    with db.read("test") as table:
        ndx = random.randint(0, 999999)
        text = table[ndx].value
        
print "done"
