import subprocess

subprocess.call(["/usr/bin/python", "innotest.py"])

print "1-0"
subprocess.call(["/usr/bin/time","-v","-otiming",           "./innotest", "0", "1",  "0"])
print "4-0"
subprocess.call(["/usr/bin/time","-v","-otiming","--append","./innotest", "0", "4",  "0"])
print "8-0"
subprocess.call(["/usr/bin/time","-v","-otiming","--append","./innotest", "0", "8",  "0"])
print "16-0"
subprocess.call(["/usr/bin/time","-v","-otiming","--append","./innotest", "0", "16", "0"])

print "1-100K"
subprocess.call(["/usr/bin/time","-v","-otiming","--append","./innotest", "0", "1",  "100000"])
print "4-100K"
subprocess.call(["/usr/bin/time","-v","-otiming","--append","./innotest", "0", "4",  "100000"])
print "8-100K"
subprocess.call(["/usr/bin/time","-v","-otiming","--append","./innotest", "0", "8",  "100000"])
print "16-100K"
subprocess.call(["/usr/bin/time","-v","-otiming","--append","./innotest", "0", "16", "100000"])

print "1-10K"
subprocess.call(["/usr/bin/time","-v","-otiming","--append","./innotest", "0", "1",  "10000"])
print "4-10K"
subprocess.call(["/usr/bin/time","-v","-otiming","--append","./innotest", "0", "4",  "10000"])
print "8-10K"
subprocess.call(["/usr/bin/time","-v","-otiming","--append","./innotest", "0", "8",  "10000"])
print "16-10K"
subprocess.call(["/usr/bin/time","-v","-otiming","--append","./innotest", "0", "16", "10000"])
