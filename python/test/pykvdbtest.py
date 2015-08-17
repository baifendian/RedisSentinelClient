#!/usr/bin/env python

import pykvdb

client = pykvdb.newClient('192.168.2.134:26379','item')

pykvdb.set(client, 'k', 'v')

vv = pykvdb.get(client, 'k')

print 'v='+vv

klist = ['k']

vlist = pykvdb.mget(client, klist)

print 'mget'
print vlist

vlist = pykvdb.mget2(client, klist)

print 'mget2'
print vlist