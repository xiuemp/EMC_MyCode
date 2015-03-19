#!/usr/bin/python

import sys
import re
import commands

COMMAND_PREFIX = 'psql -U postgres chris.db -c \" %s \"'
COMMAND_UPDATE = 'UPDATE systems SET upgrade_to=\'%s\',customer_name=\'%s\' where customer_name=\'%s\''

print sys.argv[1]
customer = sys.argv[1]
customer_refine = ''
upgrade_to = ''
pattern = re.compile(ur"\d*\Z")

nameset = customer.split("_")
if len(nameset) >= 3 and pattern.match(nameset[-2]):
    name_tmp = customer.replace(nameset[-2], '$').split('$')[0][:-1]
    if not nameset[-1] == '':
        version = '.'.join(list(nameset[-1]))
        upgrade_to= version 
    else:
        upgrade_to = 'N/A'
else:
    name_tmp = customer
    upgrade_to = 'N/A'
    
customer_refine = name_tmp

print customer_refine
print upgrade_to

cmd = COMMAND_UPDATE % (upgrade_to, customer_refine, customer)
status, output = commands.getstatusoutput(COMMAND_PREFIX % cmd)

print "name & upgrade_to refine done"