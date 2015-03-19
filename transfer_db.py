#!/usr/bin/python

import commands
import re

COMMAND_PREFIX = 'psql -U postgres chris.db -c \" %s \"'
COMMAND_UPDATE = 'UPDATE systems SET upgrade_to=\'%s\',customer_name=\'%s\' where customer_name=\'%s\''

status, output = commands.getstatusoutput(COMMAND_PREFIX % "select customer_name from systems")
customer_list = output.split("\n")[2:-2]
for i in range(len(customer_list)):
    customer_list[i] = customer_list[i][1:]
customer_refine = []
upgrade_to = []
pattern = re.compile(ur"\d*\Z")

for customer in customer_list:
    nameset = customer.split("_")
    if len(nameset) >= 3 and pattern.match(nameset[-2]):
        name_tmp = customer.replace(nameset[-2], '$').split('$')[0][:-1]
        if not nameset[-1] == '':
            version = '.'.join(list(nameset[-1]))
            upgrade_to.append(version)
        else:
            upgrade_to.append('N/A')
    else:
        name_tmp = customer
        upgrade_to.append('N/A')
        
    customer_refine.append(name_tmp)

#print customer_list
#print customer_refine
print upgrade_to

for i in range(len(customer_list)):
    cmd = COMMAND_UPDATE % (upgrade_to[i], customer_refine[i], customer_list[i])
    status, output = commands.getstatusoutput(COMMAND_PREFIX % cmd)

print "Done DB Transfer"
