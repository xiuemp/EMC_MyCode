#!/usr/bin/python

import os
import commands
import hashlib
import sys
import time
import re

FILE_PATH = "./customer_list.prop"
COMMAND_LIST_ALL_CUSTOMERS = "/var/customer_info/query.sh -d customer.db -l"
COMMAND_JUDGE_NA = "/var/customer_info/query.sh -d customer.db -g %s | grep -w system_version"
COMMAND_JUDGE_TIME = "/var/customer_info/query.sh -d customer.db -g %s | grep -w last_collect_time"
SUCCESS = 0
FAILURE = 1


def judge_customer(name):
    """
    Judge if given customer is N/A, then return False. If useful customer return True
    """
    status, output = commands.getstatusoutput(COMMAND_JUDGE_NA % name)
    if output == "":
        print "ERROR"
    else:
        info = output.split(":")
        if info[1][1:] == 'N/A':
            return False
        else:
            return True


def get_md5_by_string(str): 
    """
    Get md5 of a input string
    """
    m = hashlib.md5()
    m.update(str) 
    return m.hexdigest() 

def judge_update(new, old):
    """
    Judge if the new customer can update the old one
    """
    status, output = commands.getstatusoutput(COMMAND_JUDGE_TIME % new)
    time_set = output.split(':')[1][1:]
    new_time = time.mktime(time.strptime(time_set, "%Y-%m-%d %H"))

    status, output = commands.getstatusoutput(COMMAND_JUDGE_TIME % old)
    time_set = output.split(':')[1][1:]
    old_time = time.mktime(time.strptime(time_set, "%Y-%m-%d %H"))

    if new_time > old_time:
        return True
    else:
        return False

def old_go(md5_db, db_list):
    print "##########################################\n"
    print " Record has been out of date. Rebuild now\n"
    print "##########################################\n"
    commands.getstatusoutput("rm -f %s" % FILE_PATH)
    new_go(md5_db, db_list)

def new_go(md5_db, db_list):
    """
    No existing customer_list.prop file, create it 
    Form:
    MD5
    ctime
    customer_name:num:latest_item
    ***
    ***
    #total#:**
    """
    f_out = open(FILE_PATH, 'w')
    f_out.write(md5_db+'\n')
    f_out.write(time.ctime()+'\n')
    f_out.write("customer_name:num:latest_item\n")

    name = []
    name_compact = []
    num = []
    item = []
    pattern = re.compile(ur"\d*\Z")

    count = 0
    for line in db_list.split("\n")[5:]:
        info = line.split(':')
        if not judge_customer(info[1][1:]):
            continue
        else:
            nameset = info[1][1:].split("_")
            if len(nameset) >= 3 and pattern.match(nameset[-2]):
                name_tmp = info[1][1:].replace(nameset[-2], '$').split('$')[0][:-1]
            else:
                name_tmp = info[1][1:]

            if name_tmp.replace('_', '') in name_compact:
                loc = name_compact.index(name_tmp.replace('_', ''))
                if judge_update(info[1][1:], item[loc]):
                    print "replace : " + str(loc) + " : "+name[loc]
                    name[loc] = name_tmp
                    num[loc] = int(info[0])
                    item[loc] = info[1][1:]
                else:
                    print "older version or duplicated but write to DB : " + info[1][1:]
                    continue
            else:
                name.append(name_tmp)
                name_compact.append(name_tmp.replace('_', ''))
                num.append(int(info[0]))
                item.append(info[1][1:])
                count += 1

    for i in range(len(name)):
        f_out.write(name[i]+':'+str(num[i])+':'+item[i]+'\n')

    f_out.write('#total#:'+str(count))

    f_out.close()

def main():
    """
    the Main
    """
    new_mode = False
    old_mode = False
    ok_mode = False

    status, output = commands.getstatusoutput(COMMAND_LIST_ALL_CUSTOMERS)
    md5_db = get_md5_by_string(output)

    if os.path.exists(FILE_PATH):
    	f = open(FILE_PATH, 'r')
        md5_file = f.readline().strip('\r\n')
        if md5_file == md5_db:
            ok_mode = True
            print "#################################\n"
            print " Latest status no need to update\n"
            print "#################################\n"
        else:
            old_mode = True
    else:
        new_mode = True

    if ok_mode:
        sys.exit(SUCCESS)
    elif new_mode:
        new_go(md5_db, output)
    elif old_mode:
        old_go(md5_db, output)
    else:
        sys.exit(FAILURE)
        


if __name__ == "__main__":
    main() 
