#!/usr/bin/python

"""

"""

import os
import commands
import getopt
import sys
import time

SUCCESS = 0
FAIL = 1
TOOLS_PATH = "/var/customer_info"
COMMAND_CUSTOMER_BASIC = TOOLS_PATH+"/query.sh -d %s -g %s %s | grep -w %s | head -n 1"
COMMAND_CUSTOMER_STATISTIC = TOOLS_PATH+"/query.sh -d %s -g %s %s | grep -w %s"
COMMAND_CUSTOMER_COUNT = TOOLS_PATH+"/query.sh -d %s -g %s %s | grep -w %s | wc -l"
COMMAND_CUSTOMER_ALL = TOOLS_PATH+"/query.sh -d %s -g %s -a"
PATH_CUSTOMER = "./customer_list.prop"
PATH_OUTPUT = "./data_persist"

NODE_DOMAIN = ['diskratio', 'ss_action', 'access_method', 'multi_subtenant', 'web_service', 'cas_status', 'node_replaced']
DISK_RECOVERY_DOMAIN = ['disk_recovery']

STATISTIC_DOMAIN = ['node_replaced']
COUNT_DOMAIN = ['disk_recovery']

def usage():
    """
    Print usage
    """
    print "Show usage"

def getops():
    """
    Get commands opt arguments
    """
    try:
        opts, _ = getopt.getopt(sys.argv[1:], "hp:a:",
            ["help", "property", "all"])
        return opts
    except getopt.GetoptError:
        usage()
        sys.exit(FAILURE)


def get_customers(dbname):
    """
    Get the list of all customers in given db
    """
    customers_list = []

    if not os.path.exists(PATH_CUSTOMER):
        print "customer list not found, pls run db_check.py"
        return
    f = open(PATH_CUSTOMER, 'r')
    for i in range(3):
        f.readline()

    while True:
        line = f.readline().strip('\r\n')
        if line:
            info = line.split(':')
            if info[0][0] == '#':
                break
            else:
                customers_list.append(info[2])
        else:
            break

    return customers_list

def locate_domain(property_name):
    """
    Judge the domain of property_name and return the right appendix
    """
    if property_name in NODE_DOMAIN:
        return '-n'
    elif property_name in DISK_RECOVERY_DOMAIN:
        return '-r'
    else:
        return ''

def get_dict_statistic(dbname, customers_list, property_name):
    """
    property that has bundles of records, need statistic of count, like "node_replaced"
    output form:
    customer:yes:total
    """
    customer_prop_dict = {}
    appendix = locate_domain(property_name)
    NO = 'no'
    YES = 'yes'
    no_count = 0
    yes_count = 0

    for customer in customers_list:
        cmd = COMMAND_CUSTOMER_STATISTIC % (dbname, customer, appendix, property_name)
        status, output = commands.getstatusoutput(cmd)
        if status != SUCCESS:
            print "error in "+cmd
        if customer in customer_prop_dict:
            print "duplicate customer error"
        else:
            lines = output.split('\n')
            for line in lines:
                tip = line.split(":")[1].replace(" ","-")[1:]
                if tip == NO:
                    no_count += 1
                elif tip == YES:
                    yes_count += 1
                else:
                    print "ERROR, unpredicted entry in " + property_name
        customer_prop_dict[customer] = str(yes_count)+':'+str(no_count+yes_count)
        no_count = 0
        yes_count = 0

    return customer_prop_dict

def get_dict_common(dbname, customers_list, property_name):
    """
    One key-value pair one customer
    """
    customer_prop_dict = {}
    appendix = locate_domain(property_name)

    for customer in customers_list:
        cmd = COMMAND_CUSTOMER_BASIC % (dbname, customer, appendix, property_name) 
        status, output = commands.getstatusoutput(cmd)
        if status != SUCCESS:
            print "error in "+cmd
        if customer in customer_prop_dict:
            print "duplicate customer error"
        else:
            info = output.split(":")
            value = info[1].replace(" ","-")[1:]
            if len(info) > 2:
                for i in range (2, len(info)):
                    value += "-"+info[i]

            if property_name == "diskratio" and value[0] == "0":
                value = value[1:]  #some diskratio looks 02:13, strip to 2:13
                
            customer_prop_dict[customer] = value

    return customer_prop_dict

def get_dict_count(dbname, customers_list, property_name):
    """
    Count lines for value
    """
    customer_prop_dict = {}
    appendix = locate_domain(property_name)
    key_word = ''
    if property_name in DISK_RECOVERY_DOMAIN:
        key_word = 'fsuuid'

    for customer in customers_list:
        cmd = COMMAND_CUSTOMER_COUNT % (dbname, customer, appendix, key_word) 
        status, output = commands.getstatusoutput(cmd)
        if status != SUCCESS:
            print "error in "+cmd
        if customer in customer_prop_dict:
            print "duplicate customer error"
        else:
            customer_prop_dict[customer] = output

    return customer_prop_dict

def get_dict(dbname, property_name):
    """
    Get the dictionary of all customers and their given property
    """
    customers_list = get_customers(dbname)
    customer_prop_dict = {}

    common_mode = False
    statistic_mode = False
    
    if property_name in STATISTIC_DOMAIN:
        statistic_mode = True
        customer_prop_dict = get_dict_statistic(dbname, customers_list, property_name)
    elif property_name in COUNT_DOMAIN:
        count_mode = True
        customer_prop_dict = get_dict_count(dbname, customers_list, property_name)
    else :
        common_mode = True
        customer_prop_dict = get_dict_common(dbname, customers_list, property_name)

    return customer_prop_dict

def write_to_file(property_name, customer_prop_dict):
    """
    Write to file in the same directory
    """
    if not os.path.exists(PATH_OUTPUT):
        commands.getstatusoutput("mkdir "+ PATH_OUTPUT)
    output_path = PATH_OUTPUT+"/"+property_name.replace(" ", "_")+".pd"
    f = open(output_path, "w")
    f.write(property_name+":\n")
    for customer, value in customer_prop_dict.items():
        f.write(customer+":"+value+"\n")

    f.write("#total#:"+str(len(customer_prop_dict)))
    f.close()

def all_archive(dbname, directory):
    """
    Archive all valid & latest customer info in to ./directory, named after customer full name 
    """
    if not os.path.exists("./"+directory):
        commands.getstatusoutput("mkdir ./"+directory)
    customers_list = get_customers(dbname)
    for customer in customers_list:
        cmd = COMMAND_CUSTOMER_ALL % (dbname, customer)
        status, output = commands.getstatusoutput(cmd)
        f = open("./"+directory+"/"+customer, 'w')
        f.write(output)
        f.close()

    print time.clock()


def main():
    """
    The main
    """
    property_name = None
    all_mode = False
    dbname = "customer.db"
    customers_dic = {}

    opts = getops()
    for option, value in opts:
        if option in ("-h", "--help"):
            usage()
            sys.exit(SUCCESS)
        elif option in ("-p", "--property"):
            property_name = value
            customer_prop_dict = get_dict(dbname, property_name)
            write_to_file(property_name, customer_prop_dict)
        elif option in ("-a", "--all"):
            all_mode = True
            directory = value
            all_archive(dbname, directory)
    

if __name__ == "__main__":
    main()
