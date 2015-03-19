#!/usr/bin/python
# __CR__
# Copyright (c) 2008-2014 EMC Corporation
# All Rights Reserved
#
# This software contains the intellectual property of EMC Corporation
# or is licensed to EMC Corporation from third parties.  Use of this
# software and the intellectual property contained therein is expressly
# limited to the terms and conditions of the License Agreement under which
# it is provided by or on behalf of EMC.
# __CR__

#

"""
This tool is to query the version objects count on a certian master mds port.
Using this tool can get a high level overview of version objects in the system.
"""

import os
import sys
import getopt
import time
import datetime
import logging
from Queue import Queue
import threading
import common
import commands
from atmos import log
from atmos import const
from atmos.mdsset import get_all_mds_sets
from atmos.exception import AtmosError

ERR_ALERT_MESSAGE = "An error has occured. The final result %s may not be \
accurate. Please check if all the MDS are up."
MAX_LIST_COUNT = 8192

MAX_RMS_QUERY_TIMES = 5
RMS_RETRY_INTERVAL = 60 # seconds

RET_RESULT_INACCURATE = 2

DB_SIZE = 16384

LOG_FILE = "/var/log/maui/query_version_objects.log"

CMD_QUERY_TENANT_LIST = "mauisvcmgr -s mauimds -c mauimds_queryTenantList -m \
localhost:%s | grep tenantList | cut -d '=' -f 2"

CMD_COUNT_VEROBJ = "query_version_object -p %s -t %s"
CMD_LIST_VEROBJ = "query_version_object -p %s -t %s -l"

CMD_SHOW_MDSDIR = "mdsdir %s"
CMD_LL_BDB = "ls -l %s/%s_version.bdbdb"
CMD_LIST_LOCALMDS = "rmsview -l mauimds | grep master:$HOSTNAME"

def usage():
    """
    Print usage
    """
    usage_str = """NAME
    %s - Tool to query version objects

SYNOPSIS
    %s -L [-p <mds_port>{,<mds_port>} [-l]

OPTIONS
    Use the following options to specify a function:

    -h, --help
        Print this usage information

    -L, --local
        Query version object from local master mds port
        Or for tenants locally

    -q, --query
        Judge if there is any version object in the master(default: the whole system)
        If adding -t, it become list all tenants which contain version object

    -p, --port
        Specify the master mds port to query

    -l, --list
        List all version object ids.
        If too many version objects, not all will be listed

    -t, --tenant
        List all tenants contain version object(couple with -q)

    -a, --system
        Query version object from in the system

EXAMPLE
    1. Query there is version object on all local master MDS ports or not.
       %s -q -L

    2. List tenants containing version object on all local master MDS ports.
       %s -q -L -t

    3. Query there is version object in the system or not.
       %s -q

    4. List tenants containing version object in the system.
       %s -q -t

    5. Count version objects number on a local master MDS port.
       %s -L -p 10401

    6. Count version objects number on multiple local master MDS ports.
       %s -L -p 10401,10403

    7. Count version objects number on all local master MDS ports.
       %s -L

    8. List version objects on a local master MDS port.
       %s -L -p 10401 -l

    9. List version objects on multiple local master MDS ports.
       %s -L -p 10401,10403 -l

    10. List version objects on all local master MDS ports.
       %s -L -l

    11. Count version objects number in the system.
       %s -a

    12. List version objects in the system.
       %s -a -l
    """
    names = (os.path.basename(sys.argv[0]),) * 14
    print usage_str % names

def getopts():
    """
    Get command opt arguments
    """
    try:
        opts, _ = getopt.getopt(sys.argv[1:], "hLp:lc:aFqt",
            ["help", "local", "port", "list", "count", \
            "system", "force", "query", "tenant"])
        return opts
    except getopt.GetoptError:
        usage()
        sys.exit(const.FAILURE)

def exit_with_msg(msg):
    """
    Print the specified message, and exit with failure
    """
    print msg
    sys.exit(const.FAILURE)

def get_tenant_list(port):
    """
    get tenant list of specified mds
    """
    cmd = CMD_QUERY_TENANT_LIST % port
    logging.info("Will run '%s' locally", cmd)
    status, output = commands.getstatusoutput(cmd)
    if status != const.SUCCESS:
        raise AtmosError("Failed to run '%s', ouput: %s", output)

    return list(set(output.split()))

def get_all_master_mds(force_mode = False):
    """
    Get all the mds masters in the system

    Returns:
    An array containing the mds masters.
    Each entry as [mds_host, mds_port].

    Raises:
    AtmosError - Failed to get mds masters.
    """
    # We create a temp file to cache the mds set count
    # to avoid frequently access CMF.
    # If this value is commonly used by other services/tools
    # in the future, it should be stored in a config file
    # with proper refresh policy.
    need_refresh = True
    cache_file = "/var/tmp/mds_set_count"
    mdsset_count = 0

    # cache fresh policy:
    # 1. if cache file doesn't exists, refresh it.
    # 2. if cache file is out of date(1 day without change), refresh it.
    # 3. if failed to get all mds masters, refresh it next time.

    if os.path.isfile(cache_file):
        # cache file exists
        time_delta = datetime.datetime.now() - \
                     datetime.datetime.fromtimestamp\
                     (os.path.getmtime(cache_file))
        if time_delta.days < 1:
            # cache file keeps up to date
            try:
                with open(cache_file, 'r') as cache:
                    cache_data = cache.read()
                try:
                    mdsset_count = int(cache_data)
                    need_refresh = False
                except ValueError:
                    logging.warning("mdsset count cache file is corrupted.")
                    try:
                        os.remove(cache_file)
                    except OSError, msg:
                        logging.warning("Failed to remove cache file: %s", msg)

            except IOError, msg:
                logging.warning("Failed to read cache file: %s", msg)

    if need_refresh == True:
        try:
            all_mds_sets = get_all_mds_sets()
            mdsset_count = len(all_mds_sets)
        except AtmosError, msg:
            raise AtmosError("Failed to get all mds sets: %s" % msg)
        # refresh the cache file
        try:
            with open(cache_file, 'w') as cache:
                cache.write(str(mdsset_count))
        except IOError, msg:
            logging.warning("Failed to update cache file: %s", msg)

    for retry_count in xrange(0, MAX_RMS_QUERY_TIMES):
        try:
            mds_masters = []

            cmd = "rmsview -l mauimds | grep upmaster"
            status, output = commands.getstatusoutput(cmd)
            if status != const.SUCCESS:
                raise AtmosError("Failed to query rms: %s" % output)
            for line in output.split("\n"):
                try:
                    # upmaster:<hostname>:<port>:MDS:<location>
                    mds_masters.append(line.split(":")[1:3])
                except IndexError:
                    # ignore line with wrong format
                    continue


            if len(mds_masters) != mdsset_count:
                if(force_mode):
                    logging.warning\
                    ("Not all master mds are up, but force to proceed.")
                    return const.FAILURE, mds_masters
                else:
                    msg = "Not all master mds are up."
                    raise AtmosError(msg)

            return const.SUCCESS, mds_masters

        except AtmosError, msg:
            # During MDS failover, rmsview may not get the new master in time.
            # Retry 5 times to make this function tolerant with MDS failover.
            if retry_count + 1 < MAX_RMS_QUERY_TIMES:
                logging.warning\
                ("Failed to get mds masters. %s. Wait %d seconds and retry...",
                                msg, RMS_RETRY_INTERVAL)
                time.sleep(RMS_RETRY_INTERVAL)
            else:
                # Failed to get mds master
                # Remove cache file and refresh it next time.
                try:
                    os.remove(cache_file)
                except OSError, msg:
                    logging.warning("Failed to remove cache file: %s", msg)
                # Don't exit if we couldn't get all masters
                # Give the inaccurate result instead
                logging.error\
                ("Failed to get mds masters. Exceed max retry time.")
                return const.FAILURE, mds_masters
                #raise AtmosError
                #("Failed to get mds masters. Exceed max retry time.")


def run_threads_on_mdshost(method, result_queue, mode, local_flag, list_count="no_need"):
    """
    run a given method on all mds_ports of all mds_hosts by multi-threads, and put result on given queue
    if local_flag == True, means only run on the local mds_host
    """
    threads = []
    mds_masters = {}

    try:
        ret_status, mds_list = get_all_master_mds(mode)
        if ret_status != const.SUCCESS:
            logging.error("fail to get all_master_mds")
            return
        if local_flag:
            status, output = commands.getstatusoutput("echo $HOSTNAME")
            if status != const.SUCCESS:
                logging.error("fail to get hostname")
                return
            for mds_host, mds_port in mds_list:
                if mds_host != output:
                    continue
                else:
                    if mds_host in mds_masters:
                        mds_masters[mds_host] += ("," + mds_port)
                    else:
                        mds_masters[mds_host] = mds_port

        else:
            for mds_host, mds_port in mds_list:
                if mds_host in mds_masters:
                    mds_masters[mds_host] += ("," + mds_port)
                else:
                    mds_masters[mds_host] = mds_port

        for mds_host, mds_ports in mds_masters.items():
            if list_count == "no_need":
                threads.append(threading.Thread(target=method, \
                     args=(mds_host, mds_ports, result_queue)))
            else:
                threads.append(threading.Thread(target=method, \
                     args=(mds_host, mds_ports, result_queue, list_count)))

    except AtmosError, msg:
        exit_with_msg(msg)

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

def run_threads_on_mdshost_bytenant(method, result_queue, list_count, mode):
    """
    run a given method which aim at tenant on all mds_ports of localhost by multi-threads, and put result on given queue
    """
    threads = []

    try:
        ret_status, mds_list = get_all_master_mds(mode)
        if ret_status != const.SUCCESS:
            logging.error("fail to get all_master_mds")
            return

        status, output = commands.getstatusoutput("echo $HOSTNAME")
        if status != const.SUCCESS:
            logging.error("fail to get hostname")
            return
        for mds_host, mds_port in mds_list:
            if mds_host != output:
                continue
            else:
                tenants = get_tenant_list(mds_port)
                for tenant in tenants:
                    if list_count == 1:  # 1 is flag for just accounting
                        threads.append(threading.Thread\
                        (target=method, \
                        args=(mds_port, tenant, result_queue)))
                    else:
                        threads.append(threading.Thread\
                            (target=method, \
                            args=(mds_port, tenant, result_queue, list_count)))

    except AtmosError, msg:
        exit_with_msg(msg)

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()



def print_result(result_queue):
    """
    Print the result_queue get by multi-threads, before that, the result will be sorted
    """
    result_list = []

    while not result_queue.empty():
        status, output = result_queue.get()
        if status != const.SUCCESS:
            print output
            continue

        for objinfo in output.split("\n"):
            if objinfo == "":
                continue

            result_list.append(objinfo)

    result_list.sort()
    for result in result_list:
        print result


def list_verobj_for_port_tenat(mds_port, tenant, result_queue, list_count):
    """
    Callback for multi-threads to list on each master MDS.
    """
    if result_queue.qsize() >= list_count:
        return

    cmd = CMD_LIST_VEROBJ % (mds_port, tenant)
    status, output = commands.getstatusoutput(cmd)
    if status != const.SUCCESS:
        errmsg = "Failed to run '%s', output: %s" % (cmd, output)
        logging.error(errmsg)
        result_queue.put([status, errmsg])
        return

    if not output:
        return

    for verobj in output.split('\n'):
        if verobj[:2] != '  ':
            verobj_str = '-t %s -i %s' % (tenant, verobj)
        else:
            verobj_str = '  -t %s -i %s' % (tenant, verobj[2:])

        if result_queue.qsize() >= list_count:
            return
        else:
            result_queue.put([status, verobj_str])


def list_local_version_objects(list_count, mds_ports):
    """
    List version objects only from local master MDS given mds_ports, if no given mds_ports, will search all ports by default
    Input example
        10401
        10401,10403
        10401,10403,10405
    """
    has_error = False
    result_queue = Queue()
    if mds_ports == None:
        run_threads_on_mdshost_bytenant(list_verobj_for_port_tenat, \
            result_queue, list_count, False)
    else:
        threads = []
        try:
            for mds_port in mds_ports.split(','):
                tenants = get_tenant_list(mds_port)
                for tenant in tenants:
                    threads.append(threading.Thread\
                        (target=list_verobj_for_port_tenat, \
                        args=(mds_port, tenant, result_queue, list_count)))
        except AtmosError, msg:
            exit_with_msg(msg)

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    while not result_queue.empty():
        status, output = result_queue.get()
        if status != const.SUCCESS:
            has_error = True
        if output == '':
            continue
        print output

    if has_error:
        sys.exit(const.FAILURE)


def list_version_object_on_node(mds_host, mds_ports, result_queue, list_count):
    """
    Callback for multi-threads to list on each node
    """
    if result_queue.qsize() >= list_count:
        return
    cmd = "query_version_objects.py -L -p %s -l" % (mds_ports)
    logging.info("Will run '%s' on %s", cmd, mds_host)
    status, output = common.ssh(mds_host, cmd)

    for verobj in output.split("\n"):
        if result_queue.qsize() >= list_count:
            return
        else:
            result_queue.put([status, verobj])


def list_system_version_objects(list_count, force_mode = False):
    """
    List version objects from all system MDSes.
    """
    has_error = False
    result_queue = Queue()

    run_threads_on_mdshost(list_version_object_on_node, result_queue, \
        force_mode, False, list_count)

    while not result_queue.empty():
        status, output = result_queue.get()
        if status != const.SUCCESS:
            has_error = True
        if output == '':
            continue
        print output

    if has_error:
        sys.exit(const.FAILURE)


def list_tenants_verobj_on_node(mds_host, mds_ports, result_queue, list_count):
    """
    Callback for multi-threads to list on each node
    """
    for mds_port in mds_ports.split(","):
        cmd = CMD_SHOW_MDSDIR % (mds_port)
        logging.info("Will run '%s' on %s", cmd, mds_host)
        status, mds_dir = common.ssh(mds_host, cmd)
        cmd = CMD_QUERY_TENANT_LIST % (mds_port)
        logging.info("Will run '%s' on %s", cmd, mds_host)
        status, output = common.ssh(mds_host, cmd)
        tenants_list = output.split("\n")

        for i in range(0, len(tenants_list)):
            if result_queue.qsize() >= list_count:
                return

            cmd = CMD_LL_BDB % (mds_dir, tenants_list[i])
            logging.info("Will run '%s' on %s", cmd, mds_host)
            status, output = common.ssh(mds_host, cmd)
            output = output.split()[4]

            if int(output) > DB_SIZE:
                result_queue.put([const.SUCCESS, mds_host+":"+\
                    mds_port+":tenant:"+tenants_list[i]])
            else:
                cmd = CMD_COUNT_VEROBJ % (mds_port, tenants_list[i])
                status, output = common.ssh(mds_host, cmd)
                if status != const.SUCCESS:
                    result = "Failed to run '%s', output: %s" % (cmd, output)
                    result_queue.put(status, result)
                else:
                    try:
                        count = int(output)
                        if count > 0:
                            result_queue.put([const.SUCCESS, mds_host+":"+\
                                mds_port+":tenant:"+tenants_list[i]])
                    except ValueError:
                        result = "Failed to conver '%s' to int" % output
                        result_queue.put(const.FAILURE, result)


def list_system_tenants_verobj(list_count, force_mode = False):
    """
    List tenants contain version objects from all system MDSes.
    """
    result_queue = Queue()

    run_threads_on_mdshost(list_tenants_verobj_on_node, result_queue, \
        force_mode, False, list_count)

    print_result(result_queue)


def list_local_tenants_verobj(list_count, force_mode):
    """
    List tenants which contains version objects only from local master MDS of all mds_ports
    """
    result_queue = Queue()

    run_threads_on_mdshost(list_tenants_verobj_on_node, result_queue, \
        force_mode, True, list_count)
    
    print_result(result_queue)


def judge_tenants_verobj_on_node(mds_host, mds_ports, result_queue):
    """
    Callback for multi-threads to judge on each node
    """
    if not result_queue.empty():
        return

    for mds_port in mds_ports.split(","):
        cmd = CMD_SHOW_MDSDIR % (mds_port)
        logging.info("Will run '%s' on %s", cmd, mds_host)
        status, mds_dir = common.ssh(mds_host, cmd)
        cmd = CMD_QUERY_TENANT_LIST % (mds_port)
        logging.info("Will run '%s' on %s", cmd, mds_host)
        status, output = common.ssh(mds_host, cmd)
        tenants_list = output.split("\n")

        for i in range(0, len(tenants_list)):
            cmd = CMD_LL_BDB % (mds_dir, tenants_list[i])
            logging.info("Will run '%s' on %s", cmd, mds_host)
            status, output = common.ssh(mds_host, cmd)
            output = output.split()[4]
            if int(output) > DB_SIZE:
                result_queue.put(True)
                return
            else:
                cmd = CMD_COUNT_VEROBJ % (mds_port, tenants_list[i])
                status, output = common.ssh(mds_host, cmd)
                if status != const.SUCCESS:
                    result = "Failed to run '%s', output: %s" % (cmd, output)
                    print result
                    return
                else:
                    try:
                        count = int(output)
                        if count > 0:
                            result_queue.put(True)
                            return
                    except ValueError:
                        result = "Failed to conver '%s' to int" % output
                        print result
    return


def judge_system_tenants_verobj(force_mode = False):
    """
    List tenants contain version objects from all system MDSes.
    """
    result_queue = Queue()

    run_threads_on_mdshost(judge_tenants_verobj_on_node, result_queue, \
        force_mode, False)

    if result_queue.empty():
        print "no"
    else:
        print "yes" 
            

def judge_local_tenants_verobj(force_mode = False):
    """
    List tenants which contains version objects only from local master MDS of all mds_ports
    """
    result_queue = Queue()

    run_threads_on_mdshost(judge_tenants_verobj_on_node, result_queue, \
        force_mode, True)

    if result_queue.empty():
        print "no"
    else:
        print "yes" 


def count_verobj_for_port_tenat(mds_port, tenant, result_queue):
    """
    Callback for multi threads to count on specific master MDS with specific tenant.
    """
    count = 0
    result = ""
    cmd = CMD_COUNT_VEROBJ % (mds_port, tenant)
    status, output = commands.getstatusoutput(cmd)
    if status != const.SUCCESS:
        result = "Failed to run '%s', output: %s" % (cmd, output)
    else:
        try:
            count = int(output)
        except ValueError:
            result = "Failed to conver '%s' to int" % output

    result_queue.put([status, count , result])

def count_local_version_objects(mds_ports):
    """
    Count the version objects only from local MDS given mds_ports, not given mds_ports will search all ports by default
    Input example
        10401
        10401,10403
        10401,10403,10405
    """
    has_error = False
    verobj_count = 0
    result_queue = Queue()

    if mds_ports == None:
        run_threads_on_mdshost_bytenant(count_verobj_for_port_tenat, \
            result_queue, 1, False) # 1 is flag
    else:
        threads = []
        try:
            for mds_port in mds_ports.split(","):
                tenants = get_tenant_list(mds_port)
                for tenant in tenants:
                    threads.append(threading.Thread\
                        (target=count_verobj_for_port_tenat, \
                        args=(mds_port, tenant, result_queue)))
        except AtmosError, msg:
            exit_with_msg(msg)

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    while not result_queue.empty():
        status, count, errmsg = result_queue.get()
        if status != const.SUCCESS:
            has_error = True
            logging.error(errmsg)
        else:
            verobj_count += count

    if has_error:
        msg = "Error occurred during query. The result %d may be inaccurate." \
        % verobj_count
        print verobj_count
        print msg
        sys.exit(RET_RESULT_INACCURATE)

    print verobj_count   

def count_version_object_on_node(mds_host, mds_ports, result_queue):
    """
    Callback for multi-thread to trigger counting on each node.
    """
    cmd = "query_version_objects.py -L -p %s" % (mds_ports)
    status, output = common.ssh(mds_host, cmd)
    logging.info("Will run '%s' on %s", cmd, mds_host)
    result_queue.put([status, output])

def count_version_objects_in_system(force_mode = False):
    """
    Count version object number for all master mds
    """
    errmsgs = []
    retval = 0
    retcode = const.SUCCESS
    result_queue = Queue()

    run_threads_on_mdshost(count_version_object_on_node, result_queue, \
        force_mode, False)

    while not result_queue.empty():
        status, output = result_queue.get()
        if status == const.SUCCESS:
            try:
                retval += int(output)
            except ValueError, msg:
                retcode = const.FAILURE
                errmsgs.append(output) 
        elif status >> 8 == RET_RESULT_INACCURATE:
            retcode = const.FAILURE
            try:
                count, msg = output.split("\n")
                retval += int(count)
                errmsgs.append(msg)
            except ValueError, msg:
                errmsgs.append(str(msg) + ":" +output)
        else:
            retcode = const.FAILURE
            errmsgs.append(output)

    return retcode, str(retval), errmsgs


def count_system_version_objects(force_mode = False):
    """
    Count version objects number in the system
    """
    status, verobj_num, error_msgs = count_version_objects_in_system(force_mode)
    print verobj_num

    # if there are any errors, print the error messages
    if status != const.SUCCESS:
        for msg in error_msgs:
            logging.error(msg)
        print ERR_ALERT_MESSAGE % verobj_num
        logging.error(ERR_ALERT_MESSAGE, verobj_num)
        return RET_RESULT_INACCURATE
    return const.SUCCESS

def main():
    """
    Main
    """
    list_mode = False
    local_mode = False
    scope_system = False
    mds_ports = None
    list_count = MAX_LIST_COUNT
    force_mode = False
    version_mode = False
    list_tenant_mode = False

    #Get command options
    opts = getopts()
    for option, value in opts:
        if option in ("-h", "--help"):
            usage()
            sys.exit(const.SUCCESS)
        elif option in ("-q", "--query"):
            version_mode = True
        elif option in ("-t", "--tenant"):
            list_tenant_mode = True
        elif option in ("-L", "--local"):
            local_mode = True
        elif option in ("-p", "--port"):
            mds_ports = value
        elif option in ("-l", "--list"):
            list_mode = True
        elif option in ("-c", "--count"):
            try:
                list_count = int(value)
            except ValueError:
                usage()
                sys.exit(const.FAILURE)
        elif option in ("-a", "--system"):
            scope_system = True
        elif option in ("-F", "--force"):
            force_mode = True
        else:
            usage()
            sys.exit(const.FAILURE)

    log.set_logging(LOG_FILE)

    if local_mode == False and scope_system == False and version_mode == False:
        usage()
        sys.exit(const.FAILURE)

    ret = const.SUCCESS
    try:
        if local_mode:
            if version_mode:
                if list_tenant_mode:
                    list_local_tenants_verobj(list_count, force_mode)
                else:
                    judge_local_tenants_verobj(force_mode)
            elif list_mode:
                list_local_version_objects(list_count, mds_ports)
            else:
                count_local_version_objects(mds_ports)
        else:
            if version_mode:
                if list_tenant_mode:
                    list_system_tenants_verobj(list_count, force_mode)
                else:
                    judge_system_tenants_verobj(force_mode)
            elif list_mode:
                list_system_version_objects(list_count, force_mode)
            else:
                count_system_version_objects(force_mode)

    except KeyboardInterrupt:
        sys.exit(const.FAILURE)
    except Exception, msg:
        exit_with_msg(msg)

    sys.exit(ret)

if __name__ == "__main__":
    main()
