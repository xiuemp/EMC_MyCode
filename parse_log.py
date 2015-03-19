#!/usr/bin/python

import os
import getopt
import sys
import time

SUCCESS = 0
FAILURE = 1

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
        opts, _ = getopt.getopt(sys.argv[1:], "hf:t:",
            ["help", "file", "tps"])
        return opts
    except getopt.GetoptError:
        usage()
        sys.exit(FAILURE)

def time_convert(time_str):
    """
    Convert time string in log "[14/Dec/2014:09:01:43 +0000]" to seconds in order to compare
    """


def parse(path):
    """
    Parse log to format files of GET & POST 
    """
    #Control input number
    time_count_GET = 0
    time_last_GET = 20000
    time_count_POST = 0
    time_last_POST = 20000
    time_count_DELETE = 0
    time_last_DELETE = 20000

    latency_threshold = "600"
    timeout_count_GET = 0
    timeout_count_POST = 0
    timeout_count_DELETE = 0
    total_count_GET = 0
    total_count_POST = 0
    total_count_DELETE = 0

    case = os.path.basename(path)[11:]
    fin = open(path, "r")

    fout_GET = open("./"+case+"_GET", "w")
    fout_POST = open("./"+case+"_POST", "w")
    fout_DELETE = open("./"+case+"_DELETE", "w")

    fout_GET.write(case+"_GET:size(MB):latency(s)\n")
    fout_POST.write(case+"_POST:size(MB):latency(s)\n")
    fout_DELETE.write(case+"_DELETE:time(clock-hour):latency(s)\n")

    while True:    
        line = fin.readline().strip('\r\n')
        if line:
            info = line.split()

            if info[8][1:] == "GET":
#                if time_count_GET == 0:
#                    time_last_GET = time.mktime(time.strptime(info[1][1:], "%d/%b/%Y:%H:%M:%S"))
#                    time_count_GET += 1
#               elif time.mktime(time.strptime(info[1][1:], "%d/%b/%Y:%H:%M:%S")) > time_last_GET + 3*3600:
#                    continue
                if time_count_GET >= time_last_GET:
                    continue
                else: 
                    time_count_GET += 1
                    total_count_GET += 1
                    if float(info[18]) > float(latency_threshold)*1000*1000:
#                        info[18] = str(float(latency_threshold)*1000*1000)
                        timeout_count_GET += 1
                    else:
                        fout_GET.write(info[8][1:]+":"+str(float(info[17])/1024/1024)+":"+str(float(info[18])/1000/1000)+"\n")

            elif info[8][1:] == "POST":
#                if time_count_POST == 0:
#                    time_last_POST = time.mktime(time.strptime(info[1][1:], "%d/%b/%Y:%H:%M:%S"))
#                    time_count_POST += 1
#                elif time.mktime(time.strptime(info[1][1:], "%d/%b/%Y:%H:%M:%S")) > time_last_POST + 3*3600:
#                    continue
                if time_count_POST >= time_last_POST:
                    continue
                else:
                    time_count_POST += 1
                    total_count_POST += 1
                    if float(info[18]) > float(latency_threshold)*1000*1000:
#                        info[18] = str(float(latency_threshold)*1000*1000)
                        timeout_count_POST += 1
                    else:
                        fout_POST.write(info[8][1:]+":"+str(float(info[16])/1024/1024)+":"+str(float(info[18])/1000/1000)+"\n")
            
            elif info[8][1:] == "DELETE":
                if time_count_DELETE >= time_last_DELETE:
                    continue
                else:
                    time_count_DELETE += 1
                    total_count_DELETE += 1
                    if float(info[18]) > float(latency_threshold)*1000*1000:
                        timeout_count_DELETE += 1
                    else:
                        time_struct = time.strptime(info[1][1:], "%d/%b/%Y:%H:%M:%S")
                        time_hours = time_struct.tm_hour + time_struct.tm_min/60.0 + time_struct.tm_sec/3600.0
                        fout_DELETE.write(info[8][1:]+":"+str(time_hours)+":"+str(float(info[18])/1000/1000)+"\n")

            else:
                continue
        else:
            break

    fin.close()
    fout_GET.write("#total#:"+str(timeout_count_GET)+"/"+str(total_count_GET))
    fout_GET.close()
    fout_POST.write("#total#:"+str(timeout_count_POST)+"/"+str(total_count_POST))
    fout_POST.close()
    fout_DELETE.write("#total#:"+str(timeout_count_DELETE)+"/"+str(total_count_DELETE))
    fout_DELETE.close()

    print "GET: total: "+str(total_count_GET)+" timeout: "+str(timeout_count_GET)+"\n"
    print "POST: total: "+str(total_count_POST)+" timeout: "+str(timeout_count_POST)+"\n"
    print "DELETE: total: "+str(total_count_DELETE)+" timeout: "+str(timeout_count_DELETE)+"\n"

def tps(path):
    """
    calculate throughput data from log and then export to file *_tps
    """
    TOTAL = 20000
    INTERVAL = 1
    start_time = None
    tps_send_lst = [0]*TOTAL #every seconds
    tps_receive_lst = [0]*TOTAL #every seconds
    case = os.path.basename(path)[11:]

    f_in = open(path, "r")
    f_out = open("./"+case+"_tps", "w")

    while True:
        line = f_in.readline().strip("\r\n")
        if line:
            info = line.split()
            if info[8][1:] == "GET" or info[8][1:] == "POST":
                time_tmp = time.mktime(time.strptime(info[1][1:], "%d/%b/%Y:%H:%M:%S"))
                if start_time == None:
                    start_time = time_tmp

                elif time_tmp - start_time < TOTAL:
                    send = float(info[16])/1024/1024
                    receive = float(info[17])/1024/1024
                    latency = float(info[18])/1000/1000

                    if latency <= INTERVAL:
                        tps_send_lst[int(time_tmp-start_time)] += send
                        tps_receive_lst[int(time_tmp-start_time)] += receive
                    else:
                        shares = int(latency/INTERVAL)
                        share_send = send/latency*INTERVAL
                        share_receive = receive/latency*INTERVAL
                        rest_send = send%share_send
                        rest_receive = receive%share_receive

                        count = 0
                        for i in range(shares):
                            if int(time_tmp-start_time)+i >= TOTAL:
                                continue
                            count = i
                            tps_send_lst[int(time_tmp-start_time)+i] += share_send
                            tps_receive_lst[int(time_tmp-start_time)+i] += share_receive
                        count += 1
                        if int(time_tmp-start_time)+count >= TOTAL:
                            continue
                        tps_send_lst[int(time_tmp-start_time)+count] += rest_send
                        tps_receive_lst[int(time_tmp-start_time)+count] += rest_receive             

                else:
                    continue
            else:
                continue

        else:
            break

    f_out.write(case+"_TPS:time:TPS(MB/s):"+str(start_time)+":"+str(INTERVAL)+"\n")
    for i in range(TOTAL):
        f_out.write(str(tps_send_lst[i])+":"+str(tps_receive_lst[i])+"\n")

    f_in.close()
    f_out.close()
#    return start_time, tps_send_lst, tps_receive_lst

def main():
    """
    the Main
    """
    file_mode = False
    tps_mode = False
    opts = getops()
    for option, value in opts:
        if option in ("-h", "--help"):
            usage()
            sys.exit(SUCCESS)
        elif option in ("-f", "--file"):
            file_mode = True
            parse(value)
        elif option in ("-t", "--tps"):
            tps_mode = True
            tps(value)
        else:
            usage()
            sys.exit(FAILURE)
    try:
        pass
    except KeyboardInterrupt:
        sys.exit(FAILURE)

    sys.exit(SUCCESS)

if __name__ == "__main__":
    main() 
