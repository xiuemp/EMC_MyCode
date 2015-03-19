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
        opts, _ = getopt.getopt(sys.argv[1:], "hd:c:",
            ["help", "default", "category"])
        return opts
    except getopt.GetoptError:
        usage()
        sys.exit(FAILURE)

def time_convert(time_str):
    """
    Convert time string in log "14/Dec/2014:09:01:43" to seconds in order to compare
    """
    return time.mktime(time.strptime(time_str, "%d/%b/%Y:%H:%M:%S"))

def parse(source_path, category_name, output_path='', \
    index_time=0, index_type=8, index_sent=16, index_received=17, index_latency=18):
    """
    Parse log to format files as:
    titile:abscissa_1:abscissa_2:ordinate
    category:type:time:size:latency
    .
    .
    .
    #total#:n
    time in seconds, size is MB, latency is s 
    """
    f_in = open(source_path, "r")
    if output_path == '':
        output_path = source_path.replace(" ","_")+".pd"
  
    f_out = open(output_path, "w")

    total_count = 0
    category = ""
    type_ = ""
    time_ = ""
    size = ""
    latency = ""

    file_name = os.path.splitext(os.path.basename(output_path))[0].replace(" ","_")
    f_out.write(file_name+":time:size(MB):latency(s)\n")

    while True:    
        line = f_in.readline().strip('\r\n')
        if line:
            total_count += 1
            if category_name == "":
                category = "non"
            else:
                if category_name in line:
                    category = category_name
                else:
                    category = "non"

            info = line.split()
#            time_ = str(time_convert(info[index_time][1:]))
            time_ = info[index_time]
            latency = str(float(info[index_latency])/1000/1000)
            info_type = info[index_type]
            if info_type[0] == '\"':
                info_type = info_type[1:]
            
            if info_type == "GET":
                type_ = "GET"
                size = str(float(info[index_received])/1024/1024)

            elif info_type == "POST":
                type_ = "POST"
                size = str(float(info[index_sent])/1024/1024)
            
            elif info_type == "DELETE":
                type_ = "DELETE"
                size = ""

            else:
                continue
            f_out.write(category+":"+type_+":"+time_+":"+size+":"+latency+"\n")
        else:
            break
    f_out.write("#total#:"+str(total_count))

    f_in.close()
    f_out.close()

    return output_path


def main():
    """
    the Main
    """
    default_mode = False
    category_flag = False
    category = ""
    source_path = ""
    opts = getops()
    for option, value in opts:
        if option in ("-h", "--help"):
            usage()
            sys.exit(SUCCESS)
        elif option in ("-c", "--category"):
            category_flag = True
            category = value
        elif option in ("-d", "--default"):
            default_mode = True
            source_path = value
        else:
            usage()
            sys.exit(FAILURE)
    if not category_flag:
        print "category name not found: please use -c to input it"
        sys.exit(FAILURE)
    try:
        if default_mode:
            parse(source_path, category)

    except KeyboardInterrupt:
        sys.exit(FAILURE)

    sys.exit(SUCCESS)

if __name__ == "__main__":
    main() 
