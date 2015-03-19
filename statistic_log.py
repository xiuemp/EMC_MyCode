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
        opts, _ = getopt.getopt(sys.argv[1:], "hf:c:t:d:o:r:i:s:l:",
            ["help", "file", "category", "type", "dimension", "original", \
            "refine", "time_range", "size_range", "latency_range"])
        return opts
    except getopt.GetoptError:
        usage()
        sys.exit(FAILURE)

def time_convert(seconds):
    """
    Convert seconds to "2014-12-21:09:01:43" String in order to display
    """
    return time.strftime("%Y-%m-%d:%H:%M:%S", time.localtime(seconds))

def statistic(path, category_name, type_name):
    """
    Input files as:
    titile:abscissa_1:abscissa_2:ordinate
    category:type:time:size:latency
    .
    .
    .
    #total#:n
    time in seconds, size is MB, latency is s 

    Export a Statistical Report dipicting involved category, type
    """
    result_set = []
    f_in = open(path, "r")
    total_count = 0
    FLOAT_MAX = sys.float_info[0]
    time_min = FLOAT_MAX
    time_max = -FLOAT_MAX
    size_min = FLOAT_MAX
    size_max = -FLOAT_MAX
    latency_min = FLOAT_MAX
    latency_max = -FLOAT_MAX
    match_str = category_name+":"+type_name

    file_name = os.path.splitext(os.path.basename(path))[0]
    result_set.append(file_name+":"+category_name+":"+type_name)
    f_in.readline()

    while True:    
        line = f_in.readline().strip('\r\n')
        if line:
            if line[0] == "#":
                break
            elif match_str in line:
                total_count += 1
                info = line.split(":")
                latency_min = min(latency_min, float(info[4]))
                latency_max = max(latency_max, float(info[4]))
                time_min = min(time_min, int(info[2]))
                time_max = max(time_max, int(info[2]))
                if not type_name == 'DELETE':
                    size_min = min(size_min, float(info[3]))
                    size_max = max(size_max, float(info[3]))
                else:
                    continue
        else:
            break

    f_in.close()
    result_set.append("time:"+str(time_min)+":"+str(time_max))
    result_set.append("size:"+str(size_min)+":"+str(size_max))
    result_set.append("latency:"+str(latency_min)+":"+str(latency_max))
    result_set.append("total:"+str(total_count))

    return result_set

def original(path):
    """
    Input files as:
    titile:abscissa_1:abscissa_2:ordinate
    category:type:time:size:latency
    .
    .
    .
    #total#:n
    time in seconds, size is MB, latency is s 

    Export the Report of category, type, dimension
    """
    f_in = open(path, "r")

    f_in.close()

def get_index_suffix(dimension_name):
    """
    Return the corresponding lable & index for dimension
    """
    if dimension_name == "latency":
        return int(4),"(s)"
    elif dimension_name == "size":
        return int(3),"(MB)"
    elif dimension_name == "time":
        return int(2),""
    else:
        return "ERROR","ERROR"

def judge_if_record_valid(pd_line, time_range, size_range, latency_range):
    """
    Input a .pd file data line & return if this line is valid by the restrict
    """
    time_flag = True
    size_flag = True
    latency_flag = True
    time_index,_ = get_index_suffix("time")
    size_index,_ = get_index_suffix("size")
    latency_index,_ = get_index_suffix("latency")

    if time_range == '':
        time_flag = False
    if size_range == '':
        size_flag = False
    if latency_range == '':
        latency_flag = False

    info = pd_line.split(':')
    if time_flag:
        time_min,time_max = time_range.split(',')
        if int(info[time_index]) < int(time_min) or int(info[time_index]) > int(time_max):
            return False
    if size_flag:
        size_min,size_max = size_range.split(',')
        if float(info[size_index]) < float(size_min) or float(info[size_index]) > float(size_max):
            return False
    if latency_flag:
        latency_min,latency_max = latency_range.split(',')
        if float(info[latency_index]) < float(latency_min) or float(info[latency_index]) > float(latency_max):
            return False

    return True



def refine(source_path, category, type_, dimension, time_range='', size_range='', latency_range='', output_path='./'):
    """
    process the primary .pd file & export the .pd.refine for plot.py drawing:
    title:abscissa:ordinate
    label:x:y
    .
    .
    .
    #total#:n
    """
    total_count = 0
    file_name = os.path.splitext(os.path.basename(source_path))[0]
    diff = "."+category+"."+type_+"."+dimension+"("+time_range+"_"+size_range+"_"+latency_range+")"
    f_in = open(source_path, "r")
    if not output_path[-1] == '/':
        output_path += '/'

    output_file_name = file_name+diff+".refine"
    f_out = open(output_path+output_file_name, "w")
    f_in.readline()
    select_str = category+":"+type_
    x_label,y_label = dimension.split(',')

    x_index,x_suffix = get_index_suffix(x_label)
    y_index,y_suffix = get_index_suffix(y_label)

    if category == "non":
        t_suffix = "(common)"
    else:
        t_suffix = "(specific)"

    title = file_name+"_"+type_+t_suffix
    f_out.write(title+":"+x_label+x_suffix+":"+y_label+y_suffix+"\n")

    #process & select data
    while True:
        line = f_in.readline().strip("\r\n")
        if line:
            if line[0] == "#":
                f_out.write("#total#:"+str(total_count))
                break
            elif select_str in line:
                if judge_if_record_valid(line, time_range, size_range, latency_range):
                    total_count += 1
                    info = line.split(':')
                    x,y = info[x_index],info[y_index]
                    f_out.write(type_+":"+x+":"+y+"\n")  # now write to file
            else:
                continue
        else:
            break
    f_in.close()
    f_out.close()

    return output_file_name


def main():
    """
    the Main
    """
    original_mode = False
    statistic_mode = False
    refine_mode = False

    latency_flag = False
    time_range = ""
    size_range = ""
    latency_range = ""

    category_flag = False
    type_flag = False

    category = ""
    type_ = ""
    dimension = ""
    source_path = ""
    opts = getops()
    for option, value in opts:
        if option in ("-h", "--help"):
            usage()
            sys.exit(SUCCESS)
        if option in ("-f", "--file"):
            statistic_mode = True
            source_path = value
        elif option in ("-c", "--category"):
            category_flag = True
            category = value
        elif option in ("-t", "--type"):
            type_flag = True
            type_ = value
        elif option in ("-o", "--original"):
            original_mode = True
            source_path = value
        elif option in ("-r", "--refine"):
            refine_mode = True
            source_path = value
        elif option in ("-d", "--dimension"):
            dimension = value
        elif option in ("-i", "--time_range"):
            time_range = value
        elif option in ("-s", "--size_range"):
            size_range = value
        elif option in ("-l", "--latency_range"):
            latency_flag =  True
            latency_range = value
        else:
            usage()
            sys.exit(FAILURE)
    if statistic_mode:
        if not category_flag:
            print "category name not found: please use -c to input it"
            sys.exit(FAILURE)
        if not type_flag:
            print "type name not found: please use -t to input it"
            sys.exit(FAILURE)

    try:
        if statistic_mode:
            print "\n".join(statistic(source_path, category, type_))
        elif refine_mode:
            refine(source_path, category, type_, dimension, time_range, size_range, latency_range)
        else:
            original(source_path)

    except KeyboardInterrupt:
        sys.exit(FAILURE)

    sys.exit(SUCCESS)

if __name__ == "__main__":
    main() 
