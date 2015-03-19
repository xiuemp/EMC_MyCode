#!/usr/bin/python

import sys
import getopt
import commands
import re

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
        opts, _ = getopt.getopt(sys.argv[1:], "hv:a:m:t:c:s:j:u:n:e:",
            ["help", "version", "hardware", "merge_rmg_node", "merge_tenant_sub_uid"\
            , "capacity", "size", "judge", "unit_mkb", "unit_tgmk", "excel"])
        return opts
    except getopt.GetoptError:
        usage()
        sys.exit(FAILURE)

def reduce_version(path):
    """
    cut system_version number from 2.1.6.0.84364 to 2.1.6
    """
    f_in = open(path, "r")
    f_out = open(path.replace(".pd", ".refine"), "w")

    line = f_in.readline().strip('\r\n')
    f_out.write(line+'\n')

    while True:
        line = f_in.readline().strip('\r\n')
        if line:
            info = line.split(':')
            if info[0][0] == '#':
                f_out.write(line)
                break

            else:
                versions = info[1].split('.')
                if len(versions) < 3:
                    ver = info[1]
                else:
                    ver = versions[0]+'.'+versions[1]+'.'+versions[2]
                f_out.write(info[0]+':'+ver+'\n')
        else:
            break

    f_in.close()
    f_out.close()

def reduce_hardware(path):
    """
    cut hardware_mode from gen2-ws-3tb-120 to gen2
    """
    f_in = open(path, "r")
    f_out = open(path.replace(".pd", ".refine"), "w")

    line = f_in.readline().strip('\r\n')
    f_out.write(line+'\n')

    while True:
        line = f_in.readline().strip('\r\n')
        if line:
            info = line.split(':')
            if info[0][0] == '#':
                f_out.write(line)
                break

            else:
                versions = info[1].split('-')
                if len(versions) < 3:
                    ver = info[1].split('_')[0]
                else:
                    ver = versions[0]
                f_out.write(info[0]+':'+ver+'\n')
        else:
            break

    f_in.close()
    f_out.close()

def merge_rmg_node(dir_path):
    """
    Merge the rmg_count.pd & config_node_count.pd to rmg_node.pd.refine
    """
    max_node = 0

    if dir_path[-1] == "/":
        dir_path = dir_path[:-1]

    f_rmg = open(dir_path+"/rmg_count.pd", "r")
    f_node = open(dir_path+"/config_node_count.pd", "r")
    f_out = open(dir_path+"/rmg_node.refine", "w")

    f_rmg.readline()
    f_node.readline()
    f_out.write("rmg-node relation:RMG:nodes\n")

    while True:
        line_rmg = f_rmg.readline().strip('\r\n')
        line_node = f_node.readline().strip('\r\n')
        if line_rmg and line_node:
            info_rmg = line_rmg.split(":")
            info_node = line_node.split(":")
            if not info_rmg[0] == info_node[0]:
                print " error in Match pls check the files"
                return
            elif info_rmg[0][0] == "#":
                f_out.write(line_rmg+':'+str(max_node))
            else:
                if info_node[1] != 'N/A':
                    f_out.write("RMG-Nodes:"+info_rmg[1]+":"+info_node[1]+"\n")
                    if int(info_node[1]) > max_node:
                        max_node = int(info_node[1])
                else:
                    continue
                
        else:
            break

    f_rmg.close()
    f_node.close()
    f_out.close()


def merge_tenant_sub_uid(path):
    """
    Merge tenant_count.pd subtenant_count.pd uid_count.pd into tenant_sub_uid.pd.refine for 3d scatter ploting
    """
    if path[-1] == '/':
        path = path[:-1]
    f_tenant = open(path+'/tenant_count.pd', 'r')
    f_subtenant = open(path+'/subtenant_count.pd', 'r')
    f_uid = open(path+'/uid_count.pd', 'r')
    f_out = open(path+'/tenant_sub_uid.refine', 'w')

    f_tenant.readline()
    f_subtenant.readline()
    f_uid.readline()
    f_out.write("tenant_subtenant_uid relation:tenants:subtenants:uids\n")

    while True:
        line_tenant = f_tenant.readline().strip('\r\n')
        line_subtenant = f_subtenant.readline().strip('\r\n')
        line_uid = f_uid.readline().strip('\r\n')
        if line_tenant and line_subtenant and line_uid:
            info_tenant = line_tenant.split(':')
            info_subtenant = line_subtenant.split(':')
            info_uid = line_uid.split(':')
            if info_tenant[0] != info_subtenant[0] or info_subtenant[0] != info_uid[0]:
                print "error in Match pls check files"
                return
            elif info_tenant[0][0] == '#':
                continue
            elif info_tenant[1] == 'N/A':
                continue
            else:
                f_out.write(info_tenant[1]+':'+info_subtenant[1]+':'+info_uid[1]+'\n')
        else:
            break

    f_tenant.close()
    f_subtenant.close()
    f_uid.close()
    f_out.close()

def merge_capacity(dir_path):
    """
    Merge the total_capacity.pd & used_capacity.pd to capacity.pd.refine
    """
    if dir_path[-1] == "/":
        dir_path = dir_path[:-1]

    f_total = open(dir_path+"/total_capacity.pd", "r")
    f_used = open(dir_path+"/used_capacity.pd", "r")
    f_out = open(dir_path+"/capacity.refine", "w")

    f_total.readline()
    f_used.readline()
    f_out.write("Capacity Survey:Customer:Size(TB)\n")

    while True:
        line_total = f_total.readline().strip('\r\n')
        line_used = f_used.readline().strip('\r\n')
        if line_total and line_used:
            info_total = line_total.split(":")
            info_used = line_used.split(":")
            if not info_total[0] == info_used[0]:
                print " error in Match pls check the files"
                return
            elif info_total[0][0] == "#":
                f_out.write(line_total)
            else:
                if info_total[1] != 'N/A':
                    f_out.write(info_total[0]+":"+str(float(info_total[1][:-2])\
                        /1000)+":"+str(float(info_used[1][:-2])/1000)+"\n")
                else:
                    continue
                
        else:
            break

    f_total.close()
    f_used.close()
    f_out.close()


def size(dir_path):
    """
    Merge object_count.pd average_size.pd real_size.pd metadata_size.pd total_size.pd to size.pd.refine
    """
    if dir_path[-1] == "/":
        dir_path = dir_path[:-1]

    f_object = open(dir_path+'/object_count.pd', 'r')
    f_average = open(dir_path+'/average_size.pd', 'r')
    f_real = open(dir_path+'/real_size.pd', 'r')
    f_metadata = open(dir_path+'/metadata_size.pd', 'r')
    f_total = open(dir_path+'/total_size.pd', 'r')
    f_out = open(dir_path+'/size.refine', 'w')

    f_object.readline()
    f_average.readline()
    f_real.readline()
    f_metadata.readline()
    f_total.readline()
    f_out.write("object count:customer:object_count:average_size:real_size:metadata_size:total_size\n")

    while True:
        line_object = f_object.readline().strip('\r\n')
        line_average = f_average.readline().strip('\r\n')
        line_real = f_real.readline().strip('\r\n')
        line_metadata = f_metadata.readline().strip('\r\n')
        line_total = f_total.readline().strip('\r\n')
        if line_object:
            info_object = line_object.split(":")
            info_average = line_average.split(":")
            info_real = line_real.split(":")
            info_metadata = line_metadata.split(":")
            info_total = line_total.split(":")

            if info_object[0] != info_average[0] or info_real[0] != info_metadata[0]:
                print "Error in Match, pls check file"
            elif info_object[0][0] == "#":
                f_out.write(line_object)
            elif info_object[1] == "N/A":
                continue
            else:
                f_out.write(info_object[0]+":"+info_object[1]+":"+info_average[1]\
                    +":"+info_real[1]+":"+info_metadata[1]+":"+info_total[1]+'\n')
        else:
            break

    f_object.close()
    f_average.close()
    f_real.close()
    f_metadata.close()
    f_total.close()
    f_out.close()


def judge(path):
    """
    if entry > 0 -> Yes; 0 -> No, save file to path_judge.refine
    """
    f_in = open(path, 'r')
    f_out = open(path.replace(".pd", ".judge.refine"), 'w')
    line = f_in.readline().strip('\r\n')
    f_out.write(line+'\n')

    while  True:
        line = f_in.readline().strip('\r\n')
        if line:
            info = line.split(':')
            if info[0][0] == '#':
                f_out.write(line)
            elif info[1] == 'N/A':
                continue
            else:
                flag = 'No'
                if int(info[1]) > 0:
                    flag = 'Yes'
                f_out.write(info[0]+":"+flag+'\n')
        else:
            break
    f_in.close()
    f_out.close()

def unit_mkb(path):
    """
    Transfer size 'M, K, B' to 'M'
    """
    f_in = open(path, 'r')
    f_out = open(path.replace(".pd", ".refine"), 'w')
    line = f_in.readline().strip('\r\n')
    f_out.write(line+'customer:size(MB)\n')

    while  True:
        line = f_in.readline().strip('\r\n')
        if line:
            info = line.split(':')
            if info[0][0] == '#':
                f_out.write(line)
            elif info[1] == 'N/A':
                continue
            else:
                if info[1][-1] == 'M':
                    f_out.write(info[0]+":"+info[1][:-1]+'\n')
                elif info[1][-1] == 'K':
                    f_out.write(info[0]+":"+str(float(info[1][:-1])/1000)+'\n')
                elif info[1][-1] == 'B':
                    f_out.write(info[0]+":"+str(float(info[1][:-1])/1000/1000)+'\n')
                else:
                    print "Erro unit, please check file"
        else:
            break
    f_in.close()
    f_out.close()

def unit_tgmk(path):
    """
    Transfer size 'T, G, M, K' to 'T'
    """
    f_in = open(path, 'r')
    f_out = open(path.replace(".pd", ".refine"), 'w')
    line = f_in.readline().strip('\r\n')
    f_out.write(line+'customer:size(TB)\n')

    while  True:
        line = f_in.readline().strip('\r\n')
        if line:
            info = line.split(':')
            if info[0][0] == '#':
                f_out.write(line)
            elif info[1] == 'N/A':
                continue
            else:
                if info[1][-1] == 'T':
                    f_out.write(info[0]+":"+info[1][:-1]+'\n')
                elif info[1][-1] == 'G':
                    f_out.write(info[0]+":"+str(float(info[1][:-1])/1000)+'\n')
                elif info[1][-1] == 'M':
                    f_out.write(info[0]+":"+str(float(info[1][:-1])/1000/1000)+'\n')
                elif info[1][-1] == 'K':
                    f_out.write(info[0]+":"+str(float(info[1][:-1])/1000/1000/1000)+'\n')
                else:
                    print "Erro unit, please check file"
        else:
            break
    f_in.close()
    f_out.close()

def excel(dir_path):
    """
    Export all .pd data to excel form
    """
    COMMAND_GET_ALL_PD = "ls -l %s/*.pd | awk {'print $9'}"
    item_path = []
    property_name = []
    customer_name = []
    property_list = []
    if dir_path[-1] == '/':
        dir_path = dir_path[:-1]
    f_in = []
    f_out = open(dir_path+'/excel', 'w')

    status, output = commands.getstatusoutput(COMMAND_GET_ALL_PD % dir_path)
    if status:
        print "Error at "+ COMMAND_GET_ALL_PD
    else:
        for item in output.split('\n'):
            item_path.append(item)

    for f_tmp in item_path:
        f_in.append(open(f_tmp, 'r'))
        property_name.append(f_tmp.split('/')[-1].replace('.pd', ''))
    f_out.write('customer\t'+'\t'.join(property_name)+'\n')

    for i in range(len(f_in)):
        f_in[i].readline()
    while True:
        first_tmp = f_in[0].readline()
        if first_tmp:
            first_tmp = first_tmp.strip('\r\n')
            if first_tmp[0] == "#":
                f_out.write(first_tmp)
                break

            name_tmp = first_tmp.split(':')[0]
            pattern = re.compile(ur"\d*\Z")
            nameset = name_tmp.split("_")
            if len(nameset) >= 3 and pattern.match(nameset[-2]):
                name = name_tmp.replace(nameset[-2], '$').split('$')[0][:-1]
            else:
                name = name_tmp
            customer_name.append(name)

            property_list = []
            for f in f_in[1:]:
                property_list.append(f.readline().split(':')[1].strip('\n'))

            f_out.write(name+'\t'+first_tmp.split(":")[1]+'\t'+'\t'.join(property_list)+'\n')
        else:
            break
    print len(customer_name)

    for i in range(len(f_in)):
        f_in[i].close()
    f_out.close()




def main():
    """
    the Main
    """
    sys_ver_mode = False
    hardware_mode = False
    merge_rmg_node_mode = False
    merge_tenant_sub_uid_mode = False
    capacity_mode = False
    size_mode = False
    judge_mode = False
    unit_mkb_mode = False

    opts = getops()
    for option, value in opts:
        if option in ("-h", "--help"):
            usage()
            sys.exit(SUCCESS)
        elif option in ("-v", "--version"):
            sys_ver_mode = True
            reduce_version(value)
        elif option in ("-a", "--hardware"):
            hardware_mode = True
            reduce_hardware(value)
        elif option in ("-m", "--merge_rmg_node"):
            merge_rmg_node_mode = True
            merge_rmg_node(value)
        elif option in ("-t", "--merge_tenant_sub_uid"):
            merge_tenant_sub_uid_mode = True
            merge_tenant_sub_uid(value)
        elif option in ("-c", "--capacity"):
            capacity_mode = True
            merge_capacity(value)
        elif option in ("-s", "--size"):
            size_mode = True
            size(value)
        elif option in ("-j", "--judge"):
            judge_mode = True
            judge(value)
        elif option in ("-u", "--unit_mkb"):
            unit_mkb_mode = True
            unit_mkb(value)
        elif option in ("-n", "--unit_tgmk"):
            unit_tgmk_mode = True
            unit_tgmk(value)
        elif option in ("-e", "--excel"):
            excel_mode = True
            excel(value)
        else:
            usage()
            sys.exit(FAILURE)

    try:
        pass
    except KeyboardInterrupt:
        sys.exit(FAILURE)

if __name__ == "__main__":
    main() 
