#!/usr/bin/python

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import sys
import getopt
import os
import time
#from mpl_toolkits.mplot3d import Axes3D

SUCCESS = 0
FAILURE = 1
PI = 3.141592653589793

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
        opts, _ = getopt.getopt(sys.argv[1:], "hp:s:l:o:d:b:z:",
            ["help", "pie", "scatter", "line", "polar", "dimensions3", "bar", "outpath"])
        return opts
    except getopt.GetoptError:
        usage()
        sys.exit(FAILURE)

def test_data():
    """
    Read data from test file ./GDP.txt
    """
    quants   = []
    labels   = []
    for line in file('/root/Chris/GDP.txt'):
        info = line.split()
        labels.append(info[0])
        quants.append(float(info[1]))

    return quants, labels

def seconds_to_str(seconds):
    """
    Transfer input seconds to formatted string: 2015-02-09 21:16:01
    """
    return time.strftime("%H:%M", time.localtime(seconds))

def pie_explode(labels, target):
    """
    make a piece explode a bit
    """
    explode_lst = []
    for label in labels:
        if label == target:
            explode_lst.append(0.1)
        else:
            explode_lst.append(0)
    return explode_lst

def getdata_from_dic(data_dic):
    """
    retrive quants & labels from given data_dic
    """
    quants   = []
    labels   = []
    chart_dic = {}

    for customer, data in data_dic:
        if data in chart_dic:
            chart_dic[data] += int(1)
        else:
            chart_dic[data] = int(1)
    for label, quant in chart_dic.items():
        labels.append(label)
        quants.append(quant)

    return quants, labels

def getdata_for_pie(path):
    quants   = []
    labels   = []
    heads = []
    data_dic = {}

    f = open(path, "r")
    line = f.readline().strip('\r\n')
    info = line.split(":")
    for item in info:
        heads.append(item)

    while True:
        line = f.readline().strip('\r\n')
        if line:    
            info = line.split(":")
            if info[0] == "#total#":
                heads.append(info[1])
                break

            if info[1] in data_dic:
                data_dic[info[1]] += int(1)
            else:
                data_dic[info[1]] = int(1)
        else:
            break

    for label, quant in data_dic.items():
        labels.append(label)
        quants.append(float(quant))

    return heads, quants, labels

def stick_label(begin, end, bottom, top, x_tip, y_tip):
    """
    Change the x-ordinate's stick label
    """
    #Config label stick 
    if x_tip == '' and y_tip == '':
        return
    ax=plt.gca()  
    if x_tip == 'time':
        array = np.linspace(begin, end, 5)
        ax.set_xticks(array)
        time_stick = []
        for seconds in array:
            time_stick.append(seconds_to_str(int(seconds)))
        ax.set_xticklabels(time_stick) 

    if y_tip == 'time':
        array = np.linspace(bottom, top, 5)
        ax.set_yticks(array)
        time_stick = []
        for seconds in array:
            time_stick.append(seconds_to_str(int(seconds)))
        ax.set_yticklabels(time_stick) 

def scatter(source_path, output_path='./', x_label_tip='', y_label_tip=''):
    """
    Make a scatter chart
    """
    label_lst = []
    x = []
    y = []
    colors = ['red','blue','yellow','green','black',"pink","coral","orange"]
    markers = ['*','+','x','-','^']
    area = 1
    legend = ""
    f = open(source_path, "r")
    text = None
    max_y = 600

    #read title and xlabel, ylabel
    line = f.readline().strip('\r\n')
    info = line.split(":")
    title = info[0]
    xlabel = info[1]
    ylabel = info[2]

    #read data lines in file
    while True:
        line = f.readline().strip('\r\n')
        if line:
            info = line.split(":")
            if info[0] == "#total#":
                text = "total:"+info[1]
                break

            if not info[0] in label_lst:
                label_lst.append(info[0])
                x.append([])
                y.append([])
            label_index = label_lst.index(info[0])
            x[label_index].append(float(info[1]))
            y[label_index].append(float(info[2]))

        else:
            break

    fig = plt.figure()
    plt.title(title+"("+text.replace("_", ":")+")", fontsize = 20)
    plt.xlabel(xlabel, fontsize = 18)
    plt.ylabel(ylabel, fontsize = 18)
    begin = min(x[0])
    end = max(x[0])
    bottom = min(y[0])
    top = max(y[0])
    plt.axis([begin-0.05*(end-begin),end+0.05*(end-begin),bottom,top])    
    # stick the label as special design
    stick_label(begin, end, bottom, top, x_label_tip, y_label_tip)

    for i in range(len(label_lst)):
        plt.scatter(x[i], y[i], s=area, color="yellowgreen", \
            marker=(1,1), label = label_lst[i])
    plt.legend(loc = 'upper left')
    if not output_path[-1] == '/':
        output_path += '/'
    file_name = os.path.splitext(os.path.basename(source_path))[0]
    fig.savefig(output_path+file_name+".png")
#    plt.show()

    #cal average
    average_x = []
    average_y = []
    tmp = 0
    for i in range(len(x)):
        for lst in x[i]:
            tmp += float(lst)
        average_x.append(tmp/len(x[i]))
        tmp = 0
        for lst in y[i]:
            tmp += float(lst)
        average_y.append(tmp/len(y[i]))
        tmp = 0

        print label_lst[i]+": MBs-average: "+str(average_x[i])+ "  latency(s)-average: "+str(average_y[i])

    return file_name+".png"

def pie(data):
    """ 
    Make a pie chart
    """
    colors  = ["pink","lightskyblue","yellow","yellowgreen","coral","orange","lightcoral","gold"]

    heads, quants, labels = getdata_for_pie(data)
    title = heads[0]

    fig = plt.figure(1, figsize=(6,6))
    #expl = pie_explode(labels, '2-way')
    plt.pie(quants, explode=None, colors=colors, labels=labels, \
        autopct='%1.1f%%',pctdistance=0.8)

    plt.title(title, fontsize = 20)
    plt.text(1, 1, "total: "+str(heads[-1]))

    #plt.show()
    fig.savefig(title.replace(" ","_")+".png")

def getdata_for_line(path):

    heads = []
    x = []
    y1 = []
    y2 = []
    count = 0

    f = open(path, "r")
    line = f.readline().strip("\r\n")
    for info in line.split(":"):
        heads.append(info)

    while True:
        line = f.readline().strip("\r\n")
        if line:
            info = line.split(":")
            x.append(count)
            count += 1
            y1.append(float(info[0]))
            y2.append(float(info[0])+float(info[1]))

        else:
            break

    f.close()
    return heads, x, y1, y2

def line(data):
    """
    Make a line chart
    """
    colors = ["blue", "red", "pink", "coral", "orange", "yellow"]
    heads, x, y1, y2 = getdata_for_line(data)

    if True:
        xx = []
        yy1 = []
        yy2 = []
        for i in range(0, len(x), 10):
            xx.append(x[i])
            tmp1 = 0
            tmp2 = 0
            for j in range(i, i+10):
                tmp1 += y1[j]
                tmp2 += y2[j]
            yy1.append(tmp1/10)
            yy2.append(tmp2/10)


    fig = plt.figure()
    plt.plot(xx, yy1, color = colors[-1], label = "received")   
    plt.plot(xx, yy2, color = colors[-2], label = "sent")
    plt.fill_between(xx, yy2, 0, color = colors[-2])
    plt.fill_between(xx, yy1, 0, color = colors[-1])

    title = heads[0]
    xlabel = heads[1]
    ylabel = heads[2]
    start_time = heads[3]
    interval = heads[4]

    plt.legend()
    plt.axis([0,20000,0,30])
    plt.title(title, fontsize = 20)
    plt.xlabel(xlabel, fontsize = 18)
    plt.ylabel(ylabel, fontsize = 18)
    fig.savefig(title.replace(" ","_")+".png")

def getdata_for_polar(path):
    """
    Prepare data for polar ploting
    """
    heads = []
    r = []
    theta = []
    max_node = 0
    area = []

    f = open(path, "r")
    line = f.readline().strip('\r\n')
    for info in line.split(":"):
        heads.append(info)

    while True:
        line = f.readline().strip('\r\n')
        if line:
            info = line.split(':')
            if info[0][0] == '#':
                heads.append(info[1])
                max_node = int(info[2])
                heads.append(max_node)
                break
            elif info[0] != 'N/A':
                r.append(int(info[1]))
                theta.append(float(info[2]))
            else:
                continue
        else:
            break

    for i in range(len(theta)):
        theta[i] = theta[i]/max_node*2*PI
        area.append(100 * theta[i]**2 )

    f.close()
    return heads, r, theta, area

def polar(path):
    """
    Draw a polar chart
    """
    heads, r, theta, area = getdata_for_polar(path)
    colors = theta

    fig = plt.figure()
    ax = plt.subplot(111, polar=True)
    c = plt.scatter(theta, r, c=colors, s=area, cmap=plt.cm.hsv)
    c.set_alpha(0.75)

    title = heads[0]
    plt.text(7,7,"Max nodes: "+ str(heads[-1]),fontsize = 18)
    plt.title(title, fontsize = 20)
    fig.savefig(title.replace(" ","_")+".png")

def getdata_for_scatter3d(path):

    f = open(path, 'r')
    heads = []
    x = []
    y = []
    z = []

    line = f.readline().strip('\r\n')
    for info in line.split(':'):
        heads.append(info)

    while True:
        line = f.readline().strip('\r\n')
        if line:
            info = line.split(':')
            x.append(int(info[0]))
            y.append(int(info[1]))
            z.append(int(info[2]))
        else:
            break
    
    f.close()
    return heads, x, y, z
 
def scatter_3d(path):
    """
    Draw a 3d scatter
    """
    heads, x, y, z = getdata_for_scatter3d(path)
    fig = plt.figure()
    ax = fig.add_subplot(111, projection='3d')

    dot_size = [80]*len(x)
    ax.scatter(x, y, z, color="red", marker=(3,3), s = dot_size)

    ax.set_xlabel(heads[1])
    ax.set_ylabel(heads[2])
    ax.set_zlabel(heads[3])

    ax.set_xlim(0, 8)
    ax.set_ylim(0, 100)
    ax.set_zlim(0, 600)

    title = heads[0]
    plt.title(title, fontsize = 20)
    fig.savefig(title.replace(" ","_")+".png")

def getdata_for_bar(path, flag):
    """
    Prepare data for bar ploting
    flag: 1-single num 2-for capacity
    """
    f = open(path, "r")
    heads = []
    names = []
    high_used = []
    high_available = []

    line = f.readline().strip('\r\n')
    for info in line.split(':'):
        heads.append(info)
    while  True:
        line = f.readline().strip('\r\n')
        if line:
            info = line.split(':')
            if info[0][0] == '#':
                heads.append(info[1])
                break
            elif info[1] == 'N/A':
                continue
            else:
                names.append(info[0].split('_')[0])
                if flag == 2:
                    high_used.append(float(info[2]))
                    high_available.append(float(info[1])-float(info[2]))
                elif flag == 1:
                    high_used.append(float(info[1]))
        else:
            break

    if flag == 2:
        return heads, names, high_used, high_available
    elif flag == 1:
        return heads, names, high_used, high_available
    else:
        return None

def tackle_bar_data(names, high_used, high_available, flag, N):
    """
    Select portion of data for dispaly
    Flag: 1-ratio_max 2-ratio_min 3-size_max 4-size_min 5-single_max
    """
    selected = []
    ratio_max_mode = False
    size_max_mode = False
    single_max_mode = False
    names_new = []
    high_used_new = []
    high_available_new = []
    info = []

    if flag == 1:
        ratio_max_mode = True
    elif flag == 2:
        ratio_min_mode = True
    elif flag == 3:
        size_max_mode = True
    elif flag == 5:
        single_max_mode = True

    if ratio_max_mode:
        ratio = []
        max_ratio = 0
        max_index = 0

        for i in range(len(high_used)):
            if high_used[i]+high_available[i] == 0:
                ratio.append(0)
            else:
                ratio.append(high_used[i]/(high_used[i]+high_available[i]))

        for i in range(N):
            for j in range(len(high_used)):
                if j not in selected and ratio[j] > max_ratio:
                    max_ratio = ratio[j]
                    max_index = j

            selected.append(max_index)
            max_ratio = 0
        for i in selected:
            names_new.append(names[i])
            high_used_new.append(high_used[i])
            high_available_new.append(high_available[i])
            info.append(ratio[i])

    elif size_max_mode:
        size = []
        max_size = 0
        max_index = 0

        for i in range(len(high_used)):
            if high_used[i]+high_available[i] == 0:
                size.append(0)
            else:
                size.append(high_used[i]+high_available[i])

        for i in range(N):
            for j in range(len(high_used)):
                if j not in selected and size[j] > max_size:
                    max_size = size[j]
                    max_index = j

            selected.append(max_index)
            max_size = 0
        for i in selected:
            names_new.append(names[i])
            high_used_new.append(high_used[i])
            high_available_new.append(high_available[i])

    elif single_max_mode:
        max_set = []
        max_count = 0
        max_index = 0
        for i in range(N):
            for j in range(len(high_used)):
                if j not in selected and high_used[j] > max_count:
                    max_count = high_used[j]
                    max_index = j

            selected.append(max_index)
            max_count = 0
        for i in selected:
            names_new.append(names[i])
            high_used_new.append(high_used[i])


    return names_new, high_used_new, high_available_new, info


def bar(path, flag):
    """
    Draw a bar chart
    flag: 1-single 2-array 3-stack
    """
    N = 8
    width = 0.35       # the width of the bars: can also be len(x) sequence
    x_range = np.arange(N)
    fig = plt.figure()

    if flag == 1:
        heads, names, high_used, high_available = getdata_for_bar(path, 1)
        names, high_used, high_available, info = \
        tackle_bar_data(names, high_used, high_available, 5, N)
        p1 = plt.bar(x_range, high_used,   width, color='lightskyblue', label = "objects count")
        plt.legend()

    elif flag == 3:
        heads, names, high_used, high_available = getdata_for_bar(path, 2)
        names, high_used, high_available, info = \
        tackle_bar_data(names, high_used, high_available, 1, N)
        p1 = plt.bar(x_range, high_used,   width, color='lightcoral')
        p2 = plt.bar(x_range, high_available, width, color='lightskyblue',
                     bottom=high_used)
        plt.legend( (p1[0], p2[0]), ('used', 'available'), loc = "upper left" )

    title = heads[0]
    plt.title(title, fontsize = 20)
    plt.xlabel(heads[1])
    plt.ylabel(heads[2])
    plt.xticks(x_range+width/2., names)


    def autolabel(rects, info):
        # attach some text labels
        for i in range(len(rects)):
            rect = rects[i]
            height = rect.get_height()
            plt.text(rect.get_x()+rect.get_width()/2., 1.4*height, \
                '%2.2f'%info[i], ha='center', va='bottom')

#    autolabel(p1, info)

#    plt.show()
    fig.savefig(title.replace(" ","_")+".png")


def main():
    """
    the Main
    """
    title =  "Chart"
    pie_mode = False
    scatter_mode = False
    line_mode = False
    polar_mode = False
    scatter_3d_mode = False
    bar_mode = False
    opts = getops()
    for option, value in opts:
        if option in ("-h", "--help"):
            usage()
            sys.exit(SUCCESS)
        elif option in ("-p", "--pie"):
            pie_mode = True
            pie(value)
        elif option in ("-s", "--scatter"):
            scatter_mode = True
            scatter(value)
        elif option in ("-l", "--line"):
            line_mode = True
            line(value)
        elif option in ("-o", "--polar"):
            polar_mode = True
            polar(value)
        elif option in ("-d", "--dimensions3"):
            scatter_3d_mode = True
            scatter_3d(value)
        elif option in ("-b", "--bar"):
            bar_mode = True
            bar(value, 1)
        elif option in ("-z", "--outpath"):
            pass
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