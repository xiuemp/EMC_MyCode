#!/usr/bin/python

import commands

COMMAND_DB_CHECK = 'python db_check.py'
COMMAND_READ_PROP = 'python parse_db.py -p %s'
LATEST = "#################################\n\n Latest status no need to update\n\n#################################\n"
PATH_SCAN_LIST = "./SCAN.conf"

def read_db_to_files():
	f = open(PATH_SCAN_LIST, 'r')
	prop_list = []

	while True:
		line = f.readline()
		if line:
			prop_list.append(line.strip('\n'))
		else:
			break
#	print prop_list
	print "########################"
	print "Begin to export to files"
	print "########################"
	for prop in prop_list:
		print "Parsing \'"+prop+"\' leaving "+ str(len(prop_list)-prop_list.index(prop)-1) + " items..."
		status, output = commands.getstatusoutput(COMMAND_READ_PROP % prop)
	print "Finished Writing to .pd files, pls check ./data_persist"


def main():
	"""
	The Main 
	"""
	force_mode = False

	status, output = commands.getstatusoutput(COMMAND_DB_CHECK)
	print output

	if output == LATEST:
		pass
	else:
		read_db_to_files()

if __name__ == '__main__':
	main()
