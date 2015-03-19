#!/usr/bin/python
#__CR__
#Name format: Domain name_Port_TenantID_version.bdbdb or Domain name_Port_TenantID_version.dump
#Please copy to one of the node of Atmos system and it should work
#Author: Chris.Liu 
#2014-12-02
#__CR__

"""
This tool is build for version db
It contains two operations
1.Gather all the version db from all MDS servers and then package them to /var/tmp/version_db.tar locally
2.Dump the version db collected to get the information and store in /var/tmp/version_dump locally
"""

import commands
import common
import os

domain = []
port = []
home_dir = "/var/tmp"
tar_dir = "/var/tmp/version_db"
dump_dir = "/var/tmp/version_dump"

def get_domain_port():
	"""
	find out all MDS server's domain & port
	"""
	status, output = commands.getstatusoutput('rmsview -l mauimds | grep master')
	if status != 0:
		print "rmsview error"
	#output.strip("\n")
	array = output.split("\n")
	for i in range(0,len(array)):
		tmp = array[i].split(":")
		domain[i] = tmp[1]
		port[i] = tmp[2]

def tar_version(mds_host, mds_port, tar_dir):
	"""
	method that tackle every mds server to copy to /var/tmp/version_tar for make a tar ball
	"""
	status,output = commands.getstatusoutput('mauisvcmgr -s mauimds -c mauimds_queryTenantList -m %s:%s' %(mds_host,mds_port))
	tenant_list = output.split('\n')[1:]
	status,mds_dir = common.ssh(mds_host,'mdsdir %s'%mds_port)
	for i in range(0,len(tenant_list)):
		os.system('scp %s:%s/%s_version.bdbdb %s/%s_%s_%s_version.bdbdb' % (mds_host,mds_dir,tenant_list[i].split("=")[1],tar_dir,mds_host,mds_port,tenant_list[i].split("=")[1]))


def dump_version(tar_dir, dump_dir):
	"""
	method that dump all version db collect to a diretory-dump_dir
	"""
	for dir_path,subdir,files in os.walk(tar_dir):
		for file in files:
			os.system('db_dump -p %s/%s > %s/%s' % (tar_dir,file,dump_dir,file.replace(".bdbdb",".dump")))


#execute to dump & tar

if not os.path.exists(tar_dir):
	os.system('mkdir %s' % tar_dir)

print "\nStart to archive version db from mds servers..."
print "###############################################"
for i in range(0,len(array)):
	tar_version(domain[i],port[i],tar_dir)
if not os.path.exists(dump_dir):
	os.system('mkdir %s' % dump_dir)

print "\nFinish the transmission & Start to package as a tar to /var/tmp/version_db.tar"
os.system('tar -C %s -cvf %s/version_db.tar %s' % (home_dir,home_dir,tar_dir.replace(home_dir,".")))

print "\nFinsh packaging & Start to dump version db to /var/tmp/version_dump"
print "###############################################"
dump_version(tar_dir, dump_dir)
print "Finish dump!"
print "Deleting the tar_source leftovers"
os.system('rm -rf %s' % tar_dir)
print "\nCongratulation! All done\n"
