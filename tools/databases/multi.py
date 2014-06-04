import random
import string
import os
import subprocess
import shlex
import re
import logging
import time
import shutil
import argparse


logger = logging.getLogger('oltp')
logger.setLevel(logging.INFO)
# create file handler which logs even debug messages
fh = logging.FileHandler('ilandlord.log')
fh.setLevel(logging.ERROR)
# create console handler with a lower log level
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s  - %(levelname)s : %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)

cur_dir = os.path.dirname(os.path.realpath(__file__))
config_dir = os.path.join(cur_dir, "configs")
results_dir = os.path.join(cur_dir, "results")
dbs = []
configs = []
SERVER = 'localhost'
PG_USER = 'dbv'

####################################
# UTILS
####################################

def cleanDirs():
    shutil.rmtree(config_dir, True)
    os.mkdir(config_dir)
    shutil.rmtree(results_dir, True)
    os.mkdir(results_dir)
    
def localCmdOutput(cmd,checkStringFor=None):
  #logger.info("Calling remote cmd, blocking for output: %s " %(cmd))
  proc = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
  stdout = proc.communicate()[0]
  res = proc.wait()
  if res == 0:
    if checkStringFor:
      if stdout.count(checkStringFor) < 1:
        raise Exception("Error: %s executed, but results did not contain %s. Result:%s" % (cmd,checkStringFor,stdout))
    return stdout.replace("DEB_HOST_MULTIARCH is not a supported variable name at /usr/bin/dpkg-architecture line 214.\n","")
  else:
   raise Exception("Error : %s on remoteCmdBlock %s. Output : %s" %(res,cmd,stdout))

def checkPGActiveDB(db):
  checkActive = "select count(*) from pg_stat_activity where \"usename\" ='ilandlord' and \"datname\"= '%s';" % (db);
  remoteCheck = "psql -U %s -h %s postgres -w -c \"%s\"" % (PG_USER,SERVER,checkActive)
  res =localCmdOutput(remoteCheck,"count")
  count = res.split('\n')[2].strip()
  if count.isdigit():
    count= int(count)
    if count > 0 :
      return True
    else:
      return False
    
  else:
    raise Exception("Error processing %s. Results: %s Checking:" % (checkActive,str(res))  )
  

def cleanPGDB(db,retryCount=4,timeToSleep=30):
    #alternative http://www.postgresql.org/docs/8.2/interactive/app-dropdb.html
    if retryCount==0:
        raise Exception("Unable to remove DB file %s on %s "% (db, SERVER))
    if not checkPGActiveDB(db):
        dbDrop = "DROP DATABASE IF EXISTS %s" % (db)
        remoteCheck = "psql -U %s -h %s postgres -w -c \"%s\"" % (PG_USER,SERVER,dbDrop)
        logger.info("dropping DB %s" %db)
        res =localCmdOutput(remoteCheck,"DROP DATABASE")
        return res
    else:
        logger.info("Sleeping for %s"%timeToSleep)
        time.sleep(timeToSleep)
        retryCount-=1
        return cleanPGDB(db,retryCount)

def cleanDBs(name_base='test'):
    dbs= getListOfPGDBsMatching(name_base)
    for db in dbs:
        cleanPGDB(db)
    
def getListOfPGDBsMatching(dbNameRegex, node=SERVER):
    listDBs = "psql -l -t -h %s -U %s" % (node,PG_USER)
    res = localCmdOutput(listDBs,"en_US.UTF-8")
    dbLines =  res.split('\n')
    dbRaw = [x.split('|')[0].strip() for x in dbLines if len(x) > 0]
    dbs = [ x for x in dbRaw if len(x) > 0 and re.search(dbNameRegex,x)]
    return dbs    


####################################
# OLTPBench Inter
####################################

def genRunCommand(config):
    head, tail = os.path.split(config)
    out_file = "%s-%s" % (tail.split(".")[0],len(dbs))
    return "./oltpbenchmark --b ycsb -c %s --execute=true -d %s -o %s " % (config,results_dir, out_file)

def genLoadCommand(config):
    return "./oltpbenchmark --b ycsb -c %s --create=true --load=true " % config

def genRandomName(base='test%s',n=5):
    return base % ''.join(random.choice(string.ascii_lowercase) for _ in range(n))

def genConfigFile(db_name, outfile, template='template1.xml'):
    with open(os.path.join(cur_dir,template), 'r') as rdf:
        temp_text = rdf.read()
    temp_text = temp_text % db_name
    with open(outfile,'w') as wf:
        for line in temp_text:
            wf.write(line)
    return outfile

def addDBs(num_dbs):
    print "adding dbs:",num_dbs
    new_dbs = [genRandomName() for i in range(num_dbs)]
    dbs.extend(new_dbs)
    print ",".join(dbs)
    new_configs = [genConfigFile(db,os.path.join(config_dir,"%s.xml" % (db))) for db in new_dbs]
    configs.extend(new_configs)
    return new_configs
    

def run_commands(commands, stagger=2):
    procs = [] 
    for cmd in commands:
        print "Calling : " , cmd
        procs.append(subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.STDOUT))
        time.sleep(stagger)
    for proc in procs:
        stdout, stderr = proc.communicate()
        res = proc.wait()
        if res != 0:
            raise Exception("Failed on %s. %s" % (cmd,stderr))
        print "DONE"
        #print stdout


def run():
    cleanDirs()
    cleanDBs()
    dbs = [1,2,4]
    for i in dbs:
        new_configs = addDBs(i)
        #load new dbs
        run_commands([genLoadCommand(c) for c in new_configs])
        #run all dbs
        run_commands([genRunCommand(c) for c in configs])
    
def createDbs(args):
    logger.info("num %s"% args.num)
    db_names =  [genRandomName('usertable%s',6) for x in range(args.num)]
    CREATE = "CREATE DATABASE %s"
    CREATE_TABLE = "CREATE TABLE USERTABLE ( YCSB_KEY INT PRIMARY KEY,    FIELD1 VARCHAR(100),   	FIELD2 VARCHAR(100),  	FIELD3 VARCHAR(100),   	FIELD4 VARCHAR(100),  	FIELD5 VARCHAR(100),   	FIELD6 VARCHAR(100),  	FIELD7 VARCHAR(100),   	FIELD8 VARCHAR(100),  	FIELD9 VARCHAR(100),   	FIELD10 VARCHAR(100));"
    for db in db_names:
        create = CREATE % db
        cmd = "psql -U %s  postgres -w -c \"%s\"" % ("dbv", create)
        localCmdOutput(cmd)
        cmd = "psql -U %s  %s -w -c \"%s\"" % ("dbv", db, CREATE_TABLE)
        localCmdOutput(cmd)
          
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('-n',dest="num", type=int, help='Number of databases to create')
    parser.add_argument('-c',dest="clean", action='store_true', help='Drop')
    args = parser.parse_args()
    if args.clean:
        cleanDBs('usertable')
    createDbs(args)
