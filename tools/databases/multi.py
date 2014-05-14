import random
import string
import os

cur_dir = os.path.dirname(os.path.realpath(__file__))
config_dir = os.path.join(cur_dir,"configs")

def genRunCommand(config):
    return "./oltpbenchmark --b ycsb -c %s  --create=true --load=true --execute=true --drop=true" % config

def genRandomName(base='test%s',n=5):
    return base % ''.join(random.choice(string.ascii_lowercase) for _ in range(n))

def genConfigFile(db_name, outfile, template='template1.xml'):
    with open(template, 'r') as rdf:
        temp_text = rdf.read()
    temp_text = temp_text % db_name
    with open(outfile,'w') as wf:
        for line in temp_text:
            wf.write(line)
    return outfile

def run(num_dbs):
    dbs = [genRandomName() for i in range(num_dbs)]
    configs = [genConfigFile(db,os.path.join(config_dir,"%s.xml" % (db))) for db in dbs]
    return configs
