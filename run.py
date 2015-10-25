#! /usr/bin/python
import argparse
from gumborunner import GumboRunner 


# TODO make data optional
# TODO add permanent data flag
# TODO extract gumbo and general hadoop runner
# TODO add default optimization



parser = argparse.ArgumentParser(description='Hadoop Job History Extractor.')
parser.add_argument("-p", "--parameters", action='store', required=True,\
    help="python file with necessary vars")
parser.add_argument("-t", "--datatypes", nargs='+', action='store',required=True,\
    help="type id list")
parser.add_argument("--nosuffix", "--nosuffix", action='store_true', default=False,\
        help="avoid data directory suffixes in data-only modus")
parser.add_argument("-s", "--sizes", action='store', nargs='+', type=int, required=True,\
    help="size list")
parser.add_argument("-o", "--optimizations", action='store', nargs='+', required=False, default=["default"],\
    help="optimization id list")
parser.add_argument("-q", "--queries", action='store', nargs='+', required=False, default=[],\
    help="the list of query ids to run, when not provided, only the data data is generated")
parser.add_argument("-e", "--executescripts", action='store', nargs='+', required=False, default=[],\
    help="list of scripts to execute, can be used when no opts are given. \
    Each scripts has 3 parts: name,opt,script, e.g., pig,wide,pig -x mapreduce ./queries/query.pig")
# parser.add_argument("-n", "--name", action='store', required=False, default=None,\
#     help="the name of the query")
parser.add_argument("-l", "--local", action='store_true', default=False,\
        help="execute data generation locally instead of on hdfs")
parser.add_argument("-d", "--debug", action='store_true', default=False,\
        help="no not execute commands, only print them")
    
    
args = parser.parse_args()


# get parameters
params = vars(args)

execfile(params['parameters'])


req_optimizations = params['optimizations']
req_types = params['datatypes']
req_sizes = params['sizes']
req_queries = params['queries']
req_extra_scripts = params['executescripts']

if req_types != None:
    req_types = filter(lambda x: x[0] in req_types,datatype_map)

if req_queries != None:
    req_queries = filter(lambda x: x[0] in req_queries,query_location_map)

if req_extra_scripts != None:
    req_extra_scripts = filter(lambda x: x[0] in req_extra_scripts,extra_script_map)

    
g = GumboRunner(jar_location, session_id)
g.set_hdfs_dirs("input/experiments/"+session_id,"output/"+session_id,"scratch/"+session_id)
g.set_function_file(function_file)
g.set_local(params['local'])
g.set_debug(params['debug'])




if len(req_queries) == 0 and len(req_extra_scripts) == 0:
    g.generate_all_input(req_types, req_sizes, not params['nosuffix'])
else:
    g.run_all(req_types, req_sizes, req_optimizations, req_queries, req_extra_scripts)
