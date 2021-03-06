from subprocess import Popen
from subprocess import call
from subprocess import PIPE
import os
import datetime

class GumboRunner:
    
    hdfs_scratch_dir = None
    hdfs_output_dir = None
    hdfs_input_dir = None
    
    local = False
    debug = False
    cleanup = True
    
    exp_nr = "Gumbo_Job"
    function_file = "functions.py"

    gumbo_jar_location = ""
    
    def __init__(self,jarloc, session_id):
        self.gumbo_jar_location = jarloc
        self.exp_nr = session_id
        
    def set_hdfs_dirs(self, indir, outdir, scratchdir):
        self.hdfs_scratch_dir = scratchdir
        self.hdfs_output_dir = outdir
        self.hdfs_input_dir = indir
        
    def set_function_file(self, function_file):
        self.function_file = function_file
        
    def set_local(self, local):
        self.local = local
        
    def set_cleanup(self, cleanup):
        self.cleanup = cleanup
        
    def set_debug(self, debug):
        self.debug = debug

    def cmd(self, cmdlist, background=False):
        print "Executing command:"
        print "\t" + reduce(lambda x,y: str(x) + " " + str(y), cmdlist)
        cmdlist = map(lambda x: str(x), cmdlist)
        if not self.debug:
            if background:
                return Popen(cmdlist, stdout=PIPE, stderr=PIPE)
            else:
                return call(cmdlist)

    def hdfs_remove(self,dir):
        self.cmd(["hadoop", "dfs", "-rm", "-r", "-skipTrash", dir])

    def remove_scratch(self):
        if self.hdfs_scratch_dir != None:
            self.hdfs_remove(self.hdfs_scratch_dir)
    
    def remove_output(self):
        if self.hdfs_output_dir != None:
            self.hdfs_remove(self.hdfs_output_dir)
    
    def remove_input(self):
        if self.hdfs_input_dir != None:
            self.hdfs_remove(self.hdfs_input_dir)
    
    def remove_all_dirs(self):
        self.remove_input()
        self.remove_output()
        self.remove_scratch()
    
    def generate_all_input(self, data_types, data_sizes, add_suffix=True):
        pids = []
        for size in data_sizes:
            for data in data_types:
                pids = pids + self.generate_input(data, size, add_suffix)
                
        self.wait_for(pids)
        
    def wait_for(self, pids):
        # return
        print("waiting for subprocesses")
        return_vals = [[p.pid] + list(p.communicate()) + [p.returncode] for p in pids]
        exit_codes = [p.returncode for p in pids]
        print("subprocesses done")
        
        print
        print "---> START OF CMD OUTPUT <---"
        for return_val in return_vals:
            print "Process:", return_val[0]
            print "Return:", return_val[3]
            print "Errors:"
            print return_val[2] 
            print "Output:"
            print return_val[1]
        print "---> END OF CMD OUTPUT <---"
        print
        
        return exit_codes
        
    def generate_input(self, data, size, add_suffix=False):
        
        pids = []
        
        type = data[0]
        
        for relationdata in data:
            if isinstance(relationdata,(tuple,list)):
                relation = relationdata[0]
                function_name = relationdata[1]
                splits = max(int(size / 10000000),1)
                
                suffix = ""
                if add_suffix:
                    suffix="_t"+str(type)+"_s"+str(size)
        
                site = "hdfs"
                if self.local:
                    site = "local"
                
                gencmd = [
                    "generate_data_hdfs", 
                    size, splits, 
                    self.function_file, function_name, 
                    "tmp", #os.path.join("tmp/",relation+suffix), 
                    os.path.join(self.hdfs_input_dir,relation+suffix), 
                    1, site
                ]
                pids = pids + [self.cmd(gencmd, True)]
                
        return pids


    def get_opts(self):
        d = {
            "1" : "-Dgumbo.engine.turnOffOpts=true -Dgumbo.engine.assertConstantOptimizationOn=true",
            "2" : "-Dgumbo.engine.turnOffOpts=true -Dgumbo.engine.requestAtomIdOptimizationOn=true",
            "3" : "-Dgumbo.engine.turnOffOpts=true -Dgumbo.engine.guardKeepAliveOptimizationOn=true",
            "4" : "-Dgumbo.engine.turnOffOpts=true -Dgumbo.engine.guardAddressOptimizationOn=true",
            "5" : "-Dgumbo.engine.turnOffOpts=true -Dgumbo.engine.guardAsGuardedReReadOptimizationOn=true",
            "6" : "-Dgumbo.engine.turnOffOpts=true -Dgumbo.engine.guardedCombinerOptimizationOn=true",
            "7" : "-Dgumbo.engine.turnOffOpts=true -Dgumbo.engine.round1FiniteMemoryOptimizationOn=true",
            
            "n1" : "-Dgumbo.engine.turnOnOpts=true -Dgumbo.engine.assertConstantOptimizationOn=false",
            "n2" : "-Dgumbo.engine.turnOnOpts=true -Dgumbo.engine.requestAtomIdOptimizationOn=false",
            "n3" : "-Dgumbo.engine.turnOnOpts=true -Dgumbo.engine.guardKeepAliveOptimizationOn=false",
            "n4" : "-Dgumbo.engine.turnOnOpts=true -Dgumbo.engine.guardAddressOptimizationOn=false",
            "n5" : "-Dgumbo.engine.turnOnOpts=true -Dgumbo.engine.guardAsGuardedReReadOptimizationOn=false",
            "n6" : "-Dgumbo.engine.turnOnOpts=true -Dgumbo.engine.guardedCombinerOptimizationOn=false",
            "n7" : "-Dgumbo.engine.turnOnOpts=true -Dgumbo.engine.round1FiniteMemoryOptimizationOn=false",
            
            "default" : "-Dgumbo.engine.turnOnDefaultOpts=true",
            "ph-off" : "-Dgumbo.engine.turnOnDefaultOpts=true -Dgumbo.compiler.partitioner=gumbo.compiler.partitioner.UnitPartitioner",
            
            "nogroup" : "-Dgumbo.engine.mapOutputGroupingPolicy=NONEGROUP",
            "ng" : "-Dgumbo.engine.mapOutputGroupingPolicy=NONEGROUP",
            "allgroup" : "-Dgumbo.engine.mapOutputGroupingPolicy=ALLGROUP -Dgumbo.engine.mapOutputGroupingOptimizationOn=false -Dgumbo.engine.reduceOutputGroupingOptimizationOn=false",
            "allgroup+map" : "-Dgumbo.engine.mapOutputGroupingPolicy=ALLGROUP -Dgumbo.engine.mapOutputGroupingOptimizationOn=true -Dgumbo.engine.reduceOutputGroupingOptimizationOn=false",
            "allgroup+map+red" : "-Dgumbo.engine.mapOutputGroupingPolicy=ALLGROUP -Dgumbo.engine.mapOutputGroupingOptimizationOn=true -Dgumbo.engine.reduceOutputGroupingOptimizationOn=true",
            "agmr" : "-Dgumbo.engine.mapOutputGroupingPolicy=ALLGROUP -Dgumbo.engine.mapOutputGroupingOptimizationOn=true -Dgumbo.engine.reduceOutputGroupingOptimizationOn=true",
            "allgroup128" : "-Dgumbo.engine.mapOutputGroupingPolicy=ALLGROUP -Dgumbo.engine.mapOutputGroupingOptimizationOn=true -Dgumbo.engine.reduceOutputGroupingOptimizationOn=true -Dgumbo.engine.hadoop.reducersize_mb=128",
            "allgroup64" : "-Dgumbo.engine.mapOutputGroupingPolicy=ALLGROUP -Dgumbo.engine.mapOutputGroupingOptimizationOn=true -Dgumbo.engine.reduceOutputGroupingOptimizationOn=true -Dgumbo.engine.hadoop.reducersize_mb=64",
            
            "all" : "-Dgumbo.engine.turnOnOpts=true",
            "none" : "-Dgumbo.engine.turnOffOpts=true",
            
            "cggmr" : "-Dgumbo.engine.mapOutputGroupingPolicy=COSTGROUP_GUMBO -Dgumbo.engine.mapOutputGroupingOptimizationOn=true -Dgumbo.engine.reduceOutputGroupingOptimizationOn=true",
            "cgpmr" : "-Dgumbo.engine.mapOutputGroupingPolicy=COSTGROUP_PAPER -Dgumbo.engine.mapOutputGroupingOptimizationOn=true -Dgumbo.engine.reduceOutputGroupingOptimizationOn=true",
            "cgimr" : "-Dgumbo.engine.mapOutputGroupingPolicy=COSTGROUP_IO -Dgumbo.engine.mapOutputGroupingOptimizationOn=true -Dgumbo.engine.reduceOutputGroupingOptimizationOn=true",
            
            "bcggmr" : "-Dgumbo.engine.mapOutputGroupingPolicy=BESTCOSTGROUP_GUMBO -Dgumbo.engine.mapOutputGroupingOptimizationOn=true -Dgumbo.engine.reduceOutputGroupingOptimizationOn=true",
            "bcgpmr" : "-Dgumbo.engine.mapOutputGroupingPolicy=BESTCOSTGROUP_PAPER -Dgumbo.engine.mapOutputGroupingOptimizationOn=true -Dgumbo.engine.reduceOutputGroupingOptimizationOn=true",
            "bcgimr" : "-Dgumbo.engine.mapOutputGroupingPolicy=BESTCOSTGROUP_IO -Dgumbo.engine.mapOutputGroupingOptimizationOn=true -Dgumbo.engine.reduceOutputGroupingOptimizationOn=true",
            
            
            "red64" : "-Dgumbo.engine.hadoop.reducersize_mb=64",
            "red128" : "-Dgumbo.engine.hadoop.reducersize_mb=128",
            "red256" : "-Dgumbo.engine.hadoop.reducersize_mb=256",
            "red512" : "-Dgumbo.engine.hadoop.reducersize_mb=512",
            "red1g" : "-Dgumbo.engine.hadoop.reducersize_mb=1024",
            "red2g" : "-Dgumbo.engine.hadoop.reducersize_mb=2048",
            
            
            "PG" : "-Dgumbo.compiler.partitioner=gumbo.compiler.partitioner.GreedyPartitioner",
            "PO" : "-Dgumbo.compiler.partitioner=gumbo.compiler.partitioner.OptimalPartitioner",
            "PH" : "-Dgumbo.compiler.partitioner=gumbo.compiler.partitioner.HeightPartitioner",
            "PD" : "-Dgumbo.compiler.partitioner=gumbo.compiler.partitioner.DepthPartitioner",
            
            
            "comb1" : "-Dgumbo.engine.guardedCombinerOptimizationOn=true",
            "finm" : "-Dgumbo.engine.round1FiniteMemoryOptimizationOn=true",
            "v2" : "",
            
             
            "planonly" : "--grouponly",
            "hprof" : "-Dmapreduce.task.profile=true -Dmapreduce.task.profile.params=-agentlib:hprof=cpu=samples,depth=20,interval=4,lineno=y,thread=y,format=a,file=%s",
             "hprof2" : "-Dmapreduce.task.profile=true -Dmapreduce.task.profile.params=-agentlib:hprof=cpu=samples,depth=100,interval=9,lineno=y,thread=y,format=b,file=%s,verbose=n",
            "test" : "",
            
            # v2 options
            "unnest" : "-Dgumbo.compiler.unnest=true",
            
            "msjgroup" : "-Dgumbo.engine.valeval.group=true",
            "msjnogroup" : "-Dgumbo.engine.valeval.group=false",
            
            "no1round" : "-Dgumbo.engine.valeval.enabled=false"
            
        }
        
        for i in range(17):
            d["p"+str(i)] = "-Dgumbo.engine.grouper.beststopindicator=" + str(i)
            
        return d

    def get_opt(self, optids):
        ids = optids.split("-")
        
        opt_string = ""
        for id in ids:
            d = self.get_opts()
            if id in d:
                opt_string += [id]
                opt_string += " "
        
        return opt_string
        
        
    def remove_exp_output(self,exp_id):
        # delete scratch
        self.remove_scratch()

        # delete output
        self.remove_output()
        
        print "---> REMOVED OUTPUT & SCRATCH FOR EXP %s <---"%(exp_id)
        print
        
        
    def run_gumbo(self, opt_id, exp_id, query_file_location):
        
        print
        print "---> RUNNING EXP %s <---"%(exp_id)
        
        # create arguments
        opt_string = self.get_opt(opt_id)
        
        # create jobid
        job_id = exp_id
        
        # run gumbo
        self.cmd(["hadoop", "jar", self.gumbo_jar_location, "gumbo.Gumbo"] + opt_string.split(" ")  + [ "-f", query_file_location, "-j", job_id])
        
        
        print "---> FINISHED EXP %s <---"%(exp_id)
        print
        
        if self.cleanup:
            self.remove_exp_output(exp_id)
        
       
        
    def run_script(self, cmd, exp_id):
    
        # add job id to argument
        cmd = cmd.replace("#ID",exp_id)
        cmdlist = cmd.split(" ")
        
        print
        print "---> RUNNING EXP %s <---"%(exp_id)
        
        # run job
        self.cmd(cmdlist)
        
        print "---> FINISHED EXP %s <---"%(exp_id)
        print
        
        if self.cleanup:
            self.remove_exp_output(exp_id)
        
        
    def create_id(self, qtype, dtype, size, opt, qid):
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
        id_list = [ self.exp_nr, qtype, "t"+str(dtype), "s"+str(size), "o"+str(opt), "q" + str(qid), timestamp]
        id_string = reduce(lambda x,y: str(x) + "_"+ str(y) ,id_list)
        return id_string
    
    def run_all(self, data_types, data_sizes, opt_list, queries, extra_scripts):
        
        # clean start
        self.remove_all_dirs()
    
        try:
            for size in data_sizes:
                for type in data_types:
                    # generate data
                    pids = self.generate_input(type, size)
                    self.wait_for(pids)
        
                    # run gumbo scripts with optimizations
                    for opt in opt_list:
                        
                        for (qid,query_file_location) in queries:
                            
                            id_string = self.create_id("gumbo", type[0], size, opt, qid)
                            self.run_gumbo(opt, id_string, query_file_location)
            
                    # run extra scripts
                    for (script_id,(qtype,optid,qid,cmd)) in extra_scripts:
                        
                        id_string = self.create_id(qtype, type[0], size, optid, qid)
                        self.run_script(cmd, id_string)
                
                    # delete data
                    if self.cleanup:
                        self.remove_input()
        except:
            # clean up
            print "[ERROR] Something went wrong, cleaning up hdfs..."
            self.remove_all_dirs()
            raise

