# settings
local_exp_dir = "/user/me/experiments/EXP_023/"
jar_location = "/user/me/libraries/Gumbo-0.4.0-jar-with-dependencies.jar"
function_file = local_exp_dir+"data/functions.py"

session_id = "EXP_023"

datatype_map = [
   ["1", ("R","generate_R_T1"), ("S","generate_S_T1")],
   ["2", ("R","generate_R_T2"), ("S","generate_S_T2")],
   ["3", ("R","generate_R_T3"), ("S","generate_S_T3")],
   ]

query_location_map = [
    ["1", local_exp_dir + "queries/query1.gumbo"],
]

extra_script_map = [
    ["pig_1_wide", ("pig", "wide", 1, "pig -x mapreduce -f %squeries/query1-wide.pig -param jobname=#ID"%(local_exp_dir))],
    ["pig_1_long", ("pig", "long", 1, "pig -x mapreduce -f %squeries/query1-long.pig -param jobname=#ID"%(local_exp_dir))],
    
]