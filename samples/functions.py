import random
import math

# types
# -----

def gen_row(i):
    i = i + 1
    return map(lambda x: str(x),[i,i+1,i+2,i+3,i+4,i+5])

    
def generate_R_T1(i):
    return reduce(lambda x,y: str(x) + "," + str(y), gen_row(i))
 
def generate_S_T1(i):
    return i+1
    
    
def generate_R_T2(i):
    return reduce(lambda x,y: str(x) + "," + str(y), gen_row(i))
 
def generate_S_T2(i):
    if i % 10 == 0:
        return i/10
    return ""
    


def generate_R_T3(i):
    if i % 10 == 0:
        return reduce(lambda x,y: str(x) + "," + str(y), gen_row(i))
    return ""
 
def generate_S_T3(i):
    return i+1
    
