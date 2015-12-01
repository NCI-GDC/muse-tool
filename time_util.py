import os
import sys

import pandas as pd

def store_time(uuid,cmd,output,logger):
    user_time=float()
    system_time=float()
    percent_of_cpu=int()
    wall_clock=float()
    maximum_resident_set_size=int()
    exit_status=int()
    for line in output.decode().format().split('\n'):
        line=line.strip()
        if line.startswith('User time (seconds):'):
            user_time=float(line.split(':')[1].strip())
        if line.startswith('System time (seconds):'):
            system_time=float(line.split(':')[1].strip())
        if line.startswith('Percent of CPU this job got:'):
            percent_of_cpu=int(line.split(':')[1].strip().rstrip('%'))
            assert (percent_of_cpu is not 0)
        if line.startswith('Elapsed (wall clock) time (h:mm:ss or m:ss):'):
            value=line.replace('Elapsed (wall clock) time (h:mm:ss or m:ss):','').strip()
            #hour case
            if value.count(':')==2:
                hours=int(value.split(':')[0])
                minutes=int(value.split(':')[1])
                seconds=float(value.split(':')[2])
                total_seconds=(hours*60*60)+(minutes*60)+seconds
                wall_clock=total_seconds
            if value.count(':')==1:
                minutes=int(value.split(':')[0])
                seconds=float(value.split(':')[1])
                total_seconds=(minutes*60)+seconds
                wall_clock=total_seconds
        if line.startswith('Maximum resident set size (kbytes):'):
            maximum_resident_set_size=int(line.split(':')[1].strip())
        if line.startswith('Exit status:'):
            exit_status=int(line.split(':')[1].strip())

    df=pd.DataFrame({'uuid':[uuid],
                     'user_time':user_time,
                     'system_time':system_time,
                     'percent_of_cpu':percent_of_cpu,
                     'wall_clock':wall_clock,
                     'maximum_resident_set_size':maximum_resident_set_size,
                     'exit_status':exit_status})
    return df
