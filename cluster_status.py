#!/bin/env python3.11

# programmer: Ping Luo (ping.luo@yale.edu)

# add sys.argv[2] to control display of public or private nodes
# using argparse module
# add Class SlurmNode
# dump CSV records to files. command.rb reads from the CSV files.
# change partition to more complicated structure; currently it is only {"partition_name":"public|private"}. Add a SlurmPartition class.
# make verbose level an option from command line and revise what to print

import subprocess
import re
import sys

noshow_partitions = ["admintest", "scavenge", "scavenge_gpu", "scavenge_mpi", "gpu_test"]
# nodes in special partition may have multiple differenct GPUs in the same node
#special_partitions = ["pi_lederman"]
special_partitions = ["gpu_devel"]

# for debugging
verbose = 0
verbose2 = 0

# color code
color_map = {
    "UP"  : "green",
    "DOWN": "red",
    "RESERVED": "Orange",
    "NONE": "black",
    "FULL": "#696969",
    "L3"  : "#909090",
    "L2"  : "#D0D0D0",
    "L1"  : "#E8E8E8",
    "L0"  : "white",
}

# Utilization threshold
utilize_map = {
    "FULL": 1.0,
    "L3"  : 1.0,
    "L2"  : 0.50,
    "L1"  : 0.25,
    "L0"  : 0.0,
}

def get_node_records():

    cmd = ["/opt/slurm/current/bin/scontrol", "show", "node", "-d"]
    output = subprocess.run(cmd, capture_output=True, text=True, check=True).stdout

    node_records = []
    node_dict = {}
    for line in output.splitlines():
        if verbose:
            print(line)

        # a new record starts with NodeName=
        if re.compile("^NodeName=").search(line):
            # a new node record starts here. Append the previous node record in node_dict to node_records 
            # The first record appended is empty which should be poped out afterall records are processed
            node_records.append(node_dict)
            # reset node_dict = {}
            node_dict = {}
        if line.strip():
            tokens = line.strip().split(" ")
            for token in tokens:
                if re.search("CfgTRES|AllocTRES|GresUsed", token):
                    kv_pair = token.strip().split("=", 1)
                else: 
                    kv_pair = token.strip().split("=")

                if verbose2: 
                    print(kv_pair)

                if len(kv_pair) == 2:
                    node_dict[kv_pair[0]] = kv_pair[1]

    # append the last record
    node_records.append(node_dict)
    # pop the first record since it is empty
    node_records.pop(0)

    if verbose:
        print(node_records)

    return node_records

def convert(mem_size_str):
    # Memory size could be G, M, or K. Convert them to G.
    # We don't consider digits after a decimal point. Remove those digits if there are any
    mem_size = int(mem_size_str[0:-1].split(".")[0])
    if mem_size_str[-1] == 'M':
        mem_size = int(mem_size/1024)
    if mem_size_str[-1] == 'K':
        mem_size = int(mem_size/(1024*1024))

    return mem_size

# gpu_usage: input/output, a list of dicitonaries {"GpuType":"type", "GpuTot":num, "GpuAlloc":0}
# gres_str: input, a string. EX: gpu:a100.MIG.20gb:1(IDX:0),gpu:a100.MIG.10gb:0(IDX:N/A)
# action: update "GpuAlloc" with the actual value as specified in gres_str
# gres/gpu:2
# gres/gpu:2,gres/gpu:2
# gres/gpu:2,gres/gpu:a100:1,gres/gpu:a100:1
def process_gpu_alloc(gpu_usage, gres_str):
    gres_list = gres_str.split(",")
    for gres in gres_list:
        ret = re.compile("gpu:(.*):([0-9]+)\(").search(gres)
        if ret:
            gpu_type = ret[1]
            gpu_alloc = ret[2]
            # find the element in the gpu_usage list whose 'GpuType' is gpu_type
            for i in range(len(gpu_usage)):
                if gpu_usage[i]["GpuType"] == gpu_type: 
                    gpu_usage[i]["GpuAlloc"] = gpu_alloc
                    break

def calc_color(number1, number2):
    num1 = int(number1)
    num2 = int(number2)
    assert num1 > 0, "The first parameter of calc_color must be larger than 0."

    quotient = num2/num1
    if quotient == utilize_map["L0"]:
        return color_map["L0"]
    if quotient <= utilize_map["L1"]:
        return color_map["L1"]
    if quotient <= utilize_map["L2"]:
        return color_map["L2"]
    if quotient < utilize_map["L3"]:
        return color_map["L3"]
    else:
        return color_map["FULL"]

# return a list of dictionary [{partition:....}]
def get_partitions():
    partition_dicts = {}
    cmd = ["/opt/slurm/current/bin/scontrol", "show", "partition"]
    output = subprocess.run(cmd, capture_output=True, text=True, check=True).stdout
    for line in output.splitlines():
        if verbose: print(line)

        ret = re.compile("PartitionName=(.*)").search(line)
        if ret:
            if verbose: print(ret.group(1))

            partition = ret.group(1)
        # AllowAccounts= is right after PartitionName= is processed. So it is safe to parse it like this
        ret = re.compile("AllowAccounts=(.*) AllowQos").search(line)
        if ret:
            if verbose: print(ret.group(1))
            if ret.group(1) == 'ALL':
                partition_dicts[partition] = 'public'
            else:
                partition_dicts[partition] = 'private'

    return partition_dicts

# We are interested in NodeName, CfgTRES, AllocTRES, Gres, State of each node record
# Add each node record with  into different partitions
# generate a list of dictionary with partition name as the keywords
# node["NodeName"]
# node["State"]
# node["Type"]: GPU or CPU
# node["Gpu"]: list of 
# node["CpuTot"]
# node["CpuAlloc"]
# node["MemTot"]
# node["MemAlloc"]
def get_node_usage_by_partition(node_records, partitions):

    node_usage_records = {}

    for partition in partitions.keys():
        node_usage_records[partition] = []
#   node_usage_records["pi_lederman"] = []
    
    for node_record in node_records:
        # store the key/value pairs of our interest in a temporary dictionary
        if verbose:
            print(node_record)
            print()

        my_dict = {}
        my_dict["NodeName"] = node_record["NodeName"]
        my_dict["State"] = node_record["State"]
        my_dict["Type"] = "CPU"
        
        if verbose2: print("Gres="+node_record["Gres"])
        # process Gres. It could include different type of gpus
        # Gres=gpu:rtx4000:4(S:0),gpu:rtx8000:2(S:0),gpu:v100:2(S:0)
        # Gres=gpu:rtx3090:4
        my_dict["Gpu"] = [] 
        if 'gpu' not in node_record["Gres"]:
            my_dict["Gpu"].append({"GpuType":"None", "GpuTot":0, "GpuAlloc":0}) 

        gres_list = node_record["Gres"].split(",")
        for gres in gres_list:
            ret = re.compile("gpu:(.*):([0-9]+$|[0-9]+\()").search(gres)
            if ret:
                my_dict["Gpu"].append({"GpuType":ret[1],"GpuTot":ret[2].split('(')[0],"GpuAlloc":0})
                my_dict["Type"] = "GPU"
        if verbose2: print(my_dict["Gpu"])

#  CfgTRES=cpu=36,mem=1505G,billing=36,gres/gpu=8,gres/gpu:rtx4000=4,gres/gpu:rtx8000=2,gres/gpu:v100=2
#  AllocTRES=cpu=15,mem=450G,gres/gpu=5,gres/gpu:rtx4000=2,gres/gpu:rtx8000=2,gres/gpu:v100=1
#  CfgTRES=cpu=32,mem=1024576M,billing=200,gres/gpu=4
#  AllocTRES=cpu=15,mem=450G,gres/gpu=1
#  CfgTRES=cpu=32,mem=984G,billing=196,gres/gpu=4,gres/gpu:a100=4
#  AllocTRES=cpu=7,mem=330G,gres/gpu=4,gres/gpu:a100=4
#  CfgTRES=cpu=32,mem=1991G,billing=39
#  AllocTRES=
#  AllocTRES=cpu=7,mem=330.5G

        if verbose2: print("CfgTRES="+node_record["CfgTRES"])
        # process CfgTRES
        ret = re.compile("cpu=(.*),mem=([0-9]+[.]*[0-9]*)(G|M|K)").search(node_record["CfgTRES"])
        my_dict["CpuTot"] = ret[1]
        my_dict["MemTot"] = convert(str(ret[2])+ret[3])
        if verbose2:
            print("CpuTot="+str(my_dict["CpuTot"]))
            print("MemTot="+str(my_dict["MemTot"]))

        if verbose2: print("AllocTRES="+node_record["AllocTRES"])
        # process AllocTRES
        if node_record["AllocTRES"] == '':
            my_dict["CpuAlloc"] = 0
            my_dict["MemAlloc"] = 0
#       elif re.search('gpu', node_record["AllocTRES"]):
#           ret = re.compile("cpu=(.*),mem=(.*)(G|M|K),(.*)").search(node_record["AllocTRES"])
#           my_dict["CpuAlloc"] = ret[1]
#           my_dict["MemAlloc"] = convert(str(ret[2])+ret[3])
#           process_gpu_alloc(my_dict["Gpu"], ret[4])
        else:
            ret = re.compile("cpu=(.*),mem=(.*)(G|M|K)(.*)").search(node_record["AllocTRES"])
            my_dict["CpuAlloc"] = ret[1]
            my_dict["MemAlloc"] = convert(str(ret[2])+ret[3])
            if re.search('gpu', node_record["AllocTRES"]):
                process_gpu_alloc(my_dict["Gpu"], node_record["GresUsed"])

        # find all the partitions to which the node belongs
        partitions = node_record["Partitions"].strip().split(",")
        if verbose2: print(partitions)
        for partition in partitions:
            if verbose2: print("Append node record to "+partition)
            node_usage_records[partition].append(my_dict)

        if verbose: print(node_usage_records)

    return(node_usage_records)

# node_type
#  CPUGPU: print both cpu and gpu nodes
#  CPU: print cpu nodes only
#  GPU: print gpu nodes only
# partition_type
#  publicprivate
#  public
#  private
def print_node_usage(node_usage_records, partitions, print_all = False, print_header = False, add_colorcode = False, node_type = "CPUGPU", partition_type="publicprivate"):

    if print_header:
        print("Partition;NodeName;Total CPU;Allocated CPU;GPU Type;Total GPU;Allocated GPU;Total Mem;Allocated Mem")

    keys = list(node_usage_records.keys())
    keys.sort()
    
    if not print_all:
        for noshow in noshow_partitions:
            if noshow in keys:
                keys.remove(noshow)
                print("Partiton "+noshow+" will not be shown.")
                #if verbose: print("Partiton "+noshow+" will not be shown.")
    if verbose:
        print(keys)

    # also remove special partitions
    for key in special_partitions:
        keys.remove(key)

    if add_colorcode:
        for key in keys:
            if partitions[key] not in partition_type:
                continue

            for node in node_usage_records[key]:
                if node["Type"] not in node_type:
                    continue

                if re.compile("DOWN|DRAIN|NOT_RESPONDING").search(node["State"]):
                    node_color = color_map["DOWN"] 
                    cpu_color = color_map["DOWN"]
                    gpu_color = color_map["DOWN"]
                    mem_color = color_map["DOWN"]
                else:
                    node_color = color_map["UP"]
                    cpu_color = calc_color(node["CpuTot"], node["CpuAlloc"])
                    if node["Gpu"][0]["GpuTot"] == 0:
                        gpu_color = color_map["NONE"]
                    else:
                        gpu_color = calc_color(node["Gpu"][0]["GpuTot"], node["Gpu"][0]["GpuAlloc"])
                    mem_color = calc_color(node["MemTot"], node["MemAlloc"])

                print(key+";"+node_color+";"+node["NodeName"]+";"+cpu_color+";"+str(node["CpuTot"])+";"+str(node["CpuAlloc"])+";"
                      +gpu_color+";"+node["Gpu"][0]["GpuType"]+";"+str(node["Gpu"][0]["GpuTot"])+";"+str(node["Gpu"][0]["GpuAlloc"])+";"
                      +mem_color+";"+str(node["MemTot"])+";"+str(node["MemAlloc"]))

        # now print special partitions
        for key in special_partitions:
            if partitions[key] not in partition_type:
                continue

            for node in node_usage_records[key]:
                if node["Type"] not in node_type:
                    continue

                if re.compile("DOWN|DRAIN|NOT_RESPONDING").search(node["State"]):
                    node_color = color_map["DOWN"]
                    cpu_color = color_map["DOWN"]
                    gpu_color = color_map["DOWN"]
                    mem_color = color_map["DOWN"]

                    print(key+";"+node_color+";"+node["NodeName"]+";"+cpu_color+";"+str(node["CpuTot"])+";"+str(node["CpuAlloc"])+";"
                          +gpu_color+";"+node["Gpu"][0]["GpuType"]+";"+str(node["Gpu"][0]["GpuTot"])+";"+str(node["Gpu"][0]["GpuAlloc"])+";"
                          +mem_color+";"+str(node["MemTot"])+";"+str(node["MemAlloc"]))
                    for i in range(1, len(node["Gpu"])):
                        gpu_color = calc_color(node["Gpu"][i]["GpuTot"], node["Gpu"][i]["GpuAlloc"])
                        print(key+";"+node_color+";"+node["NodeName"]+";"+cpu_color+";"+str(node["CpuTot"])+";"+str(node["CpuAlloc"])+";"
                          +gpu_color+";"+node["Gpu"][i]["GpuType"]+";"+str(node["Gpu"][i]["GpuTot"])+";"+str(node["Gpu"][i]["GpuAlloc"])+";"
                          +mem_color+";"+str(node["MemTot"])+";"+str(node["MemAlloc"]))
                        #print(key+";"+node_color+";"+node["NodeName"]+";;;;"+gpu_color+";"+node["Gpu"][i]["GpuType"]+";"
                        #      +str(node["Gpu"][i]["GpuTot"])+";"+str(node["Gpu"][i]["GpuAlloc"])+";;;")
                else:
                    node_color = color_map["UP"]
                    cpu_color = calc_color(node["CpuTot"], node["CpuAlloc"])
                    if node["Gpu"][0]["GpuTot"] == 0:
                        gpu_color = color_map["NONE"]
                    else:
                        gpu_color = calc_color(node["Gpu"][0]["GpuTot"], node["Gpu"][0]["GpuAlloc"])
                    mem_color = calc_color(node["MemTot"], node["MemAlloc"])

                    print(key+";"+node_color+";"+node["NodeName"]+";"+cpu_color+";"+str(node["CpuTot"])+";"+str(node["CpuAlloc"])+";"
                          +gpu_color+";"+node["Gpu"][0]["GpuType"]+";"+str(node["Gpu"][0]["GpuTot"])+";"+str(node["Gpu"][0]["GpuAlloc"])+";"
                          +mem_color+";"+str(node["MemTot"])+";"+str(node["MemAlloc"]))

                    for i in range(1, len(node["Gpu"])):
                        gpu_color = calc_color(node["Gpu"][i]["GpuTot"], node["Gpu"][i]["GpuAlloc"])
                        print(key+";"+node_color+";"+node["NodeName"]+";"+cpu_color+";"+str(node["CpuTot"])+";"+str(node["CpuAlloc"])+";"
                          +gpu_color+";"+node["Gpu"][i]["GpuType"]+";"+str(node["Gpu"][i]["GpuTot"])+";"+str(node["Gpu"][i]["GpuAlloc"])+";"
                          +mem_color+";"+str(node["MemTot"])+";"+str(node["MemAlloc"]))
                        #print(key+";"+node_color+";"+node["NodeName"]+";;;;"+gpu_color+";"+node["Gpu"][i]["GpuType"]+";"
                        #      +str(node["Gpu"][i]["GpuTot"])+";"+str(node["Gpu"][i]["GpuAlloc"])+";;;")

        return 

    for key in keys:
        for node in node_usage_records[key]:
            print(key+";"+node["NodeName"]+";"+str(node["CpuTot"])+";"+str(node["CpuAlloc"])+";"
                  +node["Gpu"][0]["GpuType"]+";"+str(node["Gpu"][0]["GpuTot"])+";"+str(node["Gpu"][0]["GpuAlloc"])+";"
                  +str(node["MemTot"])+";"+str(node["MemAlloc"]))

    # now print special partitions
    for key in special_partitions:
        for node in node_usage_records[key]:
            print(key+";"+node["NodeName"]+";"+str(node["CpuTot"])+";"+str(node["CpuAlloc"])+";"
                  +node["Gpu"][0]["GpuType"]+";"+str(node["Gpu"][0]["GpuTot"])+";"+str(node["Gpu"][0]["GpuAlloc"])+";"
                  +str(node["MemTot"])+";"+str(node["MemAlloc"]))
            for i in range(1, len(node["Gpu"])):
                print(Key+";;;;"+node["Gpu"][i]["GpuType"]+";"+str(node["Gpu"][i]["GpuTot"])+";"+str(node["Gpu"][i]["GpuAlloc"])+";;")

#node_records = get_node_records()
#node_usage_records = get_node_usage_by_partition(node_records)
partitions = get_partitions()
#node_usage_by_partition = get_node_usage_by_partition(get_node_records(), partitions)
print_node_usage(get_node_usage_by_partition(get_node_records(), partitions), partitions, print_header=False, add_colorcode=True, node_type=sys.argv[1], partition_type=sys.argv[2])
