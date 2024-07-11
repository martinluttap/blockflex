import csv
from collections import OrderedDict
import numpy as np
import pandas as pd
import pickle

FILE = "container_usage_sub.csv"
# FILE = "10m_container_usage_sub.csv"
# FILE = "100k_container_usage_sub.csv"
BASENAME = f"parsed-{FILE.rstrip('.csv')}"
"""
container_usage.csv format:
+-----------------------------------------------------------------------------------------+
| container_id     | string     |       | uid of a container                              |
| machine_id       | string     |       | uid of container's host machine                 |
| time_stamp       | double     |       | time stamp, in second                           |
| cpu_util_percent | bigint     |       |                                                 |
| mem_util_percent | bigint     |       |                                                 |
| cpi              | double     |       |                                                 |
| mem_gps          | double     |       | normalized memory bandwidth, [0, 100]           |
| mpki             | bigint     |       |                                                 |
| net_in           | double     |       | normarlized in coming network traffic, [0, 100] |
| net_out          | double     |       | normarlized out going network traffic, [0, 100] |
| disk_io_percent  | double     |       | [0, 100], abnormal values are of -1 or 101      |
+-----------------------------------------------------------------------------------------+
"""

def analyze_container_ids_and_usage():

    total = {}
    count = {}
    ids = set()
    max_size = 30000

    with open(FILE,'r') as infile:
        for line in infile:
            #Currently want to average the utilization over the course of the given second
            container_id, machine_id, time_stamp, cpu_util_percent, mem_util_percent, cpi, mem_gps, mkpi, net_in, net_out, disk_io_percent = line.strip().split(",")
            time_stamp = float(time_stamp)
            if cpu_util_percent:
                ids.add(container_id)
                if (len(ids)) > max_size:
                    break
                cpu_util_percent = float(cpu_util_percent)
                if cpu_util_percent < 0 or cpu_util_percent > 100: 
                    continue
                if time_stamp not in total:
                    total[time_stamp] = cpu_util_percent
                    count[time_stamp] = 1
                else:
                    total[time_stamp] += cpu_util_percent
                    count[time_stamp] += 1

    outfile = open("ali_container_usage.dat", 'w')
    out_list = []
    for key,v in sorted(total.items()):
        out_list.append(total[key]/count[key])
    print(",".join(map(str,out_list)), file=outfile)
    outfile.close()

    ids_outfile = open("ali_container_ids.dat", 'w')
    print(f'container_id\n', file=ids_outfile)
    for c in sorted(ids):
        print(f'{str(c)}\n', file=ids_outfile)
    ids_outfile.close()

def analyze_app_groups(): 
    # Out of all existing container ids, how many app groups are we seeing? 
    # Return dataframe with one sample container per app_du / service.
    # Thus, num. rows == num. unique services. 

    meta_fp: str = 'container_meta.csv'
    container_meta_df: pd.DataFrame = pd.read_csv(meta_fp, header=None)
    container_meta_df.columns = ['container_id', 'machine_id', 'time_stamp', 'app_du', 'status', 'cpu_request', 'cpu_limit', 'mem_size']
    print(container_meta_df.head())
    ids_df: pd.DataFrame = pd.read_csv('ali_container_ids.dat', sep=',')
    print(ids_df.head())
    df: pd.DataFrame = ids_df.merge(container_meta_df, how='left')
    df = df.drop_duplicates(subset='app_du', keep='last')
    print(f'Unique app_du:{len(df.app_du.unique())}, cuid:{len(df.container_id)}')

    return df

def per_container_usage_overtime(df_unique_app: pd.DataFrame, usage_fpath: str):
    container_cpu_d = {}
    ts_cid = OrderedDict()
    ts_cpu = OrderedDict()
    max_size = 30000
    infile = open(FILE, 'r')

    # Count total lines
    total_line = 0
    for line in infile:
        total_line += 1
    infile.seek(0)

    for idx, line in enumerate(infile):
        container_id, machine_id, time_stamp, cpu_util_percent, mem_util_percent, cpi, mem_gps, mkpi, net_in, net_out, disk_io_percent = line.strip().split(",")
        time_stamp = float(time_stamp)
        if cpu_util_percent and container_id:
            cpu_util_percent = float(cpu_util_percent)
            if cpu_util_percent < 0 or cpu_util_percent > 100: 
                continue
            else:
                ts_cid[time_stamp] = container_id
                ts_cpu[time_stamp] = cpu_util_percent
        print(f'[FILE][{idx+1}/{total_line}] At {time_stamp} {container_id} {cpu_util_percent}...')

    all_ts = ts_cid.keys()
    all_cids = list(OrderedDict({v: None for v in ts_cid.values()}))

    for idx, ts in enumerate(ts_cid.keys()):
        container_cpu_d[ts] = OrderedDict({
            cid: np.nan for cid in all_cids
        })
        print(f'[DICT_CREATE][{idx+1}/{len(all_ts)}] ...')

    for idx, ts in enumerate(all_ts):
        cid = ts_cid[ts]
        cpu = ts_cpu[ts]
        container_cpu_d[ts][cid]= cpu
        print(f'[DICT_FILL][{idx+1}/{len(all_ts)}] ...')

    ord_container_cpu_d = OrderedDict()
    ord_ts = sorted(list(container_cpu_d.keys()))
    for ts in ord_ts:
        ord_container_cpu_d[ts] = container_cpu_d[ts]

    with open(f'{BASENAME}.pickle', 'wb') as handle:
        pickle.dump(ord_container_cpu_d, handle, protocol=pickle.HIGHEST_PROTOCOL)

    outfile = open(f'{BASENAME}.csv', 'w')
    w = csv.writer(outfile)
    header = ['timestamp']
    header.extend(all_cids)
    w.writerow(header)
    for idx, ts in enumerate(ord_container_cpu_d.keys()):
        row = [ts] + [ord_container_cpu_d[ts][cid] for cid in all_cids]
        w.writerow(row)
        print(f'[CSV_WRITE][{idx}/{len(all_ts)}]')

    print(f'ALL DONE!')

# analyze_container_ids_and_usage()
df_unique_app: pd.DataFrame = analyze_app_groups()
per_container_usage_overtime(df_unique_app, FILE)