#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Mar  7 14:18:30 2019

@author: bpehlivan
"""

def get_first_n_data(f_path, n):
    seconds = []
    throughputs = []
    with open(f_path, "r") as f:
        for line in f:
            data = line.rstrip().split()
            try:
                seconds.append(float(data[0]))
                throughputs.append(float(data[2]))
                if len(seconds) == n:
                    break
            except:
                pass
    return seconds, throughputs

def get_all_data(f_path):
    seconds = []
    throughputs = []
    line_cnt = 0
    
    with open(f_path, "r") as f:
        for line in f:
            data = line.rstrip().split()
            if line_cnt>50:
                break
                
            try:
                seconds.append(float(data[0]))
                throughputs.append(float(data[2]))
            except:
                pass
            
            line_cnt+=1
            
    while len(throughputs)>=2 and throughputs[0] == 0 and throughputs[1] == 0:
        del throughputs[0]
        
    while len(seconds) > len(throughputs):
        del seconds[-1]
        
    return seconds[:30], throughputs[:30]