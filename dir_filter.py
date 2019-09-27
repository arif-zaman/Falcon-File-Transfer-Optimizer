#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Mar  7 14:26:20 2019

@author: bpehlivan
"""

import os

def get_dir_paths(root_path):
    dir_paths = []
    
    for abs_path, dirs, files in os.walk(root_path):
        if len(dirs) == 0:
            dir_paths.append(os.path.relpath(abs_path, root_path) + "/")
    
    return dir_paths

def filter_paths(dir_paths, filters_list):
    filtered_list = []
    for path in dir_paths:
        for filters in filters_list:
            match = True
            for fil in filters:
                if fil not in path:
                    match = False
                    break
            if ".csv" in path:
                match = False
                break
            if match:
                filtered_list.append(path)
                break
    return filtered_list

def get_filtered_dir_paths(root_path, option):
    dir_paths = get_dir_paths(root_path)
    
    filters_list = []
    
    if option == 1:
        filters_list = [["pronghorn", "cc1/"]]
    elif option == 2:
        filters_list = [["esnet", "ftp", "cc1/"], ["esnet", "ftp", "default"]]
    elif option == 3:
        filters_list = [["dtn", "ftp", "cc1/"], ["dtn", "ftp", "default"]]
    elif option == 4:
        filters_list = [["comet"]]
    elif option == 5:
        filters_list = [["sc11-25"]]
    elif option == 37:
        filters_list = [["pronghorn", "cc1/"], ["pronghorn", "default"],\
                                     ["esnet", "ftp", "cc1/"], ["esnet", "ftp", "default"]]
    
    dir_paths = filter_paths(dir_paths, filters_list)
    
    for i in range(len(dir_paths)):
        dir_paths[i] = os.path.join(root_path, dir_paths[i])
            
    return dir_paths