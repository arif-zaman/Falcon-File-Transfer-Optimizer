import multiprocessing as mp
import os

import numpy as np

from config_sender import configurations
from multiprocessing.managers import BaseManager


class SimpleClass(object):
    def __init__(self):
        self.root = []
        self.file_names = []
        self.file_sizes = []
        self.file_count = 0
        self.file_incomplete = mp.Value("i", self.file_count)
        self.file_offsets = mp.Array("d", [0.0 for i in range(self.file_count)])

    def get_root(self):
        return self.root

    def get_file_names(self):
        return self.file_names

    def get_file_sizes(self, index):
        return self.file_sizes[index]

    def get_file_count(self):
        return self.file_count

    def get_file_incomplete(self):
        return self.file_incomplete.value

    def get_total_file_offsets(self):
        return np.sum(self.file_offsets)

    def get_file_offsets(self, index):
        return self.file_offsets[index]

    def set_root(self, a):
        self.root = a

    def set_file_names(self, a):
        self.file_names = a

    def set_file_sizes(self, a):
        self.file_sizes = a

    def set_file_count(self, a):
        self.file_count = a

    def set_file_incomplete(self, a):
        self.file_incomplete.value = a

    def set_file_offsets(self, a, offset):
        self.file_offsets[a] = offset

    def set(self, vroot):
        print(vroot)
        file_names = os.listdir(vroot)
        file_sizes = [os.path.getsize(vroot + filename) for filename in file_names]
        file_count = len(os.listdir(vroot))
        self.file_incomplete = mp.Value("i", self.file_incomplete.value + file_count)
        self.file_offsets = mp.Array("d", [i for i in self.file_offsets] + [0.0 for i in range(file_count)])
        self.file_names = self.file_names + file_names
        self.file_sizes = self.file_sizes + file_sizes
        self.file_count = self.file_count + file_count
        self.root = self.root + [vroot for i in range(file_count)]
        print("file_incomplete", self.file_incomplete)

    def print_state(self):
        print("----------------------------")
        print("root", self.root)
        print("file_names", self.file_names)
        print("file_sizes", self.file_sizes)
        print("file_count", self.file_count)
        print("file_incomplete", self.file_incomplete.value)
        print("file_offsets", self.file_offsets)
        print("----------------------------")


class Fs:
    def __init__(self):

        BaseManager.register('SimpleClass', SimpleClass)
        manager = BaseManager()
        manager.start()
        self.filesIn = manager.SimpleClass()
        self.HOST, self.PORT = 0, 0
        self.RCVR_ADDR = str(self.HOST) + ":" + str(self.PORT)


        configurations["thread_limit"] = configurations["max_cc"]
        configurations["cpu_count"] = mp.cpu_count()
        if configurations["thread_limit"] == -1:
            configurations["thread_limit"] = configurations["cpu_count"]
        self.manager = mp.Manager()
        self.probing_time = configurations["probing_sec"]
        self.throughput_logs = self.manager.list()
        self.chunk_size = 1 * 1024 * 1024
        self.num_workers = mp.Value("i", 0)
        self.process_status = mp.Array("i", [0 for i in range(configurations["thread_limit"])])

        self.emulab_test = False
        if "emulab_test" in configurations and configurations["emulab_test"] is not None:
            self.emulab_test = configurations["emulab_test"]

        self.file_transfer = True
        if "file_transfer" in configurations and configurations["file_transfer"] is not None:
            self.file_transfer = configurations["file_transfer"]
        self.q = self.manager.Queue()

    def set_connection(self, vhost, vport):
        self.HOST, self.PORT = vhost, vport
        self.RCVR_ADDR = str(self.HOST) + ":" + str(self.PORT)

    def change_obj_value(self, path):
        self.filesIn.set(path)
        for i in range(self.q.qsize(), self.q.qsize() + self.filesIn.get_file_count()):
            self.q.put(i)
        print("self.q.qsize()",self.q.qsize())

    def add_to_queue(self, vroot):
        # p = mp.Process(target=self.change_obj_value, args=(vroot,))
        # p.start()
        # p.join()
        self.change_obj_value(vroot)
