configurations = {
    "receiver": {
        "host": "10.1.2.3",
        "port": 8000
    },
    "data_dir": "/data/src/",
    "method": "probe", # options: [gradient, bayes, random, brute, probe, cg, lbfgs]
    "bayes": {
        "initial_run": 3,
        "num_of_exp": -1 #-1 for infinite
    },
    "random": {
        "num_of_exp": 10
    },
    "emulab_test": False, # True for per process I/O limit emulation
    "centralized": False, # True for centralized optimization
    "file_transfer": True,
    "B": 10, # severity of the packet loss punishment
    "K": 1.02, # cost of increasing concurrency
    "loglevel": "info",
    "probing_sec": 3, # probing interval in seconds
    "multiplier": 1, # multiplier for each files, only for testing purpose
    "mp_opt": False, # Always False for python version
    "fixed_probing": {
        "bsize": 10,
        "thread": 3
    },
    "max_cc": 100,
}