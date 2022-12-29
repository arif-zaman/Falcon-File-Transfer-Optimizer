## Falcon Configurations

configurations = {
    "receiver": {
        "host": "10.1.2.3",
        "port": 50022
    },
    "data_dir": "/data/arif/",
    "method": "mgd", # options: [gradient, bayes, random, brute, probe, cg, lbfgs]
    "bayes": {
        "initial_run": 3,
        "num_of_exp": -1 #-1 for infinite
    },
    "random": {
        "num_of_exp": 10
    },
    "centralized": False, # True for centralized optimization
    "file_transfer": True,
    "B": 10, # severity of the packet loss punishment
    "K": 1.05, # cost of increasing concurrency
    "loglevel": "info",
    "probing_sec": 3, # probing interval in seconds
    "multiplier": 1, # multiplier for each files, only for testing purpose
    "mp_opt": True,
    "network_limit": -1, # Network limit (Mbps) per thread
    "io_limit": -1, # I/O limit (Mbps) per thread
    "memory_use": {
        "maximum": 50,
        "threshold": 15,

    },
    "fixed_probing": {
        "bsize": 10,
        "thread": 3
    },
    "max_cc": {
        "network": 15,
        "io": 10
    },
}
