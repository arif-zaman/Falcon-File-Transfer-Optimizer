configurations = {
    "interface": "ens3f1",
    "receiver": {
        "host": "134.197.113.71",
        "port": 50051
    },
    "data_dir": {
        "sender": "/data/arif/",
        "receiver": "/data/arif/"
    },
    "method":"brute",
    "bayes": {
        "initial_run": 3,
        "num_of_exp": -1
    },
    "loglevel": "info",
    "probing_sec": 10,
    "multiplier": 1,
    "emulab_test": False,
    "file_transfer": True,
    "C": 10,
    "probe_config": {
        "bsize": 10,
        "thread": 16
    },
    "limits": {
        "thread": -1,
        "bsize": 10,
    }
}