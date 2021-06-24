# Falcon: Online File Transfers Optimization
The application can only correctly function on Linux-based operating systems due to several Linux-based functional dependencies. There are two configuration files for specifying the source, destinations, maximum allowed thread, and many other options. 
    
Please use python3 and install necessary packages using the requirements.txt file, preferably in a virtual environment, to avoid package version conflicts. For GridFTP client optimization, please follow the link below. 
    
GridFTP version - https://github.com/arif-zaman/Falcon-GridFTP
    

## Usage

1. Please create virtual environments on both source and destination server. For exmaple: run `python3 -m venv <venv_dir>/falcon`
2. Activate virtual environment: run `source <venv_dir>/falcon/bin/activate`
3. Install required python packages: `pip3 install -r requirements.txt`
4. On the destination server, please edit `config_receiver.py` and run `python3 receiver.py`
5. On the source server, please edit `config_sender.py` and run `python3 sender.py`
