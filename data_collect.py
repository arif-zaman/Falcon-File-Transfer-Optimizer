import os

base_format = "python3 sender.py --dirc {0} --cc {1}"
src_dir = "/data/arif/{0}/"
ccs = [1,2,4,8,16]
filesizes = ["large", "medium", "small"]

count = 0
while True:
    count += 1 
    for cc in ccs:
        for filesize in filesizes:
            src = src_dir.format(filesize)
            command = base_format.format(src, cc)
            print(count, command)
            data = os.popen(command).read()
