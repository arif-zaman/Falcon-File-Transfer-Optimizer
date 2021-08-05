import os

src = "gsiftp://oasis-dm.sdsc.xsede.org//oasis/scratch-comet/earslan/temp_project/1G-"
base_command = "mvn exec:java -Dexec.args=\"src/main/resources/config.cfg "
folders = [60, 120, 240]
algs = ["rand", "dnn", "ar", "avg"]
for folder in folders:
    for i in range(5):
        for alg in algs:
            command = "gtimeout " + str(folder*3) + " " + base_command + "-s " + src + str(folder) + "/ -sample "+alg + "\""
            print(command)
            os.system(command)
