import os

#src = "gsiftp://oasis-dm.sdsc.xsede.org//oasis/scratch-comet/earslan/temp_project/1G-"
base_command = "mvn exec:java -Dexec.args=\"src/main/resources/config.cfg "
#folders = [60, 120, 240]
algs = ["rand", "dnn", "ar", "avg"]
for i in range(3):
    for alg in algs:
        command = "gtimeout 250 " + base_command + "-s " + "/ -sample "+alg + "\""
        print(command)
        os.system(command)

