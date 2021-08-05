from subprocess import STDOUT, check_output, TimeoutExpired, CalledProcessError

#src = "gsiftp://oasis-dm.sdsc.xsede.org//oasis/scratch-comet/earslan/temp_project/1G-"
base_command = "mvn exec:java -Dexec.args=\"src/main/resources/config.cfg "
#folders = [60, 120, 240]
algs = ["dnn", "avg", "rand", "ar" ]
for i in range(1,10):
    for alg in algs:
        command = base_command  + " -sample "+alg + "\""
        print(command)
        try:
            #output = check_output("sleep 15", shell=True, stderr=STDOUT, timeout=5)
            output = check_output(command, shell=True, stderr=STDOUT, timeout=350)
        except (TimeoutExpired, ValueError) as e:
            pass;
        except CalledProcessError:
            continue;
        #os.system(command)
        command = "mv inst-throughput.txt inst-throughput-" + alg + "-stampede-comet-test_" + str(i) + ".txt"
        print(command)
        try:
            output = check_output(command,  shell=True, stderr=STDOUT, timeout=10)
        except TimeoutExpired:
            pass;
        #os.system(command)
