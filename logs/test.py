import os

def tcp_stats():
    sendq = 0
    RCVR_ADDR = "192.168.1.2:50051"

    try:
        data = os.popen("ss -ti").read().split("\n")
        for i in range(1,len(data)):
            if RCVR_ADDR in data[i-1]:
                sendq += int([i for i in data[i-1].split(" ") if i][2])
        
    except Exception as e:
        print(e)

    return sendq