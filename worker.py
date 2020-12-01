import socket
import sys
import threading
import time
import json

portNumber = int(sys.argv[1])

def updateMaster(msg):                                                      # once task is complete, send update to master
    while True:
        try:
            s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)          # port 5001
            s1.connect(("localhost", 5001)) 
            msg['starboard'] = portNumber                                   
            msg = json.dumps(msg)
            s1.send(msg.encode())
            s1.close()
            break
        except:
            continue

def getFromMaster():                                                    # listens to tasks sent from master
    while True:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)           # port 4000, 4001, 4002
            print('trying to connect', time.ctime(time.time()))
            s.connect(("localhost", portNumber))
            print('connected')
            msg = s.recv(1024)
            
            if(len(msg)>0):
                msg.decode("utf-8")
                msg = json.loads(msg)
                print("----------------------------------------------------------------------------------RECEIVED----------------------------------------------------------------------------------")
                if(len(msg)>0):
                    print("Message : ", msg)
                s.close()
                run_task(msg)                                           # sends tasks received to be executed 
                updateMaster(msg)                                       # after execution, need to update master
        except:
            continue

def run_task(msg):                                                      # executes task by waiting for the duration of task
    t = msg['duration']
    while(t):
        time.sleep(1)
        t -= 1
    print(f"Task{msg['task_id']} complete")

def initRecv():                                                     # to get the number of slots for the worker
    while True:                        
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)    # port 4000, 4001, 4002
            print('trying to connect', time.ctime(time.time()))
            s.connect(("localhost", portNumber))
            print('connected')
            s_no = s.recv(1024)
            print("----------------------------------------------------------------------------------RECEIVED----------------------------------------------------------------------------------")
            s.close()
            return s_no.decode("utf-8")
        except:
            continue
    

if __name__ == "__main__":
    # TCP streaming
    slots = int(initRecv())                                             # retrieving number of slots for each worker
    print("Slots: ", slots)                                             # creating threads for each of the slots
    slotThreads = [threading.Thread(target = getFromMaster) for _ in range(slots)]
    
    for i in range(slots):
        slotThreads[i].start()

    for i in range(slots):
        slotThreads[i].join()
        