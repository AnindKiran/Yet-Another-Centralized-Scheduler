import socket
import json
import threading
import time
from datetime import datetime
import copy
import sys
import random
import re

config_data = 0                                                                     # list of data given in the config.json file 
sockets = dict()                                                                    # Key - socket no. and value = sockets 
ports = []                                                                          # list of port numbers assigned to workers 
requests_data = dict()                     
requestsdataLock = threading.Lock()                                                 #lock for the requests_data dictionary resource 
loglock = threading.Lock()                                                          #lock on the log file resource 
configdataLock = threading.Lock()                                                   #lock on the config_data resource 

def roundRobin():                                                                   #order of worker id
    configdataLock.acquire()
    localData = copy.deepcopy(config_data)
    configdataLock.release()

    localData.sort(key = lambda x : x['worker_id'])                                 #increasing order of worker id
    while True:                                                                     
        for i in localData:                                                         
            if (i['slots'] != 0):                                               
                return i['port']

        configdataLock.acquire()                                                    #if not available
        localData = copy.deepcopy(config_data)                                      #opt for next
        configdataLock.release()
        localData.sort(key = lambda x : x['worker_id'])       
        
def leastLoaded():                                                                  #maximum empty slots 
    configdataLock.acquire()
    localData = copy.deepcopy(config_data)
    configdataLock.release()
    localData.sort(key = lambda x : x['slots'], reverse = True)                     #decreasing order of empty slots 
    while True:
        if(localData[0]['slots'] != 0):                                             
            return localData[0]['port']
        else:                                                                       #if not available, wait
            time.sleep(1)
            configdataLock.acquire()
            localData = copy.deepcopy(config_data)
            configdataLock.release()
            localData.sort(key = lambda x : x['slots'], reverse = True)             

def raNdOm():
    worker_idList = [i['worker_id'] for i in config_data]
    while True:
        num = random.choice(worker_idList)                                           #random worker picked
        for i in config_data:
            if i['worker_id'] == num:
                if i['slots'] != 0:
                    return i['port']
                else:                                                                #if slots not available
                    break                                                            #another worker picked
    
def callAlgorithm(algorithm):                                                        #alogirthm finder
    if(algorithm == 'RR' or algorithm == 'rr'):
        return roundRobin()

    elif(algorithm == 'LL' or algorithm == 'll'):
        return leastLoaded()
    else:
        return raNdOm()

def masterDispatcher(job):

    for i in job:
        portNumber = callAlgorithm(sys.argv[2])                                       #using the scheduling algorithm
        pos = ports.index(portNumber)                                                 #find the worker to dispatch the job to
        w=pos
        try:
            tid=i["task_id"]
            clienttosocket, _ = sockets["s" + str(pos + 2)].accept()                  #task dispatched
            msg = json.dumps(i)
            clienttosocket.send(msg.encode())
                        
            for j in config_data:                                                     #decrement the no. of
                if (j["port"] == portNumber):                                         #available slots in 
                    configdataLock.acquire()                                          #the worker 
                    j["slots"] -= 1 
                    configdataLock.release()
                    break
 
            loglock.acquire()
            sttime = datetime.now().strftime('%H:%M:%S - ')
            log = r"log.txt"
            with open(log, "a") as logfile:
                logfile.write('01 ' + sttime + tid + " " + "w" + str(w+1) + '\n')     #status code - 01
            loglock.release()            

        except:
            pass

def masterListenerJob():
    while True:
        clienttosocket, _ = sockets["s0"].accept()
        msg = clienttosocket.recv(1024)                                               #received job request
        msg = msg.decode("utf-8")
        msg = json.loads(msg)
        map = msg['map_tasks']
        job_id = msg['job_id']                                                        
        red = msg['reduce_tasks']

        requestsdataLock.acquire()
        requests_data[job_id] = []
        requests_data[job_id].extend((len(map), len(red), red))                       #job request saved 
        requestsdataLock.release()

        loglock.acquire()
        sttime = datetime.now().strftime('%H:%M:%S - ')
        log = r'log.txt'
        with open(log, 'a') as logfile:
            logfile.write('00 ' + sttime + job_id + '\n')                              #status code - 00
        loglock.release()

        print('\n')
        if(len(map) != 0):                                                             #if map tasks remaining
            print('Job ID = ', job_id)
            masterDispatcher(map)                                                      #dispatch map tasks 

def masterListenerWorker():                                                            #task completion status 
    global config_data
    while True:
        clienttosocket, _ = sockets["s1"].accept()
        msg = clienttosocket.recv(1024)                                                #placeholder to send recv relevant info
        msg = msg.decode("utf-8")                   
        if(len(msg) > 0):                                               
            msg = json.loads(msg)                                                      #task completed by worker

            loglock.acquire()                                               
            sttime = datetime.now().strftime('%H:%M:%S - ')                         
            log = r'log.txt'
            with open(log, 'a') as logfile:
                logfile.write('10 ' + sttime + msg["task_id"] +'\n')                   #status code - 10
            loglock.release() 

            starboard = int(msg['starboard'])
            print("Update from worker: ", msg, " from port", starboard)
            for i in config_data:
                if((i["port"]) == starboard):                                          #free the slot of the worker
                    configdataLock.acquire()
                    i["slots"] += 1
                    configdataLock.release()
                    break

            job_id = msg['task_id'].split("_")[0]
            ind = 0 if msg['task_id'].split("_")[1][0] == 'M' else 1
            requestsdataLock.acquire()
            requests_data[job_id][ind] -= 1
            if requests_data[job_id][1] == 0:                                          #checks if the no.of                       
                loglock.acquire()                                                      #remaining reduce tasks =0
                sttime = datetime.now().strftime('%H:%M:%S - ')                        #if yes, job completed  
                log = r'log.txt'                                                        
                with open(log, 'a') as logfile:
                    logfile.write('11 ' + sttime + job_id + '\n')                      #status code - 11
                loglock.release()
                print("-"*40)
                print("\t\t", f"Job {job_id} complete")
                print("-"*40)
            requestsdataLock.release()


def completionChecker():                                                                #checks for completion 
    iteratedKeys = set()                                                                # of map tasks 
    while True:                                                                         #before assigning reduce tasks 
        keys = set(requests_data.keys()) - iteratedKeys 
        for i in list(keys):
            if requests_data[i][0] == 0:
                iteratedKeys.add(i)
                masterDispatcher(requests_data[i][2])

def initConnect(i, j):                                                                  #establishing connection 
    while True:                                                                         #with the worker 
        try:                                                                            #allocating the no.of slots
            print('Ready to accept connection ', time.ctime(time.time())," on Port Index No: ", str(j + 2))
            clienttosocket, _ = sockets["s" + str(j + 2)].accept()
            print("Established")
            msg = json.dumps(i['slots'])
            clienttosocket.send(msg.encode())  
            return
        except:
            continue

def initSend():                                                                           #initialising the connection 
    init1 = threading.Thread(target = initConnect, args=(config_data_original[0], 0))     #with the workers 
    init2 = threading.Thread(target = initConnect, args=(config_data_original[1], 1))
    init3 = threading.Thread(target = initConnect, args=(config_data_original[2], 2))

    init1.start()
    init2.start()
    init3.start()

    init1.join()
    init2.join()
    init3.join()

def main():
    fh = open(sys.argv[1], "r")                                                         #config file 
    loglock.acquire()
    logfile=open(r"log.txt","a+")                                                       #noting the alogirthm into the log file 
    logfile.write("***"+sys.argv[2]+"***\n")
    logfile.close()
    loglock.release()
    global config_data
    global config_data_original

    data = json.load(fh)                                                                #converting the config file data 
    config_data = data['workers']                                                       #into a list of worker details
    config_data_original = copy.deepcopy(config_data)

    for i in config_data:
        ports.append(i['port'])                                                         # List of ports in config file

    for i in range(5):
        sockets["s" + str(i)] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)       # s0, s1, s2, s3, s4

    sockets["s0"].bind(("localhost", 5000))                                    # Listens for incoming jobs
    sockets["s0"].listen(5)                                                             # Listening
    
    sockets["s1"].bind(("localhost", 5001))                                    # Listens to worker updates
    sockets["s1"].listen(5)                                                             # Listening


    for i in range(2,5):                                                               
        sockets["s" + str(i)].bind(("localhost", ports[i - 2]))                #binding of worker sockets  
        for j in config_data_original:
            if j['port'] == ports[i - 2]:
                number = int(j['slots'])                                                # assigning the connections 
                sockets["s" + str(i)].listen(number)
                break
        
    initSend()
                                                                      
    t0 = threading.Thread(target = masterListenerJob)                                   # Running all sockets on diff threads.
    t1 = threading.Thread(target = masterListenerWorker)
    t2 = threading.Thread(target = completionChecker)

    t0.start()
    t1.start()
    t2.start()

    t0.join()
    t1.join()
    t2.join()


if __name__ == "__main__":
    main()

