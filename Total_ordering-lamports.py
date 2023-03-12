import marshal,multiprocessing,socket,threading,time,os 
from collections import defaultdict
from typing import NamedTuple




PORTS = []
for i in range(3):
    PORTS.append(8000 + i)




class Event(NamedTuple):
        sharedObject : int
        pid : int
        




def acknowledgement( id , lock , thLock , sharedObject , sock , AckCount , Ackverfication ,list):
    while True:
        
        # read the message
            data = sock.recvfrom(1024)
            msg = marshal.loads(data[0])
            # check if ack
            event = Event(sharedObject=msg["sharedObject"], pid=msg["pid"])
        
            if msg["type"] == 1:
                with thLock:
                    AckCount[event] += 1
                    if list:
                        if AckCount[list[0]] >= 3:
                            event = list.pop()
                            print(f"P{id}-> Finished Procesing Event P{event.pid}.{event.sharedObject}")
                        elif not Ackverfication[list[0]]:
                            with lock:
                                sharedObject.value += 1
                            msg = dict(type=1, pid=event.pid, sharedObject=event.sharedObject)
                            data = marshal.dumps(msg)
                            for port in PORTS:
                                sock.sendto(data, ("localhost", port))
                            Ackverfication[event] = True

        
            # msg is a new event
            if msg["type"] == 0:
                with thLock:
                # update local sharedObject
                    with lock:
                        sharedObject.value += 1

                    list.append(event)
                    list.sort(reverse = True)
                    AckCount[event] = 0

                    if not Ackverfication[event]:
                            with lock:
                                sharedObject.value += 1
                            msg = dict( pid=event.pid, sharedObject=event.sharedObject , type=1 )
                            data = marshal.dumps(msg)
                            for port in PORTS:
                                sock.sendto(data, ("localhost", port))
                            Ackverfication[event] = True






def pro(id, sharedObject, lock ,sock):
    threadSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    threadSock.bind(("localhost", PORTS[id]))
    thLock = threading.Lock()
    AckCount = defaultdict(int)
    Ackverfication = defaultdict(bool)
    list = []
    Acknowledgement1 = threading.Thread(target = acknowledgement, args=( id , lock , thLock , sharedObject ,threadSock ,AckCount, Ackverfication ,list))
    Acknowledgement2 = threading.Thread(target = acknowledgement, args=( id , lock , thLock , sharedObject ,threadSock ,AckCount, Ackverfication ,list))
    Acknowledgement1.start()
    Acknowledgement2.start()

    time.sleep(1)
    for _ in range(3):
        time.sleep(0.01)
        with lock:   #lock as a context manager ,so that no other process can access out shared object when one process is accessing it
            sharedObject.value += 1
        msg = dict(pid=id, sharedObject=sharedObject.value, type=0)
        data = marshal.dumps(msg)
        for port in PORTS:
            sock.sendto(data, ("localhost", port))
        print(f"P{id} -> Sent event P{id}.{msg['sharedObject']}")

            

def createProcess():
     # creating and starting the processes later we are waiting to for process to complete and joining them 
    sharedObject = multiprocessing.Value("i", 0)
    lock = multiprocessing.Lock()


    p = []
    for i in range(3):
        p.append( multiprocessing.Process(target= pro , args=( i , sharedObject , lock, socket.socket(socket.AF_INET, socket.SOCK_DGRAM) )))

    for elements in p:
        elements.start()
        time.sleep(0.01)

    for elements in p:
        elements.join()




if __name__ == "__main__":
   
    createProcess()