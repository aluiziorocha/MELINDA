# This is the Broker component managing the communications among nodes

import queue
import sys
import threading
import traceback
import imagezmq

# Communication pattern among Broker and FLO nodes
UNICAST = 0
MULTICAST = 1

flo_urls = ['tcp://localhost:5565', 'tcp://192.168.0.8:5555']  # , 'tcp://192.168.0.8:5555'
dlo_urls = ['tcp://localhost:5575']

# Global queues accessed by threads
imq = queue.Queue()  # Input message queue (FIFO - First-In, First-Out)
omq = queue.Queue()  # Output message queue
timq = {}            # Dictionary for accessing thread's input message queue

# Broker's hub to receive messages from MLO nodes
image_hub = imagezmq.ImageHub(open_port='tcp://*:5555')  # Hub to receive images of interest


def flo_task(mode, node_url):
    t = threading.currentThread()
    sender = imagezmq.ImageSender(connect_to=node_url)
    while getattr(t, "do_run", True):
        try:
            # Get the first item queued.
            # If queue is empty raise the Empty exception
            if mode == UNICAST:
                (node_name, jpg_buffer) = imq.get(block=False)
                reply = sender.send_jpg(node_name, jpg_buffer)
                omq.put((node_name, reply))
                # remove item from queue
                imq.task_done()
            elif mode == MULTICAST:
                (node_name, jpg_buffer) = timq[node_url].get(block=False)
                reply = sender.send_jpg(node_name, jpg_buffer)
                omq.put((node_name, reply))
                timq[node_url].task_done()
        except queue.Empty:
            pass

    print("Closing connection with FLO node:", node_url)
    sender.close()  # close the ZMQ socket and context


def dlo_task(node_url):
    t = threading.currentThread()
    sender = imagezmq.ImageSender(connect_to=node_url)
    while getattr(t, "do_run", True):
        try:
            # Get the first item queued.
            # If queue is empty raise the Empty exception
            (node_name, jpg_buffer) = omq.get(block=False)
            sender.send_jpg(node_name, jpg_buffer)
            # remove item from queue
            omq.task_done()
        except queue.Empty:
            pass
    print("Closing connection with DLO node:", node_url)
    sender.close()  # close the ZMQ socket and context


def main(mode=UNICAST):

    if mode == MULTICAST:
        # Prepare input queue for each thread
        for flonode in flo_urls:
            timq[flonode] = queue.Queue()

    # Start background task threads
    def thread_gen(mode, flnodes, dlnodes):
        flthreads = []
        dlthreads = []
        for flnode in flnodes:
            t = threading.Thread(target=flo_task, args=(mode, flnode,))
            flthreads.append(t)
        for dlnode in dlnodes:
            t = threading.Thread(target=dlo_task, args=(dlnode,))
            dlthreads.append(t)
        return flthreads, dlthreads

    flthreads, dlthreads = thread_gen(mode, flo_urls, dlo_urls)
    allthreads = flthreads + dlthreads
    for thread in allthreads:
        thread.start()

    try:
        while True:
            node_name, jpg_buffer = image_hub.recv_jpg()
            image_hub.send_reply()
            if mode == UNICAST:
                imq.put((node_name, jpg_buffer))
            elif mode == MULTICAST:
                for flonode in flo_urls:
                    timq[flonode].put((node_name, jpg_buffer))

            # code to check if input queue is full and new
            # FLO nodes must rise to process it

    except (KeyboardInterrupt, SystemExit):
        pass  # Ctrl-C was pressed to end program

    except Exception as ex:
        print('Python error with no Exception handler:')
        print('Traceback error:', ex)
        traceback.print_exc()

    finally:
        # Stop all threads
        print("Stopping threads...")
        for thread in allthreads:
            thread.do_run = False
            thread.join()
        print("Done")
        sys.exit()


if __name__ == "__main__":
    main(mode=MULTICAST)
