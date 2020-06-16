# This is the Broker component managing the communications among nodes

import queue
import sys
import threading
import traceback
import imagezmq

flo_urls = ['tcp://localhost:5565']  # , 'tcp://192.168.0.8:5555'
dlo_urls = ['tcp://localhost:5575']

imq = queue.Queue()  # Input Messages Queue (FIFO - First-In, First-Out)
omq = queue.Queue()  # Output Messages Queue
image_hub = imagezmq.ImageHub(open_port='tcp://*:5555')  # Hub to receive images of interest


def flo_task(node_url):
    t = threading.currentThread()
    sender = imagezmq.ImageSender(connect_to=node_url)
    while getattr(t, "do_run", True):
        # get the first item queued. It's blocking
        # until there is an item on queue
        (node_name, jpg_buffer) = imq.get()
        reply = sender.send_jpg(node_name, jpg_buffer)
        omq.put((node_name, reply))
        # remove item from queue
        imq.task_done()
    print("Closing connection with FLO node ", node_url)
    sender.close()  # close the ZMQ socket and context


def dlo_task(node_url):
    t = threading.currentThread()
    sender = imagezmq.ImageSender(connect_to=node_url)
    while getattr(t, "do_run", True):
        # get the first item queued. It's blocking
        # until there is an item on queue
        (node_name, jpg_buffer) = omq.get()
        sender.send_jpg(node_name, jpg_buffer)
        # remove item from queue
        omq.task_done()
    print("Closing connection with DLO node ", node_url)
    sender.close()  # close the ZMQ socket and context


def main():
    # Start background task threads
    def thread_gen(fltasks, dltasks):
        threads = []
        for fltask in fltasks:
            t = threading.Thread(target=flo_task, args=(fltask,))
            threads.append(t)
        for dltask in dltasks:
            t = threading.Thread(target=dlo_task, args=(dltask,))
            threads.append(t)
        return threads

    threads = thread_gen(flo_urls, dlo_urls)
    for thread in threads:
        thread.start()

    try:
        while True:
            node_name, jpg_buffer = image_hub.recv_jpg()
            image_hub.send_reply()
            imq.put((node_name, jpg_buffer))
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
        for thread in threads:
            thread.do_run = False
            thread.join()
        print("Done")
        sys.exit()


if __name__ == "__main__":
    main()
