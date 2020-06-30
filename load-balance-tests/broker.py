# This is the Broker component managing the communications among nodes
import argparse
import queue
import threading
import time
import traceback
import zmq
import random

ap = argparse.ArgumentParser()
ap.add_argument("-p", "--port", type=int, default=5555,
                help="First Worker port to connect")
ap.add_argument("-w", "--workers", type=int, default=3,
                help="Number of workers to connect")
ap.add_argument("-m", "--messages", type=int, default=99,
                help="Number of messages to process")
args = vars(ap.parse_args())

worker_urls = []
for i in range(args["port"], args["port"] + args["workers"]):
    worker_urls.append("tcp://localhost:{}".format(i))

# Global queues accessed by threads
imq = queue.Queue()  # Input message queue (FIFO - First-In, First-Out)
omq = queue.Queue()  # Output message queue


def worker_thread(node_url, context=None):
    t = threading.currentThread()
    context = context or zmq.Context.instance()
    socket = context.socket(zmq.REQ)
    socket.connect(node_url)
    # send 0 to cancel previous waiting time
    socket.send(b'0')
    _ = socket.recv()
    while getattr(t, "do_run", True):
        try:
            # Get the first item queued.
            # If queue is empty raise the Empty exception
            time_to_process = imq.get(block=False)
            socket.send_string(u'%i' % time_to_process)
            time_waiting = socket.recv()
            omq.put((node_url, time_to_process, int(time_waiting)))
            # remove item from queue
            imq.task_done()
        except queue.Empty:
            pass

    print("Closing connection with FLO node:", node_url)
    socket.close()  # close the ZMQ socket and context


def main():
    random.seed()

    # Start background task threads
    wrkthreads = []
    for node in worker_urls:
        t = threading.Thread(target=worker_thread, args=(node,))
        wrkthreads.append(t)

    for thread in wrkthreads:
        thread.start()

    try:
        for _ in range(args["messages"]):
            work_time = random.randint(1, 1000)
            imq.put(work_time)

    except (KeyboardInterrupt, SystemExit):
        pass  # Ctrl-C was pressed to end program

    except Exception as ex:
        print('Python error with no Exception handler:')
        print('Traceback error:', ex)
        traceback.print_exc()

    finally:
        # Wait a period to finish all tasks
        time.sleep(args["messages"] / args["workers"])
        # Stop all threads
        print("Stopping threads...")
        for thread in wrkthreads:
            thread.do_run = False
            thread.join()

        msg_counts = {}
        time_proc = {}
        time_wait = {}
        for _ in range(omq.qsize()):
            node_url, time_to_process, time_waiting = omq.get()
            if node_url in msg_counts:
                msg_counts[node_url] += 1
                time_proc[node_url] += time_to_process
                time_wait[node_url] += time_waiting
            else:
                msg_counts[node_url] = 1
                time_proc[node_url] = time_to_process
                time_wait[node_url] = time_waiting
            print("{}\t{}\t{}".format(node_url, time_to_process, time_waiting))
            omq.task_done()

        print("Statistics per node --------")
        print("Node\t\t\t\tMessages\tProcessing (total, avg)\t\tWaiting (total, avg)")
        for node in msg_counts:
            print("{}\t{}\t\t{}, {:.1f}\t\t\t\t\t{}, {:.3f}".format(node, msg_counts[node], time_proc[node],
                                                          time_proc[node] / msg_counts[node],
                                                          time_wait[node], time_wait[node] / msg_counts[node]))
        print("------\nDone")


if __name__ == "__main__":
    main()
