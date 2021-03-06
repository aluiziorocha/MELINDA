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
ap.add_argument("-f", "--fps", default='60,30,10',
                help="Processing capacities in FPS separated by comma, e.g, '40,20,10'")
ap.add_argument("-m", "--messages", type=int, default=100,
                help="Number of messages to process")
args = vars(ap.parse_args())

worker_urls = []
for i in range(args["port"], args["port"] + args["workers"]):
    worker_urls.append("tcp://localhost:{}".format(i))

listFPS = args["fps"].split(',')
worker_time_to_process = []
for fps in listFPS:
    worker_time_to_process.append(int(1000/int(fps)))

# Global queues accessed by threads
omq = queue.Queue()  # Output message queue
timq = {}            # Dictionary for accessing thread's input message queue


def worker_thread(node_url, time_to_process, context=None):
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
            _ = timq[node_url].get(block=False)
            socket.send_string(u'%i' % time_to_process)
            time_waiting = socket.recv()
            omq.put((node_url, time_to_process, int(time_waiting)))
            # remove item from queue
            timq[node_url].task_done()
        except queue.Empty:
            pass

    print("Closing connection with FLO node:", node_url)
    socket.close()  # close the ZMQ socket and context


def main():
    random.seed()

    # Start background task threads
    wrkthreads = []
    for node, time_to_process in zip(worker_urls, worker_time_to_process):
        timq[node] = queue.Queue()
        t = threading.Thread(target=worker_thread, args=(node, time_to_process,))
        wrkthreads.append(t)

    for thread in wrkthreads:
        thread.start()

    try:
        msg_number = 0
        while msg_number < args["messages"]:
            for node in worker_urls:
                work_time = random.randint(1, 1000)
                timq[node].put(work_time)
                msg_number += 1

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
        total_messages = 0
        for node in msg_counts:
            print("{}\t{}\t\t{}, {:.1f}\t\t\t\t\t{}, {:.3f}".format(node, msg_counts[node], time_proc[node],
                                                          time_proc[node] / msg_counts[node],
                                                          time_wait[node], time_wait[node] / msg_counts[node]))
            total_messages += msg_counts[node]
        print("-"*20 + "\t{} messages".format(total_messages))
        print("Done")


if __name__ == "__main__":
    main()
