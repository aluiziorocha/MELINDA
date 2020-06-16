#
#   Request-reply service in Python
#   Connects REP socket to tcp://localhost:5560
#
import zmq
import threading
import time
import random


def worker_routine(url, id, context=None):
    """Worker routine"""
    context = context or zmq.Context.instance()
    # Socket to talk to dispatcher
    socket = context.socket(zmq.REP)
    socket.connect(url)

    while True:
        string = socket.recv()
        print("Worker {}: Message request: {}".format(id, string))

        # do some 'work'
        work_time = random.randint(1, 100) * 0.01
        time.sleep(work_time)

        # send reply back to client
        socket.send_string("Worker {} took {:.3f}s".format(id, work_time))


def main():
    random.seed()
    url_broker = "tcp://localhost:5560"

    # Launch pool of worker threads
    for i in range(5):
        thread = threading.Thread(target=worker_routine, args=(url_broker, i,))
        thread.start()


if __name__ == "__main__":
    main()
