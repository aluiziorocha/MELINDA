# ZeroMQ Context
import random
import time
import zmq
import argparse

ap = argparse.ArgumentParser()
ap.add_argument("-p", "--port", type=int, default=5555,
                help="Worker port to listen")
args = vars(ap.parse_args())

worker = "tcp://*:" + str(args["port"])


def main():
    random.seed()
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(worker)

    try:
        while True:
            start_wait = time.time()
            work_time = socket.recv()
            wait_time = int(float(time.time() - start_wait) * 1000.0)

            # do some 'work'
            time.sleep(int(work_time)*0.001)

            # send reply back to client
            socket.send_string(u'%i' % wait_time)

    except (KeyboardInterrupt, SystemExit):
        pass  # Ctrl-C was pressed to end program

    finally:
        # Stop all threads
        print("Done")


if __name__ == "__main__":
    main()
