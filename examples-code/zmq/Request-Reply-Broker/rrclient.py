#
#   Request-reply client in Python
#   Connects REQ socket to tcp://localhost:5559
#   Sends "Hello" to server, expects "World" back
#
import threading
import zmq


def client_routine(id, context=None):
    """Client routine"""
    context = context or zmq.Context.instance()
    # Socket to talk to dispatcher
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://localhost:5559")

    socket.send_string("Req #{}".format(id))
    message = socket.recv_string()
    print("Received reply %s [%s]" % (id, message))


def main():
    # Launch pool of worker threads
    for i in range(10):
        thread = threading.Thread(target=client_routine, args=(i,))
        thread.start()


if __name__ == "__main__":
    main()
