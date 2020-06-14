# ZeroMQ Context
import time
import zmq
import argparse

ap = argparse.ArgumentParser()
ap.add_argument("-u", "--urls", required=True,
                help="URLs to PULL input messages (e.g. 'tcp://127.0.0.1:5690,tcp://10.0.1.2:5691'")
ap.add_argument("-p", "--port", type=int, required=True,
                help="Port to PUSH output messages on queue")
ap.add_argument("-w", "--wait", type=float, required=True,
                help="Time in seconds (ex. 0.025) of processing time")
args = vars(ap.parse_args())

context = zmq.Context()

url_bind = []
sockets = []
urls = args["urls"].split(",")
for url in urls:
    url_bind.append(url)
    # Define the socket using the "Context"
    socket = context.socket(zmq.PULL)
    socket.connect(url_bind)
    sockets.append(socket)

# Define the socket to PUSH output messages
queue_socket = context.socket(zmq.PUSH)
queue_socket.bind(url_bind)

while True:
    for socket in sockets:
        message = socket.recv()
        print("Received: {msg}".format(msg=message))

        # Processing the message
        time.sleep(args["wait"])

        # Sending output message
        queue_socket.send(message)
        print("Sent: {msg}".format(msg=message))