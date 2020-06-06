# ZeroMQ Context
import zmq
import argparse

ap = argparse.ArgumentParser()
ap.add_argument("-u", "--urls", required=True,
                help="URLs to PULL messages (e.g. 'tcp://127.0.0.1:5690,tcp://10.0.1.2:5691'")
args = vars(ap.parse_args())

context = zmq.Context()

sockets = []
urls = args["urls"].split(",")
for url in urls:
    # Define the socket using the "Context"
    socket = context.socket(zmq.PULL)
    socket.connect(url)
    sockets.append(socket)

while True:
    for socket in sockets:
        message = socket.recv()
        print("Received: {msg}".format(msg=message))