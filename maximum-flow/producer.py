import zmq
import time
import argparse

ap = argparse.ArgumentParser()
ap.add_argument("-p", "--port", type=int, required=True,
                help="Port to PUSH the messages on queue")
ap.add_argument("-w", "--wait", type=float, required=True,
                help="Time in seconds (ex. 0.025) of processing time")
args = vars(ap.parse_args())

url_bind = "tcp://127.0.0.1:" + str(args["port"])

# ZeroMQ Context
context = zmq.Context()

# Define the socket using the "Context"
queue_socket = context.socket(zmq.PUSH)
queue_socket.bind(url_bind)

node_id = args["port"]
msg_id = 0

while True:
    time.sleep(args["wait"])
    msg_id, now = msg_id + 1, time.ctime()

    # Message [id] - [message]
    message = "{node}-{id}: {time}".format(node=node_id, id=msg_id, time=now)

    queue_socket.send_string(message, encoding='utf-8')

    print("Sent: {msg}".format(msg=message))
