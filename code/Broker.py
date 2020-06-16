# This is the Broker component managing the communications among nodes

import queue
import threading
import traceback
import cv2
import imagezmq
import numpy as np
# , 'tcp://192.168.0.8:5555'
workers_url = ['tcp://localhost:5565']

mq = queue.Queue()  # Messages Queue as FIFO (First-In, First-Out)
image_hub = imagezmq.ImageHub()  # Hub to receive images of interest
frameDict = {}


def worker_task(worker_url):
    sender = imagezmq.ImageSender(connect_to=worker_url)
    while True:
        # get the first item queued. It's blocking
        # until there is an item on queue
        (node_name, jpg_buffer) = mq.get()
        reply = sender.send_jpg(node_name, jpg_buffer)
        image = cv2.imdecode(np.frombuffer(reply, dtype='uint8'), -1)
        frameDict[node_name] = image
        # remove item from queue
        mq.task_done()


def main():
    # Start background tasks
    for worker_url in workers_url:
        thread = threading.Thread(target=worker_task, args=(worker_url,))
        thread.start()

    try:
        while True:
            node_name, jpg_buffer = image_hub.recv_jpg()
            image_hub.send_reply()
            mq.put((node_name, jpg_buffer))
            for key in frameDict.keys():
                cv2.imshow(key, frameDict[key])
            # detect any kepresses
            key = cv2.waitKey(1) & 0xFF
            # if the `q` key was pressed, break from the loop
            if key == ord("q"):
                break

    except (KeyboardInterrupt, SystemExit):
        pass  # Ctrl-C was pressed to end program

    except Exception as ex:
        print('Python error with no Exception handler:')
        print('Traceback error:', ex)
        traceback.print_exc()

    finally:
        cv2.destroyAllWindows()
        print("Done.")


if __name__ == "__main__":
    main()
