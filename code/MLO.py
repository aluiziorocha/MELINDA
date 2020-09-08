# This is the template for the operator running a Measurement Level Task
import datetime
import json
import socket
import time
import traceback
import cv2
from imutils.video import VideoStream
import imagezmq

broker_url = 'tcp://localhost:5555'
sender = imagezmq.ImageSender(connect_to=broker_url)

node_name = socket.gethostname()  # send RPi hostname with each image
camera = VideoStream(usePiCamera=False).start()
time.sleep(2.0)    # allow camera sensor to warm up
jpeg_quality = 90  # 0 to 100, higher is better quality, 95 is cv2 default

try:
    while True:  # send images as stream until Ctrl-C
        image = camera.read()
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S-%f')
        ret_code, jpg_buffer = cv2.imencode(
            ".jpg", image, [int(cv2.IMWRITE_JPEG_QUALITY), jpeg_quality])
        jsondata = {'sensor_id': node_name,
                    'timestamp': timestamp}
        jsonstr = json.dumps(jsondata)
        sender.send_jpg(jsonstr, jpg_buffer)

except (KeyboardInterrupt, SystemExit):
    pass  # Ctrl-C was pressed to end program

except Exception as ex:
    print('Python error with no Exception handler:')
    print('Traceback error:', ex)
    traceback.print_exc()

finally:
    camera.stop()  # stop the camera thread
    sender.close()  # close the ZMQ socket and context

