# This is the template for the operator running a Feature Level Task
import json
import traceback
import numpy as np
import cv2
import imagezmq
from imutils import resize

jpeg_quality = 90
image_hub = imagezmq.ImageHub(open_port='tcp://*:5565')

try:
    while True:
        jsonstr, jpg_buffer = image_hub.recv_jpg()
        jsondata = json.loads(jsonstr)
        jsondata['jpeg_quality'] = jpeg_quality
        # print("Received image from {}".format(node_name))
        image = cv2.imdecode(np.frombuffer(jpg_buffer, dtype='uint8'), -1)
        image = resize(image, width=400)
        _, jpg_buffer = cv2.imencode(".jpg", image,
                                     [int(cv2.IMWRITE_JPEG_QUALITY), jpeg_quality])
        jsonstr = json.dumps(jsondata)
        image_hub.send_reply(jsonstr.encode('utf-8'))

except (KeyboardInterrupt, SystemExit):
    pass  # Ctrl-C was pressed to end program

except Exception as ex:
    print('Python error with no Exception handler:')
    print('Traceback error:', ex)
    traceback.print_exc()

finally:
    print("Done")
