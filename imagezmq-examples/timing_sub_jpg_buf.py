"""timing_sub_jpg_buf.py -- subscribe and display images, then print FPS stats
"""

import sys
import traceback
import numpy as np
import cv2
from collections import defaultdict
from imutils.video import FPS
import imagezmq

# instantiate image_hub
image_hub = imagezmq.ImageHub(open_port='tcp://192.168.0.8:5555', REQ_REP=False)

image_count = 0
sender_image_counts = defaultdict(int)  # dict for counts by sender
first_image = True

try:
    while True:  # receive images until Ctrl-C is pressed
        sent_from, jpg_buffer = image_hub.recv_jpg()
        if first_image:
            fps = FPS().start()  # start FPS timer after first image is received
            first_image = False
        image = cv2.imdecode(np.frombuffer(jpg_buffer, dtype='uint8'), -1)
        # see opencv docs for info on -1 parameter
        fps.update()
        image_count += 1  # global count of all images received
        sender_image_counts[sent_from] += 1  # count images for each RPi name
        cv2.imshow(sent_from, image)  # display images 1 window per sent_from
        cv2.waitKey(1)
        # other image processing code, such as saving the image, would go here.
        # often the text in "sent_from" will have additional information about
        # the image that will be used in processing the image.
except (KeyboardInterrupt, SystemExit):
    pass  # Ctrl-C was pressed to end program; FPS stats computed below
except Exception as ex:
    print('Python error with no Exception handler:')
    print('Traceback error:', ex)
    traceback.print_exc()
finally:
    # stop the timer and display FPS information
    print()
    print('Test Program: ', __file__)
    print('Total Number of Images received: {:,g}'.format(image_count))
    if first_image:  # never got images from any RPi
        sys.exit()
    fps.stop()
    print('Number of Images received from each RPi:')
    for RPi in sender_image_counts:
        print('    ', RPi, ': {:,g}'.format(sender_image_counts[RPi]))
    compressed_size = len(jpg_buffer)
    print('Size of last jpg buffer received: {:,g} bytes'.format(compressed_size))
    image_size = image.shape
    print('Size of last image received: ', image_size)
    uncompressed_size = 1
    for dimension in image_size:
        uncompressed_size *= dimension
    print('    = {:,g} bytes'.format(uncompressed_size))
    print('Compression ratio: {:.2f}'.format(compressed_size / uncompressed_size))
    print('Elasped time: {:,.2f} seconds'.format(fps.elapsed()))
    print('Approximate FPS: {:.2f}'.format(fps.fps()))
    cv2.destroyAllWindows()  # closes the windows opened by cv2.imshow()
    image_hub.close()  # closes ZMQ socket and context
    sys.exit()
