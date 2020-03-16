import cv2
import base64
import hashlib
import redis
from PIL import Image
from utils import *

redis_host = "localhost"
redis_port = 6379
redis_password = ""
r = redis.StrictRedis(host=redis_host, port=redis_port, password=redis_password, decode_responses=True)
msg = r.get("2a58caa7a5721c4b2f2a31f83b5e623b")
image = open("test1.png", "wb")
image.write(base64.b64decode(msg))
image.close()

img = cv2.imread("test1.png", -1)
# new_img = img_clipping(img, bg_color="TRANSPARENT")
print(img.shape)
cv2.imwrite("test_3c.png", img[:, :, 0:3])