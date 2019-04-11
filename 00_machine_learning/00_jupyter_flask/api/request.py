import argparse
import base64
import requests

# defining the api-endpoint
API_ENDPOINT = "http://localhost:5000/chart_classifier/predict"

# taking input image via command line
ap = argparse.ArgumentParser()
ap.add_argument("-i", "--image", required=True,
                help="path of the image")
args = vars(ap.parse_args())

image_path = args['image']
b64_image = ""
# Encoding the JPG,PNG,etc. image to base64 format
with open(image_path, "rb") as imageFile:
    b64_image = base64.b64encode(imageFile.read())

# data to be sent to api
data = {'encoded_image': b64_image}

# sending post request and saving response as response object
r = requests.post(url=API_ENDPOINT, data=data)

# extracting the response
print(r.content)