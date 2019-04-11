import argparse
import boto3
from sagemaker.predictor import json_serializer, json_deserializer
from tensorflow.keras.preprocessing import image
import numpy as np
import json

# get sagemaker client using AWS SDK
sage = boto3.client('sagemaker-runtime')

# defining the api-endpoint
MODEL_ENDPOINT = "test-tf-chart-cv-2019-04-11-20-35-59-270"

# taking input image via command line
ap = argparse.ArgumentParser()
ap.add_argument("-i", "--image", required=True,
                help="path of the image")
args = vars(ap.parse_args())

image_path = args['image']

# define function to read and transform image 
def transform_image(image_path):
    img = image.img_to_array(image.load_img(image_path, target_size=(250, 250))) / 255
    img = np.expand_dims(img, axis=0)
    return img

data = {'instances': transform_image(image_path)}
payload = json_serializer(data)

# sending post request and saving response as response object
response = sage.invoke_endpoint(EndpointName=MODEL_ENDPOINT,
                        ContentType='application/json',
                        Body=payload)

r = response.get('Body').read()
r = json.loads(r)

prob = r['predictions'][0][0]
cat = "Chart" if prob > .5 else "Meme"

# extracting the response
print({"probability": prob, "class": cat})