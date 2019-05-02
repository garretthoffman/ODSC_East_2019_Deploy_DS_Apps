from flask import Flask, jsonify, request
import tensorflow as tf
from tensorflow.keras.models import load_model
from tensorflow.keras.preprocessing import image
from io import BytesIO
import base64
import numpy as np

############################# Initializae App #############################

app = Flask(__name__)

######################## Load Model, Def Functions ########################

def make_response(data, status_code):
    return jsonify(data), status_code

def decode_image(b64img):
    img = image.img_to_array(image.load_img(BytesIO(base64.b64decode(b64img)),
                                            target_size=(250, 250))) / 255
    img = np.expand_dims(img, axis=0)
    return img

############################## Define Routes ##############################
# this is a health check endpoint that is hit periodically by our infra to test the
# app is still healthy
@app.route('/health', methods=['GET'])
def health_check():    
    data = {"health_check": "healthy"}
    return make_response(data, 200)

@app.route('/chart_classifier/predict', methods=['POST'])
def predict():
    # Decoding and pre-processing base64 image
    b64img = request.form.get('encoded_image')
    img = decode_image(b64img)
 
    # we are loading the model every time we get a request, which is really really bad
    # ideally we should be loading the model on the main thread as a global variable,
    # but the problem with this is that TF/Keras makes it really difficult to work with
    # our model graph across multiple threads
    model = load_model('cv_chart_model.h5')
    prob = model.predict(img)[0][0]
    
    cla = "Chart" if prob > .5 else "Meme"
    data = {'probability': str(prob), 'class': cla}

    return make_response(data, 200)

