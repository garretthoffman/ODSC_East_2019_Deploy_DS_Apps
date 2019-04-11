import argparse
import tensorflow as tf
import numpy as np
import os
import glob

from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Conv2D, MaxPooling2D, ZeroPadding2D
from tensorflow.keras.layers import Activation, Dropout, Flatten, Dense
from tensorflow.keras.preprocessing.image import ImageDataGenerator

def parse_args():

    parser = argparse.ArgumentParser()
    # input data and model directories
    parser.add_argument('--model_dir', type=str, default=os.environ.get('SM_MODEL_DIR'))
    parser.add_argument('--train', type=str, default=os.environ.get('SM_CHANNEL_TRAIN'))
    parser.add_argument('--test', type=str, default=os.environ.get('SM_CHANNEL_TEST'))

    # hyperparameters sent by the client are passed as command-line arguments to the script
    parser.add_argument('--epochs', type=int, default=5)
    parser.add_argument('--batch_size', type=int, default=32)
    parser.add_argument('--img_size', type=int, default=250)

    # can parameterize our model architecture as well, these will also be passed as
    # command-line arguments to the script
    parser.add_argument('--n_conv_layers', type=int, default=3)
    parser.add_argument('--n_filters', type=lambda s: [int(filter_size) for filter_size in s.split(",")], default=[32,32,64])
    parser.add_argument('--kernel_sizes', type=lambda s: [int(filter_size) for filter_size in s.split(",")], default=[3,3,3])
    parser.add_argument('--optimizer', type=str, default='rmsprop')

    # number of training samples to define steps per epochs
    parser.add_argument('--n_train_samples', type=int, default=1000)
    parser.add_argument('--n_test_samples', type=int, default=100)

    return parser.parse_known_args()

def get_image_data_gens(train_dir, test_dir, batch_size, img_size):
    # can define data augmentations here, we will scale images values between 0 and 1
    train_datagen = ImageDataGenerator(rescale=1/255)
    val_datagen = ImageDataGenerator(rescale=1/255)

    # this is a generator that will read pictures found in
    # subfolers of 'data/train', and indefinitely generate
    # batches of augmented image data
    train_generator = train_datagen.flow_from_directory(
            train_dir,  # this is the target directory
            batch_size=batch_size,
            target_size=(img_size, img_size),
            class_mode='binary')

    # this is a similar generator, for validation data
    test_generator = val_datagen.flow_from_directory(
            test_dir,
            batch_size=batch_size,
            target_size=(img_size, img_size),
            class_mode='binary')

    return train_generator, test_generator

def create_model(n_conv_layers, n_filters, kernel_sizes, img_size):
    # need to make sure that n_layers, filter size and kernel sizes are equal
    try:
        assert n_conv_layers == len(n_filters), f"CNN has {n_conv_layers} layers but len(n_filters) = {len(n_filters)}"
    except AssertionError as e:
        raise

    try:
        assert n_conv_layers == len(kernel_sizes), f"CNN has {n_conv_layers} layers but len(kernel_sizes) = {len(kernel_sizes)}"
    except AssertionError as e:
        raise
    
    # instantiate model
    model = Sequential()

    # add convolutional layers
    for i in range(n_conv_layers):
        model.add(Conv2D(n_filters[i], (kernel_sizes[i], kernel_sizes[i]), input_shape=(img_size, img_size, 3)))
        model.add(Activation('relu'))
        model.add(MaxPooling2D(pool_size=(2, 2)))

    # add fully connected layers
    model.add(Flatten())
    model.add(Dense(64))
    model.add(Activation('relu'))
    model.add(Dropout(0.5))
    model.add(Dense(1))
    model.add(Activation('sigmoid'))

    return model

if __name__ == "__main__":

    args, _ = parse_args()
    train_generator, test_generator = get_image_data_gens(args.train, args.test, args.batch_size, args.img_size)
    model = create_model(args.n_conv_layers, args.n_filters, args.kernel_sizes, args.img_size)

    model.compile(loss='binary_crossentropy',
              optimizer=args.optimizer,
              metrics=['accuracy'])

    model.fit_generator(
            train_generator,
            steps_per_epoch=args.n_train_samples // args.batch_size,
            epochs=args.epochs,
            validation_data=test_generator,
            validation_steps=args.n_test_samples // args.batch_size)

    # save model for serving later
    tf.contrib.saved_model.save_keras_model(model, args.model_dir)