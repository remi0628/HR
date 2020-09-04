import os
import numpy as np
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras.layers import *
import datetime


import sys
sys.path.append('../')
import settings

### remi ### メモリの開放を制限する
gpu_options = tf.compat.v1.GPUOptions(per_process_gpu_memory_fraction=0.7)
sess = tf.compat.v1.Session(config=tf.compat.v1.ConfigProto(gpu_options=gpu_options))
### remi ###


X = np.load(f"{settings.MODEL_PATH_X}")
Y = np.load(f"{settings.MODEL_PATH_Y}")
Y = Y[:, 0] - 1  # 一位のみ取得

def train_test_split(data, size=0.7):
    length = len(data)
    np.random.seed(42)
    p = np.random.permutation(length)
    data = data[p]
    return data[:int(length*size)], data[int(length*size):]


x_train, x_test = train_test_split(X, size=0.8)
y_train, y_test = train_test_split(Y, size=0.8)
del X, Y


def Layers(data, mode, filters, kernel=None, bn=True, activation="relu", drop=None):
    if mode == "conv":
        x1 = Conv2D(filters, kernel)(data)
    elif mode == "dense":
        x1 = Dense(filters)(data)
    else:
        raise Exception("there is no mode")
    if bn:
        x1 = BatchNormalization()(x1)

    x1 = Activation(activation)(x1)
    if drop:
        x1 = Dropout(drop)(x1)
    return x1


dropouts = 0.6
inputs = Input(shape=(18, 10, 16))
x = Layers(inputs, "conv", 256, kernel=(1, 1), drop=dropouts)
x = Layers(x, "conv", 256, kernel=(1, 1), drop=dropouts)
x = Layers(x, "conv", 256, kernel=(1, 1), drop=dropouts)
x = Layers(x, "conv", 512, kernel=(1, 10), drop=dropouts)
x = Layers(x, "conv", 512, kernel=(1, 1), drop=dropouts)
x = Layers(x, "conv", 1024, kernel=(18, 1), drop=dropouts)

x = Flatten()(x)

dropouts = 0.3
x = Layers(x, "dense", 512, drop=dropouts)
x = Layers(x, "dense", 512, drop=dropouts)
x = Layers(x, "dense", 256, drop=dropouts)
outputs = Layers(x, "dense", 18, activation="softmax", bn=False)

model = tf.keras.Model(inputs=inputs, outputs=outputs)

model.compile(optimizer=tf.keras.optimizers.Nadam(learning_rate=0.001),
              loss='sparse_categorical_crossentropy', metrics=['accuracy'])

model.summary()

call_back = tf.keras.callbacks.EarlyStopping(monitor="val_accuracy",
                                           patience=500,
                                           restore_best_weights=True)

epoch = 500
batch = 8
history = model.fit(x_train, y_train, batch_size=batch, epochs=epoch, validation_data=(x_test, y_test),
                    callbacks=[call_back])

now = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
model.save(f"model/horse{now}-b{batch}-epo{epoch}-valacc{int(10000 * max(history.history['val_accuracy']))}.h5")
