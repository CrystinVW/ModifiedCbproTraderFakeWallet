import os
from sklearn.preprocessing import MinMaxScaler
import csv
import pandas as pd
import numpy as np
import pickle
from sklearn.model_selection import train_test_split
from keras.models import load_model
from keras.models import Sequential
from keras.layers import Dense, Dropout, LSTM
from keras.layers import Activation, Conv1D
import time



os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'

lstm_prediction_length = 3
lstm_look_back = 10
cnn_prediction_length = 6
cnn_look_back = 20
epochs = 50
batch_size = 32
buffer = 1.5


class MachineLearning:

    def __init__(self):
        self.cnn_prediction_ask = 0
        self.cnn_prediction_bid = 0
        self.lstm_prediction_ask = 0
        self.lstm_prediction_bid = 0
        self.side = 'stay'  # ('buy' or 'sell)
        self.price = 0
        self.size = 0
        self.client_oid = None
        self.product_id = 'BTC-USD'
        self.time_in_force = 'GTT'
        self.cancel_after = None  # 'min' or 'hour'
        self.overdraft_enabled = False
        self.last_bid_price = 0
        self.last_bid_size = 0
        self.last_ask_price = 0
        self.last_ask_size = 0
        self.wait = 0
        self.running = True
        self.id = 'agent'
        self.order = {}

    # convert an array of values into a dataset matrix
    def create_dataset(self, dataset, look_back):
        dataX, dataY = [], []
        for i in range(len(dataset) - look_back - 1):
            a = dataset[i:(i + look_back), 0]
            dataX.append(a)
            dataY.append(dataset[i + look_back, 0])
        return np.array(dataX), np.array(dataY)

    def build_lstm_bid(self):

        # normalize the dataset
        scalar = MinMaxScaler(feature_range=(0, 1))
        price_data = []
        with open('best_bid.csv', "r") as f:
            reader = csv.reader(f)
            for row in reader:
                price_data.append(float(row[0]))
            f.close()
        price_data = price_data[0::lstm_prediction_length]

        # Build data frame of data
        dataframe = pd.DataFrame(price_data)
        dataset = dataframe.values
        dataset = dataset.astype('float32')

        # Fit to data set
        dataset = scalar.fit_transform(dataset)

        # split into train and test sets
        train_size = int(len(dataset) * 0.8)

        # test_size = len(dataset) - train_size
        train, test = dataset[0:train_size, :], dataset[train_size:len(dataset), :]
        #print("tt", train, test)
        # reshape into X=t and Y=t+1

        trainX, trainY = self.create_dataset(train, lstm_look_back)
        testX, testY = self.create_dataset(test, lstm_look_back)

        # reshape input to be [samples, time steps, features]
        trainX = np.reshape(trainX, (trainX.shape[0], trainX.shape[1], 1))
        testX = np.reshape(testX, (testX.shape[0], testX.shape[1], 1))

        # create and fit the LSTM network
        model = Sequential()
        model.add(LSTM(4, input_shape=(lstm_look_back, 1)))
        model.add(Dense(1))
        model.compile(loss='mean_squared_error', optimizer='adam')
        model.fit(trainX, trainY, epochs=epochs, batch_size=batch_size, verbose=2, validation_data=(testX, testY))
        model.save('lstm_bid_model.h5')

        pickle.dump(scalar, open('lstm_bid_scalar.sav', 'wb'))

    def predict_lstm_bid(self):

        # Load Model
        model = load_model('lstm_bid_model.h5')

        # Load Scalar
        scalar = pickle.load(open('lstm_bid_scalar.sav', 'rb'))

        # if read:
        data = []
        with open('best_bid.csv', 'r') as file:
            reader = csv.reader(file)
            for row in reader:
                data.append([float(row[0])])  # 1
            file.close()
        data = data[0::lstm_prediction_length]
        last_chunk = data[-lstm_look_back:]

        # Separate last_chunk
        dataframe = pd.DataFrame(last_chunk)
        dataset = scalar.fit_transform(dataframe)
        dataset = dataset.reshape(1, lstm_look_back, 1)

        # Predict from last chunk
        prediction = model.predict(dataset)
        prediction = scalar.inverse_transform(prediction)

        # Predict
        prediction = float(prediction[0])
        #print("LSTM Bid Predicting", prediction)
        self.lstm_prediction_bid = prediction

        # Dump new scalar
        pickle.dump(scalar, open('lstm_bid_scalar.sav', 'wb'))  # dataset

        return prediction

    def build_lstm_ask(self):

        # normalize the dataset
        scalar = MinMaxScaler(feature_range=(0, 1))
        price_data = []
        with open('best_ask.csv', "r") as f:
            reader = csv.reader(f)
            for row in reader:
                price_data.append(float(row[0]))
            f.close()
        price_data = price_data[0::lstm_prediction_length]

        # Build data frame of data
        dataframe = pd.DataFrame(price_data)
        dataset = dataframe.values
        dataset = dataset.astype('float32')

        # Fit to data set
        dataset = scalar.fit_transform(dataset)

        # split into train and test sets
        train_size = int(len(dataset) * 0.8)

        # test_size = len(dataset) - train_size
        train, test = dataset[0:train_size, :], dataset[train_size:len(dataset), :]
        #print("tt", train, test)
        # reshape into X=t and Y=t+1

        trainX, trainY = self.create_dataset(train, lstm_look_back)
        testX, testY = self.create_dataset(test, lstm_look_back)

        # reshape input to be [samples, time steps, features]
        trainX = np.reshape(trainX, (trainX.shape[0], trainX.shape[1], 1))
        testX = np.reshape(testX, (testX.shape[0], testX.shape[1], 1))

        # create and fit the LSTM network
        model = Sequential()
        model.add(LSTM(4, input_shape=(lstm_look_back, 1)))
        model.add(Dense(1))
        model.compile(loss='mean_squared_error', optimizer='adam')
        model.fit(trainX, trainY, epochs=epochs, batch_size=batch_size, verbose=2, validation_data=(testX, testY))
        model.save('lstm_ask_model.h5')

        pickle.dump(scalar, open('lstm_ask_scalar.sav', 'wb'))

    def predict_lstm_ask(self):

        # Load Model
        model = load_model('lstm_ask_model.h5')

        # Load Scalar
        scalar = pickle.load(open('lstm_ask_scalar.sav', 'rb'))

        # if read:
        data = []
        with open('best_ask.csv', 'r') as file:
            reader = csv.reader(file)
            for row in reader:
                data.append([float(row[0])])  # 1
            file.close()
        data = data[0::lstm_prediction_length]
        last_chunk = data[-lstm_look_back:]

        # Separate last_chunk
        dataframe = pd.DataFrame(last_chunk)
        dataset = scalar.fit_transform(dataframe)
        dataset = dataset.reshape(1, lstm_look_back, 1)

        # Predict from last chunk
        prediction = model.predict(dataset)
        prediction = scalar.inverse_transform(prediction)

        # Predict
        prediction = float(prediction[0])
        #print("LSTM Ask Predicting", prediction)
        self.lstm_prediction_ask = prediction
        # Dump new scalar
        pickle.dump(scalar, open('lstm_ask_scalar.sav', 'wb'))  # dataset

        return prediction

    def build_cnn_ask(self):
        out = []
        orig = []
        with open('best_ask.csv', 'r') as file:
            reader = csv.reader(file)
            for row in reader:
                orig.append(row)

        orig = orig[0::cnn_prediction_length]

        sequence_length = cnn_look_back + 1
        scalar = MinMaxScaler(feature_range=(0, 1))
        orig = scalar.fit_transform(orig)
        for i in range(len(orig) - sequence_length):
            out.append(orig[i: i + sequence_length])

        out = np.array(out)

        x = out[:, :-1]
        y = out[:, -1]

        # Convert to arrays
        x = np.array(x)
        y = np.array(y)

        # Split dataset
        x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=0)

        # Convert to float and normalize
        x_train = x_train.astype('float32')
        x_test = x_test.astype('float32')

        # Model Build
        model = Sequential()

        model.add(Conv1D(filters=40, kernel_size=6, padding='same', activation='relu'))
        model.add(LSTM(100, input_shape=(cnn_look_back, 1), return_sequences=False))
        model.add(Dropout(0.2))
        model.add(Dense(units=1))
        model.add(Activation('softmax'))
        # timer_start = time.time()
        model.compile(loss='mse', optimizer='rmsprop')  #
        # print('Model built in: ', time.time() - timer_start)
        # Training model
        model.fit(x_train,
                  y_train,
                  batch_size=batch_size,
                  epochs=epochs,
                  validation_data=(x_test, y_test)
                  )
        model.save('cnn_ask_model.h5')
        pickle.dump(scalar, open('cnn_ask_scalar.sav', 'wb'))

    def predict_cnn_ask(self):

        # tf.get_default_graph()
        out = []
        orig = []
        scalar = pickle.load(open('cnn_ask_scalar.sav', 'rb'))
        sequence_length = cnn_look_back + 1
        # if read:
        with open('best_ask.csv', 'r') as file:
            reader = csv.reader(file)
            for row in reader:
                orig.append(row)
            file.close()

        self.last_ask_price = float(orig[-1][0])

        orig = orig[0::cnn_prediction_length]
        # print("Orig", orig[-1])
        orig = scalar.fit_transform(orig)
        for i in range(len(orig) - sequence_length):
            out.append(orig[i: i + sequence_length])

        out = np.array(out)
        x = out[:, :-1]
        x = np.array(x)
        final_data = x[-1]
        final_data = np.array(final_data)
        final_data = final_data.reshape(1, cnn_look_back, 1)
        # Predict from last chunk
        # Load Model

        model = load_model('cnn_ask_model.h5')

        prediction = model.predict(final_data)
        prediction = scalar.inverse_transform(prediction)
        #print("CNN Ask Predicting", float(prediction[0]))
        self.cnn_prediction_ask = float(prediction[0])

        # Dump New Scalar
        pickle.dump(scalar, open('cnn_ask_scalar.sav', 'wb'))

        return prediction

    def build_cnn_bid(self):
        out = []
        orig = []
        with open('best_bid.csv', 'r') as file:
            reader = csv.reader(file)
            for row in reader:
                orig.append(row)

        orig = orig[0::cnn_prediction_length]

        sequence_length = cnn_look_back + 1
        scalar = MinMaxScaler(feature_range=(0, 1))
        orig = scalar.fit_transform(orig)
        for i in range(len(orig) - sequence_length):
            out.append(orig[i: i + sequence_length])

        out = np.array(out)

        x = out[:, :-1]
        y = out[:, -1]

        # Convert to arrays
        x = np.array(x)
        y = np.array(y)

        # Split dataset
        x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=0)

        # Convert to float and normalize
        x_train = x_train.astype('float32')
        x_test = x_test.astype('float32')

        # Model Build
        model = Sequential()

        model.add(Conv1D(filters=40, kernel_size=6, padding='same', activation='relu'))
        model.add(LSTM(100, input_shape=(cnn_look_back, 1), return_sequences=False))
        model.add(Dropout(0.2))
        model.add(Dense(units=1))
        model.add(Activation('softmax'))
        # timer_start = time.time()
        model.compile(loss='mse', optimizer='rmsprop')  #
        # print('Model built in: ', time.time() - timer_start)
        # Training model
        model.fit(x_train,
                  y_train,
                  batch_size=batch_size,
                  epochs=epochs,
                  validation_data=(x_test, y_test)
                  )
        model.save('cnn_bid_model.h5')
        pickle.dump(scalar, open('cnn_bid_scalar.sav', 'wb'))

    def predict_cnn_bid(self):

        # tf.get_default_graph()
        out = []
        orig = []
        scalar = pickle.load(open('cnn_bid_scalar.sav', 'rb'))
        sequence_length = cnn_look_back + 1
        # if read:
        with open('best_bid.csv', 'r') as file:
            reader = csv.reader(file)
            for row in reader:
                orig.append(row)
            file.close()
        self.last_bid_price = float(orig[-1][0])

        orig = orig[0::cnn_prediction_length]
        # print("Orig", orig[-1])
        orig = scalar.fit_transform(orig)
        for i in range(len(orig) - sequence_length):
            out.append(orig[i: i + sequence_length])

        out = np.array(out)
        x = out[:, :-1]
        x = np.array(x)
        final_data = x[-1]
        final_data = np.array(final_data)
        final_data = final_data.reshape(1, cnn_look_back, 1)
        # Predict from last chunk
        # Load Model

        model = load_model('cnn_bid_model.h5')
        prediction = model.predict(final_data)
        prediction = scalar.inverse_transform(prediction)
        #print("CNN Bid Predicting", float(prediction[0]))
        self.cnn_prediction_bid = float(prediction[0])

        pickle.dump(scalar, open('cnn_bid_scalar.sav', 'wb'))
        return prediction

    def predict_models(self):
        self.predict_cnn_bid()
        self.predict_lstm_bid()
        self.predict_cnn_ask()
        self.predict_lstm_ask()
        print("predictions ask", self.lstm_prediction_ask)
        print("predictions bid", self.lstm_prediction_bid)

    def train_models(self):
        self.build_cnn_ask()
        self.build_lstm_bid()
        self.build_cnn_bid()
        self.build_lstm_ask()

    def get_size(self, term):

        if self.side == 'buy':
            if term == 'lstm':
                prediction = self.lstm_prediction_bid
            elif term == 'cnn':
                prediction = self.cnn_prediction_bid
            else:
                prediction = self.last_ask_price # No Trade
            size = 1
            magic_number = 1000000.0
            while magic_number > (prediction - self.last_ask_price):
                y = size * self.last_ask_price

                magic_number = y * buffer
                size *= 0.9
                if size < 0.00005:
                    print("Trade too small!")
                    size = 0
                    break
        elif self.side == 'sell':
            takers_fee = 0.003
            if term == 'lstm':
                prediction = self.lstm_prediction_ask
            elif term == 'cnn':
                prediction = self.cnn_prediction_ask
            else:
                prediction = self.last_bid_price # No Trade
            size = 1
            magic_number = 1000000.0
            while magic_number > (self.last_bid_price - prediction):
                y = size * self.last_bid_price
                z = y * takers_fee
                temp_number = y + z
                magic_number = temp_number * buffer
                size *= 0.9

                if size < 0.00005:
                    print("Trade too small!")
                    size = 0
                    break
        else:
            size = 0
            self.running = False

        self.size = size

        return size

    def rule_based(self):
        # If price is slowly rising
        self.cancel_after = 'min'
        if self.last_ask_price < self.cnn_prediction_bid:
            # If price is moving now, thus buy, quickly and wait a while
            if self.last_ask_price < self.lstm_prediction_bid:
                #print("Should buy")
                self.side = 'buy'
                self.price = self.last_ask_price  # may need to buy at 0.01 cent higher
                self.size = self.get_size('cnn')
                self.wait = cnn_prediction_length

            # Else if there will be a quick dip in price, meaning ask will be below current bid price
            elif self.last_bid_price > self.lstm_prediction_ask:

                self.side = 'sell'
                self.price = self.last_bid_price
                self.size = self.get_size('lstm')
                self.wait = lstm_prediction_length

            else:

                self.side = 'stay'


        # If price is dropping slowly
        elif self.last_bid_price > self.cnn_prediction_ask:
            # If price is moving now, then sell, quickly,
            if self.last_bid_price > self.lstm_prediction_ask:
                print("Should sell")
                self.side = 'sell'
                self.price = self.last_bid_price # maybe drop this a little
                self.size = self.get_size('cnn')
                self.wait = cnn_prediction_length
            # Quick rise in price, buy, but be ready to sell, wait short time
            elif self.last_ask_price < self.lstm_prediction_bid:
                print("Should buy")
                self.side = 'buy'
                self.price = self.last_ask_price
                self.size = self.get_size('lstm')
                self.wait = lstm_prediction_length

            else:

                self.side = 'stay'

        # If price should stay the same long term
        elif self.last_ask_price == self.cnn_prediction_ask:
            # If price is moving now, thus buy, quickly and wait a while
            if self.last_ask_price < self.lstm_prediction_bid:

                self.side = 'buy'
                self.price = self.last_ask_price  # may need to buy at 0.01 cent higher
                self.size = self.get_size('lstm')
                self.wait = lstm_prediction_length

            # Else if there will be a quick dip in price, meaning ask will be below current bid price
            elif self.last_bid_price > self.lstm_prediction_ask:

                self.side = 'sell'
                self.price = self.last_bid_price
                self.size = self.get_size('lstm')
                self.wait = lstm_prediction_length
        else:
            #print("Should stay")
            self.side = 'stay'

    def run_agent(self):
        if self.wait == 0:
            self.reset_values()
            self.predict_models()
            self.rule_based()

        else:
            self.reset_values()
            time.sleep(abs(self.wait - 8))


        return self.get_order()

    def get_order(self):
        if float(self.size) == 0.0:
            self.side = 'stay'

        self.order = {
            'order_id': self.id,
            'side': str(self.side),
            'price': float(self.price), #Decimal(self.price),
            'size': float(self.size), #Decimal(self.size),
            'wait': float(self.wait)
        }

        return self.order

    def reset_values(self):
        self.cnn_prediction_ask = 0
        self.cnn_prediction_bid = 0
        self.lstm_prediction_ask = 0
        self.lstm_prediction_bid = 0
        self.side = 'stay'  # ('buy' or 'sell)
        self.price = 0
        self.size = 0
        self.cancel_after = None
        self.overdraft_enabled = False
        self.last_bid_price = 0
        self.last_bid_size = 0
        self.last_ask_price = 0
        self.last_ask_size = 0
        self.wait = 0


ml = MachineLearning()

train = False
if train:
    ml.train_models()









