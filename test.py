buffer = 1.5
class agent:
    def __init__(self):
        self.side = 'sell'
        self.lstm_prediction_ask = 1000
        self.cnn_prediction_ask = 1000
        self.last_bid_price = 1001

    def get_size(self, term):

        '''if self.side == 'buy':
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
                if size < 0.0001:
                    size = 0
                    break'''
        if self.side == 'sell':
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
                if size < 0.0001:
                    size = 0
                    break
        else:
            size = 0
            self.running = False

        self.size = size

        return size


ag = agent()
print(ag.get_size('lstm'))
