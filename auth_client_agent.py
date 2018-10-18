import time
from decision_process import ml
import datetime
#from modified_web_socket import ob
from modified_web_socket import ob


class AuthenticatedClient:

    def __init__(self):
        self.decision = {}
        self.wait = 0
        self.running = True
        self.sequence = 0

        self.cash = 1000
        self.coin = 2
        self.wallet = 0
        self.split_price = 0

    def get_wallet(self):
        self.split_price = (ml.last_bid_price + ml.last_ask_price) / 2
        self.wallet = (self.coin * self.split_price) + self.cash
        return self.wallet

    def get_decision_length(self):
        return len(self.decision)

    def place_limit_order(self, side, price, size):

        params = {'order_id': 'agent',
                  'price': price,
                  'side': side,
                  'sequence': self.sequence,
                  'size': size,
                  'time': str(datetime.datetime.utcnow().isoformat()) + "Z",
                  'type': 'received'
                  }

        self.sequence += 1
        self.decision = params
        print("Hopefully appending orderbook")
        #ob.order_book.append(self.decision)
        ob.add_to_order_book(self.decision)
        print("OBS BOOK", ob.order_book)

    def get_agents_offer(self):
        offer = ml.run_agent()
        print("Offer", offer)

        if offer['side'] == 'buy' or offer['side'] == 'sell':

            self.place_limit_order(side=offer['side'], price=offer['price'], size=offer['size'])

            print("Making an offer:\n\t side=", offer['side'], "\n\tprice=", offer['price'], '\n\tsize =', offer['size'])
            print("Length of dec: ", len(self.decision))

            print("time = ", datetime.datetime.utcnow().isoformat())

        else:
            print("No offer")
            pass

        self.wait = offer['wait']

    def run(self):
        print("Start wallet", self.get_wallet())
        while self.running:
            self.get_agents_offer()
            if self.wait == 0:
                time.sleep(1)
            self.cash += ob.adjust_cash
            self.coin += ob.adjust_coin
            print("Now is ", datetime.datetime.now())
            print("Agents wallet", agent.get_wallet())
            print("Agents coin ", self.coin)
            print("Agents cash", self.cash)
            ob.adjust_cash = 0
            ob.adjust_coin = 0
            self.decision = {}

agent = AuthenticatedClient()

if __name__ == "__main__":
    agent.run()
