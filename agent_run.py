from __future__ import print_function
from decision_process import ml
import json
from threading import Thread
from websocket import create_connection, WebSocketConnectionClosedException
from build_data_set import build_csv
from cbpro.cbpro_auth import get_auth_headers
import datetime
import ast
from operator import itemgetter
import datetime as dt
import dateutil.parser as dp
#from auth_client_agent import agent
from multiprocessing import Process


class WebsocketClient(object):
    def __init__(self, url="wss://ws-feed.pro.coinbase.com", products=None, message_type="subscribe", mongo_collection=None,
                 should_print=True, auth=False, api_key="", api_secret="", api_passphrase="", channels=None):
        self.url = url
        self.products = products
        self.channels = channels
        self.type = message_type
        self.stop = True
        self.error = None
        self.ws = None
        self.thread = None
        self.auth = auth
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_passphrase = api_passphrase
        self.should_print = should_print
        self.mongo_collection = mongo_collection

    def start(self):
        def _go():
            self._connect()
            self._listen()
            self._disconnect()

        self.stop = False
        self.on_open()
        self.thread = Thread(target=_go)
        self.thread.start()

    def _connect(self):
        if self.products is None:
            self.products = ["BTC-USD"]
        elif not isinstance(self.products, list):
            self.products = [self.products]

        if self.url[-1] == "/":
            self.url = self.url[:-1]

        if self.channels is None:
            sub_params = {'type': 'subscribe', 'product_ids': self.products}
        else:
            sub_params = {'type': 'subscribe', 'product_ids': self.products, 'channels': self.channels}

        if self.auth:
            timestamp = str(time.time())
            message = timestamp + 'GET' + '/users/self/verify'
            auth_headers = get_auth_headers(timestamp, message, self.api_key, self.api_secret, self.api_passphrase)
            sub_params['signature'] = auth_headers['CB-ACCESS-SIGN']
            sub_params['key'] = auth_headers['CB-ACCESS-KEY']
            sub_params['passphrase'] = auth_headers['CB-ACCESS-PASSPHRASE']
            sub_params['timestamp'] = auth_headers['CB-ACCESS-TIMESTAMP']

        self.ws = create_connection(self.url)

        self.ws.send(json.dumps(sub_params))

    def _listen(self):
        while not self.stop:
            try:
                start_t = 0
                if time.time() - start_t >= 30:
                    # Set a 30 second ping to keep connection alive
                    self.ws.ping("keepalive")
                    start_t = time.time()
                data = self.ws.recv()
                msg = json.loads(data)
            except ValueError as e:
                self.on_error(e)
            except Exception as e:
                self.on_error(e)
            else:
                self.on_message(msg)

    def _disconnect(self):
        try:
            if self.ws:
                self.ws.close()
        except WebSocketConnectionClosedException as e:
            pass

        self.on_close()

    def close(self):
        self.stop = True
        self.thread.join()

    def on_open(self):
        if self.should_print:
            print("-- Subscribed! --\n")

    def on_close(self):
        if self.should_print:
            print("\n-- Socket Closed --")

    def on_message(self, msg):
        if self.should_print:
            print(msg)

        if self.mongo_collection:  # dump JSON to given mongo collection
            self.mongo_collection.insert_one(msg)

    def on_error(self, e, data=None):
        self.error = e
        self.stop = True
        print('{} - data: {}'.format(e, data))


class OrderBook:
    def __init__(self):
        self.bids = []
        self.asks = []
        self.bid = 0
        self.ask = 0
        self.bid_depth = 0
        self.ask_depth = 0
        self.split_price = 0
        self.order_book = []
        self.opened_bids = []
        self.opened_asks = []
        self.sequences = []
        self.adjust_coin = 0
        self.adjust_cash = 0
        #self.block_thread = False

    def get_bid(self):
        if len(self.bids) > 0:
            return float(self.bids[0]['price'])
        else:
            return 0

    def get_ask(self):
        if len(self.asks) > 0:
            return float(self.asks[0]['price'])
        else:
            return 0

    def get_ask_depth(self, price):
        total = 0
        if len(self.asks) > 0:
            for order in self.asks:
                if order['price'] == price:
                    total += float(order['size'])
                else:
                    total = float(self.asks[0]['size'])
        return total

    def get_bid_depth(self, price):
        total = 0
        if len(self.bids) > 0:
            for order in self.bids:
                if order['price'] == price:
                    total += float(order['size'])
                else:
                    total = float(self.bids[0]['size'])
        return total

    def add_to_order_book(self, dictionary):
        self.order_book.append(dictionary)

    def match_books(self):
        #if self.block_thread is False:

        self.asks = sorted(self.asks, key=itemgetter('price', 'time'))
        b = sorted(self.bids, key=itemgetter('time'))
        self.bids = sorted(b, key=itemgetter('price'), reverse=True)

        while len(self.asks) > 0 and len(self.bids) > 0:

            ask = float(self.asks[0]['price'])
            ask_depth = float(self.asks[0]['size'])
            bid = float(self.bids[0]['price'])
            bid_depth = float(self.bids[0]['size'])
            a_time = self.asks[0]['time']
            parsed_t = dp.parse(a_time)
            ask_time = parsed_t.strftime('%s')
            b_time = self.bids[0]['time']
            parsed_t2 = dp.parse(b_time)
            bid_time = parsed_t2.strftime('%s')

            if ask <= bid:
                #print("Changes")
                if ask_depth < bid_depth:
                    if 'order_id' in self.asks[0]:
                        if self.asks[0]['order_id'] == 'agent':
                            print("Order ID", self.asks[0]['order_id'])
                            print("Agent's order was matched")
                            self.adjust_coin = -ask_depth
                            fee = ask_depth * ask * 0.003
                            if ask_time < bid_time:
                                self.adjust_cash = ((ask_depth * ask) - fee)
                            else:
                                self.adjust_cash = ((ask_depth * bid) - fee)

                    elif 'order_id' in self.bids[0]:

                        if self.bids[0]['order_id'] == 'agent':
                            print("Order ID", self.asks[0]['order_id'])
                            print("Agent's order was matched")
                            self.adjust_coin = ask_depth
                            if ask_time < bid_time:
                                self.adjust_cash = -(ask_depth * ask)
                            else:
                                self.adjust_cash = -(ask_depth * bid)
                    self.bids[0]['size'] = str(float(self.bids[0]['size']) - ask_depth)
                    del self.asks[0]

                elif ask_depth > bid_depth:

                    if 'order_id' in self.asks[0]:

                        if self.asks[0]['order_id'] == 'agent':
                            print("Order ID", self.asks[0]['order_id'])
                            print("Agent's order was matched")
                            self.adjust_coin = -bid_depth
                            fee = bid_depth * ask * 0.003
                            if ask_time < bid_time:
                                self.adjust_cash = ((bid_depth * ask) - fee)
                            else:
                                self.adjust_cash = ((bid_depth * bid) - fee)

                    elif 'order_id' in self.bids[0]:

                        if self.bids[0]['order_id'] == 'agent':
                            print("Order ID", self.asks[0]['order_id'])
                            print("Agent's order was matched")
                            self.adjust_coin = bid_depth
                            if ask_time < bid_time:
                                self.adjust_cash = -(bid_depth * ask)
                            else:
                                self.adjust_cash = -(bid_depth * bid)

                    self.asks[0]['size'] = str(float(self.asks[0]['size']) - bid_depth)
                    del self.bids[0]

                else:
                    if 'order_id' in self.asks[0]:

                        if self.asks[0]['order_id'] == 'agent':
                            print("Order ID", self.asks[0]['order_id'])
                            print("Agent's order was matched")
                            self.adjust_coin = -ask_depth
                            fee = ask_depth * ask * 0.003
                            if ask_time < bid_time:
                                self.adjust_cash = ((ask_depth * ask) - fee)
                            else:
                                self.adjust_cash = ((ask_depth * bid) - fee)

                    elif 'order_id' in self.bids[0]:

                        if self.bids[0]['order_id'] == 'agent':
                            print("Order ID", self.asks[0]['order_id'])
                            print("Agent's order was matched")
                            self.adjust_coin = ask_depth

                            if ask_time < bid_time:
                                self.adjust_cash = -(ask_depth * ask)
                            else:
                                self.adjust_cash = -(ask_depth * bid)

                    del self.bids[0]
                    del self.asks[0]


            else:
                break

        # Calculate newest bid-ask spread
        bid = self.get_bid()
        bid_depth = self.get_bid_depth(str(bid))
        ask = self.get_ask()
        ask_depth = self.get_ask_depth(str(ask))

        if self.bid == bid and self.ask == ask and self.bid_depth == bid_depth and self.ask_depth == ask_depth:
            # If there are no changes to the bid-ask spread since the last update, no need to print
            pass
        else:
            # If there are differences, update the cache
            self.bid = float(bid)
            self.ask = float(ask)
            self.bid_depth = float(bid_depth)
            self.ask_depth = float(ask_depth)
            self.split_price = (self.bid + self.ask) / 2
            # Build data sets
            if self.ask != 0:
                build_csv('best_ask.csv', [self.ask])
            if self.bid != 0:
                build_csv('best_bid.csv', [self.bid])

            build_csv('order_book_info.csv', [self.bid, self.bid_depth, self.ask, self.ask_depth])

            print('{} {} bid: {:.3f} @ {:.2f}\task: {:.3f} @ {:.2f}'.format(
                dt.datetime.now(), 'BTC-USD', bid_depth, bid, ask_depth, ask))


class AuthenticatedClient:

    def __init__(self):
        self.decision = {}
        #self.wait = 0
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
        #print("Hopefully appending orderbook")
        #ob.order_book.append(self.decision)
        #ob.add_to_order_book(self.decision)
        #ob.order_book.append(params)

        x = json.dumps(params, indent=4, sort_keys=True)
        ob.order_book.append(x)
        for order in ob.order_book:

            order = ast.literal_eval(order)
            if order['type'] == 'received':

                if order['side'] == 'buy' and order['sequence'] not in ob.sequences:

                    ob.bids.append(order)
                    ob.sequences.append(order['sequence'])
                elif order['side'] == 'sell' and order['sequence'] not in ob.sequences:
                    ob.asks.append(order)
                    ob.sequences.append(order['sequence'])
            # if 'order_id' in order:
            # if order['order_id'] == 'agent':
            # ob.block_thread = False

        ob.match_books()

    def get_agents_offer(self):
        offer = ml.run_agent()
        print("Offer", offer)

        if offer['side'] == 'buy' or offer['side'] == 'sell':

            self.place_limit_order(side=offer['side'], price=offer['price'], size=offer['size'])

            #print("Making an offer:\n\t side=", offer['side'], "\n\tprice=", offer['price'], '\n\tsize =', offer['size'])
            #print("Length of dec: ", len(self.decision))

            #print("time = ", datetime.datetime.utcnow().isoformat())
            pass
        else:
            #print("No offer")
            pass

    def run(self):
        while self.running:
            self.get_agents_offer()
            self.cash += ob.adjust_cash
            self.coin += ob.adjust_coin
            # print("Now is ", datetime.datetime.now())
            print("Agents wallet", agent.get_wallet())
            print("Agents coin ", self.coin)
            print("Agents cash", self.cash)
            ob.adjust_cash = 0
            ob.adjust_coin = 0
            # ob.block_thread = False
            if len(self.decision) > 0:
                time.sleep(1)
                self.decision = {}


agent = AuthenticatedClient()
ob = OrderBook()


if __name__ == "__main__":
    import sys
    import cbpro
    import time

    class MyWebsocketClient(cbpro.WebsocketClient):
        def on_open(self):
            self.url = "wss://ws-feed.pro.coinbase.com/"
            self.products = ["BTC-USD"]
            self.message_count = 0
            print("Let's count the messages!")

        def on_message(self, msg):
            #print(json.dumps(msg, indent=4, sort_keys=True))
            self.message_count += 1
            if 'type' in msg and 'price' in msg and 'side' in msg and 'time' in msg and \
                    'order_id' in msg:
                # data = msg
                if 'remaining_size' in msg:
                    newdict = {key: msg[key] for key in ['order_id', 'price', 'sequence', 'side',
                                                         'remaining_size', 'time', 'type']}

                    x = json.dumps(newdict, indent=4, sort_keys=True)

                    ob.order_book.append(x)
                    self.message_count += 1

                elif 'size' in msg:
                    newdict = {key: msg[key] for key in ['order_id', 'price', 'side', 'sequence',
                                                         'size', 'time', 'type']}
                    x = json.dumps(newdict, indent=4, sort_keys=True)

                    ob.order_book.append(x)
                    self.message_count += 1

           # length = agent.get_decision_length()
            #print("what ob reads", length)
            #if length > 0:
                #print("Received offer from client: ", agent.decision)
                #order_book.append(agent.decision)
                #agent.decision = {}

            for order in ob.order_book:

                order = ast.literal_eval(order)
                if order['type'] == 'received':

                    if order['side'] == 'buy' and order['sequence'] not in ob.sequences:

                        ob.bids.append(order)
                        ob.sequences.append(order['sequence'])
                    elif order['side'] == 'sell' and order['sequence'] not in ob.sequences:
                        ob.asks.append(order)
                        ob.sequences.append(order['sequence'])
                #if 'order_id' in order:
                    #if order['order_id'] == 'agent':
                        #ob.block_thread = False

            ob.match_books()

        def on_close(self):
            print("-- Goodbye! --")


    def main_program():
        wsClient = MyWebsocketClient()
        wsClient.start()
        print(wsClient.url, wsClient.products)
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            wsClient.close()

        if wsClient.error:
            sys.exit(1)
        else:
            sys.exit(0)

    #main_program()
    p1 = Process(target=main_program, args=())
    p1.start()
    p2 = Process(target=agent.run, args=())
    p2.start()
    p1.join()
    p2.join()

    '''
    thread = Thread(target=main_program())
    # thread = Thread(target=main_program())
    thread.daemon = True
    thread.start()

    thread = Thread(target=agent.run())
    # thread = Thread(target=main_program())
    thread.daemon = True
    thread.start()
    '''


    #agent.run()
    # Ensure all of the threads have finished
    #thread.join()



