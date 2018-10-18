from __future__ import print_function
import json
import base64
import hmac
import hashlib
import time
from threading import Thread
from websocket import create_connection, WebSocketConnectionClosedException
from pymongo import MongoClient
from cbpro.cbpro_auth import get_auth_headers
import datetime
import ast
from operator import itemgetter
import datetime as dt
import dateutil.parser as dp


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


class Agent:
    def __init__(self):
        self.decision = {}
        self.sequence = 0
        self.cash = 1000
        self.coin = 2
        self.wallet = 0

    def get_wallet(self):
        self.wallet = (self.coin * ob.split_price) + self.cash

    def send_msg(self):
        time = datetime.datetime.utcnow().isoformat()
        self.decision = {"order_id": "agent", "price": "6224.65", "sequence": self.sequence, "side": "sell",
                         "size": "10.0", "time": str(time)+"Z", "type": "received", }

    def clear_msg(self):
        self.decision = {}


class OrderBook:
    def __init__(self):
        self.bids = []
        self.asks = []
        self.bid = 0
        self.ask = 0
        self.bid_depth = 0
        self.ask_depth = 0
        self.split_price = 0

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

    '''def get_bid_depth(self):
        if len(self.bids) > 0:
            return float(self.bids[0]['size'])
        else:
            return 0'''

    '''def get_ask_depth(self):
        if len(self.asks) > 0:
            return float(self.asks[0]['size'])
        else:
            return 0'''

    def match_books(self):
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
                if ask_depth < bid_depth:
                    if 'order_id' in self.asks[0]:
                        if self.asks[0]['order_id'] == "agent":
                            agent.coin -= ask_depth
                            fee = ask_depth * ask * 0.003
                            if ask_time < bid_time:
                                agent.cash += ((ask_depth * ask) - fee)
                            else:
                                agent.cash += ((ask_depth * bid) - fee)

                    elif 'order_id' in self.bids[0]:
                        if self.bids[0]['order_id'] == 'agent':
                            agent.coin += ask_depth
                            if ask_time < bid_time:
                                agent.cash -= (ask_depth * ask)
                            else:
                                agent.cash -= (ask_depth * bid)
                    self.bids[0]['size'] = str(float(self.bids[0]['size']) - ask_depth)
                    del self.asks[0]

                elif ask_depth > bid_depth:

                    if 'order_id' in self.asks[0]:
                        if self.asks[0]['order_id'] == "agent":
                            agent.coin -= bid_depth
                            fee = bid_depth * ask * 0.003
                            if ask_time < bid_time:
                                agent.cash += ((bid_depth * ask) - fee)
                            else:
                                agent.cash += ((bid_depth * bid) - fee)

                    elif 'order_id' in self.bids[0]:
                        if self.bids[0]['order_id'] == 'agent':
                            agent.coin += bid_depth
                            if ask_time < bid_time:
                                agent.cash -= (bid_depth * ask)
                            else:
                                agent.cash -= (bid_depth * bid)

                    self.asks[0]['size'] = str(float(self.asks[0]['size']) - bid_depth)
                    del self.bids[0]

                else:
                    if 'order_id' in self.asks[0]:
                        if self.asks[0]['order_id'] == "agent":
                            agent.coin -= ask_depth
                            fee = ask_depth * ask * 0.003
                            if ask_time < bid_time:
                                agent.cash += ((ask_depth * ask) - fee)
                            else:
                                agent.cash += ((ask_depth * bid) - fee)

                    elif 'order_id' in self.bids[0]:
                        if self.bids[0]['order_id'] == 'agent':
                            agent.coin += ask_depth
                            if ask_time < bid_time:
                                agent.cash -= (ask_depth * ask)
                            else:
                                agent.cash -= (ask_depth * bid)

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
            # build_csv('best_ask.csv', [ask])
            # build_csv('best_bid.csv', [bid])

            # build_csv('order_book_info.csv', [bid, bid_depth, ask, ask_depth])

            print('{} {} bid: {:.3f} @ {:.2f}\task: {:.3f} @ {:.2f}'.format(
                dt.datetime.now(), 'BTC-USD', bid_depth, bid, ask_depth, ask))
        #print(self.bid, self.ask, self.bid_depth, self.ask_depth)


agent = Agent()
ob = OrderBook()


if __name__ == "__main__":
    import sys
    import cbpro
    import time

    order_book = []
    opened_bids = []
    opened_asks = []
    sequences = []

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
                    # print("MSG", newdict)
                    # build_csv_dict('websocket.csv', newdict)
                    x = json.dumps(newdict, indent=4, sort_keys=True)
                    #print(x)
                    order_book.append(x)
                    self.message_count += 1


                elif 'size' in msg:
                    newdict = {key: msg[key] for key in ['order_id', 'price', 'side', 'sequence',
                                                         'size', 'time', 'type']}
                    # print("MSG", newdict)
                    # build_csv_dict('websocket.csv', newdict)
                    seq = msg['sequence']  # + 1
                    # print("SSSSSS", seq)

                    if time.time() % 10 == 0:
                        agent.sequence = seq
                        agent.send_msg()

                        x = json.dumps(agent.decision, indent=4, sort_keys=True)
                        print(x)
                        order_book.append(x)
                        agent.clear_msg()
                        self.message_count += 1
                        newdict['sequence'] += 1
                    x = json.dumps(newdict, indent=4, sort_keys=True)
                    #print(x)
                    order_book.append(x)
                    self.message_count += 1
            for order in order_book:
                #item = dict(order)
                order = ast.literal_eval(order)
                if order['type'] == 'received':
                    #print(order)
                    if order['side'] == 'buy' and order['sequence'] not in sequences:
                        ob.bids.append(order)
                        sequences.append(order['sequence'])
                    elif order['side'] == 'sell' and order['sequence'] not in sequences:
                        ob.asks.append(order)
                        sequences.append(order['sequence'])

            ob.match_books()

        def on_close(self):
            print("-- Goodbye! --")


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
