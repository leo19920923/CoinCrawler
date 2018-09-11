import argparse
import asyncio
import importlib
import os
import json
import threading
import pkgutil
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from deeputils.common import log
from tokenmds import exchange as exchange_list
from tokenmds.basic.exchange import Market
from tokenmds.exchange.binance import Binance
from tokenmds.exchange.huobi import Huobi
from tokenmds.exchange.okex import OKEx


symbols = 'BTC-USDT, ETH-BTC, ETH-USDT, EOS-BTC, EOS-USDT, EOS-ETH, BCD-BTC, ETC-BTC, ETC-USDT, XRP-BTC, XRP-USDT, LTC-BTC, LTC-USDT, QTUM-BTC, QTUM-USDT, QTUM-ETH, DASH-BTC, TRX-BTC, TRX-USDT, TRX-ETH, ONT-BTC, ONT-USDT, ONT-ETH, ZEC-BTC, NEO-BTC, NEO-USDT, XLM-BTC, XLM-ETH, ADA-BTC, ADA-USDT, ADA-ETH, OMG-BTC, OMG-ETH'


class Crawler:
    def __init__(self, exchange=None, symbols=None, tf='M1', path='./data/ohlc'):
        self.path = path
        self.exchange = exchange
        self.symbols = symbols
        self.tf = tf

    def writer(self, data, exchange, tf, symbol, t):
        file_path = '{}/{}/{}/{}/'.format(self.path, exchange.__id__, tf.lower(), symbol)
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        filename = '{}.josn'.format(t)
        with open(file_path + filename, 'w') as f:
            f.write(json.dumps(data))
            log(file_path + filename)

    def work(self, market):
        utc = datetime.utcnow()
        while True:
            t = utc.strftime('%Y') if self.tf == 'D1' else utc.strftime('%Y-%m-%d')
            ohlc = self.exchange.one(market, self.tf, t)
            if ohlc:
                ohlc = [i.__dict__() for i in ohlc]
                self.writer(ohlc, self.exchange, self.tf, str(market).lower(), t)
                if self.tf == 'D1':
                    utc = utc - relativedelta(years=1)
                else:
                    utc = utc - timedelta(days=1)
            else:
                return

    def run(self):
        for m in self.symbols:
            self.work(m)


def run(args):
    available_exchanges = list(map(lambda i: i.name, pkgutil.iter_modules(exchange_list.__path__)))
    try:
        options = json.loads(args.options) if args.options is not None else None
    except json.JSONDecodeError:
        print('Options are not JSON-serializable: {}'.format(args.options))
        exit(-1)
    else:
        exchange = args.exchange.lower()
        if exchange not in available_exchanges:
            print('Unsupported exchange "{}"; available: {}'.format(exchange, ', '.join(available_exchanges)))
            exit(-1)
        exchange = getattr(importlib.import_module('tokenmds.exchange.' + exchange), '__cls__')()
        exchange = exchange(options)
        available_symbols = exchange.ls()
        symbols = list()
        for symbol in args.symbols:
            symbol = symbol.split('-')
            if len(symbol) != 2:
                print('Invalid symbol "{}" for "{}"; available: {}'.format('-'.join(symbol), exchange.__id__, ', '.join([str(s) for s in available_symbols])))
            else:
                symbol = Market(symbol[0], symbol[1])
                if symbol not in available_symbols:
                    print('Unsupported symbol "{}" for "{}"; available: {}'.format('{}-{}'.format(symbol.base, symbol.quote), exchange.__id__, ', '.join([str(s) for s in available_symbols])))
                else:
                    symbols.append(symbol)
        if not len(symbols):
            exit(-1)
        # Create Crawler
        crawler = Crawler(
            exchange=exchange,
            symbols=symbols,
            path=args.path,
            tf=args.tf
        )
        try:
            crawler.run()
        except KeyboardInterrupt:
            log('Crawler is stopping...')
            exit(0)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', type=str, default='./data/ohlc', help='path, e.g.: ./data/ohlc')
    parser.add_argument('--tf', type=str, default='M1', help='tf, e.g.: M1, H1, D1')
    parser.add_argument('--options', type=str, default=None, help='options, e.g.: \'{"proxies":{"https":"socks5h://192.168.1.100:11001"}}\'')
    parser.add_argument('exchange', type=str, help='exchange ID, e.g., binance')
    parser.add_argument('symbols', type=str, nargs='+', help='list of hyphen separated symbols, e.g., ETH-BTC')
    args, _ = parser.parse_known_args()
    run(args)
