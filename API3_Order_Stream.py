import redis
import ccxt.pro as ccxt
import asyncio
import sys
import json
import Coinex_Keys
import datetime

class COINEX_MM():
    
    def __init__(self):
    
        self.exchange = ccxt.coinex({
            'apiKey': Coinex_Keys.API_KEY,
            'secret': Coinex_Keys.API_SECRET,
            'enableRateLimit': True,
            'verbose': False,
            })
        
        self.r = redis.Redis(host='localhost', port=6379, db=0)
        self.exchange.options['defaultType'] = 'swap'
        self.market = Coinex_Keys.MARKET

    async def fetch_order_changes(self):
        orders = await self.exchange.watch_orders(symbol=self.market)
        if orders[0]['remaining'] == 0:
            print('filled {} order at time {}' .format(orders[0]['side'], datetime.datetime.now()))
            self.r.set(orders[0]['id'], json.dumps({'symbol': self.market, 'side': orders[0]['side'], 'price': orders[0]['info']['price'], 'amount': 0}), ex=300)

    async def main(self):
        while True:
            await self.fetch_order_changes()

    @staticmethod
    def start(trader):
        while True:
            try:
                asyncio.get_event_loop().run_until_complete(trader.main())
            except:
                continue

if __name__ == '__main__':
    if len(sys.argv) > 1:
        COINEX_MM.start(trader=COINEX_MM(file=f'{sys.argv[1]}'))
    else:
        COINEX_MM.start(trader=COINEX_MM())