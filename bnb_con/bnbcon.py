from binance import BinanceSocketManager


class BnbConnection:
    def __init__(self, symbol, client):
        self.symbol = symbol
        self.client = client

    def get_bnbsm(self):
        bm = BinanceSocketManager(self.client)
        ts = bm.trade_socket(self.symbol)
        return ts

    async def get_balance_coin(self, coin):
        balance_coin = await self.client.get_asset_balance(asset=coin)['free']
        return float(balance_coin)

    async def buy_order_market(self, qty):
        buy_order = await self.client.create_order(symbol=self.symbol, side='BUY', type='MARKET', quantity=qty)
        print(buy_order)
        return buy_order

    async def sell_order_market(self, qty):
        buy_order = await self.client.create_order(symbol=self.symbol, side='SELL', type='MARKET', quantity=qty)
        print(buy_order)
        return buy_order

    async def sell_order_limit(self, qty, price):
        sell_order = await self.client.create_order(symbol=self.symbol, side='SELL', type='LIMIT', price=price,
                                                    quantity=qty)
        print(sell_order)
        return sell_order

    async def exchange_order(self, qty):
        exchange_order = await self.client.create_order(symbol=self.symbol, side='SELL', type='MARKET', quantity=qty)
        print(exchange_order)
        return exchange_order
