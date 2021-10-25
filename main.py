import asyncio
import datetime
import os
from time import sleep

import pandas as pd
from binance.client import AsyncClient
from binance import BinanceSocketManager


def create_dataframe_get(msg):
    df = pd.DataFrame([msg])
    df = df.loc[:, ['s', 'E', 'p']]
    df.columns = ['symbol', 'Time', 'Price']
    df.Price = df.Price.astype(float)
    df.Time = pd.to_datetime(df.Time, unit='ms')
    return df


def buy_model(price_crypto_usdt, price_crypto_usdc):
    price_rate_usdt = 100 - price_crypto_usdt * 100 / price_crypto_usdc
    # 0.075 fee
    if price_rate_usdt >= 0.25:
        print(datetime.datetime.now())
        print(f'TRADING usdt at rate: {price_rate_usdt}')
        return 'usdt'
    elif price_rate_usdt <= -0.25:
        print(datetime.datetime.now())
        print(f'TRADING usdc at rate: {price_rate_usdt}')
        return 'usdc'
    else:
        print(datetime.datetime.now())
        print(f'NOT BUY at rate: {price_rate_usdt}')
        return None


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

    # check quantity
    async def buy_order(self, qty):
        buy_order = await self.client.create_order(symbol=self.symbol, side='BUY', type='MARKET', quantity=qty)
        print(buy_order)
        return buy_order

    async def sell_order(self, qty, price):
        sell_order = await self.client.create_order(symbol=self.symbol, side='SELL', type='LIMIT_MAKER', price=price,
                                                    quantity=qty)
        print(sell_order)
        return sell_order

    async def exchange_order(self, qty):
        exchange_order = await self.client.create_order(symbol=self.symbol, side='SELL', type='MARKET', quantity=qty)
        print(exchange_order)
        return exchange_order


async def main():
    crypto_symbol = 'EOS'
    client = AsyncClient(api_key=os.environ['API_KEY'], api_secret=os.environ['API_SECRET'])
    crypto_usdt = BnbConnection(symbol=f'{crypto_symbol}USDT', client=client)
    crypto_usdc = BnbConnection(symbol=f'{crypto_symbol}USDC', client=client)
    usdc_usdt = BnbConnection(symbol='USDCUSDT', client=client)

    async with crypto_usdt.get_bnbsm() as tscm_crypto_usdt:
        async with crypto_usdc.get_bnbsm() as tscm_crypto_usdc:
            async with usdc_usdt.get_bnbsm() as tscm_usdc_usdt:
                while True:
                    res = await tscm_crypto_usdt.recv()
                    dt_crypto_usdt = create_dataframe_get(res)
                    price_crypto_usdt = dt_crypto_usdt['Price'].iloc[-1]
                    res = await tscm_crypto_usdc.recv()
                    dt_crypto_usdc = create_dataframe_get(res)
                    price_crypto_usdc = dt_crypto_usdc['Price'].iloc[-1]
                    res = await tscm_usdc_usdt.recv()
                    dt_usdc_usdt = create_dataframe_get(res)
                    price_usdc_usdt = dt_usdc_usdt['Price'].iloc[-1]
                    # usdt to usdc
                    if buy_model(price_crypto_usdt=price_crypto_usdt, price_crypto_usdc=price_crypto_usdc) == 'usdt':
                        # await crypto_usdt.buy_order(qty=3)
                        # await crypto_usdc.sell_order(qty=3, price=round(price_usdc + (0.08 * price_usdc / 100), 3))
                        #await usdc_usdt.exchange_order(qty=usdc_usdt.get_balance_coin('usdc'))
                        break
                    # usdc to usdt
                    if buy_model(price_crypto_usdt=price_crypto_usdt, price_crypto_usdc=price_crypto_usdc) == 'usdc':
                        # await crypto_usdc.buy_order(qty=3)
                        # await crypto_usdt.sell_order(qty=3, price=round(price_usdt + (0.08 * price_usdt / 100), 3))
                        #await usdc_usdt.exchange_order(qty=usdc_usdt.get_balance_coin('USDT'))
                        break
                    sleep(10)

    await client.close_connection()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
