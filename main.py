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


def buy_model(dt_crypto1, dt_crypto2, dt_fiat):
    price_usdt = dt_crypto1['Price'].iloc[-1]
    price_eur = dt_crypto2['Price'].iloc[-1]
    price_fiat = dt_fiat['Price'].iloc[-1]
    price_exchange = price_eur * price_fiat
    price_rate = 100 - price_usdt * 100 / price_exchange
    # 0.075 fee
    if price_rate >= 0.35:
        print(datetime.datetime.now())
        print(f'TRADING usdt at rate: {price_rate}')
        return 'usdt'
    elif price_rate <= -0.35:
        print(datetime.datetime.now())
        print(f'TRADING eur at rate: {price_rate}')
        return 'eur'
    else:
        print(datetime.datetime.now())
        print(f'NOT BUY at rate: {price_rate}')
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
    async def buy_order(self, qty, price):
        buy_order = await self.client.create_order(symbol=self.symbol, side='BUY', type='LIMIT_MAKER', price=price,
                                                   quantity=qty)
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
    client = AsyncClient(api_key=os.environ['API_KEY'], api_secret=os.environ['API_SECRET'])
    eos_usdt = BnbConnection(symbol='EOSUSDT', client=client)
    eos_eur = BnbConnection(symbol='EOSEUR', client=client)
    eur_usdt = BnbConnection(symbol='EURUSDT', client=client)

    async with eos_usdt.get_bnbsm() as tscm_eos_usdt:
        async with eos_eur.get_bnbsm() as tscm_eos_eur:
            async with eur_usdt.get_bnbsm() as tscm_eur_usdt:
                while True:
                    res = await tscm_eos_usdt.recv()
                    dt_eos_usdt = create_dataframe_get(res)
                    price_usdt = dt_eos_usdt['Price'].iloc[-1]
                    res = await tscm_eos_eur.recv()
                    dt_eos_eur = create_dataframe_get(res)
                    price_eur = dt_eos_eur['Price'].iloc[-1]
                    res = await tscm_eur_usdt.recv()
                    dt_eur_usdt = create_dataframe_get(res)
                    # usdt to eur
                    if buy_model(dt_crypto1=dt_eos_usdt, dt_crypto2=dt_eos_eur, dt_fiat=dt_eur_usdt) == 'usdt':
                        await eos_usdt.buy_order(qty=3, price=price_usdt - (0.2 * price_usdt / 100))
                        await eos_eur.sell_order(qty=3, price=price_eur + (0.1 * price_eur / 100))
                        await eur_usdt.exchange_order(qty=eur_usdt.get_balance_coin('EUR'))
                        break
                    # eur to usdt
                    if buy_model(dt_crypto1=dt_eos_usdt, dt_crypto2=dt_eos_eur, dt_fiat=dt_eur_usdt) == 'eur':
                        await eos_eur.buy_order(qty=3, price=price_eur - (0.2 * price_eur / 100))
                        await eos_usdt.sell_order(qty=3, price=price_usdt + (0.1 * price_usdt / 100))
                        await eur_usdt.exchange_order(qty=eur_usdt.get_balance_coin('USDT'))
                        break
                    sleep(10)

    await client.close_connection()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
