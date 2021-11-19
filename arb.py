import asyncio
import datetime
import os
import decimal
from time import sleep

import pandas as pd
from binance.client import AsyncClient
from binance import BinanceSocketManager

from bnb_con.bnbcon import BnbConnection


def float_to_str(f):
    """
    Convert the given float to a string,
    without resorting to scientific notation
    """
    ctx = decimal.Context()
    d1 = ctx.create_decimal(repr(f))
    return format(d1, 'f')


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
        print(f'USDT price: {price_crypto_usdt} lower than USDC price: {price_crypto_usdc}')
        return 'usdt'
    elif price_rate_usdt <= -0.25:
        print(datetime.datetime.now())
        print(f'TRADING usdc at rate: {price_rate_usdt}')
        print(f'USDC price: {price_crypto_usdc} lower than USDT price: {price_crypto_usdt}')
        return 'usdc'
    else:
        print(datetime.datetime.now())
        print(f'NOT BUY at rate: {price_rate_usdt}')
        print(f'USDC price: {price_crypto_usdc}')
        print(f'USDT price: {price_crypto_usdt}')
        return None


async def main():
    crypto_symbol = 'SHIB'
    quantity_trade = 148500
    client = AsyncClient(api_key=os.environ['API_KEY'], api_secret=os.environ['API_SECRET'])
    crypto_usdt = BnbConnection(symbol=f'{crypto_symbol}USDT', client=client)
    crypto_usdc = BnbConnection(symbol=f'{crypto_symbol}BUSD', client=client)
    usdc_usdt = BnbConnection(symbol='BUSDUSDT', client=client)

    async with crypto_usdt.get_bnbsm() as tscm_crypto_usdt:
        async with crypto_usdc.get_bnbsm() as tscm_crypto_usdc:
            while True:
                res = await tscm_crypto_usdt.recv()
                dt_crypto_usdt = create_dataframe_get(res)
                price_crypto_usdt = dt_crypto_usdt['Price'].iloc[-1]
                res = await tscm_crypto_usdc.recv()
                dt_crypto_usdc = create_dataframe_get(res)
                price_crypto_usdc = dt_crypto_usdc['Price'].iloc[-1]
                # usdt to usdc
                if buy_model(price_crypto_usdt=price_crypto_usdt, price_crypto_usdc=price_crypto_usdc) == 'usdt':
                    await crypto_usdt.buy_order_market(qty=quantity_trade)
                    await crypto_usdt.sell_order_limit(qty=quantity_trade,
                                                 price=float_to_str(round(price_crypto_usdt * 1.003, 8)))
                    # await usdc_usdt.exchange_order(qty=round(price_crypto_usdc * quantity_trade, 4))
                    break
                # usdc to usdt
                if buy_model(price_crypto_usdt=price_crypto_usdt, price_crypto_usdc=price_crypto_usdc) == 'usdc':
                    await crypto_usdc.buy_order_market(qty=quantity_trade)
                    await crypto_usdc.sell_order_limit(qty=quantity_trade,
                                                 price=float_to_str(round(price_crypto_usdt * 1.003, 8)))
                    # await usdc_usdt.exchange_order(qty=round(price_crypto_usdt * quantity_trade, 4))
                    break
                sleep(10)

    await client.close_connection()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
