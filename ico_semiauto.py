import os
from time import sleep
from binance import AsyncClient

from arb import create_dataframe_get
from bnb_con.bnbcon import BnbConnection
import asyncio


def try_connection(crypto_symbol, client):
    try:
        return BnbConnection(symbol=f'{crypto_symbol}USDT', client=client)
    except Exception as e:
        print("Retrying connection...")
        # decrease this
        sleep(1)
        try_connection(crypto_symbol, client)


async def main():
    crypto_symbol = 'PORTO'
    usdt_qty = 30
    client = AsyncClient(api_key=os.environ['API_KEY'], api_secret=os.environ['API_SECRET'])
    crypto_con = try_connection(crypto_symbol=crypto_symbol, client=client)
    async with crypto_con.get_bnbsm() as tscm_crypto_usdt:
        res = await tscm_crypto_usdt.recv()
        dt_crypto_usdt = create_dataframe_get(res)
        quantity_trade = usdt_qty / dt_crypto_usdt['Price'].iloc[-1]
        print(f"Quantity to buy {quantity_trade}")
        print(f"Buy order at: \n {dt_crypto_usdt}")
        # await crypto_con.buy_order_market(qty=round(quantity_trade, 2))
        # Press to continue...
        sleep(10)
        # await crypto_con.sell_order_market(qty=round(quantity_trade, 2))
        await client.close_connection()
        print("Trade done.")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
