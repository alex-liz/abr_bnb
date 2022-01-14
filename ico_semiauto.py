import os
from time import sleep
from binance import AsyncClient
from binance.client import Client

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


def check_if_cryptopair_exists(client, crypto_symbol):
    crypto_dict = client.get_exchange_info()
    exist = None
    while not exist:
        symbols_list = crypto_dict['symbols']
        for symbol_pair in symbols_list:
            if f'{crypto_symbol}USDT' == symbol_pair['symbol']:
                exist = True
                break
            else:
                exist = False
        if not exist:
            print(f'{crypto_symbol}USDT doesnt exist. Retrying...')
            sleep(2)


async def main():
    crypto_symbol = 'BTC'
    usdt_qty = 30
    client = AsyncClient(api_key=os.environ['API_KEY'], api_secret=os.environ['API_SECRET'])
    client_direct = Client(api_key=os.environ['API_KEY'], api_secret=os.environ['API_SECRET'])
    crypto_con = try_connection(crypto_symbol=crypto_symbol, client=client)
    # Check if pair exist
    check_if_cryptopair_exists(client=client_direct, crypto_symbol=crypto_symbol)

    async with crypto_con.get_bnbsm() as tscm_crypto_usdt:
        res = await tscm_crypto_usdt.recv()
        dt_crypto_usdt = create_dataframe_get(res)
        quantity_trade = usdt_qty / dt_crypto_usdt['Price'].iloc[-1]
        print(f"Quantity to buy {quantity_trade}")
        print(f"Buy order at: \n {dt_crypto_usdt}")
        # await crypto_con.buy_order_market(qty=round(quantity_trade, 2))
        input("Press Enter to continue...")
        # await crypto_con.sell_order_market(qty=round(quantity_trade, 2))
        await client.close_connection()
        print("Trade done.")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
