import os
from time import sleep
import pandas as pd
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


def check_if_crypto_pair_exists(client, crypto_symbol):
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


def check_crypto_balance(client, crypto_symbol):
    info = client.get_account()
    df = pd.DataFrame(info["balances"])
    df["free"] = df["free"].astype(float).round(4)
    df = df.loc[(df['asset'] == crypto_symbol) & (df["free"] > 0)]
    return df["free"].item()


def check_decimals(client, crypto_symbol):
    info = client.get_symbol_info(f'{crypto_symbol}USDT')
    val = info['filters'][2]['stepSize']
    decimal = 0
    is_dec = False
    for c in val:
        if is_dec is True:
            decimal += 1
        if c == '1':
            break
        if c == '.':
            is_dec = True
    return decimal


async def main():
    crypto_symbol = 'ROSE'
    usdt_qty = 161
    client = AsyncClient(api_key=os.environ['API_KEY'], api_secret=os.environ['API_SECRET'])
    client_direct = Client(api_key=os.environ['API_KEY'], api_secret=os.environ['API_SECRET'])
    crypto_con = try_connection(crypto_symbol=crypto_symbol, client=client)
    # Check if pair exist
    check_if_crypto_pair_exists(client=client_direct, crypto_symbol=crypto_symbol)
    try:
        crypto_balance = check_crypto_balance(client=client_direct, crypto_symbol=crypto_symbol)
    except Exception as e:
        print(f"Balance of {crypto_symbol} is null")
        crypto_balance = 0
        pass
    # Check crypto decimals
    decimal = check_decimals(client=client_direct, crypto_symbol=crypto_symbol)
    async with crypto_con.get_bnbsm() as tscm_crypto_usdt:
        res = await tscm_crypto_usdt.recv()
        dt_crypto_usdt = create_dataframe_get(res)
        quantity_buy = usdt_qty / dt_crypto_usdt['Price'].iloc[-1]
        quantity_sell = quantity_buy + crypto_balance
        print(f"Quantity to buy {quantity_buy}")
        print(f"Buy order at: \n {dt_crypto_usdt}")
        await crypto_con.buy_order_market(qty=round(quantity_buy, decimal))
        input("Press Enter to continue...")
        await crypto_con.sell_order_market(qty=round(quantity_sell, decimal))
        print(f"Quantity sold {quantity_sell}")
        await client.close_connection()
        print("Trade done.")
        await client.close_connection()
        client_direct.close_connection()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
