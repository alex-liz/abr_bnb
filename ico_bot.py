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
    trade_flag = True
    total_balance = 0
    async with crypto_con.get_bnbsm() as tscm_crypto_usdt:
        while trade_flag:
            res = await tscm_crypto_usdt.recv()
            dt_crypto_usdt = create_dataframe_get(res)
            quantity_trade = usdt_qty / dt_crypto_usdt['Price'].iloc[-1]
            buy_price_trade = dt_crypto_usdt['Price'].iloc[-1]
            print(f"Quantity to buy {quantity_trade}")
            print(f"Buy order at: \n {dt_crypto_usdt}")
            await crypto_con.buy_order_market(qty=round(quantity_trade, 2))
            trading_count = 0
            sleep(3)
            async with crypto_con.get_bnbsm() as tscm_crypto_usdt:
                res = await tscm_crypto_usdt.recv()
                dt_crypto_usdt = create_dataframe_get(res)
                sell_crypto_price = dt_crypto_usdt['Price'].iloc[-1]
                count_loops = 0
                close_trade = True
                while close_trade:
                    if sell_crypto_price < buy_price_trade and count_loops < 5:
                        print("Waiting price to move up...")
                        res = await tscm_crypto_usdt.recv()
                        dt_crypto_usdt = create_dataframe_get(res)
                        sell_crypto_price = dt_crypto_usdt['Price'].iloc[-1]
                        print(f"Buying price: {buy_price_trade} and Selling price: {sell_crypto_price}")
                        count_loops += 1
                        sleep(5)
                    elif sell_crypto_price > buy_price_trade and count_loops < 5:
                        print(f"Sell order at: \n {dt_crypto_usdt}")
                        await crypto_con.sell_order_market(qty=round(quantity_trade, 2))
                        total_balance = total_balance + (dt_crypto_usdt['Price'].iloc[-1] - buy_price_trade)
                        print(f"Profit: {dt_crypto_usdt['Price'].iloc[-1] - buy_price_trade}")
                        close_trade = False
                    elif count_loops == 5:
                        print("Loop limit reached")
                        print(f"Sell order at: \n {dt_crypto_usdt} quantity {quantity_trade}")
                        await crypto_con.sell_order_market(qty=round(quantity_trade, 2))
                        print(f"Loss: {dt_crypto_usdt['Price'].iloc[-1] - buy_price_trade}")
                        total_balance = total_balance + (dt_crypto_usdt['Price'].iloc[-1] - buy_price_trade)
                        close_trade = False
                trading_count += 1
            if trading_count == 3:
                await client.close_connection()
                trade_flag = False
                print(f"Total balance: {total_balance}")
            sleep(10)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
