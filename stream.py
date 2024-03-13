import asyncio
import json

import websockets


async def stream():
    api_uri = 'ws://192.168.50.110/prd/kabusapi/websocket'
    async with websockets.connect(api_uri, ping_timeout=None) as websocket:
        while not websocket.closed:
            response = await websocket.recv()
            board = json.loads(response)
            print("{} {} {} {} {} {} {} OVER:{} UNDER: {} RATIO:{:.3f}".format(

                board['CurrentPriceTime'],
                board['Symbol'],
                board['SymbolName'],
                board['PreviousClose'],
                board['CurrentPrice'],
                board['ChangePreviousClose'],
                board['ChangePreviousClosePer'],
                board['OverSellQty'],
                board['UnderBuyQty'],
                board['UnderBuyQty'] / board['OverSellQty'] if board['OverSellQty'] != 0 else 0
            ))


loop = asyncio.get_event_loop()
loop.create_task(stream())
try:
    loop.run_forever()
except KeyboardInterrupt:
    exit()
