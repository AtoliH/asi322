import asyncio
import websockets


async def handler(websocket):
    while True:
        message = await websocket.recv()
        print(message)


async def main() -> None:
    topic = 

    channel = "scream"
    account = "asi322"
    password = "oauth:c6gx9m317pcrgro5hkyj0x4c1hwrqo"
    url = "ws://irc-ws.chat.twitch.tv:80"
    
    async with websockets.connect(url) as websocket:
        await websocket.send("PASS " + password)
        await websocket.send("NICK asi322")
        await websocket.send("JOIN #" + channel)
        await handler(websocket)

if __name__ == "__main__":
    asyncio.run(main())
