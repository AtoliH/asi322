import asyncio
import websockets
from kafka import KafkaProducer


def parsemsg(s):
    """Breaks a message from an IRC server into its prefix, command, and arguments.
    """
    prefix = ''
    trailing = []
    if not s:
        raise Exception("Empty line.")
    if s[0] == ':':
        prefix, s = s[1:].split(' ', 1)
    if s.find(' :') != -1:
        s, trailing = s.split(' :', 1)
        args = s.split()
        args.append(trailing)
    else:
        args = s.split()
    command = args.pop(0)
    return prefix, command, args


async def handler(websocket):
    topic = "asi322"
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    while True:
        rawIrcMessage = (await websocket.recv()).strip()
        rawMessages = rawIrcMessage.split('\r\n')
        for rawMessage in rawMessages:
            message = parsemsg(rawMessage)
            print(message)
            if message[1] == 'PRIVMSG':
                producer.send(topic, bytes(message[2][1], 'utf-8'))


async def main() -> None:
    channel = "fire937"
    account = "asi322"
    password = "oauth:c6gx9m317pcrgro5hkyj0x4c1hwrqo"
    url = "ws://irc-ws.chat.twitch.tv:80"

    async with websockets.connect(url) as websocket:
        await websocket.send("PASS " + password)
        await websocket.send("NICK " + account)
        await websocket.send("JOIN #" + channel)
        await handler(websocket)

if __name__ == "__main__":
    asyncio.run(main())
