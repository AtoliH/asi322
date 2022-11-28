import asyncio
import websockets
import json
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
                chatUser = message[0]
                channel = message[2][0]
                chatMessage = message[2][1]

                producer.send(topic, bytes(json.dumps({'channel': channel, 'user': chatUser, 'message': chatMessage}), 'utf-8'))


async def main() -> None:
    channel = "fire937"
    account = "asi322"
    password = "oauth:h11vzdyclft7lhswf04d8nf1u4ckij"
    url = "ws://irc-ws.chat.twitch.tv:80"

    async with websockets.connect(url) as websocket:
        await websocket.send("PASS " + password)
        await websocket.send("NICK " + account)
        await websocket.send("JOIN #" + channel)
        await handler(websocket)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except:
        print("ça a crash... MAIS JE REDEMARRE")
