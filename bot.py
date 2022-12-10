#!/bin/python
import asyncio
import datetime
import sys
import traceback
import websockets
import requests
import json
from kafka import KafkaProducer

channels_login_to_id = {}
channels_detail = {}


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


async def fetch_channels(account, password, url, client_id):
    # Fetch live channels
    channels_count = 100
    # streams_url = "https://api.twitch.tv/helix/streams?first=" + str(channels_count)
    streams_url = "https://api.twitch.tv/helix/streams?" + "first=" + str(channels_count)
    resp = requests.get(streams_url, headers={
        "Authorization": "Bearer " + password,
        "Client-Id": client_id
    }).json()
    try:
        channels = resp['data']
    except:
        print(resp)
        quit()

    for channel in channels:
        channels_login_to_id[channel['user_login']] = channel['user_id']

    channels_url = "https://api.twitch.tv/helix/channels?broadcaster_id="
    for login in channels_login_to_id:
        resp = requests.get(channels_url + channels_login_to_id[login], headers={
            "Authorization": "Bearer " + password,
            "Client-Id": client_id
        }).json()
        try:
            channels_detail[channels_login_to_id[login]] = resp['data']
        except:
            print(resp)
            quit()

    async with websockets.connect(url) as websocket:
        await websocket.send("PASS oauth:" + password)
        await websocket.send("NICK " + account)
        for channel in channels:
            await websocket.send("JOIN #" + channel["user_login"])
        return websocket


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
                channel = message[2][0][1:]
                chatMessage = message[2][1]
                details = channels_detail[channels_login_to_id[channel]][0]
                producer.send(topic, bytes(json.dumps({
                    'channel': channel,
                    'user': chatUser,
                    'message': chatMessage,
                    'date': datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S"),
                    'details': details
                }), 'utf-8'))


async def main() -> None:
    f = open("config.json", "r")
    config = json.loads(f.read())
    f.close()

    account = config['twitch_bot']['account']
    password = config['twitch_bot']['token']
    url = config['twitch_bot']['url']
    client_id = config['twitch_bot']['Client_Id']

    websocket = await fetch_channels(account, password, url, client_id)

    while True:
        try:
            await handler(websocket)
        except Exception:
            print(traceback.format_exc())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception:
        print(traceback.format_exc())
