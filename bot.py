#!/bin/python
import asyncio
import datetime
import traceback
import websockets
import requests
import aiohttp
import json
import subprocess
import os
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


async def fetch_channels(websocket, account, password, client_id):
    while True:
        # Fetch live channels
        channels_count = 100
        streams_url = "https://api.twitch.tv/helix/streams?" + "first=" + str(channels_count)
        resp = requests.get(streams_url, headers={
            "Authorization": "Bearer " + password,
            "Client-Id": client_id
        }).json()
        channels = resp['data']

        for channel in channels:
            channels_login_to_id[channel['user_login']] = channel['user_id']

        channels_url = "https://api.twitch.tv/helix/channels?broadcaster_id="
        async with aiohttp.ClientSession() as session:
            for login in channels_login_to_id:
                print("Fetching " + login + " channel info...")
                async with session.get(channels_url + channels_login_to_id[login], headers={
                    "Authorization": "Bearer " + password,
                    "Client-Id": client_id
                }) as response:
                    resp = (await response.json())

                channels_detail[channels_login_to_id[login]] = resp['data']

        for channel in channels:
            await websocket.send("JOIN #" + channel["user_login"])

        await asyncio.sleep(600)


async def handler(websocket):
    topic = "asi322"
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    while True:
        raw_irc_message = (await websocket.recv()).strip()
        raw_messages = raw_irc_message.split('\r\n')
        for rawMessage in raw_messages:
            message = parsemsg(rawMessage)
            print(message)
            if message[1] == 'PRIVMSG':
                chat_user = message[0]
                channel = message[2][0][1:]
                chat_message = message[2][1]
                details = channels_detail[channels_login_to_id[channel]][0]
                producer.send(topic, bytes(json.dumps({
                    'channel': channel,
                    'user': chat_user,
                    'message': chat_message,
                    'date': datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S"),
                    'details': details
                }), 'utf-8'))


async def main() -> None:
    # Always refresh token on startup
    print("Refreshing twitch token...")
    with open("token.json", "w") as outfile:
        subprocess.run("bash refresh.sh", stdout=outfile)

    token_file = open("token.json", "r")
    token_data = json.loads(token_file.read())
    token_file.close()
    access_token = token_data["access_token"]
    print("Token refreshed, new token: " + access_token)

    f = open("config.json", "r")
    config = json.loads(f.read())
    f.close()

    account = config['twitch_bot']['account']
    password = access_token
    url = config['twitch_bot']['url']
    client_id = config['twitch_bot']['Client_Id']

    async with websockets.connect(url) as websocket:
        # Log into IRC server
        await websocket.send("PASS oauth:" + password)
        await websocket.send("NICK " + account)

        asyncio.create_task(fetch_channels(websocket, account, password, client_id))

        # noinspection PyBroadException
        try:
            await handler(websocket)
        except Exception:
            print(traceback.format_exc())


if __name__ == "__main__":
    while True:
        # noinspection PyBroadException
        try:
            asyncio.run(main())
        except Exception:
            print(traceback.format_exc())
