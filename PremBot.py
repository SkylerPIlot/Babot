import os
import discord
import asyncio
import gc
import pandas as pd
import requests
import json
from sqlalchemy import create_engine
from sqlalchemy import text
from discord.ext import commands

import jwt

import datetime

jwt_key = os.environ.get('JWT_KEY')

tempQ = os.environ.get('BAS_Q')  # BAS

babQ = os.environ.get('BAB_Q')

LeqQ = os.environ.get('LEAG_Q')

disc_auth = os.environ.get('DISC_TOKEN')

bot_auth = os.environ.get('BOT_TOKEN')

DMWhiteList = [807214442017849344,
               353740585154379777,
               391053076846608385,
               143818887815888896,
               436982014256611338,
               294651168880197632,
               667205402693337091,
               534359839376408598,
               854072414854185020,
               349306424041668609,
               391053076846608385,
               266520936621146112,
               221011642996097024,
               199583676999139328,
               100827367324274688,
               122817245872128001]

DATABASE_URL = os.environ.get('DATABASE_URL').replace("://", "ql://", 1)
db = create_engine(DATABASE_URL)
conn = db.connect()

bot = commands.Bot(command_prefix='!')
gc.collect()


# -----------------------------Setup functions-----------------------------------
async def log_bas():
    basROn = 0
    basROff = 0
    basRProg = 0
    basPOn = 0
    basPOff = 0
    basPProg = 0
    try:
        Temp = pd.read_csv(tempQ)
    except:
        print("Error retrieving bas q retrying in 10s")
    for i in range(len(Temp)):
        type = str(Temp.values[i][2])
        status = str(Temp.values[i][0])
        id = str(Temp.values[i][1])
        nameP = str(Temp.values[i][3])
        nameR = str(Temp.values[i][4])

        if status == "Online":
            if type == "P":
                basPOn = basPOn + 1
            if type == "R":
                basROn = basROn + 1
            continue

        if status == "In Progress":
            if type == "P":
                basPProg = basPProg + 1
            if type == "R":
                basRProg = basRProg + 1
            continue

        if status == "Done":
            continue

        else:
            if type == "P":
                basPOff = basPOff + 1
            if type == "R":
                basROff = basROff + 1

    current_utc = datetime.datetime.now(datetime.timezone.utc)
    time = current_utc.strftime("%m/%d/%y %H:%M:%S")
    keysql = f"INSERT INTO bas VALUES({basROn},{basROff},{basRProg},{basPOn},{basPOff},{basPProg},'{time}');"
    conn.execute(text(keysql))


async def log_bab():
    babSOn = 0
    babSOff = 0
    babSProg = 0
    babGOn = 0
    babGOff = 0
    babGProg = 0
    babPOn = 0
    babPOff = -1
    babPProg = 0
    try:
        Temp = pd.read_csv(babQ)
    except:
        print("error reading q")
    for i in range(len(Temp)):
        type = str(Temp.values[i][1])
        status = str(Temp.values[i][0])
        id = str(Temp.values[i][2])
        name = str(Temp.values[i][3])
        if status == "Online":
            if type == "Silver":
                babSOn = babSOn + 1
            if type == "Gold":
                babGOn = babGOn + 1
            if type == "Platinum":
                babGOn = babGOn + 1

        if status == "In Progress":
            if type == "Silver":
                babSProg = babSProg + 1
            if type == "Gold":
                babGProg = babGProg + 1
            if type == "Platinum":
                babPProg = babPProg + 1

        if status == "Offline":
            if type == "Silver":
                babSOff = babSOff + 1
            if type == "Gold":
                babGOff = babGOff + 1
            if type == "Platinum":
                babPOff = babPOff + 1
    current_utc = datetime.datetime.now(datetime.timezone.utc)
    time = current_utc.strftime("%m/%d/%y %H:%M:%S")
    keysql = f"INSERT INTO bab VALUES({babSOn},{babSOff},{babSProg},{babGOn},{babGOff},{babGProg},{babPOn},{babPOff},{babPProg},'{time}');"
    conn.execute(text(keysql))


async def log_loop():
    while True:
        await log_bas()
        print("Logged time in bas db")
        await log_bab()
        print("Logged time in bab db")
        # async log_lba()
        # print("Logged time in lba db")

        # await asyncio.sleep(60)#testing
        await asyncio.sleep(3600)  # production


async def retrive_data(id):
    r = requests.get(f'https://discord.com/api/v9/channels/{id}/messages', headers={
        'Authorization': disc_auth})
    jsons = json.loads(r.text)
    all = []
    full = []
    for value in jsons:
        for item in value['embeds']:
            nameidnum = item['title'].split("ID")
            for i in range(len(nameidnum)):
                nameidnum[i] = nameidnum[i].strip(' (:)')

            onitemprio = item['description'].split("  ")
            for i in range(len(onitemprio)):
                onitemprio[i] = onitemprio[i].strip()

            all = nameidnum + onitemprio
            full.append(all)

    # print(full)
    onlineList = []
    for i in range(len(full)):
        if full[i][2] != ":red_circle:" and full[i][2] != ":yellow_circle:":
            tempList = []
            tempList.append(full[i][1])
            ts = full[i][3].split(" - ")
            tempList.append(ts[1])
            tempList.append(ts[0])
            onlineList.append(tempList)

    return (onlineList)


async def pm_person(msg, disc_id):
    user = await bot.fetch_user(disc_id)
    # print(str(user.dm_channel))
    dm = user.dm_channel
    if dm == None:
        dm = await user.create_dm()
    await dm.send(msg)


async def check_channel(ctx):
    try:
        if ctx.guild.id == 724744916693024858:
            return True

    except:
        await ctx.send("please use this command only in the correct channel")
        return False
    return False


async def check_whitelist(ctx):
    if ctx.author.id in DMWhiteList:
        return True
    await ctx.send("Unauthorized user")
    return False


def check_dm(ctx):
    if ctx.guild != None:
        return True
    return False


# ------------Actualy Commands------------------
@bot.event
async def on_ready():
    print(f'Logged in as {bot.user.name}')
    bot.loop.create_task(log_loop())


@bot.command()
@commands.check(check_channel)
async def api(ctx):
    await ctx.send("Here is your api key, check your msgs from the bot")
    await ctx.send("If you'd like an api key that includes alt gc pm #skylerminer directly")
    jwt_contents = {'disc_id': str(ctx.author.id),
                    "exp": datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(seconds=2678400)}
    encoded = jwt.encode(jwt_contents, jwt_key, algorithm="HS256")
    await pm_person("Here is your api key, it expires in 31 days from now.", ctx.author.id)
    await pm_person(encoded, ctx.author.id)


@bot.command()
@commands.check(check_whitelist)
async def on(ctx):
    if check_dm(ctx):
        await ctx.send("These commands are limited to Dms only, sorry")
        return
    await bot.change_presence(status=discord.Status.online, activity=discord.Game(name="Showing someone the Q"))
    try:
        Temp = pd.read_csv(tempQ)
    except:
        print("Error retrieving bas q retrying in 10s")
    await ctx.send("***Bas Queue***")
    for i in range(len(Temp)):
        type = str(Temp.values[i][2])
        status = str(Temp.values[i][0])
        id = str(Temp.values[i][1])
        nameP = str(Temp.values[i][3])
        nameR = str(Temp.values[i][4])

        if status == "Online":
            if type == "P":
                # await message.channel.send("Premium: " + nameP + ", ID : " +id)
                await ctx.send("Premium: <a:premsniper:932130618257604698> ID: " + id)
            if type == "R":
                # await message.channel.send("Regular: " + nameR + ", ID : " +id)
                await ctx.send("Regular ID: " + id)
    try:
        Temp = pd.read_csv(babQ)
    except:
        print("Error retrieving bab q retrying in 10s")
    await ctx.send("***Bab Queue***")
    for i in range(len(Temp)):
        type = str(Temp.values[i][1])
        status = str(Temp.values[i][0])
        id = str(Temp.values[i][2])
        name = str(Temp.values[i][3])
        if status == "Online":
            if type == "Silver":
                # await message.channel.send("Premium: " + name + ", ID : " +id)
                await ctx.send("Silver: ID: " + id)
            if type == "Gold":
                # await message.channel.send("Regular: " + name + ", ID : " +id)
                await ctx.send("Gold <a:premsniper:932130618257604698> ID: " + id)
            if type == "Platinum":
                # await message.channel.send("Regular: " + name + ", ID : " +id)
                await ctx.send("Platinum <a:premsniper:932130618257604698> ID: " + id)
    await ctx.send("***LBA Queue***")

    try:
        print("grabbing LBA q")
        LbaList = await retrive_data(832430782030151701)
    except:
        print("Error retriving lba Q")

    for i in LbaList:
        await ctx.send(i[1] + " ID: " + i[0])

    try:
        Temp = pd.read_csv(LeqQ)
    except:
        print("Error retrieving leauq q retrying in 10s")
        await asyncio.sleep(10)

    await ctx.send("***League Queue***")
    for i in range(len(Temp)):
        bid = str(Temp.values[i][1])
        status = str(Temp.values[i][0])
        id = str(Temp.values[i][2])
        name = str(Temp.values[i][3])
        if status == "Online":
            await ctx.send("Customer online ID: " + id + " With a bid of " + bid)
    print("Done")
    await bot.change_presence(status=discord.Status.online, activity=discord.Game(name="Perusing the Data"))
    return


bot.run(bot_auth)
