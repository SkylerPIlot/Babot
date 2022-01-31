import os
import discord
import asyncio
from urllib.request import urlopen
import gc
import pandas as pd
import requests
import json
from sqlalchemy import create_engine
from sqlalchemy import text
import jwt

import datetime


jwt_key = 'ex39ELAqkwlMgZGntTfNECn4qmeYmjOZRjDJj2z2lolS4QSsoNBBRqwWFHVI'

DATABASE_URL = os.environ.get('DATABASE_URL').replace("://", "ql://", 1)
db = create_engine(DATABASE_URL)
#db = create_engine('postgresql://pmbgclqggryvlc:e48ae104e3ef3fb79213815a6fa596d24b535a11e0bb9768e977a09f7b4b6d89@ec2-34-239-33-57.compute-1.amazonaws.com:5432/dasonl0ovrlp0k')
conn = db.connect()


TaskList = {}
regTaskList = {}
nexturl = "http://blairm.net/bas/update_file_75cb4f7e61.php?359e4ae2f3849098858e10975116e9c68736e91b76ee5c40ee755baaefbbe5e5=1"#BAS
tempQ = "https://docs.google.com/spreadsheets/d/1RA0RXFD61okTc9xbUWbQ9DwAW0cZjVHbY0x7NqzWH2s/export?format=csv"#BAS

babQ = "https://docs.google.com/spreadsheets/d/1oObu0rzcIMJ3E0jGFaykWpjwyg_6d9WUfCnm9NFoUI4/export?format=csv"

LeqQ = "https://docs.google.com/spreadsheets/d/1cBkjg3X0ss5xeyp6IY4KJcIv63NtVi2yQbiHR38bUmI/export?format=csv"



WhiteList = [294651168880197632,126050231308648457,143818887815888896,353740585154379777,931586337067393067]


DMWhiteList=[807214442017849344,
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


PMWhiteList = [(294651168880197632, True)]
print(PMWhiteList)

client = discord.Client()

gc.collect()

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
		#async log_lba()
		#print("Logged time in lba db")

		#await asyncio.sleep(60)#testing
		await asyncio.sleep(3600)#production


async def retrive_data(id):
	header = {
	'authorization:' 'mfa.MWFhdW_tqgOT0cHcctqYy08g6skxsDh9FwmG0SNCzecpLcpoCGnxcSmQrrB7JOI_O6QmTeZjRrevHHkdETMQ'
	}
	r = requests.get(f'https://discord.com/api/v9/channels/{id}/messages', headers={'Authorization': 'mfa.MWFhdW_tqgOT0cHcctqYy08g6skxsDh9FwmG0SNCzecpLcpoCGnxcSmQrrB7JOI_O6QmTeZjRrevHHkdETMQ'})
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
			
	#print(full)	
	onlineList = []
	for i in range(len(full)):
		if full[i][2] != ":red_circle:" and full[i][2] != ":yellow_circle:":
			tempList = []
			tempList.append(full[i][1])
			ts = full[i][3].split(" - ")
			tempList.append(ts[1])
			tempList.append(ts[0])
			onlineList.append(tempList)
					
	return(onlineList)

async def pm_people(msg):
	for i in range(len(PMWhiteList)):
		if PMWhiteList[i][1] == True:

			#print(str(PMWhiteList[i][1]))
			#print(str(PMWhiteList[i][0]))
			user = await client.fetch_user(PMWhiteList[i][0])
			#print(str(user.dm_channel))
			dm = user.dm_channel
			if dm == None:
				dm = await user.create_dm()
			await dm.send(msg)
				
async def pm_person(msg,disc_id):
	user = await client.fetch_user(disc_id)
	# print(str(user.dm_channel))
	dm = user.dm_channel
	if dm == None:
		dm = await user.create_dm()
	await dm.send(msg)

async def pm_loop():
	premList = []
	LBAList = []

	while True:
		await client.change_presence(status=discord.Status.online, activity=discord.Game(name="Observing the Qs"))
		tempList = premList
		try:
			print("Retriving file from webserver for prems")
			Temp = pd.read_csv(tempQ)
		except:
			print("Error retriving file from webserver waiting 10s and trying again")
			await asyncio.sleep(10)
			continue 
		for i in range(len(Temp)):
			type = str(Temp.values[i][2])
			status = str(Temp.values[i][0])
			id = str(Temp.values[i][1])
			nameP = str(Temp.values[i][3])
			nameR = str(Temp.values[i][4])
			if nameP in premList and status != "Online":
				premList.remove(nameP)
				await pm_people("Premium either logged out or in prog ID: " + id)	
			if status == "Online":
				if type == "P":
					if nameP not in premList:
						premList.append(nameP)
						await pm_people("Premium Logged in <a:premsniper:932130618257604698> @here ID: " + id)
		gc.collect()
		try:
			Temp = pd.read_csv(babQ)
		except:
			print("Error retrieving bab q retrying in 10s")
			await asyncio.sleep(10)
			continue 
		for i in range(len(Temp)):
			type = str(Temp.values[i][1])
			status = str(Temp.values[i][0])
			id = str(Temp.values[i][2])
			name = str(Temp.values[i][3])
			if name in premList and status != "Online":
				if id != "nan":
					premList.remove(name)
					await pm_people(type + " either logged out or in prog ID: " + id)
			if status == "Online":
				if type == "Gold" or type == "Platinum":
					if name not in premList:
						premList.append(name)
						await pm_people(type +" Logged in @here <a:premsniper:932130618257604698> ID: " + id)
		


		gc.collect()
		try:
			
			print("grabbing LBA q")
			LbaList = await retrive_data(832430782030151701)
		except:
			print("Error retriving lba Q")
			await asyncio.sleep(10)
			continue
		
		for i in LbaList:	
			
			if i[1] != "Regular":
				if i not in LBAList:
					LBAList.append(i)
					await pm_people("LBA Prem Logged in @here <a:premsniper:932130618257604698> ID: " + i[0])
		for i in LBAList:
			if i not in LbaList:
				LBAList.remove(i)
				await pm_people("LBA Prem Logged out ID: " + i[0])

		try:
			Temp = pd.read_csv(LeqQ)
		except:
			print("Error retrieving leauq q retrying in 10s")
			await asyncio.sleep(10)
			continue 
		for i in range(len(Temp)):
			bid = str(Temp.values[i][1])
			status = str(Temp.values[i][0])
			id = str(Temp.values[i][2])
			name = str(Temp.values[i][3])
			if name in premList and status != "Online":
				if id != "nan":
					premList.remove(name)
					await pm_people("Customer either logged out or in prog ID: " + id)
			if status == "Online":
				if name not in premList:
					premList.append(name)
					await pm_people("Customer Logged in with a bid of: " + bid + " @here <a:premsniper:932130618257604698> ID: " + id)
		
		print("PM Loop")

		gc.collect()

		await asyncio.sleep(60)












async def prem_task(message):
	premList = []
	LBAList = []
	while True:
		tempList = premList
		try:
			print("Retriving file from webserver for prems")
			Temp = pd.read_csv(tempQ)
		except:
			print("Error retriving file from webserver waiting 10s and trying again")
			await asyncio.sleep(10)
			continue 
		for i in range(len(Temp)):
			type = str(Temp.values[i][2])
			status = str(Temp.values[i][0])
			id = str(Temp.values[i][1])
			nameP = str(Temp.values[i][3])
			nameR = str(Temp.values[i][4])
			if nameP in premList and status != "Online":
				premList.remove(nameP)
				#await message.channel.send("Premium: " + nameP + " Either logged out or in prog")
				await message.channel.send("Premium either logged out or in prog ID: " + id)	
			if status == "Online":
				if type == "P":
					if nameP not in premList:
						premList.append(nameP)
						#await message.channel.send("Premium: " + nameP + " Logged in @here")
						await message.channel.send("Premium Logged in <a:premsniper:932130618257604698> @here ID: " + id)
		gc.collect()
		try:
			Temp = pd.read_csv(babQ)
		except:
			print("Error retrieving bab q retrying in 10s")
			await asyncio.sleep(10)
			continue 
		for i in range(len(Temp)):
			type = str(Temp.values[i][1])
			status = str(Temp.values[i][0])
			id = str(Temp.values[i][2])
			name = str(Temp.values[i][3])
			if name in premList and status != "Online":
				if id != "nan":
					premList.remove(name)
					await message.channel.send(type + " either logged out or in prog ID: " + id)
			if status == "Online":
				if type == "Gold" or type == "Platinum":
					if name not in premList:
						premList.append(name)
						await message.channel.send(type +" Logged in @here <a:premsniper:932130618257604698> ID: " + id)
		


		gc.collect()
		try:
			
			print("grabbing LBA q")
			LbaList = await retrive_data(832430782030151701)
		except:
			print("Error retriving lba Q")
			await asyncio.sleep(10)
			continue
		
		for i in LbaList:	
			
			if i[1] != "Regular":
				if i not in LBAList:
					LBAList.append(i)
					await message.channel.send("LBA Prem Logged in @here <a:premsniper:932130618257604698> ID: " + i[0])
		for i in LBAList:
			if i not in LbaList:
				LBAList.remove()
				await message.channel.send("LBA Prem Logged out ID: " + i[0])

		print(str(message.channel.id) + " Channel ran")
		await client.change_presence(status=discord.Status.online, activity=discord.Game(name="Prem Hunters 3D"))
		await asyncio.sleep(480)
		
	

async def reg_task(message):
	regList = []
	while True:
		try:	
			print("Retriving file from webserver for regs")
			Temp = pd.read_csv(tempQ)
		except:
			print("Error retriving file from webserver waiting 10s and trying again")
			await asyncio.sleep(10)
			continue 
		for i in range(len(Temp)):
			type = str(Temp.values[i][2])
			status = str(Temp.values[i][0])
			id = str(Temp.values[i][1])
			nameP = str(Temp.values[i][3])
			nameR = str(Temp.values[i][4])
			
			
			if nameR in regList and status != "Online":
					regList.remove(nameR)
					#await message.channel.send("Premium: " + nameP + " Either logged out or in prog")
					await message.channel.send("BAS Regular either logged out or in prog ID: " + id)
			#print(status)	
			if status == "Online":
				#print(type)
				if type == "R":
					#print(nameR)
					#print(regList)
					if nameR not in regList:
						#print("Added")
						regList.append(nameR)
						#await message.channel.send("Premium: " + nameP + " Logged in @here")
						await message.channel.send("BAS Regular Logged in ID: " + id)
		gc.collect()
		try:
			Temp = pd.read_csv(babQ)
		except:
			print("Error retrieving bab q retrying in 10s")
		for i in range(len(Temp)):
			type = str(Temp.values[i][1])
			status = str(Temp.values[i][0])
			id = str(Temp.values[i][2])
			name = str(Temp.values[i][3])
			#print(name)
			#print(id)
			if name in regList and status != "Online":
				if id != "nan":
					#print("HERE")
					#print(type)
					#print(id)
					regList.remove(name)
					await message.channel.send(type + " either logged out or in prog ID: " + id)
			if status == "Online":
				if type == "Silver":
					if name not in regList:
						regList.append(name)
						await message.channel.send(type +" Logged in ID: " + id)
		gc.collect()




		try:
			print("grabbing LBA q")
			LbaList = await retrive_data(832430782030151701)
		except:
			print("Error retriving lba Q")
			await asyncio.sleep(5)
			continue
		
		for i in LbaList:	
			
			if i[1] == "Regular":
				if i[0] not in regList:
					regList.append(i[0])
					await message.channel.send("LBA Regular Logged in ID: " + i[0])

	
				

		print(str(message.channel.id) + " Channel ran")
		await client.change_presence(status=discord.Status.online, activity=discord.Game(name="Prem Hunters 3D"))
		await asyncio.sleep(120)
	



@client.event
async def on_ready():
	print('We have logged in as {0.user}'.format(client))
	client.loop.create_task(log_loop())
	

@client.event
async def on_message(message):
	if message.author == client.user:
		print("Bot said: " + message.content)
		return
	if message.content == "!pmOn" and message.author.id == 294651168880197632:
		await message.channel.send("Turned on PMlist")
		client.loop.create_task(pm_loop())
		return

	if message.content == "!api" and message.guild.id == 932423487879004180:
		await message.channel.send("Here is your api key, check your msgs from the bot")
		jwt_contents = {'disc_id': str(message.author.id),
						"exp": datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(seconds=600)}
		encoded = jwt.encode(jwt_contents, jwt_key, algorithm="HS256")
		await pm_person("Here is your api key, it expires in 10 minutes from now.", message.author.id)
		await pm_person(encoded, message.author.id)
		return


	if  message.content.startswith('!PMusers') and message.author.id == 294651168880197632:
		msg = message.content.strip("!PMusers")
		await pm_people(msg)

	if  message.content.startswith('!addPM') and message.author.id == 294651168880197632:
		id = message.content.strip(" !addPM")

		try:
			print("Trying to add: " + id)
			#print(PMWhiteList)
			PMWhiteList.append((int(id), True))
			#print(PMWhiteList)
			await message.channel.send("Added " + message.content.strip(" !addDM") + " to PM whitelist")
			pmlistFile = open("pmlist.txt", "a")
			pmlistFile.write(id)
			pmlistFile.write(",True")
			pmlistFile.write("\n")
			pmlistFile.close()
		except:
			await message.channel.send("ERROR")
			


		

	if message.author.id in DMWhiteList and message.content.startswith('!on'):
		await client.change_presence(status=discord.Status.online, activity=discord.Game(name="Showing someone the Q"))
		try:
			Temp = pd.read_csv(tempQ)
		except:
			print("Error retrieving bas q retrying in 10s")
		await message.channel.send("***Bas Queue***")
		for i in range(len(Temp)):
			type = str(Temp.values[i][2])
			status = str(Temp.values[i][0])
			id = str(Temp.values[i][1])
			nameP = str(Temp.values[i][3])
			nameR = str(Temp.values[i][4])
			
			if status == "Online":
				if type == "P":
					#await message.channel.send("Premium: " + nameP + ", ID : " +id)
					await message.channel.send("Premium: <a:premsniper:932130618257604698> ID: " +id)
				if type == "R":
					#await message.channel.send("Regular: " + nameR + ", ID : " +id)
					await message.channel.send("Regular ID: " +id)
		try:
			Temp = pd.read_csv(babQ)
		except:
			print("Error retrieving bab q retrying in 10s")
		await message.channel.send("***Bab Queue***")
		for i in range(len(Temp)):
			type = str(Temp.values[i][1])
			status = str(Temp.values[i][0])
			id = str(Temp.values[i][2])
			name = str(Temp.values[i][3])
			if status == "Online":
				if type == "Silver":
					#await message.channel.send("Premium: " + name + ", ID : " +id)
					await message.channel.send("Silver: ID: " +id)
				if type == "Gold":
					#await message.channel.send("Regular: " + name + ", ID : " +id)
					await message.channel.send("Gold <a:premsniper:932130618257604698> ID: " +id)
				if type == "Platinum":
					#await message.channel.send("Regular: " + name + ", ID : " +id)
					await message.channel.send("Platinum <a:premsniper:932130618257604698> ID: " +id)
		await message.channel.send("***LBA Queue***")

		try:
			print("grabbing LBA q")
			LbaList = await retrive_data(832430782030151701)
		except:
			print("Error retriving lba Q")		

		for i in LbaList:
			await message.channel.send(i[1] + " ID: " + i[0])

		try:
			Temp = pd.read_csv(LeqQ)
		except:
			print("Error retrieving leauq q retrying in 10s")
			await asyncio.sleep(10)

		await message.channel.send("***League Queue***")
		for i in range(len(Temp)):
			bid = str(Temp.values[i][1])
			status = str(Temp.values[i][0])
			id = str(Temp.values[i][2])
			name = str(Temp.values[i][3])
			if status == "Online":
				await message.channel.send("Customer online ID: " +id + " With a bid of " + bid)
		print("Done")	
		return
			

	if message.author.id not in WhiteList and message.content.startswith('!') and message.guild != None:
		await message.channel.send("Unauthorized user, contact bot owner for permission to use these commands <a:premsniper:932130618257604698>")
		return

	if message.guild == None:
		await message.channel.send("Hello, If you have permissions use !on to see who is online in q. Otherwise pm the bot owner")
		#await message.channel.send("If you have bought premium notifications please pm bot owner questions. bot will automatically pm you")
		return

	if message.content.startswith('!addDM') and message.author.id == 294651168880197632:
		id = message.content.strip(" !addDM")
		
		try:
			#await message.channel.send(DMWhiteList)
			print("Trying to add: " + id)
			DMWhiteList.append(int(id))
			await message.channel.send("Added " + message.content.strip(" !addDM") + " to DM whitelist")
			dmlistFile = open("dmlist.txt", "a")
			dmlistFile.write(id)
			dmlistFile.write("\n")
			dmlistFile.close()
			#await message.channel.send(DMWhiteList)
			
		except:
			await message.channel.send("adding failed")
			await message.channel.send("Error check logs")
		


	if message.content.startswith('!next') or message.content.startswith('!n') :
		await message.channel.send("Cmd disabled for showing customer names <a:premsniper:932130618257604698>")
		return
		nextPage = urlopen(nexturl)
		
		nexthtml_bytes = nextPage.read()
		nexthtml = nexthtml_bytes.decode("utf-8")
		if nexthtml == "":
			nexthtml = "Error unable to retrieve"
		await message.channel.send(nexthtml)
		gc.collect()




	if message.content == "!Pon" and message.author.id == 294651168880197632:
		#await message.delete()
		if TaskList.get(str(message.channel.id)) != None:
			await message.channel.send("Notifications have already been setup for this channel")
			return
		await message.channel.send("Prem notifications Set up for Channel")
		TaskList[str(message.channel.id)] = client.loop.create_task(prem_task(message))
		await client.change_presence(status=discord.Status.online, activity=discord.Game(name="Observing the Qs"))

	if message.content == "!Poff":
		try:
			TaskList.get(str(message.channel.id)).cancel()
			del TaskList[str(message.channel.id)]
		
		except:
			await message.channel.send("Error")
		await message.channel.send("Notifications turned off for this Channel")

	if message.content == "!Ron" and message.author.id == 294651168880197632:
		#await message.delete()
		if regTaskList.get(str(message.channel.id)) != None:
			await message.channel.send("Notifications have already been setup for this channel")
			return
		await message.channel.send("Regular notifications Set up")
		regTaskList[str(message.channel.id)] = client.loop.create_task(reg_task(message))
		await client.change_presence(status=discord.Status.online, activity=discord.Game(name="Prem Hunters 3D"))

	if message.content == "!Roff":
		try:
			regTaskList.get(str(message.channel.id)).cancel()
			del regTaskList[str(message.channel.id)]
		
		except:
			await message.channel.send("Error")
		await message.channel.send("Notifications turned off for this Channel")


	if message.content == "!help" or message.content == "!h":
		embed = discord.Embed(title="Help on BasBot", description="Some useful commands")
		embed.add_field(name="!next/!n", value="Grabs next person in Queue")
		embed.add_field(name="!on", value="Shows all online members, their name and ID")
		embed.add_field(name="!Pon", value="Turns on a ping for premiums loggin into q checks q every two minutes, Cannot be turned off")
		embed.add_field(name="!Ron", value="Turns on a msg for regular loggin into q checks q every two minutes, Cannot be turned off")
		await message.channel.send(content=None, embed=embed)
		await client.change_presence(status=discord.Status.online, activity=discord.Game(name="Prem Hunters 3D"))
		#await client.user.edit(username="BA Bot")	
			

client.run('OTMxNTg2MzM3MDY3MzkzMDY3.YeGlYg.VEtCKbwqmtHJglfZhn2ak6-beeU')
