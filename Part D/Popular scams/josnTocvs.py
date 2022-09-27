import json
#Read data
with open('input/scams_input.json', 'r') as json_input:
	load_data = json_input.read()


data = json.loads(load_data)

CVS_file= open('input/scams.csv', 'w')

for sc_report in data['result']:
	try:
		scam = data["result"][sc_report]
		for addre in scam["addresses"]:
			CVS_file.write(addre)
			CVS_file.write('/')
			CVS_file.write(str(scam['category']))
			CVS_file.write('/')
			CVS_file.write(str(scam['status']))
			CVS_file.write('\n')
	except:
		pass

print("scams.cvs saved.")
