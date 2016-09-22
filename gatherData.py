import time,csv,sys
from pytz import timezone
from datetime import datetime
import fileinput
from S3 import S3

sys.path.append('../utils')
import mtaUpdates

# This script should be run seperately before we start using the application
# Purpose of this script is to gather enough data to build a training model for Amazon machine learning
# Each time you run the script it gathers data from the feed and writes to a file
# You can specify how many iterations you want the code to run. Default is 50
# This program only collects data. Sometimes you get multiple entries for the same tripId. we can timestamp the 
# entry so that when we clean up data we use the latest entry

# Change DAY to the day given in the feed
DAY = datetime.today().strftime("%A")
TIMEZONE = timezone('America/New_York')

global ITERATIONS

#Default number of iterations
ITERATIONS = 50


#################################################################
####### Note you MAY add more datafields if you see fit #########
#################################################################

# column headers for the csv file
columns =['timestamp','tripId','route','day','timeToReachExpressStation','timeToReachDestination','decision']
fileName = 'trainRecord.csv'
fileName2 = 'trainRecordFinal.csv'

def csv_dict_writer(path, fieldnames, data):
	with open(path, 'a') as out_file:
		writer = csv.DictWriter(out_file, delimiter=',', fieldnames=fieldnames)
		# writer.writeheader()
		for row in data:
			writer.writerow(row)
		out_file.close()


def remove_duplicates():
	print "removing duplicates"
	with open(fileName,'r') as in_file, open(fileName2,'w') as out_file:
		seen = set() # set for fast O(1) amortized lookup
		for line in in_file:
			if line in seen: continue # skip duplicate

			seen.add(line)
			out_file.write(line)

def getUpdates(newRecord):
	newRecord.getTripUpdates()
	trip_timestamp = newRecord.timestamp
	time = datetime.strptime(datetime.fromtimestamp(
		int(trip_timestamp)
	).strftime('%H:%M:%S'),'%H:%M:%S').time()
	minutes = time.hour * 60 + time.minute
	tripUpdates = newRecord.tripUpdates
	alerts = newRecord.alerts
	switch = False
	print "the update length " + str(len(tripUpdates))
	seen = set()
	my_list = []
	header = ['minutes','tripId','route','DAY','expressArrivalTime','destArrivalTime','decision']
	my_list.append(dict(zip(columns, header)))
	for tripUpdate in tripUpdates:
		if tripUpdate.tripId is not None:
			tripId = tripUpdate.tripId
		else:
			continue

		if tripId not in seen:
			seen.add(tripId)
		else:
			continue

		if tripUpdate.routeId is not None:
			routeId = tripUpdate.routeId
			if routeId == '1':
				route = 'local'
				switch = True
			elif routeId == '2' or routeId == '3':
				route = 'express'
				switch = False
			else:
				continue
		else:
			continue

		if tripUpdate.startDate is not None:
			startDate = tripUpdate.startDate
		else:
			continue

		if tripUpdate.direction is not None:
			direction = tripUpdate.direction
			if direction == 'N':
				continue
		else:
			continue

		destArrivalTime = ''
		expressArrivalTime = ''
		if len(tripUpdate.futureStops) > 0:
			length = len(tripUpdate.futureStops)
			for stop in tripUpdate.futureStops:
				# futureStops += tripUpdate.futureStops[stop]
				stop_station = tripUpdate.futureStops[stop][1:5]
				# index = tripUpdate.futureStops[stop].find("arrivalTime")
				stop_timestamp = tripUpdate.futureStops[stop][25:35]
				if stop_station == '120S':
					expressArrivalTime = stop_timestamp
					switch = True
				elif stop_station == '127S':
					destArrivalTime = stop_timestamp

		if tripUpdate.vehicleData is not None:
			currentStopId = tripUpdate.vehicleData.currentStopId
			if currentStopId == '120S':
					expressArrivalTime = tripUpdate.vehicleData.timestamp
			elif currentStopId == '127S':
					destArrivalTime = tripUpdate.vehicleData.timestamp
		else:
			continue

		if destArrivalTime == '' or expressArrivalTime == '':
			continue
		if minutes % 3 == 0 or minutes %10 == 0:
			switch = False
		if route == 'express':
			switch = False
		newEntry = [minutes,tripId,route,DAY,expressArrivalTime,destArrivalTime, switch]
		my_list.append(dict(zip(columns, newEntry)))
		csv_dict_writer(fileName, columns, my_list)

	newRecord.tripUpdates = []


def main():
    # API key
	with open('./key.txt', 'rb') as keyfile:
		APIKEY = keyfile.read().rstrip('\n')
		keyfile.close()
	newRecord = mtaUpdates.mtaUpdates(APIKEY)
	ITERATIONS = 50
	while ITERATIONS > 0:
		getUpdates(newRecord)
		remove_duplicates()
		time.sleep(60)
		ITERATIONS -= 1
	myS3 = S3(fileName2)
	myS3.uploadData()

if __name__ == "__main__":
	try:
		main()
	except KeyboardInterrupt:
		print "\n"
		sys.exit(1)



	# data = ["first_name,last_name,city".split(","),
	# 		"Tyrese,Hirthe,Strackeport".split(","),
	# 		"Jules,Dicki,Lake Nickolasville".split(","),
	# 		"Dedric,Medhurst,Stiedemannberg".split(","),
	# 		"AAAA,BE,SDE".split(",")
	# 		]
	# my_list = []
	# fieldnames = columns
	# for values in data[1:]:
	# 	inner_dict = dict(zip(fieldnames, values))
	# 	my_list.append(inner_dict)
	# path = "dict_output.csv"
	# csv_dict_writer(path, fieldnames, my_list)

	### INSERT YOUR CODE HERE ###

