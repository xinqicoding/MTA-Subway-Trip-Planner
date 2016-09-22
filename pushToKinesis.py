# This program sends the data to kinesis. You do not need to modify this code except the Kinesis stream name.
# Usage python pushToKinesis.py <file name>
# a lambda function will be triggered as a result, that will send it to AWS ML for classification
# Usage python pushToKinesis.py <csv file name with extension>

import sys,csv,json
from pytz import timezone
from datetime import datetime
import boto3

sys.path.append('../utils')
import aws
import mtaUpdates


KINESIS_STREAM_NAME = 'mtaStream'
DAY = datetime.today().strftime("%A")

def main():
    with open('./key.txt', 'rb') as keyfile:
        APIKEY = keyfile.read().rstrip('\n')
        keyfile.close()
    newRecord = mtaUpdates.mtaUpdates(APIKEY)
    station = prompt()
    data = getUpdates(newRecord,station)
    if len(data) < 1:
        print "Sorry, there is no available prediction at this time"
        return
    # connect to kinesis
    kinesis = aws.getClient('kinesis','us-east-1')
    # with open('trainRecordFinal.csv','rb') as f:
    #     dataReader = csv.DictReader(f)
    #     for row in dataReader:
    #         print row
    response = kinesis.describe_stream(
        StreamName=KINESIS_STREAM_NAME,
    )
    print data
    kinesis.put_record(StreamName=KINESIS_STREAM_NAME, Data=json.dumps(data), PartitionKey='0')
    #         break
        # f.close() 
def getUpdates(newRecord, myStation):
    newRecord.getTripUpdates()
    trip_timestamp = newRecord.timestamp
    time = datetime.strptime(datetime.fromtimestamp(
        int(trip_timestamp)
    ).strftime('%H:%M:%S'),'%H:%M:%S').time()
    minutes = time.hour * 60 + time.minute
    tripUpdates = newRecord.tripUpdates
    alerts = newRecord.alerts
    print "the update length " + str(len(tripUpdates))
    seen = set()
    entry = []
    earlistArrival = -1
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
            elif routeId == '2' or routeId == '3':
                route = 'express'
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

        myArrivalTime = ''
        destArrivalTime = ''
        expressArrivalTime = ''
        if len(tripUpdate.futureStops) > 0:
            length = len(tripUpdate.futureStops)
            for stop in tripUpdate.futureStops:
                # futureStops += tripUpdate.futureStops[stop]
                stop_station = tripUpdate.futureStops[stop][1:5]
                # index = tripUpdate.futureStops[stop].find("arrivalTime")
                stop_timestamp = tripUpdate.futureStops[stop][25:35]
                if stop_station == myStation+'S':
                    myArrivalTime = stop_timestamp
                if stop_station == '120S':
                    expressArrivalTime = stop_timestamp
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

        if myArrivalTime == '' or destArrivalTime == '' or expressArrivalTime == '':
            continue
        if (earlistArrival == -1):
            # entry = [minutes,tripId,route,DAY,expressArrivalTime]
            entry = [str(minutes),tripId,route,DAY,expressArrivalTime,destArrivalTime]
            earlistArrival = minutes
        elif(earlistArrival < minutes):
            entry = [str(minutes),tripId,route,DAY,expressArrivalTime,destArrivalTime]
            earlistArrival = minutes
    return entry

def prompt():
    print "Enter your current station number"
    station = raw_input()
    if(int(station) > 120):
        print "Please take the local train, information not provided yet"
        sys.exit(1)

    return station


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print "\n"
        sys.exit(1)
