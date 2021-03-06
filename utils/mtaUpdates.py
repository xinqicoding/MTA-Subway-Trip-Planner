import urllib2,contextlib
from datetime import datetime
from collections import OrderedDict

from pytz import timezone
import gtfs_realtime_pb2
import google.protobuf

import vehicle,alert,tripupdate

class mtaUpdates(object):

    # Do not change Timezone
    TIMEZONE = timezone('America/New_York')
    
    # feed url depends on the routes to which you want updates
    # here we are using feed 1 , which has lines 1,2,3,4,5,6,S
    # While initializing we can read the API Key and add it to the url
    feedurl = 'http://datamine.mta.info/mta_esi.php?feed_id=1&key='
    
    VCS = {1:"INCOMING_AT", 2:"STOPPED_AT", 3:"IN_TRANSIT_TO"}    
    tripUpdates = []
    alerts = []
    vehicles = []
    timestamp = None
    def __init__(self,apikey):
        self.feedurl = self.feedurl + apikey

    def returnVehicle(self,tripID):
        for vehicle in self.vehicles:
            if vehicle.trip_id == tripID:
                return vehicle
        return None
    
    def returnTrip(self,tripID):
        for trip in self.tripUpdates:
            if trip.tripId == tripID:
                return trip
        return None

    def returnAlert(self,tripID):
        for alert in self.alerts:
            if alert.tripId == tripID:
                return trip
        return None

    # Method to get trip updates from mta real time feed
    def getTripUpdates(self):
        feed = gtfs_realtime_pb2.FeedMessage()
        try:
            with contextlib.closing(urllib2.urlopen(self.feedurl)) as response:
                d = feed.ParseFromString(response.read())
        except (urllib2.URLError, google.protobuf.message.DecodeError) as e:
            print "Error while connecting to mta server " +str(e)
	

	self.timestamp = feed.header.timestamp
        nytime = datetime.fromtimestamp(self.timestamp,self.TIMEZONE)
	
	for entity in feed.entity:
	    # Trip update represents a change in timetable
	    if entity.trip_update and entity.trip_update.trip.trip_id:
		update = tripupdate.tripupdate()
		##### INSERT TRIPUPDATE CODE HERE ####			
                update.tripId = str(entity.trip_update.trip.trip_id)
                update.routeId = str(entity.trip_update.trip.route_id)
                update.startDate = entity.trip_update.trip.start_date
                update.direction = str(entity.trip_update.trip.trip_id[10])
                curr_vehicle = self.returnVehicle(update.tripId)
                num = 0
                for i in entity.trip_update.stop_time_update:
                    update.futureStops[num] = "\"" + str(i.stop_id) + "\": [{\"arrivalTime\": " + str(i.arrival.time) + "}, {\"departureTime\": " + str(i.departure.time) + "}]"
#                    print update.futureStops[num]
                    num+=1
                if curr_vehicle is not None:
                    update.vehicleData = curr_vehicle
                self.tripUpdates.append(update)
	    if entity.vehicle and entity.vehicle.trip.trip_id:
	    	v = vehicle.vehicle()
		##### INSERT VEHICLE CODE HERE #####
                v.currentStopNumber = entity.vehicle.current_stop_sequence
                v.currentStopId = str(entity.vehicle.stop_id)
                v.timestamp = entity.vehicle.timestamp
                v.currentStopStatus = entity.vehicle.current_status
                v.trip_id = str(entity.vehicle.trip.trip_id)
                v.start_date = entity.vehicle.trip.start_date
                v.route_id = entity.vehicle.trip.route_id
                self.vehicles.append(v)
                cor_trip = self.returnTrip(v.trip_id)
                cor_alert = self.returnAlert(v.trip_id)
                if cor_trip is not None:
                    cor_trip.vehicleData = v

	    if entity.alert:
                a = alert.alert()
		#### INSERT ALERT CODE HERE #####
                a.alertMessage = entity.alert.header_text.translation 
                self.alerts.append(a)

	return self.tripUpdates
    


    # END OF getTripUpdates method
