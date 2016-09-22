## This program is used to clean out the data from the csv that you collected.
## It aims at removing duplicate entries and extracting any further insights 
## that the author(s) of the code may see fit

## Usage (for file as is currently): python buildTrainingDataSet.py <filename of file from part 1>
  
import sys

# Pandas is a python library used for data analysis
import pandas
from pandas import read_csv
from pytz import timezone
from datetime import datetime


TIMEZONE = timezone('America/New_York')


def main(fileHandle):
	# This creates a dataframe
	rawData = read_csv(fileHandle)

	# Remove duplicate entries based on tripId, retain the one with maximum timestamp
	data  =rawData.groupby('tripId').apply(lambda x: x.ix[x.timestamp.idxmax()])

	# Seperate all the local trains and form a new data frame
	localTrains = data[data.route == 'L']

	# Express trains
	expressTrains = data[data.route == 'E']

	# 1. Find the time difference (to reach 96th) between all combinations of local trains and express
	# 2. Consider only positive delta
	# 3. Make the final table

	# Create a new data frame for final table
	finalData = pandas.DataFrame()


	############## INSERT YOUR CODE HERE ###############
	
	finalData.to_csv("finalData.csv",index=False)



if __name__ == "__main__":

	lengthArg = len(sys.argv)


	if lengthArg < 2:
		print "Missing arguments"
		sys.exit(-1)

	if lengthArg > 2:
		print "Extra arguments"
		sys.exit(-1)
	
	fileHandle = sys.argv[1]
	main(fileHandle)
