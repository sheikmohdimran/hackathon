# Solution Code for Analytics Vidhya - Big Break in Big Data Hackathon

One Paragraph of project description goes here

## Problem statement:
**Building a data pipeline:** Build a data pipeline to stream power consumption/load data using kafka, you may use any processing system of your choice to ingest the data.

**Generate real-time Alert:** Generate real-time alerts on power consumption (coming from kafka stream) when:
*	**(Alert Type 1)** Hourly consumption for a household is higher than 1 standard deviation for that hour for that household’s mean consumption historically for that hour
*	**(Alert Type 2)** Hourly consumption for a household is higher than 1 standard deviation of mean consumption across all households within that particular hour on that day

## Solution Approach:
1.	Start the Kafka consumer with code to aggregate the sum and count for each batch of RDD, based on house-household-id,date and hour combination
2.	Read the input file line by line
3.	Impute values as “0” if there are any missing values for any given house- household id combination.
4.	Publish line to Kafka producer
5.	Consume all the entries from the file and get average value based on cumulative sum and count per house-household-id,date and hour combination and store the result in csv
6.	For alert 1, read the output file from earlier step as stream (this can be a kafka stream too), and for each input value group in format "house-household-id"_"hour",date and value
7.	Send this value via flatMapGroupsWithState to compute mean and SD. Store the state and values temporarily with "house-household-id"_"hour" combination. This will be used to append future value of this group to calculate running average and SD
8.	Check the incoming entry against any existing SD/Avg and output the alert to be store in memory as a table
9.	Read the output table to the submission file format and write the file to the solution folder.
10.	Repeat steps 6,7,8 and 9 with the grouping as "date"_"hour", house-household-id, value combination to get the output for alert 2

## Data Flow
![alt text](https://github.com/sheikmohdimran/hackathon/blob/master/sapient-big-data-break/image.png?raw=true "Data Flow")

## Leader Board
![alt text](https://github.com/sheikmohdimran/hackathon/blob/master/sapient-big-data-break/leaderboard.png?raw=true "Leader Board")
