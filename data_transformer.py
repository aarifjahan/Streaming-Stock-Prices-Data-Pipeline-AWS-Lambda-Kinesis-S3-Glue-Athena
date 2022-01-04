# --------------------------------------------------------------------------------------------------------------
# | Developer: Aarif Munwar Jahan                                                                              |
# | Date: 12/17/21                                                                                             |
# | Commit Version: 0.0.1                                                                                      |
# | Code Function: Access stock dataset using yfinance module and push acquired data into a AWS kinesis stream |
# | For CIS 9760 - Big Data Technologies Class Project 3 with Professor Mottaqui Karim - Fall 2021             |
# --------------------------------------------------------------------------------------------------------------

# Import necessary packages
import yfinance as yf
import json
import boto3

# List all desired stock tikcers for acquisition
companies = ['FB', 'SHOP', 'BYND', 'NFLX', 'PINS', 'SQ', 'TTD', 'OKTA', 'SNAP', 'DDOG']

# Define lambda function
def lambda_handler(event, context):

  # Use AWS boto3 client to define kinesis stream and region
  kinesis = boto3.client('kinesis', "us-east-2")

  # Loop over each company's stock ticker
  for company in companies:

    # Get data from yfinance API using the ticker name and define the target date and interval
    data = yf.download(company,
                      start="2021-11-30",
                      end="2021-12-01",
                      interval = "5m")

    # Loop over each datetime and value in data row
    for datetime, stock_value in data.iterrows():

      # Define an empty dictionary and import the four parameters from yfinance into the dictionary
      data_out = {}
      data_out['name'] = company
      data_out['ts'] = str(datetime)
      data_out['high'] = round(stock_value['High'], 2)
      data_out['low'] = round(stock_value['Low'], 2)

      # Dump data into json format, add a line at the end to seperate each record
      json_out = json.dumps(data_out) + "\n"

      # Send data to kinesis firehose one json line at a time
      kinesis.put_record(
                StreamName="STA9760F2021_stream1",
                Data=json_out,
                PartitionKey="partitionkey"
                )

  # Define return statement for acknowldegement
  return {
    'statusCode': 200,
    'body': json.dumps(f'Stock Data Dump Complete')
  }
