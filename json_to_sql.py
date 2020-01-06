import pyodbc
import json
import requests
import datetime
from pathlib import Path

## JSON READ FILE OPERATIONS

# initialize a new empty dictionary to hold the file names
json_files = []
# create the absolute directory path
directory = Path('C:\\gw2ct\\json_get')
# loop through the folder and place the file names into the file list
for currentFile in directory.iterdir():
    json_files.append(currentFile.name)
    #print(currentFile)

# get the number of files
print('The number of files to be parsed is: ' + str(len(json_files)))

# print the names of the files
print(json_files)

# open each file and read it into memory the insert it into SQL
for file in json_files:
    print(file)


with open(data_folder/'run_2020-01-06 20.36.18_Ingots_prices.json', 'r') as jsonfile:
    data = json.load(jsonfile)
    print(data)
