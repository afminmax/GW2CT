print("Starting module imports....")

import pyodbc
import json
import requests
import datetime, time
from pathlib import Path


print("Finished loading module imports....")

print("Logging into SQL servver....")
server = '*'
database = '*'
username = '*'
password = '*'
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()
print("SQL servver logged in....")

print("Creating the API Queries...")
apiKeyDict = {}

## CREATE THE API QUERIES. OUTPUT GOES TO A DICTIONARY.
# sql query to get the trading post items by material category (ore, ingots, wood, planks, etc...)
matCatsDict = dict(cursor.execute('SELECT [material_category_id], [material_category_name] FROM [gw2cat].[dbo].[material_category]'))
for key,value in matCatsDict.items():
    # print('The api query for ' + value + '(material category type=' + str(key) + ') is:')
    # sql query to get the gw2 items game id and full name, results stored as dictionary
    gw2IdDict = dict(cursor.execute('select gw2_id, item_name FROM [gw2cat].[dbo].[item_list] where material_category_id = ' + str(key)))
    # print(gw2IdDict)
    gw2IdList = list(gw2IdDict.keys())
    gw2IdList = str(gw2IdList).strip('[]')
    # print(gw2IdList)
    query_string = 'https://api.guildwars2.com/v2/commerce/prices?ids=' + gw2IdList.replace(' ','')
    # print(query_string)
    apiKeyDict.update({key:query_string})

print("Completed the API Queries Dictionary:")
# print(apiKeyDict)



# SUBMIT API REQUESTS
print("Submitting the API Queries to the GW2 API....")

# get sql iso8601 offset timestamp 
sql_dts = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
print(sql_dts)
sql_dts_dict = {'dts':sql_dts}

# kluge. redo this later. a second easier to format dts for the file run name
filename_dts = datetime.datetime.utcnow()
print(filename_dts)

# build friendly filename for saved json
run_name = 'run_' + filename_dts.strftime("%Y-%m-%d %H.%M.%S") + '_'
print("The run name is: " + run_name)
data_folder = Path("c:\gw2ct\json_get")

for key,value in apiKeyDict.items():
    URL = value
    # print(URL)
    r = requests.get(url=URL, params=None)
    data = r.json()
    # print(data)
    data.append(sql_dts_dict)
    file_name = run_name + matCatsDict[key] + '_prices.json'
    with open(data_folder/file_name, 'w') as outfile:
        json.dump(data, outfile, indent=4)

print("The queries have been submitted and written to disk. Ready for SQL!")







# to do:
# 0. loop to run all API queries - DONE
# 1. save JSON to files with a timestamp - DONE
# 2. parse JSON to SQL
# 3. enable logging