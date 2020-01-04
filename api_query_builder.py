import pyodbc
import json
import requests

server = '*' 
database = '*'
username = '*'
password = '*'
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

apiKeyDict = {}

# sql query to get the trading post items by material category (ore, ingots, wood, planks, etc...)
matCatsDict = dict(cursor.execute('SELECT [material_category_id], [material_category_name] FROM [gw2cat].[dbo].[material_category]'))
for key,value in matCatsDict.items():
    # print('The api query for ' + value + '(material category type=' + str(key) + ') is:')
    # sql query to get the gw2 items game id, input is the ...
    gw2IdDict = dict(cursor.execute('select gw2_id, item_name FROM [gw2cat].[dbo].[item_list] where material_category_id = ' + str(key)))
    # print(gw2IdDict)
    gw2IdList = list(gw2IdDict.keys())
    gw2IdList = str(gw2IdList).strip('[]')
    # print(gw2IdList)
    query_string = 'https://api.guildwars2.com/v2/commerce/prices?ids=' + gw2IdList.replace(' ','')
    # print(query_string)
    apiKeyDict.update({key:query_string})

apiKeyDict

URL = 'https://api.guildwars2.com/v2/commerce/prices?ids=19721'
r = requests.get(url=URL, params=None)
data = r.json()
print(data)

for key,value in apiKeyDict.items():


# to do:
# 0. loop to run all API queries
# 1. save JSON to files with a timestamp
# 2. parse JSON to SQL
# 3. enable logging