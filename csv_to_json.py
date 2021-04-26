import csv
import json
 
 
# Function to convert a CSV to JSON
# Takes the file paths as arguments
def make_json(csvFilePath, jsonFilePath):
    data = {"tweets" : []}
   
    with open(csvFilePath, encoding='utf-8') as csvf:
        csvReader = csv.DictReader(csvf)
         
      
        for rows in csvReader: 
            
            data["tweets"].append(rows)
 
 
    with open(jsonFilePath, 'w', encoding='utf-8') as jsonf:
        jsonf.write(json.dumps(data, indent=4))
         

csvFilePath = r'trumptweets.csv'
jsonFilePath = r'trumptweets.json'
 

make_json(csvFilePath, jsonFilePath)