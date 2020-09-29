
#before using FindDuplicates(), using an updated asset dump, I made a column cleaned_zip for only the first 5 numbers of all the zip codes
#and then sorted the assets by ascending cleaned_zip, so that I could break up the asset dump to be deduplicated by zip code 

import pandas as pd


a=pd.read_csv("new_asset_dump_sept28.csv")

a['cleaned_zip']=''

def get_zip(zip_code):
    zip=zip_code
    if not isNaN(zip_code):
        zip = zip_code
        zip = str(zip)
        z = zip.split('-')
        zip = z[0]
    return zip
    
    
def isNaN(string):
    return string != string


for index, row in a.iterrows():
    zip_code = row.zip_code
    b = get_zip(zip_code)
    a.set_value(index, 'cleaned_zip', b)
    
    

a=a.sort_values('cleaned_zip', ascending=True)


b=a[0:417]

b.to_csv("assets_part1_unprocessed.csv")


#the reason for passing both the cleaned zip code file and the original asset dump
#is that when I first made the code to deduplicate raw assets, the second csv file parameter was 
#a csv file of the finalized assets
#its from these finalized assets that I got the location_id and finalized address info for the raw assets 
#now that I can pull a new finalized asset dump from the url http://assets.wprdc.org/asset_dump.csv, 
#instead of changing the code to accept only one csv file, I just pass the csv of assets I want to deduplicate and the full finalized asset dump

c=FindDuplicates("assets_part1_unprocessed.csv","new_asset_dump_sept28.csv")

#--

c.writeToFile()

#to get the next chunk of assets, I would to d=a[418:n], and then export that as a csv file 
#and pass it in as the first csv file in FindDuplicate() parameters

