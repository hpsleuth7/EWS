"""
Furman Center data pull

Joins tables from Furman's API to generate sorted list
of ENY properties with housing subsidies.

Murphy Austin
May 2017
"""

from helpers import *
import pandas
import requests
import json
import pdb
from StringIO import StringIO

propinfo="http://nyufc.carto.com/api/v2/sql?format=csv&q=SELECT+*+FROM+property_info"
subsidy="http://nyufc.carto.com/api/v2/sql?format=csv&q=SELECT+*+FROM+subsidy"
subsidyBBL="http://nyufc.carto.com/api/v2/sql?format=csv&q=SELECT+*+FROM+subsidy_bbl_link"

bblpath="Inputs/BBLs.csv"
coredata_file="raw_data/CoreData.csv"

def main():

    master = getFurman()
    
    BBLs = readFile(bblpath)
    master = dropSort(master,'bbl',BBLs)
    
    master.to_csv(coredata_file,encoding='utf-8')
    
    return
    
"""
Fetch and merge tables from Furman API
Return dataframe object """
def getFurman():

    resp = []
    
    urls=["http://nyufc.carto.com/api/v2/sql?format=csv&q=SELECT+*+FROM+property_info",
            "http://nyufc.carto.com/api/v2/sql?format=csv&q=SELECT+*+FROM+subsidy",
            "http://nyufc.carto.com/api/v2/sql?format=csv&q=SELECT+*+FROM+subsidy_bbl_link"]
    
    for i in urls:    
        try:
            response = requests.get(i)
                                                               
        ### Error Check ###  
        except:     
            print "Connection Error"
            return -1

        if response.status_code != 200:
            print "Error querying API."
            return -1

        resp.append(response.content)
    
    data = StringIO(resp[0])
    property_info=pandas.read_csv(data,sep=',')
    
    data=StringIO(resp[1])
    subsidy=pandas.read_csv(data,sep=',')
    
    data=StringIO(resp[2])
    subsidy_bbl_link=pandas.read_csv(data,sep=',')   
    
    result = pandas.merge(property_info,subsidy_bbl_link,how='left',on='bbl')
    result=pandas.merge(result,subsidy,how='left',on='fc_subsidy_id')
    
    return result
    
"""
Input: complete df of Furman subsidies
Output: df with only ENY properties, sorted by Lucy index
"""   
def transform(table):

    return table
    

 
main()