"""
ACRIS

Retrieve and format data from the Department of Finance in ENY,
through the Socrata OpenDataNY system. For inclusion into
Gentrification Early Warning System.

TAKES: 'streets.txt', list of streets in ENY
       'BBLs.csv', ordered list of BBLs
OUTPUT: 'ACRIS.csv', table of ACRIS documents filed in last 
        6 months, sorted by BBL index
  42t3
THERE MAY BE MANY DOCUMENTS NOT CAPTURED. Requesting by streets
    is possibly an issue.

Murphy Austin
734.660.7299
murphy.c.austin@gmail.com

March 2017
"""

import requests
import pandas
import json
import datetime
import exceptions
import pdb
import numpy as np
from helpers import *
import dask.dataframe as dd

# source URLs:
rpmasterURL="https://data.cityofnewyork.us/resource/bnx9-e6tj.json"
rppartiesURL="https://data.cityofnewyork.us/resource/636b-3b5g.json"
rplegalsURL="https://data.cityofnewyork.us/resource/8h5j-fqxa.json"

streetsfile="streets.txt"

rPath = "BBLs.csv"
ACRISfile="ACRIS_-_Real_Property_Master.csv"

# Name of output file
wPath = "ACRIS_EWS.csv"

def main():

    printStart("ACRIS")
    
    """
    BBList=readFile(rPath)
    
    master=getMaster()
    
    legals = getLegals()
    
    legals=pandas.DataFrame({'document_id':legals['document_id'],'BBL':legals['BBL']}) 
                                                                     # drop all extra cols from
                                                                     # legals
    legals=dropSort(legals,'BBL',BBList)
    
    
    # join master to legals
    #master = master.set_index("document_id")
    
    legals=legals.merge(master,left_on='document_id',right_on='DOCUMENT ID',how='inner')
    
    legals['DOC. DATE']=parseDOBDates(legals['DOC. DATE'])
    

    #legals=legals.dropna(subset=['DOC. DATE'])
    legals['new_index']=legals.index
    #legals=legals.sort_values(['new_index','DOC. DATE'],ascending=[True,False])
    
    legals=legals.set_index('new_index')

    legals.to_csv(wPath)
    """
    
    printEnd("ACRIS")

    return 0






"""Retrieve all Real Property Legals entries from Brooklyn and matching ENY
    street names. Creates and populates a BBL column. Return in dataframe.
"""
def getLegals():

    
    
    #legalFrame = pandas.read_csv('ACRIS_-_Real_Property_Legals.csv', sep=',')
    
    resp = []
    
    streets=readStreets()
    resp=[]

    for i in streets:  #currently fetching by street name - is this reliable??

        parameters = {'street_name':i,'borough':3,'$limit':50000}



        try:
            response = requests.get(rplegalsURL,params=parameters)
                                                               
        ### Error Check ###  
        except:     
            print "Connection Error"
            return -1

        if response.status_code != 200:
            print "Error querying API."
            return -1

        resp.append(response.content)
    
    ################################
    

    legalFrame = JSONtoDataFrame(resp)

    # make a BBL column in legalFrame
    BBLs=pandas.Series(-1,index=legalFrame.index)

    for i in legalFrame.index:
        temp=legalFrame.iloc[i]
        
        BBL = str(temp['borough'])
        curr=str(temp['block'])
        buff='0'*(5-len(curr))
        BBL = BBL+buff+curr
        curr=str(temp['lot'])[-4:]
        buff='0'*(4-len(curr))
        BBL=BBL+buff+curr

        try:
            BBL=int(BBL)  # BBLs are INT type
        except:           # catch errors?
            pass
        BBLs[i]=BBL

    legalFrame['BBL']=BBLs
    
    return legalFrame


"""Retrieves all entries from Real Property Master from Brooklyn and recorded
    in the last 6 months. Returns in dataframe.
"""
def getMaster():

    master = pandas.read_csv(ACRISfile, encoding='utf-8')
    
    return master
    
    
    """
    IDlist=legalIDs.values
    pdb.set_trace()
    
    legalIDs = pandas.read_csv('legalIDs.csv')
        
    inf = open('ACRIS_-_Real_Property_Master.csv', 'r')
    
    firstLine = inf.readline()
    headers=firstLine.strip().split(',')
    typeindex=headers.index("DOC. TYPE")
    idindex=headers.index("DOCUMENT ID")
    
    newCSV=firstLine
    for i in inf:
        line=i.strip().split(',')
        if ("MTGE" in line[typeindex]) or ("DEED" in line[typeindex]):
            docid=line[idindex].decode('utf-8')
            if docid in IDlist:
                newCSV=newCSV+i
    
    #read newCSV into dataframe

    return masterFrame
    
    
    
    ### Calculate date of one month ago ###
    #now = datetime.datetime.today()
    #months=datetime.timedelta(days=183)
    #t1=now-months


    ### Request data ###
    # format time for query
    #t2 = str(t1.year)+"-"+str(t1.month)+"-"+str(t1.day)+"T00:00:00" 

    pdb.set_trace()
    parameters = [{'recorded_borough':3,'$limit':50000,'doc_type':'DEED'},
                   {'recorded_borough':3,'$limit':50000,'doc_type':'MTGE'}] 


    resp=[]
    for i in parameters:
        try:
            response = requests.get(rpmasterURL, params=i)  
                                                               
        ### Error Check ###  
        except:       
            print "Connection Error"
            return -1

        if response.status_code != 200:
            print "Error querying API."
            print response.content
        ################################
    
        resp.append(response.content)

    masterFrame = JSONtoDataFrame(resp)
    
    masterFrame.to_csv('masterACRIS.csv',encoding='utf-8')
    
    return masterFrame
    """
    
"""Input: Series of document IDs
   Output: Df object containing RP Parties entries for each doc id"""
def getParties(ids):

    resp = []
    for i in ids:

        parameters = {'document_id':i}

        try:
            response = requests.get(rppartiesURL,params=parameters)
                                                               
        ### Error Check ###  
        except:     
            print "Connection Error"
            return -1

        if response.status_code != 200:
            print "Error querying API."
            return -1

        resp.append(response.content)
    
    ################################
    

    partiesFrame = JSONtoDataFrame(resp)

    return partiesFrame


def readStreets():

    inf = open(streetsfile,"r")

    streets = []
    for line in inf:
        line=line.strip()
        streets.append(line)

    inf.close()

    return streets


main()


