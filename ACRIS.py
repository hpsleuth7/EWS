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

# source URLs:
rpmasterURL="https://api.nycdb.info/real_property_master?"
rppartiesURL="https://data.cityofnewyork.us/resource/636b-3b5g.json"
rplegalsURL="https://api.nycdb.info/real_property_legals?"

streetsfile="streets.txt"

rPath = "Inputs/BBLs.csv"
ACRISfile="ACRIS_-_Real_Property_Master.csv"

# Name of output file
wPath = "raw_data/ACRIS_EWS.csv"

# doctype codes from real_property_master that indicate deed conveyance
#  HOW TO DEAL WITH COMMAS IN FIELD
types = 'DEED,DEEDO,"DEED, LE","DEED, TS","DEED, RC",ASTU,CNTR,MCON,LTPA,CONDEED,DEEDP,ACON,TORREN,IDED,TLS,MTGE'


def main():

    printStart("ACRIS")
    
    
    BBList=readFile(rPath)
    legals = getLegals(BBList)
    
    legals=legals[['documentid','bbl']] #keep only needed columns
    docids = legals['documentid'].tolist()
    master=getMaster(docids)
    master=master.merge(legals, on='documentid',how='inner')
    lucyIndex = pandas.DataFrame({'bbl':BBList,'new_index':range(len(BBList))}) # make dataframe of just ordered index, bbl from lucy's list
     
    final=lucyIndex.merge(master,on='bbl',how='right') #make sure this preserves lucy's index
    final=final.sort_values(['new_index','docdate'],ascending=[True,False])
    final=final.set_index('new_index')
    
    final.to_csv(wPath)
    
    
    
    
    
    
    """
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
def getLegals(BBLs):

    
    
    #legalFrame = pandas.read_csv('ACRIS_-_Real_Property_Legals.csv', sep=',')
    
    resp = []
    
    block=len(BBLs)/50       

    for i in range(block):
        list = ",".join(BBLs[i*50:(i+1)*50])   
        try:
            response=requests.get(rplegalsURL+"bbl=in."+list)       
        ### Error Check ###  
        except:     
            print "Connection Error in Legals"
            return -1
        if response.status_code != 200:
            print "Error querying API."
            return -1

        resp.append(response.content)
        
        
    if len(BBLs)%50 != 0:
        list=",".join(BBLs[-(len(BBLs)%50):])
        try:
            response=requests.get(rplegalsURL+"bbl=in."+list)       
        ### Error Check ###  
        except:     
            print "Connection Error"
            return -1
        if response.status_code != 200:
            print "Error querying API."
            return -1
        
        resp.append(response.content)

    legalFrame = JSONtoDataFrame(resp)

    return legalFrame

"""Retrieves all entries from Real Property Master from Brooklyn and recorded
    in the last 6 months. Returns in dataframe.
"""
def getMaster(docids):

    
    # do the block requests with list joins - should be nearly 1-1; 
    # restricted by doctype
    
    resp = []
    
    block=len(docids)/100       #figure out how to do this
    
    
    for i in range(block):
        list = ",".join(docids[i*100:(i+1)*100])   
        try:
            response=requests.get(rpmasterURL+"documentid=in."+list+"&"+\
            "doctype=in."+types)       
        ### Error Check ###  
        except:     
            print "Connection Error in Master"
            return -1
        if response.status_code != 200:
            print "Error querying API."
            return -1

        resp.append(response.content)
        
    if len(docids)%100 != 0:
        list=",".join(docids[-(len(docids)%100):])
        try:
            response=requests.get(rpmasterURL+"documentid=in."+list+"&"+
                "doctype=in."+types)       
        ### Error Check ###  
        except:     
            print "Connection Error"
            return -1
        if response.status_code != 200:
            print "Error querying API."
            return -1
        
        resp.append(response.content)

    masterFrame = JSONtoDataFrame(resp)
    masterFrame.to_csv('RPMaster.csv',encoding='utf-8')
    
    return masterFrame
    
    
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
            print "Connection Error in Parties"
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


