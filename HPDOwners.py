"""
HPD Owners

Murphy Austin
murphy.c.austin@gmail.com
September 2017
"""

import requests
import pandas
import json
import datetime
import exceptions
import pdb
import numpy as np
from helpers import *

hpdregURL="https://api.nycdb.info/hpd_registrations?"
hpdconURL="https://api.nycdb.info/hpd_contacts?"

rPath = "Inputs/BBLs.csv"

# Name of output file
wPath = "raw_data/HPD_Owners_EWS.csv"

#columns for final dataframe
cols=['registrationcontacttype','corporationname','lastname','firstname','businesshousenumber',
    'businessstreetname','businessapartment','businesscity','businessstate','businesszip']


def main():

    printStart("HPD Owners")
    BBList=readFile(rPath)
    
    regFrame=getReg(BBList)
    

    # check if BBL <--> registrationid is one-to-one -- looks like it is!
    
    regFrame=regFrame[['bbl','registrationid']]
    regids = regFrame['registrationid']
    conFrame=getContacts(regids)
    master=pandas.merge(regFrame,conFrame,on='registrationid',how='inner')
    master=dropSort(master,'bbl',BBList)
    master=master[cols]
    
    master.to_csv(wPath,encoding='utf-8')
    
    




# drop all columns except BBL and registrationid

# check if exactly one registrationid per BBL

#batch request from hpd_contacts where registrationid is in the ids 

# merge frames on registrationid

#dropsort

# order columns

def getReg(BBLs):

# batch request from hpd_registrations where BBL is in BBLs
    resp = []
    
    block=len(BBLs)/50       #figure out how to do this
    
    for i in range(block):
        list = ",".join(BBLs[i*50:(i+1)*50])   
        try:
            response=requests.get(hpdregURL+"bbl=in."+list)       
        ### Error Check ###  
        except:     
            print "Connection Error"
            return -1
        if response.status_code != 200:
            print "Error querying API."
            return -1

        resp.append(response.content)
        
        
    if len(BBLs)%50 != 0:
        list=",".join(BBLs[-(len(BBLs)%50):])
        try:
            response=requests.get(hpdregURL+"bbl=in."+list)       
        ### Error Check ###  
        except:     
            print "Connection Error"
            return -1
        if response.status_code != 200:
            print "Error querying API."
            return -1
        
        resp.append(response.content)

    regFrame = JSONtoDataFrame(resp)

    return regFrame
    
    
def getContacts(ids):


    ids=ids.tolist()
    for i in range(len(ids)):
        ids[i]=str(ids[i])
        
    resp = []
    block=len(ids)/50       #figure out how to do this
    
    for i in range(block):
        list = ",".join(ids[i*50:(i+1)*50])   
        try:
            response=requests.get(hpdconURL+"registrationid=in."+list)       
        ### Error Check ###  
        except:     
            print "Connection Error"
            return -1
        if response.status_code != 200:
            print "Error querying API."
            return -1

        resp.append(response.content)
    

    if len(ids)%50 != 0:
        list=",".join(ids[-(len(ids)%50):])
        try:
            response=requests.get(hpdconURL+"registrationid=in."+list)       
        ### Error Check ###  
        except:     
            print "Connection Error"
            return -1
        if response.status_code != 200:
            print "Error querying API."
            return -1
        
        resp.append(response.content)

    conFrame = JSONtoDataFrame(resp)

    return conFrame


main()
