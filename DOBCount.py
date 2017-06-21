"""
DoB Data

Retrieve and format data from the Department of Buildings in ENY,
through the Socrata OpenDataNY system. For inclusion into
Gentrification Early Warning System.

TAKES: 'BBL.csv', ordered list of BBLs in same directory
       'binmap.txt', csv file where first row=BBLs, second row=BIN
       'streets.txt', text file of ENY street names
OUTPUT: 'DoBCounts.csv', table of sums of the following data for 
        each ENY property:
            - DoB Complaints since 2014
            - DoB Violations 
            - DoB ECB Violations
            - DoB Permits issued
            - DoB change in units from a permit

Murphy Austin
734.660.7299
murphy.c.austin@gmail.com

January 2017
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
dobcomURL = "https://data.cityofnewyork.us/resource/muk7-ct23.json"
dobvioURL = "https://data.cityofnewyork.us/resource/dvnq-fhaa.json"
dobecbURL = "https://data.cityofnewyork.us/resource/8vf4-ne9x.json"
dobfileURL = "https://data.cityofnewyork.us/resource/rvhx-8trz.json"
dobissueURL = "https://data.cityofnewyork.us/resource/83x8-shf7.json"

binFile = "binmap.txt"
streetsfile="streets.txt"

# static reference:
zips = [11207,11208,11212,11233]
comboard = 305

# file that contains list of BBLS. MUST BE IN SAME FOLDER AS HPDComplaints.py
rPath = "BBLs.csv"

# Name of output file
wPath = "DOB_EWS.csv"

DOB_ECB_file="DOB_ECB_opendata.csv"

def main():


    BBLs = readFile(rPath)  # read in the list of BBLs

    frames = {}

    frames['com'] = getCom()
    frames['viol'] = getViol()
    frames['ecb'] = getEcb()
    frames['file'] = getFile()
    frames['issue'] = getIssue()

    # Create a dataframe of all -1s, with columns for final table
    # may not need to add columns now - just start with one
    temp = [0] * len(BBLs)
    master = pandas.DataFrame({'diff':temp}, index=BBLs)
    

    
    # get histogram of BBLs that got complaints


    for i in frames:
        hist=frames[i]['BBL'].value_counts() 
        hist.name=i
        master=master.join(hist)


    # calculate the diff column #
    s = frames['file']['BBL']
    master = master.fillna(value=0)  #NaN --> 0
    L = []
    for i in master.index:           #list all BBLs that have DoB filing
        if master.loc[i]['file']!=0:
            L.append(i)

    keys = s[s.isin(L)]   #get Series containing BBLS w/ filings, indexed by 
                          #file dataframe
    for i in keys.index:               #calc unit difference in each file index
        row=frames['file'].loc[i]
        eunits=row.loc['existing_dwelling_units']
        punits=row.loc['proposed_dwelling_units']
        try:
            eunits=float(eunits)
            punits=float(punits)
        except ValueError:
            eunits=0
            punits=0
        diff=punits-eunits
        master.loc[keys.loc[i],'diff']+=diff        #update field in master df

    
    master=master.fillna(value=0)
    master.to_csv(wPath,encoding='utf-8')
 

    # TO DO - figure out an error log #

    return 0

"""Retrieve all DoB Complaints from ENY Community Board. Calculate and add
BBL for each entry. Return dataframe."""
def getCom():

    resp = []

    parameters = {'community_board':comboard,'$limit':50000}

    try:
        response = requests.get(dobcomURL,params=parameters)  #TRY - only get columns I want?
                                                               
    ### Error Check ###  
    except SSLError:       
        print "Connection Error"
        return -1

    if response.status_code != 200:
        print "Error querying API."
    ################################
    
    resp.append(response.content)

    comFrame = JSONtoDataFrame(resp)

    # remove everything before 2014 from 'date entered' #

    toDelete = []
    """
    for i in range(len(comFrame.index)):
        curr=comFrame['date_entered'][i]
        if "2014" not in curr:
            if "2015" not in curr:
                if "2016" not in curr:
                    if "2017" not in curr:
                        toDelete.append(i)
    """
    
    
    comFrame['date_entered']=parseDOBDates(comFrame['date_entered'])
    comFrame=comFrame.drop(comFrame[comFrame['date_entered']==-1].index)
    startDate=datetime.date(2014,1,1)
    comFrame=comFrame.drop(comFrame[comFrame['date_entered']<startDate].index)
    
    comFrame=comFrame.drop(comFrame.index[toDelete])
    #print comFrame['bin']




    binMap = getBin()  #binMap is Dataframe object
    toAdd = pandas.Series(-1,index=comFrame.index) #make series to hold BBLs

    for i in comFrame.index:
        BIN=comFrame.loc[i,'bin']  # BIN is unicode type???
        BIN=np.int64(BIN)
        try:
            BBL=binMap.loc[BIN].iloc[0]
        except KeyError:
            #print "WARNING: BIN not in records"
            BBL=-1
        toAdd[i]=BBL

    comFrame['BBL']=toAdd

    comFrame=comFrame.drop_duplicates('complaint_number')

    comFrame.to_excel("allComs.xlsx")

    return comFrame


"""Retrieve all DoB Violations from ENY Community Board. Calculate and add
BBL for each entry. Return dataframe."""
def getViol():
    
    
    streets=readStreets()
    resp=[]

    for i in streets:

        parameters = {'street':i,'boro':"3",'$limit':10000}

        try:
            response = requests.get(dobvioURL,params=parameters)
                                                               
        ### Error Check ###  
        except SSLError:       
            print "Connection Error"
            return -1

        if response.status_code != 200:
            print "Error querying API."
        ################################

        resp.append(response.content)

    violFrame = JSONtoDataFrame(resp)

    BBLs=pandas.Series(-1,index=violFrame.index)
    
    # populate BBLs Series with BBL for each entry
    for i in violFrame.index:
        temp=violFrame.iloc[i]
        
        BBL = str(temp['boro'])
        curr=str(temp['block'])
        buff='0'*(5-len(curr))
        BBL = BBL+buff+curr
        curr=str(temp['lot'])[-4:]
        buff='0'*(4-len(curr))
        BBL=BBL+buff+curr
         
        try:                  #convert to int64
            BBL=np.int64(BBL)
        except:
            pass

        BBLs[i]=BBL   #some BBLs are wrong!! I could remove from '.' to end

    
    
    violFrame['BBL']=BBLs
    
    # fix date column and drop all before 2014
    violFrame['issue_date']=parseDOBDates(violFrame['issue_date'])
    violFrame=violFrame.drop(violFrame[violFrame['issue_date']==-1].index)
    startDate=datetime.date(2014,1,1)
    violFrame=violFrame.drop(violFrame[violFrame['issue_date']<startDate].index)

    return violFrame

"""Retrieve all DoB ECB Violations from ENY Community Board. Calculate and add
BBL for each entry. Return dataframe."""
def getEcb():
 
    ecbFrame=pandas.read_csv(DOB_ECB_file,encoding='utf-8', low_memory=False,dtype={'ISSUE_DATE':object})
    ecbFrame=ecbFrame.drop(ecbFrame[ecbFrame['BORO']!=3].index) #drop all non-Brooklyn
    
    """
    resp=[]

    for i in zips:

        parameters = {'respondent_zip':i,'$limit':50000}

        try:
            response = requests.get(dobecbURL,params=parameters)
                                                               
        ### Error Check ###  
        except SSLError:       
            print "Connection Error"
            return -1

        if response.status_code != 200:
            print "Error querying API."
            print response.content
            return
        ################################

        resp.append(response.content)

    ecbFrame = JSONtoDataFrame(resp)
    """
    
    
    
    BBLs=pandas.Series(-1,index=ecbFrame.index)
  
    # populate BBLs Series with BBL for each entry
    for i in range(len(ecbFrame.index)):
        temp=ecbFrame.iloc[i]
        
        BBL = str(temp['BORO'])
        curr=str(temp['BLOCK'])
        buff='0'*(5-len(curr))
        BBL = BBL+buff+curr
        curr=str(temp['LOT'])[-4:]
        buff='0'*(4-len(curr))
        BBL=BBL+buff+curr
         

        BBLs.iloc[i]=BBL    #same BBL problem

    ecbFrame['BBL']=BBLs

    # fix date column and drop all before 2014
    pdb.set_trace()
    ecbFrame['ISSUE_DATE']=parseDOBDates(ecbFrame['ISSUE_DATE'])
    startDate=datetime.date(2014,1,1)
    ecbFrame=ecbFrame.drop(ecbFrame[ecbFrame['ISSUE_DATE']<startDate].index)
    ecbFrame.to_csv("droppedECB.csv",encoding='utf-8')

    return ecbFrame

"""Retrieve all DoB ECB Jobs filed records from ENY Community Board. Calculate and add
BBL for each entry. Return dataframe."""
def getFile():
    resp=[]

    parameters = {'community___board':comboard,'$limit':50000}

    try:   # SELECT COMMAND NOT WORKING
        response = requests.get(dobfileURL,params=parameters,data=
                "$select=job__, borough, block, lot, existing_dwelling_units, proposed_dwelling_units")
                                                               
    ### Error Check ###  
    except SSLError:       
        print "Connection Error"
        return -1

    if response.status_code != 200:
        print "Error querying API."
        print response.content
        return
    ################################

    resp.append(response.content)

    fileFrame = JSONtoDataFrame(resp)
    BBLs=pandas.Series(-1,index=fileFrame.index)
  
    # populate BBLs Series with BBL for each entry
    for i in fileFrame.index:
        temp=fileFrame.iloc[i]
        BBL = "3"                #data source is annoying
        curr=str(temp['block'])
        buff='0'*(5-len(curr))
        BBL = BBL+buff+curr
        curr=str(temp['lot'])[-4:]
        buff='0'*(4-len(curr))
        BBL=BBL+buff+curr
         
        BBLs[i]=BBL    #same BBL problem

    fileFrame['BBL']=BBLs

    fileFrame=fileFrame.drop_duplicates(subset='job__') #remove duplicate filings
    
    # fix date column and drop all before 2014
    fileFrame['latest_action_date']=parseDOBDates(fileFrame['latest_action_date'])
    fileFrame=fileFrame.drop(fileFrame[fileFrame['latest_action_date']==-1].index)
    startDate=datetime.date(2014,1,1)
    fileFrame=fileFrame.drop(fileFrame[fileFrame['latest_action_date']<startDate].index)
    fileFrame.to_csv("droppedFile.csv",encoding='utf-8')

    return fileFrame

"""Retrieve all DoB Permit Issuance from ENY Community Board. Calculate and add
BBL for each entry. Return dataframe."""
def getIssue():
    
    resp=[]

    parameters = {'community_board':comboard,'$limit':50000}

    try:
        response = requests.get(dobissueURL,params=parameters)
                                                               
    ### Error Check ###  
    except SSLError:       
        print "Connection Error"
        return -1

    if response.status_code != 200:
        print "Error querying API."
        print response.content
        return
    ################################

    resp.append(response.content)

    issueFrame = JSONtoDataFrame(resp)
    BBLs=pandas.Series(-1,index=issueFrame.index)


    # populate BBLs Series with BBL for each entry
    for i in issueFrame.index:
        temp=issueFrame.iloc[i]
        
        BBL = "3"                #data source is annoying
        curr=str(temp['block'])
        buff='0'*(5-len(curr))
        BBL = BBL+buff+curr
        curr=str(temp['lot'])[-4:]
        buff='0'*(4-len(curr))
        BBL=BBL+buff+curr
         
        BBLs[i]=BBL   


    issueFrame['BBL']=BBLs
    
    # fix date column and drop all before 2014
    issueFrame['issuance_date']=parseSODADates(issueFrame['issuance_date'])
    issueFrame=issueFrame.drop(issueFrame[issueFrame['issuance_date']==-1].index)
    startDate=datetime.date(2014,1,1)
    issueFrame=issueFrame.drop(issueFrame[issueFrame['issuance_date']<startDate].index)
    issueFrame.to_csv('droppedIssue.csv',encoding='utf-8')

    return issueFrame

""" Pass in a filepath string to a list of ENY BBLs. Read in the BBLs
and return in a list """
def readFile(filepath):

    curr=pandas.read_csv(filepath,header=None)  # use pandas to read in
    series=curr[0]

    BBList=[]                          # put BBLs in a Python list as int64
    for i in range(series.size):
        BBList.append(np.int64(series[i]))   

    return BBList

""" Input: list of JSON strings from OpenData API. 
    Output: pandas DataFrame caputuring all information """
def JSONtoDataFrame(jstrings):

    frames = []
    for j in jstrings:
        frames.append(pandas.read_json(j,orient='records',dtype=False))
        if len(frames[-1].index)==10000 or len(frames[-1].index)==50000:
            print "THROTTLE WARNING"

    result = pandas.concat(frames, ignore_index=True)

    return result

"""Read binFile, a simple two-column dictionary, into a df. Return df.
    """
def getBin():

    f = pandas.read_csv(binFile,header=None,index_col=1)

    #remove duplicate BINs:
    f = f[~f.index.duplicated(keep='first')]

    return f

def readStreets():

    inf = open(streetsfile,"r")

    streets = []
    for line in inf:
        line=line.strip()
        streets.append(line)

    inf.close()

    return streets


main()

