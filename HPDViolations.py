"""
HPD Violations

Retrieve and format data about current HPD Violations in ENY,
through the Socrata OpenDataNY system. For inclusion into
Gentrification Early Warning System.

TAKES: 'BBL.csv', ordered list of BBLs in same directory
OUTPUT:'HPD_Violations.csv', table of open violations from the
last year, sorted by BBLnindex. See 'columns' variable 
for columns included.

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
from helpers import *

# source URLs:

complaintsURL = "https://data.cityofnewyork.us/resource/jspd-2hr7.json"
problemsURL = "https://data.cityofnewyork.us/resource/gp4p-wib8.json"
violationsURL = "https://data.cityofnewyork.us/resource/b2iz-pps8.json"

# list of ENY zip codes
zips = [11207,11208,11212,11233]

# list of columns for final table: modify this to change what columns are included and in what order - CHANGE THIS
columns = ['BBL','inspectiondate','apartment','class','novdescription','currentstatus',
           'currentstatusdate','novissueddate','approveddate','originalcertifybydate',
           'originalcorrectbydate','certifieddate','violationid']
    
# file that contains list of BBLS. MUST BE IN SAME FOLDER AS HPDComplaints.py
rPath = "Inputs/BBLs.csv"

# MODIFY THIS - name of output file
wPath = "raw_data/HPD_violations_EWS.csv"



def main():

    
    ### STATIC VARIABLES ###
    
   

    #############################################################################


    BBLs = readFile(rPath)  # read in the list of BBLs

    violations = getViolations(BBLs,zips)  # get table of HPD complaints from BBL properties

    
    # fix dates
    dateFixer=Datefix(violations['inspectiondate'])
    violations['inspectiondate']=dateFixer.getDates()

    ### put columns in order and remove extras, and sort rows ###
    violations = arrange(violations,columns,BBLs)


    # write to excel
    violations.to_csv(wPath,encoding='utf-8')

    return 0




""" Pass in list of BBLs and zipCodes. Query OpenData API for violations.
    Re-format to DataFrame object, return the DataFrame """
def getViolations(BBLs, zipCodes):
    

    ### Request data ###

    resp=[]  # list to contain json strings


    ### Calculate date of one year ago ###
    now = datetime.datetime.today()
    year=datetime.timedelta(days=365)
    last=now-year
    yearAgo = str(last.year)+"-"+str(last.month)+"-"+str(last.day)+"T00:00:00" # format time for query


    ### TO DO: request currentstatusid=1,2,or27 from last year
    
    codes=[1,2,27]    #statusid codes

    for i in zipCodes:
        for j in codes:    # search for open violations in last year
            
            parameters = {"currentstatusid":j,"boroid":3,"zip":i,"$limit":50000}

            try:
            
                response = requests.get(violationsURL+"?$where=inspectiondate > '"+yearAgo+"'",
                        params=parameters)

            ### Error Check ###  
            except SSLError:       
                print "Connection Error"
                return -1

            if response.status_code != 200:
                print "Error querying API."

            resp.append(response.content) 
    
    complaints = JSONtoDataFrame(resp)  # convert json strings into one dataframe
    
    ### merge address fields and BBL fields ###
    complaints = mutate(complaints)             # THIS CHANGES DEPENDING ON DATASET LABELS

    # keep only rows with correct BBLs
    toDelete = []
    for i in range(len(complaints.index)):
        curr=complaints.loc[i]
        if str(curr['BBL']) not in BBLs:
            toDelete.append(i)
    
    complaints = complaints.drop(complaints.index[toDelete])

    return complaints




""" Input: list of JSON strings from OpenData API. 
    Output: pandas DataFrame caputuring all information """
def JSONtoDataFrame(jstrings):

    frames = []
    for j in jstrings:
        frames.append(pandas.read_json(j,orient='records'))
        print len(frames[-1].index)
    

    result = pandas.concat(frames, ignore_index=True)

    return result



""" Perform custom changes to dataframe: BBL field and address. 
    Return modified dataframe """
def mutate(df):

    ### MERGE BBL ###

    borough = df.pop('boroid')  # get series for borough, block, and lot
    block = df.pop('block')
    lot = df.pop('lot')
    dataLen=borough.size

    BBL = pandas.Series('-1',index=range(dataLen))  # make new Series of same length


    B=""
    curr = ''
    for i in range(dataLen):   # string together the three values w/ buffer 0's
        B = str(borough[i])
        curr=str(block[i])
        buff='0'*(5-len(curr))
        B = B+buff+curr
        curr=str(lot[i])
        buff='0'*(4-len(curr))
        B=B+buff+curr

        BBL[i]=B

    df['BBL']=BBL          # add BBL column to dataframe


    ### MERGE ADDRESS ###

    houseNum=df.pop('housenumber')
    street=df.pop('streetname')

    address=pandas.Series('-1',index=range(dataLen)) #make new Series of same length

    for i in range(dataLen):
        address[i]=str(houseNum[i])+" "+street[i]


    df['address']=address  # add address column to dataframe

    return df

"""Input: DataFrame, list of columns
   Output: updated DataFrame rearranged to match order; extra columns deleted
   """
def arrange(df, columns,blist):

    curr = df.columns.tolist()
    toDel=[]
    for i in columns:    # check if all specified columns are in dataFrame
        if i not in curr:
            print "WARNING: Column %s not found." % (i)   # warn if column not found
            toDel.append(i)
    for i in toDel:
        columns.remove(i)
    for i in curr:                  # remove any dataFrame column not in list
        if i not in columns:
            df.pop(i)

    df = df[columns]    # rearrange dataFrame columns

    # add index by BBL and sort rows
    temp = pandas.Series([-1]*len(df.index),index=df.index)  #create new column
    temp.name='new_index'
    df=pandas.concat([df,temp],axis=1)
    
    for i in df.index:                                       #populate new_index using position of BBL
        df.loc[i,'new_index']=blist.index(df.loc[i,'BBL'])

    # sort by BBL index and date
    df=df.sort_values(['new_index','inspectiondate'],ascending=[True,False])

    
    df=df.set_index('new_index')


    return df



main()

