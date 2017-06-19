"""
HPD Complaints

Retrieve and format data about current HPD Complaints in ENY,
through the Socrata OpenDataNY system. For inclusion into
Gentrification Early Warning System.

TAKES: 'BBL.csv', ordered list of BBLs in same directory
OUTPUT: 'HPD_Complaints.csv', table of open HPD Complaints
        and closed complaints received within the last 30 days.
        See 'columns' variable for columns included in table

Murphy Austin
734.660.7299
murphy.c.austin@gmail.com

December 2016
"""

import requests
import pandas
import json
import datetime
import exceptions
from helpers import *
import pdb

# source URLs:

complaintsURL = "https://data.cityofnewyork.us/resource/jspd-2hr7.json"
problemsURL = "https://data.cityofnewyork.us/resource/gp4p-wib8.json"


def main():

    
    ### STATIC VARIABLES ###
    
    # list of ENY zip codes
    zips = [11207,11208,11212,11233]

    # list of columns for final table: modify this to change what columns are included and in what order
    columns = ['BBL','BuildingID','HouseNumber','StreetName','ComplaintID','ReceivedDate','Apartment','Type','SpaceType',            
            'MajorCategory','MinorCategory','Code','Status_x','StatusDate_x','UnitType',
            'StatusID_x','StatusDescription','Status_y','StatusDate_y']
    
    # file that contains list of BBLS. MUST BE IN SAME FOLDER AS HPDComplaints.py
    rPath = "BBLs.csv"

    # name of output file
    wPath = "HPD_Complaints.csv"

    #############################################################################


    BBLs = readFile(rPath)  # read in the list of BBLs


    complaints = getComplaints(BBLs,zips)  # get table of HPD complaints from BBL properties
    
    pdb.set_trace()
    problems = pandas.read_csv('Complaint_Problems.csv',encoding='utf-8')
    
    complaints['new_index']=complaints.index
    
    master = pandas.merge(complaints,problems,how='left',on='ComplaintID')
    
    master.loc['ReceivedDate']=parseDOBDates(master['ReceivedDate'])
    
    master.set_index('new_index')
    
    master = arrange(master,columns)

    
    # export CSV
    master.to_csv(wPath)
 
    return 0




""" Pass in list of BBLs and zipCodes. Query OpenData API for complaints.
    Re-format to DataFrame object, return the DataFrame """
def getComplaints(BBList, zipCodes):
    


    ### Request data ###

    complaints = pandas.read_csv('Housing_Maintenance_Code_Complaints.csv',encoding='utf-8')
    
    # begin to isolate ENY entries
    complaints=complaints.drop(complaints[complaints['BoroughID']!=3].index)
    complaints=complaints.drop(complaints[complaints['CommunityBoard']!=5].index)
        
    BBLs=pandas.Series(-1,index=complaints.index)

    for i in range(len(complaints.index)):
        temp=complaints.iloc[i]
        
        BBL = str(temp['BoroughID'])
        curr=str(temp['Block'])
        buff='0'*(5-len(curr))
        BBL = BBL+buff+curr
        curr=str(temp['Lot'])[-4:]
        buff='0'*(4-len(curr))
        BBL=BBL+buff+curr

        BBLs.iloc[i]=BBL

    complaints['BBL']=BBLs
    
    
    complaints=dropSort(complaints,'BBL',BBList)
    return complaints
    


"""Input: DataFrame, list of columns
   Output: updated DataFrame rearranged to match order; extra columns deleted
   """
def arrange(df, columns):

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
            print "Deleting: %s" % (i)

    df = df[columns]    # rearrange dataFrame columns

    return df


main()
