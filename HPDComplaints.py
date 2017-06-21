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

HPD_problems_file="raw_data/curled_data/HPD_com_problems_opendata.csv"
HPD_complaints_file='raw_data/curled_data/HPD_com_complaints_opendata.csv'

def main():

    
    ### STATIC VARIABLES ###
    
    # list of ENY zip codes
    zips = [11207,11208,11212,11233]

    # list of columns for final table: modify this to change what columns are included and in what order
    columns = ['BBL','BuildingID','Address','ComplaintID','ReceivedDate','Apartment','Type','SpaceType',            
            'MajorCategory','MinorCategory','Code','Status_x','StatusDate_x','UnitType',
            'StatusID_x','StatusDescription','Status_y','StatusDate_y']
    
    # file that contains list of BBLS. MUST BE IN SAME FOLDER AS HPDComplaints.py
    rPath = "Inputs/BBLs.csv"

    # name of output file
    wPath = "raw_data/HPD_omplaints_EWS.csv"

    #############################################################################


    BBLs = readFile(rPath)  # read in the list of BBLs


    complaints = getComplaints(BBLs,zips)  # get table of HPD complaints from BBL properties
    
    problems = pandas.read_csv(HPD_problems_file,encoding='utf-8')
    
    complaints['new_index']=complaints.index
    
    master = pandas.merge(complaints,problems,how='left',on='ComplaintID')
    
    master['ReceivedDate']=parseDOBDates(master['ReceivedDate'])
    
    master=master.sort_values(['new_index','ReceivedDate'],ascending=[True,False])
    master=master.set_index('new_index')
    
    master = arrange(master,columns)

    
    # export CSV
    master.to_csv(wPath)
 
    return 0




""" Pass in list of BBLs and zipCodes. Query OpenData API for complaints.
    Re-format to DataFrame object, return the DataFrame """
def getComplaints(BBList, zipCodes):
    


    ### Request data ###

    complaints = pandas.read_csv(HPD_complaints_file,encoding='utf-8')
    
    # begin to isolate ENY entries
    complaints=complaints.drop(complaints[complaints['BoroughID']!=3].index)
    complaints=complaints.drop(complaints[complaints['CommunityBoard']!=5].index)

	# Make BBL column and concatenate street address        
    BBLs=pandas.Series(-1,index=complaints.index)
    address=pandas.Series(-1,index=complaints.index)

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
        
        address.iloc[i]=str(temp['HouseNumber'])+" "+str(temp['StreetName'])


    complaints['Address']=address  # add address column to dataframe

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

