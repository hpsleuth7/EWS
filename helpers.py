"""
Helpers

Implementation of helper Classes used in Early Warning System package.

Murphy Austin
734.660.7299
murphy.c.austin@gmail.com

April 2017
"""


import pandas as pd
import datetime
import pdb
import numpy as np


""" Object to transform Pd series of SODA datestrings into a series
    of python date objects.
    Usage: (1) Datefix(<SODA series>) (2) <python dates> = Datefix.getDates()
    """
class Datefix:
    def __init__(self, datestring):
        self.sodaDates = datestring
        self.pyDates = 0

    def upload(self,series):
        self.sodaDates=series

    def getDates(self):

        # create fresh series object
        self.pyDates = pd.Series(-1,index=self.sodaDates.index)

        for i in range(len(self.sodaDates.index)):
            sDate=self.sodaDates.iloc[i]
            if not isinstance(sDate,unicode):         # check for NaNs
                print "ERROR in dateFixer: missing date"
                self.pyDates.iloc[i]=""
                continue
            if len(sDate)!= 23:                   # check correct SODA date format
                    print "ERROR in dateFixer: corrupted date"
                    self.pyDates.iloc[i]=""
                    continue
            year = int(sDate[:4])
            month = int(sDate[5:7])
            day = int(sDate[8:10])
            newDate=datetime.date(year,month,day)
            self.pyDates.iloc[i]=newDate

        return self.pyDates

"""Input: series of DOB dates - 'YYYYMMDD' or 'MM/DD/YYYY'
   Output: series of pyDates, with non-parseable entries -1"""
def parseDOBDates(dateSeries):

    pyDates = pd.Series(-1,index=dateSeries.index)

    
    for i in range(len(dateSeries.index)):
        curr=dateSeries.iloc[i]
        if isinstance(curr,float):
        	continue
        elif '/' in curr:               #try MM/DD/YYYY format
            try:
                month=int(curr[:2])
                day=int(curr[3:5])
                year=int(curr[6:10])
            except:
                continue
        else:                         #try YYYYMMDD format
            try:
                year=int(curr[:4])
                month=int(curr[4:6])
                day=int(curr[6:8])
            except:                     #bad date value
                month=1
                day=1
                year=1970
        pyDates.iloc[i]=datetime.date(year,month,day)

    
    return pyDates

"""Input: series of SODA-formatted date strings
   Output: series of pyDates, with missing/non-parseable entries -1"""
def parseSODADates(sodaDates):
    pyDates = pd.Series(-1,index=sodaDates.index)
    noDate=0

    for i in range(len(sodaDates.index)):
        sDate=sodaDates.iloc[i]
        if not isinstance(sDate,unicode):         # check for NaNs
            noDate+=1
            continue
        if len(sDate)!= 23:                   # check correct SODA date format
            print "ERROR in dateFixer: corrupted date"
            continue
        year = int(sDate[:4])
        month = int(sDate[5:7])
        day = int(sDate[8:10])
        pyDates.iloc[i]=datetime.date(year,month,day)
        
    if noDate>0:
    	print "WARNING in dateFixer:",noDate,"dates missing."
    return pyDates
    
"""Input: frame = dataframe with column 'BBL'. 
		  colname = label of column containing bbls
		  bblist = ordered list of ENY BBLs
   Output: modified dataframe with all non-ENY BBLs dropped and ordered
           by Lucy's BBL index 
    NOTE frame index values must be unique """
def dropSort(frame,colname,bblist):

    toDelete = []
    frame['new_index']=pd.Series([-1]*len(frame.index),index=frame.index)
    for i in frame.index:
        curr=str(frame.loc[i,colname])
        if curr in bblist:
        	frame.loc[i,'new_index']=bblist.index(curr)
        else:    
            toDelete.append(i)
    
    frame = frame.drop(toDelete)
    frame=frame.sort_values(['new_index'])
    frame=frame.set_index('new_index')
    
    return frame


""" Pass in a filepath string to a list of ENY BBLs. Read in the BBLs
and return in a list """
def readFile(filepath):

    curr=pd.read_csv(filepath,header=None)  # use pd to read in
    series=curr[0]

    BBList=[]                          # put BBLs in a Python list
    for i in range(series.size):
        BBList.append(str(series[i]))

    return BBList
    
""" Input: list of JSON strings from OpenData API. 
    Output: pd DataFrame caputuring all information """
def JSONtoDataFrame(jstrings, jtype='records'):

    frames = []
    for j in jstrings:
        frames.append(pd.read_json(j,orient=jtype,dtype=False))
        if len(frames[-1].index)==10000 or len(frames[-1].index)==50000:
            print "THROTTLE WARNING"

    result = pd.concat(frames, ignore_index=True)

    return result
    
def printStart(name):

    print "---- Fetching", name, "Data ----"
    print "    ------        Please Wait         ------"
    print ""
    
    return
    
def printEnd(name):

    print "*******", name, " complete *******"
    print ""

