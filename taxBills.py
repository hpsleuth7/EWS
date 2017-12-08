"""
Tax Bills

Given taxbills csv from John Krauss, remove all non-ENY 
entries and sort by Lucy's index. Outpout ENYtaxbills.csv
"""
import pandas
from helpers import *
import pdb
import requests

# path for taxbills csv
billpath="tax_bills_Krauss.csv"

wPath = "2016_tax_bills_EWS.csv"

taxBillsURL = "https://api.nycdb.info/rentstab?"
def main():
	
	BBLs=readFile("Inputs/BBLs.csv")
	
	#bills=pandas.read_csv(billpath,index_col=False)
	
	bills=getBills(BBLs)
	
	bills=dropSort(bills,"ucbbl",BBLs)
	
	bills.to_csv(wPath,encoding='utf-8')
	
	return


def getBills(BBLs):

    resp = []
    
    pdb.set_trace()
    block=len(BBLs)/50    
    
    for i in range(block):
        list = ",".join(BBLs[i*50:(i+1)*50])   
        try:
            response=requests.get(taxBillsURL+"ucbbl=in."+list)       
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
            response=requests.get(taxBillsURL+"ucbbl=in."+list)       
        ### Error Check ###  
        except:     
            print "Connection Error"
            return -1
        if response.status_code != 200:
            print "Error querying API."
            return -1
        
        resp.append(response.content)

    billFrame = JSONtoDataFrame(resp)

    return billFrame


main()
