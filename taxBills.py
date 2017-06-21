"""
Tax Bills

Given taxbills csv from John Krauss, remove all non-ENY 
entries and sort by Lucy's index. Outpout ENYtaxbills.csv
"""
import pandas
from helpers import *
import pdb

# path for taxbills csv
billpath="tax_bills_Krauss.csv"

wPath = 'tax_bills_EWS.csv"
def main():
	
	BBLs=readFile("BBLs.csv")
	
	bills=pandas.read_csv(billpath,index_col=False)
	
	bills=dropSort(bills,"ucbbl",BBLs)
	
	bills.to_csv(wPath,encoding='utf-8')
	
	return





main()
