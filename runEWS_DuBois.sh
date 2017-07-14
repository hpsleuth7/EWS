#!/bin/bash

date

curl -o /Users/admin/Desktop/EWS/raw_data/curled_data/DOB_ECB_opendata.csv https://data.cityofnewyork.us/api/views/6bgk-3dad/rows.csv?accessType=DOWNLOAD

curl -o /Users/admin/Desktop/EWS/raw_data/curled_data/HPD_com_problems_opendata.csv https://data.cityofnewyork.us/api/views/a2nx-4u46/rows.csv?accessType=DOWNLOAD

curl -o /Users/admin/Desktop/EWS/raw_data/curled_data/HPD_com_complaints_opendata.csv https://data.cityofnewyork.us/api/views/uwyv-629c/rows.csv?accessType=DOWNLOAD

python /Users/admin/Desktop/EWS/Scripts/DOBCount.py

python /Users/admin/Desktop/EWS/Scripts/furman.py

python /Users/admin/Desktop/EWS/Scripts/ACRIS.py

python /Users/admin/Desktop/EWS/Scripts/HPDComplaints.py

python /Users/admin/Desktop/EWS/Scripts/HPDViolations.py


