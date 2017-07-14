#!/bin/bash

date

curl -o /Users/Murphy/Desktop/ENV/EWS/Scripts/raw_data/curled_data/DOB_ECB_opendata.csv https://data.cityofnewyork.us/api/views/6bgk-3dad/rows.csv?accessType=DOWNLOAD

curl -o /Users/Murphy/Desktop/ENV/EWS/Scripts/raw_data/curled_data/HPD_com_problems_opendata.csv https://data.cityofnewyork.us/api/views/a2nx-4u46/rows.csv?accessType=DOWNLOAD

curl -o //Users/Murphy/Desktop/ENV/EWS/Scripts/raw_data/curled_data/HPD_com_complaints_opendata.csv https://data.cityofnewyork.us/api/views/uwyv-629c/rows.csv?accessType=DOWNLOAD

python /Users/Murphy/Desktop/ENV/EWS/Scripts/DOBCount.py

python /Users/Murphy/Desktop/ENV/EWS/Scripts/furman.py

python /Users/Murphy/Desktop/ENV/EWS/Scripts/ACRIS.py

python /Users/Murphy/Desktop/ENV/EWS/Scripts/HPDComplaints.py

python /Users/Murphy/Desktop/ENV/EWS/Scripts/HPDViolations.py


