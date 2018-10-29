#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Copyright 2012 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Simple intro to using the Google Analytics API v3.

This application demonstrates how to use the python client library to access
Google Analytics data. The sample traverses the Management API to obtain the
authorized user's first profile ID. Then the sample uses this ID to
contstruct a Core Reporting API query to return the top 25 organic search
terms.

Before you begin, you must sigup for a new project in the Google APIs console:
https://code.google.com/apis/console

Then register the project to use OAuth2.0 for installed applications.

Finally you will need to add the client id, client secret, and redirect URL
into the client_secrets.json file that is in the same directory as this sample.

Sample Usage:

  $ python hello_analytics_api_v3.py

Also you can also get help on all the command-line flags the program
understands by running:

  $ python hello_analytics_api_v3.py --help
"""

__author__ = 'akalamoyo"gmail.com (Moyosore Akala)'

import argparse
import sys
import csv
import string
import re

from apiclient.errors import HttpError
from apiclient import sample_tools
from oauth2client.client import AccessTokenRefreshError
import pandas as pd

class SampledDataError(Exception): pass


def main(argv):
    # Authenticate and construct service.
    service, flags = sample_tools.init(
        argv, 'analytics', 'v3', __doc__, __file__,
        scope='https://www.googleapis.com/auth/analytics.readonly')
    # Try to make a request to the API. Print the results or handle errors.
    try:
        profile_id = profile_ids[profile]
        if not profile_id:
            print('Could not find a valid profile for this user.')
        else:
            for start_date, end_date in date_ranges:
                limit = ga_query(service, profile_id, 0,
                                 start_date, end_date).get('totalResults')
                for pag_index in range(0, limit, 10000):
                    results = ga_query(service, profile_id, pag_index,
                                       start_date, end_date)
                    if results.get('containsSampledData'):
                        raise SampledDataError
                    print_results(results, pag_index, start_date, end_date)

    except TypeError as error:
        # Handle errors in constructing a query.
        print('There was an error in constructing your query : %s' % error)
    except HttpError as error:
        # Handle API errors.
        print('Arg, there was an API error : %s : %s' % (error.resp.status, error._get_reason()))
    except AccessTokenRefreshError:
        # Handle Auth errors.
        print('The credentials have been revoked or expired, please re-run '
              'the application to re-authorize')

    except SampledDataError:
        # force an error if ever a query returns data that is sampled!
        print('Error: Query contains sampled data!')


def ga_query(service, profile_id, pag_index, start_date, end_date):
    return service.data().ga().get(
        ids='ga:' + profile_id,
        start_date=start_date,
        end_date=end_date,
        metrics='ga:transactions',
        dimensions='ga:transactionId, ga:medium, ga:sessionsToTransaction, ga:deviceCategory, ga:city, ga:landingPagePath, ga:exitPagePath',
        # sort='-ga:pageviews',
        samplingLevel='HIGHER_PRECISION',
        start_index=str(pag_index + 1),
        max_results=str(pag_index + 10000)).execute()


def print_results(results, pag_index, start_date, end_date):
    """Prints out the results.
    This prints out the profile name, the column headers, and all the rows of
    data.
    Args:
      results: The response returned from the Core Reporting API.
    """
    # New write header
    if pag_index == 0:
        if (start_date, end_date) == date_ranges[0]:
            print('Profile Name: %s' % results.get('profileInfo').get('profileName'))
            columnHeaders = results.get('columnHeaders')
            cleanHeaders = [str(h['name']) for h in columnHeaders]
            writer.writerow(cleanHeaders)
        print('Now pulling data from %s to %s.' % (start_date, end_date))
    # Print data table.
    if results.get('rows', []):
        for row in results.get('rows'):
            for i in range(len(row)):
                old, new = row[i], str()
                for s in old:
                    new += s if s in string.printable else ''
                row[i] = new
            writer.writerow(row)

    else:
        print('No Rows Found')

    limit = results.get('totalResults')
    print(pag_index, 'of about', int(round(limit, -4)), 'rows.')
    return None


profile_ids = {'MDirect': '95522407',
               'MAggs': '167904022',
               'MSingleView': '166443173',
               'MAll': '107786762',
               'Esure': '157641541',
               'LGAgg': '168167103',
               'LGAggUn': '123788188',
               'LGIdol': '123697822',
               'LGMsm': '139517439',
               'LGAll': '117717207',
               'LGDirect': '109346252',
               'LGRaw': '109348473',
               'RacAgg': '124209887',
               'RacIdol': '122115153',
               'RacMsm': '139477936',
               'RacAll': '117713512',
               'RacDirect': '106758099',
               'RacRaw': '106785234'}
# Uncomment this line & replace with 'profile name': 'id' to query a single profile
# Delete or comment out this line to loop over multiple profiles.

# profile_ids = {'MAggs':  '167904022'}

date_data = pd.read_csv("K:/Data/Moyosore/Google_Analytics/add_dates.csv", sep = ",", encoding = "ISO-8859-1")
date_ranges = [(date_data.iloc[i][0],date_data.iloc[i][1]) for i in range(len(date_data))]

for profile in sorted(profile_ids):
    path = 'K:Data/Moyosore/Google_Analytics/'  # replace with path to your folder where csv file with data will be written
    filename = 'google_analytics_data_%s_1.csv'
    with open(path + filename % profile.lower(), 'wt') as f:
        writer = csv.writer(f, lineterminator='\n')
        if __name__ == '__main__': main(sys.argv)
    print("Profile done. Next profile...")
#
##create database connection
import pyodbc
import sqlalchemy
import urllib
conn = pyodbc.connect(
    r'DRIVER={ODBC Driver 11 for SQL Server};'
    r'SERVER=HG-SOS-MI;'
    r'DATABASE=Data_Testing;'
    r'Trusted_Connection=yes;'
)
params = urllib.parse.quote_plus(r'DRIVER={ODBC Driver 11 for SQL Server};'
                                 r'SERVER=HG-SOS-MI;'
                                 r'DATABASE=Data_Testing;'
                                 r'Trusted_Connection=yes;')
engine = sqlalchemy.create_engine('mssql+pyodbc:///?odbc_connect=%s' % params)


path = 'K:/Data/Moyosore/Google_Analytics/'
names =['google_analytics_data_esure_1.csv',
'google_analytics_data_lgaggun_1.csv',
'google_analytics_data_lgall_1.csv',
'google_analytics_data_lgdirect_1.csv',
 'google_analytics_data_lgidol_1.csv',
 'google_analytics_data_lgmsm_1.csv',
 'google_analytics_data_lgraw_1.csv',
 'google_analytics_data_maggs_1.csv',
 'google_analytics_data_mdirect_1.csv',
 'google_analytics_data_racagg_1.csv',
 'google_analytics_data_racall_1.csv',
 'google_analytics_data_racdirect_1.csv',
 'google_analytics_data_racidol_1.csv',
 'google_analytics_data_racmsm_1.csv',
 'google_analytics_data_racraw_1.csv']

data_list = []
import pandas as pd
import numpy as np
for name in names:
  data = pd.read_csv(path+name, sep = ',', encoding = 'ISO-8859-1')
  data_list.append(data)
#columns = data_list[8].columns
columns = ['ga:transactionId','ga:medium','ga:sessionsToTransaction','ga:deviceCategory','ga:city','ga:landingPagePath'\
           ,'ga:exitPagePath','ga:transactions']

data_list = []
for name in names:
  data = pd.read_csv(path+name, sep = ',', encoding = 'ISO-8859-1', names = columns, header = None)
  data_list.append(data)
data_table = data_list[0].append([data_list[1]])
data_list.pop(0)
data_list.pop(1)
data_table = data_table.append(data_list)
#searchfor = ['ga:transactionsPerUser']
#data_table = data_table[~data_table['ga:transactionsPerUser'].isin(searchfor)]
#data_table["ga:daysToTransaction"] = data_table["ga:daysToTransaction"].astype(np.int64)
#data_table["ga:sessionsToTransaction"] = data_table["ga:sessionsToTransaction"].astype(np.int64)
#data_table["ga:transactions"] = data_table["ga:transactions"].astype(np.int64)
#data_table["ga:transactionRevenue"] = data_table["ga:transactionRevenue"].astype(np.float64)
#data_table["ga:transactionsPerUser"] = data_table["ga:transactionsPerUser"].astype(np.float64)

data_table = data_table.reset_index(drop=True)
try:
    data_table = data_table.loc[~data_table["ga:sessionsToTransaction"].isin(["ga:sessionsToTransaction"])]
except KeyError:
    data_table = data_table
def restructure(y):
    x = y.lower()
    if "/confirmation" in x:
        x = "PurchaseConfirmationPage"
    if "success" in x:
        x = "PurchaseSuccessPage"
    if "document" in x:
        x = "ViewPolicyDocumentPage"
    if "yourdetails" in x:
        x = "CustomerDetailsPage"
    if "your-details" in x:
        x = "CustomerDetailsPage"
    if "yourtrip" in x:
        x = "TripDetailsPage"
    if "your-trip" in x:
        x = "TripDetailsPage"
    if "payment" in x:
        x = "PurchaseSuccessPage"
    if "medical" in x:
        x = "MedicalDetailsPage"
    if "idol?refid=" in x:
        x = "LandingPage"
    if "your-quote" in x:
        x = "QuoteDetailsPage"
    if "quote/yourquote" in x:
        x = "QuoteDetailsPage"
    if "faqs" in x:
        x = "FAQS"
    if x == "www.rac.co.uk/insurance/travel-insurance":
        x = "LandingPage"
    if "/quote/retrieve" in x:
        x = "RetrieveSavedQuotePage"
    if "/quote/save" in x:
        x = "SaveQuotePage"
    if "/extracover" in x:
        x = "ExtraCoverPage"
    if "/extra-cover" in x:
        x = "ExtraCoverPage"
    if "policy" in x:
        x = "PolicyDetailsPage"
    if "confirmemail" in x:
        x = "UserAccountDetailsPage"
    if "userid" in x:
        x = "UserAccountDetailsPage"
    if "register" in x:
        x = "UserRegistrationPage"
    if "login" in x:
        x = "UserAccountDetailsPage"
    if "forgot" in x:
        x = "UserAccountDetailsPage"
    if "generic-travel" in x:
        x = "LandingPage"
    if "conditions" in x:
        x = "Conditions-of-UsePage"
    if "quote/decline" in x:
        x = "QuoteDeclinedPage"
    if "screening/decline" in x:
        x = "ScreeningDeclinedPage"
    return x

data_table['ga:exitPagePath'] = data_table['ga:exitPagePath'].apply(restructure)
data_table.to_sql(name = "Google Analytics", con= engine, if_exists="replace", index=False)
print("All profiles data extracted and sent to database.")

##remove created csv files
not_used = ['google_analytics_data_lgagg_1.csv','google_analytics_data_mall_1.csv','google_analytics_data_msingleview_1.csv']
all_names = names + not_used
import os
for file in all_names:
    os.remove(path+file)


