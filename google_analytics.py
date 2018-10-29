#!/usr/bin/python
# -*- coding: utf-8 -*-

"""Intro to using the Google Analytics API v3.

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


"""


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


profile_ids = {'INPUT_PROFILE_NAME': 'INPUT_PROFILE_ID',
               'INPUT_PROFILE_NAME': 'INPUT_PROFILE_ID',
               }

date_data = pd.read_csv("add_dates.csv", sep = ",", encoding = "ISO-8859-1")
date_ranges = [(date_data.iloc[i][0],date_data.iloc[i][1]) for i in range(len(date_data))]

for profile in sorted(profile_ids):
    path = 'INPUT_PATH'  # replace with path to your folder where csv file with data will be written
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
    r'SERVER=SERVER_NAME;'
    r'DATABASE=DATABASE_NAME;'
    r'Trusted_Connection=yes;'
)
params = urllib.parse.quote_plus(r'DRIVER={ODBC Driver 11 for SQL Server};'
                                 r'SERVER=SERVER_NAME;'
                                 r'DATABASE=DATABASE_NAME;'
                                 r'Trusted_Connection=yes;')
engine = sqlalchemy.create_engine('mssql+pyodbc:///?odbc_connect=%s' % params)


path = 'INPUT_PATH'
names =['INPUT_NAMES_OF_DOWNLOADED_FILES_IN_A_LIST']

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
    if "yourdetails" in x:
        x = "CustomerDetailsPage"
   if "register" in x:
        x = "UserRegistrationPage"
    if "login" in x:
        x = "UserAccountDetailsPage"
     return x

data_table['ga:exitPagePath'] = data_table['ga:exitPagePath'].apply(restructure)

#send rsults to database
data_table.to_sql(name = "Google Analytics", con= engine, if_exists="replace", index=False)
print("All profiles data extracted and sent to database.")

##remove downloaded csv files
not_used = ['INPUT_ANY_UNUSED_FILE (OR EMPTY)']
all_names = names + not_used
import os
for file in all_names:
    os.remove(path+file)

#run progam from command line
