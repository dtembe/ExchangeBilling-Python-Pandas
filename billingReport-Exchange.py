#!/usr/bin/env python3

import pandas as pd
import glob
import numpy as np
import csv
import os

#global Vars -
exchVer = "2013"
lastDayCurrMon = '12/31/2018'



#Edit the path and then use this block of code on the Reporting Server
#this will list out latest 2 csv, which we will assume to be the last month and current month report.
# path = r'/home/dtembe/reports/*.csv'
# list_of_files = glob.glob(path)
# sorted_files = sorted(list_of_files, key=os.path.getmtime)
# print sorted_files[-1]
# print sorted_files[-2]
#  instead if print, we wll need to add the files to the last & current variables.

#delete old files if they exist -
for f in glob.glob("/tmp/Report/Exchange/Exchange-Detail-AllExchange*.csv"):
    try:
        os.remove(f)
    except:
        pass

for f in glob.glob("/tmp/Report/Exchange/temp*.csv"):
    try:
        os.remove(f)
    except:
        pass


for f in glob.glob("/tmp/Report/Exchange/Exchange-Billing*.csv"):
    try:
        os.remove(f)
    except:
        pass




#Read a csv file that contains a storeName, companName column so billing can have grouing by customer.
reader = csv.reader(open('/tmp/Report/Exchange/Master/companyMap.csv', 'r'))
companyMapDict = {}
for row in reader:
   k, v = row
   companyMapDict[k] = v


#These are all the raw exports from Exchange servers. Glob them into a single dataFrame
dfs = [pd.read_csv(f, index_col=None) for f in glob.glob(r'c:/tmp/Report/Exchange/*.csv')]

common_cols = list(set.intersection(*[set(x.columns.tolist()) for x in dfs]))

all_data = pd.concat((df.set_index(common_cols) for df in dfs), axis=1).reset_index()

#TODO for a later time. Not in use currently.
# def get_merged(files, **kwargs):
#     df = pd.read_csv(files[0], **kwargs)
#     for f in files[1:]:
#         df = df.merge(pd.read_csv(f, **kwargs), how='outer')
#     return df
#
# # print(get_merged(files))
#
#
# all_data = get_merged(files)
#print(all_data)

#Fill in Empty cells in certain columns.
all_data.loc[all_data['Billing'].str.len() == 0 , 'Billing'] = np.NaN
all_data.loc[all_data['Billing'].str.len() > 5 , 'Billing'] = np.NaN
all_data['Billing'] = all_data['Billing'].fillna('bill')

all_data.loc[all_data['Archive Database'].str.len() == 0 , 'Archive Database'] = np.NaN
all_data['Archive Database'] = all_data['Archive Database'].fillna('noArchive')



all_data.columns = [c.replace(' ', '_') for c in all_data.columns]

#example from Stacktrace.
# >>> data = { 'ID': [ 101, 201, 301, 401 ] }
# >>> df = pd.DataFrame(data)
# >>> class SurnameMap(dict):
# ...     def __missing__(self, key):
# ...         return ''
# ...
# >>> surnamemap = SurnameMap()
# >>> surnamemap[101] = 'Mohanty'
# >>> surnamemap[301] = 'Drake'
# >>> df['Surname'] = df['ID'].apply(lambda x: surnamemap[x])
# >>> df
#
# Consider if billable or non billable based on Mailbox type.
all_data.loc[all_data.Mailbox_Type == 'SharedMailbox', 'Billing'] = "nonBill"

all_data.loc[all_data.Mailbox_Type == 'RoomMailbox', 'Billing'] = "nonBill"

#initial clean csv.
all_data.to_csv('c:/tmp/Report/Exchange/Exchange-Billing-All.csv', sep=',', encoding="ISO-8859-1")

#read in the above file for further work.
workDf = pd.read_csv('c:/tmp/Report/Exchange/Exchange-Billing-All.csv', sep=',', encoding = "ISO-8859-1")


#add date column and then populate the column values. Change to last month / last day.
workDf['Date'] = lastDayCurrMon
workDf['Exchange'] = exchVer

#workDf.reindex(axis=1)
workDf.drop(['Unnamed: 0'], axis=1, inplace=True)

#workDf.insert(5, 'Date', '9/30/2018')
print(workDf.head(5))
workDf.columns = workDf.columns.str.replace('_', ' ')
print(workDf.head(5))

workDf = workDf.reindex(['Server Name', 'Storage Group', 'storename', 'Archive Database',  'Mailbox Type', 'Display Name', 'Mailbox Size', 'Item Count','Deleted Items Size', 'Primary Email Address', 'Email Addresses', 'Billing', 'Exchange', 'Office', 'Basic', 'ActiveSync', 'BlackBerry', 'VPN', 'Date'], axis=1)
header = ['Server Name', 'Storage Group', 'storename', 'Archive Database', 'Mailbox Type', 'Display Name', 'Mailbox Size', 'Item Count','Deleted Items Size', 'Primary Email Address', 'Email Addresses', 'Billing', 'Exchange', 'Office', 'Basic', 'ActiveSync', 'BlackBerry', 'VPN', 'Date']

#drop all nonBill rows from Billing Column
workDf = workDf[~workDf['Billing'].str.contains('nonBill')]
#no need for the below statements as we already dropped the  below mailbox type as nonBill.
# workDf = workDf[~workDf['Mailbox Type'].str.contains('DiscoveryMailbox')]
# workDf = workDf[~workDf['Mailbox Type'].str.contains('LinkedMailbox')]

#Sort on SotreName
workDf.sort_values("storename", inplace=True)
workDf.to_csv('c:/tmp/Report/Exchange/temp1.csv', columns= header, sep=',', encoding="ISO-8859-1", index=False)
workDf['storename'].replace(companyMapDict, inplace=True)
workDf.to_csv('c:/tmp/Report/Exchange/temp2.csv', columns= header, sep=',', encoding="ISO-8859-1", index=False)
#workDf.merge(companyMapDict, how='outer' )
workDf = workDf.reindex(['storename', 'Primary Email Address'], axis=1)
header = ['storename', 'Primary Email Address']
workDf.sort_values('storename', inplace=True)
workDf.to_csv('c:/tmp/Report/Exchange/temp3.csv', columns= header, sep=',', encoding="ISO-8859-1", index=False)


#TODO Sample code not used now.
#workDf.groupby(['storename']).storename.agg(['count']).to_frame('Totals').reset_index()
# workDf = workDf.groupby(['storename']).agg().reset_index()
#
# workDf.columns = ['storename', 'Primary Email Address', 'counts']
# def f(x):
#     x.loc[-1] = pd.Series([])
#     return x
# df = df.astype(str).groupby(['column1','column2'], as_index=False).apply(f)

#Since we are totaling accounts based on storeName - group in dataFrame.
workDf['count'] = workDf.groupby('storename').transform('count')
print(workDf)
workDf.rename(columns={'storename': 'Company Name', 'Primary Email Address': 'Email Address', 'count': 'Total'}, inplace=True)
print (workDf)

#Final output.
workDf.to_csv('c:/tmp/Report/Exchange/Exchange-Billing.csv', sep=',', encoding="ISO-8859-1", index=False)

