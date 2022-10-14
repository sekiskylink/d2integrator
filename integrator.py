#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = "Sekiwere Samuel"

import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import json
import base64
import logging
import getopt
import sys
import datetime
import time
import psycopg2
import psycopg2.extras
# import json
import json_stream.requests

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

dbconfig = {
    'db_name': 'd2integrator',
    'db_host': 'localhost',
    'db_port': '5432',
    'db_user': 'postgres',
    'db_passwd': 'postgres',
}

config = {
    # dispatcher2 confs
    'dispatcher2_queue_url':'http://localhost:9191/queue',
    'dispatcher2_username': 'admin',
    'dispatcher2_password': 'admin',
    'dispatcher2_source': 'hmis',
    'dispatcher2_destination': 'repo',
}

logging.basicConfig(
    format='%(asctime)s:%(levelname)s:%(message)s', filename='/tmp/integration.log',
    datefmt='%Y-%m-%d %I:%M:%S', level=logging.DEBUG
)

cmd = sys.argv[1:]
opts, args = getopt.getopt(
    cmd, 'cdy:m:q:p:n:i:l:h',
    [
        'current_date', 'direct_sync', 'year', 'month', 'quarter',
        'period', 'days_back', 'incremental', 'district_list', 'help'
    ]
)

# use current month as default
year = None
month = None
quarter = None
DIRECT_SENDING = False
USE_CURRENT_DATE = False
USE_DAYS_BACK = False
MONTH_DEFINED = False
INCREMENTAL_INTEGRATION = False
specific_period = ""
days_back = 0
district_list = ""

for option, parameter in opts:
    if option in ['-d', '--direct_sync']:
        DIRECT_SENDING = True
    if option in ['-c', '--current_date']:
        USE_CURRENT_DATE = True
    if option in ['-y', '--year']:
        year = parameter
        try:
            year = int(year)
        except:
            pass
    if option in ['-m', '--month']:
        month = parameter
        try:
            month = int(month)
        except:
            pass
    if option in ['-q', '--quarter']:
        quarter = parameter
        try:
            quarter = int(quarter)
        except:
            pass
    if option in ['-p', '--period']:
        specific_period = parameter
    if option in ['-n', '--days_back']:
        days_back = parameter
        try:
            days_back = int(days_back)
            if days_back:
                USE_DAYS_BACK = True
        except:
            pass
    if option in ['-i', '--incremental']:
        INCREMENTAL_INTEGRATION = True
    if option in ['-l', '--district_list']:
        district_list = parameter

    if option in ['-h', '--help']:
        print("A script to synchronize data from one DHIS2 instance to another")
        print("optionally via dispatcher2 or another data exhange middleware.")
        print("")
        print(
            "Usage: python integrator.py [-i] [-d ] [-c ] [-y <year>] [-m <month>] [-p <period>] [-n <days>] [-l <district_list>]")
        print("-d Direct synchronisation without use of data exchange middleware.")
        print("-c --current_date Whether to generate values only for the date when script is run.")
        print("-y --year The year for which to read and push values.")
        print("-m --month The month for which to read values for pushing to another instance.")
        print("-q --quarter The quarter for which to read values for pushing to another instance.")
        print("-n --days_back read and send values pre dating n days.")
        print("-p --period The DHIS 2 period used for pulling data from source instance. Format YYYYMMDD")
        print("-l --district_list A string of comma-separated district names")
        print("-i --incremental whether to do incremental/continuous synchronisation based on last successful sync.")
        print("-h --help This message.")
        sys.exit(2)

def get_reporting_period(
        frequency="Daily", period=None, year=None, month=None,
        quarter=None, use_current_date=False, days_back=None):
    MONTH_DAYS = {1: 31, 3: 31, 4: 30, 5: 31, 6: 30, 7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}
    start_date = None
    end_date = None
    now = datetime.datetime.now()

    if use_current_date:
        start_date = datetime.date(now.year, now.month, now.day)
        return [], start_date, end_date
    if days_back:
        end_date = datetime.date(now.year, now.month, now.day)
        start_date = end_date - datetime.timedelta(days=days_back)
        return [], start_date, end_date

    if frequency == "Daily":
        if period:
            try:
                start_date = datetime.datetime.strptime(period, '%Y%m%d').date()
                end_date = start_date
                return [period], start_date, end_date
            except:
                start_date = datetime.date(now.year, now.month, now.day)
                end_date = start_date
        if not year:
            start_date =  datetime.date(now.year, now.month, 1)
            end_date =  datetime.date(now.year, now.month, now.day)
        elif year and year <= now.year:
            if year == now.year:
                if month:
                    month = month if month <= now.month else now.month
                    start_date =  datetime.date(year, month, 1)
                    if month == 2:
                        try:
                            end_date =  datetime.date(year, month, 28)
                        except:
                            end_date =  datetime.date(year, month, 29)
                    else:
                        end_date =  datetime.date(year, month, MONTH_DAYS[month])
                else:
                    start_date =  datetime.date(year, 1, 1)
                    end_date =  datetime.date(year, now.month, now.day)
            else:
                if month:
                    start_date =  datetime.date(year, month, 1)
                    if month == 2:
                        try:
                            end_date =  datetime.date(year, month, 28)
                        except:
                            end_date =  datetime.date(year, month, 29)
                    else:
                        end_date =  datetime.date(year, month, MONTH_DAYS[month])

                else:
                    start_date =  datetime.date(year, 1, 1)
                    end_date = datetime.date(year, 12, 31)

    if frequency == "Monthly":
        if period:
            return [period], None, None
        if not year:
            # use current year
            if not month:
                period = ["{0}{1:02d}".format(now.year, now.month) for m in range(1, now.month + 1)]
            else:
                if month <= now.month:
                    period = ["{0}{1:02d}".format(now.year, month)]
                else:
                    # period = ["{0}{1:02d}".format(now.year, now.month)]
                    period = []
        elif year and year <= now.year:
            if not month:
                if year == now.year:
                    period = ["{0}{1:02d}".format(year, m) for m in range(1, now.month + 1)]
                else:
                    period = ["{0}{1:02d}".format(year, m) for m in range(1, 13)]
            else:
                period = ["{0}{1:02d}".format(year, month)]
    if frequency == "Quarterly":
        if period:
            pass
        current_quarter =  ((now.month - 1) // 3) + 1
        if not year:
            if not quarter:
                period = ["{0}Q{1}".format(now.year, q) for q in range(1, current_quarter + 1)]
            else:
                if quarter <= current_quarter:
                    period = ["{0}Q{1}".format(now.year, quarter)]
                else:
                    period =[]

        elif year and  year <= now.year:
            if not quarter:
                if year == now.year:
                    period = ["{0}Q{1}".format(year, q) for q in range(1, current_quarter + 1)]
                else:
                    period = ["{0}Q{1}".format(year, q) for q in range(1, 5)]

            else:
                if quarter <= current_quarter:
                    period = ["{0}Q{1}".format(year, quarter)]
                else:
                    period = []
    if frequency == "Yearly":
        if period:
            pass
        if not year:
            period = [now.year]
        else:
            period = [year]
    if frequency == "FinancialJuly":
        pass

    return period, start_date, end_date


def read_from_dhis2(url, username, password):
    user_pass = '{0}:{1}'.format(username, password)
    coded = base64.b64encode(user_pass.encode())
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Basic ' + coded.decode()
    }

    response = requests.get(url, headers=headers, verify=False)
    return response


def get_start_and_end_date(year, month):
    start_month = datetime.datetime(year, month, 1)
    date_in_next_month = start_month + datetime.timedelta(35)
    start_next_month = datetime.datetime(date_in_next_month.year, date_in_next_month.month, 1)
    return start_month.strftime('%Y-%m-%d'), start_next_month.strftime('%Y-%m-%d')


def queue_in_dispatcher2(data, url=config['dispatcher2_queue_url'], ctype="json", params={}):
    user_pass = '{0}:{1}'.format(config['dispatcher2_username'], config['dispatcher2_password'])
    coded = base64.b64encode(user_pass.encode())
    if 'xml' in ctype:
        ct = 'text/xml'
    elif 'json' in ctype:
        ct = 'application/json'
    else:
        ct = 'text/plain'
    response = requests.post(
        url, data=data, headers={
            'Content-Type': ct,
            'Authorization': 'Basic ' + coded.decode()},
        verify=False, params=params  # , cert=config['dispatcher2_certkey_file']
    )
    return response


conn = psycopg2.connect(
    "dbname=" + dbconfig["db_name"] + " host= " + dbconfig["db_host"] + " port=" + dbconfig["db_port"] +
    " user=" + dbconfig["db_user"] + " password=" + dbconfig["db_passwd"])
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

cur.execute(
        "SELECT id, source, destination, source_config, destination_config, source_orgunits_synced "
        "FROM integration_pair WHERE is_active = TRUE")

integration_pairs = cur.fetchall()
print(len(integration_pairs))
for pair in integration_pairs:
    source_config = pair['source_config']
    destination_config = pair['source_config']

    print(source_config['api_url'])
    print(source_config['ous_synced'])
    print(destination_config['api_url'])
    # Read Districts From Source
    if not source_config['ous_synced']:
        ou_response = read_from_dhis2(
            "{0}/organisationUnits.json?level=3&paging=false&fields=id,displayName,path,level,"
            "parent[id,name]".format(source_config['api_url']),
            source_config['username'], source_config['password'])
        logging.log(logging.INFO, "Adding district organisation units")
        print("Adding district organisation units")
        for ou in ou_response.json()['organisationUnits']:
            # print(ou)
            cur.execute(
                "INSERT INTO orgunits(integration_pair_id, dhis2_id,dhis2_name,dhis2_path,dhis2_parent,dhis2_level) "
                "VALUES(%s, %s, %s, %s, %s, %s)",
                [pair['id'], ou['id'], ou['displayName'], ou['path'], ou['parent']['id'], ou['level']]
            )
            cur.execute(
                "UPDATE integration_pair SET source_config = jsonb_set(source_config, '{ous_synced}'::text[], 'true'::JSONB) "
                "WHERE id = %s", [pair['id']]
            )
            conn.commit()

        logging.log(logging.INFO, "Finished adding district organisation units")
        print("Finished adding district organisation units")

    # Read DataSets if not already done
    if not source_config['datasets_synced']:
        ds_response = read_from_dhis2(
            "{0}/dataSets.json?paging=false&fields=id,name,periodType".format(source_config['api_url']),
            source_config['username'], source_config['password'])
        logging.log(logging.INFO, "Adding our sync dataSets")
        print("Adding our sync dataSets")
        for ds in ds_response.json()['dataSets']:
            cur.execute(
                "INSERT INTO sync_datasets(integration_pair_id, dataset_id, dataset_name, reporting_frequency) "
                "VALUES(%s, %s, %s, %s)", [pair['id'], ds['id'], ds['name'], ds['periodType']]
            )
            cur.execute(
                "UPDATE integration_pair SET source_config = jsonb_set(source_config, '{datasets_synced}'::text[], 'true'::JSONB) "
                "WHERE id = %s", [pair['id']]
            )
            conn.commit()
        logging.log(logging.INFO, "Adding our sync dataSets")
        print("Finished adding our sync dataSets")

    # Now Let the integration begin
    # First, Get the active orgUnits for the integration pair
    cur.execute(
        "SELECT dhis2_name, dhis2_id FROM orgunits WHERE is_active = TRUE  "
        "AND integration_pair_id = %s", [pair['id']])
    data_orgunits = cur.fetchall()
    # Second, Get enabled DataSets
    cur.execute(
        "SELECT dataset_id, dataset_name, reporting_frequency, include_deleted "
        "FROM sync_datasets WHERE integration_pair_id = %s AND is_active = True", [pair['id']])
    sync_datasets = cur.fetchall()
    for dataset in sync_datasets:
        print("Gonna Sync dataSet: [{0}: {1}]".format(dataset['dataset_id'], dataset['dataset_name']))
        logging.log(logging.INFO, "Gonna Sync dataSet: [{0}: {1}]".format(dataset['dataset_id'], dataset['dataset_name']))

        reporting_frequency = dataset['reporting_frequency']
        reporting_periods = get_reporting_period(
            frequency=reporting_frequency, year=year, month=month, quarter=quarter,
            use_current_date=USE_CURRENT_DATE, days_back=days_back)

        if INCREMENTAL_INTEGRATION:  # The so-called continuous integration
            pass  # remove this and just add lastUpdated to last sync for each dataSet
        else:
            if reporting_frequency == 'Daily':
                print(reporting_periods)
            else:
                print(reporting_periods)
                for period in reporting_periods[0][:1]: # XXX rember to remove me
                    # loop through the active orgUnits for the integration pair and fetch, push for each
                    for orgunit in data_orgunits:
                        print("Processing data for period:{0}, orgUnit:{1}, dataSet:{2}".format(
                            period, orgunit['dhis2_name'], dataset['dataset_id']))
                        read_url = "{0}/dataValueSets.json?".format(source_config['api_url'])
                        ba_coded = base64.b64encode('{0}:{1}'.format(
                            source_config['username'], source_config['password']).encode())
                        # try:
                        with requests.get(
                                read_url,
                                headers={
                                    'Content-Type': 'application/json',
                                    'Authorization': 'Basic {0}'.format(ba_coded.decode())
                                },
                                params={
                                    'dataSet': dataset['dataset_id'],
                                    'orgUnit': orgunit['dhis2_id'],
                                    'period': period, 'children': True,
                                    'includeDeleted': dataset['include_deleted']
                                },
                                stream=True) as dv_response:
                            count = 0
                            total_values = 0
                            bulk_payload = {"dataValues": []}
                            try:
                                for d in json_stream.requests.load(dv_response)["dataValues"].persistent():
                                    count += 1
                                    total_values += 1
                                    # print(d)
                                    dv = {
                                        "dataElement": d["dataElement"],
                                        "period": d["period"],
                                        "orgUnit": d["orgUnit"],
                                        "categoryOptionCombo": d["categoryOptionCombo"],
                                        "attributeOptionCombo": d['attributeOptionCombo'],
                                        # "value": d["value"],
                                        # "storedBy": d["storedBy"],
                                        "created": d["created"],
                                        # "followup": d["followup"],
                                        "lastUpdated": d["lastUpdated"]
                                    }
                                    if 'value' in d:
                                        dv['value'] = d["value"]
                                    if 'storedBy' in d:
                                        dv['storedBy'] = d['storedBy']
                                    if 'followup' in d:
                                        dv['followup'] = d['followup']
                                    bulk_payload["dataValues"].append(dv)
                                    # print(
                                    #     dv['dataElement'], dv['orgUnit'], dv['lastUpdated'],
                                    #     dv['value'], dv['created'], dv['period'],
                                    #     dv['attributeOptionCombo'], dv['categoryOptionCombo'])
                                    if count > 15:
                                        print(bulk_payload)
                                        extra_params = {
                                            'year': year,
                                            'month': month,
                                            'source': pair['source'],
                                            'destination': pair['destination'],
                                            'facility': orgunit['dhis2_name'],
                                            'is_qparams': "f",
                                            'report_type': '{0}'.format(dataset['dataset_id'])
                                        }
                                        try:
                                            queue_in_dispatcher2(json.dumps(bulk_payload), ctype="json", params=extra_params)
                                        except:
                                            time.sleep(4)
                                            print("Failed to queue for: ", period)
                                        bulk_payload = {"dataValues": []}
                                        count = 0
                                        # break
                            except Exception as e:
                                print("Failed to stream datavalues for:{0} period:{1}, Error: {2}".format(orgunit, period, str(e)))

                        print("Finished fetching data for period:{0}, orgUnit:{1}, dataSet:{2}, Total Values: {3}".format(
                            period, orgunit['dhis2_name'], dataset['dataset_id'], total_values))
                        # except Exception as e:
                        #     print("Failed to read data from source DHIS2. Reason: ", str(e))
        print("Finised processing for dataSet:", dataset['dataset_name'], dataset['dataset_id'])

conn.close()
