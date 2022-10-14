# d2integrator
A Python script for integrating DHIS2 instances by reading data from one to another.

## How it works?
The script works by maintaining pairs of DHSI2 instances that you want to integrate. For each instance,
we keep a configurations for the API url, personal authentication tokes or the username and password used for reading or sending data, among others.


- For each integration pair, we synchronise organisation units at the level which we want to pull data. We typically pick a higher level in 
the hierarchy so that data for organisation units lower in the hierarchy can be accessed through the
**children=true** parameter passed to the */dataValueSets* api.

- The script also automatically synchronises all data sets in the source instance, so that we can iterate through all of them during data synchronisation. 
What is most interesting about it is the ability to stream JSON data as it is read from the source DHIS2. In other words, we do not have to wait for the entire JSON file to download before we can synchronise data.

- Data can be read from the source instance based on various arguments passed to the script.

## Deployment
First, you will have to clone the repository for the integration script
```bash
$ git clone https://github.com/sekiskylink/d2integrator
$ cd d2integrator
```
In the directory for the codebase there is an SQL file to run on database

1. `integrator.sql` has the schema to run after you have created the app's database
    ```bash
        $ createdb -U postgres d2integrator
        $ psql -U postgres -f ./integrator.sql -d d2integrator
    ```

In the same directory, you have a Python script `integrator.py` that does the moving of the data from on DHIS 2 instance to another optionally through a 
data exchange middleware.

The dependencies of the script include:

1. [requests](https://docs.python-requests.org/en/latest/)
2. [psycopg2](https://pypi.org/project/psycopg2/)
3. [json-stream](https://pypi.org/project/json-stream/)

It is preferable to install the above dependencies in a virtual environment.
```bash
        $ python3 -n venv integrator 
        (integrator)$ pip install requests psycopg2 json-stream
```

Here is how to use the script
```bash
    $ python integrator.py -h
```
This will show you this help message

```
A script to synchronize data from one DHIS2 instance to another
optionally via dispatcher2 or another data exhange middleware.
    
Usage: python integrator.py [-i] [-d ] [-c ] [-y <year>] [-m <month>] [-p <period>] [-n <days>] [-l <district_list>]

-d Direct synchronisation without use of data exchange middleware.
-c --current_date Whether to generate values only for the date when script is run.
-y --year   The year for which to read and push values.
-m --month  The month for which to read values for pushing to another instance.
-q --quarter   The quarter for which to read values for pushing to another instance.
-n --days_back Read and send values pre dating n days.
-p --period The DHIS 2 period used for pulling data from source instance. Format YYYYMMDD
-l --district_list   A string of comma-separated district names
-i --incremental  Whether to do incremental/continuous synchronisation based on last successful sync.
-h --help This message. 
```

An example run to read and send values for the Year `2022` and Quarter `1` that is to say 2022Q1 would be:
```bash
    $ python integrator.py -y 2022 -q 1
```

The script can be used to run the backlog by passing the different year, month, quarter combinations, otherwise, it can be scheduled to read/send data continuously based on when it was last successfully run.

- The script can be long-running since some data sets can have a lot of data. It is advisable to background the script like so;
```bash
    $ nohup python integrator.py -y 2022 -q 1 > /tmp/integrator.log &
```
By backgrounding the script and redirecting output to a file, you can easily return to the file to inspect errors if any.