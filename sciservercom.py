"""SciServer Compute and CasJobs Interface

This library provides a Python interface to interact with the SciServer Compute and CasJobs services.
It allows users to submit queries, download tables, and monitor job status.
The library utilizes the SciServer and CasJobs modules that can be downloaded here:
https://github.com/sciserver/SciScript-Python

Author: Marco Parisi
License: MIT License

Dependencies:
    - SciServer (custom module)
    - CasJobs (custom module)
    - Authentication (custom module)
    - dotenv
    - pandas
    
Usage:
    1. Instantiate the SciServer class, optionally enabling or disabling features.
    2. Login using loginFromEnv() to authenticate with SciServer.
    3. Submit queries using queryRequest(), specifying the query, table name, and context.
    4. Download tables using downloadTable(), providing the query and context.
    5. Monitor job status using the jobDescriber() method (accessed through the jobDescription attribute).

Example:
    from SciServer import SciServer
    
    # Initialize the SciServer object
    sciserver = SciServer(isEnabled=True)
    
    # Login using environment variables
    sciserver.loginFromEnv()
    
    # Submit a query
    query = "SELECT * FROM some_table LIMIT 10"
    table_name = "my_table"
    query_context = "MyContext"
    sciserver.queryRequest(query, table_name, query_context)
    
    # Download a table
    #downloaded_data = sciserver.downloadTable("SELECT * FROM my_table")
    #print(downloaded_data)
"""

# Communicate between SciServer Compute and CasJobs
from SciServer import CasJobs     
from SciServer import Authentication
import os
import dotenv       
import pandas
import warnings

class SciServer:
    def __init__(self, isEnabled=False):
        # datasets can be very big, flag must be set True manually
        # to avoid new download on kernel restart.
        self.isEnabled = isEnabled 

    def loginFromEnv(self, env_path=None):
        dotenv_loaded = dotenv.load_dotenv(override=True, dotenv_path=env_path)
        
        if dotenv_loaded:
            print('SciServer: Get login name and password from env')
            loginName = os.getenv("SciServerUsername")
            loginPassword = os.getenv("SciServerPassword")
            
            self.autotoken = Authentication.login(loginName, loginPassword)
            
            if (self.autotoken):
                print("SciServer: Login successful with token: {0:}".format(self.autotoken))
            else:
                print("SciServer: ERROR: login failed")
        else:
            print('SciServer: dotenv not found')

    
    def jobDescriber(self):
        # Prints the results of the CasJobs job status functions in a human-readable manner
        # Input: the python dictionary returned by getJobStatus(jobId) or waitForJob(jobId)
        # Output: prints the dictionary to screen with readable formatting
        if (self.jobDescription["Status"] == 0):
            status_word = 'Ready'
        elif (self.jobDescription["Status"] == 1):
            status_word = 'Started'
        elif (self.jobDescription["Status"] == 2):
            status_word = 'Cancelling'
        elif (self.jobDescription["Status"] == 3):
            status_word = 'Cancelled'
        elif (self.jobDescription["Status"] == 4):
            status_word = 'Failed'
        elif (self.jobDescription["Status"] == 5):
            status_word = 'Finished'
        else:
            status_word = 'Status Unknown'
    
        print('JobID: ', self.jobDescription['JobID'])
        print('Status: ', status_word, ' (', self.jobDescription["Status"],')')
        print('Target (context being searched): ', self.jobDescription['Target'])
        print('Message: ', self.jobDescription['Message'])
        print('Created_Table: ', self.jobDescription['Created_Table'])
        print('Rows: ', self.jobDescription['Rows'])
        wait = pandas.to_datetime(self.jobDescription['TimeStart']) - pandas.to_datetime(self.jobDescription['TimeSubmit'])
        duration = pandas.to_datetime(self.jobDescription['TimeEnd']) - pandas.to_datetime(self.jobDescription['TimeStart'])
        print('Wait time: ',wait.seconds,' seconds')
        print('Query duration: ',duration.seconds, 'seconds')


    def queryRequest(self, main_query, my_table_name, query_context, mydb_context="MyDB"):
        if self.isEnabled:
            try:
                CasJobs.executeQuery(sql=f"""DROP TABLE {my_table_name}""", context=mydb_context)
            except Exception as e:
                print(e)
            
            with warnings.catch_warnings():
              warnings.simplefilter("ignore")
              jobid = CasJobs.submitJob(sql=main_query, context=query_context)
            
            print(f'\nJob submitted with jobId: {jobid} for table: {my_table_name}')
            
            waited = CasJobs.waitForJob(jobId=jobid)     
            self.jobDescription = CasJobs.getJobStatus(jobid)
            
            print('\nSciServer: Information about the job:')
            
            self.jobDescriber()
            
            tables = CasJobs.getTables(context=mydb_context) 
            print('\nTables in :\n',tables)
        else:
            print('SciServer: Query requests are disabled to prevent accidental drops.')

    
    def downloadTable(self, query, context='MyDB'):
        if self.isEnabled:
            return CasJobs.getPandasDataFrameFromQuery(f"""{query}""", context=context)
        else:
            print('SciServer: Query requests are disabled to prevent accidental drops.')
