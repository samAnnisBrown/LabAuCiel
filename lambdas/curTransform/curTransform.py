import re           # For all the regex
import boto3        # To interact with AWS
import argparse     # For all the aguments
import sys          # To exit the script when things go wrong or are finished
import operator     # For some sorting stuff
import csv          # To deal with the CUR CSVs

from gzip import GzipFile       # So we can gunzip stuff
from io import BytesIO          # Stream bytes from S3

# <--------------------- ARGUEMENT HANDLING --------------------->
parser = argparse.ArgumentParser(description="This scrupts uploads CUR data to and manipulate Elasticsearch indices")
# Auto uploading of CUR data for specific customers
parser.add_argument('-c', '--customer',
                    help='Customer - i.e. RMIT, Sportsbet')
parser.add_argument('-lm', '--list_months',
                    action='store_true',
                    help='Lists the available months for import in the customer\'s folder.  Use with the --customer parameter.')
parser.add_argument('-m', '--minus_month',
                    type=int,
                    default=0,
                    help='By default, the current month with be imported.  Use this flag to import previous months, which can be seen with the --list-months flag.  Integer represents number of months in the past (i.e. 1 = last month, 2 = 2 months ago, etc).  Use with the --customer parameter.')
# Ad-hoc uploading of CUR data
parser.add_argument('-r', '--report_name',
                    default='QuickSight_Red')
parser.add_argument('-db', '--athena_database_name',
                    default='cost_usage_reports')
parser.add_argument('--role_arn',
                    help='If using STS auth, the ARN of the role to be assumed.')
parser.add_argument('--from_bucket',
                    help='The S3 Bucket that contains the import CUR .csv.gz file.')
parser.add_argument('--to_bucket',
                    help='The S3 Bucket that contains the import CUR .csv.gz file.')
parser.add_argument('--key',
                    help='The S3 Bucket that contains the import CUR .csv.gz file.')
args = parser.parse_args()


def lambdaHandler(event):
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]
    fileName = re.search(".+/(.+)\.", key).group(1)
    yearMonth = re.search(".+/(\d+)-\d+/.+", key).group(1)

    curCsv = getExtractedCurFile(bucket, key)
    transformToS3(curCsv, fileName, yearMonth)
    updateAthena(curCsv)


# <--------------------- ATHENA LOGIC --------------------->
def updateAthena(curFile):
    columnList = list(csv.reader([curFile.splitlines()[0]]))[0]  # need to encapsulate/decapsulate list for csv.reader to work

    tableStructure = returnColumnTypes(columnList)

    athena = returnClientAuth('athena', False)

    createDatabase = 'CREATE DATABASE IF NOT EXISTS %s' % (args.athena_database_name)
    athena.start_query_execution(
        QueryString=createDatabase,
        ResultConfiguration={
            'OutputLocation': 's3://' + args.to_bucket + '/query_output',
        }
    )

    create_table = \
        """CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s (
        %s
     )
     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
     WITH SERDEPROPERTIES (
      'separatorChar' = ','
     ) LOCATION '%s'
     TBLPROPERTIES ('has_encrypted_data'='false');""" % (args.athena_database_name, args.from_bucket.replace("-", "_"), tableStructure[:-2], 's3://' + args.to_bucket + '/' + args.from_bucket)

    athena.start_query_execution(
        QueryString=create_table,
        QueryExecutionContext={
            'Database': args.athena_database_name
        },
        ResultConfiguration={
            'OutputLocation': 's3://' + args.to_bucket + '/query_output',
        }
    )


def uploadCurForAthena():
    print("")


# <--------------------- AUTH --------------------->
def returnClientAuth(service, assumeRole):
    if args.role_arn is not None and assumeRole:
        client = boto3.client(service)
        assumed_role = client.assume_role(
            RoleArn=args.role_arn,
            RoleSessionName='cur_temp_sts_session'
        )

        creds = assumed_role['Credentials']

        clientAuth = boto3.client(service,
                          region_name='ap-southeast-2',
                          aws_access_key_id=creds['AccessKeyId'],
                          aws_secret_access_key=creds['SecretAccessKey'],
                          aws_session_token=creds['SessionToken'], )
    else:
        clientAuth = boto3.client(service, region_name='ap-southeast-2')

    return clientAuth


# <--------------------- S3 HANDLING --------------------->
def getExtractedCurFile(bucket, key):
    # Download S3 file
    s3 = returnClientAuth('s3', True)
    print('Downloading \"' + bucket + '/' + key + '\" from S3')
    s3file = s3.get_object(Bucket=bucket, Key=key)

    # Unzip into memory
    print('Unzipping into memory...')
    bytestream = BytesIO(s3file['Body'].read())
    outfile = GzipFile(None, 'rb', fileobj=bytestream).read().decode('utf-8')

    return outfile


def transformToS3(curFile, fileName, yearMonth):
    s3 = returnClientAuth('s3', False)
    #  Remove first line by counting it's length, then adding 1 to remove the character return
    truncateLength = len(curFile.split('\n')[0]) + 1
    # Put the object in S3
    uploadKey = args.from_bucket + '/' + yearMonth[0:4] + '/' + yearMonth[4:6] + '/' + fileName
    print('Uploading unzipped and transformed CSV to ' + uploadKey)
    s3.put_object(Bucket=args.to_bucket, Key=uploadKey, Body=curFile[truncateLength:])
    return uploadKey


# <--------------------- OTHER STUFF --------------------->
def returnColumnTypes(columnList):
    tableStructure = ""
    for value in columnList:
        # DATE
        if 'Date' in value:
            tableStructure += '`' + value.replace("/", "_") + '`' + ' Date,\n'
        # INT
        elif 'engine' in value \
                or 'Iopsvol' in value \
                or 'vcpu' in value \
                or 'UnitsPerReservation' in value \
                or 'TotalReservedUnits' in value:
            tableStructure += '`' + value.replace("/", "_") + '`' + ' INT,\n'
        # FLOAT
        elif 'UsageAmount' in value \
                or 'lended' in value \
                or 'SizeFactor' in value \
                or 'Amortized' in value \
                or 'Unused' in value \
                or 'EffectiveCost' in value \
                or 'TotalReserved' in value \
                or 'NormalizedUnitsPerReservation' in value \
                or 'NumberOfReservations' in value \
                or 'UnitsPerReservation' in value \
                or 'UpfrontValue' in value \
                or 'OnDemand' in value:
            tableStructure += '`' + value.replace("/", "_") + '`' + ' FLOAT,\n'
        # STRING
        else:
            tableStructure += '`' + value.replace("/", "_") + '`' + ' STRING,\n'

    return tableStructure

# <--------------------- MANUAL LAUNCH --------------------->
def manualLaunch():  # If not in a Lambda, launch main function and pass S3 event JSON
    curFiles = getLatestCurByMonth()
    for key in curFiles:
        lambdaHandler({
            "Records": [
                {
                    "s3": {
                        "bucket": {
                            "name": args.from_bucket,
                        },
                        "object": {
                            "key": key,
                        }
                    }
                }
            ]
        })


def getLatestCurByMonth():
    # Create dit/list
    s3ObjectList = []   # Initialise a list to hold all S3 objects in our defined bucket
    allCurFiles = {}    # Initialise a tuple to hold only the S3 objects that are CUR files in the bucket
    listOfMonths = []   # Initialise a list that will contain all available months for the report

    # Get S3 Auth and list objects
    s3 = returnClientAuth('s3', True)
    s3ApiOutput = s3.list_objects_v2(Bucket=args.from_bucket)

    # While S3 is still returning truncated, keep appending to the s3ObjectList
    while s3ApiOutput['IsTruncated']:
        s3ObjectList.append(s3ApiOutput)
        s3ApiOutput = s3.list_objects_v2(Bucket=args.bucket, ContinuationToken=s3ApiOutput['NextContinuationToken'])
    s3ObjectList.append(s3ApiOutput)    # Append whatever's remaining to the s3ObjectList

    # Let's populate our tuple so that it only contains CUR files (csv.gz), and our listOfMonths
    for j in s3ObjectList:  # Each ApiOutput is a separate item in the list
        for k in j['Contents']: # With each item having contents from returned call
            if 'csv.gz' in k['Key'] and args.report_name in k['Key']:
                allCurFiles[k['Key']] = k['LastModified'].isoformat()
                # Create a list of all the different folders (i.e. months) where we might want to find the 'latest'
                try:
                    searchKey = re.search("(.+/\d+-\d+)/", k['Key']).group(1)
                    if searchKey not in listOfMonths:
                        listOfMonths.append(searchKey)
                except AttributeError:
                    searchKey = None

    # Allow user to only list months then exit if -lm flag is enabled
    if args.list_months:
        for report in listOfMonths:
            print(report)
        sys.exit()

    # Remove all keys that don't belong to the appropriate month
    keysToDelete = [k for k, v in allCurFiles.items() if sorted(listOfMonths, reverse=True)[args.minus_month] not in k]
    for key in keysToDelete:
        del allCurFiles[key]

    # Find the file with the latest date, then extract the name of the folder in which it resides
    sortedCurFiles = sorted(allCurFiles.items(), key=operator.itemgetter(1), reverse=True)
    curDate = sortedCurFiles[0][1]
    folderHash = re.search(".*/(.+-.+-.+-.+)/.+", sortedCurFiles[0][0]).group(1)

    # And finally, output grab the names of the latest CUR files
    latestCurFiles = []
    for j in s3ObjectList:
        for k in j['Contents']:
            if 'csv.gz' in k['Key'] and folderHash in k['Key']:
                latestCurFiles.append(k['Key'])
    print("Grabbing the following files that were created @ " + curDate + "\n")
    for file in latestCurFiles:
        print("- " + file)
    print("")
    return [sortedCurFiles[0][0]]


# launchImport(getLatestCurByMonth())
manualLaunch()

