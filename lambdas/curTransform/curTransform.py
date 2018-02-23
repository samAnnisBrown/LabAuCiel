import re           # For all the regex
import boto3        # To interact with AWS
import argparse     # For all the aguments
import sys          # To exit the script when things go wrong or are finished
import operator     # For some sorting stuff
import csv          # To deal with the CUR CSVs

from gzip import GzipFile       # So we can gunzip stuff
from io import BytesIO          # Stream bytes from S3
from time import sleep

# <--------------------- ARGUEMENT HANDLING --------------------->
parser = argparse.ArgumentParser(description="This scrupts uploads CUR data to and manipulate Elasticsearch indices")
# Auto uploading of CUR data for specific customers
parser.add_argument('-c', '--customer',
                    help='Customer - i.e. RMIT, Sportsbet')
parser.add_argument('-l', '--list_months',
                    action='store_true',
                    help='Lists the available months for import in the customer\'s folder.  Use with the --customer parameter.')
parser.add_argument('-m', '--minus_month',
                    type=int,
                    default=0,
                    help='By default, the current month with be imported.  Use this flag to import previous months, which can be seen with the --list-months flag.  Integer represents number of months in the past (i.e. 1 = last month, 2 = 2 months ago, etc).  Use with the --customer parameter.')

# Ad-hoc uploading of CUR data
parser.add_argument('-db', '--athena_database_name',
                    default='aws_cost_analysis')
parser.add_argument('-t', '--athena_table_name',
                    default='cur_table')

# Main Params
parser.add_argument('-f', '--folder_filter',
                    default='QuickSight_Red')
parser.add_argument('--role_arn',
                    help='If using STS auth, the ARN of the role to be assumed.')
parser.add_argument('--from_bucket',
                    help='The S3 Bucket that contains the import CUR .csv.gz file.')
parser.add_argument('--to_bucket',
                    default='ansamual-athena',
                    help='The S3 Bucket that contains the import CUR .csv.gz file.')
args = parser.parse_args()

try:
    if args.customer.lower() == 'rmit':
        args.role_arn = 'arn:aws:iam::182132151869:role/AWSEnterpriseSupportCURAccess'
        args.from_bucket = 'rmit-billing-reports'
        args.folder_filter = 'CUR/Hourly'
    elif args.customer.lower() == 'ansamual':
        args.from_bucket = 'ansamual-costreports'
        args.folder_filter = 'QuickSight_RedShift_CostReports'
    elif args.customer.lower() == 'sportsbet':
        args.role_arn = 'arn:aws:iam::794026524096:role/awsEnterpriseSupportCURAccess'
        args.from_bucket = 'sportsbet-billing-data'
        args.folder_filter = 'hourly'
    else:
        print('Customer \'' + args.customer.lower() + '\' unknown.  Exiting...')
        sys.exit()
except AttributeError:
    customerImport = False


# <--------------------- MAIN IMPORT/LAMBDA FUNCTION --------------------->
def curToAthena(event):
    # Generate Variables
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]
    fileName = re.search(".+/(.+)\.", key).group(1)
    yearMonth = re.search(".+/(\d+)-\d+/.+", key).group(1)

    # Complete Transform
    curCsv = getExtractedCurFile(bucket, key)
    transformToS3(curCsv, fileName, yearMonth)
    updateAthena(curCsv)


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
    print('Transforming CUR file...')
    curFileTransformed = []
    # Let's read line by line
    for line in curFile.splitlines()[1:]:
        # Isolate lines with quotes in them
        if '"' in line:
            # Split on quotes
            lineAsList = line.split('"')
            # If index odd, the item is between quotes, so replace all commas with escaped commas
            for i, part in enumerate(lineAsList):
                # Replace on odds only
                if i % 2 != 0:
                    lineAsList[i] = part.replace(",", "|,")
            # Rejoin line as string
            line = ''.join(lineAsList)
        # Append to transformed file
        curFileTransformed.append(line)
    # Update curFile with transformed content
    curFile = '\n'.join(curFileTransformed)

    # Put the object in S3
    uploadKey = args.from_bucket + '/year=' + yearMonth[0:4] + '/month=' + yearMonth[4:6] + '/' + fileName
    print('Uploading unzipped and transformed CSV to ' + uploadKey.lower())
    s3.put_object(Bucket=args.to_bucket, Key=uploadKey.lower(), Body=curFile)
    return uploadKey


# <--------------------- ATHENA LOGIC --------------------->
def updateAthena(curFile):
    athena = returnClientAuth('athena', False)

    createDatabase = 'CREATE DATABASE IF NOT EXISTS %s' % (args.athena_database_name)
    athena.start_query_execution(
        QueryString=createDatabase,
        ResultConfiguration={
            'OutputLocation': 's3://' + args.to_bucket + '/query_output',
        }
    )

    columnList = list(csv.reader([curFile.splitlines()[0]]))[0]  # need to encapsulate/decapsulate list for csv.reader to work

    dbName = args.athena_database_name
    tableName = args.from_bucket.replace("-", "_")
    tableFields = returnColumnTypes(columnList)
    s3Location = 's3://' + args.to_bucket + '/' + args.from_bucket

    create_table = \
        """CREATE EXTERNAL TABLE IF NOT EXISTS %s."%s" (
        %s
     )
     PARTITIONED BY (year string, month string)
     ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ESCAPED BY '|' LINES TERMINATED BY '\n'
     LOCATION '%s'
     TBLPROPERTIES (
     'has_encrypted_data'='false',
     'serialization.null.format'='',
     'timestamp.formats'="yyyy-MM-dd'T'HH:mm:ss'Z'");""" % (dbName, tableName, tableFields, s3Location)

    update_partitioning = """MSCK REPAIR TABLE %s.%s;""" % (dbName, tableName)

    print("Updating Athena " + dbName + '.' + tableName)
    queries = [create_table, update_partitioning]
    for query in queries:
        athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': args.athena_database_name
            },
            ResultConfiguration={
                'OutputLocation': 's3://' + args.to_bucket + '/query_output',
            }
        )
        sleep(2)


# <--------------------- OTHER STUFF --------------------->
def returnColumnTypes(columnList):
    tableStructure = ""
    for value in columnList:
        if 'Date' in value:
            tableStructure += '`' + value.replace("/", "_") + '`' + ' timestamp,\n'
        elif 'engine' in value \
                or 'Iopsvol' in value \
                or 'vcpu' in value \
                or 'UnitsPerReservation' in value \
                or 'TotalReservedUnits' in value:
            tableStructure += '`' + value.replace("/", "_") + '`' + ' INT,\n'
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
        else:
            tableStructure += '`' + value.replace("/", "_") + '`' + ' STRING,\n'

    return tableStructure[:-2]


# <--------------------- AUTH --------------------->
def returnClientAuth(service, assumeRole):
    if args.role_arn is not None and assumeRole:
        client = boto3.client('sts')
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


# <--------------------- GETTING OLDER REPORTS --------------------->
def getLatestCurByMonth(listMonths=args.list_months):
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
        s3ApiOutput = s3.list_objects_v2(Bucket=args.from_bucket, ContinuationToken=s3ApiOutput['NextContinuationToken'])
    s3ObjectList.append(s3ApiOutput)    # Append whatever's remaining to the s3ObjectList

    # Let's populate our tuple so that it only contains CUR files (csv.gz), and our listOfMonths
    for j in s3ObjectList:  # Each ApiOutput is a separate item in the list
        for k in j['Contents']: # With each item having contents from returned call
            if 'csv.gz' in k['Key'] and args.folder_filter in k['Key']:
                allCurFiles[k['Key']] = k['LastModified'].isoformat()
                # Create a list of all the different folders (i.e. months) where we might want to find the 'latest'
                try:
                    searchKey = re.search("(.+/\d+-\d+)/", k['Key']).group(1)
                    if searchKey not in listOfMonths:
                        listOfMonths.append(searchKey)
                except AttributeError:
                    searchKey = None

    # Allow user to only list months then exit if -lm flag is enabled
    if listMonths:
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

    return latestCurFiles


# <--------------------- MANUAL LAUNCH --------------------->
def manualLaunch():  # If not in a Lambda, launch main function and pass S3 event JSON
    curFiles = getLatestCurByMonth()
    for key in curFiles:
        curToAthena({
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


manualLaunch()

