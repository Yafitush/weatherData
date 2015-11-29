# coding=utf-8

__author__ = 'Yafit'
import urllib2
import urllib
import os.path
import csv
import time
import datetime
import linecache
from bs4 import BeautifulSoup
import inspect
from datetime import date, timedelta
import sys
import ssl
import MySQLdb
import pytz
import lxml

reload(sys)
sys.setdefaultencoding('utf-8')
path = '/opt/skyspark-2.1.12/db/newnewnew/traklin/'
mysqlDir = '/var/lib/mysql/'
user_agent = 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)'
ssl.PROTOCOL_SSLv23 = ssl.PROTOCOL_TLSv1
utc = pytz.utc
tz = pytz.timezone('Asia/Jerusalem')


def PrintFrame():
    callerframerecord = inspect.stack()[1]  # 0 represents this line           # 1 represents line at caller
    frame = callerframerecord[0]
    info = inspect.getframeinfo(frame)
    print info.filename  # __FILE__     -> Test.py
    print info.function  # __FUNCTION__ -> Main
    print info.lineno  # __LINE__     -> 13


def readCredentialsFile():
    """
    Function reads from credantials.csv file and put the data:client name, username, password and t.z  in traklin_sites_credentials
    input: credDir- the path to the 'credentials.csv'
    :return:traklin_sites_credentials
    """
    with open(path + 'credentials.csv', 'rt') as f:
        traklin_sites_credentials = []
        reader = csv.DictReader(f)
        for row in reader:
            traklin_sites_credentials.append(row)
    return traklin_sites_credentials


def parseCredentials(traklin_site_credentials):
    """
    The function get the site credentials and parse it into cred
    :param traklin_site_credentials:
    :return: cred
    """
    PrintFrame()
    cred = {'client_name': traklin_site_credentials.get('name'), 'username': traklin_site_credentials.get('username'),
            'password': traklin_site_credentials.get('password'), 'tz': traklin_site_credentials.get('tz')}
    if len(cred["tz"]) < 9:
        cred["tz"] = '0' + str(cred["tz"])  # adding leading '0'
    return cred


def getkey(credentials):
    """
    The function get a dict which contains the site's credentials and makes a request to traklin
     and returns the key
    :param credentials: the client'e name, username, password and t.z
    :return: key
    """
    PrintFrame()
    url_to_app = 'https://srvt1.i-ecnet.co.il/magic94Scripts/mgrqispi94.dll#'
    v_user = credentials.get("username")
    v_password = credentials.get("password")
    v_tz = credentials.get("tz")

    data = {'APPNAME': 'traklin',
            'PRGNAME': 'Identification',
            'ARGUMENTS': 'v_user,v_password,v_taz',
            'v_user': v_user,
            'v_password': v_password,
            'v_taz': v_tz}

    header = {'User-Agent': user_agent,
              'content-type': 'application/x-www-form-urlencoded',
              'charset': 'utf-8'}

    req = urllib2.Request(url_to_app, urllib.urlencode(data), header)
    try:
        response = urllib2.urlopen(req)
    except urllib2.URLError as e:
        if hasattr(e, 'reason'):
            print 'We failed to reach a server.'
            print 'Reason: ', e.reason
        elif hasattr(e, 'code'):
            print 'The server couldn\'t fulfill the request.'
            print 'Error code: ', e.code
    else:
        soup = BeautifulSoup(response.read(), 'html.parser')
        result = soup.find_all('a')[-1]
        result = str(result).split('&amp')[2]
        result = str(result).split(';')[1]
        result = str(result).split('\"')[0]
        return result


def get_lists(key, client_name):
    """
    The function build index_raw.csv which contains the sites numbers, names and mone numbers.
    :param key:
    :param client_name: is needed for the path of files
    :return:
    """
    PrintFrame()
    if os.path.isfile(path + client_name + '/index_raw.csv'):
        with open(path + client_name + '/index_raw.csv', 'r') as inFile:
            csv.reader(inFile, delimiter=',', quotechar='"')
            sitekeys = []
            sitevalues = []
            mone_list = []
            for row in inFile:
                row = row.split(',')
                sitekeys.append(row[2])
                sitevalues.append(row[1])
                mone_list.append(row[0])
    else:
        print "get_list- else"
        outFile = open(path + client_name + '/index_raw.csv', 'w+')
        outwriter = csv.writer(outFile)
        sites = getsites(key)
        print "returned from getsites"
        for sitekeys, sitevalues in sites.items():
            mone_list = getmonim(sitekeys, key)
            print(sitekeys)
            print(sitevalues)
            if mone_list.__len__() > 0:
                for mone in mone_list:
                    outwriter.writerow([mone.encode("UTF-8"), '\"' + sitevalues.encode("UTF-8").replace('"', '') + '\"',
                                        sitekeys.encode("UTF-8")])
            else:
                outwriter.writerow(
                    ['-1', '\"' + sitevalues.encode("UTF-8").replace('"', '') + '\"', sitekeys.encode("UTF-8")])
        outFile.close()
        print "get list end else"
    index_cleaner('index_raw.csv', client_name)
    return 0


def getsites(key):
    """
    The function makes a request to traklin and gets  a list of sites numbers and names
    :param key:
    :return:  a tuple of of sites numbers and names
    """
    PrintFrame()
    url_to_app = 'https://srvt1.i-ecnet.co.il/magic94Scripts/mgrqispi94.dll'

    data = {'APPNAME': 'traklin',
            'PRGNAME': 'HozeRepByDate',
            'ARGUMENTS': key.split('=')[1] + ',-N6'}

    header = {'User-Agent': user_agent,
              'content-type': 'application/x-www-form-urlencoded',
              'charset': 'utf-8'}
    sites_list = []
    sites_tuple = {}

    req = urllib2.Request(url_to_app, urllib.urlencode(data), header)
    try:
        response = urllib2.urlopen(req)
    except urllib2.URLError as e:
        if hasattr(e, 'reason'):
            print 'We failed to reach a server.'
            print 'Reason: ', e.reason
        elif hasattr(e, 'code'):
            print 'The server couldn\'t fulfill the request.'
            print 'Error code: ', e.code
    else:
        print "get sites- fine"
        soup = BeautifulSoup(response.read(), 'html.parser')
        for site in soup.find_all('option'):
            sites_list.append(site.string)
        for stringer in sites_list[1:]:
            stringer = stringer.split('-')
            sites_tuple[stringer[0]] = stringer[1]
        print "end get sites - else "
    return sites_tuple


def getmonim(sitekey, key):
    """
    The function makes a request by the sitekey and returns it's mone number
    :param sitekey:
    :param key:
    :return: mone number of sitekey
    """
    PrintFrame()
    url_to_app = 'https://srvt1.i-ecnet.co.il/magic94Scripts/mgrqispi94.dll'
    print "getting mone for site" + sitekey
    data = {'APPNAME': 'traklin',
            'PRGNAME': 'MoneRepByDate',
            'ARGUMENTS': key.split('=')[1] + ',atar',
            'year2Value': 'true',
            'type': 'hoze',
            'atar': sitekey}

    header = {'User-Agent': user_agent,
              'content-type': 'application/x-www-form-urlencoded',
              'charset': 'utf-8'}

    mone_list = []
    req = urllib2.Request(url_to_app, urllib.urlencode(data), header)
    try:
        response = urllib2.urlopen(req)
    except urllib2.URLError as e:
        if hasattr(e, 'reason'):
            print 'We failed to reach a server.'
            print 'Reason: ', e.reason
        elif hasattr(e, 'code'):
            print 'The server couldn\'t fulfill the request.'
            print 'Error code: ', e.code
    else:
        soup = BeautifulSoup(response.read(), 'html.parser')
        for mone in soup.find_all('option'):
            mone_list.append(mone['value'])
    return mone_list


def index_cleaner(rawfile, client_name):
    """
    The function goes over index_raw.csv and creates indx.csv which contains only the relevant rows from index_raw.csv
    :param rawfile:
    :param client_name:
    :return:
    """
    PrintFrame()
    print "index cleaner"
    with open(path + client_name + '/' + rawfile, 'r') as inFile:
        print "opened index_raw!!!"
        outfile = open(path + client_name + '/index.csv', 'w+')
        reader = csv.reader(inFile, delimiter=',', quotechar="\"")
        for row in reader:
            if int(row[0]) > 3:
                outfile.write(', '.join(row) + '\n')
    inFile.close()


def writeMeterCsv(key, client_name):
    """
    The function asks 15-min data for each site, saves the data in mone number.csv file and saves the last date
     that date was saved in lastRead+mone number.csv
    :param key:
    :param client_name:
    :return:
    """
    PrintFrame()
    with open(path + client_name + '/index.csv', 'r') as inFile:
        reader = csv.reader(inFile, delimiter=',', quotechar='"')
        for row in reader:
            if os.path.isfile(row[0].strip() + '.csv'):
                print 'skip to next'
            else:
                meterkey = row[0].strip()  # row[0]= monekey
                print "meterkey " +str(meterkey)
                dates = gettingDates(meterkey, client_name)

                file_desc = open(path + client_name + '/moneFiles/' + meterkey + '.csv', 'w')
                writer_file_desc= csv.writer(file_desc)
               # with csv.writer(open(path + client_name + '/moneFiles/' + meterkey + '.csv', 'w')) as writer_file_desc:
                iterativeGetEnergyMeter(key, client_name, row[-1].strip(), row[0].strip(), dates, writer_file_desc)
                file_desc.close()

                saveDatesFile = open(path + client_name + '/LastReadFiles/lastRead' + meterkey + '.csv', "ab+")
                writer_saveDatesFile = csv.writer(saveDatesFile)
                mone_csv_file = open(path + client_name + '/moneFiles/' + meterkey + '.csv', 'r')
                numlines = len(mone_csv_file.readlines())
                if numlines > 0:
                    print "numlines in mone.csv: " + str(numlines)
                    print "meterkey: " + str(meterkey)
                    line = linecache.getline(path + client_name +
                                             '/moneFiles/' + meterkey + '.csv', numlines)
                    print "line : " + str(line)
                    lastDatethatHasData = linecache.getline(path + client_name +
                                                            '/moneFiles/' + meterkey + '.csv', numlines).split(',')[0]
                    linecache.clearcache()
                    mone_csv_file.close()
                    print "lastDatethatHasData" + str(lastDatethatHasData)
                    lastDay = int(lastDatethatHasData.split('/')[0])
                    lastmonth = int(lastDatethatHasData.split('/')[1])
                    lastyear = int(lastDatethatHasData.split('/')[2])
                    print "lastDay: " + str(lastDay)
                    print "lastmonth: " + str(lastmonth)
                    print "lastyear: " + str(lastyear)
                    endData = date(lastyear, lastmonth, lastDay)

                    writer_saveDatesFile.writerow(
                        [str(dates.get("start")).encode('utf-8'),
                         str(endData).encode('utf-8'), str(row[0]).encode('utf-8'),
                         str(row[1]).encode('utf-8'), str(row[2]).encode('utf-8')])
                    saveDatesFile.close()
    return 0


def iterativeGetEnergyMeter(key, client_name, sitekey, meterkey, dates, writer_file_desc):
    PrintFrame()
    for s, e in perdelta(dates.get("start"), dates.get("end"), timedelta(days=120)):
        print "s:" + str(s)
        print "e:" + str(e)
        getEnergyMeter(key, client_name, sitekey, meterkey, timeFormat(s), timeFormat(e), writer_file_desc)


def perdelta(start, end, delta):
    PrintFrame()
    curr = start
    while curr < end:
        yield curr, min(curr + delta, end)
        curr += delta
        curr += timedelta(days=1)


def timeFormat(dateStamp):
    PrintFrame()
    return datetime.datetime.strptime(str(dateStamp), '%Y-%m-%d').strftime('%d/%m/%y')


def getEnergyMeter(key, client_name, sitekey, meterkey, startdate, stopdate, writer_file_desc):
    """
    The function makes a request for the data and writes it in a file.
    :param key:
    :param client_name:
    :param sitekey:
    :param meterkey:
    :param startdate:
    :param stopdate:
    :param writer_file_desc:
    :return:
    """
    PrintFrame()
    print "satrtdate: " + str(startdate)
    print "stopdate: " + str(stopdate)
    url_to_app = 'https://srvt1.i-ecnet.co.il/magic94Scripts/mgrqispi94.dll'
    key = str(key).split('=')[1]
    key = key.split('-N')
    data = {'APPNAME': 'traklin',
            'PRGNAME': 'MoneMehkarHoze',
            'ARGUMENTS': '-N' + key[1] + '-N' + key[2] + '-A' + key[
                3] + ',-N' + sitekey + ',-N' + meterkey + ',-A' + startdate + ',-A' + stopdate + ',-A1,-A1,-N1'}

    header = {'User-Agent': user_agent,
              'content-type': 'application/x-www-form-urlencoded',
              'charset': 'utf-8'}

    req = urllib2.Request(url_to_app, urllib.urlencode(data), header)
    try:
        response = urllib2.urlopen(req)
    except urllib2.URLError as e:
        if hasattr(e, 'reason'):
            print 'We failed to reach a server.'
            print 'Reason: ', e.reason
        elif hasattr(e, 'code'):
            print 'The server couldn\'t fulfill the request.'
            print 'Error code: ', e.code
    else:
        page = response.read()
        with open(path + client_name + '/' + meterkey + '.html', 'wb+') as hh:
            hh.write(page)

        # out = open(path + client_name + '/moneFiles/' + meterkey + '.csv', 'ab+')
        # writer = csv.writer(out)
        soup = BeautifulSoup(page, 'lxml')
        table2 = soup.find_all('table')
        if table2.__len__() > 0:
            table2 = table2[1]
            for tr in table2.find_all('tr'):
                tds = tr.find_all('td')
                row = [elem.text.encode('utf-8').strip() for elem in tds]
                if row.__len__() > 1:
                    writer_file_desc.writerow(row)
        # out.close()
        response.close()
    return 0


def gettingDates(mone, client_name):
    """
    The function updates startDate- the date should start reading from
                        endDate- today date - the last date to read from
    :param mone:
    :param client_name:
    :return:
    """
    PrintFrame()
    dates = {}
    lastReadFile = open(path + client_name + '/LastReadFiles/' +
                        'lastRead' + mone + '.csv', "ab+")
    numline_lastReadFile = len(lastReadFile.readlines())

    startDate = None

    if numline_lastReadFile == 0:
        #if it's the first time read from 1/1/2012
        startDate = date(2012, 1, 1)
    else:
        #start reading from finish date of the last reading
        tempDate = linecache.getline(path + client_name + '/LastReadFiles/' +
                                     'lastRead' + mone + '.csv', numline_lastReadFile).split(",")[1]
        print "tempDate: " + str(tempDate)
        tempYear = int(tempDate.split("-")[1])

        if tempYear < 100:
            tempYear += 2000
        tempMonth = int(tempDate.split("-")[1])
        tempDay = int(tempDate.split("-")[2])

        print "tempDate: " + str(tempDate)
        print "tempYear: " + str(tempYear)
        print "tempMonth: " + str(tempMonth)
        print "tempDay: " + str(tempDay)

        startDate = date(tempYear, tempMonth, tempDay) + timedelta(days=1)
        linecache.clearcache()

    endDate = getTodayDate()
    dates['start'] = startDate
    dates['end'] = endDate
    print "getDates: start dates" + str(startDate)
    print "getDates: end dates" + str(endDate)
    return dates


def getTodayDate():
    """
    :return: today date
    """
    PrintFrame()
    todayStr = time.strftime("%x")
    month = int(todayStr.split('/')[0])
    day = int(todayStr.split('/')[1])
    year = int(todayStr.split('/')[2]) + 2000
    return date(year, month, day)


def timeStamper(dateStamp):
    """
    parse date to linux time
    :param dateStamp:
    :return:
    """
    IST_dt = tz.localize(datetime.datetime.strptime(dateStamp, "%d/%m/%Y"))
    UTC_dt = IST_dt.astimezone(utc)
    timeStamp = time.mktime(UTC_dt.timetuple())
    return timeStamp


def indexParser(csvDir):
    """
    :param csvDir:
    :return: a list of mone numbers
    """
    csvFiles = []
    with open(csvDir + 'index.csv', 'rb') as csvreadout:
        csvread = csv.reader(csvreadout, delimiter=',', quotechar='"')
        for row in csvread:
            csvFiles.append(row[0].strip())
        csvreadout.close()
    return csvFiles


def csvDataParser(csvDir, csvFileName, client_name):
    """
    The function creates a new file for each mone_number.csv file.
    the new file has 3 cols: time (linux), data and mone number.
    :param csvDir:
    :param csvFileName:
    :param client_name:
    :return:
    """
    with open(csvDir + '/moneFiles/' + csvFileName + '.csv', 'r') as inFile:
        csv.reader(inFile, delimiter=',', quotechar='"')
        if not os.path.exists(mysqlDir+ siteCred['client_name']+'Files/changed_csv/'):
            os.makedirs(mysqlDir + siteCred['client_name']+'Files/changed_csv/')
        outFile = open(mysqlDir +siteCred['client_name']+'Files/changed_csv/' + csvFileName + '_edited.csv', 'wb')
        for row in inFile:
            row = row.split(',')
            dateStamp = row.pop(0)
            timeStamp = timeStamper(dateStamp)
            for i in range(len(row)):
                outFile.write(str(timeStamp) + "," + row[i].rstrip() + "," + csvFileName + "\n")
                timeStamp += 15 * 60
        outFile.close()
    inFile.close()
    return 0


def dataBaseConnect(client_name):
    database = MySQLdb.connect(host="localhost", user="root", passwd="XXX")
    sql = 'create database if not exists ' + client_name + ';'
    sanitizedClient = str(client_name).replace('\'', '')  # use better way to sanitize input!!!!
    print "sql " + str(sql)
    database.cursor().execute("""create database if not exists %s""" % sanitizedClient)
    return MySQLdb.connect(host="localhost", user="root", passwd="XXX", db=sanitizedClient)


credentialsList = readCredentialsFile()
toDay = getTodayDate()

i = 0
while i < len(credentialsList):  # for all customers
    siteCred = parseCredentials(credentialsList[i])
    if not os.path.exists(path + siteCred['client_name']):
        os.makedirs(path + siteCred['client_name'])
        os.makedirs(path + siteCred['client_name'] + '/moneFiles')
        os.makedirs(path + siteCred['client_name'] + '/LastReadFiles')
    print "siteCred['client_name']: "+str(siteCred['client_name'])
    key = getkey(siteCred)
    print key
    get_lists(key, siteCred['client_name'])
    writeMeterCsv(key, siteCred['client_name'])

    print "START SQL!!!!!"

    csvDir = path + siteCred['client_name'] + '/'
    db = dataBaseConnect(siteCred['client_name'])
    print 'connected to db'
    cur = db.cursor()
    sqlcreatetable = 'create table if not exists pointValues (date varchar(30) NOT NULL, value varchar(10) NOT NULL, moneNum varchar(20)  NOT NULL)'
    cur.execute(sqlcreatetable)

    csvFileNames = indexParser(csvDir)
    readFiles = 0
    for csvFileName in csvFileNames:
        csvDataParser(csvDir, csvFileName, siteCred['client_name'])
        sqlStatement = 'LOAD DATA INFILE \'' + mysqlDir + siteCred['client_name']+'Files/changed_csv/' + csvFileName + \
                       "_edited.csv\' INTO TABLE pointValues FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'"
        print sqlStatement
        cur.execute(sqlStatement)
        db.commit()
    i += 1
