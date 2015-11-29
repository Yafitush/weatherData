__author__ = 'Yafit'
import urllib2
import urllib
import os.path
import csv
import time
import linecache
from bs4 import BeautifulSoup
import inspect
import sys
import ssl
import MySQLdb
import pytz

reload(sys)
sys.setdefaultencoding('utf-8')
path = '/opt/skyspark-2.1.12/db/newnewnew/traklin_Calendar_months/'
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


def get_mySites(key, client_name):
    """
    :param key:
    :param client_name:
    :return:
    """
    PrintFrame()
    url_to_app = 'https://srvt1.i-ecnet.co.il/magic94Scripts/mgrqispi94.dll'
    key = str(key).split('=')[1]
    key = key.split('-N')
    data = {'APPNAME': 'traklin',
            'PRGNAME': 'Atarim',
            'ARGUMENTS': '-N' + key[1] + '-N' + key[2] + '-N' + key[3] + ',-N2,Atarim'}

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

        out = open(path + client_name + '/MySites/mySites.csv', 'w+')
        writer = csv.writer(out)
        soup = BeautifulSoup(response.read(), 'html.parser')
        table = soup.find_all('table')
        if table.__len__() > 0:
            table2 = table[0]
            for tr in table2.find_all('tr'):
                ths = tr.find_all('th')
                rec = [elem.text.encode('utf-8').strip() for elem in ths]
                if rec.__len__() > 1:
                    writer.writerow(rec)

                tds = tr.find_all('td')
                row = [elem.text.encode('utf-8').strip() for elem in tds]
                if row.__len__() > 1:
                    writer.writerow(row)
        out.close()
        response.close()
    return 0


def get_lists(key, client_name):
    """
    The function build index.csv which contains the sites names and numbers.
    :param key:
    :param client_name: is needed for the path of files
    :return:
    """
    PrintFrame()
    if os.path.isfile(path + client_name + '/index.csv'):
        with open(path + client_name + '/index.csv', 'r') as inFile:
            csv.reader(inFile, delimiter=',', quotechar='"')
            sitekeys = []
            sitevalues = []
            for row in inFile:
                row = row.split(',')
                sitekeys.append(row[1])
                sitevalues.append(row[0])
    else:
        outFile = open(path + client_name + '/index.csv', 'w+')
        outwriter = csv.writer(outFile)
        sites = getsites(key, client_name)
        print "returned from getsites"
        for sitekeys, sitevalues in sites.items():
            print(sitekeys)
            print(sitevalues)
            outwriter.writerow([sitevalues.encode("UTF-8").replace('"', '') + '\"',
                                sitekeys.encode("UTF-8")])
        outFile.close()
    return 0


def getsites(key, client_name):
    PrintFrame()
    url_to_app = 'https://srvt1.i-ecnet.co.il/magic94Scripts/mgrqispi94.dll'

    data = {'appname': 'traklin',
            'prgname': 'HozeRepByDate',
            'ARGUMENTS': key.split('=')[1] + ',-N2'}

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
        sitesFile = open(path + client_name + '/MySites/mySites.csv', "rb+")
        numOfSites = len(sitesFile.readlines())
        print "get_mySites, numOfNumbers: " + str(numOfSites)
        sitesFile.close()

        for stringer in sites_list[1:numOfSites + 1]:
            print stringer + "##########"
            stringer = stringer.split('-')
            if len(stringer) > 1:
                print "len(stringer)>1"
                sites_tuple[stringer[0]] = stringer[1]
        print "end get sites - else "
        return sites_tuple


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
            if os.path.isfile(row[1].strip() + '.csv'):
                print 'skip to next'
            else:
                sitekey = row[1].strip()
                print "sitekey " + str(sitekey)
                dates = gettingDates(sitekey, client_name)

                file_desc = open(path + client_name + '/CalendarMonthlyReports/' + sitekey + '.csv', 'w')
                writer_file_desc = csv.writer(file_desc)
                iterativeGetEnergyMeter(key, client_name, sitekey, dates, writer_file_desc)
                file_desc.close()

                saveDatesFile = open(path + client_name + '/LastReadFiles/lastRead' + sitekey + '.csv', "ab+")
                writer_saveDatesFile = csv.writer(saveDatesFile)
                mone_csv_file = open(path + client_name + '/CalendarMonthlyReports/' + sitekey + '.csv', 'r')
                numlines = len(mone_csv_file.readlines())
                if numlines > 0:
                    print "WITING TO LAST READ FILE!!!!!"
                    print "numlines in mone.csv: " + str(numlines)
                    print "sitekey: " + str(sitekey)
                    line = linecache.getline(path + client_name + '/CalendarMonthlyReports/' + sitekey + '.csv',
                                             numlines)
                    print "line : " + str(line)
                    lastDatethatHasData = linecache.getline(path + client_name +
                                                            '/CalendarMonthlyReports/' + sitekey + '.csv',
                                                            numlines).split(',')[0]
                    linecache.clearcache()
                    mone_csv_file.close()
                    print "lastDatethatHasData" + str(lastDatethatHasData)
                    lastmonth = int(lastDatethatHasData.split('/')[0])
                    lastyear = int(lastDatethatHasData.split('/')[1])
                    print "lastmonth: " + str(lastmonth)
                    print "lastyear: " + str(lastyear)

                    writer_saveDatesFile.writerow(
                        [str(dates.get("month1")).encode('utf-8'),
                         str(dates.get("year1")).encode('utf-8'), str(lastmonth).encode('utf-8'),
                         str(lastyear).encode('utf-8')])
                    saveDatesFile.close()
    return 0


def iterativeGetEnergyMeter(key, client_name, sitekey, dates, writer_file_desc):
    PrintFrame()
    for m1, y1, m2, y2 in perdelta(int(dates.get("month1")), int(dates.get("year1")), int(dates.get("end_month")),
                                   int(dates.get("end_year"))):
        print "m1:" + str(m1)
        print "y1:" + str(y1)
        print "m2:" + str(m2)
        print "y2:" + str(y2)
        getEnergyMeter(key, client_name, sitekey, str(m1), str(y1), str(m2), str(y2), writer_file_desc)


def perdelta(startMonth, startYear, endMonth, endYear):
    PrintFrame()
    startingYear = startYear
    startingMonth = startMonth
    currEndMonth = 12
    while startingYear < endYear:
        yield startingMonth, startingYear, currEndMonth, startingYear
        startingYear += 1
        startingMonth = 1
    if startingYear == endYear:
        yield 1, startingYear, endMonth, endYear


def getEnergyMeter(key, client_name, sitekey, month1, year1, month2, year2, writer_file_desc):
    """
    The function makes a request for the data and writes it in a file.
    :param key:
    :param client_name:
    :param sitekey:
    :param writer_file_desc:
    :return:
    """
    PrintFrame()
    print "##################"
    print "sitekey: " + str(sitekey)
    print "month1: " + str(month1)
    print "year1: " + str(year1)
    print "month2: " + str(month2)
    print "year2: " + str(year2)
    print "###################"

    url_to_app = 'https://srvt1.i-ecnet.co.il/magic94Scripts/mgrqispi94.dll'
    key = str(key).split('=')[1]
    key = key.split('-N')
    data = {'appname': 'traklin',
            'prgname': 'ShimushCalendary_Hoze',
            'ARGUMENTS': '-N' + key[1] + '-N' + key[2] + '-A' + key[3] +
                         ',-N' + sitekey + ',-A' + year1 + ',-A' +
                         month1.zfill(2) + ',-A' + year2 +
                         ',-A' + month2.zfill(2) + ',-N1'
            }

    header = {'User-Agent': user_agent,
              'content-type': 'application/x-www-form-urlencoded',
              'charset': 'utf-8'}

    req = urllib2.Request(url_to_app, urllib.urlencode(data), header)
    print req.data
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
        with open(path + client_name + '/' + sitekey + '.html', 'wb+') as hh:
            hh.write(page)
        print page

        soup = BeautifulSoup(page, "lxml")
        numOfWritings = 0
        table2 = soup.find_all('table')
        print "table2 len: " + str(table2.__len__())
        if table2.__len__() == 1:
            table2 = table2[0]
            for tr in table2.find_all('tr'):
                tds = tr.find_all('td')
                row = [elem.text.encode('utf-8').strip() for elem in tds]
                if row.__len__() > 1:
                    writer_file_desc.writerow(row)
                    numOfWritings += 1
    response.close()


def getTodayDate():
    """
    :return: today month and year
    """
    PrintFrame()
    toDayData = {}
    todayStr = time.strftime("%x")
    month = int(todayStr.split('/')[0])
    year = int(todayStr.split('/')[2]) + 2000
    toDayData['toDay_month'] = month
    toDayData['toDay_yeer'] = year
    return toDayData


def gettingDates(siteNumber, client_name):
    PrintFrame()
    dates = {}
    lastReadFile = open(path + client_name + '/LastReadFiles/' +
                        'lastRead' + siteNumber + '.csv', "ab+")
    numline = int(len(lastReadFile.readlines()))
    year1 = None
    month1 = None

    if numline == 0:
        year1 = "2011"
        month1 = "1"

    else:
        tempMonth = linecache.getline(path + client_name + '/LastReadFiles/' +
                                      'lastRead' + siteNumber + '.csv', numline).split(",")[2]
        tempyear = linecache.getline(path + client_name + '/LastReadFiles/' +
                                     'lastRead' + siteNumber + '.csv', numline).split(",")[3]
        month1 = int(tempMonth)
        if month1 != 12:
            year1 = int(tempyear)
        else:
            month1 = 1
            year1 = int(tempyear) + 1

    toDay = getTodayDate()
    dates['month1'] = month1
    dates['year1'] = year1
    dates['end_month'] = toDay.get("toDay_month")
    dates['end_year'] = toDay.get("toDay_yeer")
    print "month1: " + str(month1)
    print "year1: " + str(year1)
    print "end month " + str(dates['end_month'])
    print "end year " + str(dates['end_year'])
    linecache.clearcache()
    lastReadFile.close()
    return dates


def csvDataParser(csvDir, csvFileName, client_name):
    """
    The function creates a new file for each mone_number.csv file.
    the new file has 3 cols: time (linux), data and mone number.
    :param csvDir:
    :param csvFileName:
    :param client_name:
    :return:
    """
    with open(csvDir + 'CalendarMonthlyReports/' + csvFileName + '.csv', 'r') as inFile:
        csv.reader(inFile, delimiter=',', quotechar='"')
        if not os.path.exists(mysqlDir + siteCred['client_name'] + 'MonthlyReports/' + 'changed_csv/'):
            os.makedirs(mysqlDir + siteCred['client_name'] + 'MonthlyReports/' + 'changed_csv/')

        with open(path + client_name + '/CalendarMonthlyReports/' + csvFileName + '.csv', 'r+') as infile:
            outfile = open(
                mysqlDir + siteCred['client_name'] + 'MonthlyReports/' + 'changed_csv/' + csvFileName + '_edited.csv',
                'wb+')
            print "INSIDE WITH!!!"
            writer_outFile = csv.writer(outfile)
            reader = csv.reader(infile)
            for row in reader:
                print "INSIDE FOR!!"
                row = [x.replace(',', '') for x in row]
                row = [x.replace("\xc2\xa0", "") for x in row]
                # row = [ str(x).decode('utf-8').encode('ascii','ignore')]
                row = [x.strip() for x in row]
                if len(row) == 4:
                    row.extend([0, 0, 0, 0])
                elif len(row) == 10:
                    del row[-1]
                    del row[-1]
                row.append(csvFileName)
                if len(row) == 9:
                    print "WRITTING TO FILE"
                    writer_outFile.writerow(row)
    return 0


def dataBaseConnect(client_name):
    database = MySQLdb.connect(host="localhost", user="root", passwd="XXX")
    sanitizedClient = str(client_name).replace('\'', '')  # use better way to sanitize input!!!!
    database.cursor().execute("""create database if not exists %s""" % sanitizedClient)
    return MySQLdb.connect(host="localhost", user="root", passwd="XXX", db=sanitizedClient)


def indexParser(csvDir):
    """
    :param csvDir:
    :return: a list of keySites
    """
    csvFiles = []
    with open(csvDir + 'index.csv', 'rb') as csvreadout:
        csvread = csv.reader(csvreadout, delimiter=',', quotechar='"')
        for row in csvread:
            csvFiles.append(row[1].strip())
        csvreadout.close()
    return csvFiles


credentialsList = readCredentialsFile()
i = 0
while i < len(credentialsList):  # for all customers
    siteCred = parseCredentials(credentialsList[i])
    if not os.path.exists(path + siteCred['client_name']):
        os.makedirs(path + siteCred['client_name'])
        os.makedirs(path + siteCred['client_name'] + '/MySites')
        os.makedirs(path + siteCred['client_name'] + '/CalendarMonthlyReports')
        os.makedirs(path + siteCred['client_name'] + '/LastReadFiles')

    key = getkey(siteCred)
    print key
    get_mySites(key, siteCred['client_name'])
    get_lists(key, siteCred['client_name'])
    writeMeterCsv(key, siteCred['client_name'])

    csvDir = path + siteCred['client_name'] + '/'
    db = dataBaseConnect(siteCred['client_name'])
    print 'connected to db'
    cur = db.cursor()
    sqlcreatetable = 'create table if not exists monthValues' \
                     ' (date varchar(10) NOT NULL, mainkwh varchar(20) NOT NULL,' \
                     ' pisgakwh varchar(20) NOT NULL, pisga_percent varchar(20) NOT NULL,' \
                     ' gevakwh varchar(20) NOT NULL, geva_percent varchar(20) NOT NULL,' \
                     ' shefelkwh varchar(20) NOT NULL, shefell_percent varchar(20) NOT NULL,' \
                     ' pointId  varchar(15) NOT NULL)'

    cur.execute(sqlcreatetable)

    csvFileNames = indexParser(csvDir)
    readFiles = 0
    for csvFileName in csvFileNames:
        csvDataParser(csvDir, csvFileName, siteCred['client_name'])

        sqlStatement = 'LOAD DATA INFILE \'' + mysqlDir + siteCred[
            'client_name'] + 'MonthlyReports/' + 'changed_csv/' + csvFileName + \
                       "_edited.csv\' INTO TABLE monthValues FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'"

        print sqlStatement
        cur.execute(sqlStatement)
        db.commit()

    i += 1
