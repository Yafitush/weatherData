# coding=utf-8

__author__ = 'Yafit'
import urllib2
import urllib
import os.path
import csv
import datetime
import calendar
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

def readCSVfile():
    """

    :return:
    """
    with open(path+'credentials.csv', 'rt') as f:
    #with open('credentials.csv', 'rt') as f:
        reader = csv.DictReader(f)
        for row in reader:
            #print row
            traklin_sites_credentials.append(row)


def PrintFrame():
  callerframerecord = inspect.stack()[1]    # 0 represents this line           # 1 represents line at caller
  frame = callerframerecord[0]
  info = inspect.getframeinfo(frame)
  print info.filename                       # __FILE__     -> Test.py
  print info.function                       # __FUNCTION__ -> Main
  print info.lineno                         # __LINE__     -> 13


def getkey(v_user,v_password,v_tz):
    PrintFrame()
    url_to_app = 'https://srvt1.i-ecnet.co.il/magic94Scripts/mgrqispi94.dll#'

    data = {'APPNAME' : 'traklin',
            'PRGNAME' : 'Identification',
            'ARGUMENTS' : 'v_user,v_password,v_taz',
            'v_user' : v_user,
            'v_password' : v_password ,
            'v_taz' : v_tz}

    header = { 'User-Agent' : user_agent ,
              'content-type' : 'application/x-www-form-urlencoded',
             'charset' : 'utf-8'}

    req = urllib2.Request(url_to_app,urllib.urlencode(data),header)
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


def get_lists(key):
    PrintFrame()
    if os.path.isfile(path+site_details['client_name']+'/index_raw.csv'):
        print "get_list- if"
        with open(path+site_details['client_name']+'/index_raw.csv', 'r') as inFile:
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
        outFile = open(path+site_details['client_name']+'/index_raw.csv', 'w+')
        outwriter = csv.writer(outFile)
        sites = getsites(key)
        print "returned from getsites"
        for sitekeys,sitevalues in sites.items():
            mone_list = getmonim(sitekeys,key)
            print(sitekeys)
            print(sitevalues)
            if mone_list.__len__() >0:
                for mone in mone_list:
                    outwriter.writerow([mone.encode("UTF-8") , '\"' +sitevalues.encode("UTF-8").replace('"','') + '\"'  , sitekeys.encode("UTF-8") ])
            else:
                outwriter.writerow([ '-1' , '\"' +sitevalues.encode("UTF-8").replace('"','') + '\"'  , sitekeys.encode("UTF-8")])
        outFile.close()
        print "get list end else"
    index_cleaner('index_raw.csv')
    return 0


def getsites(key):
    PrintFrame()
    url_to_app = 'https://srvt1.i-ecnet.co.il/magic94Scripts/mgrqispi94.dll'

    data = {'APPNAME' : 'traklin',
            'PRGNAME' : 'HozeRepByDate',
            'ARGUMENTS' : key.split('=')[1]+',-N6'}

    header = { 'User-Agent' : user_agent ,
               'content-type' : 'application/x-www-form-urlencoded',
               'charset' : 'utf-8'}
    sites_list = []
    sites_tuple = {}

    req = urllib2.Request(url_to_app,urllib.urlencode(data),header)
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
        for stringer in sites_list[1:] :
            stringer = stringer.split('-')
            sites_tuple[stringer[0]] = stringer[1]
        print "end get sites - else "
        return sites_tuple


def getmonim(sitekey,key):
    PrintFrame()
    url_to_app = 'https://srvt1.i-ecnet.co.il/magic94Scripts/mgrqispi94.dll'
    print "getting mone for site" + sitekey
    data = {'APPNAME' : 'traklin',
            'PRGNAME' : 'MoneRepByDate',
            'ARGUMENTS' : key.split('=')[1]+',atar',
            'year2Value' : 'true',
            'type' : 'hoze' ,
            'atar' : sitekey}

    header = { 'User-Agent' : user_agent ,
               'content-type' : 'application/x-www-form-urlencoded',
               'charset' : 'utf-8'}
    mone_list = []

    req = urllib2.Request(url_to_app,urllib.urlencode(data),header)
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


def index_cleaner(rawfile):
    PrintFrame()
    print "index cleaner"
    with open(path+site_details['client_name']+'/'+rawfile, 'r') as inFile:
        print "opened index_raw!!!"
        outfile = open('/opt/skyspark-2.1.12/db/newnewnew/traklin/'+site_details['client_name']+'/index.csv','w+')
        reader = csv.reader(inFile, delimiter=',', quotechar="\"")
        for row in reader:
            if (int(row[0])> 3):
                outfile.write(', '.join(row) + '\n')
    inFile.close()


def writeMeterCsv(key):
    PrintFrame()
    with open(path+site_details['client_name']+'/index.csv','r') as inFile:
        reader = csv.reader(inFile, delimiter=',', quotechar='"')
        for row in reader:
            if os.path.isfile(row[0].strip()+'.csv'):
                print 'skip to next'
            else :
                print(row[0])
                gettingDates(row[0])
                iterativeGetEnergyMeter(key,row[-1].strip(),row[0].strip())

                saveDatesFile = open(path+client_name+'/LastReadFiles/lastRead'+row[0]+'.csv',"ab+")
                writer = csv.writer(saveDatesFile)
                writer.writerow([ str(startDate).encode('utf-8'), str(endDate).encode('utf-8'), str(row[0]).encode('utf-8'),
                    str(row[1]).encode('utf-8'), str(row[2]).encode('utf-8')])

    return 0

def iterativeGetEnergyMeter(key,sitekey,meterkey):
    PrintFrame()
    for s,e in perdelta(startDate, endDate, timedelta(days=120)):
        print "s:"+str(s)
        print "e:"+str(e)
        getEnergyMeter(key,sitekey,meterkey,timeFormat(s),timeFormat(e))



def perdelta(start, end, delta):
    PrintFrame()
    curr = start
    while curr < end:
        yield curr, min(curr + delta, end)
        curr += delta
        curr += timedelta(days=1)

def getEnergyMeter(key,sitekey,meterkey,startdate,stopdate):
    PrintFrame()
    url_to_app = 'https://srvt1.i-ecnet.co.il/magic94Scripts/mgrqispi94.dll'
    key = str(key).split('=')[1]
    key = key.split('-N')
    data = {'APPNAME' : 'traklin',
            'PRGNAME' : 'MoneMehkarHoze',
            'ARGUMENTS' : '-N'+key[1]+'-N'+key[2]+'-A'+key[3]+',-N'+sitekey+',-N'+meterkey+',-A'+startdate+',-A'+stopdate+',-A1,-A1,-N1'}

    header = { 'User-Agent' : user_agent ,
               'content-type' : 'application/x-www-form-urlencoded',
               'charset' : 'utf-8'}

    req = urllib2.Request(url_to_app,urllib.urlencode(data),header)
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
        #add check for empty tables!!!
        hh =open(path+site_details['client_name']+'/'+meterkey+'.html','wb+')
        hh.write(page)

        out =open(path+site_details['client_name']+'/moneFiles/'+meterkey+'.csv','ab+')

        writer = csv.writer(out)
        soup = BeautifulSoup(page,'lxml')
        table2 = soup.find_all('table')
        if table2.__len__() > 0 :
            table2 = table2[1]
            for tr in table2.find_all('tr'):
                tds = tr.find_all('td')
                row = [elem.text.encode('utf-8').strip() for elem in tds]
                if row.__len__() > 1 :
                    writer.writerow(row)
        response.close()
    return 0


def timeFormat(dateStamp):
    PrintFrame()
    return datetime.datetime.strptime(str(dateStamp), '%Y-%m-%d').strftime('%d/%m/%y')


def gettingDates(mone):
    lastReadFile = open(path+site_details['client_name']+'/LastReadFiles/'+
                        'lastRead'+mone+'.csv',"ab+")
    numline = len(lastReadFile.readlines())
    global startDate
    global endDate
    if (numline==0):
        startDate = date(2012,1,1)
    else:
        tempDate =  linecache.getline(path+site_details['client_name']+'/LastReadFiles/'+
                        'lastRead'+mone+'.csv',numline).split(",")[1]
        tempYear = int(tempDate.split("-")[0])
        tempMonth = int(tempDate.split("-")[1])
        tempDay = int(tempDate.split("-")[2])
        startDate = date(tempYear,tempMonth,tempDay) + timedelta(days=1)
        linecache.clearcache()
    year = startDate.year
    month = startDate.month
    newMonth = month+2
    if newMonth>12:
        newMonth =  month%12
        year+=1
    endDate= date(2015, 11, 22)
    #numOfDaysInMonth = timedelta(days=(calendar.monthrange(year,month)[1]) -1)
    #endDate = startDate+numOfDaysInMonth





#path='/opt/skyspark-2.1.12/db/newnewnew/traklin/'
path = 'C:/Users/Yafit/PycharmProjects/untitled/'
traklin_sites_credentials=[] #contains the sites name, username, password and tz
user_agent = 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)'

###############3333
utc= pytz.utc
tz = pytz.timezone('Asia/Jerusalem')

##################

readCSVfile()

startDate = None
endDate = None
ssl.PROTOCOL_SSLv23 = ssl.PROTOCOL_TLSv1
i = 0
while (i < len(traklin_sites_credentials)): #for all customers
    dict = traklin_sites_credentials[i]
    client_name = dict.get('name')
    site_username = dict.get('username')
    site_password = dict.get('password')
    site_tz = dict.get('tz')
    if (len(site_tz) < 9):
        site_tz = '0'+site_tz  #adding leading '0'
    site_details = { }
    site_details['client_name'] = client_name
    if not os.path.exists(path+client_name):
        os.makedirs(path+client_name)
        os.makedirs(path+client_name+'/moneFiles')
        os.makedirs(path+client_name+'/LastReadFiles')

    print "start date: " + str(startDate)
    print "end date: " + str(endDate)

    key = getkey(site_username,site_password,site_tz)
    print key
    get_lists(key)
    writeMeterCsv(key)
'''
    csvDir= csvDir=path+client_name+'/moneFiles'
    db = dataBaseConnect()
    cur = db.cursor()

    csvFileNames = indexParser(csvDir)
    readFiles = 0
    for csvFileName in csvFileNames:
    csvDataParser(csvDir, csvFileName)
    sqlStatement = 'LOAD DATA INFILE \''+mysqlDir+'changed_csv/' + csvFileName + "_edited.csv\' INTO TABLE pointValues FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'"
    print sqlStatement
    cur.execute(sqlStatement)
    db.commit()
'''



   # i += 1

