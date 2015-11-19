# coding=utf-8
from _ctypes import sizeof
from genericpath import getsize

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

def PrintFrame():
  callerframerecord = inspect.stack()[1]    # 0 represents this line           # 1 represents line at caller
  frame = callerframerecord[0]
  info = inspect.getframeinfo(frame)
  print info.filename                       # __FILE__     -> Test.py
  print info.function                       # __FUNCTION__ -> Main
  print info.lineno                         # __LINE__     -> 13



def readCSVfile():
    with open('/opt/skyspark-2.1.12/db/newnewnew/traklin2/credentials.csv', 'rt') as f:
        reader = csv.DictReader(f)
        for row in reader:
            traklin_sites_credentials.append(row)


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
        response.close()
        return result


def get_mySites(key):
    PrintFrame()
    url_to_app = 'https://srvt1.i-ecnet.co.il/magic94Scripts/mgrqispi94.dll'
    key = str(key).split('=')[1]
    key = key.split('-N')
    data = {'APPNAME' : 'traklin',
            'PRGNAME' : 'Atarim',
            'ARGUMENTS' : '-N'+key[1]+'-N'+key[2]+'-N'+key[3]+',-N2,Atarim'}

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
        #add check for empty tables!!!
        out =open('/opt/skyspark-2.1.12/db/newnewnew/traklin2/'+site_details['client_name']+'/My Sites/mySites.csv','w+')
        writer = csv.writer(out)
        soup = BeautifulSoup(response.read(),'html.parser' )
        table = soup.find_all('table')
        if table.__len__() > 0 :
            table2 = table[0]
            for tr in table2.find_all('tr'):
                #ths = tr.find_all('th')
                #rec = [elem.text.encode('utf-8').strip() for elem in ths]
                #if rec.__len__() > 1 :
                 #   writer.writerow(rec)

                tds = tr.find_all('td')
                row = [elem.text.encode('utf-8').strip() for elem in tds]
                if row.__len__() > 1 :
                    writer.writerow(row)

                    ##DEGUG##
            # out =open('temp.html','wb+')
            # out.write(response.read())
        out.close()
        response.close()
    return 0


def get_lists(key):
    PrintFrame()
    #outFile = open('/opt/skyspark-2.1.12/db/newnewnew/traklin/'+site_details['client_name']+'/index_raw.csv', 'w+')
    #outwriter = csv.writer(outFile)
    sites = getsites(key)
    for sitekeys,sitevalues in sites.items():
        gettingDates(sitekeys)
        getreport(key,sitekeys)
        saveDatesFile = open('/opt/skyspark-2.1.12/db/newnewnew/traklin2/'+site_details['client_name']+'/LastReadFiles/'+
                        'lastRead'+sitekeys+'.csv',"ab+")
        writer = csv.writer(saveDatesFile)
        writer.writerow([ month1.encode('utf-8'), year1.encode('utf-8'), month2.encode('utf-8'),
                    year2.encode('utf-8')])
        saveDatesFile.close()


def getreport(key,sitekey):
    PrintFrame()
    url_to_app = 'https://srvt1.i-ecnet.co.il/magic94Scripts/mgrqispi94.dll'
    data = {'appname' : 'traklin',
            'prgname' : 'ShimushCalendary_Hoze',
            'ARGUMENTS' : key.split('=')[1]+',-N'+sitekey+','+'-A'+year1+','+'-A'+month1+','+'-A'+year2+','+'-A'+month2+','+'-N2'}

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
        out = open('/opt/skyspark-2.1.12/db/newnewnew/traklin2/'+client_name+
                   '/Calendar Monthly Reports/'+year1+'/'+sitekey+'.csv','ab+')
        writer = csv.writer(out)
        soup = BeautifulSoup(response.read(),'html.parser' )
        table = soup.find_all('table')
        if table.__len__() == 1 :
            table2 = table[0]
            for tr in table2.find_all('tr'):
                ths = tr.find_all('th')
                rec = [elem.text.encode('utf-8').strip() for elem in ths]
                if rec.__len__() > 1 :
                    writer.writerow(rec)

                tds = tr.find_all('td')
                row = [elem.text.encode('utf-8').strip() for elem in tds]
                if row.__len__() > 1 :
                    writer.writerow(row)
        out.close()
        response.close()
    return 0





def getsites(key):
    PrintFrame()
    url_to_app = 'https://srvt1.i-ecnet.co.il/magic94Scripts/mgrqispi94.dll'

    data = {'appname' : 'traklin',
            'prgname' : 'HozeRepByDate',
            'ARGUMENTS' : key.split('=')[1]+',-N2'}

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
        sitesFile = open('/opt/skyspark-2.1.12/db/newnewnew/traklin2/'+site_details['client_name']+
                         '/My Sites/mySites.csv',"rb+")
        numOfSites = len(sitesFile.readlines())
        print "get_mySites, numOfNumbers: " +str(numOfSites)
        sitesFile.close()

        for stringer in sites_list[1:numOfSites+1] :
            print stringer+"##########"
            stringer = stringer.split('-')
            if (len(stringer)>1):
                print "len(stringer)>1"
                sites_tuple[stringer[0]] = stringer[1]
        print "end get sites - else "
        return sites_tuple




def gettingDates(siteNumber):
    lastReadFile = open('/opt/skyspark-2.1.12/db/newnewnew/traklin2/'+site_details['client_name']+'/LastReadFiles/'+
                        'lastRead'+siteNumber+'.csv',"ab+")
    numline = len(lastReadFile.readlines())
    global year1
    global year2
    if (numline==0):
        year1 = str(2011)

    else:
        tempyear =  linecache.getline('/opt/skyspark-2.1.12/db/newnewnew/traklin2/'+site_details['client_name']+'/LastReadFiles/'+
                        'lastRead'+siteNumber+'.csv',numline).split(",")[1]
        year1 = str (int(tempyear)+1)
        linecache.clearcache()
    year2 = year1
    if not os.path.exists('/opt/skyspark-2.1.12/db/newnewnew/traklin2/'+client_name+'/Calendar Monthly Reports/'+year1):
        os.makedirs('/opt/skyspark-2.1.12/db/newnewnew/traklin2/'+client_name+'/Calendar Monthly Reports/'+year1)
    lastReadFile.close()



reload(sys)
sys.setdefaultencoding('utf-8')

#ssl.PROTOCOL_SSLv23 = ssl.PROTOCOL_TLSv1
traklin_sites_credentials=[] #contains the sites name, username, password and tz
user_agent = 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)'
readCSVfile()

month1 = str(1)
year1 = None
month2 = str(12)
year2 = None




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
    if not os.path.exists('/opt/skyspark-2.1.12/db/newnewnew/traklin2/'+client_name):
        os.makedirs('/opt/skyspark-2.1.12/db/newnewnew/traklin2/'+client_name)
        os.makedirs('/opt/skyspark-2.1.12/db/newnewnew/traklin2/'+client_name+'/My Sites')
        os.makedirs('/opt/skyspark-2.1.12/db/newnewnew/traklin2/'+client_name+'/Calendar Monthly Reports')
        os.makedirs('/opt/skyspark-2.1.12/db/newnewnew/traklin2/'+client_name+'/LastReadFiles')

    key = getkey(site_username,site_password,site_tz)
    print key
    #get_mySites(key)
   # get_lists(key)

    i+=1