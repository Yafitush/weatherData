# coding=utf-8
__author__ = 'nick'

import urllib2
import urllib
import sys
import re
import base64
import os.path
import csv
import datetime
from urlparse import urlparse
from bs4 import BeautifulSoup

def getkey(v_user,v_password,v_tz):
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

   # print data

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
        print "fine1"
        # everything is fine
        soup = BeautifulSoup(response.read(), 'html.parser')
        result = soup.find_all('a')[-1]
        result = str(result).split('&amp')[2]
        result = str(result).split(';')[1]
        result = str(result).split('\"')[0]
        return result

from datetime import date, timedelta
v_user = '2258329'
v_password = 'Avner84'
v_tz = '057882730'

user_agent = 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)'
#user_agent = 'Mozilla/5.0 (Windows NT 6.1; WOW64)'



def getsites(key):
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
    url_to_app = 'https://srvt1.i-ecnet.co.il/magic94Scripts/mgrqispi94.dll'

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
    print "index cleaner"
    with open('/opt/skyspark-2.1.12/db/newnewnew/io/'+rawfile, 'r') as inFile:
        outfile = open('/opt/skyspark-2.1.12/db/newnewnew/io/index.csv','w+')
        reader = csv.reader(inFile, delimiter=',', quotechar="\"")
        for row in reader:
            if ( int(row[0]) > 3  ):
                outfile.write(', '.join(row) + '\n')
    inFile.close()



    return 0

def get_lists(key):
    if os.path.isfile('/opt/skyspark-2.1.12/db/newnewnew/io/index_raw.csv'):
        print "get_list- if"
        with open('/opt/skyspark-2.1.12/db/newnewnew/io/index_raw.csv', 'r') as inFile:
            csv.reader(inFile, delimiter=',', quotechar='"')
            sitekeys = []
            sitevalues = []
            mone_list = []
            for row in inFile:
                row = row.split(',')
                sitekeys.append(row[2])
                sitevalues.append(row[1])
                mone_list.append(row[0])
    else:#
        print "get_list- else"
        outFile = open('/opt/skyspark-2.1.12/db/newnewnew/io/index_raw.csv', 'w+')
        outwriter = csv.writer(outFile)
        sites = getsites(key)
        print "returned from getsites"
        for sitekeys,sitevalues in sites.items():
            mone_list = getmonim(sitekeys,key)
            #print(sitekeys)
            #print(sitevalues)
            if mone_list.__len__() >0:
                for mone in mone_list:
                    #print(mone)
                    outwriter.writerow([mone.encode("UTF-8") , '\"' +sitevalues.encode("UTF-8").replace('"','') + '\"'  , sitekeys.encode("UTF-8") ])
            else :
                outwriter.writerow([ '-1' , '\"' +sitevalues.encode("UTF-8").replace('"','') + '\"'  , sitekeys.encode("UTF-8")])
        outFile.close()
        print "get lise end else"
    index_cleaner('index_raw.csv')
    return 0

def getEnergyMeter(key,sitekey,meterkey,startdate,stopdate):
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
        #add check for empty tables!!!
        out =open('/opt/skyspark-2.1.12/db/newnewnew/io/'+meterkey+'.csv','ab+')
        writer = csv.writer(out)
        soup = BeautifulSoup(response.read())
        table2 = soup.find_all('table')
        if table2.__len__() > 0 :
            table2 = table2[1]
            for tr in table2.find_all('tr'):
                tds = tr.find_all('td')
                row = [elem.text.encode('utf-8').strip() for elem in tds]
                if row.__len__() > 1 :
                    writer.writerow(row)
                    ##DEGUG##
            # out =open('temp.html','wb+')
            # out.write(response.read())
        response.close()
    return 0

def writeMeterCsv(key):
    print "write meter csv"
    with open('/opt/skyspark-2.1.12/db/newnewnew/io/index.csv','r') as inFile:
        reader = csv.reader(inFile, delimiter=',', quotechar='"')
        for row in reader:
            if os.path.isfile(row[0].strip()+'.csv'):
                print 'skip to next'
            else :
                print(row[0])
                iterativeGetEnergyMeter(key,row[-1].strip(),row[0].strip())
    return 0

def perdelta(start, end, delta):
    curr = start
    while curr < end:
        yield curr, min(curr + delta, end)
        curr += delta
        curr += timedelta(days=1)

def timeFormat(dateStamp):
    return datetime.datetime.strptime(str(dateStamp), '%Y-%m-%d').strftime('%d/%m/%y')

def iterativeGetEnergyMeter(key,sitekey,meterkey):
    for s,e in perdelta(date(2014,7,1), date(2014,7,31), timedelta(days=120)):
        getEnergyMeter(key,sitekey,meterkey,timeFormat(s),timeFormat(e))

key = getkey(v_user,v_password,v_tz)
get_lists(key)
writeMeterCsv(key)
# getEnergyMeter(key,'4021209','046712572551','01/05/2010','01/07/2010')
# iterativeGetEnergyMeter(key,'4021209','046712572551')