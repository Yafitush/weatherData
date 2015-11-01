__author__ = 'Yafit'
import urllib2

f = '10minWeatherData.xmll'
gh_url='https://data.gov.il//sites/data.gov.il/files/xml/imslasthour.xml'
req = urllib2.Request(gh_url)
cookies_manager = urllib2.HTTPCookieProcessor()
opener = urllib2.build_opener(cookies_manager)
urllib2.install_opener(opener)
n=0
while (1):
    print ('This is the ' + str(n) + ' time invoking the site;')
    handler = urllib2.urlopen(req).read()
    print handler
    n=n+1









