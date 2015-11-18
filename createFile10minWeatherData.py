__author__ = 'Yafit'
import urllib2

f = '/opt/skyspark-2.1.11/db/shufersal/io/10minWeatherData.xml'
gh_url='https://data.gov.il//sites/data.gov.il/files/xml/imslasthour.xml'
req = urllib2.Request(gh_url)
cookies_manager = urllib2.HTTPCookieProcessor()
opener = urllib2.build_opener(cookies_manager)
urllib2.install_opener(opener)
handler = urllib2.urlopen(req)
with open(f, 'w') as f:
    f.write(handler.read())
