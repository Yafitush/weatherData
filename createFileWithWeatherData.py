__author__ = 'Yafit'
import urllib2

f = '/opt/skyspark-2.1.11/db/newnewnew/io/weatherStationsData.xml'
url = "http://www.ims.gov.il/ims/PublicXML/observ.xml"

p = urllib2.build_opener(urllib2.HTTPCookieProcessor).open(url)

with open(f, 'w') as f:
    f.write(p.read())

f.close()
