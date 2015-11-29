__author__ = 'Yafit'
import mechanize
import csv
import argparse
import ssl
import httplib

def readCredentialsFile(credDir):
    """
    Function reads from credantials.csv file and put the data in traklin_sites_credentials
    input: credDir- the path to the 'credentials.csv'
    :return:traklin_sites_credentials
    """
    with open(credDir + 'credentials.csv', 'rt') as f:
        traklin_sites_credentials = []
        reader = csv.DictReader(f)
        for row in reader:
            traklin_sites_credentials.append(row)
    return traklin_sites_credentials


def init():
    ssl.PROTOCOL_SSLv23 = ssl.PROTOCOL_TLSv1
    browser = mechanize.Browser()
    browser.addheaders = [{'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64)'
                                        ' AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
                          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                          # 'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
                          # 'Accept-Encoding': 'none',
                          # 'Accept-Language': 'en-US,en;q=0.8',
                          # 'Connection': 'keep-alive'
                          }]
    browser.set_handle_robots(False)
    return browser


def getkey(br, credentials):
    """
    :param br:
    :param v_user:
    :param v_password:
    :param v_tz:
    :return:
    """
    url_to_app = 'https://srvt1.i-ecnet.co.il/magic94Scripts/mgrqispi94.dll?appname=traklin&prgname=Logon'


    try:
        br.open(url_to_app)
        br.form = list(br.forms())[0]
    except httplib.BadStatusLine as e:
        print(e)

    br.form["v_user"] = credentials.get("username")
    br.form["v_password"] = credentials.get("password")
    br.form["v_taz"] = credentials.get("tz")
    res = br.submit()

    print res.read()



    # data = {'APPNAME' : 'traklin',
    #         'PRGNAME' : 'Identification',
    #         'ARGUMENTS' : 'v_user,v_password,v_taz',
    #         'v_user' : v_user,
    #         'v_password' : v_password ,
    #         'v_taz' : v_tz}
    #
    # header = { 'User-Agent' : user_agent ,
    #           'content-type' : 'application/x-www-form-urlencoded',
    #          'charset' : 'utf-8'}
    #
    # req = urllib2.Request(url_to_app,urllib.urlencode(data),header)
    # try:
    #     response = urllib2.urlopen(req)
    # except urllib2.URLError as e:
    #     if hasattr(e, 'reason'):
    #         print 'We failed to reach a server.'
    #         print 'Reason: ', e.reason
    #     elif hasattr(e, 'code'):
    #         print 'The server couldn\'t fulfill the request.'
    #         print 'Error code: ', e.code
    # else:
    #     soup = BeautifulSoup(response.read(), 'html.parser')
    #     result = soup.find_all('a')[-1]
    #     result = str(result).split('&amp')[2]
    #     result = str(result).split(';')[1]
    #     result = str(result).split('\"')[0]
    #     return result
    #


def parseCredentials(traklin_sites_credentials):
    i = 0
    cred = {}
    while (i < len(traklin_sites_credentials)):  # for all customers
        dict = traklin_sites_credentials[i]
        cred["client_name"] = dict.get('name')
        cred["username"] = dict.get('username')
        cred["password"] = dict.get('password')
        cred["tz"] = dict.get('tz')
        if (len(cred["tz"]) < 9):
            cred["tz"] = '0' + str(cred["tz"])  # adding leading '0'
        i = +1
        yield cred


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='A Traklin client',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-c', '--creds', default="C:\\Users\\Yafit\\PycharmProjects\\Untitled\\",
                        help='This is the adress of the credentials file')

    args = parser.parse_args()
    credDir = args.creds
    user_agent = 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)'

    credentialsList = readCredentialsFile(credDir)
    br = init()
    for creds in parseCredentials(credentialsList):
        getkey(br, creds)
