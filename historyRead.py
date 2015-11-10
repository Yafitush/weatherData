#!/usr/bin/env python
# -*- coding: utf-8 -*-
from requests import cookies

__author__ = 'Yafit'
import requests

with requests.Session() as c:
    cookies_ = {
        "SESSe6f72e592b4ce65a520353778e27132d":"bujebn1ru8mjffrcbhh8vdhpq3",
        "_atuvc":"54%7C42%2C0%7C43%2C47%7C44",
        "__atuvs":"5638d22ef6c734c7001",
        "_pk_ref.97.9fd9":"%5B%22%22%2C%22%22%2C1446564398%2C%22http%3A%2F%2Fwww.ims.gov.il%2Fims%2Fall_tahazit%2F%22%5D",
        "has_js" : "1",
        "_pk_ses.97.9fd9":"*",
        }

    headers_ ={
        "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "en-US,en;q=0.8",
        "Cache-Control" : "max-age=0",
        "Connection": "keep-alive",
        "Content-Type": "application/x-www-form-urlencoded",
        "Host": "data.gov.il",
        "Origin" : "https://data.gov.il",
        "Referer" : "https://data.gov.il/ims",
        "Upgrade-Insecure-Requests":"1",
        "User-Agent":"Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko",
        #"Cookies":cookies
        #"Cookie":";_pk_ref.97.9fd9=%5B%22%22%2C%22%22%2C1446535261%2C%22http%3A%2F%2Fwww.ims.gov.il%2Fims%2Fall_tahazit%2F%22%5D;has_js=1; _pk_id.97.9fd9=ae8781626f15a489.1445334905.14.1446536942.1446535261.;_pk_ses.97.9fd9=*;__atuvc=68%7C42%2C6%7C43%2C45%7C44;__atuvs=5638605e4d686e5d00c",
    }
    DATA_ = {
    'ims_query[vars][vars_options][4]':'4',
    'ims_query[vars][vars_options][7]':'7',
    'ims_query[vars][vars_options][9]':'9',
    'ims_query[vars][vars_options][1]':'1',
    'ims_query[vars][temp_select]':'Celsius',
    'ims_query[vars][WindSpeedUnit_select]':'Meter_Second',
    'ims_query[vars][TimeZone]':'LST',
    'ims_query[dates][from_date]':'01/11/2014',
    'ims_query[dates][to_date]':'07/11/2014',
    'ims_query[dates2][from_day]':'0',
    'ims_query[dates2][to_day]':'0',
    'ims_query[dates2][from_month]':'0',
    'ims_query[dates2][to_month]':'0',
    'ims_query[dates2][from_year]':'0',
    'ims_query[dates2][to_year]':'0',
    'ims_query[dates2][from_hour]':'0',
    'ims_query[dates2][to_hour]':'0',
    'ims_query[dates2][pick_hour]':'LST',
    'ims_query[stations][right][available][]':'2523',
    'ims_query[data_display][display]':'0',
    'op':'הפקת דו"ח',
    'form_build_id':'form-C9qn1S99e7_G6gK_qVXxnPDH12mwDiqt1ys9CrnpDTI',
    'form_id':'data_IMS_query_form2',
    }

    url='https://data.gov.il//ims'
    init = c.get(url)
    init_cookie_ = init.headers['Set-Cookie'].split(';')[0]
    init_cookie = {}
    init_cookie[init_cookie_.split('=')[0]] = init_cookie_.split('=')[1]
    second = c.get(url,cookies=init_cookie)
    print second.headers
    print second.text
    r = c.post(url,data=DATA_,headers=headers_,cookies= init_cookie )
    #print r.request.headers

   # c.get("http://data.gov.il/ims-results")
    page = c.get('https://data.gov.il/ims')
    print page.text









