# encoding=utf-8
# __author__ = 'nick'
import mechanize
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

stations_numbers = ['4798', '8643', '8642', '4441', '9974', '9972', '6251', '301', '6262',
  '3014', '4741', '3541', '7841', '7151', '2523', '2520', '9445', '8730',
  '3891', '9415', '1192', '4738', '3551', '7333', '4756', '8263', '1980',
  '6301', '1160', '1381', '6101', '600', '850', '3113', '9630', '4696', '5371',
  '9772', '7020', '6771', '6770', '8472', '4087', '8736', '7416', '9181', '6663',
  '6055', '9460','8229', '4191', '5771', '8243', '7933', '3502', '5201', '6490',
  '3220', '7220', '9571', '8224', '8204', '9476', '1546', '1121', '1054',
  '8690', '5811', '7822', '9713', '7011', '7972', '9111', '4642', '4640',
  '3081', '6247', '21', '7246', '190', '9376', '8206', '7490', '1990',
  '5358', '5843', '2410']

stations_names = ['AVNE ETAN','AYYELET HASHAHAR','AYYELET HASHAHAR MAN','ELON' ,'ELAT','ELAT MAN','ITAMAR','AFEQ','ARIEL',
'ASHDOD PORT','ESHHAR','ASHQELON PORT','BEER SHEVA','BEIT JIMAL','BET DAGAN', 'BET DAGAN MAN','BET HAARAVA', 'BET ZAYDA',
'BESOR FARM', 'GILGAL','GALED', 'GAMLA', 'GAT','DOROT','DEIR HANNA', 'DAFNA','HAKFAR HAYAROK',
'HAR HARASHA', 'ZIKHRON YAAQOV', 'HADERA PORT','EDEN FARM','HAIFA REFINERIES','HAIFA TECHNION','HAFEZ HAYYIM','HAZEVA','HARASHIM','YAVNEEL',
'YOTVATA','JERUSALEM GIVAT RAM', 'JERUSALEM CENTRE', 'JERUSALEM CENTRE MAN', 'KEFAR BLUM','KEFAR GILADI', 'KEFAR NAHUM','LAHAV','MASSADA', 'MAALE ADUMMIM',
'MAALE GILBOA','METZOKE DRAGOT','MIZPE RAMON', 'MEROM GOLAN PICMAN', 'MERHAVYA','NEOT SMADAR','NEVATIM','NEGBA','NEWE YAAR', 'NAHSHON',
'NIZZAN', 'NETIV HALAMED HE', 'SEDOM', 'AVDAT','EZUZ', 'EN GEDI', 'EN HAHORESH', 'EN HASHOFET', 'EN KARMEL',
'AMMIAD', 'AFULA NIR HAEMEQ', 'ARAD','PARAN', 'ZOVA', 'ZOMET HANEGEV', 'ZEMAH', 'ZEFAT HAR KENAAN', 'ZEFAT HAR KENAAN MAN',
'QEVUZAT YAVNE','QARNE SHOMERON', 'ROSH HANIQRA','ROSH ZURIM','SHAVE ZIYYON', 'SEDE ELIYYAHU','SEDE BOQER','SHANI', 'SHAARE TIQWA',
'TAVOR KADOORIE','TEL YOSEF' ,'TEL AVIV COAST']

i=0
while(i<len(stations_numbers)):
    urltosite = 'https://data.gov.il/ims'
    br = mechanize.Browser()
    br.set_handle_redirect(mechanize.HTTPRedirectHandler)
    br.addheaders = [('User-agent', 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36')]
    br.open(urltosite)
    br.open(urltosite)

    #add this for requests heads.
    #br.set_debug_http(True)

    br.form = list(br.forms())[0]
    br.form.set_all_readonly(False)
    # print br.form
    br.form["ims_query[vars][vars_options][4]"] = ['4']  # this takes the temperature
    br.form["ims_query[vars][vars_options][10]"] = False
    br.form["ims_query[vars][vars_options][5]"] = False
    br.form["ims_query[vars][vars_options][11]"] = False
    br.form["ims_query[vars][vars_options][6]"] = False
    br.form["ims_query[vars][vars_options][12]"] = False
    br.form["ims_query[vars][vars_options][7]"] = ['7']
    br.form["ims_query[vars][vars_options][13]"] = False
    br.form["ims_query[vars][vars_options][9]"] = ['9']
    br.form["ims_query[vars][vars_options][14]"] = False
    br.form["ims_query[vars][vars_options][8]"] = False
    br.form["ims_query[vars][vars_options][15]"] = False
    br.form["ims_query[vars][vars_options][1]"] = ['1']
    br.form["ims_query[vars][vars_options][16]"] = False
    br.form["ims_query[vars][vars_options][2]"] = False
    br.form["ims_query[vars][vars_options][17]"] = False
    br.form["ims_query[vars][vars_options][18]"] = False

    br.form['ims_query[vars][temp_select]'] = ['Celsius']                   # chooses celsius
    br.form['ims_query[vars][WindSpeedUnit_select]'] = ['Metera_Second']     # uses meters
    br.form['ims_query[vars][TimeZone]'] = ['LST']                          # uses UTC
    br.form['ims_query[dates][from_date]'] = '01/01/2013'                   # date picker is a string
    br.form['ims_query[dates][to_date]'] = '01/11/2015'                     # same here
    br.form['ims_query[dates2][pick_hour]'] = ['LST']                       # uses UTC

    br.submit('op')                                                         # opens the stations menu!!!

    # br.form['ims_query[data_display][display]'] = ['1']

    br.form = list(br.forms())[0]

    br.form['ims_query[stations][right][available][]']=[stations_numbers[i]]
    br.submit('op')

    br.form = list(br.forms())[0]
    br.form['ims_query[stations][right][available][]']=[stations_numbers[i]] #submit again, looks buggy but whatever.

    br.submit(nr=2)
    #response = br.follow_link(text_regex=r"json")
    response = br.follow_link(text_regex=r"csv")   ## here we need to fix encoding
    # print response.read()
    file_name ='/opt/skyspark-2.1.12/db/newnewnew/io/'+stations_names[i]+'.csv'
    with open(file_name, 'w+') as infile:
      infile.write(response.read())


    f=open(file_name, 'r')
    lines=f.readlines()
    f.close()

    f = open(file_name, 'w')
    for line in lines:
        f.write(unicode(line.rstrip(),"cp866").encode("utf-8") )
        f.write('\n')
    f.close()
    i = i+1