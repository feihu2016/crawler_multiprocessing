# -*- coding=utf-8 -*-
import time
from datetime import datetime
from datetime import date
from datetime import timedelta
import string
import re
import hashlib

def date_today():
    return datetime.now().strftime("%Y-%m-%d")
def date_now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def date_timestamp():
    a = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    b = time.strptime(a, "%Y-%m-%d %H:%M:%S")
    return int(time.mktime(b))

def strtime_fmt_datetime(timestr, format=None):
    if format is None:
        format = "%Y-%m-%d %H:%M:%S"
    return datetime.strptime(timestr, format)

def whichday(d):
    today = date.today()
    if d < 0:
        dt = today + timedelta(days=d)
    else:
        dt = today - timedelta(days=d)
    format = "%Y-%m-%d"
    return strtime_fmt_datetime(str(dt), format)

def cityname2id(cityname):
    citys = {u'北京': 12, u'上海': 13, u'广州': 16, u'深圳': 17, u'成都': 45, u'大连': 56, u'武汉': 194, u'西安': 176, u'杭州':26,u'天津':14}
    # cityname = cityname.encode('utf8')
    if cityname in citys:
        return citys[cityname]
    else:
        return 0

def cityInfo(pinyin):

    cityinfo = {
        'bj' : {'city_id' : 12, 'city_name' : u'北京'},
        'beijing' : {'city_id' : 12, 'city_name' : u'北京'},
        'sh' : {'city_id' : 13, 'city_name' : u'上海'},
        'shanghai' : {'city_id' : 13, 'city_name' : u'上海'},
        'gz' : {'city_id' : 16, 'city_name' : u'广州'},
        'guangzhou' : {'city_id' : 16, 'city_name' : u'广州'},
        'sz' : {'city_id' : 17, 'city_name' : u'深圳'},
        'shenzhen' : {'city_id' : 17, 'city_name' : u'深圳'},
        'cd' : {'city_id' : 45, 'city_name' : u'成都'},
        'chengdu' : {'city_id' : 45, 'city_name' : u'成都'},
        'dl' : {'city_id' : 56, 'city_name' : u'大连'},
        'dalian' : {'city_id' : 56, 'city_name' : u'大连'},
        'wh' : {'city_id' : 194, 'city_name' : u'武汉'},
        'wuhan' : {'city_id' : 194, 'city_name' : u'武汉'},
        'xa' : {'city_id' : 176, 'city_name' : u'西安'},
        'xian' : {'city_id' : 176, 'city_name' : u'西安'},
        'hz' : {'city_id' : 26, 'city_name' : u'杭州'},
        'hangzhou' : {'city_id' : 26, 'city_name' : u'杭州'},
        'tj' : {'city_id' : 14, 'city_name' : u'天津'},
        'tianjin' : {'city_id' : 14, 'city_name' : u'天津'},
    }

    return  cityinfo[pinyin]  if pinyin in cityinfo else False


def strip(listd):
    return string.strip(''.join(listd))

# from collections import OrderedDict
# ranks = OrderedDict()
# def getStreetInfo(url):
#     r = re.search(r'searchlist-d(\d+)-c(\d+)(/y(\d+))?', url)

#     district_id = r.group(1)
#     street_id = r.group(2)
#     page_id = r.group(4) if r.group(4) else 1
#     return { "district_id": district_id, "street_id": street_id, "page_id": page_id}

# def getBuildingId(url):
#     r = re.search(r'/detail-(\d+).html', url)
#     if r:
#         return int(r.group(1))
#     return 0

# def setRank(url, url_list):
#     pagesize = 10
#     pageinfo = getStreetInfo(url)
#     idlist = [ getBuildingId(url) for url in url_list]
#     for id in idlist:
#         key = str(pageinfo['district_id']) + ':' + str(pageinfo['street_id']) + ':' + str(id)
#         rank =  int(int(pageinfo['page_id']) - 1) * pagesize  + idlist.index(id) + 1
#         ranks[key] = rank
#     return ranks

# def getRank(url, id):
#     pageinfo = getStreetInfo(url)
#     key = str(pageinfo['district_id']) + ':' + str(pageinfo['street_id']) + ':' + str(id)

#     return  ranks[key] if key in ranks else 0

def retstr(listi, index = 0):
    if listi and isinstance(listi, list) and len(listi) > index:
        return listi[index] #.strip()
    return ''

def retstr_replace(listi, index = 0, default = ''): 
    if isinstance(listi, list) and len(listi) > index:
        return listi[index].replace('\r','').replace('\n','').replace(' ','').strip().rstrip('\\')
    return default

def retcnstr(stri):
    if not stri:
        return ''
    stri1 = unicode(stri, encoding='utf-8')
    return stri1.encode('utf-8')

def hash_md5(stri):
    m = hashlib.md5()
    m.update(stri)
    return m.hexdigest()

def hash_sha1(stri):
	return  hashlib.sha1(stri).hexdigest()

def hash_sha1_sub(stri, length):
	return hash_sha1(stri)[:length]

def is_str(s):
    return isinstance(s, str)

def is_unicode(s):
    return isinstance(s, unicode)

def test():
    print "当前时间戳:" , date_timestamp()
    print "今天日期：", date_today()
    print "当前时间：", date_now()
    # print "北京的城市ID:", cityid2name("北京")
    s = [u"\n\nhello\t\tworld\n\n\t\t!"]
    print s[0]
    print "from <%s> to <%s>" % (s[0], strip(s[0]))
    print hash_sha1_sub('哈哈', 7)


if __name__ == '__main__':
    test()
