# -*- coding: utf-8 -*-

import re,os,urllib,urllib2,math,sys,time
from xml.dom.minidom import parseString
import xml.dom.minidom

reload(sys)
sys.setdefaultencoding('utf-8')

def get_pages(location='0,0', radius=500, output='xml', page_size=20):
    
    url_vars = {
        'query' : u'银行$酒店',  #检索关键字。周边检索和矩形区域内检索支持多个关键字并集检索，不同关键字间以$符号分隔，最多支持10个关键字检索。如:”银行$酒店”
        'location' : location, # lat<纬度>,lng<经度>
        'radius' : radius,  #周边检索半径，单位为米。
        'output' : output, #输出格式为json或者xml
        'ak' : '***',
        'page_num' : 0,
        'page_size' : page_size
    }

    zhoubian_url_mode = 'http://api.map.baidu.com/place/v2/search?query=%(query)s&location=%(location)s&radius=%(radius)s&output=%(output)s&ak=%(ak)s&page_num=%(page_num)s&page_size=%(page_size)s&scope=2'
    query_keys = [u'美食', u'中餐厅', u'外国餐厅', u'小吃快餐店', u'蛋糕甜品店', u'咖啡厅', u'茶座', u'酒吧',
    u'购物', u'便利店', u'超市', u'商场', u'商铺', u'集市',
    u'酒店', u'星级酒店', u'快捷酒店', u'公寓式酒店', u'酒店', u'宾馆', u'招待所',
    u'写字楼', u'公司', u'写字楼', u'工业园区', u'产业园区',
    u'咖啡厅', u'咖啡馆', u'咖啡厅', 'cafe', 'coffee',
    u'茶楼', u'茶楼', u'茶吧', u'水吧', u'茶座'
    ]

    for query_key in query_keys:
        url_vars['query'] = query_key
        zhoubian_url = zhoubian_url_mode % url_vars        
        print zhoubian_url
        print '-------------------------URL---------------------------------'
        sys.stdout.flush()
        response = urllib2.urlopen(zhoubian_url)
        result = response.read()        
        list_count = re.search('<total>(\d+)</total>', result).groups()[0]
        list_count = int(list_count)
        if list_count>0:
            yield result
        else:
            continue
        page = int(math.ceil(list_count/url_vars.get('page_size')))
        for p in range(1, page):
            page_str = 'page_num=%s' % p
            current_url = re.sub('page_num=\d+', page_str, zhoubian_url)
            print current_url
            print '-------------------------URL:%s---------------------------------' % (p)
            sys.stdout.flush()
            response_page = urllib2.urlopen(current_url)
            result_page = response_page.read()
            response_page.close()
            yield result_page

#type: str/int/float
def get_XMLElement_data(element, type):
    if type=='str':
        re_data = element[0].firstChild.data if len(element)>0 else ''
        return re_data.replace('\'', '#')
    elif type=='int':
        return int(element[0].firstChild.data) if len(element)>0 else 0
    elif type=='float':
        return float(element[0].firstChild.data) if len(element)>0 else 0.00

def get_latlng(baidu_data):
    if baidu_data:
        baidu_data = baidu_data[1:]
        baidu_data = re.sub(r'(\d+\.\d+),(\d+\.\d+)', r'\2,\1', baidu_data)
        return baidu_data
    else:
        return '0,0'
    






