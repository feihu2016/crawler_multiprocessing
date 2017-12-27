# -*- coding: utf-8 -*-

import re,os,urllib,urllib2,math,sys,time
from xml.dom.minidom import parseString
import xml.dom.minidom
import multiprocessing
#
#reload(sys)
#sys.setdefaultencoding('utf-8')
#path = os.getcwd()
#misc = path[0:path.index('crawler')+7]
#sys.path.append(misc)
#
from include.helpers import cityname2id
from include.helpers import date_today
from include.helpers import date_now
from include.baidu import *

import MySQLdb
from include.config import DB_SERVER, DB_CONNECT

def SaveXML(data, data_vars):
    item_data = {}
    item_data['cmsid'] = data_vars.get('cmsid')    #cms楼盘id
    item_data['uid'] = ''     #百度的POI标识id
    item_data['bp_name'] = ''  #商家、楼盘名称
    item_data['created_at'] = int(time.time())    #首次入库时间
    item_data['updated_at'] = 0    #最后更新时间
    item_data['crawled_at'] = date_today()   #更新时间
    item_data['location_lat'] = 0.00  #纬度值
    item_data['location_lng'] = 0.00  #经度值
    item_data['address'] = ''  #地址信息
    item_data['price'] = 0  #商户的价格
    item_data['shop_hours'] = ''  #营业时间
    item_data['overall_rating'] = '' #总体评分
    item_data['service_rating'] = '' #服务评分
    item_data['environment_rating'] = ''  #环境评分
    item_data['hygiene_rating'] = ''  #卫生评分
    item_data['comment_num'] = 0   #评论数
    item_data['city_id'] = data_vars.get('city_id') #城市ID
    item_data['city_name'] = data_vars.get('city_name') #城市名称
    item_data['telephone'] = '' #电话
    item_data['distance'] = 0 #距离

    dom = parseString(data)
    root = dom.documentElement
    lists = root.getElementsByTagName('result')
    for xml_data in lists:
        item_data['uid'] = get_XMLElement_data(xml_data.getElementsByTagName('uid'), 'str')
        item_data['bp_name'] = get_XMLElement_data(xml_data.getElementsByTagName('name'), 'str')
        item_data['location_lat'] = get_XMLElement_data(xml_data.getElementsByTagName('lat'), 'float')
        item_data['location_lng'] = get_XMLElement_data(xml_data.getElementsByTagName('lng'), 'float')
        item_data['address'] = get_XMLElement_data(xml_data.getElementsByTagName('address'), 'str')
        #item_data['shop_hours'] = xml_data.getElementsByTagName('price')[0].firstChild.data
        item_data['overall_rating'] = get_XMLElement_data(xml_data.getElementsByTagName('overall_rating'), 'str')
        item_data['comment_num'] = get_XMLElement_data(xml_data.getElementsByTagName('comment_num'), 'int')
        item_data['telephone'] = get_XMLElement_data(xml_data.getElementsByTagName('telephone'), 'str')
        item_data['distance'] = get_XMLElement_data(xml_data.getElementsByTagName('distance'), 'int')

        insert_mod = "insert into %s(%s) values(%s)"
        insert_keys = ''
        insert_values = ''
        for k,v in item_data.items():
            insert_keys += "%s," % k
            if k in ('cmsid', 'created_at', 'updated_at', 'location_lat', 'location_lng', 'price', 'comment_num', 'city_id', 'distance'):
                insert_values += "%s," % v
            else:
                insert_values += "'%s'," % v
        insert_keys = insert_keys.rstrip(',')
        insert_values = insert_values.rstrip(',')
        insert_sql = insert_mod % (data_vars.get('table_name'), insert_keys, insert_values)
        conn = MySQLdb.connect(host=DB_CONNECT['host'],port=DB_CONNECT['port'],user=DB_CONNECT['user'],passwd=DB_CONNECT['passwd'],charset=DB_CONNECT['charset'])
        conn.autocommit(1) 
        cur = conn.cursor()
        cur.execute('use %s' % data_vars.get('database'))
        repeat_sql = "select * from %s where uid='%s' limit 1" % (data_vars.get('table_name'), item_data.get('uid'))
        cur.execute(repeat_sql)
        is_repeat = cur.fetchone()
        if not is_repeat:
            print insert_sql
            try:
                cur.execute(insert_sql)
            except Exception, e:
                print e            
        else:
            print '--------------------- 重复数据 -----------------------------'
            print item_data
        cur.close()

def start_func(num=100):
    var_init = {
        'database' : 'webspider',
        'cmsid' : 0,
        'city_name' : u'北京',
        'city_id' : cityname2id(u'北京'),
        'table_name' : 'ws_beijing_list_17',
        'latlng' : ''
    }

    request_count_total = 0
    request_count_limit = num
    while True:
        conn = MySQLdb.connect(host=DB_CONNECT['host'],port=DB_CONNECT['port'],user=DB_CONNECT['user'],passwd=DB_CONNECT['passwd'],charset=DB_CONNECT['charset'])
        conn.autocommit(1) 
        cur = conn.cursor()
        cur.execute('use %s' % var_init.get('database'))
        cur.execute('select * from building_info where `city_id`=%s and `status`=1 and crawled_at=0 limit 1' % (var_init.get('city_id')))
        baidu_data = cur.fetchone()
        if not baidu_data:
            cur.close()
            print '爬取完毕！'
            sys.stdout.flush()
            break
        update_sql = 'update building_info set crawled_at=%s where id=%s' % (time.time(),baidu_data[0])
        cur.execute(update_sql)
        cur.close()
        var_init['cmsid'] = baidu_data[0]
        var_init['latlng'] = get_latlng(baidu_data[10])

        request_count = 0
        for rr in get_pages(var_init['latlng']):
            SaveXML(rr, var_init)  
            request_count += 1 
            request_count_total += 1     

        else:
            print '============================================================================'
            print '%s: 参照ID：%s | 参照名：%s | 本次参照请求：%s 次 | 共 %s 次请求' %(date_now(), var_init.get('cmsid'), baidu_data[1], request_count, request_count_total)
            print '============================================================================'

        if request_count_total > request_count_limit:
            print '============================================================================'
            print '%s: 本次程序运行已经达到请求上限：%s' % (date_now(), request_count_total)
            sys.stdout.flush()
            break


#进程启动控制器
def start_process(num=20):
    p = multiprocessing.Process(target=start_func, args=(num,))
    p.daemon = True
    p.start()
    return p 

if __name__ == '__main__':

    #多进程维护程序
    process_list = range(2)
    while True:
        n = 0
        for process in process_list:
            if isinstance(process, int):
                process_list[n] = start_process(20)
                print 'process_list[%s] 已启动!' % (n)
            else:
                print 'process_list[%s] 正在执行, 状态：%s' % (n, process_list[n].is_alive())
                if not process_list[n].is_alive():
                    process_list[n].terminate()
                    process_list[n] = start_process(20)
                    print 'process_list[%s] 再次启动!'
            n += 1
        time.sleep(2)






