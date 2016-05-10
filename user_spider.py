# -*-coding:utf8-*-

import requests
import time
import re
from lxml import etree
import MySQLdb
from multiprocessing.dummy import Pool as ThreadPool
import sys

reload(sys)

sys.setdefaultencoding('utf-8')

urls = []

head = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36'
}

time1 = time.time()
for i in range(10000, 3300000):
    url = 'http://www.imooc.com/u/' + str(i)
    urls.append(url)


def getsource(url):
    html = requests.get(url, headers=head)
    selector = etree.HTML(html.text)
    content = selector.xpath("//html")
    time2 = time.time()
    for each in content:
        user_id = int(url.replace('http://www.imooc.com/u/', ''))
        user_name = each.xpath('//h3[@class="user-name clearfix"]/span/text()')
        if user_name:
            user_name = user_name[0]
            user_sex = each.xpath('//span[@class="gender "]/@title')
            if user_sex:
                user_sex = user_sex[0]
            else:
                user_sex = u'女'
            user_place = ''
            user_job = re.findall(r'\r\n             \r\n                \r\n            .+\r\n        ', each.xpath('//p[@class="about-info"]/text()')[1])
            if user_job:
                user_job = user_job[0].replace('\r\n', '').replace(' ', '')
                user_place = each.xpath('//p[@class="about-info"]/text()')[1].replace('\r\n', '').replace(' ', '').replace(user_job, '')
            elif re.findall(r'\r\n    \r\n     \r\n    \r\n        \r\n            .+\r\n        ', each.xpath('//p[@class="about-info"]/text()')[1]):
                # place is null
                user_job = re.findall(r'\r\n    \r\n     \r\n    \r\n        \r\n            .+\r\n        ', each.xpath('//p[@class="about-info"]/text()')[1])[0]\
                    .replace('\r\n', '').replace(' ', '')
                user_place = ''
            elif re.findall(r'\r\n     \r\n    \r\n                    \r\n                \r\n            .+\r\n        ', each.xpath('//p[@class="about-info"]/text()')[1]):
                # place is a city
                user_job = re.findall(r'\r\n     \r\n    \r\n                    \r\n                \r\n            .+\r\n        ', each.xpath('//p[@class="about-info"]/text()')[1])[0]\
                    .replace('\r\n', '').replace(' ', '')
                user_place = each.xpath('//p[@class="about-info"]/text()')[1].replace('\r\n', '').replace(' ', '').replace(user_job, '')
            else:
                user_job = ''
                user_place = each.xpath('//p[@class="about-info"]/text()')[1].replace('\r\n', '').replace(' ', '').replace(user_job, '')
            user_head = each.xpath('//div[@class="user-pic"]/img/@src')[0]
            user_word = each.xpath('//p[@class="user-desc"]/text()')
            if user_word:
                user_word = user_word[0]
            else:
                user_word = ''
            user_time = each.xpath('//div[@class="item study-time"]/em/text()')[0].replace(' ', '')
            user_time_number = 0
            user_time_2 = user_time.replace(u'小时', 'h').replace(u'分', 'm')
            user_time_hour = re.findall('.+h', user_time_2)
            if user_time_hour:
                user_time_hour = int(user_time_hour[0].replace('h', ''))
                user_time_min = int(user_time_2.replace(str(user_time_hour) + 'h', '').replace('m', ''))
                user_time_number = user_time_hour * 60 + user_time_min
            else:
                user_time_min = int(user_time_2.replace('m', ''))
                user_time_number = user_time_min
            user_score = int(each.xpath('//div[@class="item integral"]/em/text()')[0])
            user_exp = int(each.xpath('//div[@class="item experience"]/em/text()')[0])
            last_page = each.xpath('//div[@class="page"]/a[last()]/text()')
            user_learn_number = 0
            if last_page:
                if last_page[0] == u'尾页':
                    last_page_num = int(each.xpath('//div[@class="page"]/a[last()]/@href')[0].replace('/u/', '').replace(str(user_id), '').replace('/courses?page=', ''))
                    user_learn_number = (last_page_num - 1) * 20
                    html_2 = requests.get('http://www.imooc.com' + each.xpath('//div[@class="page"]/a[last()]/@href')[0], headers=head)
                    selector_2 = etree.HTML(html_2.text)
                    content_2 = selector_2.xpath("//html")
                    for each_2 in content_2:
                        user_learn_number_last = len(each_2.xpath('//li[@class="course-one"]'))
                        user_learn_number = user_learn_number + user_learn_number_last
            else:
                if each.xpath('//li[@class="course-one"]'):
                    user_learn_number = len(each.xpath('//li[@class="course-one"]'))
                else:
                    user_learn_number = 0
            print "Succeed: " + str(user_id) + "\t" + str(time2 - time1)
            try:
                conn = MySQLdb.connect(host='localhost', user='root', passwd='', port=3306, charset='utf8')
                cur = conn.cursor()
                conn.select_db('python')
                cur.execute('INSERT INTO imooc_user VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                            [user_id, user_name, user_sex, user_place, user_job, user_head, user_word,
                             user_time, user_score, user_exp, user_learn_number, user_time_number])
            except MySQLdb.Error, e:
                print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        else:
            print "404: " + url


pool = ThreadPool(8)
# results = pool.map(getsource, urls)
try:
    results = pool.map(getsource, urls)
except Exception, e:
    print e
    time.sleep(300)
    results = pool.map(getsource, urls)

pool.close()
pool.join()
