# -*-coding:utf8-*-

import time
import re
from lxml import etree
from multiprocessing.dummy import Pool as ThreadPool
import sys
import requests
import random

reload(sys)

sys.setdefaultencoding('utf-8')

head = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36'
}

proxies = {
    'http': 'http://cs:cs@183.163.137.133:9013',
    'https': 'https://cs:cs@183.163.137.133:9013',
}

time1 = time.time()

def getsource(url):
    html = requests.get(url, headers=head)
    selector = etree.HTML(html.text)
    content = selector.xpath("//html")
    time2 = time.time()
    for each1 in content:
        course = each1.xpath('//li[@class="course-one"]')
        if course:
            user_id = each1.xpath('//@data-uid')[0]
            last_page = each1.xpath('//div[@class="page"]/a[last()]/text()')
            if last_page:
                if last_page[0] == u'尾页':
                    last_page_num = int(each1.xpath('//div[@class="page"]/a[last()]/@href')[0].replace('/u/', '')
                                        .replace(str(user_id), '').replace('/courses?page=', ''))
                    for j in range(1, last_page_num):
                        url1 = url + '?page=' + str(j)
                        print(url1)
                        html1 = requests.get(url1, headers=head)
                        selector1 = etree.HTML(html1.text)
                        content1 = selector1.xpath("//html")
                        for each in content1:
                            course_number = len(each.xpath('//li[@class="course-one"]'))
                            if course_number > 0:
                                # <li class="course-one" data-courseid="321" data-uid="2088112">
                                for n in range(course_number):
                                    course_id = each.xpath('//@data-courseid')[n]
                                    learn_id = random.randint(1, 4294967295)
                                    print course_id
                                    course_name = each.xpath('//h3[@class="study-hd"]/a/text()')[n]
                                    course_time = each.xpath('//span[@class="i-left span-common"]/text()')[n].replace(u'已学', '').replace('%', '')
                                    course_learn_time = each.xpath('//span[@class="i-mid span-common"]/text()')[n].replace(u'用时', '').replace(' ', '')\
                                        .replace(u'小时', 'h').replace(u'分', 'm')
                                    user_time_hour = re.findall('.+h', course_learn_time)
                                    if user_time_hour:
                                        user_time_hour = int(user_time_hour[0].replace('h', ''))
                                        user_time_min = int(course_learn_time.replace(str(user_time_hour) + 'h', '').replace('m', ''))
                                        user_time_number = user_time_hour * 60 + user_time_min
                                    else:
                                        user_time_min = int(course_learn_time.replace('m', ''))
                                        user_time_number = user_time_min

                                    f.writelines('(' + str(learn_id) + ',' + str(user_id) + ',' + str(course_id) + ',\'' + str(course_name) + '\',' + str(course_time) + ',' + str(user_time_number) + '),\n')

            print "Succeed: " + str(user_id) + "\t" + str(time2 - time1)
        else:
            print "404: " + url

if __name__ == '__main__':
    f = open('imooc_learn.sql', 'a')
    pool = ThreadPool(1)
    # results = pool.map(getsource, urls)
    urls = []
    for i in range(100000, 1000000):  # 154849  157500. 158605  -
        url = 'http://www.imooc.com/u/' + str(i) + '/courses'
        urls.append(url)
    try:
        results = pool.map(getsource, urls)
    except Exception, e:
        print e
        time.sleep(300)
        results = pool.map(getsource, urls)

    pool.close()
    pool.join()
    f.close()

