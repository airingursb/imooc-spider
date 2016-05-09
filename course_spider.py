# -*-coding:utf8-*-

import requests
import time
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
for i in range(1, 1000):
    url = 'http://www.imooc.com/view/' + str(i)
    urls.append(url)

def getsource(url):
    html = requests.get(url, headers=head)
    selector = etree.HTML(html.text)
    content = selector.xpath("//html")
    time2 = time.time()
    for each in content:
        course_id = int(url.replace('http://www.imooc.com/view/', ''))
        course_name = each.xpath('//h2[@class="l"]/text()')
        if course_name:
            course_name = course_name[0]
            course_url = url
            course_type_other = each.xpath('//div[@class="path"]/a[3]/text()')
            if course_type_other:
                course_type = each.xpath('//div[@class="path"]/a[2]/text()')[0] + '-' + each.xpath('//div[@class="path"]/a[3]/text()')[0]
            else:
                course_type = 'other-' + each.xpath('//div[@class="path"]/a[2]/span/text()')[0]
            course_number = each.xpath('//div[@class="static-item"]/span[@class="meta-value"]/strong/text()')[0]
            course_time_min = each.xpath('//div[@class="static-item static-time"]/span[@class="meta-value"]/strong[2]/text()')
            course_time = 0
            if course_time_min:
                course_time_hour = each.xpath('//div[@class="static-item static-time"]/span[@class="meta-value"]/strong[1]/text()')[0]
                course_time = int(course_time_hour) * 60 + int(course_time_min[0])
            else:
                course_time = int(each.xpath('//div[@class="static-item static-time"]/span[@class="meta-value"]/strong[1]/text()')[0])
            course_hard = each.xpath('//div[@class="static-item "]/span[@class="meta-value"]/strong/text()')[0]
            course_score = each.xpath('//div[@class="satisfaction-degree-info"]/h4/text()')[0]
            course_teacher = each.xpath('//div[@class="box mb40"]/div[1]/span[@class="tit"]/a/text()')
            if course_teacher:
                course_teacher = course_teacher[0]
            else:
                course_teacher = ''
            course_teacher_job = each.xpath('//div[@class="box mb40"]/div[1]/span[@class="job"]/text()')
            if course_teacher_job:
                course_teacher_job = course_teacher_job[0]
            else:
                course_teacher_job = ''
            print "Succeed: " + str(course_id) + "\t" + str(time2 - time1)
            try:
                conn = MySQLdb.connect(host='localhost', user='root', passwd='', port=3306, charset='utf8')
                cur = conn.cursor()
                conn.select_db('python')
                cur.execute('INSERT INTO imooc_course VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                            [course_id, course_name, course_url, course_type, course_number, course_time, course_hard,
                             course_score, course_teacher, course_teacher_job])
            except MySQLdb.Error, e:
                print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        else:
            print "404: " + url


pool = ThreadPool(1)
results = pool.map(getsource, urls)

pool.close()
pool.join()
