import re
import time
import math
import json
import requests
from bs4 import BeautifulSoup

########################################################################################################################
# categories and step, breakpoint continue:
########################################################################################################################
cate = 'cs'
step = 2000
fin = 68500

########################################################################################################################
# spider settings
########################################################################################################################
crawl_delay = 0  # 注：在程序运行起来之后，最好要保证实际爬取间隔大于1s 这里的delay是对数据处理时间的一个弥补 不建议修改成并发操作

########################################################################################################################
# patterns:
########################################################################################################################
title_pattern = r'<span class="descriptor">Title:</span> (.*?)</div>'
comments_pattern = r'<span class="descriptor">Comments:</span> (.*)'

authors_pattern = r'<span class="descriptor">Authors:</span>(.*?)</div>'
author_list_pattern = r'>(.*?)</a>'

subject_pattern = r'<span class="descriptor">Subjects:</span> <span class="primary-subject">(.*)'
subject_list_pattern = r'[(](.*?)[)]'

abstract_url_pattern = r'<a href="(.*)" title="Abstract">'
abstract_pattern = r'<span class="descriptor">Abstract:</span> (.*?)</blockquote>'

########################################################################################################################
# cate total number and spider url
########################################################################################################################
url = "https://export.arxiv.org/list/" + cate + "/20"
res = requests.get(url=url)
total_num = int(re.findall(pattern=r'total of (.*?) entries', string=res.text)[0])

url_list = []
for i in range(math.ceil(total_num / step)):
    url_list.append("https://export.arxiv.org/list/cs/20?skip=" + str(i * step) + "&show=" + str(step))

########################################################################################################################
# crawl
########################################################################################################################
with open("arxiv-metadata-oai-cs2020.json", 'a') as f:
    count, print_fre, start_time = 0, 500, time.time()
    for url in url_list:
        res = requests.get(url)

        soup = BeautifulSoup(res.text, 'lxml')
        dt_items, dd_items = soup.find_all("dt"), soup.find_all("dd")

        for dt, dd in zip(dt_items, dd_items):
            count += 1
            if count < fin:
                continue
            elif count == fin:
                print(count, "Finished. Start at", count + 1)
                continue

            item_dic = {}
            dt_text = str(dt)
            abstract_url = "https://export.arxiv.org" + re.findall(abstract_url_pattern, dt_text)[0]
            arxiv_id = abstract_url[29:]
            item_dic["id"] = arxiv_id

            dd_text = str(dd)
            title = re.findall(title_pattern, dd_text, re.DOTALL)
            item_dic["title"] = title[0].strip('\n')

            authors = re.findall(authors_pattern, dd_text, re.DOTALL)[0]
            author_list = re.findall(author_list_pattern, authors)
            item_dic["authors_list"] = author_list

            comment = re.findall(comments_pattern, dd_text)
            if comment:
                item_dic["comments"] = comment[0]
            else:
                item_dic["comments"] = None

            subject = re.findall(subject_pattern, dd_text)[0]
            subject_list = re.findall(subject_list_pattern, subject)
            item_dic["categories"] = ' '.join((x for x in subject_list))

            res = requests.get(abstract_url)

            abstract = re.findall(abstract_pattern, res.text, re.DOTALL)
            abstract = abstract[0].replace('\n', ' ').strip()
            item_dic["abstract"] = abstract

            # write
            json.dump(item_dic, f)
            f.write('\n')
            f.flush()

            print(count, '/', total_num, item_dic)
            if not count % print_fre:
                time_cost = time.time() - start_time
                print("About %.3f" % (time_cost / print_fre), "second per item.")
                print("Remain time: %.3f" % ((total_num - count) * (time_cost / print_fre) / 3600), "hours")
                start_time = time.time()

            time.sleep(crawl_delay)
