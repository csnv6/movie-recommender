import json
import sys
import time
import requests
import threading

from config import *
from movie_introduction_spider import movie_introduction_spider_main

# 豆瓣电影（最新）
url = "https://movie.douban.com/j/new_search_subjects"

# 根据时间进行排序
# querystring = {"sort": "R", "range": "0,10", "tags": "电影", "start": "0"}
# 根据热度进行排序
querystring = {"sort": "T", "range": "0,10", "tags": "电影", "start": "0"}


def movide_spider(index):
    """
    将index页下的所有电影链接爬取出来
    :param index: 第几页下的电影(字符串形式)
    :return: 当页的电影信息
    """
    querystring["start"] = str(index * 20)
    response = requests.request("GET", url, headers=headers, params=querystring, cookies=cookies)#,proxies={'https':'111.225.153.240:8089'})

    try:
        if 'data' in json.loads(response.text):
            response_json = json.loads(response.text)['data']
        else:
            response_json = None
        return response_json
    except:
        print(response.text)
        sys.exit(1)


def movie_spider_main():

    # 日志打印
    logging.getLogger('').addHandler(movie_handler)

    index = 0
    movie_num = 0
    while True:
        movie_data_list = movide_spider(index)

        if movie_data_list is not None:
            movie_num += len(movie_data_list)
            thread = threading.Thread(target=movie_introduction_spider_main, args=(movie_data_list,))
            thread.start()

        if index % 10 == 0:
            print("当前" + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()) + "爬取：" + str(movie_num) + " 部电影")
        index += 1

        if index == 100:
            break

        # 防止爬虫访问过度导致IP封锁
        time.sleep(5)
