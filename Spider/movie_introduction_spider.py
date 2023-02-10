import time
import redis
import pymongo
import requests
from lxml import etree

from config import *
from util import call_eip_worker_api


def movie_introduction_saver(movie_data_list):
    """
    将解析出来的电影信息入库到redis和mongo中
    :param movie_data_dict: 需要入库的电影信息
    :return:
    """
    try:
        # 这里可以借助redis实现数据去重
        # redis_client = redis.Redis(host='127.0.0.1')
        # redis_client = redis.Redis(host='192.168.41.6')
        # if not redis_client.hget('douban_movie', movie_data_dict['id']):
        #     redis_client.hset('douban_movie', movie_data_dict['id'], movie_data_dict['rate'])

        mongo_client = pymongo.MongoClient(host='127.0.0.1', port=27017)
        mongo_db = mongo_client['douban']
        mongo_collection = mongo_db['movie']
        mongo_collection.insert_many(movie_data_list, ordered=False)
    except Exception as ex:
        print(ex)


def movie_introduction_parser(html_page, movie_data_dict):
    """
    对电影详情页进行解析
    :param html_page: 电影详情页
    :return:
    """
    html = etree.HTML(html_page)

    try:
        year = html.xpath('//*[@class="year"]/text()')
        if year:
            year = year[0][1:-1]

            # 由于是按照时间顺序排序的，所以当出现十年以外的电影时，其实是已经爬取完近十年的电影了，退出程序/非时间排序的时候，直接返回
            if int(year) < 2012:
                # sys.exit(1)
                return
        else:
            # 对于无法获取到年份的电影页，放弃爬取
            return

        directors = html.xpath('//div[@id="info"]//*[@rel="v:directedBy"]/text()')
        types = html.xpath('//*[@id="info"]//*[@property="v:genre"]/text()')

        rate = html.xpath('//*[@property="v:average"]/text()')
        if rate:
            rate = rate[0]

        runtime = html.xpath('//*[@id="info"]//*[@property="v:runtime"]/text()')
        if runtime:
            runtime = runtime[0]

        synopsis = html.xpath('//*[@class="all hidden"]/text()')
        # 如果剧情简介没有"更多简介部分"，爬取没有收起来部分的剧情简介
        if not synopsis:
            synopsis = html.xpath('//*[@property="v:summary"]/text()')

        country_or_region = html.xpath('//*[@id="info"]//*[text()="制片国家/地区:"]')
        if country_or_region:
            country_or_region = country_or_region[0].tail.strip()

        language = html.xpath('//*[@id="info"]//*[text()="语言:"]')
        if language:
            language = language[0].tail.strip()

        aliases = html.xpath('//*[@id="info"]//*[text()="又名:"]')
        if aliases:
            aliases = aliases[0].tail.strip()

        screenwriters = html.xpath('//*[@id="info"]//*[text()="编剧"]/following-sibling::*/a/text()')
        casts = html.xpath('//*[@id="info"]//*[text()="主演"]/following-sibling::*/a/text()')

        tags = html.xpath('//*[@class="tags-body"]/a/text()')

        movie_data_dict.update({
            'directors': directors,
            'screenwriters': screenwriters,
            'casts': casts,
            'types': types,
            'tags': tags,
            'year': year,
            'runtime': runtime,
            'rate': rate,
            'language': language,
            'country_or_region': country_or_region,
            'aliases': aliases,
            'synopsis': synopsis
        })

        return movie_data_dict

    except Exception as ex:
        logging.warning(movie_data_dict['id'] + ': ' + movie_data_dict['title'] + '\t' + ex)


def movie_introduction_spider(movie_data):
    """
    对得到的电影页上的所有电影进行爬取
    :param movie_data:
    :return:
    """
    movie_id = movie_data['id']
    title = movie_data['title']
    url = movie_data['url']
    cover = movie_data['cover']

    response = requests.request("GET", url, headers=headers, cookies=cookies)

    # if response.status_code != 200:
    #     # call_eip_worker_api()
    #     time.sleep(180)
    #     logging.info('更换eip')
    #
    #     return movie_introduction_spider(movie_data)

    html_page = response.content

    # 防止IP封锁，适当减少频率
    time.sleep(2)

    movie_data_dict = movie_introduction_parser(html_page, {'title': title, 'cover': cover, 'id': movie_id})

    return movie_data_dict

def movie_introduction_spider_main(movie_data_list):
    """
    电影详情页爬虫入口
    :param movie_data_list:
    :return:
    """
    movie_data_to_mongo = []
    for movie_data in movie_data_list:
        movie_data_dict = movie_introduction_spider(movie_data)
        if movie_data_dict is not None:
            movie_data_to_mongo.append(movie_data_dict)
    # print(movie_data_to_mongo)
    size = len(movie_data_to_mongo)
    # 采用批写入，需注意不能为空，也不能有None的元素
    if size > 0:
        movie_introduction_saver(movie_data_to_mongo)
    print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()) + "写入mongo完成当前批:" + str(size))

