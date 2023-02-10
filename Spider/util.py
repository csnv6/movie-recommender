import os
import time
import pymongo
import threading
from urllib import parse

"""一些常用的模块放在这里"""


def deduplicate():
    """
    文本去重
    :return:
    """
    with open('user_list.txt', 'r') as file_stream:
        user_url_list = [line.strip() for line in file_stream.readlines()]

    # 文件内的用户url去重
    user_url_list = list(set(user_url_list))

    with open('user_list.txt', 'w') as file_stream:
        for user_url in user_url_list:
            file_stream.write(user_url + '\n')


def flush_user_list():
    """
    去除user_list.txt文件中已经入库的那部分用户url
    :return:
    """
    client = pymongo.MongoClient(host='127.0.0.1')
    db = client['douban']
    col = db['user']

    id_list = [user_id['id'] for user_id in col.find({}, {'_id': 0, 'id': 1})]

    with open('user_list.txt', 'r') as file_stream:
        user_url_list = [line.strip() for line in file_stream.readlines()]

    for user_id in id_list:
        url = 'https://www.douban.com/people/'
        user_url = parse.urljoin(url, user_id) + '/'
        if user_url in user_url_list:
            user_url_list.remove(user_url)

    with open('user_list.txt', 'w') as file_stream:
        for user_url in user_url_list:
            file_stream.write(user_url + '\n')
