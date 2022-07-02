#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" Douban house group crawler using proxy pool strategy

@author      : QiangZiBro (qiangzibro@gmail.com)
@created     : 12/06/2022
@filename    : main
"""
import re
import time
import random
import requests
from bs4 import BeautifulSoup
import datetime
from dateutil import parser
from fake_useragent import UserAgent
from config import keywords, groups, N_DAYS
from multiprocessing import Queue, Process
from proxy import benchmark
from concurrent.futures import ThreadPoolExecutor
from utils import batch_insert
from send_qq_mail import send_qq_mail

ua = UserAgent()


def get_proxy():
    return requests.get("http://127.0.0.1:5010/get/").json()


def delete_proxy(proxy):
    requests.get("http://127.0.0.1:5010/delete/?proxy={}".format(proxy))


def get_params(use_proxy=False):
    # use proxy from https://github.com/jhao104/proxy_pool
    if use_proxy:
        try:
            #proxy = get_proxy().get("proxy")
            #proxy = '139.9.64.238:443'
            if cache == [] or not queue.empty():
                print("getting ip...")
                proxy = queue.get()
                cache.append(proxy)
            else:
                proxy = random.choice(cache)
            proxies = {
                "http": "http://{}".format(proxy),
                "https": "https://{}".format(proxy),
            }
        except:
            proxies = proxy = None
    else:
        proxies = proxy = None
    # get param
    params = {
        "headers": {
            "User-Agent": ua.random,
            "Referer": "http://www.douban.com/",
        },
        "proxies": proxies,
        "timeout": 3,
    }
    return params, proxy


def parse_entry(url):
    retry_count = 2
    params, proxy = get_params(use_proxy=True)
    while retry_count > 0:
        try:
            html = requests.get(url, **params)
            # test available
            soup = BeautifulSoup(html.content.decode("utf8"),"html.parser")
            tables = soup.findChildren("table")
            if html.status_code == 200 and tables != []:
                return html.content.decode("utf8")
        except Exception:
            retry_count -= 1
    # 删除代理池中代理
    delete_proxy(proxy)
    cache.remove(proxy)
    return None


def parse_table(html):
    # parse infos in a page
    # result [(timestap,name,title,link)]
    result = set()
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.findChildren("table")
    my_table = tables[0]
    rows = my_table.findChildren(["tr"])[1:]
    # print(rows)
    for row in rows:
        cells = row.findChildren("td")
        link = cells[0].a.attrs["href"]
        title = cells[0].a.attrs["title"]
        timestap = cells[-1].text
        n = int(re.findall(r'\d+', link)[0])
        name = cells[1].text.replace("\n", "")
        result.add((n, timestap, name, title, link))
    return result


# Use cases:
# url = "https://www.douban.com/group/shanghaizufang/discussion?start=75&type=new"
# html = parse_entry(url)
# pages = parse_table(html)


def parse_with_strategy(group, keywords, days=3):
    # retrieve with condition that:
    # - post time is less than days
    # - has keywords in title
    is_target_post = lambda page_item: any(word in page_item[3] for word in keywords)
    in_n_days = (
        lambda page_item: parser.parse(page_item[1]) + datetime.timedelta(days=days)
        > datetime.datetime.now()
    )

    url = "https://www.douban.com/group/" + group + "/discussion?start={}&type=new"
    i = 0
    result = set()
    while True:
        print("parsing {}".format(url.format(i)))
        html = parse_entry(url.format(i))
        if html is None:
            print("Being blocked, try again!")
            continue
        time.sleep(random.randint(0, 1))
        pages = parse_table(html)
        pages = list(filter(in_n_days, pages))
        if pages == []:
            # no more page satisfied
            break
        pages = set(filter(is_target_post, pages))
        result |= pages
        i += 25
    return list(result)

def send_msg(news):
    s = """
距上次爬取豆瓣相关住房信息，爬虫程序为你搜集到了更多信息

"""
    for w in keywords:
        s += "---------------------------------------------------------------------------------\n"
        s += w
        s += "---------------------------------------------------------------------------------\n"
        for d in news:
            if w in d[3]:
                s += d[3] + "\n"
                s += d[4] + "\n"
    print(s)
    send_qq_mail(s)
if __name__ == "__main__":
    # 爬虫进程所使用的代理ip，全局变量
    cache = []
    # 另一个进程测可用ip，全局变量
    queue = Queue()
    p = Process(target=benchmark, args=(queue,))
    p.start()
    # 爬虫开始
    result = []
    news = []
    for group in groups:
        res = parse_with_strategy(group, keywords, N_DAYS)
        news += batch_insert(res)
        result += res
    # 处理新获得的消息，比如发邮件
    send_msg(news)
    # Thread pool method 
    # Thread not safe? - cache modified between threads
    # Still very slow because of proxy pool
    #with ThreadPoolExecutor(max_workers=len(groups)) as executor:
    #    for res in executor.map(task_func, groups):
    #        result.extend(res)

    # 爬虫结束
    p.terminate()
