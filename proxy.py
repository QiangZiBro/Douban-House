import re
import requests
from lxml import etree
from fake_useragent import UserAgent
from multiprocessing import Pool, Queue, Process
ua = UserAgent()

def get_params(use_proxy=False):
    # use proxy from https://github.com/jhao104/proxy_pool
    if use_proxy:
        try:
            proxy = get_proxy().get("proxy")
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

def test_proxy(params):
    https_url = 'https://www.douban.com/group/shanghaizufang/discussion?start=0&type=new'
    try:
        r = requests.get(https_url, **params)
        content = r.content.decode("utf-8")
        root = etree.HTML(content)
        items = root.xpath('.//li[@class="subject-item"]')
        if r.status_code == 200:
            return True
        return False
    except Exception as e:
        msg = str(e)
        return False


def get_proxy():
    return requests.get("http://127.0.0.1:5010/get/").json()


def delete_proxy(proxy):
    requests.get("http://127.0.0.1:5010/delete/?proxy={}".format(proxy))

def test(i):
    count = 0
    while True:
        params, proxy = get_params(True)
        if test_proxy(params):
            print(proxy)
            count += 1
        else:
            delete_proxy(proxy)
        if count == 5:
            break
def threading_test():
    N = 10
    with Pool(N) as p:
        print(p.map(test, range(N)))
def benchmark(queue: Queue):
    print("benchmarking...")
    useful = set()
    while True:
        params, proxy = get_params(True)
        if proxy not in useful:
            if test_proxy(params):
                print("test useful:",proxy)
                queue.put(proxy)
                useful.add(proxy)
            else:
                delete_proxy(proxy)
        
if __name__ == "__main__":
    queue = Queue()
    p = Process(target=benchmark, args=(queue,))
    p.start()
    p.join()
    print("finished")
