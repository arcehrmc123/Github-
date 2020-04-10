# -*- coding: utf-8 -*-
import scrapy
from scrapy import Request
import itertools
import json
import csv
import pandas as pd
import numpy as np


class Con1Spider(scrapy.Spider):
    i=0
    j=1
    name = 'commits'
    allowed_domains = ['github.com']
    handle_httpstatus_list = [404, 403, 401, 422,451,409]
    output_text = open('commits.txt', 'a', encoding="utf-8") #记录httpstatus出错状态信息
    csv_data = pd.read_csv("F:\\github\\spider\data\\repo_name.csv", encoding="utf-8")
    org_login = csv_data['org_login']
    repo_name = csv_data['repo_name']
    d1 = pd.DataFrame(columns=['contributor_login','org_login','repo_name','sha','author_name'])




    token_list = [
        'ee28e2b42120443c23f53280d7179daae0a7b479',  # 万茂慈token
        '5c3926ed1fba9474d9a3a49f48082b0b640b875b',  # 张晨涵token
        '480375787b605a305dc3f317fb67065fe702653b',  # 谢佳辰token
        '3b190252df55503ff9b4c37227cad83bab5e46db',  # 谢佳辰token
        '3db468c256b3b129275da5b6d168b7015ce31ff4',  # 韩艳婷token
        'bce5c1a50146717b9a1bf0b6251bbd7b4b40b818',  # 韩艳婷token
        '9bc7807922c099eee0ab41b7cf193dbe222c4c06',  # 商亚男token
        '816b381c76f48ede43296ccb61b57e397fe9dc19',  # 马骢token
        'e8c204609b83e971df273e66656b568aef28ea70',  # 胡锦token
        '8397d07a7d4966a84a9378059ace13277df51122',  # 胡锦token
        'e323e941165c708a447ee71432c5cb61dcd0442e',  # 马骢token
        '66a7ba0d1e07f72842c673e6ecc2c7d4789068dc ',  # 董思辰token
    ]

    token_iter = itertools.cycle(token_list)

    user_list = [
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 ",
        "(KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1",
        "Mozilla/5.0 (X11; CrOS i686 2268.111.0) AppleWebKit/536.11 ",
        "(KHTML, like Gecko) Chrome/20.0.1132.57 Safari/536.11",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 ",
        "(KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 ",
        "(KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6",
        "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 ",
        "(KHTML, like Gecko) Chrome/19.77.34.5 Safari/537.1",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/536.5 ",
        "(KHTML, like Gecko) Chrome/19.0.1084.9 Safari/536.5",
        "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 ",
        "(KHTML, like Gecko) Chrome/19.0.1084.36 Safari/536.5",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 ",
        "(KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/536.3 ",
        "(KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_0) AppleWebKit/536.3 ",
        "(KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 ",
        "(KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 ",
        "(KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 ",
        "(KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 ",
        "(KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/536.3 ",
        "(KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 ",
        "(KHTML, like Gecko) Chrome/19.0.1061.0 Safari/536.3",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.24 ",
        "(KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24",
        "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/535.24 ",
        "(KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24",
    ]

    user_iter = itertools.cycle(user_list)  # 生成循环迭代器，迭代到最后一个token后，会重新开始迭代

    def __init__(self):  # 初始化
        scrapy.spiders.Spider.__init__(self)
        self.d1.to_csv('ContributionActivity_add.csv', index=False, encoding='utf-8')

    def __del__(self):  # 爬虫结束时，关闭文件
        self.d1.to_csv('ContributionActivity_add.csv',index=False,encoding='utf-8', mode='a', header=False)

    def start_requests(self):
        start_urls = []  # 初始爬取链接列表
        url = 'https://api.github.com/repos/' +self.repo_name[self.i]+'/commits?page='+ str(self.j) +'&per_page=100'# 第一条爬取url
        # 添加一个爬取请求
        start_urls.append(scrapy.FormRequest(url, headers={
            # 'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:36.0) Gecko/20100101 Firefox/36.0',
            'User-Agent': next(self.user_iter),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en',
            'Authorization': 'token ' + next(self.token_iter),  # 这个字段为添加token字段
        }, callback=self.parse))
        return start_urls

    def yield_request(self):  # 定义一个生成请求函数
        url = 'https://api.github.com/repos/' +self.repo_name[self.i]+'/commits?page='+ str(self.j)+'&per_page=100'  # 生成url
        # 返回请求
        return Request(url, headers={
            'User-Agent':next(self.user_iter),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en',
            'Authorization': 'token ' + next(self.token_iter),
        }, callback=self.parse, dont_filter=True)

    def parse(self, response):
        repo_length = len(self.repo_name)
        if response.status in self.handle_httpstatus_list:  # 如果遇见handle_httpstatus_list中出现的状态码
            print(response.status)
            a = "fail to crawl(data&pape):" + str(response.url) + ' ' + ' ' + str(response.status)
            self.output_text.write(a + '\n')
            self.output_text.flush()
            self.i = self.i + 1
            if self.i == repo_length:
                print("结束")
                exit()
            yield self.yield_request()  # 产生新的请求
            return
        json_data = json.loads(response.body, encoding='utf-8') # 获取json
        length = len(json_data)  # 获取json长度
        print(str(response.url))
        print("当前所在行数：",self.i+1,'/'+str(repo_length),"当前所在页数：",self.j)

        try:
            for b in json_data:
                # print(pd.isnull(b['author']))
                if pd.isnull(b['author']) is not True:
                    new = pd.DataFrame({'contributor_login': b['author']['login'],
                                        'org_login': self.org_login[self.i],
                                        'repo_name': self.repo_name[self.i],
                                        'sha': b['sha'],
                                        'author_name': b['commit']['author']['name']
                                        }, index=[0])
                    self.d1 = self.d1.append(new, ignore_index=True)

                if len(self.d1)==10000:  #爬取10000行后写入csv
                    self.d1.to_csv('ContributionActivity_add.csv', index=False, encoding='utf-8', mode='a',
                                   header=False)
                    self.d1 = pd.DataFrame(columns=['contributor_login','org_login','repo_name','sha','author_name'])

            self.j = self.j + 1  #如果页面大小小于100翻页，j=1
            if length != 100:
                self.j = 1
                self.i = self.i + 1
            if self.i == repo_length:  #如果进行的项目到达最后一项，停止爬取
                print("结束")
                exit()

            yield self.yield_request()

        except Exception as e:
            print(e)
            self.i = self.i + 1 #遇到错误尝试下一个repo_name
            if self.i == repo_length:  #如果进行的项目到达最后一项，停止爬取
                print("结束")
                exit()
            yield self.yield_request()




