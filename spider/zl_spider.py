# -*- coding:utf-8 -*-
"""
智联招聘关键词搜索结果收集

@author:James
Created on:18-2-12 19:48
"""
import re
import requests
import gevent
from bs4 import BeautifulSoup

from spider import settings


class GetDetailInfo:
	def __init__(self, urls):
		self.urls = urls
		pass

	def get_detail_info(self, urls):
		works = [gevent.spawn(self.get_detail_info_page, i) for i in urls]
		gevent.joinall(works)
		pass

	def get_detail_info_page(self, url):
		pass


class UrlRepository:
	def __init__(self):
		self.urls = []

	def push(self, url):
		self.urls.append(url)


# 详细信息 URL 采集
class GetResultUrls:
	def __init__(self):
		self.url_repository = UrlRepository()
		self.page_limit = settings.PAGE_LIMIT
		self.url_search = settings.URL_RESULT
		self.page_maximum = 0

	# 获取指定搜索条件的所有详情页链接
	def get_detail_urls(self):
		data = {
			settings.KEY_KEYWORD: settings.VALUE_KEYWORD,
			settings.KEY_AREA: settings.VALUE_AREA,
		}
		response = requests.get(self.url_search, params=data)
		content = response.content
		soup = BeautifulSoup(content, 'lxml')
		result_count = int(re.findall(r"共<em>(.*?)</em>个职位满足条件", str(soup))[0])
		self.page_maximum = result_count // 60
		works = [gevent.spawn(self.get_detail_urls_page, i) for i in range(self.page_limit)]
		gevent.joinall(works, timeout=10)
		return self.url_repository.urls

	# 获取指定页码内所有的详情页链接
	def get_detail_urls_page(self, page_number):
		url_result = []
		data = {
			settings.KEY_KEYWORD: settings.VALUE_KEYWORD,
			settings.KEY_AREA: settings.VALUE_AREA,
			settings.KYE_PAGENUM: page_number
		}
		response = requests.get(self.url_search, params=data)
		content = response.content
		soup = BeautifulSoup(content, 'lxml')
		detail_links = soup.select("table.newlist > tr > td.zwmc > div > a")
		for link in detail_links:
			href = link['href']
			text = link.getText()
			# 筛选出详情链接
			if href.find('.do') > -1:
				continue
			# 过滤关键字
			if text.lower().find(settings.VALUE_KEYWORD.lower()) == -1:
				continue
			print(text)
			print(href)
			url_result.append(href)
			self.url_repository.push(href)
		return url_result


class SpiderMain:
	def __init__(self):
		self.url_result = []
		self.url_search = settings.URL_RESULT

	def run(self):
		url_collector = GetResultUrls()
		self.url_result = url_collector.get_detail_urls()
		print(self.url_result)
		collector = GetDetailInfo(self.url_result)


if __name__ == '__main__':
	app = SpiderMain()
	app.run()
