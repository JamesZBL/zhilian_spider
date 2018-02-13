# -*- coding:utf-8 -*-
"""
智联招聘关键词搜索结果收集

@author:James
Created on:18-2-12 19:48
"""
import re
import requests
import gevent
from sqlalchemy import Column, String, Integer, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from bs4 import BeautifulSoup

from spider import settings


class DB_Base:
	Base = declarative_base()
	DBEngine = create_engine('mysql://root:root@localhost:3306/zhilian?charset=utf8', echo=True)
	DBSession = sessionmaker(bind=DBEngine)

	DB_SESSION = DBSession()


class JobItemInfo(DB_Base.Base):
	__tablename__ = 'JOB_ITEM_INFO_' + settings.VALUE_KEYWORD

	ID = Column(Integer, primary_key=True)
	TITLE = Column(String(200))
	CORPORATION = Column(String(200))
	SALARY = Column(String(200))
	WORK_PLACE = Column(String(200))
	RELEASE_DATE = Column(String(200))
	CATEGORY = Column(String(200))
	EXPERIENCE = Column(String(200))
	MIN_EDU_REQUIREMENTS = Column(String(200))
	RECRUITING_NUMBER = Column(String(200))
	JOB_CATEGORY = Column(String(200))
	JOB_DETAIL = Column(String(1500))


# 详情收集器
class GetDetailInfo:
	def __init__(self, urls):
		self.urls = urls

	def get_detail_info(self):
		works = [gevent.spawn(self.get_detail_info_page, i) for i in self.urls]
		gevent.joinall(works)

	def get_detail_info_page(self, url):
		response = requests.get(url)
		content = response.content
		soup = BeautifulSoup(content, 'lxml')
		job_name = soup.find('h1').get_text()
		job_organization = soup.select('h2 > a')[0].get_text()
		job_details = soup.select('div.terminalpage-left > ul > li > strong')
		job_monthly_salary = job_details[0].get_text()
		job_work_place = job_details[1].get_text()
		job_release_date = job_details[2].get_text()
		job_category = job_details[3].get_text()
		job_work_experience = job_details[4].get_text()
		job_minimum_education_requirements = job_details[5].get_text()
		job_recruiting_numbers = job_details[6].get_text()
		job_job_category = job_details[7].get_text()
		job_detail = soup.select('div.tab-cont-box > div.tab-inner-cont > p')[0].get_text()
		job_item = JobItemInfo(ID=None,
		                       TITLE=job_name, CORPORATION=job_organization,
		                       SALARY=job_monthly_salary,
		                       WORK_PLACE=job_work_place, RELEASE_DATE=job_release_date,
		                       CATEGORY=job_category,
		                       EXPERIENCE=job_work_experience,
		                       MIN_EDU_REQUIREMENTS=job_minimum_education_requirements,
		                       RECRUITING_NUMBER=job_recruiting_numbers, JOB_CATEGORY=job_job_category,
		                       JOB_DETAIL=job_detail)

		session = DB_Base.DB_SESSION
		session.add(job_item)
		session.commit()


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
			# if text.lower().find(settings.VALUE_KEYWORD.lower()) == -1:
			# 	continue
			print(text)
			print(href)
			url_result.append(href)
			self.url_repository.push(href)
		return url_result


# URL 仓库
class UrlRepository:
	def __init__(self):
		self.urls = []

	def push(self, url):
		self.urls.append(url)


# 主类
class SpiderMain:
	def __init__(self):
		self.url_result = []
		self.url_search = settings.URL_RESULT

	def run(self):
		JobItemInfo.metadata.create_all(DB_Base.DBEngine)
		url_collector = GetResultUrls()
		self.url_result = url_collector.get_detail_urls()
		print(self.url_result)
		collector = GetDetailInfo(self.url_result)
		collector.get_detail_info()


if __name__ == '__main__':
	app = SpiderMain()
	app.run()
