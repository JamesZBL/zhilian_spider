# -*- coding:utf-8 -*-
"""
智联招聘关键词搜索结果收集

@author:James
Created on:18-2-12 19:48
"""
import winsound
import re
import requests as rq
import gevent
from sqlalchemy import Column, String, Text, Integer, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from bs4 import BeautifulSoup as BS

from spider import settings


class DB_Base:
	Base = declarative_base()
	DBEngine = create_engine('mysql://root:root@localhost:3306/zhilian?charset=utf8', echo=True)
	DBSession = sessionmaker(bind=DBEngine)

	DB_SESSION = DBSession()


class JobItemInfo(DB_Base.Base):
	__tablename__ = 'JOB_ITEM_INFO_' + settings.VALUE_KEYWORD

	# 主键
	ID = Column(Integer, primary_key=True)
	# 职位发布标题
	TITLE = Column(String(200))
	# 公司
	CORPORATION = Column(String(200))
	# 月薪
	SALARY = Column(String(200))
	# 工作地点
	WORK_PLACE = Column(String(200))
	# 发布日期
	RELEASE_DATE = Column(String(200))
	# 工作性质
	CATEGORY = Column(String(200))
	# 工作经历
	EXPERIENCE = Column(String(200))
	# 最低学历
	MIN_EDU_REQUIREMENTS = Column(String(200))
	# 招聘人数
	RECRUITING_NUMBER = Column(String(200))
	# 岗位类别
	JOB_CATEGORY = Column(String(200))
	# 详细描述
	JOB_DETAIL = Column(Text(5000))


# 详情收集器
class GetDetailInfo:
	def __init__(self, urls):
		self.urls = urls

	def get_detail_info(self):
		works = [gevent.spawn(self.get_detail_info_page, i) for i in self.urls]
		gevent.joinall(works)

	def get_detail_info_page(self, url):
		response = rq.get(url)
		content = response.content
		print (response.url)
		soup = BS(content, 'lxml')
		try:
			job_name = soup.find('h1').get_text()
		except Exception:
			return 0  
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
		job_detail = ''
		job_detail_list = soup.select('div.tab-cont-box > div.tab-inner-cont > p')
		for i in job_detail_list:
			job_detail += i.get_text()
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
		try:
			session.add(job_item)
			session.commit()
		except Exception:
			session.rollback()


# 详细信息 URL 采集
class GetResultUrls:
	def __init__(self):
		self.url_repository = UrlRepository()
		self.page_limit = settings.PAGE_LIMIT_NUM
		self.page_limit_on = settings.PAGE_LIMIT
		self.url_search = settings.URL_RESULT
		self.page_maximum = 0

	# 获取指定搜索条件的所有详情页链接
	def get_detail_urls(self):
		data = {
			settings.KEY_KEYWORD: settings.VALUE_KEYWORD,
			settings.KEY_AREA: settings.VALUE_AREA,
		}
		response = rq.get(self.url_search, params=data)
		content = response.content
		soup = BS(content, 'lxml')
		try:
			result_count = int(re.findall(r"共<em>(.*?)</em>个职位满足条件", str(soup))[0])
		except IndexError:
			result_count = 100000
		self.page_maximum = result_count // 60
		if not self.page_limit_on:
			self.page_limit = self.page_maximum
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
		response = rq.get(self.url_search, params=data)
		content = response.content
		soup = BS(content, 'lxml')
		detail_links = soup.select("div.newlist_list_content > table.newlist > tr > td.zwmc > div > a")
		for link in detail_links:
			href = link['href']
			text = link.getText()
			# 筛选出详情链接
			if href.find('.do') > -1:
				continue
			# 筛选广告
			if href.find('xiaoyuan') > -1:
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
		winsound.PlaySound("SystemExclamation", winsound.SND_ALIAS)
