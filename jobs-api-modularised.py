from bs4 import BeautifulSoup
import requests
import pandas as pd

from threading import Thread
import functools
import time
import itertools
from datetime import datetime


def timer(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        value = func(*args, **kwargs)
        end = time.perf_counter()
        duration = end - start
        print("{} completed in {:.4f} seconds".format(func.__name__, duration))
        return value

    return wrapper


class QueryParameters:

    def __init__(self, keyword, starting_salary):
        self.keyword = keyword.lower()
        self.starting_salary = starting_salary
        if len(self.keyword.split()) > 1:
            self.keyword = '+'.join(self.keyword.split())
            self.search_str = keyword.lower()
        else:
            self.search_str = self.keyword


class DataFrame:

    def __init__(self):
        self.df = self.create_pandas_df()

    @staticmethod
    def create_pandas_df():
        columns = ['Title', 'Salary', 'Location', 'Url', 'Advertiser', 'Type', ]
        df = pd.DataFrame(columns=columns)
        return df

    def append_to_df(self,
                     title,
                     salary,
                     location,
                     job_link,
                     advertiser,
                     job_type,
                     description,
                     search_str):
        if search_str in str(title).lower() or search_str in str(description).lower():
            JobsCounter.RELEVANT_JOBS_COUNT += 1
            self.df = self.df.append({'Title': title, 'Salary': salary, 'Location': location,
                                      'Url': job_link, 'Advertiser': advertiser, 'Type': job_type},
                                     ignore_index=True)
        print(JobsCounter.RELEVANT_JOBS_COUNT)


class MakeSoup:
    HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                             'Chrome/79.0.3945.117 Safari/537.36'}

    def soup(self, url_link):
        return BeautifulSoup(requests.get(url_link, headers=self.HEADERS).text, 'lxml')


class WriteToDisk:
    CSV_FILE = "{}_jobs_{}.csv"
    XLSX_FILE = "{}_jobs_{}.xlsx"
    TIME_FORMAT = "%d-%m-%Y-%H-%M-%S"

    @timer
    def to_excel(self, df, keyword):
        df.to_excel(self.XLSX_FILE.format(keyword, datetime.utcnow().strftime(self.TIME_FORMAT)),
                    index=False)

    def to_csv(self, df, keyword):
        df.to_csv(self.CSV_FILE.format(keyword, datetime.utcnow().strftime(self.TIME_FORMAT)),
                  index=False, encoding='utf-8')


class JobsCounter:
    TOTAL_JOBS_COUNT = 0
    RELEVANT_JOBS_COUNT = 0

    @timer
    def get_job_counts(self, total_count, relevant_count):
        print("Total jobs found: {}; Total relevant jobs: {}".format(total_count, relevant_count))


class ResultsPerPage:
    REED_RESULTS_PER_PAGE = 25
    TOTAL_JOBS_RESULTS_PER_PAGE = 20
    CW_JOBS_RESULTS_PER_PAGE = 20
    JOBSERVE_RESULTS_PER_PAGE = 20
    INDEED_RESULTS_PER_PAGE = 50


class URLS:
    JOBSERVE_BASE_URL = 'https://www.jobserve.com'
    JOBSERVE_URL = JOBSERVE_BASE_URL + "/gb/en/JobListingBasic.aspx?shid=70C465132748773534BA"
    JOBSERVE_DYNAMIC_URL = JOBSERVE_URL + "&page={}"

    INDEED_BASE_URL = 'https://www.indeed.co.uk'
    INDEED_URL_PAGES = INDEED_BASE_URL + "/jobs?q={}+£{}&sort=date&limit=50&fromage=3&radius=25"
    INDEED_DYNAMIC_URL = INDEED_URL_PAGES + "&start={}"

    REED_BASE_URL = "https://www.reed.co.uk"
    REED_URL_PAGES = REED_BASE_URL + "/jobs/{}-jobs?salaryfrom={}&datecreatedoffset=LastThreeDays"
    REED_DYNAMIC_URL = REED_BASE_URL + "/jobs/{}-jobs?pageno={}&salaryfrom={}&datecreatedoffset=LastThreeDays"

    TOTAL_JOBS_BASE_URL = "https://www.totaljobs.com"
    TOTAL_JOBS_URL_PAGES = TOTAL_JOBS_BASE_URL + "/jobs/{}?postedwithin=3&salary={}&salarytypeid=1"
    TOTAL_JOBS_DYNAMIC_URL = TOTAL_JOBS_URL_PAGES + "&page={}"

    CW_JOBS_BASE_URL = "https://www.cwjobs.co.uk/"
    CW_JOBS_URL_PAGES = CW_JOBS_BASE_URL + "/jobs/{}?postedwithin=3&salary={}&salarytypeid=1"
    CW_JOBS_DYNAMIC_URL = CW_JOBS_URL_PAGES + "&page={}"

    @staticmethod
    def get_pagination_url(url_pagination, keyword, starting_salary):
        return url_pagination.format(keyword, starting_salary)

    @timer
    def initialise_jobserve_links(self, results_per_page, tag_name, attrs_dict, keyword, starting_salary,
                                  url_pagination=None):
        num_of_pages = PageCounter(keyword, starting_salary).get_page_count(results_per_page,
                                                                            tag_name,
                                                                            attrs_dict,
                                                                            url_pagination)
        urls = (self.JOBSERVE_DYNAMIC_URL.format(page) for page in range(1, num_of_pages + 1))
        soups = (MakeSoup().soup(url) for url in urls)
        hrefs = (soup.find_all('div', attrs={'class': 'jobListHeaderPanel'}) for soup in soups)
        flat_hrefs_list = list(itertools.chain.from_iterable(hrefs))
        print([self.JOBSERVE_BASE_URL + div.a['href'] for div in flat_hrefs_list])
        return [self.JOBSERVE_BASE_URL + div.a['href'] for div in flat_hrefs_list]

    @timer
    def initialise_indeed_links(self, results_per_page, tag_name, attrs_dict, keyword, starting_salary,
                                url_pagination=None):
        num_of_pages = PageCounter(keyword, starting_salary).get_indeed_page_count(results_per_page,
                                                                                   tag_name,
                                                                                   attrs_dict)
        print('num_of_pages:', num_of_pages)
        # 50 results displayed per page, start=0, next=50, next=100, etc.
        print([page for page in range(0, (num_of_pages * 50), 50)])
        urls = [self.INDEED_DYNAMIC_URL.format(keyword, starting_salary, page) for page in
                range(0, (num_of_pages * 50), 50)]
        print(urls)
        soups = [MakeSoup().soup(url) for url in urls]
        hrefs = [soup.find_all('div', attrs={'class': 'title'}) for soup in soups]
        flat_hrefs_list = list(itertools.chain.from_iterable(hrefs))
        print([self.INDEED_BASE_URL + div.a['href'] for div in flat_hrefs_list])
        return [self.INDEED_BASE_URL + div.a['href'] for div in flat_hrefs_list]

    @timer
    def initialise_reed_links(self, keyword, starting_salary, results_per_page):
        num_of_pages = PageCounter(keyword, starting_salary).get_reed_page_count(results_per_page)
        urls = (self.REED_DYNAMIC_URL.format(keyword, page, starting_salary)
                for page in range(1, num_of_pages + 1))
        soups = (MakeSoup().soup(url) for url in urls)
        hrefs = (soup.find_all('h3', attrs={'class': 'title'}) for soup in soups)
        flat_hrefs_list = list(itertools.chain.from_iterable(hrefs))
        return [self.REED_BASE_URL + div.a['href'] for div in flat_hrefs_list]

    @timer
    def initialise_base_links(self, keyword, starting_salary, tag_name, url_pagination,
                              attrs_dict, results_per_page, dynamic_url):
        num_of_pages = PageCounter(keyword, starting_salary).get_page_count(results_per_page,
                                                                            tag_name,
                                                                            attrs_dict,
                                                                            url_pagination)
        urls = (dynamic_url.format(keyword, starting_salary, page_no)
                for page_no in range(1, num_of_pages + 1))
        total_soups = (MakeSoup().soup(link) for link in urls)
        hrefs = (total_soup.find_all('div', {'class': 'job-title'}) for total_soup in total_soups)
        flat_hrefs_list = list(itertools.chain.from_iterable(hrefs))
        return [div.a['href'] for div in flat_hrefs_list]


class PageCounter(QueryParameters, MakeSoup, URLS):

    def __init__(self, keyword, starting_salary):
        super().__init__(keyword, starting_salary)

    @timer
    def get_page_count(self, results_per_page, tag_name, attrs_dict, url_pagination=None):
        soup = self.soup(self.get_pagination_url(url_pagination, self.keyword, self.starting_salary))
        results_count = int(soup.find(tag_name, attrs=attrs_dict).span.text.strip().replace(',', ''))
        if results_count:
            if results_count < results_per_page:
                return int(results_count)
            else:
                return int(round(results_count / results_per_page))

    @timer
    def get_indeed_page_count(self, results_per_page, tag_name, attrs_dict):
        soup = self.soup(self.get_pagination_url(self.INDEED_URL_PAGES, self.keyword, self.starting_salary))
        results_count = int(soup.find(tag_name, attrs=attrs_dict).text.strip().split(' ')[-2].replace(',', ''))
        if results_count:
            if results_count < results_per_page:
                return int(results_count)
            elif ((results_count % results_per_page) / results_per_page) > 0:
                return int(round(results_count / results_per_page) + 1)
            else:
                return int(round(results_count / results_per_page))

    @timer
    def get_reed_page_count(self, results_per_page):
        reed_soup = MakeSoup().soup(self.get_pagination_url(self.REED_URL_PAGES, self.keyword, self.starting_salary))
        results_count = int(reed_soup.find('span', attrs={'class': 'count'}).text.strip().replace(',', ''))
        if results_count:
            if results_count < results_per_page:
                return int(results_count)
            elif ((results_count % results_per_page) / results_per_page) > 0:
                return int(round(results_count / results_per_page) + 1)
            else:
                return int(round(results_count / results_per_page))

    @timer
    def get_total_cw_jobs_count(self, results_per_page, pagination_url):
        soup = MakeSoup().soup(self.get_pagination_url(pagination_url, self.keyword, self.starting_salary))
        results_count = int(soup.find('div', attrs={'class': 'page-title'}).span.text.strip().replace(',', ''))
        if results_count:
            if results_count < results_per_page:
                return int(results_count)
            elif ((results_count % results_per_page) / results_per_page) > 0:
                return int(round(results_count / results_per_page) + 1)
            else:
                return int(round(results_count / results_per_page))


class Interfaces(QueryParameters,
                 DataFrame,
                 MakeSoup,
                 WriteToDisk,
                 JobsCounter,
                 ResultsPerPage,
                 URLS):
    pass


class JobServeJobs(Interfaces, PageCounter):

    def __init__(self, keyword, staring_salary):
        super().__init__(keyword, staring_salary)
        self.df = self.create_pandas_df()
        self.jobserve_job_links = self.initialise_jobserve_links(results_per_page=self.JOBSERVE_RESULTS_PER_PAGE,
                                                                 tag_name='span',
                                                                 attrs_dict={'class': 'resultnumber'},
                                                                 url_pagination=self.JOBSERVE_URL,
                                                                 keyword=self.keyword,
                                                                 starting_salary=self.starting_salary)
        self.get_jobserve_jobs()
        self.TOTAL_JOB_COUNT = len(self.jobserve_job_links)
        self.to_excel(df=self.df, keyword=self.keyword)
        self.get_job_counts(total_count=self.TOTAL_JOB_COUNT,
                            relevant_count=JobsCounter.RELEVANT_JOBS_COUNT)

    @timer
    def get_jobserve_jobs(self):
        for job_link in self.jobserve_job_links[:3]:
            soup = self.soup(job_link)
            time.sleep(2)
            description = soup.find('div', {'class': "md_skills"}).text
            title = soup.find('h1', {'id': 'positiontitle'}).text.strip()
            salary_tag = soup.find('span', {'id': "md_rate"})
            advertiser_tag = soup.find('div', {'id': "recruitername"}).span
            location_tag = soup.find('span', {'id': "md_location"})
            job_type_tag = soup.find('span', {'id': "td_job_type"})

            salary = salary_tag.text if salary_tag is not None else None
            advertiser = advertiser_tag.text if advertiser_tag is not None else None
            location = location_tag.text if location_tag is not None else None
            job_type = job_type_tag.text if job_type_tag is not None else None
            job_data = dict(title=title,
                            salary=salary,
                            location=location,
                            job_link=job_link,
                            advertiser=advertiser,
                            job_type=job_type,
                            description=description,
                            search_str=self.search_str)
            # print(job_data)
            self.append_to_df(**job_data)
        print(JobsCounter.RELEVANT_JOBS_COUNT)
        return self.df


class IndeedJobs(Interfaces, PageCounter):

    def __init__(self, keyword, staring_salary):
        super().__init__(keyword, staring_salary)
        self.df = self.create_pandas_df()
        self.indeed_job_links = self.initialise_indeed_links(results_per_page=self.INDEED_RESULTS_PER_PAGE,
                                                             tag_name='',
                                                             attrs_dict={'id': 'searchCountPages'},
                                                             url_pagination=self.INDEED_URL_PAGES,
                                                             keyword=self.keyword,
                                                             starting_salary=self.starting_salary)

    @timer
    def get_indeed_jobs(self):
        for job_link in self.indeed_job_links:
            location, job_type, salary, advertiser = (None,) * 4
            soup = self.soup(job_link)
            description = soup.find('div', {'id': "jobDescriptionText", 'class': "jobsearch-jobDescriptionText"}).text
            title = soup.find('div', {'class': 'jobsearch-JobInfoHeader-title-container'}).text
            location_type_salary_container = \
                soup.find_all('span', {'class': 'jobsearch-JobMetadataHeader-iconLabel'})
            container_is_not_empty = location_type_salary_container is not None
            container_len = len(location_type_salary_container)
            location = location_type_salary_container[0].text \
                if container_is_not_empty and container_len >= 1 else None
            if container_is_not_empty and container_len >= 2:
                is_salary = '£' in location_type_salary_container[1].text
                job_type = location_type_salary_container[1].text if not is_salary else None
                if container_len >= 3:
                    salary = location_type_salary_container[2].text if len(
                        location_type_salary_container) >= 2 else None
            advertiser = soup.find('div', {'class': "icl-u-lg-mr--sm icl-u-xs-mr--xs"}).text
            job_data = dict(title=title,
                            salary=salary,
                            location=location,
                            job_link=job_link,
                            advertiser=advertiser,
                            job_type=job_type,
                            description=description,
                            search_str=self.search_str)
            # print(job_data)
            self.append_to_df(**job_data)
        return self.df


class ReedJobs(Interfaces, PageCounter):

    def __init__(self, keyword, staring_salary):
        super().__init__(keyword, staring_salary)
        self.df = self.create_pandas_df()
        self.reed_job_links = self.initialise_reed_links(self.keyword,
                                                         self.starting_salary,
                                                         results_per_page=self.REED_RESULTS_PER_PAGE)

    @timer
    def get_reed_jobs(self):
        for job_link in self.reed_job_links:
            soup = self.soup(job_link)
            description = soup.find('span', {'itemprop': "description"}).text
            title = soup.find('h1').text
            salary_tag = soup.find('span', {'data-qa': "salaryLbl"})
            advertiser_tag = soup.find('span', {'itemprop': "name"})
            location_tag = soup.find('span', {'itemprop': "addressLocality"})
            job_type_tag = soup.find('span', {'itemprop': "employmentType"})

            salary = soup.find('span', {'data-qa': "salaryLbl"}).text if salary_tag is not None else None
            advertiser = soup.find('span', {'itemprop': "name"}).text if advertiser_tag is not None else None
            location = soup.find('span', {'itemprop': "addressLocality"}).text if location_tag is not None else None
            job_type = soup.find('span', {'itemprop': "employmentType"}).text if job_type_tag is not None else None
            job_data = dict(title=title,
                            salary=salary,
                            location=location,
                            job_link=job_link,
                            advertiser=advertiser,
                            job_type=job_type,
                            description=description,
                            search_str=self.search_str)
            # print(job_data)
            self.append_to_df(**job_data)
        return self.df


class TotalCWJobs(Interfaces, PageCounter):

    def __init__(self, keyword, staring_salary):
        super().__init__(keyword, staring_salary)
        # self.df = self.create_pandas_df()
        self.total_job_links = self.initialise_base_links(keyword=self.keyword,
                                                          starting_salary=self.starting_salary,
                                                          url_pagination=self.TOTAL_JOBS_URL_PAGES,
                                                          tag_name='div',
                                                          attrs_dict={'class': 'page-title'},
                                                          results_per_page=self.TOTAL_JOBS_RESULTS_PER_PAGE,
                                                          dynamic_url=self.TOTAL_JOBS_DYNAMIC_URL)
        self.cw_job_links = self.initialise_base_links(keyword=self.keyword,
                                                       starting_salary=self.starting_salary,
                                                       url_pagination=self.CW_JOBS_URL_PAGES,
                                                       tag_name='div',
                                                       attrs_dict={'class': 'page-title'},
                                                       results_per_page=self.CW_JOBS_RESULTS_PER_PAGE,
                                                       dynamic_url=self.CW_JOBS_DYNAMIC_URL)

    @timer
    def get_jobs(self, job_links):
        for job_link in job_links:
            soup = self.soup(job_link)
            description = soup.find('div', {'class': "job-description"}).text
            title = soup.find('h1').text.strip()
            salary_tag = soup.find('li', {'class': "salary icon"})
            advertiser_tag = soup.find('a', {'id': "companyJobsLink"})
            location_tag_type_1 = soup.find('li', {'class': "location icon"})
            location_tag_type_2 = soup.find('div', {'class': "col-xs-12 col-sm-7 travelTime-locationText"})
            location1 = location_tag_type_1.div.text if location_tag_type_1 is not None else None
            location2 = location_tag_type_2.ul.text if location_tag_type_2 is not None else None
            location_options = [location1, location2]
            job_type_tag = soup.find('li', {'class': "job-type icon"})

            salary = salary_tag.text.strip() if salary_tag is not None else None
            advertiser = advertiser_tag.text.strip() if advertiser_tag is not None else None
            location = next(item for item in location_options if item is not None)
            job_type = job_type_tag.div.text if job_type_tag is not None else None
            job_data = dict(title=title,
                            salary=salary,
                            location=location,
                            job_link=job_link,
                            advertiser=advertiser,
                            job_type=job_type,
                            description=description,
                            search_str=self.search_str)
            # print(job_data)
            self.append_to_df(**job_data)
        return self.df


class MainWrapper(ReedJobs, IndeedJobs, TotalCWJobs):

    @timer
    def __init__(self, keyword, starting_salary):
        super().__init__(keyword, starting_salary)
        self.df = self.create_pandas_df()
        indeed = Thread(target=self.get_indeed_jobs, args=())
        reed = Thread(target=self.get_reed_jobs, args=())
        total_jobs = Thread(target=self.get_jobs, args=(self.total_job_links,))
        cw_jobs = Thread(target=self.get_jobs, args=(self.cw_job_links,))

        indeed.start()
        reed.start()
        total_jobs.start()
        cw_jobs.start()

        self.TOTAL_JOB_COUNT = len(self.total_job_links) + len(self.cw_job_links) \
                               + len(self.indeed_job_links) + len(self.reed_job_links)

        indeed.join()
        reed.join()
        total_jobs.join()
        cw_jobs.join()

        self.to_excel(df=self.df, keyword=self.keyword)
        self.get_job_counts(total_count=self.TOTAL_JOB_COUNT,
                            relevant_count=JobsCounter.RELEVANT_JOBS_COUNT)


if __name__ == "__main__":
    MainWrapper('python', 85000)
