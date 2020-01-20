from bs4 import BeautifulSoup
import requests
import pandas as pd
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

    def __init__(self, keyword: str, starting_salary: str, contract_only: bool):
        self.keyword = keyword.lower()
        self.starting_salary = starting_salary
        self.contract_only = contract_only
        time_format = "%d-%m-%Y-%H-%M-%S"
        if len(self.keyword.split()) > 1:
            self.keyword = '+'.join(self.keyword.split())
            self.search_str = keyword.lower()
            self.db_format = '-'.join(self.keyword.split()) + '-{}'.format(datetime.utcnow().strftime(time_format))
        else:
            self.search_str = self.keyword
            self.db_format = self.keyword + '-{}'.format(datetime.utcnow().strftime(time_format))


class DataFrame:

    def __init__(self):
        self.df = self.create_pandas_df()

    @staticmethod
    def create_pandas_df():
        columns = ['Title', 'Salary', 'Location', 'Advertiser', 'Type', 'Url', ]
        df = pd.DataFrame(columns=columns)
        return df

    @staticmethod
    def make_variable_name(variable):
        # ref: 'https://stackoverflow.com/a/15361037'
        for name in globals():
            if eval(name) == variable:
                return str(name)

    def convert_to_dict(self, list_input: list):
        return {self.make_variable_name(item) for item in list_input}

    def append_to_df(self,
                     title,
                     salary,
                     location,
                     job_link,
                     advertiser,
                     job_type,
                     description,
                     search_str,
                     contract_only):
        is_search_str = (search_str in str(title).lower() or search_str in str(description).lower())
        if contract_only:
            if 'contract' in str(job_type).lower() and is_search_str:
                JobsCounter.RELEVANT_JOBS_COUNT += 1
                self.df = self.df.append({'Title': title, 'Salary': salary, 'Location': location,
                                          'Advertiser': advertiser, 'Type': job_type, 'Url': job_link, },
                                         ignore_index=True)
        else:
            if is_search_str:
                JobsCounter.RELEVANT_JOBS_COUNT += 1
                self.df = self.df.append({'Title': title, 'Salary': salary, 'Location': location,
                                          'Advertiser': advertiser, 'Type': job_type, 'Url': job_link, },
                                         ignore_index=True)


class MakeSoup:
    HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                             'Chrome/79.0.3945.117 Safari/537.36'}

    def soup(self, url_link):
        return BeautifulSoup(requests.get(url_link, headers=self.HEADERS).text, 'lxml')

    @staticmethod
    def find(tag, attrs_dict, soup):
        return soup.find(tag, attrs_dict)

    @staticmethod
    def find_all(tag, attrs_dict, soup):
        return soup.find(tag, attrs_dict)


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
    def get_job_counts(self, total_count, relevant_count, job_count_by_site: dict):
        print('''
        Total jobs found: {}; Total relevant jobs: {}.
        Of which:
        'total_cw_jobs': {total_cw_jobs},
        'indeed_uk_jobs': {indeed_uk_jobs},
        'reed_jobs': {reed_jobs},
        'cvlibrary_jobs': {cvlibrary_jobs}) 
        '''.format(total_count, relevant_count, **job_count_by_site))

    @staticmethod
    def get_relevant_jobs_count(title,
                                job_type,
                                description,
                                search_str,
                                contract_only,
                                job_dict,
                                jobs_list: list):
        is_search_str = (search_str in str(title).lower() or search_str in str(description).lower())
        if contract_only:
            if 'contract' in str(job_type).lower() and is_search_str:
                jobs_list.append(job_dict)
        else:
            if is_search_str:
                jobs_list.append(job_dict)


class ResultsPerPage:
    REED_RESULTS_PER_PAGE = 25
    TOTAL_JOBS_RESULTS_PER_PAGE = 20
    CW_JOBS_RESULTS_PER_PAGE = 20
    JOBSERVE_RESULTS_PER_PAGE = 20
    INDEED_UK_RESULTS_PER_PAGE = 50
    INDEED_US_RESULTS_PER_PAGE = 50
    CVLIBRARY_RESULTS_PER_PAGE = 25


class URLS:
    JOBSERVE_BASE_URL = 'https://www.jobserve.com'
    JOBSERVE_URL = JOBSERVE_BASE_URL + "/gb/en/JobListingBasic.aspx?shid=70C465132748773534BA"
    JOBSERVE_DYNAMIC_URL = JOBSERVE_URL + "&page={}"

    INDEED_UK_BASE_URL = 'https://www.indeed.co.uk'
    INDEED_UK_URL_PAGES = INDEED_UK_BASE_URL + "/jobs?q={}+£{}&sort=date&limit=50&fromage=3&radius=25"
    INDEED_UK_DYNAMIC_URL = INDEED_UK_URL_PAGES + "&start={}"

    INDEED_US_BASE_URL = 'https://www.indeed.com'
    INDEED_US_URL_PAGES = INDEED_US_BASE_URL + "/jobs?q={}+${}&sort=date&limit=50&fromage=3&radius=25"
    INDEED_US_DYNAMIC_URL = INDEED_US_URL_PAGES + "&start={}"

    REED_BASE_URL = "https://www.reed.co.uk"
    REED_URL_PAGES = REED_BASE_URL + "/jobs/{}-jobs?salaryfrom={}&datecreatedoffset=LastThreeDays"
    REED_DYNAMIC_URL = REED_BASE_URL + "/jobs/{}-jobs?pageno={}&salaryfrom={}&datecreatedoffset=LastThreeDays"

    TOTAL_JOBS_BASE_URL = "https://www.totaljobs.com"
    TOTAL_JOBS_URL_PAGES = TOTAL_JOBS_BASE_URL + "/jobs/{}?postedwithin=3&salary={}&salarytypeid=1"
    TOTAL_JOBS_DYNAMIC_URL = TOTAL_JOBS_URL_PAGES + "&page={}"

    CW_JOBS_BASE_URL = "https://www.cwjobs.co.uk"
    CW_JOBS_URL_PAGES = CW_JOBS_BASE_URL + "/jobs/{}?postedwithin=3&salary={}&salarytypeid=1"
    CW_JOBS_DYNAMIC_URL = CW_JOBS_URL_PAGES + "&page={}"

    CVLIBRARY_BASE_URL = "https://www.cv-library.co.uk"
    CVLIBRARY_URL_PAGES = CVLIBRARY_BASE_URL + "/search-jobs?category=&distance=15&geo=&industry=&order" \
                                               "=&perpage=25&posted=3&q={}&salarymax=&salarymin={}" \
                                               "=&salarytype=annum&tempperm="
    CVLIBRARY_DYNAMIC_URL = CVLIBRARY_URL_PAGES + "&offset={}"

    @staticmethod
    def get_pagination_url(url_pagination, keyword, starting_salary):
        return url_pagination.format(keyword, starting_salary)

    @timer
    def initialise_jobserve_links(self, results_per_page, tag_name, attrs_dict, keyword, starting_salary,
                                  contract_only, url_pagination=None):
        num_of_pages = PageCounter(keyword, starting_salary, contract_only).get_page_count(results_per_page,
                                                                                           tag_name,
                                                                                           attrs_dict,
                                                                                           url_pagination)
        urls = (self.JOBSERVE_DYNAMIC_URL.format(page) for page in range(1, num_of_pages + 1))
        soups = (MakeSoup().soup(url) for url in urls)
        hrefs = (soup.find_all('div', attrs={'class': 'jobListHeaderPanel'}) for soup in soups)
        flat_hrefs_list = list(itertools.chain.from_iterable(hrefs))
        return [self.JOBSERVE_BASE_URL + div.a['href'] for div in flat_hrefs_list]

    @timer
    def initialise_indeed_links(self, results_per_page, tag_name, attrs_dict, keyword, starting_salary,
                                contract_only, dynamic_url, base_url, url_page):
        num_of_pages = PageCounter(keyword, starting_salary, contract_only).get_indeed_page_count(results_per_page,
                                                                                                  tag_name,
                                                                                                  attrs_dict,
                                                                                                  url_page)
        print('indeed_page_count: ', num_of_pages)
        # 100 results displayed per page, start=0, next=50, next=100, etc.
        urls = [dynamic_url.format(keyword, starting_salary, page) for page in
                range(0, (num_of_pages * results_per_page), results_per_page)]
        print([('indeed_url: ', url) for url in urls])

        # TODO pear-shaped from here!
        soups = [MakeSoup().soup(url) for url in urls]
        hrefs = [soup.find_all('div', attrs={'class': 'title'}) for soup in soups]
        flat_hrefs_list = list(itertools.chain.from_iterable(hrefs))
        print('indeed_jobs_urls:', [base_url + div.a['href'] for div in flat_hrefs_list])
        return [base_url + div.a['href'] for div in flat_hrefs_list]

    @timer
    def initialise_cvlibrary_links(self, results_per_page, tag_name, attrs_dict, keyword,
                                   starting_salary, contract_only):
        num_of_pages = PageCounter(keyword, starting_salary, contract_only) \
            .get_cvlibrary_page_count(results_per_page, tag_name, attrs_dict)
        # 100 results displayed per page, start=0, next=100, next=200, etc.
        urls = (self.CVLIBRARY_DYNAMIC_URL.format(keyword, starting_salary, page) for page in
                range(0, (num_of_pages * results_per_page), results_per_page))
        soups = (MakeSoup().soup(url) for url in urls)
        hrefs = (soup.find_all('', attrs={'class': 'results__item'}) for soup in soups)
        flat_hrefs = list(itertools.chain.from_iterable(hrefs))
        return [self.CVLIBRARY_BASE_URL + div.a['href'] for div in flat_hrefs
                if 'trainee' not in str(div.a['href']).lower()]

    @timer
    def initialise_reed_links(self, keyword, starting_salary, contract_only, results_per_page):
        num_of_pages = PageCounter(keyword, starting_salary, contract_only).get_reed_page_count(results_per_page)
        urls = (self.REED_DYNAMIC_URL.format(keyword, page, starting_salary)
                for page in range(1, num_of_pages + 1))
        soups = (MakeSoup().soup(url) for url in urls)
        hrefs = (soup.find_all('h3', attrs={'class': 'title'}) for soup in soups)
        flat_hrefs_list = list(itertools.chain.from_iterable(hrefs))
        return [self.REED_BASE_URL + div.a['href'] for div in flat_hrefs_list]

    @timer
    def initialise_base_links(self, keyword, starting_salary, tag_name, url_pagination,
                              attrs_dict, results_per_page, dynamic_url, contract_only):
        num_of_pages = PageCounter(keyword, starting_salary, contract_only).get_page_count(results_per_page,
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

    def __init__(self, keyword, starting_salary, contract_only):
        super().__init__(keyword, starting_salary, contract_only)

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
    def get_indeed_page_count(self, results_per_page, tag_name, attrs_dict, url_page):
        soup = self.soup(self.get_pagination_url(url_page, self.keyword, self.starting_salary))
        results_count = int(soup.find(tag_name, attrs=attrs_dict).text.strip().split(' ')[-2].replace(',', ''))
        if results_count:
            if results_count < results_per_page:
                return int(results_count)
            elif ((results_count % results_per_page) / results_per_page) > 0:
                return int(round(results_count / results_per_page) + 1)
            else:
                return int(round(results_count / results_per_page))

    @timer
    def get_cvlibrary_page_count(self, results_per_page, tag_name, attrs_dict):
        soup = self.soup(self.get_pagination_url(self.CVLIBRARY_URL_PAGES, self.keyword, self.starting_salary))
        results_count = int(soup.find(tag_name, attrs=attrs_dict).text.strip().split('\n')[-2].strip().replace(',', ''))
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
