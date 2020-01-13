from bs4 import BeautifulSoup
import requests
from threading import Thread
import functools
import time
import itertools
import pandas as pd
import queue
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


class JobsAPI:
    
    JOB_COUNT = 0
    RESULTS_PER_PAGE = 25
    CSV_FILE = "{}_jobs_{}.csv"
    TIME_FORMAT = "%d-%m-%Y-%H-%M-%S"

    INDEED_BASE_URL = 'https://www.indeed.co.uk'
    INDEED_DYNAMIC_URL = INDEED_BASE_URL + "/jobs?as_and={}&as_phr=&as_any=&as_not=&as_ttl=&as_cmp=&jt=all&st=&as_src" \
                                           "=&salary=&radius=25&l=&fromage=3&limit=1000&sort=date&psf=advsrch&from" \
                                           "=advancedsearch"

    REED_BASE_URL = "https://www.reed.co.uk"
    REED_URL_PAGES = REED_BASE_URL + "/jobs/{}-jobs?salaryfrom={}&datecreatedoffset=LastThreeDays"
    REED_DYNAMIC_URL = REED_BASE_URL + "/jobs/{}-jobs?pageno={}&salaryfrom={}&datecreatedoffset=LastThreeDays"

    TOTAL_JOBS_BASE_URL = "https://www.totaljobs.com/"
    TOTAL_JOBS_URL_PAGES = TOTAL_JOBS_BASE_URL + "/jobs/{}?postedwithin=3&salary={}&salarytypeid=1"
    TOTAL_JOBS_DYNAMIC_URL = TOTAL_JOBS_URL_PAGES + "&page={}"
    TOTAL_JOBS_RESULTS_PER_PAGE = 20
    HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) '
                             'Chrome/61.0.3163.100 Safari/537.36'}

    @timer
    def __init__(self, keyword, starting_salary=45000):
        self.reed_results_queue = queue.Queue()  # to get results from thread
        self.indeed_results_queue = queue.Queue()
        self.total_jobs_results_queue = queue.Queue()
        self.keyword = keyword.lower()
        if len(self.keyword.split()) > 1:
            self.keyword = '+'.join(self.keyword.split())
            self.search_str = keyword.lower()
        else:
            self.search_str = self.keyword
        self.starting_salary = starting_salary
        self.df = self.initialise_pandas_df()
        reed_links = Thread(target=self.initialise_reed_links, args=())
        indeed_links = Thread(target=self.initialise_indeed_links, args=())
        total_job_links = Thread(target=self.initialise_total_jobs_links, args=())

        reed_links.start()
        indeed_links.start()
        total_job_links.start()

        reed_links.join()
        indeed_links.join()
        total_job_links.join()

        self._reed_job_links = self.reed_results_queue.get()
        self._indeed_job_links = self.indeed_results_queue.get()
        self._total_jobs_links = self.total_jobs_results_queue.get()
        self.total_links = len(self.reed_job_links) + len(self.indeed_job_links) + len(self.total_jobs_links)

        indeed = Thread(target=self.get_indeed_jobs, args=())
        reed = Thread(target=self.get_reed_jobs, args=())
        total_jobs = Thread(target=self.get_total_jobs, args=())

        indeed.start()
        reed.start()
        total_jobs.start()

        indeed.join()
        reed.join()
        total_jobs.join()

        self.write_to_cvs()
        self.get_job_counts()

    @timer
    def write_to_cvs(self):
        self.df.to_csv(self.CSV_FILE.format(self.keyword,
                                            datetime.utcnow().strftime(self.TIME_FORMAT)),
                       index=False, encoding='utf-8')

    @timer
    def get_job_counts(self):
        print("Total jobs found: {}; Total relevant jobs: {}".format(self.total_links, self.JOB_COUNT))

    @property
    def reed_job_links(self):
        return self._reed_job_links

    @property
    def indeed_job_links(self):
        return self._indeed_job_links

    @property
    def total_jobs_links(self):
        return self._total_jobs_links

    @staticmethod
    def initialise_pandas_df():
        columns = ['Title', 'Salary', 'Location', 'Url', 'Advertiser', 'Type', ]
        df = pd.DataFrame(columns=columns)
        return df

    def append_to_pandas_df(self, title, salary, location, job_link, advertiser, job_type, description):
        if self.search_str in str(title).lower() or self.search_str in str(description).lower():
            self.JOB_COUNT += 1
            self.df = self.df.append({'Title': title, 'Salary': salary, 'Location': location,
                                      'Url': job_link, 'Advertiser': advertiser, 'Type': job_type},
                                     ignore_index=True)

    def get_pagination_url(self, url_pagination):
        return url_pagination.format(self.keyword, self.starting_salary)

    def make_soup(self, url_link):
        return BeautifulSoup(requests.get(url_link, headers=self.HEADERS).text, 'lxml')

    @timer
    def get_reed_page_count(self):
        reed_soup = self.make_soup(self.get_pagination_url(self.REED_URL_PAGES))
        results_count = int(reed_soup.find('span', attrs={'class': 'count'}).text.strip())
        if results_count:
            if results_count < self.RESULTS_PER_PAGE:
                return int(results_count)
            else:
                return int(round(results_count / self.RESULTS_PER_PAGE))

    @timer
    def get_total_jobs_page_count(self):
        total_jobs_pagination_url = self.get_pagination_url(self.TOTAL_JOBS_URL_PAGES)
        total_jobs_soup = self.make_soup(total_jobs_pagination_url)
        results_count = int(total_jobs_soup.find('div', attrs={'class': 'page-title'}).span.text.strip())
        if results_count:
            if results_count < self.TOTAL_JOBS_RESULTS_PER_PAGE:
                return int(results_count)
            else:
                return int(round(results_count / self.TOTAL_JOBS_RESULTS_PER_PAGE))

    @timer
    def initialise_indeed_links(self):
        url = self.INDEED_DYNAMIC_URL.format(self.keyword)
        soup = self.make_soup(url)
        hrefs = soup.find_all('div', attrs={'class': 'title'})
        result = [self.INDEED_BASE_URL + div.a['href'] for div in hrefs]  # or div.find('a')['href']
        self.indeed_results_queue.put(result)

    @timer
    def initialise_reed_links(self):
        urls = [self.REED_DYNAMIC_URL.format(self.keyword, page, self.starting_salary)
                for page in range(1, self.get_reed_page_count() + 1)]
        soups = [self.make_soup(url) for url in urls]
        hrefs = [soup.find_all('h3', attrs={'class': 'title'}) for soup in soups]
        flat_hrefs_list = list(itertools.chain.from_iterable(hrefs))
        result = [self.REED_BASE_URL + div.a['href'] for div in flat_hrefs_list]
        self.reed_results_queue.put(result)

    @timer
    def initialise_total_jobs_links(self):
        urls = [self.TOTAL_JOBS_DYNAMIC_URL.format(self.keyword, self.starting_salary, page_no)
                for page_no in range(1, self.get_total_jobs_page_count() + 1)]
        total_soups = [self.make_soup(link) for link in urls]
        hrefs = [total_soup.find_all('div', {'class': 'job-title'}) for total_soup in total_soups]
        flat_hrefs_list = list(itertools.chain.from_iterable(hrefs))
        result = [div.a['href'] for div in flat_hrefs_list]
        self.total_jobs_results_queue.put(result)

    @timer
    def get_indeed_jobs(self):
        for job_link in self.indeed_job_links:
            location, job_type, salary, advertiser = (None,) * 4
            soup = self.make_soup(job_link)
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
            self.append_to_pandas_df(title, salary, location, job_link, advertiser, job_type, description)

    @timer
    def get_reed_jobs(self):
        for job_link in self.reed_job_links:
            soup = self.make_soup(job_link)
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
            self.append_to_pandas_df(title, salary, location, job_link, advertiser, job_type, description)

    @timer
    def get_total_jobs(self):
        for job_link in self.total_jobs_links:
            total_soup = self.make_soup(job_link)
            description = total_soup.find('div', {'class': "job-description"}).text
            title = total_soup.find('h1', {'class': 'brand-font'}).text.strip()
            salary_tag = total_soup.find('li', {'class': "salary icon"})
            advertiser_tag = total_soup.find('a', {'id': "companyJobsLink"})
            location_tag_type_1 = total_soup.find('li', {'class': "location icon"})
            location_tag_type_2 = total_soup.find('div', {'class': "col-xs-12 col-sm-7 travelTime-locationText"})
            location1 = location_tag_type_1.div.text if location_tag_type_1 is not None else None
            location2 = location_tag_type_2.ul.text if location_tag_type_2 is not None else None
            location_options = [location1, location2]
            job_type_tag = total_soup.find('li', {'class': "job-type icon"})

            salary = salary_tag.text.strip() if salary_tag is not None else None
            advertiser = advertiser_tag.text.strip() if advertiser_tag is not None else None
            location = next(item for item in location_options if item is not None)
            job_type = job_type_tag.div.text if job_type_tag is not None else None
            self.append_to_pandas_df(title, salary, location, job_link, advertiser, job_type, description)


if __name__ == "__main__":
    JobsAPI(keyword='python', starting_salary=85000)
