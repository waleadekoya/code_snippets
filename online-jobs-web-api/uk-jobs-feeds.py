from datetime import datetime

import pandas as pd
from threading import Thread
from multiprocessing.pool import ThreadPool
import time
import itertools

from models import db_connect, create_table
from utils import (PageCounter, Interfaces, JobsCounter, timer)


class JobServeJobs(Interfaces, PageCounter):

    def __init__(self, keyword, staring_salary, contract_only):
        super().__init__(keyword, staring_salary, contract_only)
        self.df = self.create_pandas_df()
        self.jobserve_job_links = self.initialise_jobserve_links(results_per_page=self.JOBSERVE_RESULTS_PER_PAGE,
                                                                 tag_name='span',
                                                                 attrs_dict={'class': 'resultnumber'},
                                                                 url_pagination=self.JOBSERVE_URL,
                                                                 keyword=self.keyword,
                                                                 contract_only=self.contract_only,
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
                            search_str=self.search_str,
                            contract_only=self.contract_only)
            self.append_to_df(**job_data)
        return self.df


class IndeedUKJobs(Interfaces, PageCounter):

    def __init__(self, keyword, staring_salary, contract_only):
        super().__init__(keyword, staring_salary, contract_only)
        self.indeed_uk_df = None
        self.indeed_uk_jobs_list = []
        self.indeed_uk_relevant_jobs = 0
        self.indeed_uk_job_links = self.initialise_indeed_links(results_per_page=self.INDEED_UK_RESULTS_PER_PAGE,
                                                                tag_name='',
                                                                attrs_dict={'id': 'searchCountPages'},
                                                                keyword=self.keyword,
                                                                starting_salary=self.starting_salary,
                                                                contract_only=self.contract_only,
                                                                dynamic_url=self.INDEED_UK_DYNAMIC_URL,
                                                                base_url=self.INDEED_UK_BASE_URL,
                                                                url_page=self.INDEED_UK_URL_PAGES)
        print(self.indeed_uk_jobs_list)

    @timer
    def get_indeed_uk_jobs(self, job_links):
        for job_link in job_links:
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
                            search_str=self.search_str,
                            contract_only=self.contract_only)
            self.append_to_df(**job_data)
            self.get_relevant_jobs_count(title=title,
                                         job_type=job_type,
                                         description=description,
                                         search_str=self.search_str,
                                         contract_only=self.contract_only,
                                         job_dict=job_data,
                                         jobs_list=self.indeed_uk_jobs_list)
        self.indeed_uk_df = pd.DataFrame(self.indeed_uk_jobs_list).drop_duplicates()
        self.indeed_uk_relevant_jobs = self.indeed_uk_df.shape[0]


class ReedJobs(Interfaces, PageCounter):

    def __init__(self, keyword, staring_salary, contract_only):
        super().__init__(keyword, staring_salary, contract_only)
        self.reed_df = None
        self.reed_jobs_list = []
        self.reed_relevant_jobs = 0
        self.reed_job_links = self.initialise_reed_links(keyword=self.keyword,
                                                         starting_salary=self.starting_salary,
                                                         contract_only=self.contract_only,
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
                            search_str=self.search_str,
                            contract_only=self.contract_only)
            self.append_to_df(**job_data)
            self.get_relevant_jobs_count(title=title,
                                         job_type=job_type,
                                         description=description,
                                         search_str=self.search_str,
                                         contract_only=self.contract_only,
                                         job_dict=job_data,
                                         jobs_list=self.reed_jobs_list)
        self.reed_df = pd.DataFrame(self.reed_jobs_list).drop_duplicates()
        self.reed_relevant_jobs = self.reed_df.shape[0]
        print(self.reed_relevant_jobs)
        return self.df


class TotalCWJobs(Interfaces, PageCounter):

    def __init__(self, keyword, staring_salary, contract_only):
        super().__init__(keyword, staring_salary, contract_only)
        self.total_cw_df = None
        self.total_cw_jobs_list = []
        self.total_cw_relevant_jobs = 0
        self.total_job_links = self.initialise_base_links(keyword=self.keyword,
                                                          starting_salary=self.starting_salary,
                                                          contract_only=self.contract_only,
                                                          url_pagination=self.TOTAL_JOBS_URL_PAGES,
                                                          tag_name='div',
                                                          attrs_dict={'class': 'page-title'},
                                                          results_per_page=self.TOTAL_JOBS_RESULTS_PER_PAGE,
                                                          dynamic_url=self.TOTAL_JOBS_DYNAMIC_URL)
        self.cw_job_links = self.initialise_base_links(keyword=self.keyword,
                                                       starting_salary=self.starting_salary,
                                                       contract_only=self.contract_only,
                                                       url_pagination=self.CW_JOBS_URL_PAGES,
                                                       tag_name='div',
                                                       attrs_dict={'class': 'page-title'},
                                                       results_per_page=self.CW_JOBS_RESULTS_PER_PAGE,
                                                       dynamic_url=self.CW_JOBS_DYNAMIC_URL)

    @timer
    def get_jobs(self, job_links):
        for job_link in job_links:
            soup = self.soup(job_link)
            description = self.find('div', {'class': "job-description"}, soup).text
            title = soup.find('h1').text.strip()
            salary_tag = self.find('li', {'class': "salary icon"}, soup)
            advertiser_tag = self.find('a', {'id': "companyJobsLink"}, soup)
            location_tag_1 = self.find('li', {'class': "location icon"}, soup)
            location_tag_2 = self.find('div', {'class': "col-xs-12 col-sm-7 travelTime-locationText"}, soup)
            location1 = location_tag_1.div.text if location_tag_1 is not None else None
            location2 = location_tag_2.ul.text if location_tag_2 is not None else None
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
                            search_str=self.search_str,
                            contract_only=self.contract_only)
            self.append_to_df(**job_data)
            self.get_relevant_jobs_count(title=title,
                                         job_type=job_type,
                                         description=description,
                                         search_str=self.search_str,
                                         contract_only=self.contract_only,
                                         job_dict=job_data,
                                         jobs_list=self.total_cw_jobs_list)
        self.total_cw_df = pd.DataFrame(self.total_cw_jobs_list).drop_duplicates()
        self.total_cw_relevant_jobs = self.total_cw_df.shape[0]
        print(self.total_cw_relevant_jobs)
        return self.df


class CVLibraryJobs(Interfaces, PageCounter):

    def __init__(self, keyword, staring_salary, contract_only):
        super().__init__(keyword, staring_salary, contract_only)
        self.cvlibrary_jobs_df = None
        self.cvlibrary_jobs_list = []
        self.cvlibrary_relevant_jobs = 0
        self.cvlibrary_job_links = self.initialise_cvlibrary_links(results_per_page=self.CVLIBRARY_RESULTS_PER_PAGE,
                                                                   tag_name='p',
                                                                   attrs_dict={'class': 'search-header__results'},
                                                                   keyword=self.keyword,
                                                                   starting_salary=self.starting_salary,
                                                                   contract_only=False)

    @timer
    def get_cvlibrary_jobs(self, job_links):
        for job_link in job_links:
            soup = self.soup(job_link)
            title_tag = soup.find('h1', {'class': 'job__title'})
            title = title_tag.text.strip().splitlines()[0] if title_tag is not None else None
            description_tag = soup.find('div', {'class': 'job__description'})
            description = description_tag.text if description_tag is not None else None
            container = soup.find('div', {'class': 'job__header-info'}).find_all('dd')
            sal_loc_advert_container = list(
                itertools.chain.from_iterable((item.text.strip().splitlines() for item in container)))
            location = sal_loc_advert_container[0]
            salary = sal_loc_advert_container[1] if '£' in sal_loc_advert_container[1] else None
            advertiser = sal_loc_advert_container[-1]
            job_type = soup.find('dl', {'class': 'job__details bottom mt20'}).find_next('dd').text
            job_data = dict(title=title,
                            salary=salary,
                            location=location,
                            job_link=job_link,
                            advertiser=advertiser,
                            job_type=job_type,
                            description=description,
                            search_str=self.search_str,
                            contract_only=self.contract_only)
            self.append_to_df(**job_data)
            self.get_relevant_jobs_count(title=title,
                                         job_type=job_type,
                                         description=description,
                                         search_str=self.search_str,
                                         contract_only=self.contract_only,
                                         job_dict=job_data,
                                         jobs_list=self.cvlibrary_jobs_list)
        self.cvlibrary_jobs_df = pd.DataFrame(self.cvlibrary_jobs_list).drop_duplicates()
        self.cvlibrary_relevant_jobs = self.cvlibrary_jobs_df.shape[0]
        print(self.cvlibrary_relevant_jobs)
        return self.df


class MainWrapper(ReedJobs,
                  IndeedUKJobs,
                  TotalCWJobs):

    @timer
    def __init__(self, keyword, staring_salary, contract_only):
        super().__init__(keyword, staring_salary, contract_only)
        self.df = self.create_pandas_df()
        indeed_uk = Thread(target=self.get_indeed_uk_jobs, args=(self.indeed_uk_job_links,))
        reed = Thread(target=self.get_reed_jobs, args=())
        total_jobs = Thread(target=self.get_jobs, args=(self.total_job_links,))
        cw_jobs = Thread(target=self.get_jobs, args=(self.cw_job_links,))
        # cv_library_jobs = Thread(target=self.get_cvlibrary_jobs, args=(self.cvlibrary_job_links,))

        indeed_uk.start()
        reed.start()
        total_jobs.start()
        cw_jobs.start()
        # cv_library_jobs.start()

        self.TOTAL_JOB_COUNT = len(self.total_job_links) + len(self.cw_job_links) + len(self.indeed_uk_job_links) + \
                               + len(self.reed_job_links)
        # + len(self.cvlibrary_job_links)

        indeed_uk.join()
        reed.join()
        total_jobs.join()
        cw_jobs.join()
        # cv_library_jobs.join()

        job_count_by_site = {
            'total_cw_jobs': [len(self.total_job_links) + len(self.cw_job_links), self.total_cw_relevant_jobs],
            'indeed_uk_jobs': [len(self.indeed_uk_job_links), self.indeed_uk_relevant_jobs],
            'reed_jobs': [len(self.reed_job_links), self.reed_relevant_jobs],
            'cvlibrary_jobs': None,  # [len(self.cvlibrary_job_links), self.cvlibrary_relevant_jobs]
        }
        self.df = self.df.drop_duplicates()
        engine = db_connect()
        create_table(engine)
        self.df.to_sql(name=self.db_format, con=engine, index=False)
        self.to_excel(df=self.df, keyword=self.keyword)
        self.get_job_counts(total_count=self.TOTAL_JOB_COUNT,
                            relevant_count=JobsCounter.RELEVANT_JOBS_COUNT,
                            job_count_by_site=job_count_by_site)


if __name__ == "__main__":
    MainWrapper('python', 80000, contract_only=True)
