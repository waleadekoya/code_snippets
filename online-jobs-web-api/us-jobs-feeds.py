from utils import (PageCounter, Interfaces, timer)
from threading import Thread
from sqlalchemy.orm import sessionmaker
from models import db_connect, create_table, JobsDB
import pandas as pd


class IndeedUSJobs(Interfaces, PageCounter):

    @timer
    def __init__(self, keyword, staring_salary, contract_only):
        super().__init__(keyword, staring_salary, contract_only)
        self.all_jobs = []
        self.engine = db_connect()
        create_table(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

        t = Thread(target=self.run, args=())
        t.start()
        t.join()

    @timer
    def run(self):
        indeed_us_job_links = self.initialise_indeed_links(results_per_page=self.INDEED_US_RESULTS_PER_PAGE,
                                                           tag_name='',
                                                           attrs_dict={'id': 'searchCountPages'},
                                                           keyword=self.keyword,
                                                           starting_salary=self.starting_salary,
                                                           contract_only=self.contract_only,
                                                           dynamic_url=self.INDEED_US_DYNAMIC_URL,
                                                           base_url=self.INDEED_US_BASE_URL,
                                                           url_page=self.INDEED_US_URL_PAGES)
        print(len(indeed_us_job_links))
        self.get_indeed_us_jobs(indeed_us_job_links)
        self.to_db()
        self.to_excel(df=self.out_df, keyword=self.disk_format)

    def add_to_db(self, all_jobs: list):
        for job_data in all_jobs:
            row = JobsDB(**job_data)
            self.session.add(row)

    def commit_session(self):
        self.session.commit()
        print('session committed to database.')

    def close_session(self):
        self.session.close()
        print('session closed.')

    @property
    def create_df(self):
        return pd.DataFrame(self.all_jobs)

    @property
    def _remove_duplicates(self):
        return self.create_df.drop_duplicates()

    @property
    def out_df(self):
        return self._remove_duplicates

    def to_db(self):
        self.out_df.to_sql(name=self.db_format, con=self.engine, index=False)

    def get_indeed_us_jobs(self, job_links):
        for job_link in job_links:
            job_data = {}
            location, job_type, salary, advertiser = (None,) * 4
            soup = self.soup(job_link)
            description = soup.find('div', {'id': "jobDescriptionText", 'class': "jobsearch-jobDescriptionText"}).text
            title = soup.find('div', {'class': 'jobsearch-JobInfoHeader-title-container'}).text
            advertiser_tag = soup.find('div', {'class': 'icl-u-lg-mr--sm icl-u-xs-mr--xs'})
            advertiser = advertiser_tag.text if advertiser_tag is not None else None
            location_tag = soup.find('div', {'class': 'icl-u-xs-mt--xs icl-u-textColor--secondary '
                                                      'jobsearch-JobInfoHeader-subtitle '
                                                      'jobsearch-DesktopStickyContainer-subtitle'})

            if self.search_str in str(title).lower() or self.search_str in str(description).lower():
                location = location_tag.text.strip().splitlines()[-1].split('-')[
                    -1] if location_tag is not None else None
                job_data['title'] = title
                job_data['salary'] = salary
                job_data['location'] = location
                job_data['advertiser'] = advertiser
                job_data['job_type'] = job_type
                job_data['job_link'] = job_link
                self.all_jobs.append(job_data)


if __name__ == "__main__":
    IndeedUSJobs('python', 85000, contract_only=True)
