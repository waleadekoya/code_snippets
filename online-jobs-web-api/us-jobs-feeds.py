from utils import (PageCounter, Interfaces, timer)
from multiprocessing.pool import ThreadPool
from threading import Thread
from sqlalchemy.orm import sessionmaker
from models import db_connect, create_table, JobsDB


class IndeedUSJobs(Interfaces, PageCounter):

    @timer
    def __init__(self, keyword, staring_salary, contract_only):
        super().__init__(keyword, staring_salary, contract_only)
        engine = db_connect()
        create_table(engine)
        self.Session = sessionmaker(bind=engine)
        self.session = self.Session()
        self.df = self.create_pandas_df()
        self.indeed_us_job_links = self.initialise_indeed_links(results_per_page=self.INDEED_US_RESULTS_PER_PAGE,
                                                                tag_name='',
                                                                attrs_dict={'id': 'searchCountPages'},
                                                                keyword=self.keyword,
                                                                starting_salary=self.starting_salary,
                                                                contract_only=self.contract_only,
                                                                dynamic_url=self.INDEED_US_DYNAMIC_URL,
                                                                base_url=self.INDEED_US_BASE_URL,
                                                                url_page=self.INDEED_US_URL_PAGES)
        print('indeed_links: ', self.indeed_us_job_links, len(self.indeed_us_job_links))
        self.run()

    @timer
    def run(self):
        print(len(self.indeed_us_job_links))
        # t = Thread(target=self.get_indeed_us_jobs, args=(self.indeed_us_job_links,))
        self.get_indeed_us_jobs(self.indeed_us_job_links)
        # t.start()
        self.commit_session()
        self.close_session()
        print(self.all_jobs)
        self.df = self.df.drop_duplicates()
        self.to_excel(df=self.df, keyword=self.keyword)

    def add_to_db(self, job_data: dict):
        row = JobsDB(**job_data)
        self.session.add(row)

    def commit_session(self):
        self.session.commit()
        print('session committed to database.')

    def close_session(self):
        self.session.close()
        print('session closed.')

    def get_indeed_us_jobs(self, job_links):
        for job_link in job_links[:5]:
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
            location = location_tag.text.strip().splitlines()[-1].split('-')[-1] if location_tag is not None else None
            job_data['title'] = title
            job_data['salary'] = salary
            job_data['location'] = location
            job_data['job_link'] = job_link
            job_data['advertiser'] = advertiser
            job_data['job_type'] = job_type
            self.add_to_db(job_data)


if __name__ == "__main__":
    IndeedUSJobs('python', 85000, contract_only=True)
