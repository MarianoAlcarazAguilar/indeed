from selenium_controler import Controler
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
import pandas as pd
import requests
import time
import os
import re

def get_site_url(site_url:str) -> str:
    '''
    Utiliza esta función para convertir los caracteres a la forma en como se busca en google
    '''
    # Es importante destacar que en las búsquedas de Google se cambian los caracteres de la siguiente forma:
    # ':' -> '%3A'
    # '/' -> '%2F'
    aux = site_url.replace(':', '%3A')
    aux = aux.replace('/', '%2F')
    return aux

def search_on_google_and_save_html(controler:Controler, job_title:str, html_files:str, scroll_down_times:int=25):
    google_url = 'https://www.google.com/search?q='
    site_url = 'site:https://www.indeed.com/hire/job-description/'

    job_url = f'{google_url}{job_title}+{get_site_url(site_url)}'
    controler.open_url(url=job_url, maximize_window=True)

    # Bajamos n veces en la pantalla
    for _ in range(scroll_down_times):
        controler.scroll_down_with_keys()
        time.sleep(.2)

    controler.get_html(f'{html_files}/{job_title}.html')

def open_html_file(html_files:str, job_title:str) -> BeautifulSoup:
    with open(f'{html_files}/{job_title}.html', 'r', encoding='utf-8') as f:
        content = f.read()

    soup = BeautifulSoup(content, 'html.parser')
    return soup

def get_hrefs_in_soup(soup:BeautifulSoup):
    divs = soup.find_all('div', class_='yuRUbf')
    hrefs = [div.find('a').get('href') for div in divs]
    return hrefs

def create_and_save_hrefs(hrefs:list, job_title:str, indeed_links_location:str):
    if f'{job_title}.parquet' in os.listdir(indeed_links_location):
        existing_links = pd.read_parquet(f'{indeed_links_location}/{job_title}.parquet')
    else:
        existing_links = pd.DataFrame()

    (pd
     .DataFrame(hrefs, columns=['url'])
     .assign(search_term=job_title)
     .pipe(lambda df: pd.concat((df, existing_links), ignore_index=True))
     .drop_duplicates()
     .to_parquet(f'{indeed_links_location}/{job_title}.parquet', index=False)
    )

def clean_and_save_html(html_files:str, job_title:str):
    soup = open_html_file(html_files=html_files, job_title=job_title)
    hrefs = get_hrefs_in_soup(soup=soup)
    create_and_save_hrefs(hrefs=hrefs, job_title=job_title, indeed_links_location=indeed_links_location)

def clean_indeed_job_title(job:str):
    resultado = re.sub(r'\?.*', '', job)
    return resultado

def search_jobs_indeed(controler:Controler, indeed_links_files:str, job_title:str, indeed_html_files:str):
    # Abrimos el parquet
    links = pd.read_parquet(f'{indeed_links_files}/{job_title}.parquet')

    # Iteramos sobre los sub-trabajos de cada búsqueda
    for link in links.url:
        indeed_job = link.split('/')[-1]
        indeed_job = clean_indeed_job_title(indeed_job)
        controler.open_url(url=link, maximize_window=True)
        controler.get_html(location=f'{indeed_htmls}/{indeed_job}.html')

def get_job_skills(indeed_html_files:str, job:str):
    '''
    job en formato nombre.html
    '''
    # TODO: Asegurarse de que el job no esté ya en los tasks limpios
    skills = []
    soup = open_html_file(html_files=indeed_html_files, job_title=job.split('.')[0])
    content = soup.find('div', class_='job-description-upper-content col-lg-6')
    tasks = content.find_all('li')
    for task in tasks:
        parent_attrs = task.parent.attrs
        if len(parent_attrs) == 0:
            skills.append(task.text)

    return skills

def save_skills(indeed_html_files:str, job:str, skills_dataset_location:str, skills_dataset_filename:str):
    '''
    job en formato nombre.html
    '''
    # Abrimos el html y lo limpiamos
    skills = get_job_skills(indeed_html_files=indeed_html_files, job=job)
    if len(skills) == 0:
        return
    
    if f'{skills_dataset_filename}.parquet' in os.listdir(skills_dataset_location):
        existing_skills = pd.read_parquet(f'{skills_dataset_location}/{skills_dataset_filename}.parquet')
    else:
        existing_skills = pd.DataFrame()
    # Creamos el dataframe
    df = pd.DataFrame(skills, columns=['skill']).assign(job=job.split('.')[0]).rename(lambda x: x+1).reset_index().rename(columns={'index':'n_skill'})[['job', 'n_skill', 'skill']]
    # Lo guardamos con el existente previamente
    pd.concat((df, existing_skills)).drop_duplicates(subset=['skill', 'job']).to_parquet(f'{skills_dataset_location}/{skills_dataset_filename}.parquet', index=False)
    pd.concat((df, existing_skills)).drop_duplicates(subset=['skill', 'job']).to_csv(f'{skills_dataset_location}/{skills_dataset_filename}.csv', index=False)

    
if __name__ == '__main__':
    search_jobs_in_google = False
    clean_html_files = False
    search_jobs_in_indeed = False
    clean_skills = True

    if search_jobs_in_google or search_jobs_in_indeed:
        controler = Controler(dont_load_images=True)
    
    job_titles_location = '../data/static/job_names.csv'
    google_htmls = '../data/html/google'
    indeed_htmls = '../data/html/indeed'
    indeed_links_location = '../data/indeed_links'

    job_titles = pd.read_csv(job_titles_location)

    if search_jobs_in_google:
        for job_title in job_titles.job:
            # TODO Si la página de google no es de scrollear infinito hasta abajo, hay que darse cuenta y buscar por páginas
            # TODO Dar la opción de no buscar jobs que ya existan
            search_on_google_and_save_html(controler=controler, job_title=job_title, html_files=google_htmls, scroll_down_times=3)

    if clean_and_save_html:
        for job_title in job_titles.job:
            clean_and_save_html(html_files=google_htmls, job_title=job_title)

    if search_jobs_in_indeed:
        for job_title in job_titles.job:
            # TODO Evitar búsqueda de trabajos que ya existan en mi dataset
            search_jobs_indeed(controler=controler, indeed_links_files=indeed_links_location, job_title=job_title, indeed_html_files=indeed_htmls)

    if search_jobs_in_google or search_jobs_in_indeed:
        controler.quit_driver()

    if clean_skills:
        for job in os.listdir(indeed_htmls):
            save_skills(indeed_html_files=indeed_htmls, job=job, skills_dataset_location='../data', skills_dataset_filename='skills_dataset')

            



        
        
    # Vamos a abrir uno de los html de indeed, porque creo que no hay nada ahí
    #soup = open_html_file(html_files=indeed_htmls, job_title='farmer')
    #content = soup.find('div', class_='job-description-upper-content col-lg-6')
    #print(content.find_all('li'))
    


    

