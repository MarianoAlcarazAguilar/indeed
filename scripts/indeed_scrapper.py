from selenium_controler import Controler
from bs4 import BeautifulSoup
import pandas as pd
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

def search_on_google_and_save_html(controler:Controler, job_title:str, html_files:str, scroll_down_times:int=25, maximize_window=False):
    google_url = 'https://www.google.com/search?q='
    site_url = 'site:https://www.indeed.com/hire/job-description/'

    job_url = f'{google_url}{job_title}+{get_site_url(site_url)}'
    controler.open_url(url=job_url, maximize_window=maximize_window)

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

def search_jobs_indeed(controler:Controler, indeed_links_files:str, job_title:str, indeed_html_files:str, skills_dataset_location:str, skills_dataset_filename:str, maximize_window=False):
    # Abrimos el parquet
    if not os.path.exists(f'{indeed_links_files}/{job_title}.parquet'):
        return
    # Abrimos el parquet de existing skills para asegurarnos de que no estemos abriendo cosas que ya habíamos abierto
    if os.path.exists(f'{skills_dataset_location}/{skills_dataset_filename}.parquet'):
        existing_skills = pd.read_parquet(f'{skills_dataset_location}/{skills_dataset_filename}.parquet').job.unique()
    else:
        existing_skills = []
    
    links = pd.read_parquet(f'{indeed_links_files}/{job_title}.parquet')

    # Iteramos sobre los sub-trabajos de cada búsqueda
    for link in links.url:
        indeed_job = link.split('/')[-1]
        indeed_job = clean_indeed_job_title(indeed_job)
        # Aquí tenemos que asegurarnos de que indeed_job no esté ya en skills_dataset
        if indeed_job not in existing_skills:
            controler.open_url(url=link, maximize_window=maximize_window)
            controler.get_html(location=f'{indeed_html_files}/{indeed_job}.html')

def get_job_skills(indeed_html_files:str, job:str):
    '''
    job en formato nombre.html
    '''
    skills = []
    soup = open_html_file(html_files=indeed_html_files, job_title=job.split('.')[0])
    content = soup.find('div', class_='job-description-upper-content col-lg-6')
    if content is None:
        return skills
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
        # No existen skills asociadas a dicho trabajo, entonces lo eliminamos de los htmls existentes
        os.remove(f'{indeed_html_files}/{job}')
        return
    
    if f'{skills_dataset_filename}.parquet' in os.listdir(skills_dataset_location):
        existing_skills = pd.read_parquet(f'{skills_dataset_location}/{skills_dataset_filename}.parquet')
    else:
        existing_skills = pd.DataFrame()
    # Creamos el dataframe
    df = pd.DataFrame(skills, columns=['skill']).assign(job=job.split('.')[0]).rename(lambda x: x+1).reset_index().rename(columns={'index':'n_skill'})[['job', 'n_skill', 'skill']]
    # Lo guardamos con el existente previamente
    pd.concat((df, existing_skills)).drop_duplicates(subset=['skill', 'job']).to_parquet(f'{skills_dataset_location}/{skills_dataset_filename}.parquet', index=False)
    #pd.concat((df, existing_skills)).drop_duplicates(subset=['skill', 'job']).to_csv(f'{skills_dataset_location}/{skills_dataset_filename}.csv', index=False)

def find_new_jobs(indeed_html_files:str, skills_dataset_location:str, skills_dataset_filename=str):
    '''
    Esta función encuentra aquellos nombres de trabajos que no se hayan encontrado anteriormente para solo limpiar esos htmls.
    Regresa una lista con los que hay que limpiar todavía.
    '''
    # Intentamos abrir los skills_dataset_filename para encontrar los existentes, si no existe el archivo, envíamos todos los nombres de los html
    existing_html_jobs = os.listdir(indeed_html_files)
    
    if f'{skills_dataset_filename}.parquet' not in os.listdir(skills_dataset_location): # Si el archivo no existe, entonces no se ha limpiado nada
        return existing_html_jobs
    
    existing_jobs = pd.read_parquet(f'{skills_dataset_location}/{skills_dataset_filename}.parquet').job.unique()
    # Ahora lo que tenemos que hacer es encontrar los nombres de trabajos que estén en existing_html_jobs y no estén en existing_jobs
    new_found_jobs = [job for job in existing_html_jobs if job.split('.')[0] not in existing_jobs]
    
    return new_found_jobs

if __name__ == '__main__':
    search_jobs_in_google = False 
    search_jobs_in_indeed = False
    clean_skills = False

    headless = False
    maximize_window = False

    if search_jobs_in_google or search_jobs_in_indeed:
        controler = Controler(dont_load_images=True, headless=headless)
    
    job_titles_location = '../data/static/job_names.csv'
    google_htmls = '../data/html/google'
    indeed_htmls = '../data/html/indeed'
    indeed_links_location = '../data/indeed_links'
    skills_dataset_location='../data'
    skills_dataset_filename='skills_dataset'

    job_titles = pd.read_csv(job_titles_location)

    if search_jobs_in_google:
        for job_title in job_titles.job:
            print(f'Searching: {job_title}')
            # TODO Si la página de google no es de scrollear infinito hasta abajo, hay que darse cuenta y buscar por páginas
            # TODO Dar la opción de no buscar jobs que ya existan
            search_on_google_and_save_html(controler=controler, job_title=job_title, html_files=google_htmls, scroll_down_times=10, maximize_window=maximize_window)
            clean_and_save_html(html_files=google_htmls, job_title=job_title)
            
    if search_jobs_in_indeed:
        for job_title in job_titles.job:
            print(f'Searching jobs in indeed: {job_title}')
            search_jobs_indeed(controler=controler, indeed_links_files=indeed_links_location, job_title=job_title, indeed_html_files=indeed_htmls, maximize_window=maximize_window, skills_dataset_location=skills_dataset_location, skills_dataset_filename=skills_dataset_filename)

    if search_jobs_in_google or search_jobs_in_indeed:
        controler.quit_driver()

    if clean_skills:
        new_found_jobs  = find_new_jobs(indeed_html_files=indeed_htmls, skills_dataset_location=skills_dataset_location, skills_dataset_filename=skills_dataset_filename)
        for job in new_found_jobs:
            save_skills(indeed_html_files=indeed_htmls, job=job, skills_dataset_location=skills_dataset_location, skills_dataset_filename=skills_dataset_filename)

    
    df = pd.read_parquet(f'{skills_dataset_location}/{skills_dataset_filename}.parquet')
    print(f'Total de skills hasta ahora: {df.shape[0]}')
    print(df.head())

