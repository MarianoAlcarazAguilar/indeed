import yaml
import datetime
import streamlit as st
from yaml.loader import SafeLoader
import streamlit_authenticator as stauth
from scripts import classify_interface as ci

def iterator_session(iterator:iter):
    st.session_state.iterator = iterator

def submit():
    st.session_state.something = st.session_state.widget
    st.session_state.widget = ''

def render_page():
    if 'something' not in st.session_state:
        st.session_state.something = ''

    st.title('Labeling')
    classifier = ci.LabelingInterface('/data/skills_dataset.parquet', '/skills_classified.parquet', current_dir='.')
    unclassified_skills = classifier.get_unclassified_skills()

    row = unclassified_skills[0]
    job, skill = row.job, row.skill
    
    st.write(job)
    st.write(skill)
    label = st.text_input('Give the label', key='widget', on_change=submit)
    now = datetime.datetime.now()
    
    new_label = [list(row.values) + [label, now]]
    classifier.save_new_classifications(new_label)

def authenticate_user():
    '''
    Utiliza esta función para autenticar usuarios.
    Yo no la voy a usar
    '''
    config_yaml_file_location = 'config.yml'

    with open(config_yaml_file_location) as file:
        config = yaml.load(file, Loader=SafeLoader)

    authenticator = stauth.Authenticate(
        config['credentials'],
        config['cookie']['name'],
        config['cookie']['key'],
        config['cookie']['expiry_days'],
        config['preauthorized']
    )

    return authenticator.login('Login', 'main')


if __name__ == '__main__':
    render_page()