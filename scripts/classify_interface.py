import pandas as pd
import os

class LabelingInterface:
    def __init__(self, all_skills_file:str, classified_skills_file:str, current_dir:str='.'):
        # Abrimos el archivo de todas las skills
        self.current_dir = current_dir
        self.all_skills_file = f'{self.current_dir}/{all_skills_file}'
        self.classified_skills_file = f'{self.current_dir}/{classified_skills_file}'
        assert os.path.exists(self.all_skills_file)
        self.all_skills = pd.read_parquet(self.all_skills_file)
        if not os.path.exists(self.classified_skills_file): # Suponemos que aún no se ha clasificado ninguna skill
            self.classified_skills = pd.DataFrame(columns=['job', 'n_skill', 'skill', 'label', 'time_log'])
        else:
            self.classified_skills = pd.read_parquet(self.classified_skills_file)
        #self.classified_skills_file = classified_skills_file
        
    def start_labeling(self):
        new_classifications = []
        # Vamos a buscar los skills que no existan en las que ya están clasificadas
        unclassified = self.get_unclassified_skills()
        for row in unclassified:
            job, skill = row.job, row.skill

            
            # Le preguntamos al usuario cuál es el label
            label = input(f'\n{job} \t{skill}\n')
            if label == 'exit':
                if len(new_classifications) > 0:
                    self.save_new_classifications(new_classifications)
                return
            else:
                new_row = list(row.values) + [label]
                new_classifications.append(new_row)
            
    def save_new_classifications(self, new_classifications:list):
        # Creamos un nuevo dataframe con la lista
        new_df = pd.DataFrame(new_classifications, columns=self.classified_skills.columns)
        pd.concat([new_df, self.classified_skills]).to_parquet(self.classified_skills_file, index=False)

    def get_unclassified_skills(self):
        unclassified = []
        for _, row in self.all_skills.iterrows():
            skill = row.skill
            if skill not in self.classified_skills.skill.values:
                unclassified.append(row)
        return unclassified
                

if __name__ == '__main__':
    print('Vamos a comenzar con la clasificación de los skills')
    label_interface = LabelingInterface('data/skills_dataset.parquet', 'skills_classified.parquet', current_dir='..')
    label_interface.start_labeling()

    df = pd.read_parquet('../skills_classified.parquet')
    print(f'Total classified skills: {df.shape[0]}')

