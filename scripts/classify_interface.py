import pandas as pd
import os

class LabelingInterface:
    def __init__(self, all_skills_file:str, classified_skills_file:str):
        # Abrimos el archivo de todas las skills
        assert os.path.exists(all_skills_file)
        self.all_skills = pd.read_parquet(all_skills_file)
        if not os.path.exists(classified_skills_file): # Suponemos que aún no se ha clasificado ninguna skill
            self.classified_skills = pd.DataFrame(columns=['job', 'n_skill', 'skill', 'label'])
        else:
            self.classified_skills = pd.read_parquet(classified_skills_file)
        self.classified_skills_file = classified_skills_file
        
    def start_labeling(self):
        new_classifications = []
        # Vamos a buscar los skills que no existan en las que ya están clasificadas

        for _, row in self.all_skills.iterrows():
            job, skill = row.job, row.skill

            if skill not in self.classified_skills.skill.values:
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
                

if __name__ == '__main__':
    print('Vamos a comenzar con la clasificación de los skills')
    label_interface = LabelingInterface('../data/skills_dataset.parquet', '../skills_classified.parquet')
    label_interface.start_labeling()

    df = pd.read_parquet('../skills_classified.parquet')
    print(f'Total classified skills: {df.shape[0]}')

