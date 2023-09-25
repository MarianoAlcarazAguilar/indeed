# Scrapping Indeed
Este repositorio sirve para poder descargar los skills de los trabajos que se especifiquen.

### Instrucciones
1. Ejecuta `set_env.sh` desde la ubicación donde está este README
    - Si lo necesitas, instala los requerimientos de python y asegúrate de tener el chromedriver y que selenium funcione adecuadamente
2. Asegúrate de que tienes una archivo en la carpeta de `static` con el formato igual al que se especifica en el ejemplo `ejemplo_jobs.csv`. El título del archivo tiene que ser `job_names.csv`. Puedes moverlo y cambiarle el nombre, pero tendrás que cambiar esos datos en el archivo de `indeed_scrapper.py`
3. Ejecuta `indeed_scrapper.py` desde la carpeta de scripts. Si lo haces desde la ubicación de el README probablemente no funcione.
4. Puedes especificar también las tareas que quieres que haga el scrapper. Pero por default hace lo siguiente:
    - Busca los jobs en google y descarga el html con los links a las páginas de Indeed
    - Limpia los htmls extraídos de google para encontrar los links de Indeed
    - Busca los links de Indeed y guarda los htmls de cada uno de los que se encontró en la búsqueda de Google
    - Limpia los htmls de Indeed y guarda los skills encontrados en un archivo llamado `skills_dataset.parquet` en la carpeta de data.