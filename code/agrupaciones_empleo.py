# Registro Estadístico de Empleo en la Seguridad Social (REESS)
# Laboratorio de Investigación para el Desarrollo del Ecuador (LIDE)

# Este script produce agrupaciones de empleo en diferentes agregaciones

# Preliminares ---------------------------------------------------------------------------------------------

# Importación de librerías

import polars as pl

# Cargar datos y agrupar por provincia ----------------------------------------------------------------------

# Lista de provincias validas

provinces = list(range(1,25))

# Utilizamos "lazy loading" de polars para cargar los datos y agruparlos, optimizando memoria

reess_2009 = (pl.scan_csv("data/BDD_REESS_2009_*.csv", infer_schema_length = 100000000)
              .filter(pl.col("provincia").is_in(provinces))
              .groupby(['ano', 'mes', 'provincia'])
              .count()
              .sort(['provincia', 'ano', 'mes']))

empleo_2009 = reess_2009.collect()  


