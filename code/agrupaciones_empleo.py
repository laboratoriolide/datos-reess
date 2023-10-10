# Registro Estadístico de Empleo en la Seguridad Social (REESS)
# Laboratorio de Investigación para el Desarrollo del Ecuador (LIDE)

# Este script produce agrupaciones de empleo en diferentes agregaciones

# Preliminares ---------------------------------------------------------------------------------------------

# Importación de librerías

import polars as pl

# Cargar datos y agrupar por año, mes y provincia----------------------------------------------------------------------

# Lista de provincias validas

provinces = list(range(1,25))

# Utilizamos "lazy loading" de polars para cargar los datos y agruparlos, optimizando memoria

reess_2009 = (pl.scan_csv("data/BDD_REESS_2009_*.csv", infer_schema_length = 100000000)
                .filter(pl.col("provincia").is_in(provinces))
                .group_by(['ano', 'mes', 'provincia'])
                .count()
                .sort(['provincia', 'ano', 'mes']))

reess_2010 = (pl.scan_csv("data/BDD_REESS_2010_*.csv", infer_schema_length = 100000000)
                .filter(pl.col("provincia").is_in(provinces))
                .group_by(['ano', 'mes', 'provincia'])
                .count()
                .sort(['provincia', 'ano', 'mes']))

reess_2011 = (pl.scan_csv("data/BDD_REESS_2011_*.csv", infer_schema_length = 100000000)
                .filter(pl.col("provincia").is_in(provinces))
                .groupby(['ano', 'mes', 'provincia'])
                .count()
                .sort(['provincia', 'ano', 'mes']))

reess_2012 = (pl.scan_csv("data/BDD_REESS_2012_*.csv", infer_schema_length = 100000000)
                .filter(pl.col("provincia").is_in(provinces))
                .group_by(['ano', 'mes', 'provincia'])
                .count()
                .sort(['provincia', 'ano', 'mes']))

reess_2013 = (pl.scan_csv("data/BDD_REESS_2013_*.csv", infer_schema_length = 100000000)
                .filter(pl.col("provincia").is_in(provinces))
                .group_by(['ano', 'mes', 'provincia'])
                .count()
                .sort(['provincia', 'ano', 'mes']))

reess_2014 = (pl.scan_csv("data/BDD_REESS_2014_*.csv", infer_schema_length = 100000000)
                .filter(pl.col("provincia").is_in(provinces))
                .group_by(['ano', 'mes', 'provincia'])
                .count()
                .sort(['provincia', 'ano', 'mes']))

reess_2015 = (pl.scan_csv("data/BDD_REESS_2015_*.csv", infer_schema_length = 100000000)
                .filter(pl.col("provincia").is_in(provinces))
                .group_by(['ano', 'mes', 'provincia'])
                .count()
                .sort(['provincia', 'ano', 'mes']))

reess_2016 = (pl.scan_csv("data/BDD_REESS_2016_*.csv", infer_schema_length = 100000000)
                .filter(pl.col("provincia").is_in(provinces))
                .group_by(['ano', 'mes', 'provincia'])
                .count()
                .sort(['provincia', 'ano', 'mes']))

reess_2017 = (pl.scan_csv("data/BDD_REESS_2017_*.csv", infer_schema_length = 100000000)
                .filter(pl.col("provincia").is_in(provinces))
                .group_by(['ano', 'mes', 'provincia'])
                .count()
                .sort(['provincia', 'ano', 'mes']))

reess_2018 = (pl.scan_csv("data/BDD_REESS_2018_*.csv", infer_schema_length = 100000000)
                .filter(pl.col("provincia").is_in(provinces))
                .group_by(['ano', 'mes', 'provincia'])
                 .count()
                .sort(['provincia', 'ano', 'mes']))

reess_2019 = (pl.scan_csv("data/BDD_REESS_2019_*.csv", infer_schema_length = 100000000)
                .filter(pl.col("provincia").is_in(provinces))
                .group_by(['ano', 'mes', 'provincia'])
                .count()
                .sort(['provincia', 'ano', 'mes']))

reess_2020 = (pl.scan_csv("data/BDD_REESS_2020_*.csv", infer_schema_length = 100000000)
                .filter(pl.col("provincia").is_in(provinces))
                .group_by(['ano', 'mes', 'provincia'])
                .count()
                .sort(['provincia', 'ano', 'mes']))

reess_2021 = (pl.scan_csv("data/BDD_REESS_2021_*.csv", infer_schema_length = 100000000)
                .filter(pl.col("provincia").is_in(provinces))
                .group_by(['ano', 'mes', 'provincia'])
                .count()
                .sort(['provincia', 'ano', 'mes'])) 

reess_2022 =  (pl.scan_csv("data/BDD_REESS_2022_*.csv", infer_schema_length = 100000000)
                .filter(pl.col("provincia").is_in(provinces))
                .group_by(['ano', 'mes', 'provincia'])
                .count()
                .sort(['provincia', 'ano', 'mes']))

reess_2023 =  (pl.scan_csv("data/BDD_REESS_2023_*.csv", infer_schema_length = 100000000)
                .filter(pl.col("provincia").is_in(provinces))
                .group_by(['ano', 'mes', 'provincia'])
                .count()
                .sort(['provincia', 'ano', 'mes']))

# Lista de todos los años (agrupados)

lista_reess_agrupado = [reess_2009, reess_2010, reess_2011, reess_2012, reess_2013, reess_2014, reess_2015, reess_2016, reess_2017, reess_2018, reess_2019, reess_2020, reess_2021, reess_2022, reess_2023]

# Juntar todos los años en un solo dataframe y transformar a pandas

reess_agrupado = pl.concat(lista_reess_agrupado)

empleo_provincia = reess_agrupado.collect()

empleo_provincia_pd = empleo_provincia.to_pandas()

# Análisis exploratorio de datos ---------------------------------------------------------------------------------------------

# Confirmar cantidad de provincias

provincias_en_base = empleo_provincia_pd['provincia'].unique()

# Confirmar cantidad de meses

meses_en_base = empleo_provincia_pd['mes'].unique()