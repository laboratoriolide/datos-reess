# Registro Estadístico de Empleo en la Seguridad Social (REESS)
# Laboratorio de Investigación para el Desarrollo del Ecuador (LIDE)

# Este script produce agrupaciones de empleo por mes-año y CIIU nivel 1

# Preliminares ---------------------------------------------------------------------------------------------

# Importación de librerías

import pandas as pd
import polars as pl
import os
import matplotlib.pyplot as plt
import datetime

# Cargar agrupacion de empleos por ciiu en periodo mas reciente

empleo_por_ciiu = pd.read_csv("output/empleo_ciiu_1.csv")

# Extraer los 5 CIIUs mas grandes del mes-año mas reciente

top_5_ciiu_1 = empleo_por_ciiu.head(5)['ciiu4_1'].tolist()

# Cargar datos (agrupando) ---------------------------------------------------------------------------------------------

# Lista de provincias validas

provinces = list(range(1,25))

# Utilizamos "lazy loading" de polars para cargar los datos y agruparlos, optimizando memoria

empleo_ciiu_2009 = (pl.scan_csv("data/BDD_REESS_2009_*.csv", infer_schema_length = 100000000)
                    .filter(pl.col("provincia").is_in(provinces))
                    .filter(pl.col("ciiu4_1").is_in(top_5_ciiu_1))
                    .group_by(['ano', 'mes', 'provincia', 'ciiu4_1'])
                    .count()
                    .sort(['provincia', 'ciiu4_1', 'ano', 'mes']))

empleo_ciiu_2010 = (pl.scan_csv("data/BDD_REESS_2010_*.csv", infer_schema_length = 100000000)
                    .filter(pl.col("provincia").is_in(provinces))
                    .filter(pl.col("ciiu4_1").is_in(top_5_ciiu_1))
                    .group_by(['ano', 'mes', 'provincia', 'ciiu4_1'])
                    .count()
                    .sort(['provincia', 'ciiu4_1', 'ano', 'mes']))

empleo_ciiu_2011 = (pl.scan_csv("data/BDD_REESS_2011_*.csv", infer_schema_length = 100000000)
                    .filter(pl.col("provincia").is_in(provinces))
                    .filter(pl.col("ciiu4_1").is_in(top_5_ciiu_1))
                    .group_by(['ano', 'mes', 'provincia', 'ciiu4_1'])
                    .count()
                    .sort(['provincia', 'ciiu4_1', 'ano', 'mes']))

empleo_ciiu_2012 = (pl.scan_csv("data/BDD_REESS_2012_*.csv", infer_schema_length = 100000000)
                    .filter(pl.col("provincia").is_in(provinces))
                    .filter(pl.col("ciiu4_1").is_in(top_5_ciiu_1))
                    .group_by(['ano', 'mes', 'provincia', 'ciiu4_1'])
                    .count()
                    .sort(['provincia', 'ciiu4_1', 'ano', 'mes']))

empleo_ciiu_2013 = (pl.scan_csv("data/BDD_REESS_2013_*.csv", infer_schema_length = 100000000)
                    .filter(pl.col("provincia").is_in(provinces))
                    .filter(pl.col("ciiu4_1").is_in(top_5_ciiu_1))
                    .group_by(['ano', 'mes', 'provincia', 'ciiu4_1'])
                    .count()
                    .sort(['provincia', 'ciiu4_1', 'ano', 'mes']))

empleo_ciiu_2014 = (pl.scan_csv("data/BDD_REESS_2014_*.csv", infer_schema_length = 100000000)
                    .filter(pl.col("provincia").is_in(provinces))
                    .filter(pl.col("ciiu4_1").is_in(top_5_ciiu_1))
                    .group_by(['ano', 'mes', 'provincia', 'ciiu4_1'])
                    .count()
                    .sort(['provincia', 'ciiu4_1', 'ano', 'mes']))

empleo_ciiu_2015 = (pl.scan_csv("data/BDD_REESS_2015_*.csv", infer_schema_length = 100000000)
                    .filter(pl.col("provincia").is_in(provinces))
                    .filter(pl.col("ciiu4_1").is_in(top_5_ciiu_1))
                    .group_by(['ano', 'mes', 'provincia', 'ciiu4_1'])
                    .count()
                    .sort(['provincia', 'ciiu4_1', 'ano', 'mes']))

empleo_ciiu_2016 = (pl.scan_csv("data/BDD_REESS_2016_*.csv", infer_schema_length = 100000000)
                    .filter(pl.col("provincia").is_in(provinces))
                    .filter(pl.col("ciiu4_1").is_in(top_5_ciiu_1))
                    .group_by(['ano', 'mes', 'provincia', 'ciiu4_1'])
                    .count()
                    .sort(['provincia', 'ciiu4_1', 'ano', 'mes']))

empleo_ciiu_2017 = (pl.scan_csv("data/BDD_REESS_2017_*.csv", infer_schema_length = 100000000)
                    .filter(pl.col("provincia").is_in(provinces))
                    .filter(pl.col("ciiu4_1").is_in(top_5_ciiu_1))
                    .group_by(['ano', 'mes', 'provincia', 'ciiu4_1'])
                    .count()
                    .sort(['provincia', 'ciiu4_1', 'ano', 'mes']))

empleo_ciiu_2018 = (pl.scan_csv("data/BDD_REESS_2018_*.csv", infer_schema_length = 100000000)
                    .filter(pl.col("provincia").is_in(provinces))
                    .filter(pl.col("ciiu4_1").is_in(top_5_ciiu_1))
                     .group_by(['ano', 'mes', 'provincia', 'ciiu4_1'])
                    .count()
                    .sort(['provincia', 'ciiu4_1', 'ano', 'mes']))

empleo_ciiu_2019 = (pl.scan_csv("data/BDD_REESS_2019_*.csv", infer_schema_length = 100000000)
                    .filter(pl.col("provincia").is_in(provinces))
                    .filter(pl.col("ciiu4_1").is_in(top_5_ciiu_1))
                     .group_by(['ano', 'mes', 'provincia', 'ciiu4_1'])
                    .count()
                    .sort(['provincia', 'ciiu4_1', 'ano', 'mes']))

empleo_ciiu_2020 = (pl.scan_csv("data/BDD_REESS_2020_*.csv", infer_schema_length = 100000000)
                    .filter(pl.col("provincia").is_in(provinces))
                    .filter(pl.col("ciiu4_1").is_in(top_5_ciiu_1))
                     .group_by(['ano', 'mes', 'provincia', 'ciiu4_1'])
                    .count()
                    .sort(['provincia', 'ciiu4_1', 'ano', 'mes']))

empleo_ciiu_2021 = (pl.scan_csv("data/BDD_REESS_2021_*.csv", infer_schema_length = 100000000)
                    .filter(pl.col("provincia").is_in(provinces))
                    .filter(pl.col("ciiu4_1").is_in(top_5_ciiu_1))
                     .group_by(['ano', 'mes', 'provincia', 'ciiu4_1'])
                    .count()
                    .sort(['provincia', 'ciiu4_1', 'ano', 'mes']))

empleo_ciiu_2022 = (pl.scan_csv("data/BDD_REESS_2022_*.csv", infer_schema_length = 100000000)
                     .filter(pl.col("provincia").is_in(provinces))
                     .filter(pl.col("ciiu4_1").is_in(top_5_ciiu_1))
                     .group_by(['ano', 'mes', 'provincia', 'ciiu4_1'])
                     .count()
                     .sort(['provincia', 'ciiu4_1', 'ano', 'mes']))

empleo_ciiu_2023 = (pl.scan_csv("data/BDD_REESS_2023_*.csv", infer_schema_length = 100000000)
                     .filter(pl.col("provincia").is_in(provinces))
                     .filter(pl.col("ciiu4_1").is_in(top_5_ciiu_1))
                     .group_by(['ano', 'mes', 'provincia', 'ciiu4_1'])
                     .count()
                     .sort(['provincia', 'ciiu4_1', 'ano', 'mes']))

# Unir los datos en un solo dataframe ---------------------------------------------------------------------------------------------

# Lista de todos los años (agrupados)

lista_empleo_agrupado = [empleo_ciiu_2009, empleo_ciiu_2010, empleo_ciiu_2011, empleo_ciiu_2012, empleo_ciiu_2013, empleo_ciiu_2014, empleo_ciiu_2015, empleo_ciiu_2016, empleo_ciiu_2017, empleo_ciiu_2018, empleo_ciiu_2019, empleo_ciiu_2020, empleo_ciiu_2021, empleo_ciiu_2022, empleo_ciiu_2023]

# Juntar todos los años en un solo dataframe y transformar a pandas

empleo_agrupado = pl.concat(lista_empleo_agrupado)

empleo_provincia_ciiu = empleo_agrupado.collect()

empleo_provincia_ciiu_pd = empleo_provincia_ciiu.to_pandas()

# Manipular datos ---------------------------------------------------------------------------------------------

# Transformar mes y año a formato fecha

empleo_provincia_ciiu_pd['dia'] = 1

empleo_provincia_ciiu_pd['fecha_string'] = empleo_provincia_ciiu_pd['ano'].astype(str) + '-' + empleo_provincia_ciiu_pd['mes'].astype(str) + '-' + empleo_provincia_ciiu_pd['dia'].astype(str) 

empleo_provincia_ciiu_pd['fecha'] = pd.to_datetime(empleo_provincia_ciiu_pd['fecha_string'], format='%Y-%m-%d')

# Transformar provincias a formato string

empleo_provincia_ciiu_pd['provincia'] = empleo_provincia_ciiu_pd.loc[:,'provincia'].astype(str)

# Seleccionar solo columnas relevantes

empleo_provincia_ciiu_pd = empleo_provincia_ciiu_pd[['fecha', 'provincia', 'ciiu4_1', 'count',]]

# Agrupar a nivel nacional por mes-año y CIIU, cambiar a formato wide (reshape)

empleo_provincia_ciiu_pd = (empleo_provincia_ciiu_pd
                            .groupby(['fecha', 'ciiu4_1'])
                            .sum('count')
                            .sort_values(['fecha', 'ciiu4_1'])
                            .reset_index()
                            .pivot(index='fecha', columns='ciiu4_1', values='count'))

# Exportar a csv 

empleo_provincia_ciiu_pd.to_csv('output/empleo_nacional_por_ciiu.csv')

# Visualización de datos ---------------------------------------------------------------------------------------------

# Visualizacion simple del trabajo por cada CIIU, para todas las provincias

empleo_ciiu = (empleo_provincia_ciiu_pd
               .groupby(['fecha', 'ciiu4_1'])
               .sum('count')
               .sort_values(['fecha', 'ciiu4_1'])
               .reset_index())

# Filtrar el dataframe por cada ciiu

empleo_ciiu_A = empleo_ciiu[empleo_ciiu['ciiu4_1'] == 'A']

empleo_ciiu_C = empleo_ciiu[empleo_ciiu['ciiu4_1'] == 'C']

empleo_ciiu_G = empleo_ciiu[empleo_ciiu['ciiu4_1'] == 'G']

empleo_ciiu_N = empleo_ciiu[empleo_ciiu['ciiu4_1'] == 'N']

empleo_ciiu_O = empleo_ciiu[empleo_ciiu['ciiu4_1'] == 'O']

# Grafico simple de lineas

plt.plot(empleo_ciiu_A['fecha'], 
         empleo_ciiu_A['count'], 
         label = 'Agricultura, ganadería, caza y silvicultura')

plt.plot(empleo_ciiu_C['fecha'],
            empleo_ciiu_C['count'],
            label = 'Industrias manufactureras')

plt.plot(empleo_ciiu_G['fecha'],
            empleo_ciiu_G['count'],
            label = 'Comercio al por mayor y al por menor')

plt.plot(empleo_ciiu_N['fecha'],
            empleo_ciiu_N['count'],
            label = 'Actividades administrativas y servicios de apoyo')

plt.plot(empleo_ciiu_O['fecha'],
            empleo_ciiu_O['count'],
            label = 'Actividades de organizaciones y entidades extraterritoriales')

year_2020 = datetime.datetime(2020, 3, 1)

plt.axvline(x= year_2020, color='red', linestyle='--')

plt.legend(loc='upper center', bbox_to_anchor=(0.5, -0.1))
plt.title('Evolución del empleo por CIIU')
plt.show()