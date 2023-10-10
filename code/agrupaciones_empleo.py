# Registro Estadístico de Empleo en la Seguridad Social (REESS)
# Laboratorio de Investigación para el Desarrollo del Ecuador (LIDE)

# Este script produce agrupaciones de empleo en diferentes agregaciones

# Preliminares ---------------------------------------------------------------------------------------------

# Importación de librerías

import pandas as pd
import polars as pl
import os

# Cargar diccionarios de variables:

# Diccionario de codigos CIIU (codigos de industria)

ciiu_nivel_1 = pd.read_excel("metadata/diccionario/REESS_Diccionario_Variables.xlsx",
                             sheet_name= 'CIIU1_D',
                             skiprows= 3,
                             names = ['codigo', 'descripcion'])

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

# Agrupacion por CIIU del periodo mas reciente ---------------------------------------------------------------------

# Listar los nombres de todos los archivos en el directorio de datos

archivos_reess = os.listdir('data')

# Eliminar la primera parte de los nombres de los archivos

archivos_reess_anio = [s.replace("BDD_REESS_", "") for s in archivos_reess]

# Extraer los primeros cuatro caracteres de cada nombre de archivo

anios = [int(s[:4]) for s in archivos_reess_anio]

# Seleccionar el maximo

anio_mas_reciente = max(anios)

# Seleccionar solo los nombres de los archivos del año mas reciente

archivos_reess_ultimo_anio = [s for s in archivos_reess if str(anio_mas_reciente) in s]

# Eliminar todo menos los meses

archivos_reess_ultimo_anio_mes = [s.replace("BDD_REESS_" + str(anio_mas_reciente) + "_", "") for s in archivos_reess_ultimo_anio]

# Eliminar la terminacion .csv de los nombres de los archivos

archivos_reess_ultimo_anio_mes = [s.replace(".csv", "") for s in archivos_reess_ultimo_anio_mes]

# Transformar a entero

archivos_reess_ultimo_anio_mes = [int(s) for s in archivos_reess_ultimo_anio_mes]

# Seleccionar el mes mas reciente

mes_mas_reciente = max(archivos_reess_ultimo_anio_mes)

# Crear el nombre del archivo del mes-año mas reciente

archivo_reess_ultimo_anio_mes = "BDD_REESS_" + str(anio_mas_reciente) + "_" + str(mes_mas_reciente) + ".csv"

# Cargar el archivo del mes-año mas reciente

reess_ultimo_anio_mes = (pl.scan_csv("data/" + archivo_reess_ultimo_anio_mes, 
                                     infer_schema_length = 100000000))

# Lista de CIIUs validos

ciiu_validos = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U']

# Agrupar por CIIU 

empleo_ciiu = (reess_ultimo_anio_mes
                .filter(pl.col("ciiu4_1").is_in(ciiu_validos))
                .group_by(['ciiu4_1'])
                .count()
                .sort(['count'], descending=True))

empleo_ciiu_pd = empleo_ciiu.collect().to_pandas()

# Juntar a la base de ciius para obtener la descripcion

empleo_ciiu_pd_merged = pd.merge(empleo_ciiu_pd, ciiu_nivel_1, left_on='ciiu4_1', right_on='codigo', how='left')

# Análisis exploratorio de datos ---------------------------------------------------------------------------------------------

# Confirmar cantidad de provincias

provincias_en_base = empleo_provincia_pd['provincia'].unique()

# Confirmar cantidad de meses

meses_en_base = empleo_provincia_pd['mes'].unique()