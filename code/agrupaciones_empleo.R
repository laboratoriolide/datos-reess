# Registro Estadístico de Empleo en la Seguridad Social (REESS)
# Laboratorio de Investigación para el Desarrollo del Ecuador (LIDE)

# Este script produce agrupaciones de empleo en diferentes agregaciones mediante R. 

# Preliminares ---------------------------------------------------------------------------------------------

# Cargar librerías

library(data.table)
library(dplyr)

# Cargar datos ---------------------------------------------------------------------------------------------

list_of_files <- list.files(path = "data",
                            recursive = TRUE,
                            pattern = ".*2009*.",
                            full.names = TRUE)

data_list <-
  lapply(list_of_files,
         fread)

raw <- 
  rbindlist(data_list)

reess_2009 <-
  raw[provincia %in% 1:24]

# Agrupaciones ---------------------------------------------------------------------------------------------

# Agrupar 2009 por año, mes y provincia (mediante data.table)

empleo_2009 <-
  reess_2009[, .(row_count = .N), by = .(ano, mes, provincia)]

# Repetir el procedimiento para otros años y periodos. 