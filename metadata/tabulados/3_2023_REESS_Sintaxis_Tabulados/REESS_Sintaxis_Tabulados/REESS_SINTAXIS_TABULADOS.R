################################################################################
# TÍTULO DE LA SINTAXIS:                                                       #
# Indicadores Laborales (Tabulados)                                            #
# OPERACIÓN ESTADÍSTICA:                                                       #
# Registros Administrativos de la Seguridad Social (IESS)                      #
# ENTIDAD EJECUTORA:                                                           #
# Instituto Nacional de Estadísticas y Censo                                   #
# UNIDAD TÉCNICA RESPONSABLE:                                                  #
# Dirección de Estudios y Análisis de la Información (DEAN)                    #
# UNIDAD TÉCNICA EJECUTORA:                                                    #
# Direccción de Estadísticas Económicas (DECON)                                #
# Gestión de Estadísticas Económicas en Base a Registros                       #
# Administrativos (GEERA)                                                      #
# =============================================================================#
# Fecha de elaboración: 22/02/2021                                             #
# Fecha última modificación: 27/08/2021                                        #
# =============================================================================#
# Elaborado por:                                                               #
# Andrés Villacís M.                                                           #
# Analista de la Dirección de Estudios y Análisis de la Información            #
# Instituto Nacional de Estadística y Censos                                   #
# andres_villacis@inec.gob.ec                                                  #
# =============================================================================#
# Revisado por                                                                 #
# Diego del Pozo                                                               #
# Dirección de Estudios y Análisis de la Información (DEAN)                    #
#                                                                              #
# Cristhian Rosales                                                            #
# Dirección de Estudios y Análisis de la Información (DEAN)                    #
# =============================================================================#
# Modificado para publicación:                                                 #
# Daniel Vera I.                                                               #
# Analista DECON                                                               #
# Instituto Nacional de Estadística y Censos                                   #
# daniel_vera@inec.gob.ec                                                      #
#                                                                              # 
# Kevin Estrella P.                                                            #
# Asistente DECON                                                              #
# Instituto Nacional de Estadística y Censos                                   #
# kevin_estrella@inec.gob.ec                                                   #
################################################################################

#==============================================================================#
# CARGAR LIBRERIAS                                                             #
#==============================================================================#

# Instalar librerias (en caso que no se tengan instaladas previamente)
# install.packages("future.apply")
# install.packages("dplyr")
# install.packages("openxlsx")
# install.packages("stringr")
# Llamar a las librerias
library(dplyr)
library(openxlsx)
library(stringr)
library(future.apply)

#===============================================================================
rm(list = ls())
gc()
T1 <- Sys.time()
#==============================================================================#
# REFERENCIA DOCUMENTOS                                                        #
#==============================================================================#

# Referencias Excel
setwd("D:\\REESS_Sintaxis_Tabulados\\Archivos para excel")
d_clas <- paste0(getwd(),"\\Clasificaciones_1.xlsx") 
img_banner <- paste0(getwd(),"\\INEC_imagen_banner_2021.jpg") 
img_empleo_plazas <- paste0(getwd(),"\\Empleo_Plazas_IESS.jpg")
re_tab <- "D:\\REESS_Sintaxis_Tabulados\\Tabulados"

# Referencia filtro Empleo o Plazas (Plazas (1)- Empleo (2))
EP <- 1

# Referencia tipo de carga de bases Hist_Men (Histórico (1))
Hist_Men <- 1

#==============================================================================#
# CARGAR BASES DE DATOS                                                        #
#==============================================================================#

# Directorio y Archivos a cargar
# Las bases de datos deben estar guardadas en la siguiente ubicación: 
setwd("D:\\Bases_REESS")
bases <- gtools::mixedsort(list.files(getwd(), pattern = "BDD_REESS")) 
bases <- bases[!str_detect(bases, "2006|2007|2008")] 

#==============================================================================#
# FUNCIONES PARA OBTENCIÓN DE INDICADORES LABORALES                            #                                      
#==============================================================================#

# 1
Ind_Lab <- function (y,...){
  a <- y %>% 
    select(..., starts_with("m-")) %>% 
    group_by(!!!ensyms(...)) %>%
    summarise(across(starts_with("m-"),sum))
  b <- y %>% 
    group_by(total) %>% 
    select(total,starts_with("m-")) %>% 
    summarise(across(starts_with("m-"), sum))%>%  
    rename("{names(a[1])}":="total") 
  c <- full_join(a,b)
}

# 2
Ind_Lab_2 <- function (y,...,z){
  a <- y %>%
    select(..., starts_with("sueldo")) %>% 
    group_by(!!!ensyms(...)) %>%
    summarise(across(starts_with("sueldo"), z))
  b <- y %>% 
    select(total, starts_with("sueldo")) %>% 
    group_by(total) %>% 
    summarise(across(starts_with("sueldo"), z)) %>%  
    rename("{names(a)[1]}":="total")
  c <- full_join(a,b)
}

#===============================================================================
#                INDICADORES LABORALES (CODIGO EN PARALELO)
#===============================================================================

#################################################################
# Establecer el ambiente para trabajo en paralelo (n-2 núcleos) #
  plan(multisession,workers = availableCores()-2)               #
#################################################################

#-------------------------------------------------------------------------------
# Generación de Indicadores Laborales
#-------------------------------------------------------------------------------
Bases_ind_lab <- future_lapply(bases,function(bases){

##########################
# Manejo Bases de Datos  #
##########################
  
# Actualización Información
d <- c("sueldo", "tipo_empleador", "genero","sector_afiliacion",
         "agrupa","canton","provincia","plazas_total","empleo_total",
         "tamano_empleo","edadc","edadcj",
         "ciiu4_4","ciiu4_1","ciiu4_2","ciiu4_3","ano","mes")

Base_trabajo <- readr::read_csv(paste(getwd(),{{bases}}, sep="/"), col_select = d)

# Filtros de  Plazas y Empleo
if(EP == 1){
  Base_trabajo_f <- filter(Base_trabajo, plazas_total<=6)
} else {
  Base_trabajo_f <- filter(Base_trabajo, empleo_total<=6)  
}
rm (Base_trabajo)
gc()
Base_trabajo_f<- Base_trabajo_f %>%
  mutate(!!paste("m", 
                 format(as.Date(paste(Base_trabajo_f$ano[1], Base_trabajo_f$mes[1],
                 "01",sep = "-")),"%b-%y"), sep = "-") := 1)

Base_trabajo_f<- Base_trabajo_f %>%
  rename(!!paste("sueldo", 
                 format(as.Date(paste(Base_trabajo_f$ano[1], Base_trabajo_f$mes[1],
                 "01",sep = "-")),"%b-%y"),sep = "-") := "sueldo")

# Cambiar valores por etiqutes
Base_trabajo_f <- Base_trabajo_f %>% 
  mutate(across(c("ciiu4_1","ciiu4_4","ciiu4_3","ciiu4_2",
                  "tamano_empleo", "agrupa","edadc","edadcj"),sjlabelled::as_label))

Base_trabajo_f <- Base_trabajo_f %>% 
  mutate(across(c("ciiu4_1","ciiu4_4","ciiu4_3","ciiu4_2",
                  "tamano_empleo", "agrupa","edadc","edadcj"),as.character))

# Variables Rama de Actividad agrupada comparativo ENEMDU

# Ciiu
Base_trabajo_f <- Base_trabajo_f %>% 
  mutate(ciiu_e = case_when(ciiu4_1 == "A" ~ "Agricultura",
                            ciiu4_1 == "B" ~ "Minas",             
                            ciiu4_1 == "C" ~ "Manufacturas",
                            ciiu4_1 == "G" ~ "Comercio",
                            ciiu4_1 == "F" ~ "Construcción",
                            ciiu4_1 == "Z0_Nocla_CIIU" ~ "Z0_No clasificado_CIIU",
                            sector_afiliacion == 5 ~ "Z4_Voluntario",
                            sector_afiliacion == 3 ~ "Z1_Doméstico",
                            sector_afiliacion == 4 ~ "Z3_Semicontribuyentes",
                            sector_afiliacion == 6 ~ "Z2_Campesino",
                            TRUE ~ "Servicios"))
# Provincia
Base_trabajo_f <- Base_trabajo_f %>% 
  mutate(across(provincia, ~case_when(provincia <= 9 ~ as.character(paste0("0", provincia)),
                                      provincia == 99999 ~ "Z0_Nocla_ubicacion",
                                      provincia == 999999 ~ "Z4_Voluntario",
                                      provincia == 9999999 ~ "Z1_Doméstico",
                                      provincia == 99999999 ~ "Z3_Semicontribuyente",
                                      provincia == 999999999 ~ "Z2_Campesino",
                                      TRUE ~ as.character(provincia))))

# Cantón
Base_trabajo_f <-Base_trabajo_f %>% 
  mutate(across(canton,
                ~case_when(nchar(canton) == 3 ~ as.character(paste0("0",canton)),
                           nchar(canton) == 2 ~ as.character(paste0("00",canton)),
                           nchar(canton) == 1 ~ as.character(paste0("000",canton)),
                           canton == 99999 ~ "Z0_Nocla_ubicacion",
                           canton == 999999 ~ "Z4_Voluntario",
                           canton == 9999999 ~ "Z1_Doméstico",
                           canton == 99999999 ~ "Z3_Semicontribuyente",
                           canton == 999999999 ~ "Z2_Campesino",
                           TRUE ~ as.character(canton))))

# Ciiu_1
Base_trabajo_f <- Base_trabajo_f %>% 
  mutate(across(ciiu4_1, 
                ~case_when(ciiu4_1 == "Z0_Nocla_CIIU" ~ "Z0_No clasificado_CIIU",
                           sector_afiliacion == 3 ~ "Z1_Doméstico",
                           sector_afiliacion == 4 ~ "Z3_Semicontribuyente",
                           sector_afiliacion == 5 ~ "Z4_Voluntario",
                           sector_afiliacion == 6 ~ "Z2_Campesino",
                           TRUE ~ ciiu4_1)))

# Ciiu_2
Base_trabajo_f <- Base_trabajo_f %>% 
  mutate(across(ciiu4_2, 
                ~case_when(ciiu4_2 == "Z0_Nocla_CIIU" ~ "Z0_No clasificado_CIIU",
                           sector_afiliacion == 3 ~ "Z1_Doméstico",
                           sector_afiliacion == 4 ~ "Z3_Semicontribuyente",
                           sector_afiliacion == 5 ~ "Z4_Voluntario",
                           sector_afiliacion == 6 ~ "Z2_Campesino",
                           TRUE ~ ciiu4_2)))
# Ciiu_3
Base_trabajo_f <- Base_trabajo_f %>% 
  mutate(across(ciiu4_3, 
                ~case_when(ciiu4_3 == "Z0_Nocla_CIIU" ~ "Z0_No clasificado_CIIU",
                sector_afiliacion == 3 ~ "Z1_Doméstico",
                sector_afiliacion == 4 ~ "Z3_Semicontribuyente",
                sector_afiliacion == 5 ~ "Z4_Voluntario",
                sector_afiliacion == 6 ~ "Z2_Campesino",
                TRUE ~ ciiu4_3)))

# Ciiu_4
Base_trabajo_f <- Base_trabajo_f %>% 
  mutate(across(ciiu4_4, 
                ~case_when(ciiu4_4 == "Z0_Nocla_CIIU" ~ "Z0_No clasificado_CIIU",
                sector_afiliacion == 3 ~ "Z1_Doméstico",
                sector_afiliacion == 4 ~ "Z3_Semicontribuyente",
                sector_afiliacion == 5 ~ "Z4_Voluntario",
                sector_afiliacion == 6 ~ "Z2_Campesino",
                TRUE ~ ciiu4_4)))

# Tamaño Empleo
Base_trabajo_f <- Base_trabajo_f %>% 
  mutate(across(tamano_empleo,
                ~case_when(sector_afiliacion == 3 ~ "Z1_Doméstico",
                sector_afiliacion == 4 ~ "Z3_Semicontribuyente",
                sector_afiliacion == 5 ~ "Z4_Voluntario",
                sector_afiliacion == 6 ~ "Z2_Campesino",
                tamano_empleo == 1 ~ "1_Microempresa",   
                tamano_empleo == 2 ~ "2_Pequeña",
                tamano_empleo == 3 ~ "3_Mediana A",
                tamano_empleo == 4 ~ "4_Mediana B",
                TRUE ~ "5_Grande")))

# Condición de trabajo
Base_trabajo_f <- Base_trabajo_f %>% 
  mutate(across(agrupa,
                ~case_when(sector_afiliacion == 6 ~ "Z2_Campesino",
                agrupa == 1 ~ "1_Cuenta Propia",
                agrupa == 2 ~ "2_Patronos",
                agrupa == 3 ~ "3_Asalariados",
                TRUE ~ agrupa)))

# Género
Base_trabajo_f <- Base_trabajo_f %>% 
  mutate(across(genero,
                ~case_when(genero == 1 ~ "1_Hombre",
                TRUE ~ "2_Mujer")))

# Rango de edad ENEMDU
Base_trabajo_f <- Base_trabajo_f %>% 
  mutate(across(edadc,
                ~case_when(edadc == 1 ~ "0_Menor a 15 años",
                edadc == 2 ~ "1_Entre 15 y 24 años",
                edadc == 3 ~ "2_Entre 25 y 34 años",
                edadc == 4 ~ "3_Entre 35 y 44 años",
                edadc == 5 ~ "4_Entre 45 y 64 años",
                TRUE ~ "5_65 años y más")))

# Rango de edad
Base_trabajo_f <- Base_trabajo_f %>% 
  mutate(across(edadcj,
                ~case_when(edadcj == 1 ~ "0_Menor a 15 años",
                edadcj == 2 ~ "1_Entre 15 y 17 años",
                edadcj == 3 ~ "2_Entre 18 y 29 años",
                edadcj == 4 ~ "3_Entre 30 y 44 años",
                edadcj == 5 ~ "4_Entre 45 y 64 años",
                TRUE ~ "5_65 años y más")))

# Sector de afiliacion
Base_trabajo_f <- Base_trabajo_f %>% 
  mutate(across(sector_afiliacion,
                ~case_when(sector_afiliacion == 1 ~ "1_Privado",
                sector_afiliacion == 2 ~ "2_Público",
                sector_afiliacion == 3 ~ "3_Empleo doméstico",
                sector_afiliacion == 6 ~ "4_Seguro Social Campesino",
                sector_afiliacion == 4 ~ "5_Semi contribuyente",
                TRUE ~ "6_Voluntarios")))

#Total
Base_trabajo_f <- Base_trabajo_f %>% mutate(total = "Z6_Total")


###################################
# INDICADORES LABORALES           #
###################################

#############################################################
# 1.1  Características ocupacionales y del empleador - Total#
#############################################################
vars_1 <- c("ciiu4_1", "ciiu4_2", "ciiu4_3", "ciiu4_4","sector_afiliacion", "tamano_empleo", 
            "agrupa","provincia")
# Indicador previo
I_1_1 <- lapply(vars_1, Ind_Lab, y = Base_trabajo_f)
# Filtro (aplicado var agrupa)
a <- Base_trabajo_f %>% filter(!is.na(agrupa))
#Indicador final
k2 <- Ind_Lab(agrupa, y = a)
I_1_1[[7]] <- k2
rm(a, k2)

#######################################################################
# 1.2. Características ocupacionales y del empleador  - Sector Privado#
#######################################################################
vars_1 <- c("ciiu4_1", "ciiu4_4", "tamano_empleo", "agrupa")
# Filtro 
c <- Base_trabajo_f %>% filter(grepl("1_Privado", sector_afiliacion)) 
# Indicador previo
I_1_2 <- lapply(vars_1, Ind_Lab, y =c )
# Filtro (aplicado var agrupa)
c <- c %>% filter(!is.na(agrupa))
# Indicador
k2 <- Ind_Lab(agrupa, y = c) 
I_1_2[[4]] <- k2
rm(k2, c)

######################################################################
# 1.3. Características ocupacionales y del empleador - Sector Público#
######################################################################
vars_1 <- c("ciiu4_1", "ciiu4_4", "tamano_empleo")
# Filtro 
c <- Base_trabajo_f %>% filter(grepl("2_Público", sector_afiliacion))
# Indicadores
I_1_3 <- lapply(vars_1, Ind_Lab, y = c)
rm(c)

################################################################
# 2. Características sociodemográficas - (Total-Empleo del SSC)#
################################################################
vars_1 <- c("genero", "edadc", "edadcj", "provincia")
vars_2 <- c("1_Privado", "2_Público", "3_Empleo doméstico", 
            "4_Seguro Social Campesino")
# Filtro
if(EP==1){
 B_F <- Base_trabajo_f %>% filter(plazas_total <= 4)
} else {
 B_F <- Base_trabajo_f %>% filter(empleo_total <= 4)  
}
# Indicador
I_2_p <- lapply(vars_2[1:2], function(vars_2){
  a <-  B_F %>% filter(grepl(vars_2, sector_afiliacion))
  lapply(vars_1, Ind_Lab,y = a)
})
I_2_p <- unlist(I_2_p, recursive = FALSE)
I_2_p_1 <- lapply(vars_2[3:4], function(vars_2){
  a <-  B_F %>% filter(grepl(vars_2, sector_afiliacion))
  lapply(vars_1[1:3], Ind_Lab,y = a)
})
I_2_p_1 <- unlist(I_2_p_1, recursive = FALSE)

j <- lapply(vars_1, Ind_Lab, y = B_F)
I_2 <- append(j, I_2_p) %>% append(I_2_p_1)
rm(I_2_p, j)

#############################################################
# 3.1. Características ocupacionales y del empleador - Sexo #
#############################################################
vars_1 <- c("ciiu4_1", "ciiu4_4", "agrupa", "edadc", "edadcj") 
# Indicador
I_3_1_p <- lapply(vars_1, Ind_Lab, genero, y = B_F)
# Filtro (aplicado var agrupa)
d <- B_F %>% filter(!is.na(agrupa))
# Indicador final
k1 <- Ind_Lab(y = d, agrupa, genero)
I_3_1_p[[3]] <-k1 
e <- list(Ind_Lab(y = B_F, sector_afiliacion, edadc, genero))
f <- list(Ind_Lab(y = B_F, sector_afiliacion, edadcj, genero))
I_3_1 <- c(I_3_1_p, e, f)
rm(I_3_1_p, d, e, f, k1, B_F)

######################################################################
# 3.2. Características ocupacionales y del empleador - Grupo de Edad #
######################################################################
vars_1 <- c("ciiu4_1", "ciiu4_4", "agrupa")
# Indicador
I_3_2 <- lapply(vars_1, Ind_Lab, edadc, y = Base_trabajo_f)
# Filtro (aplicado var agrupa)
d1 <- Base_trabajo_f %>% filter(!is.na(agrupa))
# Indicador final
k1 <- Ind_Lab(y = d1, agrupa, edadc)
I_3_2[[3]] <- k1 
rm(d1, k1)

#####################################################################################
# 3.3. Características ocupacionales y del empleador - Grupo de Edad - 18 a 29 años #
#####################################################################################
vars_1 <- c("ciiu4_1", "ciiu4_4", "agrupa")
# Indicador
I_3_3 <- lapply(vars_1, Ind_Lab, edadcj, y = Base_trabajo_f)
# Filtro (aplicado var agrupa)
d1 <- Base_trabajo_f %>% filter(!is.na(agrupa))
# Indicador final
k1 <- Ind_Lab(y = d1, agrupa, edadcj)
I_3_3[[3]] <- k1 
rm(d1, k1)

####################################################################
#      4.1. Características Socio-demográficas                     #
####################################################################
vars_1 <- c("genero", "edadc", "edadcj")
# Filtros Base de Trabajo
if(EP == 1){
  c <- Base_trabajo_f %>% filter(plazas_total <= 3)  
}else{
  c <- Base_trabajo_f %>% filter(empleo_total <= 3)  
}

#Indicador
I_4_1 <- lapply(vars_1, Ind_Lab_2, y = c, z = mean)

###############################################################
#    4.2. Características ocupacionales y del empleador       #
###############################################################

# Indicador preliminar
vars_1 <-c("ciiu4_1", "ciiu4_4", "sector_afiliacion")
k1 <- lapply(vars_1, Ind_Lab_2, y = c, z = mean)
# Filtro (aplicado var agrupa)
d <- c %>% filter(!is.na(agrupa))
# Indicador
k2 <- list(Ind_Lab_2(y = d, agrupa, z = mean))
I_4_2 <- c(k1, k2)
rm(k1, k2)

# Sector Inec subtotal privado (doméstico + privado)
d <- c %>% filter(!grepl("2_Público", sector_afiliacion))
I1 <- Ind_Lab_2(y = d, sector_afiliacion, z = mean) %>% 
  filter(grepl("Z6_Total",sector_afiliacion))
I1[1,1] <- "4_Subtotal_Privado(1+3)" 
I_fin <- Reduce(full_join,list(I_4_2[[3]],I1)) %>% 
  arrange(sector_afiliacion)
I_4_2[[3]] <- I_fin

# Desagegación adicional Tamaño de empresa
k2 <- list(Ind_Lab_2(y = c, tamano_empleo, z = mean))
I_4_2 <- c(I_4_2, k2)
rm(k2, d, I1, I_fin)

###############################################################
#               4.3. Otros                                    #
###############################################################
vars_1 <- c("genero", "edadc", "edadcj")
#Indicador
x <- lapply(vars_1, Ind_Lab_2, sector_afiliacion, y = c, z = mean)
I_4_3 <- lapply(x, function(x) x[, c(2, 1, 3:ncol(x))])
rm(x)

###############################################################
#    5.1. Características ocupacionales y del empleador       #
###############################################################
vars_1 <- c("ciiu4_1", "ciiu4_4", "sector_afiliacion")
# Indicador preliminar
k1 <- lapply(vars_1, Ind_Lab_2, y = c, z = sum)
# Filtro (aplicado var agrupa)
d <- c %>% filter(!is.na(agrupa))
#Indicador
k2 <- list(Ind_Lab_2(y = d, agrupa, z = sum))
I_5 <- c(k1, k2)
rm(c, d, k1, k2)

################################################################
#       Indicadores laborales Sección (1-5)                    #
################################################################
Indicadores_lab_Tot <- c(I_1_1, I_1_2, I_1_3, I_2, I_3_1, I_3_2, I_3_3, I_4_1, 
                         I_4_2, I_4_3, I_5)
eval(call("<-", as.name(paste("Base_Ind_lab", Base_trabajo_f$ano[1], 
                        Base_trabajo_f$mes[1], sep = "_")), 
          Indicadores_lab_Tot))
}, future.seed = TRUE)
gc()

#-------------------------------------------------------------------------------
# Indicadores Laborales consolidados (Clasificación y bases de datos)
#-------------------------------------------------------------------------------
## Vectores(posición)
###Número de indicadores que se obtendran
ubi <- c(1:61)

###Vector de ubicación Clasificación x Indicadores (Clas_Ind) 
### El número representa la clasificación (1:27) y la posición indica el indicador (1:61)
Clas_Ind <- c(1:8, 1, 4, 6, 7, 1, 4, 6,                  #Indicadores sección 1
              9:11, 8:11, 8:11, 8:11, 9:11,              #Indicadores sección 2
              12:24,                                     #Indicadores sección 3
              9:11, 1, 4, 5, 7, 6, 25:27, 1, 4, 5, 7)    #Indicadores sección 4

## Clasificaciones 
Clasificacion <- future_lapply(Clas_Ind,function (Clas_Ind){
  #número de hojas electronicas
  f_2 <- 1:11
  #documento xlsx de clasificaciones 
  a <- lapply(f_2,function(f_2) read.xlsx(d_clas, sheet = f_2))
  # clasificaciones compuestas
  #12-16
  f_3 <- c(1,4,7,10,11)
  a2 <- lapply(f_3, function(f_3) merge(a[[f_3]], a[[9]], all=TRUE))
  #17-18
  f_4 <- c(10, 11)
  a3 <- lapply(f_4, function(f_4) Reduce(merge,list(a[[5]], a[[f_4]], a[[9]])))
  #19-21
  f_5 <- c(1,4,7)
  a4 <- lapply(f_5, function(f_5) merge(a[[f_5]], a[[10]], all=TRUE))
  #22-24
  a5 <- lapply(f_5, function(f_5) merge(a[[f_5]], a[[11]], all=TRUE))
  #25-27
  f_6 <- 9:11
  a6 <- lapply(f_6, function(f_6) merge(a[[5]], a[[f_6]], all=TRUE))
  #Clasificaciones consolidado (27 total)
  a <- c(a,a2,a3,a4,a5,a6)
  #obtener las clasificaciones de acuerdo a la ubicación en Clas_Ind (esto ordena en función de Indicadores_lab_Con)
  a[[Clas_Ind]]
}, future.seed = TRUE)

## Indicadores Consolidados (generar 12 meses o serie historica)
Indicadores_lab_Con_prev <- future_lapply(ubi, function(ubi) x <- lapply(Bases_ind_lab,`[[`,ubi),future.seed = TRUE)
rm(Bases_ind_lab)
gc()

##Indicadores Consolidados (unión 12 meses o serie historica)
Indicadores_lab_Con <- future_lapply(Indicadores_lab_Con_prev,function(Indicadores_lab_Con_prev) Reduce(full_join,Indicadores_lab_Con_prev), 
                                     future.seed = TRUE)
rm(Indicadores_lab_Con_prev)
gc()

#-------------------------------------------------------------------------------
# Indicadores Laborales (12 meses - Historico)
#-------------------------------------------------------------------------------
# Consolidación de Indicadores finales (Clasificaciones y Serie histórica)
Indicadores_fin <- future_lapply(ubi,function(ubi,Clasificacion) 
                                      full_join(Clasificacion[[ubi]], Indicadores_lab_Con[[ubi]]),
                                 Clasificacion, future.seed = TRUE)
rm(Clasificacion, Indicadores_lab_Con)
gc()

#-------------------------------------------------------------------------------
# Formato para documento Excel
#-------------------------------------------------------------------------------
##Vectores de posición para subtotales y cambio de nombres
subt <- c(1,2,3,4,6,8,41,42,44,45)
subt_2 <- 47:57 # Para tabulados de salario promedio
subt_3 <- 58:61 # Para tabulados de masa salarial

##Subtotales y orden de la base
Indicadores <- future_lapply(ubi, function(ubi){
  if (ubi %in% subt){
    j <- Indicadores_fin[[ubi]] %>% 
         filter(!grepl("Z3_Semicontribuyente|Z4_Voluntario|Z6_Total", c_across(cols = 1))) %>% 
          summarise(across(starts_with("m-"),~sum(.x, na.rm = TRUE)))
    Indicadores_fin[[ubi]] <- full_join(Indicadores_fin[[ubi]],j) %>% mutate(across(1,~ifelse(is.na(.x),"Z2_Subtotal",.x)))
    Indicadores_fin[[ubi]]
  }
#Indicadores_fin[[ubi]] <- Indicadores_fin[[ubi]] %>% 
# filter(!rowSums(is.na(Indicadores_fin[[ubi]][grepl("^m-|^sueldo-",colnames(Indicadores_fin[[ubi]]))])) == length(bases))
arrange(Indicadores_fin[[ubi]], across(!starts_with(c("m-", "sueldo-"))), .by_group = TRUE)
},future.seed = TRUE)

## Nombres subtotales y encabezados
Indicadores <- future_lapply(ubi, function(ubi) {
  if (ubi %in% subt){
    if(EP==2) {
      Indicadores[[ubi]][which(Indicadores[[ubi]]=="Z2_Subtotal"), 1] <- "Total Empleo registrado"
      Indicadores[[ubi]][nrow(Indicadores[[ubi]]),1] <- "Total afiliados IESS"
    } else {
      Indicadores[[ubi]][which(Indicadores[[ubi]]=="Z2_Subtotal"), 1] <- "Total Plazas de empleo registrado"
      Indicadores[[ubi]][nrow(Indicadores[[ubi]]),1] <- "Total afiliados IESS"
    }
  }
  colnames(Indicadores[[ubi]]) <- gsub("^m-", "", colnames(Indicadores[[ubi]]))
  colnames(Indicadores[[ubi]]) <- gsub("^sueldo-", "", colnames(Indicadores[[ubi]]))
  if (!grepl("ciiu4|provincia_sri|canton_sri|canton|provincia", colnames(Indicadores[[ubi]])[1])) {
    colnames(Indicadores[[ubi]])[1] <- "Desagregaciones"  
  } else {
    colnames(Indicadores[[ubi]])[grepl("ciiu4", colnames(Indicadores[[ubi]]))] <- "Desagregaciones (CIIU Rev. 4)"
    colnames(Indicadores[[ubi]])[grepl("provincia_sri", colnames(Indicadores[[ubi]]))] <- "Desagregaciones (Provincia SRI)"
    colnames(Indicadores[[ubi]])[grepl("provincia", colnames(Indicadores[[ubi]]))] <- "Desagregaciones (Provincia)"
    colnames(Indicadores[[ubi]])[grepl("canton_sri", colnames(Indicadores[[ubi]]))] <- "Desagregaciones (Canton SRI)"
    colnames(Indicadores[[ubi]])[grepl("canton", colnames(Indicadores[[ubi]]))] <- "Desagregaciones (Canton)"
  }
  if(EP==2) {
    if(!(ubi %in% subt)){
      Indicadores[[ubi]][nrow(Indicadores[[ubi]]),1] <- "Total Empleo registrado"  
    }
  } else {
      if(!(ubi %in% subt)){
      Indicadores[[ubi]][nrow(Indicadores[[ubi]]),1] <- "Total Plazas de empleo registrado"      
    }
  }
  if(ubi %in% subt_2) {
    Indicadores[[ubi]][nrow(Indicadores[[ubi]]),1] <- "Total sueldo corriente medio"  
  }
  if(ubi %in% subt_3) {
    Indicadores[[ubi]][nrow(Indicadores[[ubi]]),1] <- "Total masa salarial"  
  }
  Indicadores[[ubi]]
},future.seed = TRUE)

##########################################################################
# Establecer el ambiente para trabajo en secuencia nuevamente (1 núcleo) #
  plan(sequential)                                                       #  
##########################################################################

#========================================================================#
#               GUARDAR LAS SERIES HISTORICAS                            #
##########################################################################
if(Hist_Men == 1){                                                       #
  if(EP == 1){                                                           #
    save(Indicadores, file = "ind_laborales_plazas.rda")                 #
  }                                                                      #
  if(EP == 2){                                                           #
    save(Indicadores, file = "ind_laborales_empleo.rda")                 #
  }                                                                      #
}                                                                        #
#========================================================================#

#===============================================================================
# DOCUMENTO DE EXCEL
#===============================================================================
#-------------------------------------------------------------------------------
# Estilos documento Excel
#-------------------------------------------------------------------------------
# Estilo de Encabezados (Índice e Indicadores)
## Formato hoja Indicadorees 
I_L_H <- createStyle(fontName = "Century Gothic", fontSize = 10, 
                     textDecoration = "Bold", fontColour = "#595959", 
                     border = "TopBottomLeftRight", borderColour = "#A2E8FF",
                     borderStyle = "thin", fgFill = "#A2E8FF", 
                     halign = "center", valign = "center")

## Formato hoja índice tabla 
I_L_I_H <- createStyle(fontName = "Century Gothic", fontSize = 12, 
                       textDecoration = "Bold", fontColour = "#595959", 
                       border = "TopBottomLeftRight", borderColour = "#A2E8FF",
                       borderStyle = "thin", fgFill = "#A2E8FF", 
                       halign = "center", valign = "center")

#===============================================================================
# Índices

I_L_I <- createStyle(fontName = "Century Gothic", fontSize = 12, 
                     fontColour = "#595959", halign = "center", 
                     valign = "center",
                     textDecoration = c("bold", "italic"))
I_L_C_M <- createStyle(fontName = "Century Gothic", fontSize = 11, 
                       fontColour = "#595959", halign = "left", 
                       valign = "center", textDecoration = "bold", 
                       border = "TopBottomLeftRight", borderColour = "#A2E8FF", 
                       borderStyle = "thin")
I_L_C_I <- createStyle(fontName = "Century Gothic", fontSize = 11, 
                       fontColour = "#595959", halign = "left", 
                       valign = "center", border = "TopBottomLeftRight", 
                       borderColour = "#A2E8FF", borderStyle = "thin")
#===============================================================================
# Definiciones
I_L_D_1 <- createStyle(fontName = "Century Gothic", fontSize = 18, 
                       fontColour = "#595959", halign = "left", 
                       valign = "center", textDecoration = "bold")
I_L_D_2 <- createStyle(fontName = "Century Gothic", fontSize = 11, 
                       fontColour = "#595959", halign = "left", 
                       valign = "center", wrapText = TRUE)
I_L_D_3 <- createStyle(fontName = "Century Gothic", fontSize = 12, 
                       fontColour = "#595959", halign = "left", 
                       valign = "center", wrapText = TRUE, 
                       textDecoration = "bold", fgFill = "#A2E8FF")
I_L_D_4 <- createStyle(fontName = "Century Gothic", fontSize = 10, 
                       fontColour = "#595959", halign = "left", 
                       valign = "center", wrapText = TRUE)
I_L_D_5 <- createStyle(fontName = "Century Gothic", fontSize = 12, 
                       fontColour = "#595959", halign = "left", 
                       valign = "center", wrapText = TRUE)
I_L_D_6 <- createStyle(fontName = "Century Gothic", fontSize = 11, 
                       fontColour = "#595959", halign = "left", 
                       valign = "center", wrapText = FALSE)
#===============================================================================
# Indicadores
I_L_F <- createStyle(numFmt = "mmm-yy")
I_L_T <- createStyle(fontName = "Century Gothic", fontSize = 12, 
                     fontColour = "#595959", 
                     halign = "left", valign = "center",
                     textDecoration = c("bold", "italic"))
I_L_P_N <- createStyle(fontName = "Century Gothic", fontSize = 9, 
                       fontColour = "#595959", 
                       halign = "left", valign = "center",
                       textDecoration = "bold")
I_L_P <- createStyle(fontName = "Century Gothic", fontSize = 9, 
                     fontColour = "#595959", 
                     halign = "left", valign = "center")
I_L_I_RI <- createStyle(fontName = "Century Gothic", fontSize = 10, 
                        fontColour = "#595959", halign = "left", 
                        valign = "center", textDecoration = c("bold", "italic"), 
                        border = "TopBottomLeftRight", 
                        borderColour = "#A2E8FF", borderStyle = "thin")
I_L_c_1 <- createStyle(fontName = "Century Gothic", fontSize = 10, 
                       fontColour = "#595959", border = "TopBottomLeftRight", 
                       borderColour = "#A2E8FF", borderStyle = "thin", 
                       halign = "left", valign = "center",wrapText = TRUE)
I_L_c_2 <- createStyle(fontName = "Century Gothic", fontSize = 10, 
                       fontColour = "#595959", border = "TopBottomLeftRight", 
                       borderColour = "#A2E8FF", borderStyle = "thin", 
                       halign = "center", valign = "center", numFmt = "COMMA")

#-------------------------------------------------------------------------------
# Contenido(Indice, Definiciones, Hojas(Indicadores))
#-------------------------------------------------------------------------------
a_e <- read.xlsx(d_clas, sheet = "Indice_E")
a_p <- read.xlsx(d_clas, sheet = "Indice_P")
a1 <- read.xlsx(d_clas, sheet = "Indicadores")
def <- read.xlsx(d_clas, sheet = "Definiciones")
d <- a_e$Contenido[grepl("1|2|3|4|5",a_e$`Cuadro.N°`)]
c <- a_e$`Cuadro.N°`[grepl("1|2|3|4|5",a_e$`Cuadro.N°`)]
#===============================================================================
if (Hist_Men == 1) {
#===============================================================================
#_________________________________________________________________________________
# D.2 Documento Excel (Historico)
#_________________________________________________________________________________
#---------------------------------------------------------------------------------
# Directorio de resultados
#-------------------------------------------------------------------------------
setwd(re_tab)
if (dir.exists(paste(getwd(), tail(colnames(Indicadores[[1]]), n = 1), sep = "/"))) {
  setwd(paste(getwd(), tail(colnames(Indicadores[[1]]), n = 1), sep = "/"))
} else {
  dir.create(paste(getwd(), tail(colnames(Indicadores[[1]]), n = 1), sep = "/"))  
  setwd(paste(getwd(), tail(colnames(Indicadores[[1]]), n = 1), sep = "/"))
}
#-------------------------------------------------------------------------------
# Crear documento
#-------------------------------------------------------------------------------
wb <- createWorkbook()
#-------------------------------------------------------------------------------
# Pag Índices
#-------------------------------------------------------------------------------
# Crear pag 
addWorksheet(wb, sheetName = "Índice", gridLines = FALSE, zoom = 85)

# Imagen
insertImage(wb,"Índice", file = img_banner, width = 55, height = 4.49, 
            units = "cm", startRow = 1, startCol = 1)

# Tamaño y Combinar (Celdas, Columnas)
setColWidths(wb, "Índice", cols = 1:4, width = c(15.43,12.43,172.14,25.29))
setRowHeights(wb, "Índice", rows = 1:4, heights = c(127.5, 9.75, 19.5, 19.5))
setRowHeights(wb, "Índice", rows = 5:91, heights = c(16.5))
x1 <- c(7,8,24,43,57,69)
lapply(x1, function(x1) mergeCells(wb,"Índice", rows = x1, cols = 2:3))

# Contenido
if (EP == 1) {
  writeData(wb, "Índice", "Tabulados: Plazas de empleo registrado en la seguridad social (IESS)", 
            startCol = 3, startRow = 3,)
} else {
  writeData(wb, "Índice", "Tabulados: Empleo registrado en la seguridad social (IESS)", 
            startCol = 3, startRow = 3,)
}

writeData(wb, "Índice", "Instituto Nacional de Estadística y Censos (INEC)",
          startCol = 3, startRow = 4)

if (EP == 1) {
writeData(wb, "Índice", a_p, xy = c(2,6), headerStyle = I_L_I_H,
          borders = "all", borderColour = "#A2E8FF", borderStyle = "thin")
} else {
  writeData(wb, "Índice", a_e, xy = c(2,6), headerStyle = I_L_I_H,
            borders = "all", borderColour = "#A2E8FF", borderStyle = "thin")
}

# Agregar estilos
addStyle(wb, "Índice", I_L_I, row = 3:4, col = 2:3, gridExpand = TRUE)
addStyle(wb, "Índice", I_L_C_M, row = c(7,8,24,43,57,69), 
         col = 2:3, gridExpand = TRUE)

# Hipervinculos con estilos de celdas
n <- c(9:23,25:42,44:56,58:68,70:73)
mapply(function(n,c,d){
  writeFormula(wb, "Índice", xy = c(3,n), 
               x =  makeHyperlinkString(c, row = 6, col = 2, text = d))  
  addStyle(wb, "Índice", I_L_C_I, rows = n, cols = 2:3 )
},n,c,d)

#-------------------------------------------------------------------------------
# Pag Definiciones
#-------------------------------------------------------------------------------
# Crear pag
addWorksheet(wb, sheetName = "Definiciones", gridLines = FALSE, zoom = 75)

# Imagenes
insertImage(wb,"Definiciones", file = img_banner, width = 55, 
            height = 4.49, units = "cm", startRow = 1, startCol = 1)
insertImage(wb,"Definiciones", file = img_empleo_plazas, width = 27.41, 
            height = 6.06, units = "cm", startRow = 10, startCol = 2)

# Tamaño (Celdas, Columnas) y Combinar (Celdas)
setColWidths(wb, "Definiciones", cols = 1:6, widths = 10.86)
setColWidths(wb, "Definiciones", cols = 9:14, widths = 13.71)
setColWidths(wb, "Definiciones", cols = c(7:8,15), widths = c(13.86,3.29,17.14))
setRowHeights(wb, "Definiciones", rows = c(5,54), heights = 15.75)
setRowHeights(wb, "Definiciones", rows = c(6,56), heights = 49.5)
setRowHeights(wb, "Definiciones", rows = c(11,13,16,30:50), heights = 16.5)
setRowHeights(wb, "Definiciones", rows = 23:22, heights = 19.5)
setRowHeights(wb, "Definiciones", rows = c(1,2:4,7:8,12,14:15,17,19:22,25,26,28,29,53,55), 
              heights = c(127.5,22.5,16.5,37.5,54.75,15.75,37.5,38.25,51.75,24.75,18,18,
                          18,20.25,26.25,17.25,34.5,18.75,33.75,33))

# Contenido
defcon <- data.frame(a =c(2:8,17:28,52:56),b=def)
mapply(function(x,y) writeData(wb,"Definiciones",x,startCol = 1, startRow = y),
       x=defcon$Definiciones,y=defcon$a)
writeData(wb,"Definiciones",Indicadores[[1]][1:21,1],startCol = 1,startRow = 30,colNames = FALSE)
writeData(wb,"Definiciones",Indicadores[[1]][1:21,2],startCol = 2,startRow = 30,colNames = FALSE)

# Unir celdas y Paneles
filas <- c(3:8,17:28,52:56)
lapply(filas,function(filas) mergeCells(wb,"Definiciones", cols = 1:15, rows = filas))
freezePane(wb, "Definiciones", firstActiveRow = 5, firstActiveCol = 1)

# Agregar estilos
addStyle(wb, "Definiciones", I_L_D_1, 2, 1, gridExpand = TRUE)
addStyle(wb, "Definiciones", I_L_D_2, c(3:4,17,19:22,28,30:50,53,55:56), 1,
         gridExpand = TRUE)
addStyle(wb, "Definiciones", I_L_D_6, 30:50, 1:2, gridExpand = TRUE)
addStyle(wb, "Definiciones", I_L_D_3, c(5,8,18,23,24,27,52,54), 1, 
         gridExpand = TRUE)
addStyle(wb, "Definiciones", I_L_D_4, 17, 1, gridExpand = TRUE)
addStyle(wb, "Definiciones", I_L_D_5, c(6,7,25,26), 1, gridExpand = TRUE)

#-------------------------------------------------------------------------------
# Pag Indicadores 
#-------------------------------------------------------------------------------
# Vectores de referencia (Hojas Electronicas)

pag_ind_1 <- c(1:4, 8:10, 13, 14, 19, 23, 27, 50, 51, 58, 59)
pag_ind_2<- c(5:7, 11, 12, 15:18, 20:22, 24:26, 28:33, 47:49, 52:54, 60:61)
pag_ind_3 <- c(36:38, 43, 46, 55, 56, 57)
pag_ind_4 <- c(34, 35, 41, 42, 44, 45)
pag_ind_5 <- c(39,40)
pag_sub1 <- c(1:4,8)
pag_sub2 <- c(6) 
pag_sub4 <- c(41,42,44,45) 
pag_ver1 <- c(34:35)
pag_ver2 <- c(36:38, 55)
pag_ver3 <- c(41,42,44,45)
pag_ver4 <- c(43,46,56,57)
pag_ver5 <- c(39,40)
pag_hor1 <- c(34:35)
pag_hor2 <- c(41,42,44,45)
b <- 1:61

# Contenido (Image e hypervinculo)
mapply(function(c,n){ 
  addWorksheet(wb, sheet = c, gridLines = FALSE, zoom = 80)
  setRowHeights(wb, c, rows = c(1,3,6), heights = c(128,14,30))
  insertImage(wb,c,file = img_banner, width = 55, height = 4.49, 
              units = "cm",startRow = 1, startCol = 1)
  writeFormula(wb, c, xy = c(1,2), 
               makeHyperlinkString("Índice", row = n, col = 3, text = "Índice"))
},c,n)

# Contenido (Títulos)
mapply(function(c,b){ 
  writeData(wb, c, paste("CUADRO","N°", b, sep = " "), startCol = 2,startRow = 3)
  writeData(wb, c, paste(toupper(d[b]), paste("2009", format(Sys.Date(),"%Y"),
                                              sep =" - " ), sep = ", "), startCol = 2,startRow = 4)
},c,b)

# Contenido (Tablas y Notas al Pie) y Tamaños (Columnas y filas)
mapply(function(c,Indicadores) {
  writeData(wb, c, Indicadores, startCol = 2,startRow = 6, headerStyle = I_L_H ,
            borders = "all",borderColour = "#A2E8FF", borderStyle = "thin")
  setRowHeights(wb, c, rows = 7:(5+nrow(Indicadores)),heights = 26)
  setRowHeights(wb, c, rows = (6+nrow(Indicadores)),heights = 28)
  addStyle(wb, c, style = I_L_F, rows = 6, cols = 2:(1+ncol(Indicadores)),
           gridExpand = TRUE, stack = TRUE)
  addStyle(wb, c, style = I_L_T, rows = 3:4, cols = 2, 
           gridExpand = TRUE, stack = TRUE)
  addStyle(wb, c, style = I_L_P_N,
           rows = (8+nrow(Indicadores)):(9+nrow(Indicadores)), 
           cols = 2, gridExpand = TRUE)
  addStyle(wb, c, style = I_L_P_N, rows = 2, cols = 1, 
           gridExpand = TRUE, stack = TRUE)
  addStyle(wb, c, style = I_L_P, 
           rows = (10+nrow(Indicadores)):(13+nrow(Indicadores)), 
           cols = 2, gridExpand = TRUE,stack = TRUE)
  addStyle(wb, c, style = I_L_I_RI, rows = (6+nrow(Indicadores)), 
           cols = 2:(1+ncol(Indicadores)), gridExpand = TRUE, stack = TRUE)
  writeData(wb, c, {{a1}}, startCol = 2,
            startRow = (nrow(Indicadores)+8), colNames = FALSE)
},c,Indicadores)

# Formatos de hojas electronicas 
mapply(function(c,Indicadores,b){ 
  
  # FORMATOS HOJAS ELECTRONICAS 
  if (b %in% pag_ind_1){
    # Tamaño Columnas  
    setColWidths(wb, c, cols = c(2,3), widths = c(9,88))
    setColWidths(wb, c, cols = 4:(1+ncol(Indicadores)), widths = 13)
    # Estilos
    addStyle(wb, c, style = I_L_c_1, rows = 7:(5+nrow(Indicadores)), 
             cols = 2:3, gridExpand = TRUE, stack = TRUE)
    if(b %in% pag_sub1){
      # Estilo Subtotal 1
      addStyle(wb, c, style = I_L_I_RI, rows = (3+nrow(Indicadores)), 
               cols = 2:(1+ncol(Indicadores)), 
               gridExpand = TRUE,stack = TRUE)
    }
    addStyle(wb, c, style = I_L_c_2, rows = 7:(6+nrow(Indicadores)), 
             cols = 4:(1+ncol(Indicadores)), gridExpand = TRUE, stack = TRUE)
    # Unir Celdas e Inmovilizar paneles
    if(b %in% pag_sub1) {
      uc <- 1:6
      lapply(uc,function(uc) mergeCells(wb, c, cols = 2:3,
                                        rows = (6-uc+nrow(Indicadores))))
      mergeCells(wb, c, cols = 2:3, rows = (6+nrow(Indicadores)))
      mergeCells(wb, c, cols = 2:3, rows = 6) 
      freezePane(wb, c, firstActiveRow = 7, firstActiveCol = 4)
    } else{
      uc <- 1:5
      lapply(uc,function(uc) mergeCells(wb, c, cols = 2:3, rows = (6-uc+nrow(Indicadores))))
      mergeCells(wb, c, cols = 2:3, rows = (6+nrow(Indicadores)))
      mergeCells(wb, c, cols = 2:3, rows = 6) 
      freezePane(wb, c, firstActiveRow = 7, firstActiveCol = 4)
    }
  }
  if (b %in% pag_ind_2){
    # Tamaño Columnas  
    setColWidths(wb, c, cols = 2, widths = 51)
    setColWidths(wb, c, cols = 3:(1+ncol(Indicadores)), widths = 13)
    # Estilos 
    addStyle(wb,c, style = I_L_c_1, rows = 7:(5+nrow(Indicadores)), cols = 2, 
             gridExpand = TRUE,stack = TRUE)
    addStyle(wb, c, style = I_L_c_2,rows = 7:(6+nrow(Indicadores)), 
             cols = 3:(1+ncol(Indicadores)), 
             gridExpand = TRUE, stack = TRUE)
    # Inmovilizar paneles
    freezePane(wb, c, firstActiveRow = 7, firstActiveCol = 3)
    # Subtotal 2
    if (b %in% pag_sub2){  
      addStyle(wb, c, style = I_L_I_RI, rows = (3+nrow(Indicadores)), 
               cols = 2:(1+ncol(Indicadores)), 
               gridExpand = TRUE,stack = TRUE)
      addStyle(wb, c, style = I_L_c_2, rows = 7:(6+nrow(Indicadores)), 
               cols = 3:(1+ncol(Indicadores)), gridExpand = TRUE, stack = TRUE)  
    }
  }
  if (b %in% pag_ind_3){  
    # Tamaño Columnas
    setColWidths(wb,c,cols = 2:3,widths = 28)  
    setColWidths(wb,c,cols = 4:(1+ncol(Indicadores)),widths = 13)
    # Estilos 
    addStyle(wb, c, style = I_L_c_1, rows = 7:(5+nrow(Indicadores)), 
             cols = 2:3, gridExpand = TRUE)
    addStyle(wb, c, style = I_L_c_2, rows = 7:(6+nrow(Indicadores)), 
             cols = 4:(1+ncol(Indicadores)), 
             gridExpand = TRUE, stack = TRUE)
    # Unir Celdas
    mergeCells(wb, c, cols = 2:3, rows = (6+nrow(Indicadores)))
    mergeCells(wb, c, cols = 2:3, rows = 6)
    # Inmovilizar paneles
    freezePane(wb, c, firstActiveRow = 7, firstActiveCol = 4)
  }
  if (b %in% pag_ind_4){  
    # Tamaño Columnas
    setColWidths(wb,c, cols = 2:4, widths = c(7,88,28))
    setColWidths(wb,c, cols = 5:(1+ncol(Indicadores)), widths = 13)
    # Estilos  
    addStyle(wb, c, style = I_L_c_1, rows = 7:(5+nrow(Indicadores)), 
             cols = 2:4, gridExpand = TRUE)
    if (b %in% pag_sub4){ 
      addStyle(wb, c, style = I_L_I_RI, rows = (nrow(Indicadores)-7), 
               cols = 2:(1+ncol(Indicadores)), 
               gridExpand = TRUE,stack = TRUE)
    } 
    addStyle(wb,c, style = I_L_c_2, rows = 7:(6+nrow(Indicadores)), 
             cols = 5:(1+ncol(Indicadores)), gridExpand = TRUE, stack = TRUE)
    # Unir Celdas
    mergeCells(wb,c , cols = 2:4, rows = (6+nrow(Indicadores)))
    mergeCells(wb, c, cols = 2:4, rows = 6) 
    # Inmovilizar paneles
    freezePane(wb, c, firstActiveRow = 7, firstActiveCol = 5)
  }

  if (b %in% pag_ind_5){  
    # Tamaño Columnas
    setColWidths(wb, c, cols = 2:4, widths = c(28,28,11))
    setColWidths(wb,c, cols = 5:(1+ncol(Indicadores)), widths = 13)
    # Estilos
    addStyle(wb, c, style = I_L_c_1, rows = 7:(5+nrow(Indicadores)), 
             cols = 2:4, gridExpand = TRUE)
    addStyle(wb, c, style = I_L_c_2, rows = 7:(6+nrow(Indicadores)), 
             cols = 5:(1+ncol(Indicadores)), gridExpand = TRUE, stack = TRUE)
    # Unir Celdas
    mergeCells(wb, c, cols = 2:4, rows = (6+nrow(Indicadores)))
    mergeCells(wb, c, cols = 2:4, rows = 6)  
    # Inmovilizar paneles
    freezePane(wb, c, firstActiveRow = 7, firstActiveCol = 5)
  }

  # UNIONES VERTICALES Y FORMATO SUBTOTAL
  if (b %in% pag_ver1){ 
    # Uniones celdas (Verticales) #1  
    odd <- seq(7, nrow(Indicadores)+5,by=2)
    even <- seq(8, nrow(Indicadores)+5,by=2)
    mapply(function(odd,even) mergeCells(wb,c, cols = 2, rows = c(odd,even)), 
           odd, even)
    mapply(function(odd,even) mergeCells(wb,c, cols = 3, rows = c(odd,even)), 
           odd, even)
  }
  if (b %in% pag_ver2){ 
    # Uniones celdas (Verticales) #2 
    odd <- seq(7,(nrow(Indicadores)+5),by=2)
    even <- seq(8,(nrow(Indicadores)+5),by=2)
    mapply(function(odd,even) mergeCells(wb,c, cols = 2, rows = c(odd,even)),
           odd,even)
  }
  if (b %in% pag_ver3){ 
    # Uniones celdas (Verticales) #3 
    odd <- seq(7,(nrow(Indicadores)-13),by=6)
    even <- seq(12,(nrow(Indicadores)-8),by=6)
    mapply(function(odd,even) {
      mergeCells(wb,c, cols = 2, rows = c(odd,even))
      mergeCells(wb,c, cols = 3, rows = c(odd,even))
    },odd,even)
    odd <- seq((nrow(Indicadores)-1),(nrow(Indicadores)+5),by=6)
    even <- seq((nrow(Indicadores)-6),(nrow(Indicadores)+4),by=6)
    mapply(function(odd,even) {
      mergeCells(wb,c, cols = 2, rows = c(even,odd))
      mergeCells(wb,c, cols = 3, rows = c(even,odd))
    },even,odd)
  }
  if (b %in% pag_ver4){ 
    # Uniones celdas (Verticales) #4 
    odd <- seq(7,(nrow(Indicadores)+5),by=6)
    even <- seq(12,(nrow(Indicadores)+5),by=6)
    mapply(function(odd,even) mergeCells(wb,c, cols = 2, rows = c(odd,even)),
           odd,even)
  }

  if (b %in% pag_ver5){
    # Uniones celdas (Verticales) #5
    odd <- seq(7,(nrow(Indicadores)+5),by=12)
    even <- seq(18,(nrow(Indicadores)+5),by=12)
    odd_2 <- seq(7,(nrow(Indicadores)+5),by=2)
    even_2 <- seq(8,(nrow(Indicadores)+5),by=2)
    mapply(function(odd,even) mergeCells(wb,c, cols = 2, rows = c(odd,even)),
           odd,even)
    mapply(function(odd_2,even_2) mergeCells(wb,c, cols = 3, rows = c(odd_2,even_2)),
           odd_2,even_2)
  }
  
  #UNIONES HORIZONTALES
  if (b %in% pag_hor1){ 
    # Uniones celdas (Verticales) #1  
    odd <- seq((nrow(Indicadores)-4),(nrow(Indicadores)+4),by=2)
    even <- seq((nrow(Indicadores)-3),nrow(Indicadores)+6,by=2)
    mapply(function(odd,even) {
      removeCellMerge(wb,c,cols = 2:3,rows = c(odd,even))
      mergeCells(wb, c, cols = 2:3,rows = c(odd,even))
    },odd,even)  
    #    removeCellMerge(wb,c,cols = 2:4,rows = (nrow(Indicadores[[46]])+1))
    #    mergeCells(wb,c,cols = 2:4, rows = (nrow(Indicadores[[46]])+1))  
  }
  
  # Uniones celdas (Verticales) #2      
  if (b %in% pag_hor2){ 
    odd <- seq((nrow(Indicadores)-25),(nrow(Indicadores)-13),by=6)
    even <- seq((nrow(Indicadores)-20),nrow(Indicadores)-8,by=6)
    mapply(function(odd,even) {
      removeCellMerge(wb,c,cols = 2:3,rows = c(odd,even))
      mergeCells(wb,c,cols = 2:3,rows = c(odd,even))
    },odd,even)
    odd <- seq((nrow(Indicadores)-6),nrow(Indicadores),by=6)
    even <- seq((nrow(Indicadores)-1),(nrow(Indicadores)+5),by=6)
    mapply(function(even, odd) {
      removeCellMerge(wb,c,cols = 2:3,rows = c(even,odd))
      mergeCells(wb, c, cols = 2:3, rows = c(even,odd))
    },even,odd)  
    removeCellMerge(wb,c,cols = 2:4,rows = (nrow(Indicadores)-7))
    mergeCells(wb,c,cols = 2:4, rows = (nrow(Indicadores)-7))  
  }
},c,Indicadores,b)

rm(list=ls(pattern = "pag_"))
#-------------------------------------------------------------------------------
# Guardar documento
#-------------------------------------------------------------------------------
if (EP == 1){
  saveWorkbook(wb, paste0(paste("Indicadores Laborales", "Plazas", 
               format(as.Date(paste0("01-",tail(colnames(Indicadores[[1]]),n=1)),
               format="%d-%b-%y"),"%m_%y"), sep = "_"),".xlsx"), 
               overwrite = TRUE)
}else{
  saveWorkbook(wb, paste0(paste("Indicadores Laborales", "Empleo", 
               format(as.Date(paste0("01-",tail(colnames(Indicadores[[1]]),n=1)),
               format="%d-%b-%y"),"%m_%y"), sep = "_"),".xlsx"), 
               overwrite = TRUE)
}
#===============================================================================
} else {
#===============================================================================
#_______________________________________________________________________________
# D.2 Documento Excel (12 meses)
#_______________________________________________________________________________
#-------------------------------------------------------------------------------
# Directorio de resultados mes previo
#-------------------------------------------------------------------------------
  setwd(re_tab)
  setwd(paste(getwd(), colnames(Indicadores[[1]])[ncol(Indicadores[[1]])-1], sep = "/"))
#-------------------------------------------------------------------------------
# Cargar documento mes previo y crear vector de referencias (Columnas Excel)
#-------------------------------------------------------------------------------
  if (EP == 1){
    lb <- loadWorkbook(paste(getwd(),list.files(getwd(),
                                                pattern = "Indicadores Laborales_Plazas_"),sep="/"))
  } else {  
    lb <- loadWorkbook(paste(getwd(),list.files(getwd(),
                                                pattern = "Indicadores Laborales_Empleo_"),sep="/"))
  }
  Fechas<- data.frame(format(seq.Date(as.Date("2009/01/01"),
                                      as.Date(paste0("01-",tail(colnames(Indicadores[[1]]),n=1)),
                                              format="%d-%b-%y"), by = "month"),"%m_%Y"))
  Fechas <- Fechas %>% mutate(Colum=seq(1,nrow(Fechas)))    
  
#-------------------------------------------------------------------------------
# Directorio de Resultados
#-------------------------------------------------------------------------------
  setwd(re_tab)
  if (dir.exists(paste(getwd(),tail(colnames(Indicadores[[1]]),n=1), sep = "/"))) {
    setwd(paste(getwd(),tail(colnames(Indicadores[[1]]),n=1), sep = "/"))
  } else { 
    dir.create(paste(getwd(),tail(colnames(Indicadores[[1]]),n=1), sep = "/"))  
    setwd(paste(getwd(),tail(colnames(Indicadores[[1]]),n=1), sep = "/"))
  }
#-------------------------------------------------------------------------------
# Pag Indicadores
#-------------------------------------------------------------------------------
  pg_ind_1 <- c(1:4, 8:10, 13, 14, 19, 23, 27, 36:38, 43, 46, 50, 51, 55: 59)
  
  pg_ind_2  <- c(5:7, 11, 12, 15:18, 20:22, 24:26, 28:33, 47:49, 52:54, 60:61)
  pg_ind_4  <- c(46, 47,51,52,53,54,56,57)
  pag_sub1 <- c(1:4,8)
  pag_sub2 <- c(6) 
  pag_sub4 <- c(41,42,44,45) 
  b <- 1:61
  
  # Filtrar tablas de indicadores
  B1 <- lapply(Indicadores,function(Indicadores) Indicadores[tail(colnames(Indicadores),n=12)])
  #Formato Indicafores
  mapply(function(b,c,B1){
    if (b %in% pg_ind_1) {
      # Borrar Contenido previo
      deleteData(lb, c, cols = (tail(Fechas$Col,n=11)+2), rows = 6:(6+nrow(B1)), 
                 gridExpand = TRUE) 
      # Agregar nuevo contenido (12 meses)
      writeData(lb, c, B1, startCol = (nrow(Fechas)-8), startRow = 6, 
                headerStyle = I_L_H , borders = "all", 
                borderColour = "#A2E8FF", borderStyle = "thin") 
      #Incluir Estilo
      addStyle(lb, c, style = I_L_I_RI, rows = 6+nrow(B1), 
               cols = (tail(Fechas$Col,n=12)+3), 
               gridExpand = TRUE,stack = TRUE)
      addStyle(lb, c, style = I_L_c_2, rows = 7:(6+nrow(B1)), 
               cols = (tail(Fechas$Col,n=12)+3), 
               gridExpand = TRUE, stack = TRUE)
      if(b %in% pag_sub1){
        #Estilo subtotal
        addStyle(lb, c, style = I_L_I_RI, rows = (3+nrow(B1)), cols = (tail(Fechas$Col,n=12)+3), 
                 gridExpand = TRUE,stack = TRUE)
        addStyle(lb, c, style = I_L_c_2, rows = 7:(6+nrow(B1)), cols = (tail(Fechas$Col,n=12)+3), 
                 gridExpand = TRUE, stack = TRUE)
      }
    }
    if (b %in% pg_ind_2) {
      # Borrar Contenido previo
      deleteData(lb, c, cols = (tail(Fechas$Col,n=11)+1), rows = 6:(6+nrow(B1)), 
                 gridExpand = TRUE) 
      # Agregar nuevo contenido (12 meses)
      writeData(lb, c, B1, startCol = (nrow(Fechas)-9), startRow = 6, 
                headerStyle = I_L_H , borders = "all", 
                borderColour = "#A2E8FF", borderStyle = "thin") 
      #Incluir Estilo
      addStyle(lb, c, style = I_L_I_RI, rows = 6+nrow(B1), 
               cols = (tail(Fechas$Col,n=12)+2), 
               gridExpand = TRUE,stack = TRUE)
      addStyle(lb, c, style = I_L_c_2, rows = 7:(6+nrow(B1)), 
               cols = (tail(Fechas$Col,n=12)+2), 
               gridExpand = TRUE, stack = TRUE)
      
      if(b %in% pag_sub2){
        #Estilo subtotal
        addStyle(lb, c, style = I_L_I_RI, rows = (3+nrow(B1)), 
                 cols = (tail(Fechas$Col,n=12)+2), 
                 gridExpand = TRUE,stack = TRUE)
        addStyle(lb, c, style = I_L_c_2, rows = 7:(6+nrow(B1)), 
                 cols = (tail(Fechas$Col,n=12)+2), 
                 gridExpand = TRUE, stack = TRUE)
      } 
    }
    if (b %in% pg_ind_4) {
      # Borrar Contenido previo
      deleteData(lb, c, cols = (tail(Fechas$Col,n=11)+3), rows = 6:(6+nrow(B1)), 
                 gridExpand = TRUE) 
      # Agregar nuevo contenido (12 meses)
      writeData(lb, c, B1, startCol = (nrow(Fechas)-7), startRow = 6, 
                headerStyle = I_L_H , borders = "all", 
                borderColour = "#A2E8FF", borderStyle = "thin") 
      #Incluir Estilo
      addStyle(lb, c, style = I_L_I_RI, rows = 6+nrow(B1), 
               cols = (tail(Fechas$Col,n=12)+4), 
               gridExpand = TRUE,stack = TRUE)
      addStyle(lb, c, style = I_L_c_2, rows = 7:(6+nrow(B1)), 
               cols = (tail(Fechas$Col,n=12)+4), 
               gridExpand = TRUE, stack = TRUE)
      if(b %in% pag_sub4){
        #Estilo subtotal
        addStyle(lb, c, style = I_L_I_RI, rows = (6+nrow(B1))-13,  
                 cols = (tail(Fechas$Col,n=12)+4), 
                 gridExpand = TRUE,stack = TRUE)
        addStyle(lb, c, style = I_L_c_2, rows = 7:(6+nrow(B1)), 
                 cols = (tail(Fechas$Col,n=12)+4), 
                 gridExpand = TRUE, stack = TRUE)
      }
    }
  },b,c,B1)
#-------------------------------------------------------------------------------
# Guardar documento
#-------------------------------------------------------------------------------
  if (EP == 1){
    saveWorkbook(lb, paste0(paste("Indicadores Laborales", "Plazas", tail(Fechas[1],n=1), sep = "_"), ".xlsx"), overwrite = TRUE)
  }else{
    saveWorkbook(lb, paste0(paste("Indicadores Laborales", "Empleo", tail(Fechas[1],n=1), sep = "_"), ".xlsx"), overwrite = TRUE)
  }
}
######
T2 <- Sys.time()
T2-T1
