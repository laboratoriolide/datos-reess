# Registro Estadístico de Empleo en la Seguridad Social (REESS)

Este repositorio contiene las bases de datos del REESS, obtenido de los datos del Instituto Nacional de Estadística y Censos del Ecuador [aqui](https://www.ecuadorencifras.gob.ec/registro-empleo-seguridad-social/). El repositorio utiliza Git Large File Storage (LFS) para poder mantener los archivos en la nube.

## Contenido del repositorio

- `data`: Bases de datos del REESS en formato `.csv`. Cada archivo contiene los datos de un período año-mes en específico a partir de enero del 2009 (`2009-01`). 
- `code`: Código para limpieza y generación de reportes simples de los datos.

## Instrucciones para descarga

Es posible descargar estos datos directamente del repositorio descargando un archivo comprimido. Sin embargo, se recomienda utilizar un script de descarga o clonación del repositorio mediante un cliente de Git, como GitHub Desktop. Para ello, se necesita instalar el [cliente de Git](https://desktop.github.com/) y también [Git LFS](https://git-lfs.com/). 

Este repositorio es también clonable como un proyecto de R (`reess.Rproj`). Esto facilita el análisis de los datos mediante R desde la plataforma RStudio. Para ello, se necesita instalar [R](https://www.r-project.org/) y [RStudio](https://rstudio.com/).

## Descargo de responsabilidad

El Laboratorio de Investigación para Desarrollo del Ecuador (LIDE) proporciona este repositorio como un recurso para acceder a los datos del REESS para Ecuador. Sin embargo, es importante tener en cuenta que el LIDE no es dueño de los datos del INEC y no tiene control sobre su contenido, calidad o actualización.

Los datos del Barómetro de las Américas utilizados en este repositorio son de libre acceso y se mantienen de forma oficial por el INEC. Para obtener información oficial sobre los datos, su utilización y cualquier restricción de acceso, se recomienda visitar el sitio web del INEC [aquí](https://www.ecuadorencifras.gob.ec/registro-empleo-seguridad-social/).

El LIDE no asume ninguna responsabilidad por el uso que se haga de los datos ni por los resultados obtenidos a partir de su análisis. Se recomienda a los usuarios revisar y seguir los términos y condiciones establecidos por el INEC para el uso adecuado de los datos.

## Referencias

INEC. https://www.ecuadorencifras.gob.ec/registro-empleo-seguridad-social/.

