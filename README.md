# Trabajo Analisis de Tweets Paralelo
El archivo se probo en la pagina de Play With Docker con la imagen proporcionada y todas las funcionalidades sirven correctamente.
## Notas importantes:
- Para el parametro de directorio de datos -d se necesita pasar la dirección de la carpeta en donde se encuentran todos los archivos de datos, el formato de la carpeta y sus sub-carpetas no es relevante para el programa, siempre y cuando todos los archivos en esta sean **.json.bz2**.

- Para el parametro de filtro por hashtags -h se necesita pasar la dirección de un archivo **.txt** que cuente con cada hashtag, uno en cada linea. El programa interpreta los hashtags con y sin #, por ejemplo, **#funny** y **funny** seran tratados por el programa de la misma manera.

- Los filtros por fecha son inclusivos **[fi, ff]**, y necesitan seguir el formato **dd-mm-aaaa** para que el programa los maneje correctamente.

- Para visualizar en **Gephi** los grafos .gexf generados por el programa es necesario refrescar el area de previsualización despues de importar el archivo.
## Integrantes:
- David Henriquez
- Natalia Martinez
- Julian Almario
