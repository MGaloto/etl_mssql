
# etl_mssql


### Contenido:
<br>
</br>

- [**Introduccion**](https://github.com/MGaloto/etl_mssql#introduccion)
- [**Ejecucion**](https://github.com/MGaloto/etl_mssql#ejecucion)


## Introduccion


<div style="text-align: right" class="toc-box">
 <a href="#top">Volver al Inicio</a>
</div>

<br>
</br>


El trabajo consiste en restaurar una base de datos que contiene registros duplicados para luego poder programar un ETL que se ejecute una vez por semana. 

La restauracion se hace desde SQL Server y la eliminacion de duplicados mas el alter table para que no se puedan insertar duplicados se hace desde el script configdb.py

En primer lugar hay que setear las variables de entorno en el .env file:

SERVER=SERVER
DB=DB

Luego creal el virutal env e instalar las librerias:


``` shell
python -m venv <nombre del entorno>
```

``` shell
pip install -r requirements.txt
```

### Estructura del Repositorio

``` shell
.
├── backup
├── requirements.txt
├── configdb.py
└── etl.py
```


- `backup` contiene el archivo .bak
- `requirements.txt` librerias a instalar
- `configdb.py` python script.
- `etl.py` pythoon script.


## Ejecucion


<div style="text-align: right" class="toc-box">
 <a href="#top">Volver al Inicio</a>
</div>

<br>
</br>

Los scripts se ejecutan en este orden:

Eliminacion de duplicados + Alter Table  + conteo de registros inicial y final.

``` shell
python configdb.py
```

ETL + conteo de registros inicial y final.

``` shell
python etl.py
```

