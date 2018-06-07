# Base de Datos I

## Trabajo Práctico Especial

## 1 er Cuatrimestre 201 8

## 1. Objetivo

El objetivo de este Trabajo Práctico Especial es aplicar los conceptos de SQL Avanzado
(PSM, Triggers) vistos a lo largo del curso, para implementar funcionalidades no disponibles de
forma estándar (que no pueden resolverse con Primary Keys, Foreign Key, etc)

## 2. Descripción del Trabajo

El sitio Buenos Aires Data, https://data.buenosaires.gob.ar/, ofrece datasets de
información abierta del Gobierno de la Ciudad de Buenos Aires. Entre otros datasets, se
encuentra el que contiene información sobre los recorridos de bicicletas públicas para todos los
años a partir del 2010. Estos datos se encuentran en formato .csv (comma separated values) y
para este trabajo utilizaremos los datos correspondientes al año 201 6. Hay un archivo .csv para
cada año.
Es posible que el formato varíe ligeramente año a año, pero en líneas generales, los
archivos csv contienen las siguientes columnas:

La información detallada de cada columna se encuentra junto con los archivos de datos
en:
https://data.buenosaires.gob.ar/layout/H1giQXf7kx/preview

Se busca en este trabajo práctico especial utilizar los datos del archivo csv para poblar
la tabla RECORRIDO_FINAL que tiene la siguiente estructura:

CREATE TABLE recorrido_final
(periodo TEXT,
usuario INTEGER,
fecha_hora_ret TIMESTAMP NOT NULL,
est_origen INTEGER NOT NULL,
est_destino INTEGER NOT NULL,
fecha_hora_dev TIMESTAMP NOT NULL CHECK(fecha_hora_dev >=
fecha_hora_ret),
PRIMARY KEY(usuario,fecha_hora_ret));


La siguiente es la correspondencia entre los campos en el csv y los de la tabla RECORRIDO_FINAL

### CSV RECORRIDO_FINAL

```
periodo periodo
id_usuario usuario
fecha_hora_retiro fecha_hora_ret
origen_estacion est_origen
destino_estacion est_destino
fecha_hora_retiro + tiempo_uso fecha_hora_dev
```
Analizando los datos del csv se han detectado dos problemas que pueden surgir en la
migración:
1) Podrían contener valor NULL los campos: _usuario, fecha_hora_retiro, est_origen,
destino_estacion, tiempo_uso._ Este último además podría no representar un tiempo
o su valor ser < 0.
2) Los campos _id_usuario+fecha_hora_retiro_ pueden estar repetidos
3) Para un mismo usuario, pueden existir intervalos solapados de uso de las bicicletas.

```
Por ejemplo, en los datos del recorrido de 2016 aparece el usuario 74710 con los
siguientes datos:
```
Aquí se ve que la _fecha_hora_retiro_ de la primera fila + el _tiempo_uso_ que tuvo prestada
la bicicleta, 13/08/2016 13:09 + 18min9seg=13/08/2016 13:27:09 es mayor que la
_fecha_hora_retiro_ en la fila siguiente: 13/08/2016 13:22.

Se quiere realizar la migración de los datos del csv a la tabla RECORRIDO_FINAL
resolviendo los problemas 1), 2) y 3) de la siguiente manera:

```
1) Las filas en el csv con este tipo de problema, deben descartarse, es decir, no se
migran a la tabla RECORRIDO_FINAL.
2) Dado que en RECORRIDO_FINAL usuario+fecha_hora_ ret son clave, en caso de que
detecten en el csv varios registros que coincidan en el id del usuario y la
fecha_hora_retiro , se deben ordenar los datos por el atributo tiempo_uso y migrar
a la tabla RECORRIDO_FINAL la segunda tupla de acuerdo con dicho orden. El resto
de las filas del mismo usuario y fecha_hora_retiro se descartan, es decir, no pasan
a la tabla RECORRIDO_FINAL.
3) Si para un usuario se detectan intervalos solapados encadenados, se debe migrar a
la tabla RECORRIDO_FINAL, una tupla que contenga el ISUM de dichos intervalos.
La estación origen debe coincidir con el intervalo de menor fecha_hora_retiro y la
estación destino debe coincidir con la estación del intervalo con mayor
fecha_hora_retiro.
```
Además, una vez que se han migrado los datos exitosamente, se pretende garantizar que estos
errores no vuelvan a producirse al ingresar nuevos datos a RECORRIDO_FINAL.


**_3. Procedimiento_**

```
a) Se deben crear la función Postgresql migración() que realice la migración desde el
csv a la tabla RECORRIDO_FINAL resolviendo los problemas 1, 2 y 3 de la manera
que se explica en el ítem anterior. Dicha función no debe retornar nada.
Los alumnos pueden crear las tablas temporarias y las funciones auxiliares que
consideren necesarias para lograr el objetivo solicitado.
```
## b) Y para garantizar que, para un usuario dado, no se vuelvan a introducir intervalos

```
solapados en la tabla RECORRIDO_FINAL, se debe crear el trigger
detecta_solapado que al intentar insertar una tupla en la tabla RECORRIDO_FINAL
cuyo intervalo de uso de una bicicleta para un usuario fuera solapado con otro
intervalo para el mismo usuario, rechace la inserción y emita por consola un aviso
```
## indicando el motivo del rechazo.

Tener en cuenta que:

- En la tabla RECORRIDO_FINAL no hay un atributo que represente el tiempo de uso sino
    que en base a la hora de retiro y el tiempo de uso del csv se debe calcular el valor del
    atributo _fecha_hora_dev_ que es de tipo TIMESTAMP.
- No se tiene que tener en cuenta para la migración el atributo del csv _fecha_creación:_
    este atributo se descarta, no se migra.
- No es posible cambiar los tipos de datos de RECORRIDO_FINAL, los datos del csv deben
    convertirse y adaptarse mediante funciones Postgresql para que la migración sea
    exitosa.
- Es importante el orden en que se resuelven los problemas: primero se debe resolver el
    1 y luego el 2 y por último el 3.
- Al finalizar se deben eliminar todas las tablas temporarias.

Se debe garantizar que antes de ejecutar **migracion()** la tabla RECORRIDO_FINAL está vacía.

**Ejemplo:**

```
a) Se tienen los siguientes datos en el archivo test1.csv (una muestra de 23 filas del archivo
recorridos_realizados_2016.csv)
```

Luego de ejecutar:

SELECT migracion();
SELECT * FROM recorrido_final;

Se obtiene:

Explicación del resultado:

Las filas 2 a 6 de **test1.csv** coinciden individualmente con las filas 1 a 5 de RECORRIDO_FINAL y
se calculó el atributo _fecha_hora_dev_ en base a su correspondiente
_fecha_hora_retiro+tiempo_uso._
Las filas 7, 8 y 9 de **test1.csv** presentan un problema tipo 2 ) – clave duplicada, por lo cual dado
que la segunda fila ordenada por _fecha_hora_retiro+tiempo_uso_ es la 8, dicha fila es la única
que pasa a la tabla y en el RECORRIDO_FINAL es la tupla 6.
Las filas 10 a 15 de **test1.csv,** correspondientes al usuario 74710, presentan un problema tipo 3 )
ya que a cada una de ellas corresponden los siguientes intervalos:

```
fila 10 [2016- 08 - 13 13:03:00,2016- 08 - 13 13:22:04]
fila 11 [2016- 08 - 13 13:09:00,2016- 08 - 13 13:27:09]
fila 12 [2016- 08 - 13 13:22:00,2016- 08 - 13 14:14:05]
fila 13 [2016- 08 - 13 13:28:00,2016- 08 - 13 13:47:46]
fila 14 [2016- 08 - 13 13:35:00,2016- 08 - 13 14:11:12]
fila 15 [2016- 08 - 13 13:42:00,2016- 08 - 13 13:54:52]
```
Por eso, en la tupla 7 de la tabla aparece el ISUM de dichos intervalos con la estación origen
correspondiente a la fila 10 de **test1.csv** y la estación destino correspondiente a la fila 15 de
**test1.csv.**

La fila 16 no presenta solapamiento con lo cual se transforma en la tupla 8 de la tabla
RECORRIDO_FINAL.

Así continúa todo el análisis.

```
b) Asumiendo que se han migrado exitosamente los datos y que la tabla
RECORRIDO_FINAL contiene los datos mostrados en el ejemplo anterior, los siguientes
son los resultados obtenidos de intentar insertar las siguientes tuplas:
```
```
solapados
```

- INSERT INTO recorrido_final VALUES('201601',8,'2016- 01 - 18
    16:28:00',23,23, '2016- 01 - 13 20:28:00');
Esta tupla se inserta sin problemas.
- INSERT INTO recorrido_final VALUES('201601',74710,'2016- 09 - 29
11:30:00',23,23, '2016- 09 - 29 11:32:00');
Se produce una excepción con el cartel ‘INSERCION IMPOSIBLE POR SOLAPAMIENTO’ y no se
producen cambios en la tabla.

## 4. Modalidad

El Trabajo Práctico estará disponible en Campus a partir del 07/06/201 8 , indicándose
allí mismo, la fecha de entrega.
Se incluye junto con el enunciado:
a) El archivo **recorridos-realizados-2016.csv** tal como se encuentra en
https://data.buenosaires.gob.ar/dataset/bicicletas-publicas.
b) Archivo para realizar pruebas **test1.csv** (ejemplo del enunciado).

El TP deberá realizarse en grupos de 3 alumnos y entregarse a través de la plataforma
Campus ITBA hasta la fecha allí indicada.

**_5. Entregables_**

```
Los alumnos deberán entregar los siguientes documentos:
```
- El script sql **funciones.sql** con el código necesario para crear las tablas que utilicen, los
    comandos para la importación, todas las funciones y el trigger.
- Un informe que debe contener:
    ▪ El rol de cada uno de los participantes del grupo. Si bien en el TP deben estar
       involucrados todos los integrantes, se debe asignar un rol de supervisión de
       cada una de las tareas. Mínimamente los roles son: encargado del informe,
       encargado de las funciones, encargado del trigger, encargado del
       funcionamiento global del proyecto y encargado de investigación. Pueden
       asignarse más roles en caso de requerirse.
    ▪ Todo lo investigado para realizar el TP.


```
▪ Las dificultades encontradas y cómo se resolvieron.
▪ El informe debe tener como máximo 3 páginas.
```
**_6. Evaluación_**

La evaluación del trabajo se llevará a cabo teniendo en cuenta los parámetros
establecidos en la rúbrica asociada a la actividad en Campus.
Se tendrá en cuenta que las consultas, más allá del funcionamiento (lo cual es
fundamental), sean genéricas.
Los docentes ejecutarán el proceso usando los conjuntos de datos entregados.
El informe deberá estar completo y sin faltas de ortografía.
En caso de que el trabajo no cumpliera los requisitos básicos para ser aprobado, los
alumnos serán citados en la fecha de recuperatorio para defenderlo y corregir los errores
detectados.
