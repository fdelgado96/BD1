SET DATESTYLE = DMY;

CREATE TEMP TABLE IF NOT EXISTS import_table(
        periodo TEXT,
        id_usuario INTEGER,
        fecha_hora_retiro TEXT,
        origen_estacion INTEGER,
        nombre_origen TEXT,
        destino_estacion INTEGER,
        nombre_destino TEXT, 
        tiempo_uso TEXT,
        fecha_creacion TIMESTAMP
);
COPY import_table FROM '/home/francisco/itba/BD1/resources/test1.csv' DELIMITER ';' CSV HEADER;

CREATE TEMP TABLE IF NOT EXISTS aux_table(
        periodo TEXT,
        id_usuario INTEGER,
        fecha_hora_retiro TEXT,
        origen_estacion INTEGER,
        nombre_origen TEXT,
        destino_estacion INTEGER,
        nombre_destino TEXT, 
        tiempo_uso INTERVAL,
        fecha_creacion TIMESTAMP,
        PRIMARY KEY(id_usuario, fecha_hora_retiro)
);

CREATE TEMP TABLE IF NOT EXISTS invalidated_table(
        periodo TEXT,
        id_usuario INTEGER,
        fecha_hora_retiro TEXT,
        origen_estacion INTEGER,
        nombre_origen TEXT,
        destino_estacion INTEGER,
        nombre_destino TEXT, 
        tiempo_uso INTERVAL,
        fecha_creacion TIMESTAMP
);

CREATE OR REPLACE FUNCTION migrate()
RETURNS void 
AS $$

DECLARE
        
        remove_cursor CURSOR FOR 
        SELECT DISTINCT id_usuario, fecha_hora_retiro 
        FROM import_table 
        WHERE id_usuario IS NOT NULL AND fecha_hora_retiro IS NOT NULL AND origen_estacion IS NOT NULL AND destino_estacion IS NOT NULL
        AND tiempo_uso IS NOT NULL 
        AND CAST(REPLACE(REPLACE(REPLACE(tiempo_uso,'SEG',' second'),'MIN',' min'),'H',' hours') AS INTERVAL)IS NOT NULL
        GROUP BY id_usuario, fecha_hora_retiro
        HAVING count(id_usuario) > 1;
        touple RECORD;

        -- fechaCursor CURSOR FOR
        -- SELECT fecha_hora_retiro FROM invalidated_table; 

        invalid_touple RECORD;
        --fecha_retiro TIMESTAMP;

BEGIN

        INSERT INTO aux_table SELECT periodo, id_usuario, fecha_hora_retiro, origen_estacion, 
                                nombre_origen, destino_estacion, nombre_destino, CAST(REPLACE(REPLACE(REPLACE(tiempo_uso,'SEG',' second'),'MIN',' min'),'H',' hours') AS INTERVAL), 
                                fecha_creacion
                                FROM import_table
                                WHERE (id_usuario, fecha_hora_retiro) NOT IN (SELECT id_usuario, fecha_hora_retiro FROM import_table
                                        GROUP BY id_usuario, fecha_hora_retiro HAVING COUNT(*) > 1); 
        -- save second -- 
        
        OPEN remove_cursor;
        LOOP
        FETCH remove_cursor INTO touple;
        EXIT WHEN NOT FOUND;

        CREATE TABLE invalid_touples AS SELECT * FROM aux_table WHERE
        tuple.id_usuario = aux.id_usuario AND tuple.fecha_hora_retiro = aux.fecha_hora_retiro
        ORDER BY CAST(replace(replace(replace(aux.tiempo_uso, 'H ', 'H'), 'MIN ', 'M'), 'SEG', 'S') AS INTERVAL);

        FOR invalid_touple IN SELECT * FROM invalid_touples LIMIT 1 
        LOOP
        DELETE FROM aux_table WHERE (invalid_touple.destino_estacion = aux.destino_estacion AND invalid_touple.nombre_destino = aux.nombre_destino AND
                invalid_touple.nombre_origen = aux.nombre_origen AND invalid_touple.periodo = aux.periodo AND
                 invalid_touple.origen_estacion = aux.origen_estacion AND invalid_touple.tiempo_uso = aux.tiempo_uso AND invalid_touple.fecha_creacion = aux.fecha_creacion);
        END LOOP;

        FOR invalid_touple IN SELECT * FROM invalid_touples OFFSET 2 
        LOOP
        DELETE FROM aux WHERE (invalid_touple.destino_estacion = aux.destino_estacion AND invalid_touple.nombre_destino = aux.nombre_destino AND
                invalid_touple.nombre_origen = aux.nombre_origen AND invalid_touple.periodo = aux.periodo AND
                 invalid_touple.origen_estacion = aux.origen_estacion AND invalid_touple.tiempo_uso = aux.tiempo_uso AND invalid_touple.fecha_creacion = aux.fecha_creacion);
        END LOOP;

        DELETE FROM invalid_touples;
        END LOOP;
        CLOSE remove_cursor;
END; 
$$ LANGUAGE plpgSQL; 

CREATE OR REPLACE FUNCTION get_second(aux_usuario_id aux_table.id_usuario%TYPE, 
aux_retiro aux_table.fecha_hora_retiro%TYPE) RETURNS void
AS $$
DECLARE 
    fechaCursor CURSOR FOR 
    SELECT * FROM import_table
    WHERE id_usuario = aux_usuario_id AND fecha_hora_retiro = aux_retiro
    ORDER BY tiempo_uso ASC;
    usuario RECORD; 
BEGIN
    OPEN fechaCursor; 
        FETCH fechaCursor INTO usuario;
        FETCH fechaCursor INTO usuario;
            INSERT INTO aux_table (periodo, id_usuario, fecha_hora_retiro, origen_estacion,
                nombre_origen, destino_estacion, nombre_destino, tiempo_uso,
                fecha_creacion) VALUES (fechaCursor.periodo, fechaCursor.id_usuario, 
                fechaCursor.fecha_hora_retiro, fechaCursor.origen_estacion, fechaCursor.nombre_destino,
                fechaCursor.destino_estacion, fechaCursor.nombre_destino, fechaCursor.tiempo_uso,
                fechaCursor.fecha_creacion); 
    CLOSE fechaCursor; 
    RETURN; 
END; 
$$ LANGUAGE plpgSQL; 

DROP TABLE aux_table; 
DROP TABLE import_table; 
DROP TABLE invalidated_table; 

SELECT * FROM migrate(); 
SELECT * FROM aux_table; 
SELECT * FROM import_table;
SELECT * FROM invalidated_table;