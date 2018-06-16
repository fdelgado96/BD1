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
COPY import_table FROM '/Users/martinascomazzon/BD1/resources/test1.csv' DELIMITER ';' CSV HEADER;

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
        SELECT id_usuario, fecha_hora_retiro FROM invalidated_table;
        
        -- fechaCursor CURSOR FOR
        -- SELECT fecha_hora_retiro FROM invalidated_table; 

        usuario RECORD;
        --fecha_retiro TIMESTAMP;

BEGIN

        DELETE FROM import_table 
                WHERE id_usuario IS NULL
                        or fecha_hora_retiro IS NULL
                        or origen_estacion IS NULL
                        or destino_estacion IS NULL
                        or tiempo_uso IS NULL;
                UPDATE import_table

        SET tiempo_uso = REPLACE(REPLACE(REPLACE(tiempo_uso,'SEG',' second'),'MIN',' min'),'H',' hours');
        
        INSERT INTO aux_table SELECT periodo, id_usuario, fecha_hora_retiro, origen_estacion, 
                                nombre_origen, destino_estacion, nombre_destino, CAST(tiempo_uso AS INTERVAL), 
                                fecha_creacion
                                FROM import_table
                                WHERE (id_usuario, fecha_hora_retiro) NOT IN (SELECT id_usuario, fecha_hora_retiro FROM import_table
                                        GROUP BY id_usuario, fecha_hora_retiro HAVING COUNT(*) > 1); 
        
        INSERT INTO invalidated_table SELECT periodo, id_usuario, fecha_hora_retiro, origen_estacion, 
                                nombre_origen, destino_estacion, nombre_destino, CAST(tiempo_uso AS INTERVAL), 
                                fecha_creacion
                                FROM import_table
                                WHERE (id_usuario, fecha_hora_retiro) IN (SELECT id_usuario, fecha_hora_retiro FROM import_table
                                        GROUP BY id_usuario, fecha_hora_retiro HAVING COUNT(*) > 1); 
        -- save second -- 
        OPEN remove_cursor; 
               LOOP
                    FETCH remove_cursor INTO usuario;   
                    EXIT WHEN NOT FOUND;
                    PERFORM get_second (remove_cursor.id_usuario, remove_cursor.fecha_hora_retiro); 
                END LOOP
        CLOSE remove_cursor; 
        RETURN; 
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
        FETCH fechaCursor INTO fecha_retiro;
        FETCH fechaCursor INTO fecha_retiro;
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

Select * from migrate(); 

select*from aux_table; 
select*from import_table;


OPEN fechaCursor;
                                
                        END LOOP; 