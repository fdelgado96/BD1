--WIP
CREATE OR REPLACE FUNCTION migrate()
RETURNS void 
AS $$

DECLARE
        usuarioCursor CURSOR FOR
        SELECT distinct id_usuario FROM import_table;
        fechaCursor CURSOR FOR
        SELECT fecha_hora_retiro FROM import_table; 

        usuario INTEGER;
        fecha_retiro TIMESTAMP;

BEGIN
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

        SET DATESTYLE = DMY;
        COPY import_table
        FROM '/Users/martinascomazzon/BD1/resources/test1.csv' DELIMITER ';' CSV HEADER;
        
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
        -- CREATE TABLE key_violation_touples AS ( SELECT *
        -- FROM import_table t1
        -- WHERE t1.id_usuario IN (SELECT id
        --                         FROM import_table t2
        --                         GROUP BY t2.id_usuario, t2.fecha_hora_retiro
        --                         HAVING COUNT(id_usuario) > 1)
        -- ORDER BY id_usuario ASC);

        -- DELETE FROM import_table (SELECT * FROM key_violation_touples);
        -- DELETE FROM import_table (SELECT * FROM key_violation_touples);
        
        -- save second -- 
        OPEN usuarioCursor;
               LOOP
                FETCH usuarioCursor INTO usuario;   
                        EXIT WHEN NOT FOUND;

                        OPEN fechaCursor;
                                FETCH fechaCursor INTO fecha_retiro;
                                FETCH fechaCursor INTO fecha_retiro;
                                INSERT INTO aux_table (periodo, id_usuario, fecha_hora_retiro, origen_estacion,
                                        nombre_origen, destino_estacion, nombre_destino, tiempo_uso,
                                        fecha_creacion, tiempo_uso) VALUES (fechaCursor.periodo, fechaCursor.id_usuario, 
                                        fechaCursor.fecha_hora_retiro, fechaCursor.origen_estacion, fechaCursor.nombre_destino,
                                        fechaCursor.destino_estacion, fechaCursor.nombre_destino, fechaCursor.tiempo_uso,
                                        fechaCursor.fecha_creacion, fechaCursor.tiempo_uso); 
                             CLOSE fechaCursor;
               END LOOP;
        CLOSE usuarioCursor;
        DROP TABLE import_table;
        DROP TABLE aux_table; 
END; 
$$ LANGUAGE plpgSQL

select * from migrate(); 