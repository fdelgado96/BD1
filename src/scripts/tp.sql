SET DATESTYLE = DMY;

CREATE TABLE import_table(
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

CREATE TABLE aux_table(
        periodo TEXT,
        id_usuario INTEGER,
        fecha_hora_retiro TEXT,
        origen_estacion INTEGER,
        nombre_origen TEXT,
        destino_estacion INTEGER,
        nombre_destino TEXT, 
        tiempo_uso TEXT,
        fecha_creacion TIMESTAMP,
        PRIMARY KEY(id_usuario, fecha_hora_retiro)
);

CREATE TABLE validated_table(
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

CREATE OR REPLACE FUNCTION eliminar_nulls() RETURNS VOID
AS $$
  BEGIN
  INSERT INTO validated_table (periodo, id_usuario, fecha_hora_retiro,
  origen_estacion, nombre_origen, destino_estacion, nombre_destino,
  tiempo_uso, fecha_creacion)
  SELECT periodo, id_usuario, fecha_hora_retiro,
  origen_estacion, nombre_origen, destino_estacion, nombre_destino,
  tiempo_uso, fecha_creacion
  FROM import_table
  WHERE id_usuario IS NOT NULL AND fecha_hora_retiro IS NOT NULL and origen_estacion IS NOT NULL AND destino_estacion IS NOT NULL and tiempo_uso IS NOT NULL 
  AND (tiempo_uso LIKE '_H _MIN _SEG' or tiempo_uso LIKE '_H _MIN __SEG' or tiempo_uso LIKE '_H __MIN _SEG' or tiempo_uso LIKE '_H __MIN __SEG' or
  tiempo_uso LIKE '__H _MIN _SEG' or tiempo_uso LIKE '__H _MIN __SEG' or tiempo_uso LIKE '__H __MIN _SEG' or tiempo_uso LIKE '__H __MIN __SEG');
END;  
$$ LANGUAGE plpgSQL; 

CREATE OR REPLACE FUNCTION eliminar_repetidos()
RETURNS void 
AS $$

DECLARE
        remove_cursor CURSOR FOR
        SELECT id_usuario, fecha_hora_retiro FROM validated_table
        GROUP BY id_usuario, fecha_hora_retiro HAVING COUNT(*) > 1; 
        usuario RECORD;
BEGIN
        -- DELETE FROM import_table 
        --         WHERE id_usuario IS NULL
        --                 or fecha_hora_retiro IS NULL
        --                 or origen_estacion IS NULL
        --                 or destino_estacion IS NULL
        --                 or tiempo_uso IS NULL;
        --         UPDATE import_table
        --         SET tiempo_uso = REPLACE(REPLACE(REPLACE(tiempo_uso,'SEG',' second'),'MIN',' min'),'H',' hours');
        
        -- INSERT INTO aux_table SELECT periodo, id_usuario, fecha_hora_retiro, origen_estacion, 
        --                         nombre_origen, destino_estacion, nombre_destino, CAST(tiempo_uso AS INTERVAL), 
        --                         fecha_creacion
        --                         FROM import_table
        --                         WHERE (id_usuario, fecha_hora_retiro) NOT IN (SELECT id_usuario, fecha_hora_retiro FROM import_table
        --                                 GROUP BY id_usuario, fecha_hora_retiro HAVING COUNT(*) > 1); 
        
        -- INSERT INTO invalidated_table SELECT periodo, id_usuario, fecha_hora_retiro, origen_estacion, 
        --                         nombre_origen, destino_estacion, nombre_destino, CAST(tiempo_uso AS INTERVAL), 
        --                         fecha_creacion
        --                         FROM import_table
        --                         WHERE (id_usuario, fecha_hora_retiro) IN (SELECT id_usuario, fecha_hora_retiro FROM import_table
        --                                 GROUP BY id_usuario, fecha_hora_retiro HAVING COUNT(*) > 1); 
        
        -- tomar el segundo -- 
        OPEN remove_cursor; 
               LOOP
                    FETCH remove_cursor INTO usuario;   
                    EXIT WHEN NOT FOUND;
                    PERFORM tomar_el_segundo(usuario.id_usuario, usuario.fecha_hora_retiro); 
                END LOOP; 
        CLOSE remove_cursor;  
        RETURN; 
END; 
$$ LANGUAGE plpgSQL; 

CREATE OR REPLACE FUNCTION tomar_el_segundo(aux_usuario_id aux_table.id_usuario%TYPE, 
aux_retiro aux_table.fecha_hora_retiro%TYPE) RETURNS void
AS $$
DECLARE 
    fechaCursor CURSOR FOR 
    SELECT * FROM validated_table
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
                fechaCursor.destino_estacion, fechaCursor.nombre_destino, CAST(REPLACE(REPLACE(REPLACE(fechaCursor.tiempo_uso,'SEG',' second'),'MIN',' min'),'H',' hours') AS INTEREVAL),
                fechaCursor.fecha_creacion); 
    CLOSE fechaCursor; 
    RETURN; 
END; 
$$ LANGUAGE plpgSQL;

CREATE OR REPLACE FUNCTION eliminar_superposicion()
RETURNS VOID AS $$

DECLARE

usuario aux_table.id_usuario%TYPE;
fecha_ret aux_table.fecha_hora_retiro%TYPE;
fecha_dev aux_table.fecha_hora_dev%TYPE;
destino aux_table.destino_estacion%TYPE;
cant INT;
fecha_ret_aux aux_table.fecha_hora_retiro%TYPE;
fecha_dev_aux aux_table.fecha_hora_dev%TYPE;
destino_nombre aux_table.nombre_destino%TYPE;

usuarioCursor CURSOR FOR
    SELECT distinct id_usuario FROM aux_table;
fechaCursor CURSOR FOR
    SELECT fecha_hora_retiro FROM aux_table;

BEGIN

    OPEN usuarioCursor;
    LOOP
        FETCH usuarioCursor INTO usuario;   
        EXIT WHEN NOT FOUND;

            OPEN fechaCursor;
            LOOP
                FETCH fechaCursor INTO fecha_retiro;
                EXIT WHEN NOT FOUND;
                    
                    SELECT fecha_hora_dev INTO fecha_dev
                    FROM aux_table
                    WHERE id_usuario = usuario
                    AND fecha_hora_retiro = fecha_retiro;

                    SELECT COUNT(*) INTO cant  
                    FROM aux_table
                    WHERE id_usuario = usuario
                    AND fecha_hora_retiro <> fecha_retiro
                    AND fecha_hora_retiro BETWEEN fecha_retiro AND fecha_dev;


                    IF cant > 0 THEN

                        SELECT fecha_hora_dev, destino_estacion, nombre_destino, fecha_hora_retiro INTO fecha_dev_aux, destino, destino_nombre,fecha_retiro_aux  
                        FROM aux_table
                        WHERE id_usuario = usuario                                                
                        AND fecha_hora_retiro <> fecha_retiro
                        AND fecha_hora_retiro BETWEEN fecha_retiro AND fecha_dev;
                                            
                        IF fecha_dev_aux BETWEEN fecha_dev AND current_timestamp THEN

                            UPDATE aux_table set fecha_hora_dev = fecha_dev_aux, destino_estacion = destino, nombre_destino = destino_nombre
                            WHERE id_usuario = usuario
                            AND fecha_hora_retiro = fecha_retiro;
                        END IF;
                                                  
                        DELETE FROM aux_table
                        WHERE id_usuario = usuario
                        AND fecha_hora_retiro = fecha_retiro_aux;

                    END IF;         

                END LOOP;
                CLOSE fechaCursor;
    
        END LOOP;
        CLOSE usuarioCursor;
    
END;
$$ LANGUAGE plpgsql; 

CREATE OR REPLACE FUNCTION migrate () RETURNS VOID 
AS $$
  BEGIN
  PERFORM eliminar_nulls();

  UPDATE import_table
  SET tiempo_uso = REPLACE(REPLACE(REPLACE(tiempo_uso,'SEG',' second'),'MIN',' min'),'H',' hours');
  
   
  PERFORM eliminar_repetidos();
  --PERFORM eliminar_superposicion();

  DROP TABLE aux_table;
  DROP TABLE validated_table; 
  DROP TABLE import_table;
END; 
$$ LANGUAGE PLPGSQL;


SELECT * FROM migrate(); 
SELECT * FROM aux_table; 
SELECT * FROM import_table;
SELECT * FROM invalidated_table; 
