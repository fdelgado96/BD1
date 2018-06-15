--WIP
CREATE OR REPLACE FUNCTION migrate()
RETURNS void 
AS $$

DECLARE
        usuarioCursor CURSOR FOR
	SELECT distinct id_usuario FROM keyViolationTouples;
        fechaCursor CURSOR FOR
	SELECT fecha_hora_retiro FROM keyViolationTouples;

        usuario auxTable.id_usuario%TYPE;
        fecha_retiro auxTable.fecha_hora_retiro%TYPE;

BEGIN
        CREATE TEMP TABLE IF NOT EXISTS auxTable(
        periodo TEXT,
        id_usuario INTEGER,
        fecha_hora_retiro TEXT,
        origen_estacion INTEGER,
        nombre_origen TEXT,
        destino_estacion INTEGER,
        nombre_destino TEXT, 
        tiempo_uso TEXT,
        fecha_creation TIMESTAMP,
        primaryKey(id_usuario, fecha_hora_retiro, tiempo_uso),
        );
        
        COPY auxTable
        FROM '/home/francisco/itba/BD1/resources/test1.csv' DELIMITER ';' CSV HEADER;
        
        DELETE FROM auxTable 
        WHERE id_usuario IS NULL
                or fecha_hora_retiro IS NULL
                or origen_estacion IS NULL
                or destino_estacion IS NULL
                or tiempo_uso IS NULL;
        UPDATE auxTable
        SET tiempo_uso = REPLACE(REPLACE(REPLACE(tiempo_uso,'SEG','s'),'MIN','m'),'H','h')
         
        -- Eliminamos problema 2. Funciona para el trial pero porque solo tiene uno que cumple. 
        CREATE TABLE keyViolationTouples AS ( SELECT *
        FROM auxTable t1
        WHERE t1.id_usuario IN (SELECT id
                                FROM auxTable t2
                                GROUP BY t2.id_usuario, t2.fecha_hora_retiro
                                HAVING COUNT(id_usuario) > 1)
        ORDER BY id_usuario ASC);

        DELETE FROM auxTable (SELECT * FROM keyViolationTouples );

        DELETE FROM auxTable (SELECT * FROM keyViolationTouples);

BEGIN

	OPEN usuarioCursor;
	LOOP
		FETCH usuarioCursor INTO usuario;   
	
			OPEN fechaCursor;
                        FETCH fechaCursor INTO fecha_retiro;
                        DELETE 
                        FETCH fechaCursor INTO fecha_retiro;

                        INSERT INTO validatedKeyTable 
			LOOP
				FETCH fechaCursor INTO fecha_retiro;			
                                
				EXIT WHEN NOT FOUND;
				END LOOP;
				CLOSE fechaCursor;
	
		EXIT WHEN NOT FOUND;
		END LOOP;
		CLOSE usuarioCursor;


        SELECT * FROM auxTable, newTable 
        WHERE auxTable.id_usuario = newTable.id AND auxTable.fecha_hora_retiro = newTable.fecha 
        ORDER BY tiempo_uso ASC
        LIMIT 1 OFFSET 1;
        DROP TABLE auxTable; 
END; 
$$ LANGUAGE plpgSQL