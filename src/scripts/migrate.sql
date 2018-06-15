--WIP
CREATE OR REPLACE FUNCTION migrate()
RETURNS void 
AS $$

DECLARE
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

        DELETE FROM auxTable (SELECT * FROM keyViolationTouples)


        
        SELECT * FROM bici, newTable 
        WHERE bici.id_usuario = newTable.id AND bici.fecha_hora_retiro = newTable.fecha 
        ORDER BY tiempo_uso ASC
        LIMIT 1 OFFSET 1;
        DROP TABLE auxTable; 
END; 
$$ LANGUAGE plpgSQL