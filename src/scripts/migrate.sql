--WIP
CREATE OR REPLACE FUNCTION migrate()
RETURNS void 
AS $$

DECLARE

--auxTable TABLE;
--validatedTable TABLE;

BEGIN
        CREATE TEMP TABLE IF NOT EXISTS auxTable(
        periodo TEXT,
        id_usuario INTEGER,
        fecha_hora_retiro TEXT,
        origen_estacion INTEGER,
        nombre_origen TEXT,
        destino_estacion INTEGER,
        nombre_destino TEXT, 
        tiempo_uso TIMESTAMP,
        fecha_creation TIMESTAMP,
        PRIMARY KEY(id_usuario, fecha_hora_retiro, tiempo_uso) --esto va a ver que reveerlo, pero por ahora con test funciona ... 
        );
        
        COPY auxTable
        FROM '../BD1/resources/test1.csv' DELIMITER ';' CSV HEADER;
        
        -- Eliminamos problema 1. Usuario, fecha & hora retiro, estacion origen, estacion destino, tiempo de uso NULL. 
        DELETE FROM auxTable 
        WHERE usuario IS NULL
                or fecha_hora_retiro IS NULL
                or origen_estacion IS NULL
                or destino_estacion IS NULL
                or tiempo_uso IS NULL; 
        
        -- Eliminamos problema 2. Funciona para el trial pero porque solo tiene uno que cumple. 
        CREATE TABLE newTable AS ( SELECT id_usuario as id, fecha_hora_retiro as fecha
        FROM bici
        GROUP BY id_usuario, fecha_hora_retiro
        HAVING COUNT(id_usuario) > 1 AND COUNT(fecha_hora_retiro) > 1
        ORDER BY id_usuario ASC); 
        
        SELECT * FROM bici, newTable 
        WHERE bici.id_usuario = newTable.id AND bici.fecha_hora_retiro = newTable.fecha 
        ORDER BY tiempo_uso ASC
        LIMIT 1 OFFSET 1;
        
--    SELECT * into auxTable FROM
--    OPENROWSET ('MSDASQL', 'Driver={Microsoft Text Driver (*.txt; *.csv)};DefaultDir={Directory Path of the CSV File};', 
--    'SELECT * from yourfile.csv');

--   SELECT * INTO validatedTable FROM auxTable A
--INNER JOIN validation_table B ON A.Datatype = B.Datatype
--INNER JOIN validation_table C ON A.Country = C.Country
--INNER JOIN validation_table D ON A.Currency = D.Currency
        DROP TABLE auxTable; 
END; 
$$ LANGUAGE plpgSQL