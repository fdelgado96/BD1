
CREATE OR REPLACE FUNCTION  migrate()
RETURNS VOID AS $$

DECLARE

usuario auxTable.id_usuario%TYPE;
fecha_ret auxTable.fecha_hora_ret%TYPE;
fecha_dev auxTable.fecha_hora_dev%TYPE;
destino auxTable.destino_estacion%TYPE;
cant INT;
fecha_ret_aux auxTable.fecha_hora_ret%TYPE;
fecha_dev_aux auxTable.fecha_hora_dev%TYPE;

usuarioCursor CURSOR FOR
	SELECT distinct id_usuario FROM auxTable;
fechaCursor CURSOR FOR
	SELECT fecha_hora_ret FROM auxTable;

BEGIN

	OPEN usuarioCursor;
	LOOP
		FETCH usuarioCursor INTO usuario;   

	
			OPEN fechaCursor;
			LOOP
				FETCH fechaCursor INTO fecha_ret;
					
					SELECT fecha_hora_dev INTO fecha_dev 
                                        FROM auxTable
					WHERE id_usuario = usuario
					AND fecha_hora_ret = fecha_ret;

					SELECT COUNT(*), fecha_hora_dev, destino_estacion, fecha_hora_ret INTO cant, fecha_dev_aux, destino, fecha_ret_aux  
                                        FROM auxTable
					WHERE id_usuario = usuario
					GROUP BY fecha_hora_dev, destino_estacion, fecha_hora_ret
					HAVING fecha_hora_ret <> fecha_ret
					AND fecha_hora_ret BETWEEN fecha_ret AND fecha_dev;
					

					IF cant > 0 THEN
					
						IF fecha_dev_aux > fecha_dev THEN

							UPDATE auxTable set fecha_hora_dev = fecha_dev_aux, destino_estacion = destino
							FROM auxTable
							WHERE id_usuario = usuario
							AND fecha_hora_ret = fecha_ret;
                        END IF;
                                                
                                                
						DELETE FROM auxTable
						WHERE id_usuario = usuario
						AND fecha_hora_ret = fecha_ret_aux;

					END IF;			


				EXIT WHEN NOT FOUND;
				END LOOP;
				CLOSE fechaCursor;
	
		EXIT WHEN NOT FOUND;
		END LOOP;
		CLOSE usuarioCursor;
	
RETURN;
END;
$$ LANGUAGE plpgsql; 