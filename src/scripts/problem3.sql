CREATE OR REPLACE FUNCTION  remove_overlap()
RETURNS VOID AS $$

DECLARE

usuario aux_table.id_usuario%TYPE;
fecha_ret aux_table.fecha_hora_ret%TYPE;
fecha_dev aux_table.fecha_hora_dev%TYPE;
destino aux_table.destino_estacion%TYPE;
cant INT;
fecha_ret_aux aux_table.fecha_hora_ret%TYPE;
fecha_dev_aux aux_table.fecha_hora_dev%TYPE;
destino_nombre aux_table.nombre_destino%TYPE;

usuarioCursor CURSOR FOR
	SELECT distinct id_usuario FROM aux_table;
fechaCursor CURSOR FOR
	SELECT fecha_hora_ret FROM aux_table;

BEGIN

	OPEN usuarioCursor;
	LOOP
		FETCH usuarioCursor INTO usuario;   
		EXIT WHEN NOT FOUND;

			OPEN fechaCursor;
			LOOP
				FETCH fechaCursor INTO fecha_ret;
				EXIT WHEN NOT FOUND;
					
					SELECT fecha_hora_dev INTO fecha_dev
			        FROM aux_table
					WHERE id_usuario = usuario
					AND fecha_hora_ret = fecha_ret;

					SELECT COUNT(*) INTO cant  
                    FROM aux_table
					WHERE id_usuario = usuario
					AND fecha_hora_ret <> fecha_ret
					AND fecha_hora_ret BETWEEN fecha_ret AND fecha_dev;


					IF cant > 0 THEN

						SELECT fecha_hora_dev, destino_estacion, nombre_destino, fecha_hora_ret INTO fecha_dev_aux, destino, destino_nombre,fecha_ret_aux  
						FROM aux_table
						WHERE id_usuario = usuario                                                
						AND fecha_hora_ret <> fecha_ret
						AND fecha_hora_ret BETWEEN fecha_ret AND fecha_dev;
											
						IF fecha_dev_aux BETWEEN fecha_dev AND current_timestamp THEN

							UPDATE aux_table set fecha_hora_dev = fecha_dev_aux, destino_estacion = destino, nombre_destino = destino_nombre
							WHERE id_usuario = usuario
							AND fecha_hora_ret = fecha_ret;
                        END IF;
                                                  
						DELETE FROM aux_table
						WHERE id_usuario = usuario
						AND fecha_hora_ret = fecha_ret_aux;

					END IF;			

				END LOOP;
				CLOSE fechaCursor;
	
		END LOOP;
		CLOSE usuarioCursor;
	
END;
$$ LANGUAGE plpgsql; 