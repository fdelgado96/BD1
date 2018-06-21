
CREATE OR REPLACE FUNCTION  bici_publica()
RETURNS VOID AS $$

DECLARE

usu recorrido.usuario%TYPE;
fecha_ret recorrido.fecha_hora_ret%TYPE;
fecha_dev recorrido.fecha_hora_dev%TYPE;
cant INT;
fecha_dev_aux recorrido.fecha_hora_dev%TYPE;
fecha_max recorrido.fecha_hora_ret%TYPE;
fecha_aux recorrido.fecha_hora_ret%TYPE;

usuarioCursor CURSOR FOR
	SELECT distinct usuario FROM recorrido;
fechaCursor CURSOR FOR
	SELECT fecha_hora_ret FROM recorrido where usuario = usu;
auxCursor CURSOR FOR
	SELECT fecha_hora_ret FROM recorrido WHERE usuario = usu AND fecha_hora_ret <> fecha_ret AND fecha_hora_ret BETWEEN fecha_ret AND fecha_dev;	

BEGIN
	OPEN usuarioCursor;
	LOOP
		FETCH usuarioCursor INTO usu;   
		EXIT WHEN NOT FOUND;

			OPEN fechaCursor;
			LOOP
				FETCH fechaCursor INTO fecha_ret;
				EXIT WHEN NOT FOUND;
					
					SELECT fecha_hora_dev INTO fecha_dev
			        FROM recorrido
					WHERE usuario = usu
					AND fecha_hora_ret = fecha_ret;

					SELECT COUNT(*) INTO cant  
                    FROM recorrido
					WHERE usuario = usu
					AND fecha_hora_ret <> fecha_ret
					AND fecha_hora_ret BETWEEN fecha_ret AND fecha_dev;

					IF cant > 0 THEN

						fecha_max = NULL;
						OPEN auxCursor;
						LOOP
							FETCH auxCursor INTO fecha_aux;
							EXIT WHEN NOT FOUND;						
											
							SELECT fecha_hora_dev INTO fecha_dev_aux
							FROM recorrido
							WHERE usuario = usu	
							AND fecha_hora_ret = fecha_aux;

								IF fecha_dev_aux > fecha_max OR fecha_max IS NULL THEN

									fecha_max = fecha_dev_aux;

								END IF;

						END LOOP;
						CLOSE auxCursor;

						IF fecha_dev > fecha_max THEN
							fecha_max = fecha_dev;
						END IF;	

						UPDATE recorrido SET fecha_hora_dev = fecha_max
						WHERE usuario = usu
						AND fecha_hora_ret = fecha_ret;
					
					END IF;			

						INSERT INTO recorrido_final
						SELECT * FROM recorrido
						WHERE usuario = usu
						AND fecha_hora_ret = fecha_ret;

						DELETE FROM recorrido
						WHERE usuario = usu
						AND fecha_hora_ret <> fecha_ret
						AND fecha_hora_ret BETWEEN fecha_ret AND fecha_dev;


				END LOOP;
				CLOSE fechaCursor;
		END LOOP;
		CLOSE usuarioCursor;

END;
$$ LANGUAGE plpgsql; 