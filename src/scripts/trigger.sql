--Funcion que utiliza el trigger
CREATE OR REPLACE FUNCTION verifica_solapado()
RETURNS trigger AS $$
DECLARE

usu recorrido_final.usuario%TYPE;
fecha_ret recorrido_final.fecha_hora_ret%TYPE;
fecha_dev recorrido_final.fecha_hora_dev%TYPE;

usuarioCursor CURSOR FOR
 SELECT usuario FROM recorrido_final;
fechaCursor CURSOR FOR
 SELECT fecha_hora_ret FROM recorrido_final where usuario = usu;

BEGIN
OPEN usuarioCursor;
LOOP
        FETCH usuarioCursor INTO usu;

        OPEN fechaCursor;
        LOOP
            
            FETCH fechaCursor INTO fecha_ret;
            SELECT fecha_hora_dev INTO fecha_dev FROM recorrido_final WHERE usuario = usu AND fecha_hora_ret = fecha_ret;

            IF NEW.usuario = usu THEN
                IF NEW.fecha_hora_ret BETWEEN fecha_ret AND fecha_dev THEN
                    raise EXCEPTION 'INSERCION IMPOSIBLE POR SOLAPAMIENTO';
                END IF;    
            END IF;    

            EXIT WHEN NOT FOUND;
        END LOOP;
        CLOSE fechaCursor;
        
        EXIT WHEN NOT FOUND;
END LOOP;
CLOSE usuarioCursor;
RETURN NEW;
END;
$$ LANGUAGE plpgsql; 


--Creacion del trigger
CREATE TRIGGER detecta_solapado
  BEFORE insert
  ON recorrido_final
  FOR EACH ROW
  EXECUTE PROCEDURE verifica_solapado();