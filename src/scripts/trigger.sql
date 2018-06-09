--Funcion que utiliza el trigger
CREATE OR REPLACE FUNCTION verifica_solapado()
RETURNS trigger AS $$
DECLARE

encontrado int;

BEGIN

        SELECT COUNT(*) INTO encontrado 
        FROM recorrido_final 
        WHERE usuario = NEW.usuario 
        AND NEW.fecha_hora_ret BETWEEN fecha_hora_ret AND fecha_hora_dev;
        
        IF encontrado > 0 THEN
                raise EXCEPTION 'INSERCION IMPOSIBLE POR SOLAPAMIENTO';
        END IF;

RETURN NEW;
END;
$$ LANGUAGE plpgsql; 


--Creacion del trigger
CREATE TRIGGER detecta_solapado
  BEFORE insert
  ON recorrido_final
  FOR EACH ROW
  EXECUTE PROCEDURE verifica_solapado();