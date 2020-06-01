delete from isys_querys_tx where llave='12766';

--Se envia al Flujo NAR del 10k
insert into isys_querys_tx values ('12766',10,1,8,'Llamada NAR',12796,0,0,0,0,20,20);
--Validamos la publicacion del NAR
insert into isys_querys_tx values ('12766',20,1,1,'select valida_publicacion_nar_12766(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

CREATE or replace FUNCTION valida_publicacion_nar_12766(varchar) RETURNS varchar AS $$
declare
        xml1    alias for $1;
        xml2    varchar;
	resp1	varchar;
	codigo_resp1	varchar;
	id1	varchar;	
	
begin
        xml2:=xml1;
        xml2:=put_campo(xml2,'__SECUENCIAOK__','0');
	--Sacamos la respuesta y vemos si fue exitoso el NAR
	id1:=get_campo('__ID_DTE__',xml2);
	resp1:=split_part(get_campo('RESPUESTA',xml2),chr(10)||chr(10),2);
	--Si se aprobo el NAR
	if (get_json('CODIGO_RESPUESTA',resp1::json)='1') then
		 xml2:=logapp(xml2,'Se borra NAR de la cola '||get_campo('__COLA_MOTOR__',xml2)||' URI='||get_campo('URI_DTE',xml2)||' ID='||id1);
		 execute 'delete from '||get_campo('__COLA_MOTOR__',xml2)||' where id='||id1;
	else
		--Si el mensaje es que no tiene certificados para firmar, no se puede enviar el NAR
		if (strpos(get_json('MENSAJE_RESPUESTA',resp1::json),'No existen certificados para el rut y password')>0) then
		 	xml2:=logapp(xml2,'Se borra NAR de la cola '||get_campo('__COLA_MOTOR__',xml2)||' URI='||get_campo('URI_DTE',xml2)||' ID='||id1);
			execute 'delete from '||get_campo('__COLA_MOTOR__',xml2)||' where id='||id1;
		else
		 	xml2:=logapp(xml2,'Falla NAR , se mantiene en la cola '||get_campo('__COLA_MOTOR__',xml2)||' URI='||get_campo('URI_DTE',xml2)||' ID='||id1);
		 	execute 'update '||get_campo('__COLA_MOTOR__',xml2)||' set reintentos=reintentos+1 where id='||id1;
		end if;
	end if;
        return xml2;
END;
$$ LANGUAGE plpgsql;

