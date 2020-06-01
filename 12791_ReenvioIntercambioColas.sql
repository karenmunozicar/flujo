delete from isys_querys_tx where llave='12791';

--Se envia al Flujo NAR del 10k
insert into isys_querys_tx values ('12791',10,1,8,'Llamada Intercambio',12764,0,0,0,0,20,20);
--Validamos la publicacion del NAR
insert into isys_querys_tx values ('12791',20,19,1,'select valida_envio_inter_12791(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

--insert into isys_querys_tx values ('12791',30,19,1,'select sp_procesa_respuesta_cola_motor(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,0,0);

CREATE or replace FUNCTION valida_envio_inter_12791(varchar) RETURNS varchar AS $$
declare
        xml1    alias for $1;
        xml2    varchar;
	resp1	varchar;
	codigo_resp1	varchar;
	id1	varchar;	
	
begin
        xml2:=xml1;
        xml2:=put_campo(xml2,'__SECUENCIAOK__','0');
	id1:=get_campo('__ID_DTE__',xml2);

	--Si me fue bien enviando al EDTE borro de la cola
	if (get_campo('__EDTE_REENVIO_INTER_OK__',xml2)='SI') then
		 xml2:=logapp(xml2,'Se borra Reenvio Inter de la cola '||get_campo('__COLA_MOTOR__',xml2)||' URI='||get_campo('URI_IN',xml2));
		 xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
		  --execute 'delete from '||get_campo('__COLA_MOTOR__',xml2)||' where id='||id1;
	else
		 xml2:=logapp(xml2,'Falla envio Inter de la cola '||get_campo('__COLA_MOTOR__',xml2)||' URI='||get_campo('URI_IN',xml2));
		 xml2:=put_campo(xml2,'RESPUESTA','Status: 400 NK');
		 --execute 'update '||get_campo('__COLA_MOTOR__',xml2)||' set reintentos=reintentos+1 where id='||id1;
	end if;
	return sp_procesa_respuesta_cola_motor_original(xml2);
        --return xml2;
END;
$$ LANGUAGE plpgsql;

