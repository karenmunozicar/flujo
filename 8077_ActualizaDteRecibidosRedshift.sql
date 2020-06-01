--Publica documento
delete from isys_querys_tx where llave='8077';

insert into isys_querys_tx values ('8077',10,1,1,'select lee_parametro_8077(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('8077',20,19,1,'select actualiza_dte_recibidos_redshift_8077(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,0,0);

CREATE OR REPLACE FUNCTION lee_parametro_8077(character varying) RETURNS varchar AS $$
declare
        xml1    alias for $1;
        xml2    varchar;
begin
        xml2:=xml1;
	xml2:=put_campo(xml2,'__SECUENCIAOK__','20');
	xml2:=get_parametros_motor(xml2,'BASE_REDSHIFT_RECIBIDOS');
	return xml2;
end;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION actualiza_dte_recibidos_redshift_8077(character varying) RETURNS varchar AS $$
declare
        xml1    alias for $1;
        xml2    varchar;
	json1	json;
	json_par1	json;
	query1	varchar;
	json_resp1	json;
	mensaje1	varchar;
	fecha_nar1	varchar;
begin
        xml2:=xml1;
	xml2:=put_campo(xml2,'__SECUENCIAOK__','0');
	json1:=decode(get_campo('INPUT',xml2),'hex')::varchar::json;
	mensaje1:=decode(get_json('mensaje_nar',json1),'hex');
	fecha_nar1:=get_json('fecha_nar',json1);
	if (fecha_nar1='') then
		fecha_nar1:=null;
	end if;

--	query1:='update dte_recibidos set uri_arm='||quote_literal(get_json('uri_arm',json1))||',fecha_ult_modificacion='|| quote_literal(get_json('fecha_ult_modificacion',json1))||',uri_nar='||quote_literal(get_json('uri_nar',json1))||',estado_nar='||quote_literal(get_json('estado_nar',json1))||',mensaje_nar='||quote_literal(mensaje1)||',fecha_nar='||quote_literal(fecha_nar1)||'::timestamp,estado='||quote_literal(get_json('estado',json1))||',estado_sii='||quote_literal(get_json('estado_sii',json1))||' where codigo_txel='||get_json('codigo_txel',json1);
	insert into dte_recibidos_actulizacion((codigo_txel,uri_arm,fecha_ult_modificacion,uri_nar,estado_nar,mensaje_nar,fecha_nar,estado,estado_sii) values (
	
	json_resp1:=query_db_json(get_campo('__IP_CONEXION_CLIENTE__',xml2),get_campo('__IP_PORT_CLIENTE__',xml2)::integer,query1);
	xml2:=logapp(xml2,'Respuesta Redshift '||json_resp1::varchar);
	if (get_json('STATUS',json_resp1)='OK') then
		xml2:=logapp(xml2,'Se procesa OK URI='||get_json('uri',json1)||' en REDSHIFT');
		xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
	else
		xml2:=put_campo(xml2,'RESPUESTA','Status: 400 NK');
		xml2:=logapp(xml2,'Falla URI='||get_json('uri',json1)||' en REDSHIFT');
	end if;
	return sp_procesa_respuesta_cola_motor_original(xml2);
end;
$$ LANGUAGE plpgsql;
