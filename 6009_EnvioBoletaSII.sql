delete from isys_querys_tx where llave='6009';
insert into isys_querys_tx values ('6009',2,9,16,'LOG_JSON',0,0,0,1,1,5,5);
insert into isys_querys_tx values ('6009',5,19,1,'select control_flujo_80101(''$$__JSONCOMPLETO__["__PROC_ACTIVOS__","TX","REQUEST_URI","__ARGV__","__CATEGORIA_COLA__","__FLUJO_ACTUAL__"]$$''::json) as __json__',0,0,0,1,1,-1,10);
insert into isys_querys_tx values ('6009',10,1,1,'select lee_datos_boleta_6009(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,1000);
insert into isys_querys_tx values ('6009',15,19,1,'select envio_boleta_6009(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,1000);
insert into isys_querys_tx values ('6009',22,1,2,'Microservicioe 127.0.0.1',4013,300,101,0,0,25,25);
insert into isys_querys_tx values ('6009',25,1,1,'select valida_respuesta_envio_boleta_sii_8010(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,30);
insert into isys_querys_tx values ('6009',30,19,1,'select valida_respuesta_envio_boleta_sii_error_8010(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,1000);
insert into isys_querys_tx values ('6009',1000,19,1,'select sp_procesa_respuesta_cola_motor_original(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,0,0);
insert into isys_querys_tx values ('6009',1010,19,1,'select sp_procesa_respuesta_cola_motor_original(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,0,0);

CREATE OR REPLACE FUNCTION encola_envio_6009(varchar,varchar)
returns varchar as
$$
declare
	uri1	alias for $1;
	cod1	alias for $2;
	nombre_tabla1	varchar;
	xml7	varchar;
	id1	bigint;
begin
	xml7:=put_campo('','URI_IN',uri1);
	if is_number(cod1) is false then
		return 'FALLA_COD_TXEL';
	end if;
	xml7:=put_campo(xml7,'CODIGO_TXEL',cod1::varchar);
	xml7:=put_campo(xml7,'TX','6009');
        nombre_tabla1:='cola_motor_4';
	--||nextval('id_cola_procesamiento')::varchar;
        execute 'insert into '||nombre_tabla1||' (fecha,uri,reintentos,data,tx,rut_emisor,reproceso,categoria,nombre_cola) values (now(),'||quote_literal(uri1)||',0,'||quote_literal(xml7)||','||'30'||',null,''NO'',''ENVIO_BOLETA_SII'','''||nombre_tabla1||''') returning id' into id1;
	return 'ENCOLA_ENVIO_BOLETA ID='||id1::varchar||' COD_TXEL='||cod1::varchar||' '||uri1;
end;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION lee_datos_boleta_6009(json)
returns json as
$$
declare
        json1   alias for $1;
        json2   json;
        track1  varchar;
        campo   record;
        j3      json;
	cod1	bigint;
begin
        json2:=json1;
	cod1:=get_json('CODIGO_TXEL',json2);
	select * into campo from dte_boletas_generica where codigo_txel=cod1;
	if not found then
		json2:=logjson(json2,'No se encuentra boleta cod='||cod1::varchar);
		json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
		json2:=put_json(json2,'__SECUENCIAOK__','1000');
		return json2;
	end if;
	--Revisamos si ya esta enviada
	if get_xml('ESTADO_BOLETA',campo.data_dte) in ('ENVIADO_AL_SII','ACEPTADO_POR_EL_SII','RECHAZADO_POR_EL_SII','ACEPTADO_CON_REPAROS_POR_EL_SII') then
		if get_xml('ESTADO_BOLETA',campo.data_dte)='RECHAZADO_POR_EL_SII' and strpos(campo.mensaje_sii,'Rut No Autorizado a Firmar')>0 then
			json2:=logjson(json2,'Boleta con Rut No Autorizado a Firmar se reintenta cod='||cod1::varchar);
		else
			json2:=logjson(json2,'Boleta Ya Enviada cod='||cod1::varchar||' '||get_xml('ESTADO_BOLETA',campo.data_dte));
	                json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
        	        json2:=put_json(json2,'__SECUENCIAOK__','1000');
                	return json2;
		end if;
	end if;
	json2:=put_json(json2,'RUT_EMISOR',campo.rut_emisor::varchar);
	json2:=put_json(json2,'RUT_RECEPTOR',campo.rut_receptor::varchar);
	json2:=put_json(json2,'TIPO_DTE',campo.tipo_dte::varchar);
	if (exists(select * from rut_tabla_boletas where rut_boleta=campo.rut_emisor)) then
		json2:=put_json(json2,'TABLA_BOLETA','dte_boletas_'||to_char(campo.fecha_ingreso,'YYMM')||'_'||campo.rut_emisor::varchar);
	else
		json2:=put_json(json2,'TABLA_BOLETA','dte_boletas_'||to_char(campo.fecha_ingreso,'YYMM'));
	end if;
	select * into campo from maestro_clientes where rut_emisor=campo.rut_emisor;
	if not found then 
		json2:=logjson(json2,'No se encuentra cliente en maestro_clientes '||campo.rut_emisor::varchar);
		json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
		json2:=put_json(json2,'__SECUENCIAOK__','1000');
		return json2;
	end if;
	json2:=put_json(json2,'FLAG_BOLETAS_MASIVAS',campo.flag_boletas_masivas);
	json2:=put_json(json2,'__SECUENCIAOK__','15');
	return json2;
end;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION envio_boleta_6009(json)
returns json as
$$
declare
        json1   alias for $1;
        json2   json;
        track1  varchar;
        campo   record;
        j3      json;
begin
        json2:=json1;
	if get_json('RESPUESTA_ENVIO_SII',json2)<>'' then
		json2:=logjson(json2,'Ya tenemos RESPUESTA_ENVIO_SII');
		json2:=put_json(json2,'RESPUESTA',get_json('RESPUESTA_ENVIO_SII',json2));
		--json2:=put_json(json2,'RESPUESTA_ENVIO_SII','');
		json2:=put_json(json2,'__SECUENCIAOK__','25');	
		return json2;
	end if;
	/*
	if get_json('FLAG_BOLETAS_MASIVAS',json2)='SI' and to_char(now(),'HH24')::integer>7 then
		json2:=put_json(json2,'RESPUESTA','Status: 555 NK');
                json2:=put_json(json2,'MENSAJE_XML_FLAGS','ENVIO_NOCTURNO');
                json2:=put_json(json2,'__FECHA_FUTURO_COLA__',to_char(now()+interval '1 day','YYYY-MM-DD 00:05:00'));
		json2:=put_json(json2,'__SECUENCIAOK__','1000');
		return json2;
	end if;*/
        j3:=put_json('{}','URI_IN',get_json('URI_IN',json2));
        j3:=put_json(j3,'INPUT_CUSTODIUM',get_input_almacen(put_json('{}','uri',get_json('URI_IN',json2))::varchar));

	json2:=put_json(json2,'__SECUENCIAOK__','22');
        json2:=put_json(json2,'URI_MS','ms/EnvioBoletaSII');
        json2:=put_json(json2,'__IP_CONEXION_CLIENTE__','127.0.0.1');
        json2:=put_json(json2,'__IP_PORT_CLIENTE__','5010');
        json2:=put_json(json2,'HOST_MS','127.0.0.1:5010');
        json2:=put_json(json2,'__TIMEOUT_SERV_PXML__','32');
        json2:=put_json(json2,'DATA_JSON',encode_hex(j3::varchar));
        json2:=put_json(json2,'LARGO_JSON',(length(encode_hex(j3::varchar))/2)::varchar);
        return json2;
end;
$$
LANGUAGE plpgsql;

