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
	if (exists(select 1 from cola_motor_4 where uri=uri1 and categoria='ENVIO_BOLETA_SII')) then
		return 'YA_ENCOLADO';
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
		elsif get_xml('ESTADO_BOLETA',campo.data_dte)='RECHAZADO_POR_EL_SII' and strpos(campo.mensaje_sii,'Error de Firma')>0 then
			json2:=logjson(json2,'Boleta con Falla Firma se reintenta cod='||cod1::varchar);
		elsif get_xml('ESTADO_BOLETA',campo.data_dte)='RECHAZADO_POR_EL_SII' and strpos(campo.mensaje_sii,'Error en Schema')>0 then
			json2:=logjson(json2,'Boleta con Error en Schema se reintenta cod='||cod1::varchar);
		elsif get_xml('ESTADO_BOLETA',campo.data_dte)='RECHAZADO_POR_EL_SII' and strpos(campo.mensaje_sii,'Error en Monto(HED-2-223)')>0 then
			json2:=logjson(json2,'Boleta con Error en Monto Neto debe ser mayor que cero se reintenta cod='||cod1::varchar);
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
	json2:=put_json(json2,'FOLIO',campo.folio::varchar);
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

CREATE OR REPLACE FUNCTION public.valida_respuesta_envio_boleta_sii_8010(character varying)
 RETURNS character varying
AS $$
DECLARE
        xml1        alias for $1;
        xml2    varchar;
        xml7    varchar;
        jresp   json;
        jaux    json;
        cod1    bigint;
        id1     bigint;
        nombre_tabla1   varchar;
        xml3    varchar;
        tabla_boleta1   varchar;
        fecha_cola1     timestamp;
BEGIN
        xml2:=xml1;
        xml2:=logapp(xml2,'RESPUESTA ENVIO SII='||get_campo('RESPUESTA',xml2));
        BEGIN
                jresp:=replace(split_part(get_campo('RESPUESTA',xml2),chr(10)||chr(10),2),chr(6),';')::json;
        EXCEPTION WHEN OTHERS THEN
                if is_number(get_campo('__ID_DTE__',xml2)) is false then
                        xml2:=logapp(xml2,'Vamos a Encolar el Envio');
                        xml2 := put_campo(xml2,'__SECUENCIAOK__','1632');
                        return xml2;
                end if;
                xml2:=logapp(xml2,'RESPUESTA no es un json '||split_part(get_campo('RESPUESTA',xml2),chr(10)||chr(10),2));
                xml2:=put_campo(xml2,'RESPUESTA','Status: 400 NK');
                xml2 := pivote_borrado_8010(xml2);
                return xml2;
        END;
        if get_json('CODIGO_RESPUESTA',jresp)<>'1' then
                if is_number(get_campo('__ID_DTE__',xml2)) is false then
                        xml2:=logapp(xml2,'Vamos a Encolar el Envio');
                        xml2 := put_campo(xml2,'__SECUENCIAOK__','1632');
                        return xml2;
                end if;
                xml2:=logapp(xml2,'Falla Envio Boleta');
                xml2:=put_campo(xml2,'RESPUESTA','Status: 400 NK');
                xml2 := pivote_borrado_8010(xml2);
                return xml2;
        end if;
        --jresp:=get_json('RESPUESTA',jresp);
        --Grabamos al ESI y actualizamos la boleta
        jresp:=put_json(jresp,'__FLUJO_ACTUAL__',get_campo('__FLUJO_ACTUAL__',xml2));
        jresp:=put_json(jresp,'__IDPROC__',get_campo('__IDPROC__',xml2));
        jaux:=graba_bitacora(jresp,'ESI');
        xml2:=logapp(xml2,get_json('_LOG_',jaux));
        cod1:=get_campo('CODIGO_TXEL',xml2);
        --Temporal
        tabla_boleta1:=get_campo('TABLA_BOLETA',xml2);
        if tabla_boleta1='' then
                tabla_boleta1:='dte_boletas_generica';
        end if;
        execute 'update '||tabla_boleta1||' set data_dte=put_data_dte(put_data_dte(data_dte,''ESTADO_BOLETA'',''ENVIADO_AL_SII''),''TRACK_ID'','''||get_json('TRACK_ID',jresp)||'''),mensaje_sii='''||get_json('MSG_SII',jresp)||''' where codigo_txel='||cod1::varchar;
        --execute 'update '||tabla_boleta1||' set estado=''ENVIADO_AL_SII'',estado_sii=''ENVIADO_AL_SII'',mensaje_sii='''||get_json('MSG_SII',jresp)||''' where codigo_txel='||cod1::varchar and estado_sii<>''ENVIADO_AL_SII'';

        --Insertamos en la cola la consulta del estado...
        xml3:=put_campo('','CODIGO_TXEL',cod1::varchar);
        xml3:=put_campo(xml3,'URI_IN',get_campo('URI_IN',xml2));
        xml3:=put_campo(xml3,'FLAG_BOLETAS_MASIVAS',get_campo('FLAG_BOLETAS_MASIVAS',xml2));
        xml3:=put_campo(xml3,'RUT_EMISOR',get_campo('RUT_EMISOR',xml2));
        xml3:=put_campo(xml3,'TIPO_DTE',get_campo('TIPO_DTE',xml2));
        xml3:=put_campo(xml3,'FOLIO',get_campo('FOLIO',xml2));
        xml3:=put_campo(xml3,'RUT_RECEPTOR',get_campo('RUT_RECEPTOR',xml2));
        xml3:=put_campo(xml3,'TRACK_ID',get_json('TRACK_ID',jresp));
        xml3:=put_campo(xml3,'FECHA_RECEPCION',get_json('FECHA_RECEPCION',jresp));
        xml3:=put_campo(xml3,'TABLA_BOLETA',tabla_boleta1);
        --xml3:=put_campo(xml3,'TX','6001');
        xml3:=put_campo(xml3,'TX','6110');
        --xml3:=put_campo(xml3,'tipo_tx','consulta_trackid_boleta_6001');

        --Si es cliente Masivo, lo encolamos para la noche
        fecha_cola1:=now();
        /*
        if get_campo('FLAG_BOLETAS_MASIVAS',xml2)='SI' and to_char(now(),'HH24')::integer>7 then
                fecha_cola1:=to_char(now()+interval '1 day','YYYY-MM-DD 00:05:00')::timestamp;
        end if;*/

        --nombre_tabla1:='cola_sii_'||nextval('id_cola_sii')::varchar;
        --nombre_tabla1:='cola_motor_'||nextval('id_cola_procesamiento_colas')::varchar;
        nombre_tabla1:='cola_motor_'||nextval('id_cola_procesamiento_boleta')::varchar;
        --Consultas solo a la cola 5
        nombre_tabla1:='cola_motor_5';
        xml7:=put_campo('','TX','8060');
        xml7:=put_campo(xml7,'CATEGORIA','COLAS');
        xml7:=put_campo(xml7,'SUB_CATEGORIA','CONSULTA_TRACKID_BOLETA');
        xml7:=put_campo(xml7,'URI_IN',get_campo('URI_IN',xml2));
        xml7:=put_campo(xml7,'QUERY',encode_hex('insert into '||nombre_tabla1||' (fecha,uri,reintentos,data,tx,rut_emisor,reproceso,categoria,nombre_cola) values ( '''||fecha_cola1::varchar||'''::timestamp,'||quote_literal(get_campo('URI_IN',xml2))||',0,'||quote_literal(xml3)||','||'40'||','||quote_literal(get_campo('RUT_EMISOR',xml2))||',''NO'',''CONSULTA_TRACKID_BOLETA'','''||nombre_tabla1||''') returning id'));
        nombre_tabla1:='cola_motor_5';
        execute 'insert into '||nombre_tabla1||' (fecha,uri,reintentos,data,tx,rut_emisor,reproceso,categoria) values (now(),'||quote_literal(get_campo('URI_IN',xml2))||',0,'||quote_literal(xml7)||','||'10'||',null,''NO'',''ACT_REMOTO'') returning id' into id1;
        xml2:=logapp(xml2,'Inserto CONSULTA_TRACKID_BOLETA '||get_campo('URI_IN',xml2)||' id='||id1::varchar);

        if (get_campo('__DTE_CON_MANDATO__',xml2)='SI') then
                --Encolamos el mandato
                nombre_tabla1:='cola_motor_'||nextval('id_cola_procesamiento_colas')::varchar;
                xml7:=put_campo('','TX','8060');
                xml7:=put_campo(xml7,'CATEGORIA','COLAS');
                xml7:=put_campo(xml7,'URI_IN',get_campo('URI_IN',xml2));
                xml7:=put_campo(xml7,'QUERY',encode_hex('select sp_reprocesa_mandato2(''URI_IN[]='||get_campo('URI_IN',xml2)||'###'')'));
                nombre_tabla1:='cola_motor_'||nextval('id_cola_procesamiento')::varchar;
                execute 'insert into '||nombre_tabla1||' (fecha,uri,reintentos,data,tx,rut_emisor,reproceso,categoria) values (now(),'||quote_literal(get_campo('URI_IN',xml2))||',0,'||quote_literal(xml7)||','||'10'||',null,''NO'',''ACT_REMOTO'') returning id' into id1;
                xml2:=logapp(xml2,'Inserto MANDATO_BOLETA_SI '||get_campo('URI_IN',xml2)||' id='||id1::varchar);
        end if;
        xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
        xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
        xml2 := pivote_borrado_8010(xml2);
        --xml2 := proc_procesa_respuesta_dte_8010(xml2);
        return xml2;
end;
$$
LANGUAGE plpgsql;

