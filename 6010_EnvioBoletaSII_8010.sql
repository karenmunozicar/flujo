delete from isys_querys_tx where llave='6010';

--insert into isys_querys_tx values ('6010',5,19,1,'select control_flujo_80101(''$$__JSONCOMPLETO__["__PROC_ACTIVOS__","TX","REQUEST_URI","__ARGV__","__CATEGORIA_COLA__","__FLUJO_ACTUAL__"]$$''::json) as __json__',0,0,0,1,1,-1,10);
insert into isys_querys_tx values ('6010',2,9,16,'["__ID_DTE__"]',0,0,0,1,1,5,5);
insert into isys_querys_tx values ('6010',5,1,14,'{"f":"INSERTA_JSON","p1":{"__SECUENCIAOK__":"10","__SOCKET_RESPONSE__":"RESPUESTA","__TIPO_SOCKET_RESPONSE__":"SCGI","RESPUESTA":"Status: 555 OK\nContent-Type: text/plain\n\n{\"STATUS\":\"Responde sin Espera\",\"__PROC_ACTIVOS__\":\"$$__PROC_ACTIVOS__$$\"}"}}',0,0,0,0,0,10,10);

insert into isys_querys_tx values ('6010',10,19,1,'select envio_boleta_6010(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,1000);
insert into isys_querys_tx values ('6010',22,1,2,'Microservicioe 127.0.0.1',4013,300,101,0,0,25,25);
insert into isys_querys_tx values ('6010',25,19,1,'select valida_respuesta_envio_boleta_sii_6010(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,30);
insert into isys_querys_tx values ('6010',30,19,1,'select valida_respuesta_envio_boleta_sii_error_6010(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,1000);
insert into isys_querys_tx values ('6010',1000,19,1,'select sp_procesa_respuesta_cola_motor_original(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,0,0);

--Para reproceso un sp que reinserta y encola la consulta por trackid
CREATE or replace FUNCTION repro_consulta_estado_boleta_sii(varchar,varchar,varchar) RETURNS varchar AS $$
DECLARE
	rut1	alias for $1;
	tipo1	alias for $2;
	folio1	alias for $3;
	campo	record;
	xml3	varchar;
	json_par1	json;
	json_aux	json;
BEGIN
	--Leemos desde la tabla
	select * into campo from dte_boletas_generica where rut_emisor=rut1::bigint and tipo_dte=tipo1::integer and folio=folio1::bigint;
	if not found then
		return 'BOLETA_NO_ENCOTRADA';
	end if;
        --Insertamos en la cola la consulta del estado...
        xml3:=put_campo('','CODIGO_TXEL',campo.codigo_txel::varchar);
        xml3:=put_campo(xml3,'URI_IN',campo.uri);
        xml3:=put_campo(xml3,'FLAG_BOLETAS_MASIVAS','NO');
        xml3:=put_campo(xml3,'RUT_EMISOR',rut1);
        xml3:=put_campo(xml3,'TIPO_DTE',tipo1);
        xml3:=put_campo(xml3,'RUT_RECEPTOR',campo.rut_receptor::varchar);
        xml3:=put_campo(xml3,'TRACK_ID',get_xml('TRACK_ID',campo.data_dte));
        xml3:=put_campo(xml3,'TABLA_BOLETA','dte_boletas_generica');
        xml3:=put_campo(xml3,'TX','6110');

	--Consultas solo a la cola 5
	json_par1:=get_parametros_motor_json('{}','BASE_COLAS');
	json_aux:=query_db_json(get_json('__IP_CONEXION_CLIENTE__',json_par1),get_json_int('__IP_PORT_CLIENTE__',json_par1)::integer,'insert into cola_motor_5 (fecha,uri,reintentos,data,tx,rut_emisor,reproceso,categoria,nombre_cola) values ( now(),'||quote_literal(campo.uri)||',0,'||quote_literal(xml3)||','||'40'||','||quote_literal(get_campo('RUT_EMISOR',xml3))||',''NO'',''CONSULTA_TRACKID_BOLETA'',''cola_motor_5'') returning id');
	return json_aux::varchar;
END;
$$ LANGUAGE plpgsql;



CREATE or replace FUNCTION valida_respuesta_envio_boleta_sii_error_6010(varchar) RETURNS varchar AS $$
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
                jresp:=split_part(get_campo('RESPUESTA',xml2),chr(10)||chr(10),2)::json;
        EXCEPTION WHEN OTHERS THEN
                xml2:=put_campo(xml2,'RESPUESTA','Status: 400 NK');
		xml2:=put_campo(xml2,'__SECUENCIAOK__','1000');
                return xml2;
        END;
        if get_json('CODIGO_RESPUESTA',jresp)<>'1' then
                xml2:=logapp(xml2,'Falla Envio Boleta');
                xml2:=put_campo(xml2,'RESPUESTA','Status: 400 NK');
		xml2:=put_campo(xml2,'__SECUENCIAOK__','1000');
                return xml2;
        end if;
	--Actualizamos en las colas en RESPUESTA_ENVIO_SII para no ir al SII de nuevo
	execute 'update '||get_campo('__COLA_MOTOR__',xml2)||' set data=put_campo(put_campo(data,''RESPUESTA_ENVIO_SII'','''||replace(get_campo('RESPUESTA',xml2),chr(39),'')||'''),''TABLA_BOLETA'','''||get_campo('TABLA_BOLETA',xml2)||''') where id='||get_campo('__ID_DTE__',xml2);	
	xml2:=put_campo(xml2,'RESPUESTA','Status: 400 NK');
	xml2:=put_campo(xml2,'__SECUENCIAOK__','1000');
	return xml2;
END;
$$ LANGUAGE plpgsql;


--Funcion que recibe un json y se encola en para ser aplicada a motor7
CREATE or replace FUNCTION actualiza_esi_boleta_6010(json) RETURNS json AS $$
DECLARE
	json1	alias for $1;
	json2	json;
	cod1	varchar;
	tabla_boleta1	varchar;
BEGIN
	json2:=json1;
        json2:=graba_bitacora(json2,'ESI');
        cod1:=get_json('CODIGO_TXEL',json2);
	--Temporal
	tabla_boleta1:=get_json('TABLA_BOLETA',json2);
	if tabla_boleta1='' then
		tabla_boleta1:='dte_boletas_generica';
	end if;
	execute 'update '||tabla_boleta1||' set data_dte=put_data_dte(put_data_dte(data_dte,''ESTADO_BOLETA'',''ENVIADO_AL_SII''),''TRACK_ID'','''||get_json('TRACK_ID',json2)||'''),mensaje_sii='''||get_json('MSG_SII',json2)||''' where codigo_txel='||cod1::varchar||' and get_xml(''ESTADO_BOLETA'',data_dte) not in (''ACEPTADO_POR_EL_SII'',''ACEPTADO_CON_REPAROS_POR_EL_SII'',''RECHAZADO_POR_EL_SII'')';
	return json2;
END;
$$ LANGUAGE plpgsql;


--Se ejecuta en las colas y encola la act para motor7 de la boleta y la consulta del track id y el mandato si la boleta tiene mandato
CREATE or replace FUNCTION valida_respuesta_envio_boleta_sii_6010(varchar) RETURNS varchar AS $$
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
	tabla_boleta1	varchar;
	fecha_cola1	timestamp;
BEGIN
        xml2:=xml1;
        xml2:=logapp(xml2,'RESPUESTA ENVIO SII='||get_campo('RESPUESTA',xml2));
        BEGIN
                jresp:=replace(split_part(get_campo('RESPUESTA',xml2),chr(10)||chr(10),2),chr(6),';')::json;
        EXCEPTION WHEN OTHERS THEN
		xml2:=logapp(xml2,'RESPUESTA no es un json '||split_part(get_campo('RESPUESTA',xml2),chr(10)||chr(10),2));
                xml2:=put_campo(xml2,'RESPUESTA','Status: 400 NK');
		xml2:=put_campo(xml2,'__SECUENCIAOK__','1000');
                return xml2;
        END;
        if get_json('CODIGO_RESPUESTA',jresp)<>'1' then
                xml2:=logapp(xml2,'Falla Envio Boleta');
                xml2:=put_campo(xml2,'RESPUESTA','Status: 400 NK');
		xml2:=put_campo(xml2,'__SECUENCIAOK__','1000');
                return xml2;
        end if;

	jresp:=put_json(jresp,'CODIGO_TXEL',get_campo('CODIGO_TXEL',xml2));
	jresp:=put_json(jresp,'TABLA_BOLETA',get_campo('TABLA_BOLETA',xml2));
	--Encolamos hacia motor7 la act de la boleta
        nombre_tabla1:='cola_motor_10';
	xml7:=put_campo('','TX','8060');
	xml7:=put_campo(xml7,'CATEGORIA','MOTOR');
	xml7:=put_campo(xml7,'SUB_CATEGORIA','ACT_ESI_BOLETA');
	xml7:=put_campo(xml7,'URI_IN',get_campo('URI_IN',xml2));
	xml7:=put_campo(xml7,'QUERY',encode_hex('select actualiza_esi_boleta_6010('''||jresp::varchar||''')'));
	execute 'insert into '||nombre_tabla1||' (fecha,uri,reintentos,data,tx,rut_emisor,reproceso,categoria) values (now(),'||quote_literal(get_campo('URI_IN',xml2))||',0,'||quote_literal(xml7)||','||'10'||',null,''NO'',''ACT_REMOTO'') returning id' into id1;
	xml2:=logapp(xml2,'Inserto ACT_ESI_BOLETA '||get_campo('URI_IN',xml2)||' id='||id1::varchar);

        --Insertamos en la cola la consulta del estado...
        xml3:=put_campo('','CODIGO_TXEL',get_campo('CODIGO_TXEL',xml2));
        xml3:=put_campo(xml3,'URI_IN',get_campo('URI_IN',xml2));
        xml3:=put_campo(xml3,'FLAG_BOLETAS_MASIVAS',get_campo('FLAG_BOLETAS_MASIVAS',xml2));
        xml3:=put_campo(xml3,'RUT_EMISOR',get_campo('RUT_EMISOR',xml2));
        xml3:=put_campo(xml3,'TIPO_DTE',get_campo('TIPO_DTE',xml2));
        xml3:=put_campo(xml3,'RUT_RECEPTOR',get_campo('RUT_RECEPTOR',xml2));
        xml3:=put_campo(xml3,'TRACK_ID',get_json('TRACK_ID',jresp));
        xml3:=put_campo(xml3,'FECHA_RECEPCION',get_json('FECHA_RECEPCION',jresp));
        xml3:=put_campo(xml3,'TABLA_BOLETA',get_campo('TABLA_BOLETA',xml2));
        xml3:=put_campo(xml3,'TX','6110');

	--Si es cliente Masivo, lo encolamos para la noche
	fecha_cola1:=now();

	--Consultas solo a la cola 5
        nombre_tabla1:='cola_motor_5';
	execute 'insert into '||nombre_tabla1||' (fecha,uri,reintentos,data,tx,rut_emisor,reproceso,categoria,nombre_cola) values ( '''||fecha_cola1::varchar||'''::timestamp,'||quote_literal(get_campo('URI_IN',xml2))||',0,'||quote_literal(xml3)||','||'40'||','||quote_literal(get_campo('RUT_EMISOR',xml2))||',''NO'',''CONSULTA_TRACKID_BOLETA'','''||nombre_tabla1||''') returning id' into id1;
	xml2:=logapp(xml2,'Inserto CONSULTA_TRACKID_BOLETA '||get_campo('URI_IN',xml2)||' id='||id1::varchar);

        if (get_campo('__DTE_CON_MANDATO__',xml2)='SI') then
		--Encolamos el mandato
		execute 'select sp_reprocesa_mandato2(''URI_IN[]='||get_campo('URI_IN',xml2)||'###'')';
		xml2:=logapp(xml2,'Inserto MANDATO_BOLETA_SI '||get_campo('URI_IN',xml2));
        end if;
	xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
	xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
	xml2:=put_campo(xml2,'__SECUENCIAOK__','1000');
        return xml2;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION envio_boleta_6010(json)
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
        j3:=put_json('{}','URI_IN',get_json('URI_IN',json2));
        j3:=put_json(j3,'INPUT_CUSTODIUM',get_json('INPUT',json2));

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

