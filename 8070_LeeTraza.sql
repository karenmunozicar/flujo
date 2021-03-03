--Publica documento
delete from isys_querys_tx where llave='8070';

insert into isys_querys_tx values ('8070',10,19,1,'select pivote_lee_traza_8070(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);
--Traza 2014
insert into isys_querys_tx values ('8070',2014,38,1,'$$QUERY_DATA$$',0,0,0,9,1,1000,1000);
--Traza 2015
insert into isys_querys_tx values ('8070',2015,37,1,'$$QUERY_DATA$$',0,0,0,9,1,1000,1000);
--Traza 2016
insert into isys_querys_tx values ('8070',2016,36,1,'$$QUERY_DATA$$',0,0,0,9,1,1000,1000);
--Traza 2017
insert into isys_querys_tx values ('8070',2017,33,1,'$$QUERY_DATA$$',0,0,0,9,1,1000,1000);
--Traza 2018
insert into isys_querys_tx values ('8070',2018,46,1,'$$QUERY_DATA$$',0,0,0,9,1,1000,1000);
--Traza 2019
insert into isys_querys_tx values ('8070',2019,49,1,'$$QUERY_DATA$$',0,0,0,9,1,1000,1000);
--Traza 2020
insert into isys_querys_tx values ('8070',2020,50,1,'$$QUERY_DATA$$',0,0,0,9,1,1000,1000);
--Traza 2021
insert into isys_querys_tx values ('8070',2021,2021,1,'$$QUERY_DATA$$',0,0,0,9,1,1000,1000);
--Local
insert into isys_querys_tx values ('8070',4000,2021,1,'$$QUERY_DATA$$',0,0,0,9,1,4010,4010);
insert into isys_querys_tx values ('8070',4010,1,14,'{"f":"INSERTA_JSON","p1":{"JSON_RDS":$$RES_JSON_1$$}}',0,0,0,0,0,4020,4020);
insert into isys_querys_tx values ('8070',4020,8021,1,'$$QUERY_DATA$$',0,0,0,9,1,1000,1000);

insert into isys_querys_tx values ('8070',1000,19,1,'select respuesta_lee_traza_8070(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

CREATE or replace FUNCTION respuesta_lee_traza_8070(json) RETURNS json AS $$
DECLARE
        json1   alias for $1;
        json2   json;
        campo   record;
        per1    varchar;
	json_rds_actual1	varchar;
BEGIN
        json2:=json1;
        json2:=put_json(json2,'__SECUENCIAOK__','0');
	json_rds_actual1:=get_json('JSON_RDS',json2);
	if is_json_dict(json_rds_actual1) is false then
		json_rds_actual1:='';
	elsif get_json('STATUS',json_rds_actual1::json)='SIN_DATA' then
		json_rds_actual1:='';
	else
		if get_json('array_to_json',json_rds_actual1::json)='' then
			json_rds_actual1:='';
		else
			json_rds_actual1:=get_json('array_to_json',json_rds_actual1::json);
		end if;
	end if;
	if get_json('RES_JSON_1',json2)='' or is_json_dict(get_json('RES_JSON_1',json2)) is false then
		json2:=put_json(json2,'MSG_LEE_TRAZA','Falla Query '||get_json('RES_JSON_1',json2));
		json2:=put_json(json2,'STATUS_LEE_TRAZA','NK');
        elsif get_json('STATUS',get_json('RES_JSON_1',json2)::json)='SIN_DATA' then
		json2:=put_json(json2,'MSG_LEE_TRAZA','Sin data');
                json2:=put_json(json2,'STATUS_LEE_TRAZA','OK');
		json2:=put_json(json2,'RESPUESTA_LEE_TRAZA','[]');
		if json_rds_actual1<>'' then
			json2:=put_json(json2,'MSG_LEE_TRAZA','OK');
			json2:=put_json(json2,'RESPUESTA_LEE_TRAZA',json_rds_actual1);
		end if;
        elsif get_json('STATUS',get_json('RES_JSON_1',json2)::json)='OK' then
		if get_json('array_to_json',get_json('RES_JSON_1',json2)::json)='' then
			json2:=put_json(json2,'MSG_LEE_TRAZA','Sin data.');
			json2:=put_json(json2,'STATUS_LEE_TRAZA','OK');
			json2:=put_json(json2,'RESPUESTA_LEE_TRAZA','[]');
			if json_rds_actual1<>'' then
				json2:=put_json(json2,'MSG_LEE_TRAZA','OK');
				json2:=put_json(json2,'RESPUESTA_LEE_TRAZA',json_rds_actual1);
			end if;
		else
			json2:=put_json(json2,'STATUS_LEE_TRAZA','OK');
			json2:=put_json(json2,'MSG_LEE_TRAZA','OK');
			if json_rds_actual1<>'' then
				json2:=logjson(json2,'FLUJO_8070 Merge');
				json2:=put_json(json2,'RESPUESTA_LEE_TRAZA',json_merge_lists(json_rds_actual1,get_json('array_to_json',get_json('RES_JSON_1',json2)::json)));
			else
				json2:=put_json(json2,'RESPUESTA_LEE_TRAZA',get_json('array_to_json',get_json('RES_JSON_1',json2)::json));
			end if;
		end if;
        else
		json2:=put_json(json2,'MSG_LEE_TRAZA','Repuesta Fallida');
		json2:=put_json(json2,'STATUS_LEE_TRAZA','NK');
        end if;
	json2:=logjson(json2,'STATUS_LEE_TRAZA='||get_json('STATUS_LEE_TRAZA',json2)||' RESPUESTA_LEE_TRAZA='||substring(get_json('RESPUESTA_LEE_TRAZA',json2),1,1024));
	json2:=put_json(json2,'RESPUESTA','Status: 400 NK');

	return json2;

END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION pivote_lee_traza_8070(json) RETURNS json AS $$
DECLARE
	json1	alias for $1;
	json2	json;
	campo	record;
	per1	varchar;
	query1	varchar;
BEGIN
	json2:=json1;
	per1:=get_fecha_uri(get_json('URI_IN',json2));
	json2:=logjson(json2,'per1='||per1||' URI='||get_json('URI_IN',json2));
	if per1='0' then
		json2:=logjson(json2,'URI_IN Invalida');
		json2:=put_json(json2,'MSG_LEE_TRAZA','URI_IN Invalida');
		json2:=put_json(json2,'STATUS_LEE_TRAZA','NK');
		json2:=put_json(json2,'__SECUENCIAOK__','0');
		return json2;
	end if;
	select * into campo from config_tabla_traza where per1::integer>=periodo_desde and per1::integer<=periodo_hasta;
	if not found then
		json2:=logjson(json2,'Falla config_tabla_traza');
		json2:=put_json(json2,'MSG_LEE_TRAZA','Falla config_tabla_traza');
                json2:=put_json(json2,'STATUS_LEE_TRAZA','NK');
                json2:=put_json(json2,'__SECUENCIAOK__','0');
                return json2;
	end if;
	if get_json('FILTRO_LEE_TRAZA_HEX',json2)<>'' then
		query1:='select array_to_json(array_agg(row_to_json(sql))) from ('||replace(replace(campo.funcion,'##URI##',get_json('URI_IN',json2)),'##PERIODO##',per1)||' and '||decode_hex(get_json('FILTRO_LEE_TRAZA_HEX',json2))||') sql';
	else
		query1:='select array_to_json(array_agg(row_to_json(sql))) from ('||replace(replace(campo.funcion,'##URI##',get_json('URI_IN',json2)),'##PERIODO##',per1)||') sql';
	end if;
	--Ya q la base rds cambia el timestamp y le pone una "T", casteamos a varchar lass fechas
	query1:=replace(query1,'select * from ','select fecha::varchar,folio,tipo_dte,rut_emisor,rut_receptor,canal,evento,uri,comentario1,comentario2,url_get,codigo_txel,veces,fecha_actualizacion::varchar,fecha_emision::varchar,fecha_ingreso::varchar,fecha_despacho_erp::varchar,comentario_erp from ');
	json2:=put_json(json2,'QUERY_DATA',query1);
	--Para ciertos eventos q solo queremos leer en aws. Ej: EMA
	if get_json('SOLO_TRAZA_AWS',json2)='SI' then
		json2:=logjson(json2,'QUERY='||query1);
		json2:=put_json(json2,'__SECUENCIAOK__','2021');
	--Si es local...
	elsif campo.parametro is null then
		json2:=logjson(json2,'QUERY='||query1);
		json2:=put_json(json2,'__SECUENCIAOK__','4000');
	else
		json2:=logjson(json2,'QUERY='||query1||' __SECUENCIAOK__='||split_part(campo.parametro,'_',2));
		json2:=put_json(json2,'__SECUENCIAOK__',split_part(campo.parametro,'_',2));
	end if;
	return json2;
END;
$$ LANGUAGE plpgsql;

