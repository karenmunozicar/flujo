delete from isys_querys_tx where llave='8060';

insert into isys_querys_tx values ('8060',10,19,1,'select inserta_remoto_8060(''$$__XMLCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,1000);

--Bases Traza
--Traza Antigua
insert into isys_querys_tx values ('8060','20',31,1,'$$QUERY_DATA$$',0,0,0,9,1,900,900);
--Traza 2017
insert into isys_querys_tx values ('8060','30',33,1,'$$QUERY_DATA$$',0,0,0,9,1,900,900);
--Traza 2016
insert into isys_querys_tx values ('8060','40',36,1,'$$QUERY_DATA$$',0,0,0,9,1,900,900);
--Traza 2015
insert into isys_querys_tx values ('8060','50',37,1,'$$QUERY_DATA$$',0,0,0,9,1,900,900);
--Traza 2014
insert into isys_querys_tx values ('8060','60',38,1,'$$QUERY_DATA$$',0,0,0,9,1,900,900);
--Traza 2018
insert into isys_querys_tx values ('8060','61',46,1,'$$QUERY_DATA$$',0,0,0,9,1,900,900);
--Traza 2019
insert into isys_querys_tx values ('8060','62',49,1,'$$QUERY_DATA$$',0,0,0,9,1,900,900);

--Base Boletas
--Boletas 2014
insert into isys_querys_tx values ('8060','70',17,1,'$$QUERY_DATA$$',0,0,0,9,1,900,900);
--Boletas Historicas
insert into isys_querys_tx values ('8060','80',39,1,'$$QUERY_DATA$$',0,0,0,9,1,900,900);
--Base Mordor DND
insert into isys_querys_tx values ('8060','90',41,1,'$$QUERY_DATA$$',0,0,0,9,1,900,900);
--Base Bitacora-Casilla Hist
insert into isys_querys_tx values ('8060','100',42,1,'$$QUERY_DATA$$',0,0,0,9,1,900,900);

--Colas de procesamiento, segun donde se ejecute el procesador de colas
insert into isys_querys_tx values ('8060','110',19,1,'$$QUERY_DATA$$',0,0,0,9,1,900,900);


insert into isys_querys_tx values ('8060','900',1,1,'select valida_respuesta_insert_8060(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,1000);
insert into isys_querys_tx values ('8060',1000,1,1,'select sp_procesa_respuesta_cola_motor_original_json(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,0,0);
insert into isys_querys_tx values ('8060',1010,19,1,'select sp_procesa_respuesta_cola_motor_original_json(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,0,0);


CREATE or replace FUNCTION pivote_borrado_8060(json) RETURNS json AS $$
DECLARE
	json1	alias for $1;
	json2	json;
BEGIN
	json2:=json1;
        json2:=logjson(json2,'BD_ORIGEN='||get_json('_CATEGORIA_BD_',json2));
        if(get_json('_CATEGORIA_BD_',json2)='COLAS')then
		json2 := put_json(json2,'__SECUENCIAOK__','1010');
        else
		json2 := put_json(json2,'__SECUENCIAOK__','1000');
        end if;
        return json2;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION valida_respuesta_insert_8060(json) RETURNS json AS $$
DECLARE
        json1    alias for $1;
        json2  json;
	pg_context	varchar;
BEGIN
        json2:=json1;
	json2 := put_json(json2,'__SECUENCIAOK__','0');
	if get_json('RES_JSON_1',json2)='' then
		json2:=logjson(json2,'Falla Query');
		json2:=put_json(json2,'RESPUESTA','Status: 400 NK');
	elsif get_json('STATUS',get_json('RES_JSON_1',json2)::json)='OK' then
		--Chequeamos si viene funcion de respuesta
		if (get_json('FUNCION_RESPUESTA',json2)<>'') then
			json2:=logjson(json2,'Query OK Ejecuto '||get_json('FUNCION_RESPUESTA',json2));
			execute 'select '||get_json('FUNCION_RESPUESTA',json2)||'('''||get_json('PAR1',json2)||''','''||get_json('RES_JSON_1',json2)||'''::json,'''||json2::varchar||'''::json)' into json2;
		else
			json2:=logjson(json2,'Query OK '||get_json('RES_JSON_1',json2));
			json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
		end if;
	else
		json2:=logjson(json2,'Respuesta Fallida');
		json2:=put_json(json2,'RESPUESTA','Status: 444 NK');
	end if;
	return pivote_borrado_8060(json2);
	--return sp_procesa_respuesta_cola_motor_original_json(json2);
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION inserta_remoto_8060(varchar) RETURNS json AS $$
DECLARE
        xml1    alias for $1;
        xml2    varchar;
	categoria1	varchar;
	lista1	json;
	query1	varchar;
	parametro1	varchar;
	json2		json;
BEGIN
        xml2:=xml1;
	json2:='{}';
	json2:=put_json(json2,'CATEGORIA',get_campo('CATEGORIA',xml2));
	json2:=put_json(json2,'PARAMETRO',get_campo('PARAMETRO',xml2));
	json2:=put_json(json2,'__FLUJO_ACTUAL__',get_campo('__FLUJO_ACTUAL__',xml2));
	json2:=put_json(json2,'__IDPROC__',get_campo('__IDPROC__',xml2));
	categoria1:=get_campo('CATEGORIA',xml2);
	if categoria1 in ('DOCS_RELACIONADOS','TRAZA_REMOTA') then
		json2:=put_json(json2,'__TOTAL_RESP_ESPERADAS__','1');
		parametro1:=get_campo('PARAMETRO',xml2);
		json2:=logjson(json2,'Ejecuta Remoto '||categoria1||' '||parametro1||' '||get_campo('URI_IN',xml2));
		query1:=decode_hex(get_campo('QUERY',xml2));
		xml2:=put_campo(xml2,'QUERY_DATA',query1);
		json2:=put_json(json2,'QUERY_DATA',query1);
		if (parametro1='TRAZA_2014') then
			json2:=put_json(json2,'__SECUENCIAOK__','60');
		elsif (parametro1='TRAZA_2015') then
			json2:=put_json(json2,'__SECUENCIAOK__','50');
		elsif (parametro1='TRAZA_2016') then
			json2:=put_json(json2,'__SECUENCIAOK__','40');
		elsif (parametro1='TRAZA_2017') then
			json2:=put_json(json2,'__SECUENCIAOK__','30');
		elsif (parametro1='TRAZA_2018') then
			json2:=put_json(json2,'__SECUENCIAOK__','61');
		elsif (parametro1='TRAZA_2019') then
			json2:=put_json(json2,'__SECUENCIAOK__','62');
		else
			--Traza no definida
			json2:=logjson(json2,'Traza no definida '||parametro1);
			json2:=put_json(json2,'RESPUESTA','Status: 444 NK');
			--json2:=put_json(json2,'__SECUENCIAOK__','1000');
			json2:=pivote_borrado_8060(json2);

		end if;
	elsif categoria1 in ('BOLETA_AMAZON') then
		json2:=put_json(json2,'__TOTAL_RESP_ESPERADAS__','1');
		parametro1:=get_campo('PARAMETRO',xml2);
		json2:=logjson(json2,'Ejecuta Remoto '||categoria1||' '||parametro1||' '||get_campo('URI_IN',xml2));
		query1:=decode_hex(get_campo('QUERY',xml2));
		xml2:=put_campo(xml2,'QUERY_DATA',query1);
		json2:=put_json(json2,'QUERY_DATA',query1);
		if (parametro1='BASE_AMAZON_BOLETAS_2014') then
			json2:=put_json(json2,'__SECUENCIAOK__','70');
		elsif (parametro1='BASE_AMAZON_BOLETAS_HISTORICAS') then
			json2:=put_json(json2,'__SECUENCIAOK__','80');
		else
                        --Boleta no definida
                        json2:=logjson(json2,'Parametro boletas amazon no definida '||parametro1);
                        json2:=put_json(json2,'RESPUESTA','Status: 444 NK');
                        --json2:=put_json(json2,'__SECUENCIAOK__','1000');
			json2:=pivote_borrado_8060(json2);
                end if;
	elsif categoria1 in ('DND') then
		json2:=put_json(json2,'__TOTAL_RESP_ESPERADAS__','1');
		json2:=logjson(json2,'Ejecuta Remoto DND');
		query1:=decode_hex(get_campo('QUERY',xml2));
		xml2:=put_campo(xml2,'QUERY_DATA',query1);
		json2:=put_json(json2,'QUERY_DATA',query1);
		json2:=put_json(json2,'__CATEGORIA__',categoria1);
		json2:=logjson(json2,'Ejecuta Remoto = '||query1);
		json2:=put_json(json2,'__SECUENCIAOK__','90');
	elsif categoria1 in ('CD_HIST','BITACORA_HIST') then
		json2:=put_json(json2,'__TOTAL_RESP_ESPERADAS__','1');
		json2:=logjson(json2,'Ejecuta Remoto '||categoria1);
		query1:=decode_hex(get_campo('QUERY',xml2));
		xml2:=put_campo(xml2,'QUERY_DATA',query1);
		json2:=put_json(json2,'QUERY_DATA',query1);
		json2:=put_json(json2,'__CATEGORIA__',categoria1);
		json2:=logjson(json2,'Ejecuta Remoto = '||query1);
		json2:=put_json(json2,'__SECUENCIAOK__','100');
	elsif categoria1 in ('COLAS') then
		json2:=put_json(json2,'__TOTAL_RESP_ESPERADAS__','1');
		json2:=logjson(json2,'Ejecuta Remoto '||categoria1);
		query1:=decode_hex(get_campo('QUERY',xml2));
		xml2:=put_campo(xml2,'QUERY_DATA',query1);
		json2:=put_json(json2,'QUERY_DATA',query1);
		json2:=put_json(json2,'__CATEGORIA__',categoria1);
		json2:=logjson(json2,'Ejecuta Remoto = '||query1);
		json2:=put_json(json2,'__SECUENCIAOK__','110');
	else
		json2:=logjson(json2,'Categoria no reconocida '||categoria1);
		json2:=put_json(json2,'RESPUESTA','Status: 444 NK');
                --json2:=put_json(json2,'__SECUENCIAOK__','1000');
		json2:=pivote_borrado_8060(json2);
	end if;
	--perform logfile('inserta_remoto_8060 '||json2::varchar);
	return json2;
END;
$$ LANGUAGE plpgsql;
