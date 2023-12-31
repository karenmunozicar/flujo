delete from isys_querys_tx where llave='8060';

--insert into isys_querys_tx values ('8060',5,19,1,'select control_flujo_80101(''$$__JSONCOMPLETO__["__PROC_ACTIVOS__","TX","REQUEST_URI","__ARGV__","__CATEGORIA_COLA__","__FLUJO_ACTUAL__"]$$''::json) as __json__',0,0,0,1,1,-1,10);
--Responde Acelerado si ir a ninguna base
insert into isys_querys_tx values ('8060',5,1,14,'{"f":"INSERTA_JSON","p1":{"__SECUENCIAOK__":"10","__SOCKET_RESPONSE__":"RESPUESTA","__TIPO_SOCKET_RESPONSE__":"SCGI","RESPUESTA":"Status: 555 OK\nContent-Type: text/plain\n\n{\"STATUS\":\"Responde sin Espera\",\"__PROC_ACTIVOS__\":\"$$__PROC_ACTIVOS__$$\"}"}}',0,0,0,0,0,10,10);

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
--Traza 2020
insert into isys_querys_tx values ('8060','63',50,1,'$$QUERY_DATA$$',0,0,0,9,1,900,900);
--Traza 2021 que se borra en motor7
insert into isys_querys_tx values ('8060','64',2021,1,'$$QUERY_DATA$$',0,0,0,9,1,900,900);
--Traza 2021 que se borra en las colas
insert into isys_querys_tx values ('8060','65',2021,1,'$$QUERY_DATA$$',0,0,0,9,1,910,910);

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
insert into isys_querys_tx values ('8060','110',19,1,'$$QUERY_DATA$$',0,0,0,9,1,910,910);
insert into isys_querys_tx values ('8060','113',1913,1,'$$QUERY_DATA$$',0,0,0,9,1,910,910);
insert into isys_querys_tx values ('8060','114',1914,1,'$$QUERY_DATA$$',0,0,0,9,1,910,910);

--Base MOTOR ahora por api motor edr
insert into isys_querys_tx values ('8060','120',8022,1,'$$QUERY_DATA$$',0,0,0,9,1,910,910);
--Categoria MOTOR_MOTOR
insert into isys_querys_tx values ('8060','122',8022,1,'$$QUERY_DATA$$',0,0,0,9,1,900,900);

--Borra sobre base motor usando base 8021 api motor edr
insert into isys_querys_tx values ('8060','900',8022,1,'select valida_respuesta_insert_8060(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,1000);
insert into isys_querys_tx values ('8060','910',19,1,'select valida_respuesta_insert_8060_colas(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,1010);
--insert into isys_querys_tx values ('8060',1000,8022,1,'select sp_procesa_respuesta_cola_motor_original_json(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,0,0);
insert into isys_querys_tx values ('8060',1000,8022,1,'select sp_procesa_respuesta_cola_motor_original_json(''$$__JSONCOMPLETO__["__ID_DTE__","__COLA_MOTOR__","CODIGO_TXEL","RESPUESTA","MENSAJE_XML_FLAGS","__FECHA_FUTURO_COLA__"]$$''::json) as __json__',0,0,0,1,1,0,0);
--insert into isys_querys_tx values ('8060',1010,19,1,'select sp_procesa_respuesta_cola_motor_original_json(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,0,0);
insert into isys_querys_tx values ('8060',1010,19,1,'select sp_procesa_respuesta_cola_motor_original_json(''$$__JSONCOMPLETO__["__ID_DTE__","__COLA_MOTOR__","CODIGO_TXEL","RESPUESTA","MENSAJE_XML_FLAGS","__FECHA_FUTURO_COLA__"]$$''::json) as __json__',0,0,0,1,1,0,0);
--Borrado en Base Traza
insert into isys_querys_tx values ('8060',1020,2021,1,'select sp_procesa_respuesta_cola_motor_original_json(''$$__JSONCOMPLETO__["__ID_DTE__","__COLA_MOTOR__","CODIGO_TXEL","RESPUESTA","MENSAJE_XML_FLAGS","__FECHA_FUTURO_COLA__"]$$''::json) as __json__',0,0,0,1,1,0,0);


CREATE or replace FUNCTION pivote_borrado_8060(json) RETURNS json AS $$
DECLARE
	json1	alias for $1;
	json2	json;
BEGIN
	json2:=json1;
        json2:=logjson(json2,'BD_ORIGEN='||get_json('_CATEGORIA_BD_',json2));
        if(get_json('_CATEGORIA_BD_',json2)='COLAS')then
		json2 := put_json(json2,'__SECUENCIAOK__','1010');
        elsif(get_json('_CATEGORIA_BD_',json2)='TRAZA')then
		json2 := put_json(json2,'__SECUENCIAOK__','1020');
        else
		json2 := put_json(json2,'__SECUENCIAOK__','1000');
        end if;
        return json2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION valida_respuesta_insert_8060_colas(json) RETURNS json AS $$
DECLARE
        json1    alias for $1;
        json2  json;
        pg_context      varchar;
BEGIN
        json2:=json1;
        json2 := put_json(json2,'__SECUENCIAOK__','0');
	--Si falla la conexion a la base...
	if get_json('__STS_ERROR_SOCKET__',json2)='FALLA_CONEXION_BD' then
		json2:=logjson(json2,'Falla Conexion BD');
		json2:=put_json(json2,'__CATEGORIA_COLA__',get_json('__CATEGORIA_COLA__',json2)||'_FALLA_CONEXION_BD');
        elsif get_json('RES_JSON_1',json2)='' or is_json_dict(get_json('RES_JSON_1',json2)) is false then
                json2:=logjson(json2,'Falla Query '||get_json('RES_JSON_1',json2));
                json2:=put_json(json2,'RESPUESTA','Status: 400 NK');
        elsif get_json('STATUS',get_json('RES_JSON_1',json2)::json)='OK' then
                json2:=logjson(json2,'Query OK '||get_json('CATEGORIA',json2)||' '||get_json('RES_JSON_1',json2)||' '||get_json('SUB_CATEGORIA',json2));
                json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
	--Si responde SIN_DATA y es un update lo borramos
	elsif get_json('STATUS',get_json('RES_JSON_1',json2)::json)='SIN_DATA' and lower(substring(trim(get_json('QUERY_DATA',json2)),1,7))='update ' then
		json2:=logjson(json2,'Update OK '||get_json('CATEGORIA',json2));
		json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
        else
                json2:=logjson(json2,'Respuesta Fallida '||get_json('RES_JSON_1',json2));
                json2:=put_json(json2,'RESPUESTA','Status: 444 NK');
        end if;
	json2:=pivote_borrado_8060(json2);
	--Para que no devuelva la query nuevamente
	json2:=put_json(json2,'QUERY_DATA','');
	json2:=put_json(json2,'RES_JSON_1','');
	--Si va a las colas nos ahorramos un llamado
	if get_json('__SECUENCIAOK__',json2)='1010' then
		json2 := put_json(json2,'__SECUENCIAOK__','0');
		return sp_procesa_respuesta_cola_motor_original_json(json2);
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
        if get_json('RES_JSON_1',json2)='' or is_json_dict(get_json('RES_JSON_1',json2)) is false then
                json2:=logjson(json2,'Falla Query '||get_json('RES_JSON_1',json2));
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
	--Para que no devuelva la query nuevamente
	json2:=pivote_borrado_8060(json2);
        json2:=put_json(json2,'QUERY_DATA','');
        json2:=put_json(json2,'RES_JSON_1','');
        --Si va a las colas nos ahorramos un llamado
        if get_json('__SECUENCIAOK__',json2)='1000' then
                json2 := put_json(json2,'__SECUENCIAOK__','0');
                return sp_procesa_respuesta_cola_motor_original_json(json2);
        end if;
	return json2;
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
	j1		json;
	id1		bigint;
BEGIN
        xml2:=xml1;
	json2:='{}';
	json2:=put_json(json2,'CATEGORIA',get_campo('CATEGORIA',xml2));
	json2:=put_json(json2,'PARAMETRO',get_campo('PARAMETRO',xml2));
	json2:=put_json(json2,'_CATEGORIA_BD_',get_campo('_CATEGORIA_BD_',xml2));
	json2:=put_json(json2,'__FLUJO_ACTUAL__',get_campo('__FLUJO_ACTUAL__',xml2));
	json2:=put_json(json2,'__IDPROC__',get_campo('__IDPROC__',xml2));
	--Si tengo sub categoria se la agrego a la __CATEGORIA_COLA__
	if get_campo('SUB_CATEGORIA',xml2)<>'' then
		json2:=put_json(json2,'__CATEGORIA_COLA__',get_campo('__CATEGORIA_COLA__',xml2)||'_'||get_campo('SUB_CATEGORIA',xml2));
	end if;
	categoria1:=get_campo('CATEGORIA',xml2);
	if categoria1 in ('DOCS_RELACIONADOS','TRAZA_REMOTA') then
		json2:=put_json(json2,'__TOTAL_RESP_ESPERADAS__','1');
		parametro1:=get_campo('PARAMETRO',xml2);
		json2:=logjson(json2,'Ejecuta Remoto '||categoria1||' '||parametro1||' '||get_campo('URI_IN',xml2));
		query1:=decode_hex(get_campo('QUERY',xml2));
		--parche
		if (parametro1 in ('TRAZA_2021','TRAZA_2020')) then
			query1:=replace(query1,'Sender'||chr(39)||'s','Sender'||chr(39)||chr(39)||'s');
			query1:=replace(query1,'recipient'||chr(39)||'s','recipient'||chr(39)||chr(39)||'s');
			query1:=replace(query1,'domain'||chr(39)||'s','domain'||chr(39)||chr(39)||'s');
			query1:=replace(query1,'won'||chr(39)||'t','won'||chr(39)||chr(39)||'t');
			json2:=logjson(json2,'recipient '||query1);
		end if;
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
		elsif (parametro1='TRAZA_2020') then
			json2:=put_json(json2,'__SECUENCIAOK__','63');
		elsif (parametro1='TRAZA_2021') then
			--FAY si el origen es de las colas vamos a la secuencia 65 para que vaya a borrar a las colas y no pase por la sec 900 que va a motor7
        		if(get_json('_CATEGORIA_BD_',json2)='COLAS')then
				json2:=put_json(json2,'__SECUENCIAOK__','65');
			else
				json2:=put_json(json2,'__SECUENCIAOK__','64');
			end if;
		else
			--Parche xq grabamos unos eventos sin URI que no correspondian
			if get_campo('URI_IN',xml2)='' or strpos(get_campo('URI_IN',xml2),'http')=0 then
				json2:=logjson(json2,'Traza no definida '||parametro1||' SIN URI');
				json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
				--json2:=put_json(json2,'__SECUENCIAOK__','1000');
				json2:=pivote_borrado_8060(json2);
				return json2;
			end if;
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
	elsif categoria1 in ('MOTOR') then
		json2:=put_json(json2,'__TOTAL_RESP_ESPERADAS__','1');
                json2:=logjson(json2,'Ejecuta Remoto '||categoria1);
                query1:=decode_hex(get_campo('QUERY',xml2));
                xml2:=put_campo(xml2,'QUERY_DATA',query1);
                json2:=put_json(json2,'QUERY_DATA',query1);
                json2:=put_json(json2,'__CATEGORIA__',categoria1);
                json2:=logjson(json2,'Ejecuta Remoto = '||query1);
		json2:=put_json(json2,'__SECUENCIAOK__','120');
	elsif categoria1 in ('MOTOR_MOTOR') then
		json2:=put_json(json2,'__TOTAL_RESP_ESPERADAS__','1');
                json2:=logjson(json2,'Ejecuta Remoto '||categoria1);
                query1:=decode_hex(get_campo('QUERY',xml2));
                xml2:=put_campo(xml2,'QUERY_DATA',query1);
                json2:=put_json(json2,'QUERY_DATA',query1);
                json2:=put_json(json2,'__CATEGORIA__',categoria1);
                json2:=logjson(json2,'Ejecuta Remoto = '||query1);
		json2:=put_json(json2,'__SECUENCIAOK__','122');
	elsif categoria1 in ('COLAS_14') then
		json2:=put_json(json2,'__TOTAL_RESP_ESPERADAS__','1');
		json2:=logjson(json2,'Ejecuta Remoto '||categoria1);
		query1:=decode_hex(get_campo('QUERY',xml2));
		xml2:=put_campo(xml2,'QUERY_DATA',query1);
		json2:=put_json(json2,'QUERY_DATA',query1);
		json2:=put_json(json2,'__CATEGORIA__',categoria1);
		json2:=logjson(json2,'Ejecuta Remoto = '||query1);
		json2:=put_json(json2,'__SECUENCIAOK__','114');
	elsif categoria1 in ('COLAS') then
		json2:=put_json(json2,'__TOTAL_RESP_ESPERADAS__','1');
		json2:=logjson(json2,'Ejecuta Remoto '||categoria1);
		query1:=decode_hex(get_campo('QUERY',xml2));
		if get_campo('SUB_CATEGORIA',xml2)='COPIA_GRABA_BITACORA3_COLAS' then
			--Seteamos la tabla segun el parametro, para poder hacer mantencion
			if strpos(query1,'##TABLA##')>0 then
				j1:=get_parametros_motor_json('{}','NOMBRE_COLA_COPIA_GRABA_BITACORA3_COLAS');
				json2:=logjson(json2,'Reemplazamos ##TABLA## '||get_json('PARAMETRO_RUTA',j1));
				query1:=replace(query1,'##TABLA##',get_json('PARAMETRO_RUTA',j1));
			elsif strpos(query1,' cola_motor_11 ')>0 then
				j1:=get_parametros_motor_json('{}','NOMBRE_COLA_COPIA_GRABA_BITACORA3_COLAS');
				json2:=logjson(json2,'Reemplazamos ##TABLA## '||get_json('PARAMETRO_RUTA',j1));
				query1:=replace(query1,'cola_motor_11',get_json('PARAMETRO_RUTA',j1));
			end if;	
			json2:=logjson(json2,'Ejecuta Remoto = '||query1);
			--Ejecutamos aqui mismo ya que es la misma base
			execute query1 into id1;
			--Armamos la respuesta
			j1:=put_json('{}','id',id1::varchar);
			j1:=put_json(j1,'STATUS','OK');
			j1:=put_json(j1,'TOTAL_REGISTROS','1');
			json2:=put_json(json2,'RES_JSON_1',j1::varchar);
			return valida_respuesta_insert_8060_colas(json2);	
		end if;
		xml2:=put_campo(xml2,'QUERY_DATA',query1);
		json2:=put_json(json2,'QUERY_DATA',query1);
		json2:=put_json(json2,'__CATEGORIA__',categoria1);
		json2:=logjson(json2,'Ejecuta Remoto = '||query1);
		--Ahora que tenemos ProcesaColasMotor132 en ambas maquinas no es necesario el serial
		json2:=put_json(json2,'__SECUENCIAOK__','110');
		/*
		if nextval('alterna_colas')=13 then
			json2:=put_json(json2,'__SECUENCIAOK__','113');
		else
			json2:=put_json(json2,'__SECUENCIAOK__','114');
		end if;*/
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
