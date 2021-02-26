delete from isys_querys_tx where llave='17101';
delete from isys_querys_tx where llave in ('17102','17103','17104','17105','17106','17107','17108','17109','17110','17111','17112','17113','17114','17115','17116','17117');

CREATE or replace FUNCTION arma_querys_paralelo_17101(json) RETURNS json AS $$
DECLARE
        json2   json;
        json1   alias for $1;
	i	integer;
BEGIN
	json2:=json1;
	--Si no es numerico falla
	if is_number(get_json('FOLIO',json2)) is false then
		json2:=logjson(json2,'Folio Solo Numerico');
		json2:=put_json(json2,'CODIGO_RESPUESTA','1');
		json2:=put_json(json2,'MENSAJE_RESPUESTA','Folio Solo Numerico');
		json2:=responde_pantalla_15100(json2);
		return json2;
	end if;
	json2:=put_json(json2,'SOLO_QUERY','SI');
	json2:=arma_filtros_15101(json2);
	json2:=put_json(json2,'QUERY_'||get_json('CAT_EST',json2),get_json('QUERY_DATA',json2));
	i:=0;
	while(i<20) loop
		json2:=logjson(json2,'CONTADOR_SECOK='||get_json('CONTADOR_SECOK',json2));
		json2:=pivote_15101(json2);
		json2:=logjson(json2,'OUT CONTADOR_SECOK='||get_json('CONTADOR_SECOK',json2));
		i:=i+1;
		--La ultima secuencia
		/*if get_json('CAT_EST',json2)='FOLIO_EMITIDOS_ERRORES_HIST' then
			json2:=logjson(json2,'FOLIO_EMITIDOS_ERRORES_HIST='||get_json('QUERY_DATA',json2));
		end if;*/
		json2:=logjson(json2,'CATEGORIA='||get_json('CAT_EST',json2)||' '||get_json('CONTADOR_SECOK',json2)||' '||get_json('rutUsuario',json2));
		if get_json('CONTADOR_SECOK',json2)='180' then
			--or get_json('QUERY_DATA',json2)='' then
			exit;
		end if;
		if trim(get_json('QUERY_DATA',json2))='' then
			json2:=logjson(json2,'Query vacia '||get_json('CAT_EST',json2));
			json2:=put_json(json2,'QUERY_'||get_json('CAT_EST',json2),'select '||get_json('CAT_EST',json2));
		else
			json2:=put_json(json2,'QUERY_'||get_json('CAT_EST',json2),get_json('QUERY_DATA',json2));
			/*if get_json('rutUsuario',json2)='17597643' then
				json2:=logjson(json2,'pg_sleep');
				json2:=put_json(json2,'QUERY_'||get_json('CAT_EST',json2),'select pg_sleep(20);');
			end if;*/
		end if;
	end loop;
	if get_json('busca_errores',json2)='NO' then
        	json2:=put_json(json2,'__SECUENCIAOK__','21');
	else
        	json2:=put_json(json2,'__SECUENCIAOK__','20');
	end if;
        return json2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION junta_respuestas_17101(json) RETURNS json AS $$
DECLARE
        json2   json;
        json1   alias for $1;
        i       integer;
	aux1	varchar;
	jbases	json;
	aux2	varchar;
	v_out_resultado	json;
	v_total	integer;
BEGIN
        json2:=json1;
	/*if get_json('rutUsuario',json2)='17597643' then
		perform logfile('select junta_respuestas_17101('''||json2::varchar||''');');
	end if;*/

	--json2:=logjson(json2,'junta_respuestas_17101='||json2::varchar);
	--Juntamos los resultados 
	jbases:='["QUERY_FOLIO_BOLETAS_2014","QUERY_FOLIO_BOLETAS_EMISION_DISTINTA_2016","QUERY_FOLIO_BOLETAS_EMISION_DISTINTA_2014","QUERY_FOLIO_BOLETAS_EMISION_DISTINTA","QUERY_FOLIO_BOLETAS_HISTORICAS","QUERY_FOLIO_BOLETAS_LOCAL","QUERY_FOLIO_BOLETAS_IMP_LOCAL","QUERY_FOLIO_BOLETAS_IMP_AWS","QUERY_FOLIO_EMITIDOS_HIST","QUERY_FOLIO_EMITIDOS_LOCAL","QUERY_FOLIO_EMITIDOS_IMP_LOCAL","QUERY_FOLIO_EMITIDOS_IMP_AWS","QUERY_FOLIO_EMITIDOS_ERRORES_HIST","QUERY_FOLIO_EMITIDOS_ERRORES_LOCAL","QUERY_FOLIO_EMITIDOS_COLAS_13","QUERY_FOLIO_EMITIDOS_COLAS_14"]';
	i:=0;
	aux1:=get_json_index(jbases,i);
	v_out_resultado='[]';
	while (aux1<>'')loop
		aux2:=get_json(replace(aux1,'QUERY_',''),json2);
		if aux2<>'' then
			if get_json('array_to_json',aux2::json)<>'' then
				if get_json('rutUsuario',json2)='17597643' then
					perform logfile('json_merge_lists_17101 CAT='||aux1::varchar||' '||substring(aux2::varchar,1,100));
					--||' aux2='||aux2::varchar);
					json2:=logjson(json2,'CAT='||aux1::varchar||' aux2='||aux2::varchar);
				end if;
				v_out_resultado:=json_merge_lists(v_out_resultado::varchar,get_json('array_to_json',aux2::json));
			else
				if get_json('rutUsuario',json2)='17597643' then
					perform logfile('json_merge_lists_17101 FALLA QUERY '||aux2::varchar);
				end if;
				json2:=logjson(json2,'FALLA QUERY '||aux1::varchar||aux2);
			end if;
		else
			if get_json('rutUsuario',json2)='17597643' then
				perform logfile('json_merge_lists_17101 FALLA QUERY1 '||aux1::varchar||aux2::varchar);
			end if;
			json2:=logjson(json2,'FALLA QUERY1 '||aux2);
		end if;
		i:=i+1;
		aux1:=get_json_index(jbases,i);
	end loop;
	if get_json('rutUsuario',json2)='17597643' then
		perform logfile('json_merge_lists_17101 FIN LOOP');
	end if;
	v_total:=count_array_json(v_out_resultado);
	--json2:=logjson(json2,'junta_respuestas_17101 v_out_resultado='||v_out_resultado::varchar||' v_total='||v_total::varchar);
	json2:=put_json(json2,'v_out_resultado',v_out_resultado::varchar);
	json2:=put_json(json2,'v_total_registros',v_total::varchar);
	json2:=put_json(json2,'CODIGO_RESPUESTA','1');
	json2:=responde_pantalla_15100(json2);

	json2:=put_json(json2,'__SECUENCIAOK__','0');
        return json2;
END;
$$ LANGUAGE plpgsql;


--insert into isys_querys_tx values ('17101',10,9,1,'select arma_filtros_15101(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,15,15);
insert into isys_querys_tx values ('17101',15,9,1,'select arma_querys_paralelo_17101(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('17101',20,1,9,'<EJECUTA0>17102</EJECUTA0><EJECUTA1>17103</EJECUTA1><EJECUTA2>17104</EJECUTA2><EJECUTA3>17105</EJECUTA3><EJECUTA4>17106</EJECUTA4><EJECUTA5>17107</EJECUTA5><EJECUTA6>17108</EJECUTA6><EJECUTA7>17109</EJECUTA7><EJECUTA8>17110</EJECUTA8><EJECUTA9>17111</EJECUTA9><EJECUTA10>17112</EJECUTA10><EJECUTA11>17113</EJECUTA11><EJECUTA12>17114</EJECUTA12><EJECUTA13>17115</EJECUTA13><EJECUTA14>17116</EJECUTA14><EJECUTA15>17117</EJECUTA15><TIMEOUT>5</TIMEOUT>',0,0,0,1,1,30,30);
--Sin bases de errores
insert into isys_querys_tx values ('17101',21,1,9,'<EJECUTA0>17102</EJECUTA0><EJECUTA1>17103</EJECUTA1><EJECUTA2>17104</EJECUTA2><EJECUTA3>17105</EJECUTA3><EJECUTA4>17106</EJECUTA4><EJECUTA5>17107</EJECUTA5><EJECUTA6>17108</EJECUTA6><EJECUTA7>17109</EJECUTA7><EJECUTA8>17110</EJECUTA8><EJECUTA9>17111</EJECUTA9><EJECUTA10>17112</EJECUTA10><EJECUTA11>17113</EJECUTA11><EJECUTA12>17116</EJECUTA12><EJECUTA13>17117</EJECUTA13><TIMEOUT>5</TIMEOUT>',0,0,0,1,1,30,30);

--insert into isys_querys_tx values ('17101',30,9,16,'LOG_JSON',0,0,0,1,1,35,35);
insert into isys_querys_tx values ('17101',30,1,14,'{"f":"INSERTA_JSON","p1":{"v_parametro_rut_emisor_com":"","__ARGV__":"","V_PARAMETRO_RUT_EMISOR_COM":"","json_sesion":"","v_nombres_parametros_var":"","QUERY_DATA":"","QUERY_DATA_HEX":"","QUERY_PATRON":"","JSON5":"","QUERY_FOLIO_BOLETAS_2014":"","QUERY_FOLIO_BOLETAS_EMISION_DISTINTA_2016":"","QUERY_FOLIO_BOLETAS_EMISION_DISTINTA_2014":"","QUERY_FOLIO_BOLETAS_EMISION_DISTINTA":"","QUERY_FOLIO_BOLETAS_HISTORICAS":"","QUERY_FOLIO_BOLETAS_LOCAL":"","QUERY_FOLIO_BOLETAS_IMP_LOCAL":"","QUERY_FOLIO_BOLETAS_IMP_AWS":"","QUERY_FOLIO_EMITIDOS_HIST":"","QUERY_FOLIO_EMITIDOS_LOCAL":"","QUERY_FOLIO_EMITIDOS_IMP_LOCAL":"","QUERY_FOLIO_EMITIDOS_IMP_AWS":"","QUERY_FOLIO_EMITIDOS_ERRORES_HIST":"","QUERY_FOLIO_EMITIDOS_ERRORES_LOCAL":"","QUERY_FOLIO_EMITIDOS_COLAS_13":"","QUERY_FOLIO_EMITIDOS_COLAS_14":""}}',0,0,0,0,0,35,35);
insert into isys_querys_tx values ('17101',35,9,1,'select junta_respuestas_17101(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

--BOLETAS_2014
insert into isys_querys_tx values ('17102','10',17,1,'$$QUERY_FOLIO_BOLETAS_2014$$',0,0,0,9,1,15,15);
insert into isys_querys_tx values ('17102',15,1,14,'{"f":"INSERTA_JSON","p1":{"FOLIO_BOLETAS_2014":$$RES_JSON_1$$}}',0,0,0,0,0,0,0);
--Boleta Emision Distinta
insert into isys_querys_tx values ('17103','10',39,1,'$$QUERY_FOLIO_BOLETAS_EMISION_DISTINTA_2016$$',0,0,0,9,1,15,15);
insert into isys_querys_tx values ('17103',15,1,14,'{"f":"INSERTA_JSON","p1":{"FOLIO_BOLETAS_EMISION_DISTINTA_2016":$$RES_JSON_1$$}}',0,0,0,0,0,0,0);
insert into isys_querys_tx values ('17104','10',17,1,'$$QUERY_FOLIO_BOLETAS_EMISION_DISTINTA_2014$$',0,0,0,9,1,15,15);
insert into isys_querys_tx values ('17104',15,1,14,'{"f":"INSERTA_JSON","p1":{"FOLIO_BOLETAS_EMISION_DISTINTA_2014":$$RES_JSON_1$$}}',0,0,0,0,0,0,0);
--Boletas Emision Distinta
insert into isys_querys_tx values ('17105','10',9,1,'$$QUERY_FOLIO_BOLETAS_EMISION_DISTINTA$$',0,0,0,9,1,15,15);
insert into isys_querys_tx values ('17105',15,1,14,'{"f":"INSERTA_JSON","p1":{"FOLIO_BOLETAS_EMISION_DISTINTA":$$RES_JSON_1$$}}',0,0,0,0,0,0,0);
--Boletas Historicas
insert into isys_querys_tx values ('17106','10',39,1,'$$QUERY_FOLIO_BOLETAS_HISTORICAS$$',0,0,0,9,1,15,15);
insert into isys_querys_tx values ('17106',15,1,14,'{"f":"INSERTA_JSON","p1":{"FOLIO_BOLETAS_HISTORICAS":$$RES_JSON_1$$}}',0,0,0,0,0,0,0);
--Boletas Base Actual
insert into isys_querys_tx values ('17107','10',9,1,'$$QUERY_FOLIO_BOLETAS_LOCAL$$',0,0,0,9,1,15,15);
insert into isys_querys_tx values ('17107',15,1,14,'{"f":"INSERTA_JSON","p1":{"FOLIO_BOLETAS_LOCAL":$$RES_JSON_1$$}}',0,0,0,0,0,0,0);
--Boletas Importadas Base Actual
insert into isys_querys_tx values ('17108','10',9,1,'$$QUERY_FOLIO_BOLETAS_IMP_LOCAL$$',0,0,0,9,1,15,15);
insert into isys_querys_tx values ('17108',15,1,14,'{"f":"INSERTA_JSON","p1":{"FOLIO_BOLETAS_IMP_LOCAL":$$RES_JSON_1$$}}',0,0,0,0,0,0,0);
--Base Importados Amazon
insert into isys_querys_tx values ('17109','10',26,1,'$$QUERY_FOLIO_BOLETAS_IMP_AWS$$',0,0,0,9,1,15,15);
insert into isys_querys_tx values ('17109',15,1,14,'{"f":"INSERTA_JSON","p1":{"FOLIO_BOLETAS_IMP_AWS":$$RES_JSON_1$$}}',0,0,0,0,0,0,0);
--Base BASE_EMITIDOS_HISTORICOS
insert into isys_querys_tx values ('17110','10',25,1,'$$QUERY_FOLIO_EMITIDOS_HIST$$',0,0,0,9,1,15,15);
insert into isys_querys_tx values ('17110',15,1,14,'{"f":"INSERTA_JSON","p1":{"FOLIO_EMITIDOS_HIST":$$RES_JSON_1$$}}',0,0,0,0,0,0,0);
--Base Local Emitidos
insert into isys_querys_tx values ('17111',10,9,1,'$$QUERY_FOLIO_EMITIDOS_LOCAL$$',0,0,0,9,1,15,15);
insert into isys_querys_tx values ('17111',15,1,14,'{"f":"INSERTA_JSON","p1":{"FOLIO_EMITIDOS_LOCAL":$$RES_JSON_1$$}}',0,0,0,0,0,0,0);

--Base Local Emitidos Importados
insert into isys_querys_tx values ('17112','10',9,1,'$$QUERY_FOLIO_EMITIDOS_IMP_LOCAL$$',0,0,0,9,1,15,15);
insert into isys_querys_tx values ('17112',15,1,14,'{"f":"INSERTA_JSON","p1":{"FOLIO_EMITIDOS_IMP_LOCAL":$$RES_JSON_1$$}}',0,0,0,0,0,0,0);
--Base Amazon Importados
insert into isys_querys_tx values ('17113','10',26,1,'$$QUERY_FOLIO_EMITIDOS_IMP_AWS$$',0,0,0,9,1,15,15);
insert into isys_querys_tx values ('17113',15,1,14,'{"f":"INSERTA_JSON","p1":{"FOLIO_EMITIDOS_IMP_AWS":$$RES_JSON_1$$}}',0,0,0,0,0,0,0);
--Base Emitidos Historicos Errores
insert into isys_querys_tx values ('17114','10',25,1,'$$QUERY_FOLIO_EMITIDOS_ERRORES_HIST$$',0,0,0,9,1,15,15);
insert into isys_querys_tx values ('17114',15,1,14,'{"f":"INSERTA_JSON","p1":{"FOLIO_EMITIDOS_ERRORES_HIST":$$RES_JSON_1$$}}',0,0,0,0,0,0,0);
--Base Local Emitidos Errores
insert into isys_querys_tx values ('17115','10',9,1,'$$QUERY_FOLIO_EMITIDOS_ERRORES_LOCAL$$',0,0,0,9,1,15,15);
insert into isys_querys_tx values ('17115',15,1,14,'{"f":"INSERTA_JSON","p1":{"FOLIO_EMITIDOS_ERRORES_LOCAL":$$RES_JSON_1$$}}',0,0,0,0,0,0,0);
--Base Colas (Encolados)
insert into isys_querys_tx values ('17116','10',1913,1,'$$QUERY_FOLIO_EMITIDOS_COLAS_13$$',0,0,0,9,1,15,15);
insert into isys_querys_tx values ('17116',15,1,14,'{"f":"INSERTA_JSON","p1":{"FOLIO_EMITIDOS_COLAS_13":$$RES_JSON_1$$}}',0,0,0,0,0,0,0);
insert into isys_querys_tx values ('17117','10',1914,1,'$$QUERY_FOLIO_EMITIDOS_COLAS_14$$',0,0,0,9,1,15,15);
insert into isys_querys_tx values ('17117',15,1,14,'{"f":"INSERTA_JSON","p1":{"FOLIO_EMITIDOS_COLAS_14":$$RES_JSON_1$$}}',0,0,0,0,0,0,0);


