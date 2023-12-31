delete from isys_querys_tx where llave='15101';

CREATE or replace FUNCTION pivote_busqueda_15101(json) RETURNS json AS $$
DECLARE
        json2   json;
        json1   alias for $1;
        v_fecha_inicio      integer;
        v_fecha_fin         integer;
        fecha_in1       varchar;
        json3       json;
        json4       json;
        json5       json;
        texto_ref1      varchar;
BEGIN
        json2:=json1;
        json2:=put_json(json2,'LLAMA_FLUJO','SI');
        json2:=put_json(json2,'__SECUENCIAOK__','15101');
        return json2;
END;
$$ LANGUAGE plpgsql;

insert into isys_querys_tx values ('15101','10',9,1,'select arma_filtros_15101(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);
--Pivote
insert into isys_querys_tx values ('15101','15',9,1,'select pivote_15101(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);
--Boletas 2014
insert into isys_querys_tx values ('15101','20',17,1,'$$QUERY_DATA$$',0,0,0,9,1,15,15);
--Boletas Emision Diferente
insert into isys_querys_tx values ('15101','30',39,1,'$$QUERY_DATA$$',0,0,0,9,1,15,15);
insert into isys_querys_tx values ('15101','40',17,1,'$$QUERY_DATA$$',0,0,0,9,1,15,15);
insert into isys_querys_tx values ('15101','50',9,1,'$$QUERY_DATA$$',0,0,0,9,1,15,15);
--Boletas Historicas
insert into isys_querys_tx values ('15101','60',39,1,'$$QUERY_DATA$$',0,0,0,9,1,15,15);
--Boletas Base Actual
insert into isys_querys_tx values ('15101','70',9,1,'$$QUERY_DATA$$',0,0,0,9,1,15,15);
--Boletas Importadas Base Actual
insert into isys_querys_tx values ('15101','80',9,1,'$$QUERY_DATA$$',0,0,0,9,1,15,15);
--Base Importados Amazon
insert into isys_querys_tx values ('15101','90',26,1,'$$QUERY_DATA$$',0,0,0,9,1,15,15);
--Base BASE_EMITIDOS_HISTORICOS
insert into isys_querys_tx values ('15101','100',25,1,'$$QUERY_DATA$$',0,0,0,9,1,15,15);
--Base Local Emitidos
insert into isys_querys_tx values ('15101','110',9,1,'$$QUERY_DATA$$',0,0,0,9,1,15,15);
--Base Local Emitidos Importados
insert into isys_querys_tx values ('15101','120',9,1,'$$QUERY_DATA$$',0,0,0,9,1,15,15);
--Base Amazon Importados
insert into isys_querys_tx values ('15101','130',26,1,'$$QUERY_DATA$$',0,0,0,9,1,15,15);
--Base Emitidos Historicos Errores
insert into isys_querys_tx values ('15101','140',25,1,'$$QUERY_DATA$$',0,0,0,9,1,15,15);
--Base Local Emitidos Errores
insert into isys_querys_tx values ('15101','150',9,1,'$$QUERY_DATA$$',0,0,0,9,1,15,15);
--Base Colas (Encolados)
insert into isys_querys_tx values ('15101','160',1913,1,'$$QUERY_DATA$$',0,0,0,9,1,15,15);
insert into isys_querys_tx values ('15101','170',1914,1,'$$QUERY_DATA$$',0,0,0,9,1,15,15);
--insert into isys_querys_tx values ('15101','180',1901,1,'$$QUERY_DATA$$',0,0,0,9,1,15,15);


CREATE or replace FUNCTION pivote_15101(json) RETURNS json AS $$
declare

        json1                alias for $1;
	json2			json;
	json5			json;
	query1	varchar;
	query2	varchar;
	select_vars1	varchaR;
	v_total	integer;
	sec1		integer;
	v_out_resultado	varchar;	
	crit_busq1	varchar;
	aux1	varchar;
begin
	json2:=json1;
	sec1:=get_json('CONTADOR_SECOK',json2)::integer+10;
	json2:=put_json(json2,'CONTADOR_SECOK',sec1::varchar);
	if (get_json('order_excel',json2)='SI') then
		json2:=put_json(json2,'__TIMEOUT_SERV_PXML__','10');
	else
		json2:=put_json(json2,'__TIMEOUT_SERV_PXML__','3');
	end if;
	--Remplazo QUERY_DATA con QUERY_DATA_HEX porque el motor borra las comillas simples
	json2:=put_json(json2,'QUERY_DATA',decode_hex(get_json('QUERY_DATA_HEX',json2)));
	--Si viene resultado
	if get_json('SOLO_QUERY',json2)<>'SI' then
		if (get_json('TOTAL_RES_JSON',json2)<>'1') then
			json2:=logjson(json2,get_json('TAG_MENSAJE',json2)||' Falla Consulta');
			--Si falla vamos a indicar la falla en el mensaje
			json2:=put_json(json2,'MENSAJE_RESPUESTA',get_json('MENSAJE_RESPUESTA',json2)||'<br>'||get_json('MENSAJE_ERROR',json2));
			perform graba_estadisticas_busqueda(json1,get_json('CAT_EST',json2),'NK');
		else
			perform graba_estadisticas_busqueda(json1,get_json('CAT_EST',json2),'OK');
			v_out_resultado:=get_json('array_to_json',get_json('RES_JSON_1',json2)::json);
			--Si viene data
			if v_out_resultado<>'' then
				json2:=logjson(json2,get_json('TAG_MENSAJE',json2)||' Concateno Registros');
				--Si esta vacio lo inicializo, sino agrego
				if get_json('v_out_resultado',json2)='' then
					json2:=put_json(json2,'v_out_resultado',v_out_resultado::varchar);
				else
					json2:=put_json(json2,'v_out_resultado',json_merge_lists(get_json('v_out_resultado',json2),v_out_resultado::varchar));
				end if;
			else
				json2:=logjson(json2,get_json('TAG_MENSAJE',json2)||' Sin Data');
			end if;
		end if;
	end if;
	json2:=put_json(json2,'TOTAL_RES_JSON','');
	json2:=put_json(json2,'RES_JSON_1','');
	--Saco la anterior 
	--DAO 20201110 Si viene el flag nos saltamos la busqueda en la tabla de errores
	if (get_json('CONTADOR_SECOK',json2)='140' and get_json('busca_errores',json2)='NO') then
		json2:=put_json(json2,'CONTADOR_SECOK','160');
		sec1:='160';
	end if;
	
	if (get_json('CONTADOR_SECOK',json2) in ('30') and get_json('FLAG_OC',json2)<>'SI') then
		json5:=decode_hex(get_json('JSON5',json2))::json;
		query2:=decode_hex(get_json('QUERY_PATRON',json2));
		json5:=put_json(json5,'TABLA','dte_boletas_amazon2016_no_borrar_fay');
		json5:=put_json(json5,'FILTRO_FECHA',' and estado_reclamo is null ');
		query1:=remplaza_tags_json_c(json5,query2);
		json2:=put_json(json2,'QUERY_DATA',query1);

		json2:=put_json(json2,'MENSAJE_ERROR','Falla Busqueda en Base Boletas EMISION_DISTINTA');
		json2:=put_json(json2,'TAG_MENSAJE','BUSCO FOLIO Boletas Base Boletas EMISION_DISTINTA');
		json2:=put_json(json2,'CAT_EST','FOLIO_BOLETAS_EMISION_DISTINTA_2016');
	elsif (get_json('CONTADOR_SECOK',json2) in ('40') and get_json('FLAG_OC',json2)<>'SI') then
		json5:=decode_hex(get_json('JSON5',json2))::json;
		query2:=decode_hex(get_json('QUERY_PATRON',json2));
		json5:=put_json(json5,'TABLA','dte_boletas_amazon2014_no_borrar_fay');
		json5:=put_json(json5,'FILTRO_FECHA',' and estado_reclamo is null ');
		query1:=remplaza_tags_json_c(json5,query2);
		json2:=put_json(json2,'QUERY_DATA',query1);

		json2:=put_json(json2,'MENSAJE_ERROR','Falla Busqueda en Base Boletas EMISION_DISTINTA');
		json2:=put_json(json2,'TAG_MENSAJE','BUSCO FOLIO Boletas Base Boletas EMISION_DISTINTA');
		json2:=put_json(json2,'CAT_EST','FOLIO_BOLETAS_EMISION_DISTINTA_2014');
	elsif (get_json('CONTADOR_SECOK',json2) in ('50') and get_json('FLAG_OC',json2)<>'SI') then
		json5:=decode_hex(get_json('JSON5',json2))::json;
		query2:=decode_hex(get_json('QUERY_PATRON',json2));
		json5:=put_json(json5,'TABLA','dte_boletas_no_borrar_fay');
		json5:=put_json(json5,'FILTRO_FECHA',' and estado_reclamo is null ');
		query1:=remplaza_tags_json_c(json5,query2);
		json2:=put_json(json2,'QUERY_DATA',query1);

		json2:=put_json(json2,'MENSAJE_ERROR','Falla Busqueda en Base Boletas EMISION_DISTINTA');
		json2:=put_json(json2,'TAG_MENSAJE','BUSCO FOLIO Boletas Base Boletas EMISION_DISTINTA');
		json2:=put_json(json2,'CAT_EST','FOLIO_BOLETAS_EMISION_DISTINTA');
	elsif (get_json('CONTADOR_SECOK',json2) in ('60') and get_json('FLAG_OC',json2)<>'SI') then
		json5:=decode_hex(get_json('JSON5',json2))::json;
		query2:=decode_hex(get_json('QUERY_PATRON',json2));
		json5:=put_json(json5,'TABLA','dte_boletas_generica');
		query1:=remplaza_tags_json_c(json5,query2);
		json2:=put_json(json2,'QUERY_DATA',query1);
		
		json2:=put_json(json2,'MENSAJE_ERROR','Falla Busqueda en Base Boletas Historicas');
		json2:=put_json(json2,'TAG_MENSAJE','BUSCO FOLIO Boletas Base Boletas Historicas');
		json2:=put_json(json2,'CAT_EST','FOLIO_BOLETAS_HISTORICAS');
	elsif (get_json('CONTADOR_SECOK',json2) in ('70') and get_json('FLAG_OC',json2)<>'SI') then
		json2:=put_json(json2,'MENSAJE_ERROR','Falla Busqueda en Base Local Boletas');
		json2:=put_json(json2,'TAG_MENSAJE','BUSCO FOLIO Boletas Base Actual');
		json2:=put_json(json2,'CAT_EST','FOLIO_BOLETAS_LOCAL');
		aux1:=get_json('FILTRO_FECHA',json5);
		json5:=decode_hex(get_json('JSON5',json2))::json;
		json5:=put_json(json5,'FILTRO_FECHA',get_json('FILTRO_FECHA',json5)||' and dia>=20160901 ');
		query2:=decode_hex(get_json('QUERY_PATRON',json2));
		query1:=remplaza_tags_json_c(json5,query2);
		json2:=put_json(json2,'QUERY_DATA',query1);
		json5:=put_json(json5,'FILTRO_FECHA',aux1);
                json2:=put_json(json2,'JSON5',encode_hex(json5::varchar));
	elsif (get_json('CONTADOR_SECOK',json2)='80' and get_json('FLAG_OC',json2)<>'SI') then
		json2:=put_json(json2,'MENSAJE_ERROR','Falla Busqueda en Base Local Boletas Importadas');
		json5:=decode_hex(get_json('JSON5',json2))::json;
		json5:=put_json(json5,'IMPUESTOS',' ,''[]''::json as impuestos ');
		json5:=put_json(json5,'REFERENCIAS',' ,''[]''::json as referencias ');
		query2:=decode_hex(get_json('QUERY_PATRON',json2));
		json5:=put_json(json5,'ESTADO_ACEPTA',quote_literal('IMPORTADO'));
		json5:=put_json(json5,'TABLA','dte_boletas_importadas_generica');
		query1:=remplaza_tags_json_c(json5,query2);
		json2:=put_json(json2,'QUERY_DATA',query1);
		json2:=put_json(json2,'JSON5',encode_hex(json5::varchar));
		json2:=put_json(json2,'TAG_MENSAJE','BUSCO FOLIO Boletas Importadas Base Actual');
		json2:=put_json(json2,'CAT_EST','FOLIO_BOLETAS_IMP_LOCAL');
	elsif (get_json('CONTADOR_SECOK',json2)='90') then
		json2:=put_json(json2,'TAG_MENSAJE','BUSCO FOLIO Importados Amazon');
		json2:=put_json(json2,'MENSAJE_ERROR','Falla Busqueda en Base Boletas Importadas (AWS)');
		json2:=put_json(json2,'CAT_EST','FOLIO_BOLETAS_IMP_AWS');
	elsif (get_json('CONTADOR_SECOK',json2)='100') then
		json5:=decode_hex(get_json('JSON5',json2))::json;
		query2:=decode_hex(get_json('QUERY_PATRON',json2));
	        json5:=put_json(json5,'ESTADO_ACEPTA',quote_literal('EMITIDO'));
        	json5:=put_json(json5,'IMPUESTOS','');
        	json5:=put_json(json5,'REFERENCIAS','');
        	json5:=put_json(json5,'ESTADO_CESION','');
        	json5:=put_json(json5,'TABLA','dte_emitidos');
		select_vars1:=get_campos_generica_6000_2(json2,get_json('flag_rut_emisor1',json2),get_json('v_nombres_parametros_var',json2),'NO');
		--select_vars1:=get_json('__campos_busqueda__',json2);
		json5:=put_json(json5,'CAMPOS',select_vars1);
		--En el Historico solo muestra publicados menor al 2014
		--json5:=put_json(json5,'FILTRO_FECHA',' and dia<20140101 ');
		query1:=remplaza_tags_json_c(json5,query2);
		json5:=put_json(json5,'FILTRO_FECHA','');
		json2:=put_json(json2,'QUERY_DATA',query1);
		json2:=put_json(json2,'JSON5',encode_hex(json5::varchar));
		json2:=put_json(json2,'TAG_MENSAJE','BUSCO FOLIO BASE_EMITIDOS_HISTORICOS');
		json2:=put_json(json2,'MENSAJE_ERROR','Falla Busqueda en Base Emitidos Historicos');
		json2:=put_json(json2,'CAT_EST','FOLIO_EMITIDOS_HIST');
	elsif (get_json('CONTADOR_SECOK',json2)='110') then
		json2:=put_json(json2,'TAG_MENSAJE','BUSCO FOLIO Local Emitidos'); 
		json2:=put_json(json2,'MENSAJE_ERROR','Falla Busqueda en Base Local Emitidos');
		json2:=put_json(json2,'CAT_EST','FOLIO_EMITIDOS_LOCAL');
	elsif (get_json('CONTADOR_SECOK',json2)='120') then
		json2:=put_json(json2,'MENSAJE_ERROR','Falla Busqueda en Base Local Emitidos Importados');
		json5:=decode_hex(get_json('JSON5',json2))::json;
		query2:=decode_hex(get_json('QUERY_PATRON',json2));
		json5:=put_json(json5,'TABLA','dte_emitidos_importados_generica');
		query1:=remplaza_tags_json_c(json5,query2);
		json2:=put_json(json2,'QUERY_DATA',query1);
		json2:=put_json(json2,'JSON5',encode_hex(json5::varchar));
		json2:=put_json(json2,'TAG_MENSAJE','BUSCO FOLIO Local Emitidos Importados');
		json2:=put_json(json2,'CAT_EST','FOLIO_EMITIDOS_IMP_LOCAL');
	elsif (get_json('CONTADOR_SECOK',json2)='130') then
		json2:=put_json(json2,'MENSAJE_ERROR','Falla Busqueda en Base Emitidos Importados (AWS)');
		json2:=put_json(json2,'TAG_MENSAJE','BUSCO FOLIO Amazon Importados');
		json2:=put_json(json2,'CAT_EST','FOLIO_EMITIDOS_IMP_AWS');
	elsif (get_json('CONTADOR_SECOK',json2)='140') then
		json5:=decode_hex(get_json('JSON5',json2))::json;
		query2:=decode_hex(get_json('QUERY_PATRON',json2));
	        json5:=put_json(json5,'ESTADO_ACEPTA','estado');
       		json5:=put_json(json5,'TABLA','dte_emitidos_errores');
	        json5:=put_json(json5,'ALIAS',' ,monto_exento as monto_excento ');
		--Ocupamos el filtro fecha para poner un limit en caso de que la cantidad de errore se exceda
		json5:=put_json(json5,'FILTRO_FECHA',' order by codigo_txel desc limit 40 ');
		query1:=remplaza_tags_json_c(json5,query2);
		json5:=put_json(json5,'FILTRO_FECHA','');
		json2:=put_json(json2,'QUERY_DATA',query1);
		json2:=put_json(json2,'JSON5',encode_hex(json5::varchar));
		json2:=put_json(json2,'TAG_MENSAJE','BUSCO FOLIO Emitidos Historicos Errores');
		json2:=put_json(json2,'MENSAJE_ERROR','Falla Busqueda en Base Errores Emitidos Historicos');
		json2:=put_json(json2,'CAT_EST','FOLIO_EMITIDOS_ERRORES_HIST');
	elsif (get_json('CONTADOR_SECOK',json2)='150') then
		json2:=put_json(json2,'TAG_MENSAJE','BUSCO FOLIO Local Emitidos Errores');
		json2:=put_json(json2,'MENSAJE_ERROR','Falla Busqueda en Base Local Errores');
		json2:=put_json(json2,'CAT_EST','FOLIO_EMITIDOS_ERRORES_LOCAL');
	elsif (get_json('CONTADOR_SECOK',json2)='160') then
                json5:=decode_hex(get_json('JSON5',json2))::json;
		query2:=decode_hex(get_json('QUERY_PATRON',json2));
	        json5:=put_json(json5,'ESTADO_ACEPTA','xml_flags');
       		json5:=put_json(json5,'IMPUESTOS',' ,''[]''::json as impuestos ');
	        json5:=put_json(json5,'REFERENCIAS',' ,''[]''::json as referencias ');
   	     	json5:=put_json(json5,'ESTADO_CESION',',''''::varchar as estado_cesion ');
        	json5:=put_json(json5,'ALIAS','');
	        json5:=put_json(json5,'TIPO_DTE',get_json('v_tipo_dte_com',json2));
        	json5:=put_json(json5,'RUT_EMISOR',get_json('v_parametro_rut_emisor_com',json2));
	        json5:=put_json(json5,'FOLIO',get_json('v_in_num_folio_com',json2));
        	json5:=put_json(json5,'TABLA','(select ''EN_PROCESO (''||coalesce(xml_flags,''Dte aun no procesado'')||'')'' as xml_flags,coalesce(xml_flags,''Dte aun no procesado'') as xml_flags_visual,fecha,null::varchar as data_dte,fecha as fecha_ingreso,null::varchar as monto_neto,null::varchar as monto_excento,null::varchar as monto_iva,null::timestamp as fecha_sii,null::varchar as estado_sii,null::varchar as mensaje_sii,null::timestamp as fecha_inter,null::varchar as estado_inter,null::varchar as fecha_vencimiento,null::varchar as mensaje_inter,uri,rut_emisor,rut_receptor::integer,tipo_dte::integer,folio,get_campo(''MONTO_TOTAL'',data) as monto_total,get_campo(''FECHA_EMISION'',data) as fecha_emision,to_char(fecha,''YYYYMMDD'')::integer as dia,case when get_campo(''FECHA_EMISION'',data)='''' then 0 else replace(get_campo(''FECHA_EMISION'',data),''-'','''')::integer end as dia_emision,''''::varchar as estado,''''::varchar parametro1,''''::varchar parametro2,''''::varchar parametro3,''''::varchar parametro4,''''::varchar parametro5,null::bigint as codigo_txel from colas_motor_generica where '||chr(36)||chr(36)||chr(36)||'RUT_EMISOR'||chr(36)||chr(36)||chr(36)||' and tipo_dte in '||chr(36)||chr(36)||chr(36)||'TIPO_DTE'||chr(36)||chr(36)||chr(36)||' '||chr(36)||chr(36)||chr(36)||'FOLIO'||chr(36)||chr(36)||chr(36)||' and is_number(rut_receptor) and is_number(tipo_dte)) colas_motor_generica ');
        	json2:=put_json(json2,'flag_encolados','SI');
		select_vars1:=get_campos_generica_6000_2(json2,get_json('flag_rut_emisor1',json2),get_json('v_nombres_parametros_var',json2),'NO');
		--select_vars1:=get_json('__campos_busqueda__',json2);
	        json2:=put_json(json2,'flag_encolados','');
        	json5:=put_json(json5,'CAMPOS',select_vars1);
		query1:=remplaza_tags_json_c(json5,query2);
   	     	query1:=remplaza_tags_json_c(json5,query1);
	        json2:=put_json(json2,'QUERY_DATA',query1);
                json2:=put_json(json2,'JSON5',encode_hex(json5::varchar));
		json2:=put_json(json2,'TAG_MENSAJE','BUSCO FOLIO Colas (Encolados) ');
		json2:=put_json(json2,'MENSAJE_ERROR','Falla Busqueda en Base Colas');
		json2:=put_json(json2,'CAT_EST','FOLIO_EMITIDOS_COLAS_13');
	elsif (get_json('CONTADOR_SECOK',json2)='170') then
		json2:=put_json(json2,'TAG_MENSAJE','BUSCO FOLIO Colas (Encolados) 14');
		json2:=put_json(json2,'MENSAJE_ERROR','Falla Busqueda en Base Colas 14');
		json2:=put_json(json2,'CAT_EST','FOLIO_EMITIDOS_COLAS_14');
	/*
	elsif (get_json('CONTADOR_SECOK',json2)='180') then
		json2:=put_json(json2,'TAG_MENSAJE','BUSCO FOLIO Colas (Encolados) 14');
		json2:=put_json(json2,'MENSAJE_ERROR','Falla Busqueda en Base Colas 14');
		json2:=put_json(json2,'CAT_EST','FOLIO_EMITIDOS_COLAS');
	*/
	--Finalmente
	elsif (get_json('CONTADOR_SECOK',json2)='180' or get_json('FLAG_OC',json2)='SI') then
		json2:=put_json(json2,'__SECUENCIAOK__','0');
		 --Si no es nulo el resultado, cuente.
		if (get_json('v_out_resultado',json2)='') then 
			v_total:=0;
			json2:=put_json(json2,'CODIGO_RESPUESTA','2');
			json2:=put_json(json2,'MENSAJE_RESPUESTA','No se encontro el folio');
			if get_json('SOLO_QUERY',json2)<>'SI' then
				json2:=responde_pantalla_15100(json2);
			end if;
			return json2;
		else
                	v_total:=count_array_json(get_json('v_out_resultado',json2)::json);
		end if;
        	--Criterios de busqueda para reportes
	        crit_busq1:=' <b>Folio=</b>'||get_json('FOLIO',json2);

        	if (length(get_json('texto_filtro_params',json2))>0) then
	                crit_busq1:=crit_busq1||'<b>Parametros </b>'||get_json('texto_filtro_params',json2);
        	end if;
		json2:=logjson(json2,'Total Resultados '||v_total::varchar);
		json2:=put_json(json2,'v_total_registros',v_total::varchar);
		json2:=put_json(json2,'v_in_offset',get_json('v_in_offset',json2));
	        json2:=put_json(json2,'criterio_busqueda_excel',crit_busq1);
		json2:=put_json(json2,'CODIGO_RESPUESTA','1');
		json2:=logjson(json2,'MENSAJE='||get_json('MENSAJE_RESPUESTA',json2));
		json2:=put_json(json2,'MENSAJE',get_json('MENSAJE_RESPUESTA',json2));
		json2:=put_json(json2,'MENSAJE_RESPUESTA','OK');
		if get_json('SOLO_QUERY',json2)<>'SI' then
			json2:=responde_pantalla_15100(json2);
		end if;
		return json2;		
	end if;

	--Se guarda QUERY_DATA_HEX con QUERY_DATA para usarla en el proximo ciclo
	json2:=put_json(json2,'QUERY_DATA_HEX',encode_hex(get_json('QUERY_DATA',json2)));
	json2:=put_json(json2,'__SECUENCIAOK__',sec1::varchar);
        return json2;
end
$$ LANGUAGE plpgsql;




CREATE or replace FUNCTION arma_filtros_15101(json) RETURNS json AS $$
declare

        json1                alias for $1;
        json2                   json;
        v_in_rut_emisor        integer;
        v_in_grupo             varchar;
        v_in_evento            varchar;
        v_in_offset             varchar;
        v_in_offset1            integer;
        v_in_cant_reg           varchar;
        v_out_resultado        json;
        json3                   json;
        json4                   json;
        v_tipo_dte             varchar;
        v_tipo_dte_com             varchar;
        v_estado               varchar;
        v_estado_var           varchar;
        v_total                integer;
        v_total1                       integer;
        v_total2               integer;
        v_estado_indexer        varchar;
        v_codigo_indexer        varchar;
        tipo_dia1       varchar;
        total_pag1      varchar;
        v_in_rut_receptor       varchar;
        v_in_rut_receptor_com       varchar;
        v_in_rut_receptor_ori   varchar;
        v_in_num_folio          varchar;
        v_in_num_folio_com          varchar;
        v_in_avanzada           varchar;
        v_parametro_tipo_dte    varchar;
        v_parametro_var varchar;
        aux             varchar;
        campo           record;
        texto_filtro    varchar;
        texto_filtro_params    varchar;
        query1  varchar;
        tmp1    varchar;
        crit_busq1      varchar;
        v_rut_usuario integer;
        flag_cuenta_estado      boolean;
        rol1    varchar;
        flag_cesion     varchar;
        flag_exportar   varchar;
        select_flag     varchar;
        json_acciones_masivo    json;
        v_parametro_rut_emisor  varchar;
        v_parametro_rut_emisor_com  varchar;
        json_rut1       json;
        flag_rut_emisor1        varchar;
        codigo_inicio1  varchar;
        acciones1       varchar;
        json_flags      json;
        fecha_in1       varchar;
        order_excel1    varchar;
        v_nombres_parametros_var        varchar;
        v_nombres_parametros_var_vacio        varchar;
        flag_boleta1    varchar;
        filtro_dias1    varchar;
        flag_offset     boolean;
        flag_ok boolean;
        desde1  integer;
        hasta1  integer;
        limit1  integer;
        of1     integer;
        flag_excel1     boolean;
        id_reporte1 varchar;
        v_in_cant_reg_fijo      varchar;
        flag_solo_base1 boolean;
        flag_mixto_base1        boolean;
        fecha_ini1      integer;
        limit_mixto1    integer;
        fecha_json4     varchar;
        offset_mixto1   integer;
        json_par1       json;
        select_vars1    varchar;
        select_vars2    varchar;
        v_lista_errores varchar;
        stMasivo        busqueda_masiva_header%ROWTYPE;
        id_masivo1      varchar;
        tabla_dinamica1 varchar;
        vin_fstart      varchar;
        vin_fend        varchar;
        vin_estado      varchar;
        vin_rut_receptor        varchar;
        vin_rut_emisor  varchar;
        vin_folio       varchar;
        vin_tipo_fecha  varchar;
        vin_tipo_dte    varchar;
        vin_offset      varchar;
        vin_rol         varchar;
        vin_count_table         varchar;
        stEstadoDte     estado_dte%ROWTYPE;
        v_glosa_estado  varchar;
	json5		json;
	lista_cesion1	varchar;
	tabla_defecto1	varchar;
	query2		varchar;
	json_resp1	json;
	json_out2	json;
        json_pdf1	json;
	estado1		varchar;
	evento1		varchar;
        json_oc		json;
	app1	varchar;
BEGIN
        json2:=json1;
        flag_boleta1:='NO';
        flag_cuenta_estado:=false;
        v_in_rut_emisor:=get_json('rutCliente',json2)::integer;
        v_rut_usuario:=get_json('rutUsuario',json2)::integer;

        --VARIABLES DE ENTRADA--
        vin_fstart:=get_json('FSTART',json2);
        vin_fend:=get_json('FEND',json2);
        vin_estado:=get_json('ESTADO',json2);
        vin_rut_receptor:=get_json('RUT_RECEPTOR',json2);
        vin_rut_emisor:=get_json('RUT_EMISOR',json2);
        vin_folio:=get_json('FOLIO',json2);
        vin_tipo_fecha:=get_json('TIPO_FECHA',json2);
        vin_tipo_dte:=get_json('TIPO_DTE',json2);
	--if(vin_tipo_dte='801') then
        --        return select_oc_folio_emitidos(json2);
        --end if;

        vin_offset:=get_json('offset',json2);
        vin_rol:=get_json('rol_usuario',json2);
        vin_count_table:=replace(get_json('count_table',json2),'.','');
	--vin_count_table:=get_json('count_table',json2);

        json2:=logjson(json2,'Entro a select_detalle_dte_emitidos_6000 VARIABLES_ENTRADA= vin_fstart->'||vin_fstart||', vin_fend->'||vin_fend||', vin_estado->'||vin_estado||', vin_rut_receptor->'||vin_rut_receptor||', vin_rut_emisor->'||vin_rut_emisor||', vin_folio->'||vin_folio||', vin_tipo_fecha->'||vin_tipo_fecha||', vin_tipo_dte->'||vin_tipo_dte||', vin_offset->'||vin_offset||', vin_rol->'||vin_rol||', vin_count_table->'||vin_count_table);
        --/VARIABLES DE ENTRADA--

        rol1:=vin_rol;

        --Si viene de una exportacion excel, le aplicamos order by
        if (get_json('order_excel',json2)='SI') then
        	if (vin_tipo_fecha='Emision') then
                	tipo_dia1:='_emision';
	        else
        	        tipo_dia1:='';
	        end if;
                flag_excel1:=true;
                --order_excel1:=' ORDER BY rut_emisor,tipo_dte,folio ';
                order_excel1:=' ORDER BY dia'||tipo_dia1||',codigo_txel ';
                ---order_excel1:=' ORDER BY codigo_txel ';
                id_reporte1:=get_json('id_reporte',json2);
		if is_number(get_json('_registros_por_reporte_',json2)) then
                        v_in_cant_reg_fijo:=get_json('_registros_por_reporte_',json2);
                        v_in_cant_reg := get_json('_registros_por_reporte_',json2);
                else
                        v_in_cant_reg_fijo:='1000';
                        v_in_cant_reg := '1000';
                end if;
        else
                flag_excel1:=false;
                order_excel1:=' ';
                id_reporte1:= ' No Reporte';
                v_in_cant_reg_fijo:='100';
                v_in_cant_reg := get_json('cantregs',json2);
                if (is_number(v_in_cant_reg) is false) then
                        v_in_cant_reg:='100';
                end if;
        end if;

        v_in_offset := vin_offset;
        if(v_in_offset is null or v_in_offset='')then
                v_in_offset=1;
        end if;
        v_in_offset1:=(v_in_offset::integer-1)*v_in_cant_reg::integer;
	json2:=put_json(json2,'v_in_offset',v_in_offset::varchar);


        --Si viene el total de la pagina, no cuento
        total_pag1:=replace(vin_count_table,'.','');
        if (is_number(total_pag1) is false) then
                total_pag1:='-1';
                json2:=logjson(json2,'Total de Pag '||total_pag1);
        end if;

        --Si el offset es 0, debo contar
        if (v_in_offset1=0) then
                total_pag1:='-1';
        end if;


        json2:=logjson(json2,'Entro a busqueda por folio ID='||id_reporte1);


        --PARAMETOS--
        --Agrega parametro tipo_dte
        v_parametro_tipo_dte:='';
        v_parametro_tipo_dte:=obtiene_filtro_perfilamiento_usuario_6000(v_in_rut_emisor,v_rut_usuario,'tipo_dte',vin_tipo_dte);
        json2:=logjson(json2,'PARAMETRO v_parametro_tipo_dte='||coalesce(v_parametro_tipo_dte,'vacio'));

	--rut_emisor
	if get_json('aplicacion',json2)='FINANCIAMIENTO_FIN' then
		vin_rut_emisor:=trim(vin_rut_emisor);
		json_rut1:=obtiene_filtro_perfilamiento_rut_emisor_6000(v_in_rut_emisor,v_rut_usuario,'rut_emisor','FINANCIADOR__'||vin_rut_emisor);
                --json_rut1:=obtiene_filtro_perfilamiento_rut_emisor_6000(v_in_rut_emisor,v_rut_usuario,'rut_emisor','FINANCIADOR');
        else
                json_rut1:=obtiene_filtro_perfilamiento_rut_emisor_6000(v_in_rut_emisor,v_rut_usuario,'rut_emisor',vin_rut_emisor);
        end if;
        v_parametro_rut_emisor:=get_json('TAG_RUT_EMISOR',json_rut1);
	v_parametro_rut_emisor_com:=get_json('TAG_RUT_EMISOR_COMILLAS',json_rut1);
        flag_rut_emisor1:=get_json('FLAG_RUT_EMISOR',json_rut1);
	json2:=put_json(json2,'flag_rut_emisor1',flag_rut_emisor1);
	json2:=put_json(json2,'v_parametro_rut_emisor_com',v_parametro_rut_emisor_com);
        json2:=logjson(json2,'v_parametro_rut_emisor='||v_parametro_rut_emisor ||' v_in_rut_emisor=' ||v_in_rut_emisor||' rut_emisor_filtro='||vin_rut_emisor);

        --Si tiene parametros adicionales, los usamos para filtrar la query
        --parametro1='E512' and parametro2='ERP'
        v_parametro_var:='';
        v_nombres_parametros_var:='';
	v_nombres_parametros_var_vacio:='';
        texto_filtro_params:='';
        for campo in select lower(parametro) as parametro,alias_web from filtros_rut where rut_emisor=v_in_rut_emisor and parametro<>'PARAMETRO5' loop
                aux:=get_json(campo.parametro,json2);
                tmp1:=obtiene_filtro_perfilamiento_usuario_6000(v_in_rut_emisor,v_rut_usuario,upper(campo.parametro),aux);
                json2:=logjson(json2,'PARAMETRO '||campo.parametro||'='||coalesce(tmp1,'vacio') || ' aux[' || aux ||']');
                texto_filtro_params:=texto_filtro_params||' <b>'||campo.alias_web||'=</b>'||replace(aux,'*','TODOS');

                v_parametro_var:=v_parametro_var|| ' ' ||tmp1;
		--perform logfile('DAO_FOLIO '||v_parametro_var||' '||v_in_rut_emisor||' '||v_rut_usuario||' '||aux);

                --Tenemos que construir los nombre para la grilla de los parametros adicionales
                v_nombres_parametros_var:=v_nombres_parametros_var||',coalesce('||trim(campo.parametro)||','''') as INFO__'||trim(campo.alias_web)||'__ON';
                v_nombres_parametros_var_vacio:=v_nombres_parametros_var_vacio||','''' as INFO__'||trim(campo.alias_web)||'__ON';
        end loop;
	json2:=put_json(json2,'v_nombres_parametros_var',v_nombres_parametros_var);
	json2:=put_json(json2,'texto_filtro_params',texto_filtro_params);
        --/PARAMETOS--

        --RUT_RECEPTOR--
        v_in_rut_receptor := split_part(replace(trim(vin_rut_receptor),'.',''),'-',1);
        v_in_rut_receptor_ori := v_in_rut_receptor;
        if (is_number(v_in_rut_receptor)) then
		v_in_rut_receptor_com:=' and  rut_receptor='||quote_literal(v_in_rut_receptor)||' ';
                v_in_rut_receptor:=' and  rut_receptor='||v_in_rut_receptor||' ';
        else
		v_in_rut_receptor_com:='';
                v_in_rut_receptor:='';
        end if;
        --/RUT_RECEPTOR--

        --TIPO_DTE--
	--No me importa lo que venga de pantalla, busca todo
        v_tipo_dte:=(select '('||string_agg(codigo,',')||')' from detalle_parametros where id_parametro = 31 );
        v_tipo_dte_com:=(select '('||string_agg(quote_literal(codigo),',')||')' from detalle_parametros where id_parametro = 31);

	json2:=put_json(json2,'v_tipo_dte_com',v_tipo_dte_com);
        json2:=logjson(json2,'v_tipo_dte='||v_tipo_dte);
        --/TIPO_DTE--

        --FOLIO--
        --Si viene el folio
        v_in_num_folio := trim(replace(vin_folio,'.',''));
	vin_folio:=v_in_num_folio;
        if (is_number(v_in_num_folio)) then
                json2:=logjson(json2,'Folio Numerico' || ' and folio='||v_in_num_folio||' ');
                v_in_num_folio_com:=' and folio='||quote_literal(v_in_num_folio)||' '||' '||v_in_rut_receptor_com;
                v_in_num_folio:=' and folio='||v_in_num_folio||' '||' '||v_in_rut_receptor_com;
		json2:=put_json(json2,'v_in_num_folio_com',v_in_num_folio_com);
        else
                json2:=response_requests_6000('200', 'Folio no numerico','',json2);
		return json2;
        end if;
        --/FOLIO--


        --FAY,DAO si viene PARAMETRO5, solo busca en busqueda masiva y hace join con dte_emitidos
        id_masivo1:=get_json('parametro5',json2);
        if (is_number(id_masivo1) and id_masivo1<>'0') then
                --Verifico que el id masivo corresponda a la empresa
                select * into stMasivo from busqueda_masiva_header where id=id_masivo1::bigint;
                if not found then
                        json2:=response_requests_6000('2', 'No existe grupo de busqueda','',json2);
                        return json2;
                end if;
                --Vemos si el id pertenece a la empresa
                if (stMasivo.rut_empresa<>v_in_rut_emisor) then
                        json2:=response_requests_6000('666', 'Grupo de Busqueda no pertenece a la empresa','',json2);
                        return json2;
                end if;

                --Cambiamos el total_pag1 para que no cuente
                --total_pag1:=stMasivo.total_reg;
                --select string_agg(column_name,',') from information_schema.columns where table_name='dte_emitidos';
                if vin_tipo_dte in ('39','41') or flag_boleta1='SI' then
                        tabla_dinamica1:=' (select a.codigo_txel,a.fecha_ingreso,a.mes,a.dia,b.tipo_dte,b.folio,a.fecha_emision,a.mes_emision,a.dia_emision,a.fecha_vencimiento,b.rut_emisor,a.rut_receptor as rut_receptor,a.monto_neto,a.monto_total,a.fecha_ult_modificacion,a.estado,a.hash_md5,a.uri,a.estado_sii,a.parametro1,a.parametro2,a.parametro3,a.parametro4,a.parametro5,a.fecha_sii,a.dia_sii,a.digest,a.data_dte,a.estado_inter,a.estado_mandato,a.monto_excento,a.monto_iva,a.fecha_inter,a.mensaje_inter,a.mensaje_sii from (select * from busqueda_masiva_detalle where id='||id_masivo1||') b left join ##TABLA## a on a.rut_emisor=b.rut_emisor and a.tipo_dte=b.tipo_dte and a.folio=b.folio) ##TABLA## ';
                else
                        tabla_dinamica1:=' (select a.codigo_txel,a.fecha_ingreso,a.mes,a.dia,b.tipo_dte,b.folio,a.fecha_emision,a.mes_emision,a.dia_emision,a.fecha_vencimiento,b.rut_emisor,a.rut_receptor as rut_receptor,a.monto_neto,a.monto_total,a.fecha_ult_modificacion,a.estado,a.hash_md5,a.uri,a.estado_sii,a.parametro1,a.parametro2,a.parametro3,a.parametro4,a.parametro5,a.fecha_sii,a.dia_sii,a.digest,a.data_dte,a.estado_inter,a.estado_mandato,a.monto_excento,a.monto_iva,a.fecha_inter,a.mensaje_inter,a.mensaje_sii,a.referencias,a.impuestos,a.estado_cesion as estado_cesion from (select * from busqueda_masiva_detalle where id='||id_masivo1||') b left join ##TABLA## a on a.rut_emisor=b.rut_emisor and a.tipo_dte=b.tipo_dte and a.folio=b.folio) ##TABLA## ';
                end if;
        else
                tabla_dinamica1:=' ##TABLA## ';
        end if;

        --if v_tipo_dte = '(39)' or v_tipo_dte = '(41)' then
        json2:=logjson(json2,'cantregs1='||v_in_cant_reg);

	json4:='[]';
	v_out_resultado:=null;
        --Se borra el offset y el limit, saca todo lo que tenga

	--Leemos las acciones
	if get_json('__BOTONES_TABLA__',json2)='' then
		app1:=get_json('app_dinamica',json2);
		if(get_json('TIPO_DTE',json2) in ('39','41') or get_json('v_lista_errores',json2)='SI') then
			--Las boletas no tienen boton cesion
			select array_to_json(array_agg(row_to_json(sql))) from (select titulo as nombre,tx,labels_no_editable,funcion,funcion_modal,titulo,tipo, reemplaza_regex_valores_json(reemplaza_regex_json(href,json2),json2) as href,glyphicon,caption from acciones_grillas where id_pantalla=app1 and categoria='BOTONERA_GRILLA' and check_funcionalidad_6000(json2,valor) and case when valor='ExportarPDF' then case when get_json('parametro_pdf_masivo',json2)='SI' then true else false end else true end and valor not in ('CESION') order by orden) sql into json_out2;
		else
			if get_json('ESTADO',json2) in ('EN_PROCESO','RETENIDOS') then
				select array_to_json(array_agg(row_to_json(sql))) from (select titulo as nombre,tx,labels_no_editable,funcion,funcion_modal,titulo,tipo, reemplaza_regex_valores_json(reemplaza_regex_json(href,json2),json2) as href,glyphicon,informacion,caption from acciones_grillas where id_pantalla=app1 and categoria='BOTONERA_GRILLA' and labels_editable='EN_PROCESO' and (case when get_json('rutCliente',json2) in ('97018000','96919050','96722460') and get_json('rol_usuario',json2)<>'CallCenter' and valor='AnularNC' then true else check_funcionalidad_6000(json2,valor) end) and case when valor='ExportarPDF' then case when get_json('parametro_pdf_masivo',json2)='SI' then true else false end else true end order by orden) sql into json_out2;
			else
				select array_to_json(array_agg(row_to_json(sql))) from (select titulo as nombre,tx,labels_no_editable,funcion,funcion_modal,titulo,tipo, reemplaza_regex_valores_json(reemplaza_regex_json(href,json2),json2) as href,glyphicon,informacion,caption from acciones_grillas where id_pantalla=app1 and categoria='BOTONERA_GRILLA' and coalesce(labels_editable,'')<>'EN_PROCESO' and check_funcionalidad_6000(json2,valor) and case when valor='ExportarPDF' then case when get_json('parametro_pdf_masivo',json2)='SI' then true else false end else true end order by orden) sql into json_out2;
			end if;
		end if;
		-- NBV 201705
		if(get_json('TIPO_DTE',json2)='801') then
			json_oc:='[]';
			select row_to_json(sql) from (select titulo as nombre,tx,labels_no_editable,funcion,funcion_modal,titulo,tipo,reemplaza_regex_valores_json(reemplaza_regex_json(href,json2),json2) as href,glyphicon,caption from acciones_grillas where id_pantalla=app1 and categoria='BOTONERA_GRILLA' and valor='Exportar') sql into json_pdf1;
			json_oc:=put_json_list(json_oc,json_pdf1);
			json2:=put_json(json2,'__BOTONES_TABLA__',json_oc);
		else
		-- NBV 201705
			json2:=put_json(json2,'__BOTONES_TABLA__',json_out2);
		end if;
		estado1:=get_json('ESTADO',json2);
		if(estado1<>'') then
			select glosa from estado_dte where descripcion=estado1 into evento1;
			json2:=put_json(json2,'EVENTO_CUAD',evento1);
		end if;
		if(get_json('TIPO_DTE',json2)<>'') then
			if(get_json('TIPO_DTE',json2) in ('39','41')) then
				json2:=put_json(json2,'GRUPO_CUAD','documentos_boletas');
			elsif(get_json('TIPO_DTE',json2) in ('110','111','112')) then
				json2:=put_json(json2,'GRUPO_CUAD','documentos_exportacion');
			else
				json2:=put_json(json2,'GRUPO_CUAD','documentos_nacionales');
			end if;
		end if;
	end if;

	--QUERY--
	--tabla_defecto1:='dte_boletas_generica';
	json5:='{}';
	json5:=put_json(json5,'ESTADO_ACEPTA',quote_literal('EMITIDO'));
	--json5:=put_json(json5,'IMPUESTOS',' ,''[]''::json as impuestos ');
	--json5:=put_json(json5,'REFERENCIAS',' ,''[]''::json as referencias ');
	json5:=put_json(json5,'ESTADO_CESION',',''''::varchar as estado_cesion ');
	json5:=put_json(json5,'IMPUESTOS','');
	json5:=put_json(json5,'REFERENCIAS','');
	json5:=put_json(json5,'ALIAS','');
	json5:=put_json(json5,'TABLA','dte_boletas_generica');

	json5:=put_json(json5,'TIPO_DTE',v_tipo_dte);
        json5:=put_json(json5,'RUT_EMISOR',v_parametro_rut_emisor);
        json5:=put_json(json5,'FOLIO',v_in_num_folio);
        --select_vars1:=get_campos_generica_6000(json2,flag_rut_emisor1,v_nombres_parametros_var,'SI');
	--perform logfile('DAO TABLA='||get_json('TABLA',json5));
	--if(get_json('rutUsuario',json2) in ('17597643','17705226')) then
	--	select_vars1:=get_campos_generica_emi_v2(json2,flag_rut_emisor1,v_nombres_parametros_var,'SI');
	--else
        	select_vars1:=get_campos_generica_6000_2(json2,flag_rut_emisor1,v_nombres_parametros_var,'SI');
		--select_vars1:=get_json('__campos_busqueda__',json2);
	--end if;
	json5:=put_json(json5,'CAMPOS',select_vars1);
	
	--Si buscan OC, otra tabla y tipo_dte y folio no son numericos
	if(vin_tipo_dte='801') then
		json5:=put_json(json5,'IMPUESTOS','');
		json5:=put_json(json5,'REFERENCIAS','');
		json5:=put_json(json5,'ESTADO_CESION','');
		json2:=put_json(json2,'FLAG_OC','SI');
	        json5:=put_json(json5,'TIPO_DTE',v_tipo_dte_com);
        	json5:=put_json(json5,'RUT_EMISOR',v_parametro_rut_emisor_com);
	        json5:=put_json(json5,'FOLIO',v_in_num_folio_com);
		json5:=put_json(json5,'TABLA','de_emitidos');
		json2:=put_json(json2,'__SECUENCIAOK__','30');
		json2:=put_json(json2,'CONTADOR_SECOK','30');
		json2:=put_json(json2,'TAG_MENSAJE','BUSCO FOLIO BASE LOCAL');
		json2:=put_json(json2,'MENSAJE_ERROR','Falla Busqueda en Base Local');
        	query2:='SELECT array_to_json(array_agg(row_to_json(sql))) FROM ( '||chr(36)||chr(36)||chr(36)||'CAMPOS'||chr(36)||chr(36)||chr(36)||' from (select *,'||chr(36)||chr(36)||chr(36)||'ESTADO_ACEPTA'||chr(36)||chr(36)||chr(36)||'::varchar as estado_acepta '||chr(36)||chr(36)||chr(36)||'IMPUESTOS'||chr(36)||chr(36)||chr(36)||' '||chr(36)||chr(36)||chr(36)||'REFERENCIAS'||chr(36)||chr(36)||chr(36)||' '||chr(36)||chr(36)||chr(36)||'ESTADO_CESION'||chr(36)||chr(36)||chr(36)||' '||chr(36)||chr(36)||chr(36)||'ALIAS'||chr(36)||chr(36)||chr(36)||' from '||chr(36)||chr(36)||chr(36)||'TABLA'||chr(36)||chr(36)||chr(36)||' where '||chr(36)||chr(36)||chr(36)||'RUT_EMISOR'||chr(36)||chr(36)||chr(36)||' '||chr(36)||chr(36)||chr(36)||'FOLIO'||chr(36)||chr(36)||chr(36)||' '||v_parametro_tipo_dte||' '||v_parametro_var||' '||chr(36)||chr(36)||chr(36)||'FILTRO_FECHA'||chr(36)||chr(36)||chr(36)||' ) x left join contribuyentes  m ON m.rut_emisor=x.rut_receptor LEFT JOIN tipo_dte t ON t.codigo = x.tipo_dte LEFT JOIN (select * from dte_pagado where canal=''EMITIDOS'') dp on dp.codigo_txel=x.codigo_txel) sql';
	else
		json2:=put_json(json2,'__SECUENCIAOK__','20');
		json2:=put_json(json2,'CONTADOR_SECOK','20');
		json2:=put_json(json2,'TAG_MENSAJE','BUSCO FOLIO BASE_BOLETAS_2014');
		json2:=put_json(json2,'MENSAJE_ERROR','Falla Busqueda en Base Boletas Historicas (2014)');
        	query2:='SELECT array_to_json(array_agg(row_to_json(sql))) FROM ( '||chr(36)||chr(36)||chr(36)||'CAMPOS'||chr(36)||chr(36)||chr(36)||' from (select *,'||chr(36)||chr(36)||chr(36)||'ESTADO_ACEPTA'||chr(36)||chr(36)||chr(36)||'::varchar as estado_acepta '||chr(36)||chr(36)||chr(36)||'IMPUESTOS'||chr(36)||chr(36)||chr(36)||' '||chr(36)||chr(36)||chr(36)||'REFERENCIAS'||chr(36)||chr(36)||chr(36)||' '||chr(36)||chr(36)||chr(36)||'ESTADO_CESION'||chr(36)||chr(36)||chr(36)||' '||chr(36)||chr(36)||chr(36)||'ALIAS'||chr(36)||chr(36)||chr(36)||' from '||chr(36)||chr(36)||chr(36)||'TABLA'||chr(36)||chr(36)||chr(36)||' where '||chr(36)||chr(36)||chr(36)||'RUT_EMISOR'||chr(36)||chr(36)||chr(36)||' and tipo_dte in '||chr(36)||chr(36)||chr(36)||'TIPO_DTE'||chr(36)||chr(36)||chr(36)||' '||chr(36)||chr(36)||chr(36)||'FOLIO'||chr(36)||chr(36)||chr(36)||' '||v_parametro_tipo_dte||' '||v_parametro_var||' '||chr(36)||chr(36)||chr(36)||'FILTRO_FECHA'||chr(36)||chr(36)||chr(36)||' ) x left join contribuyentes  m ON m.rut_emisor=x.rut_receptor LEFT JOIN tipo_dte t ON t.codigo = x.tipo_dte LEFT JOIN (select * from dte_pagado where canal=''EMITIDOS'') dp on dp.codigo_txel=x.codigo_txel) sql';
	end if;
	json5:=put_json(json5,'FILTRO_FECHA','');
        query1:=remplaza_tags_json_c(json5,query2);

--	json2:=logjson(json2,'CONSULTA COMPLETA-->'||query1);

	json2:=put_json(json2,'QUERY_DATA',query1);
	json2:=put_json(json2,'QUERY_DATA_HEX',encode_hex(query1));
	json2:=put_json(json2,'QUERY_PATRON',encode_hex(query2));
	json2:=put_json(json2,'JSON5',encode_hex(json5::varchar));
	json2:=put_json(json2,'__TOTAL_RESP_ESPERADAS__','1');
	
	if (get_json('order_excel',json2)='SI') then
		json2:=put_json(json2,'__TIMEOUT_SERV_PXML__','10');
	else
		json2:=put_json(json2,'__TIMEOUT_SERV_PXML__','3');
	end if;
	json2:=put_json(json2,'CAT_EST','FOLIO_BOLETAS_2014');
	return json2;
end
$$ LANGUAGE plpgsql;

