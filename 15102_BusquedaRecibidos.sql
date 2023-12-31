delete from isys_querys_tx where llave='15102';
CREATE or replace FUNCTION pivote_busqueda_15102(json) RETURNS json AS $$
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
	json2:=put_json(json2,'__SECUENCIAOK__','15102');
        return json2;
END;
$$ LANGUAGE plpgsql;

insert into isys_querys_tx values ('15102','10',9,1,'select arma_filtros_15102(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0); 

--Cuenta Recibidos Redshift
insert into isys_querys_tx values ('15102','30',22,1,'$$QUERY_RS$$',0,0,0,9,1,50,50);

--Junto resultados del contado
insert into isys_querys_tx values ('15102','50',9,1,'select resultado_cuenta_pivote_query_15102(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('15102','60',9,1,'select responde_pantalla_15102(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,0,0);

--LOOP de tablas
--Base Normal
insert into isys_querys_tx values ('15102','70',9,1,'$$QUERY_DATA$$',0,0,0,9,1,80,80);
--Base Replica - Solo Reportes
insert into isys_querys_tx values ('15102','71',11,1,'$$QUERY_DATA$$',0,0,0,9,1,80,80);
--Base Emitidos Historicos
insert into isys_querys_tx values ('15102','72',25,1,'$$QUERY_DATA$$',0,0,0,9,1,80,80);
--Base Importados
insert into isys_querys_tx values ('15102','74',26,1,'$$QUERY_DATA$$',0,0,0,9,1,80,80);
--Boletas 2014
insert into isys_querys_tx values ('15102','76',17,1,'$$QUERY_DATA$$',0,0,0,9,1,80,80);

insert into isys_querys_tx values ('15102','80',19,1,'select arma_next_query1_15102(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);
--Base Normal por Folio
insert into isys_querys_tx values ('15102','90',9,1,'$$QUERY_DATA$$',0,0,0,9,1,100,100);
--Base Importados Folio
insert into isys_querys_tx values ('15102','92',26,1,'$$QUERY_DATA$$',0,0,0,9,1,100,100);
--Base Hist
insert into isys_querys_tx values ('15102','94',25,1,'$$QUERY_DATA$$',0,0,0,9,1,100,100);
insert into isys_querys_tx values ('15102','100',9,1,'select arma_next_query_folio_15102(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

CREATE or replace FUNCTION arma_next_query_folio_15102(json) RETURNS json AS $$
DECLARE
        json2   json;
        json1   alias for $1;
        i       integer;
        j1      json;
        aux     varchar;
        fecha_c1        varchar;
        tabla1 varchar;
        json5   json;
        query1 varchar;
        query2 varchar;
        par1 varchar;
        v_out_resultado varchar;
begin
        json2:=json1;
        json2:=put_json(json2,'__FUNC__','arma_next_query_folio_15102');
	if (get_json('RES_JSON_1',json2)='') then
		perform graba_estadisticas_busqueda(json1,get_json('CAT_EST',json2),'NK');	
		json2:=logjsonfunc(json2,'Falla obtener resultado de '||get_json('TABLA',json2)|| ' Base '||get_json('PARAMETRO_TABLA',json2));
		json2:=put_json(json2,'MENSAJE_RESPUESTA','Reintente por favor (FT)');
		json2:=put_json(json2,'CODIGO_RESPUESTA','2');
		json2:=put_json(json2,'__SECUENCIAOK__','0');
		json2:=responde_pantalla_15102(json2);
		return json2;
	end if;
	--Vemos si hay resultado
	v_out_resultado:=get_json('v_out_resultado',json2);
	if (v_out_resultado<>'') then
		v_out_resultado:=json_merge_lists(v_out_resultado::varchar,get_json('array_to_json',get_json('RES_JSON_1',json2)::json));
	else
		v_out_resultado:=get_json('array_to_json',get_json('RES_JSON_1',json2)::json);
	end if;
	json2:=put_json(json2,'v_out_resultado',v_out_resultado);
	if (v_out_resultado<>'') then
		json2:=put_json(json2,'v_total_registros',count_array_json(v_out_resultado::json)::varchar);
	else
		json2:=put_json(json2,'v_total_registros','0');
	end if;
	--perform logfile('CUENTA '||get_json('RES_JSON_1',json2));

	if get_json('TIPO_DTE',json2)='801' or get_json('GUBERNAMENTAL',json2)='S' then
		json2:=put_json(json2,'MENSAJE_RESPUESTA','OK');
		json2:=put_json(json2,'CODIGO_RESPUESTA','1');
		json2:=put_json(json2,'__SECUENCIAOK__','0');
		json2:=responde_pantalla_15102(json2);
        	return json2;
	end if;
	if (get_json('__CONTADOR__',json2)='0') then
		json2:=put_json(json2,'QUERY_DATA',get_json('QUERY_DATA',json2));
		json2:=put_json(json2,'__SECUENCIAOK__','94');
		json2:=put_json(json2,'__CONTADOR__','1');
        	json2:=logjsonfunc(json2,'Ejecuta Folio en Hist');
	elsif (get_json('__CONTADOR__',json2)='1') then
		json2:=put_json(json2,'QUERY_DATA',replace(get_json('QUERY_DATA',json2),'dte_recibidos','dte_recibidos_importados_generica'));
		json2:=put_json(json2,'__SECUENCIAOK__','92');
		json2:=put_json(json2,'__CONTADOR__','2');
        	json2:=logjsonfunc(json2,'Ejecuta Folio en Importados');
	/*elsif(get_json('__CONTADOR__',json2)='1') then
		json2:=put_json(json2,'__SECUENCIAOK__','90');
		json2:=put_json(json2,'__CONTADOR__','2');
		json2:=logjsonfunc(json2,'Ejecuta Folio en PREA');
	*/
	else
		json2:=put_json(json2,'MENSAJE_RESPUESTA','OK');
		json2:=put_json(json2,'CODIGO_RESPUESTA','1');
		json2:=put_json(json2,'__SECUENCIAOK__','0');
		json2:=responde_pantalla_15102(json2);
        end if;
        return json2;
end;
$$
LANGUAGE plpgsql;

CREATE or replace FUNCTION arma_next_query1_15102(json) RETURNS json AS $$
DECLARE
        json2   json;
        json1   alias for $1;
	i	integer;
	j1	json;
	aux	varchar;
	fecha_c1	varchar;
	tabla1 varchar;
	json5	json;
	query1 varchar;
	query2 varchar;
	par1 varchar;
	v_out_resultado	varchar;
	res1	varchar;
begin
	json2:=json1;
	json2:=put_json(json2,'__FUNC__','arma_next_query1_15102');
	--json2:=logjsonfunc(json2,'RES_JSON_1='||get_json('RES_JSON_1',json2));
	res1:=get_json('RES_JSON_1',json2);
	--Si no contesta el API...
	if (is_json_dict(res1) is false) then
		perform graba_estadisticas_busqueda(json1,get_json('CAT_EST',json2),'NK');	
		json2:=logjsonfunc(json2,'Falla obtener resultado de '||get_json('TABLA',json2)|| ' Base '||get_json('PARAMETRO_TABLA',json2));
		json2:=put_json(json2,'MENSAJE_RESPUESTA','Reintente por favor (FT)');
		json2:=put_json(json2,'CODIGO_RESPUESTA','2');
		json2:=put_json(json2,'__SECUENCIAOK__','0');
		json2:=responde_pantalla_15102(json2);
		return json2;
	end if;
	--Si el API contesta con error..
	if (get_json('STATUS',res1::json)='OK') then
		--json2:=logjsonfunc(json2,'RES_JSON_1='||get_json('RES_JSON_1',json2));
		json2:=logjsonfunc(json2,'Resultado OK de '||get_json('TABLA',json2)|| ' Base '||get_json('PARAMETRO_TABLA',json2));
		v_out_resultado:=get_json('v_out_resultado',json2);
		if (v_out_resultado<>'') then
			v_out_resultado:=json_merge_lists(v_out_resultado::varchar,get_json('array_to_json',res1::json));
		else
			v_out_resultado:=get_json('array_to_json',res1::json);
		end if;
		json2:=put_json(json2,'v_out_resultado',v_out_resultado);
	elsif (get_json('STATUS',res1::json)='SIN_DATA') then
		json2:=logjsonfunc(json2,'Resultado SIN_DATA de '||get_json('TABLA',json2)|| ' Base '||get_json('PARAMETRO_TABLA',json2));
	else
		perform graba_estadisticas_busqueda(json1,get_json('CAT_EST',json2),'NK');	
		json2:=logjsonfunc(json2,'Falla obtener resultado de '||get_json('TABLA',json2)|| ' Base '||get_json('PARAMETRO_TABLA',json2)||' '||res1);
		json2:=put_json(json2,'MENSAJE_RESPUESTA','Reintente por favor (FT).');
		json2:=put_json(json2,'CODIGO_RESPUESTA','2');
		json2:=put_json(json2,'__SECUENCIAOK__','0');
		json2:=responde_pantalla_15102(json2);
		return json2;
	end if;

	perform graba_estadisticas_busqueda(json1,get_json('CAT_EST',json2),'OK');

	i:=get_json('__CONTADOR__',json2)::integer;
	j1:=get_json('__aux_tablas',json2);
	--Si el contador es mayor que el total de tablas..
	if (i=count_array_json(j1)) then
		json2:=logjsonfunc(json2,'Termino de hacer querys');
		json2:=put_json(json2,'MENSAJE_RESPUESTA','OK');
		json2:=put_json(json2,'CODIGO_RESPUESTA','1');
		json2:=put_json(json2,'__SECUENCIAOK__','0');
		json2:=responde_pantalla_15102(json2);
		return json2;	
	end if;

	--Saco la siguiente tabla
	aux:=get_json_index(j1,i);
	/*
	if(is_number(get_json_index(j1,i)) is false) then
		json2:=logjsonfunc(json2,'Falla periodo de la tabla');
		json2:=put_json(json2,'MENSAJE_RESPUESTA','Reintente por favor (FP)');
		json2:=put_json(json2,'CODIGO_RESPUESTA','2');
		json2:=put_json(json2,'__SECUENCIAOK__','0');
		json2:=responde_pantalla_15102(json2);
		return json2;
	end if;
	*/
	
	json5:=decode_hex(get_json('JSON5',json2));
	query2:=decode_hex(get_json('QUERY2',json2));
	fecha_c1:=get_json_index(j1,i);
	tabla1:=get_json('PREFIJO_TABLA',json2)||fecha_c1;
	json5:=put_json(json5,'TABLA',tabla1);	
	json2:=put_json(json2,'TABLA',tabla1);	
	query1:=remplaza_tags_json_c(json5,query2);
	json2:=put_json(json2,'QUERY_DATA',query1);
	
	execute 'select parametro_motor from '||get_json('TABLA_CONFIGURACION',json2)||' where periodo_desde<='||split_part(fecha_c1,'_',1)||' and periodo_hasta>='||split_part(fecha_c1,'_',1) into par1;
		
	json2:=put_json(json2,'PARAMETRO_TABLA',par1::varchar);
	json2:=put_json(json2,'CAT_EST',coalesce(par1::varchar,'LOCAL')||'__'||tabla1);
	if (par1='BASE_RECIBIDOS_HISTORICOS') then
		--json2:=logjson(json2,'query='||query1);
		json2:=put_json(json2,'__SECUENCIAOK__','72');
	elsif (par1='BASE_AMAZON_IMPORTADOS') then
		json2:=put_json(json2,'__SECUENCIAOK__','74');
	elsif (par1='BASE_AMAZON_BOLETAS_2014') then
		json2:=put_json(json2,'__SECUENCIAOK__','76');
	else
		--DAO 20201001 para los reportes vamos a la base de replica
		if is_number(get_json('id_reporte',json2)) then
                        json2:=put_json(json2,'__SECUENCIAOK__','71');
                else
                        json2:=put_json(json2,'__SECUENCIAOK__','70');
                end if;

		if get_json('rutUsuario',json2)='17597643' then
		json2:=logjson(json2,'QUERY17597643 '||replace(query1,chr(10),' '));
		end if;
	end if;
	json2:=put_json(json2,'__TOTAL_RESP_ESPERADAS__','1');
	json2:=put_json(json2,'__CONTADOR__',(i+1)::Varchar);
	json2:=logjsonfunc(json2,'Ejecuta '||coalesce(par1,'LOCAL')||' Tabla '||tabla1);
	return json2;
end;
$$
LANGUAGE plpgsql;

CREATE or replace FUNCTION responde_pantalla_15102(json) RETURNS json AS $$
DECLARE
        json2   json;
        json1   alias for $1;
	json3               json;
    	rut_cliente1          varchar;
    	rut_usuario1          varchar;
    	json_out2   json;
        app1    varchar;
        evento1 varchar;
        estado1 varchar;
        tipo_dte1       varchar;
        json_pdf1       json;
        json_info       json;
        json_oc         json;
BEGIN
        json2:=json1;
	app1:=get_json('app_dinamica',json2);
	json2:=put_json(json2,'__FUNC__','responde_pantalla_15102');
	json2:=put_json(json2,'__SECUENCIAOK__','0');
	json3:='{}';


	if(get_json('CODIGO_RESPUESTA',json2)<>'1') then
		json3:='{}';
                json3:=put_json(json3,'datos_tabla','[]');
                json3:=put_json(json3,'total_regs','0');
                json3:=put_json(json3,'criterio_busqueda_excel',get_json('criterio_busqueda_excel',json2));
		json2:=response_requests_6000('1', get_json('MENSAJE_RESPUESTA',json2), json3::varchar,json2);
		return json2;
	end if;
	tipo_dte1:=get_json('TIPO_DTE',json2);
        if(get_json('CODIGO_RESPUESTA',json2)='1') then
		if get_json('v_out_resultado',json2)='' then
			json2:=logjsonfunc(json2,'Busqueda no exitosa, v_out_resultado es nulo');
			json2:=put_json(json2,'MENSAJE_RESPUESTA','No se encontraron registros..');
	                json3:=put_json(json3,'datos_tabla','[]');
                	json3:=put_json(json3,'total_regs','0');
                	json3:=put_json(json3,'criterio_busqueda_excel',get_json('criterio_busqueda_excel',json2));
			json2:=response_requests_6000('1', get_json('MENSAJE_RESPUESTA',json2), json3::varchar,json2);
			return json2;
		end if;

                if(tipo_dte1='801') then
                        json_info:='{"ESTADOS":"<ul><li><b>PENDIENTE</b>: Documento sin revisión.</li><li><b>ACEPTADO</b>: Documento aceptado.</li><li><b>RECHAZADO</b>: Documento rechazado.</li></ul>"}';
                else
                        json_info:='{"ESTADOS":"<ul><li><b>SII</b>: Recepción del documento en el SII.</li><li><b>INTER</b>: Recepción del documento (receptor).</li><li><i class=''fa fa-file-text'' aria-hidden=''true''></i>: Aceptación o Reclamo del Contenido del Documento.</li><li><i class=''fa fa-truck'' aria-hidden=''true''></i>: Aceptación o reclamo de la Mercadería/Servicio.</li></ul>"}';
                end if;
                json3:=put_json(json3,'total_regs',get_json('v_total_registros',json2));
                json3:=put_json(json3,'MENSAJE',get_json('MENSAJE',json2));
		json2:=logjsonfunc(json2,'__hash__='||get_json('__hash__',json2));
                json3:=put_json(json3,'__hash__',get_json('__hash__',json2));
                json3:=put_json(json3,'cantidad_paginas',(get_json('v_total_registros',json2)::integer/100 + case when get_json('v_total_registros',json2)::integer%100>0 then 1 else 0 end)::varchar);
                json3:=put_json(json3,'cant_paginas',(get_json('v_total_registros',json2)::integer/100 + case when get_json('v_total_registros',json2)::integer%100>0 then 1 else 0 end)::varchar);
                json3:=put_json(json3,'offset',get_json('v_in_offset',json2));
                json3:=put_json(json3,'cantregs','100');
                json3:=put_json(json3,'datos_tabla',get_json('v_out_resultado',json2));
                json3:=put_json(json3,'criterio_busqueda_excel',get_json('criterio_busqueda_excel',json2));
                json3:=put_json(json3,'mostrar_mensaje_paginacion','SI');
                json3:=put_json(json3,'mostrar_paginador','false'); -- no muestra el paginador del datatables
                json3:=put_json(json3,'mostrar_info','false'); -- no muestra el mensaje de paginación
                json3:=put_json(json3,'mostrar_filtros','false'); -- no muestra el buscar del plugin
                --if(get_json('rutUsuario',json2) in ('17597643','17705226')) then
                        json3:=put_json(json3,'informacion',json_info::varchar);
                --end if;
        else
                json3:=put_json(json3,'criterio_busqueda_excel',get_json('criterio_busqueda_excel',json2));
                json3:=put_json(json3,'total_regs','0');
                json3:=put_json(json3,'cantidad_paginas','0');
        end if;

        json3:=put_json(json3,'titulo','Búsqueda Emitidos');
        json3:=put_json(json3,'flag_paginacion','NO');
        json3:=put_json(json3,'flag_paginacion_manual','SI');
        json3:=put_json(json3,'flag_tipo_cuadro','GRILLA');
        json3:=put_json(json3,'caption_buscar','Buscar en esta pagina :');


	json3:=put_json(json3,'botones_tabla',get_json('__BOTONES_TABLA__',json2));
        json2:=response_requests_6000('1', 'OK', json3::varchar,json2);
        RETURN json2;
end;
$$
LANGUAGE plpgsql;


CREATE or replace FUNCTION arma_filtros_15102(json) RETURNS json AS $$
DECLARE
        json2   json;
        json1   alias for $1;
        v_in_rut_emisor        integer;
        v_in_fecha_inicio      integer;
        v_in_fecha_fin         integer;
        v_in_evento            varchar;
        v_in_offset             varchar;
        v_in_offset1            integer;
        v_out_resultado        json;
        json3                   json;
	json4		json;
        v_tipo_dte             varchar;
        v_tipo_dte_com             varchar;
        v_estado               varchar;
        v_total1                       integer;
        v_estado_indexer        varchar;
        tipo_dia1       varchar;
        tipo_dia_ind1   varchar;
        total_pag1      varchar;
        v_in_rut_receptor       varchar;
        v_in_rut_receptor_com       varchar;
        v_in_rut_receptor_ori   varchar;
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
        v_parametro_rut_emisor  varchar;
        v_parametro_rut_emisor_com  varchar;
        json_rut1       json;
        flag_rut_emisor1        varchar;
	rut_emisor1	varchar;
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
	v_in_cant_reg	varchar;
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
        json5           json;
        lista_cesion1   varchar;
        tabla_defecto1  varchar;
        v_parametro_referencias1        varchar;
        v_parametro_adicional1  varchar;
        flag_codigo_txel        varchar;
        json_resp1      json;
        j1      json;
        fecha_c1        integer;
        i       integer;
        tabla1  varchar;
	query_data1	varchar;
	rut1	integer;
	flag_prea1	boolean;
	total1	integer;
	rc	record;
	stEstado	record;
	stEstado1	record;
	v_emisor	varchar;
	v_parametro_sucursales	varchar;
	v_parametro_rut_receptor	varchar;
	flag_rut_receptor1	varchar;
	consulta1	varchar;
	where_folio1	varchar;
	where1	varchar;
	aux_filtro_fechas varchar;	
	ret1	varchar;
	campo_dev	record;
	
	json_out2       json;
        json_pdf1       json;
        estado1         varchar;
        evento1         varchar;
        json_oc         json;
        app1    varchar;
BEGIN
        json2:=json1;
	json2:=put_json(json2,'__FUNC__','arma_filtro');
        flag_boleta1:='NO';
        flag_cuenta_estado:=false;
        v_in_rut_emisor:=get_json('rutCliente',json2)::integer;
        rut1:=get_json('rutCliente',json2)::integer;
        v_rut_usuario:=get_json('rutUsuario',json2)::integer;
        flag_codigo_txel:='NO';

	json2:=put_json(json2,'__SECUENCIAOK__','0');

        --VARIABLES DE ENTRADA--
        vin_fstart:=get_json('FSTART',json2);
        vin_fend:=get_json('FEND',json2);
        vin_estado:=get_json('ESTADO',json2);
        vin_rut_receptor:=get_json('RUT_RECEPTOR',json2);
        vin_rut_emisor:=get_json('RUT_EMISOR',json2);
	rut_emisor1 := replace(split_part(trim(vin_rut_emisor),'-',1),'.','');
        vin_folio:=get_json('FOLIO',json2);
        vin_tipo_fecha:=get_json('TIPO_FECHA',json2);
        vin_tipo_dte:=get_json('TIPO_DTE',json2);
	json2:=put_json(json2,'FLAG_BUSCAR','SI');

        vin_offset:=get_json('offset',json2);
        vin_rol:=get_json('rol_usuario',json2);
        vin_count_table:=replace(get_json('count_table',json2),'.','');

        json2:=logjsonfunc(json2,'VARIABLES_ENTRADA= vin_fstart->'||vin_fstart||', vin_fend->'||vin_fend||', vin_estado->'||vin_estado||', vin_rut_receptor->'||vin_rut_receptor||', vin_rut_emisor->'||vin_rut_emisor||', vin_folio->'||vin_folio||', vin_tipo_fecha->'||vin_tipo_fecha||', vin_tipo_dte->'||vin_tipo_dte||', vin_offset->'||vin_offset||', vin_rol->'||vin_rol||', vin_count_table->'||vin_count_table);
        --/VARIABLES DE ENTRADA--
        rol1:=vin_rol;

        --FECHAS--
        json2:=corrige_fechas(json2);
        v_in_fecha_inicio:=get_json('fstart',json2)::integer;
        v_in_fecha_fin:=get_json('fend',json2)::integer;

	tipo_dia1:=vin_tipo_fecha;

	if (tipo_dia1='Emision') then
		tipo_dia1:='_emision';
		tipo_dia_ind1:='E';
	elsif (tipo_dia1='recepcion_sii') then
		tipo_dia1:='_recepcion_sii';
		tipo_dia_ind1:='R';
	else
		tipo_dia1:='';
		tipo_dia_ind1:='A';
	end if;

	--Genera el texto de la fecha para hacer in
	fecha_in1:=genera_in_fechas(v_in_fecha_inicio,v_in_fecha_fin);
	if (fecha_in1 is null) then
		json2:=response_requests_6000('200', 'Rango de Fechas Invalida','',json2);
		return json2;
        end if;
        --/FECHAS--

        --OFFSET/EXCEL--
	--Si viene de una exportacion excel, le aplicamos order by
	if (get_json('order_excel',json2)='SI') then
		flag_excel1:=true;
		--order_excel1:=' ORDER BY rut_emisor,tipo_dte,folio ';
		order_excel1:=' ORDER BY dia'||tipo_dia1||',d.codigo_txel ';
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
	if(v_in_offset is null or v_in_offset='' or lower(v_in_offset)='undefined' or vin_offset='0')then
		v_in_offset=1;
	end if;
	v_in_offset1:=(v_in_offset::integer-1)*v_in_cant_reg::integer;
	json2:=put_json(json2,'v_in_offset',v_in_offset::varchar);

	--Si viene el total de la pagina, no cuento
	total_pag1:=replace(vin_count_table,'.','');
	if (is_number(total_pag1) is false) then
		total_pag1:='-1';
		json2:=logjsonfunc(json2,'Total de Pag '||total_pag1);
	end if;

	--Si el offset es 0, debo contar
	if (v_in_offset1=0) then
		total_pag1:='-1';
	end if;

	flag_prea1:=false;
        if(vin_estado='PREA' or strpos(vin_estado,'_1')>0 or vin_estado='PREA_RACS') then
                flag_prea1:=true;
        end if;

	json2:=logjsonfunc(json2,'Entro a select_detalle_dte_emitidos_6000 ID='||id_reporte1);
	--/OFFSET/EXCEL--

	--PARAMETOS--
	--Agrega parametro tipo_dte
	--Soporte multirut receptor
        json_rut1:=obtiene_filtro_perfilamiento_rut_receptor_6000(rut1::bigint,v_rut_usuario::bigint,'rut_emisor',vin_rut_receptor);
        flag_rut_receptor1:=get_json('FLAG_RUT_RECEPTOR',json_rut1);
        v_parametro_rut_receptor:=get_json('TAG_RUT_RECEPTOR',json_rut1);
        --Para el indexer hash
        v_parametro_rut_emisor:=replace(v_parametro_rut_receptor,'receptor','emisor');
        json2:=logjson(json2,'v_parametro_rut_receptor='||v_parametro_rut_receptor);
	json2:=put_json(json2,'flag_rut_receptor1',flag_rut_receptor1);
	json2:=put_json(json2,'v_parametro_rut_receptor',v_parametro_rut_receptor);
	json2:=put_json(json2,'v_parametro_rut_emisor',v_parametro_rut_emisor);

	--Si tiene parametros adicionales, los usamos para filtrar la query
	--parametro1='E512' and parametro2='ERP'
	v_parametro_var:='';
	v_nombres_parametros_var:='';
	v_nombres_parametros_var_vacio:='';
	texto_filtro_params:='';
--/PARAMETOS--


	--Armo la busqueda para las refernecias
	v_parametro_referencias1:='';
	if (get_json('TIPO_REFERENCIA',json2)<>'*' and get_json('TIPO_REFERENCIA',json2)<>'' and get_json('VALOR_REFERENCIA',json2)<>'') then
		v_parametro_referencias1:=' and strpos(referencias::varchar,''"Tipo":"'||get_json('TIPO_REFERENCIA',json2)||'","Folio":"'||get_json('VALOR_REFERENCIA',json2)||'"'')>0 ';
	elsif (get_json('TIPO_REFERENCIA',json2)<>'*' and get_json('TIPO_REFERENCIA',json2)<>'') then
		v_parametro_referencias1:=' and strpos(referencias::varchar,''"Tipo":"'||get_json('TIPO_REFERENCIA',json2)||'"'')>0 ';
	elsif (get_json('VALOR_REFERENCIA',json2)<>'') then
                v_parametro_referencias1:=v_parametro_referencias1||' and strpos(referencias::varchar,''"Folio":"'||get_json('VALOR_REFERENCIA',json2)||'"'')>0 ';
        end if;

        v_parametro_adicional1:='';
        --Parametros Adicionales PARAMETRO_ADICIONAL, VALOR_PARAMETRO_ADICIONAL
        if (get_json('PARAMETRO_ADICIONAL',json2)<>'' and get_json('VALOR_PARAMETRO_ADICIONAL',json2)<>'') then
                v_parametro_adicional1:=' and strpos(data_dte,''<'||get_json('PARAMETRO_ADICIONAL',json2)||'>'||get_json('VALOR_PARAMETRO_ADICIONAL',json2)||'</'||get_json('PARAMETRO_ADICIONAL',json2)||'>'')>0 ';
        elsif(get_json('PARAMETRO_ADICIONAL',json2)<>'') then
                v_parametro_adicional1:=' and strpos(data_dte,''<'||get_json('PARAMETRO_ADICIONAL',json2)||'>'')>0 ';
        elsif(get_json('VALOR_PARAMETRO_ADICIONAL',json2)<>'') then
                v_parametro_adicional1:=' and strpos(data_dte,''>'||get_json('VALOR_PARAMETRO_ADICIONAL',json2)||'</'')>0 ';
        end if;
        --CONTROLLER
        if(get_json('CONTROLLER',json2)<>'') then
                --v_parametro_adicional1:=' and strpos(data_dte,''"'||replace(get_json('CONTROLLER',json2),'---',' ')||'"'')>0 ';
                v_parametro_adicional1:=' and strpos(data_dte,''"'||decode_hex(get_json('CONTROLLER',json2))||'"'')>0 ';
        end if;

	 -- MVG ESTADO_DEVENGO
        if get_json('ESTADO_DEVENGO',json2)<>'' then
		select * into campo_dev from parametros_emitir where categoria='dipres_estado_devengo' and codigo=get_json('ESTADO_DEVENGO',json2);
		if found then
                	v_parametro_adicional1:=v_parametro_adicional1||campo_dev.explicacion;
		end if;
                --v_parametro_adicional1:=v_parametro_adicional1|| ' and strpos(data_dte,''<ESTADO_DEVENGO>'||get_json('ESTADO_DEVENGO',json2)||''')>0 ';
        end if;

	for campo in select lower(parametro) as parametro,alias_web from filtros_rut where rut_emisor=rut1::integer and canal='RECIBIDOS' loop
                aux:=get_json(campo.parametro,json2);
		tmp1:=obtiene_filtro_perfilamiento_usuario_6000_rec(v_in_rut_emisor,v_rut_usuario,upper(campo.parametro),aux);
		
		v_parametro_adicional1:=v_parametro_adicional1||' '||tmp1;	
        end loop;
	
	if (get_json('FORMA_PAGO',json2)<>'') then
                if (get_json('FORMA_PAGO',json2)='-1') then
                        v_parametro_adicional1:=v_parametro_adicional1|| ' and strpos(data_dte,''<FmaPago>'')=0 ';
                else
                        v_parametro_adicional1:=v_parametro_adicional1|| ' and strpos(data_dte,''<FmaPago>'||get_json('FORMA_PAGO',json2)||'</FmaPago>'')>0 ';
                end if;
        end if;

	--DAO 20171219 Para Reportes de Codelco
	aux_filtro_fechas:='';
	if (get_json('FSII_START',json2)<>'') then
		if aux_filtro_fechas<>'' then
			aux_filtro_fechas:=aux_filtro_fechas||' or ';
		end if;
		aux_filtro_fechas:=aux_filtro_fechas||' fecha_recepcion_sii::timestamp>='''||get_json('FSII_START',json2)||'''::timestamp ';
	end if;
	if (get_json('FECHA_ARM',json2)<>'') then
		if aux_filtro_fechas<>'' then
			aux_filtro_fechas:=aux_filtro_fechas||' or ';
		end if;
		aux_filtro_fechas:=aux_filtro_fechas||' coalesce(fecha_arm,''1900-01-01'')::date='''||get_json('FECHA_ARM',json2)||'''::date ';
		--Aqui no existe Fecha ARM
		if(vin_estado='PREA' or strpos(vin_estado,'_1')>0) then
			aux_filtro_fechas:='';
		end if;
	end if;
	if (get_json('FECHA_NAR',json2)<>'') then
		if aux_filtro_fechas<>'' then
                        aux_filtro_fechas:=aux_filtro_fechas||' or ';
                end if;
		aux_filtro_fechas:=aux_filtro_fechas||' strpos(coalesce(data_dte,''''),''<FNAR>'||get_json('FECHA_NAR',json2)||''')>0 ';
	end if;
	if (get_json('FECHA_SII_CONT',json2)<>'') then
                if aux_filtro_fechas<>'' then
                        aux_filtro_fechas:=aux_filtro_fechas||' or ';
                end if;
                aux_filtro_fechas:=aux_filtro_fechas||' coalesce(fecha_nar,''1900-01-01'')::date='''||get_json('FECHA_SII_CONT',json2)||'''::date ';
        end if;
	if (get_json('FECHA_SII_MER',json2)<>'') then
                if aux_filtro_fechas<>'' then
                        aux_filtro_fechas:=aux_filtro_fechas||' or ';
                end if;
                aux_filtro_fechas:=aux_filtro_fechas||' coalesce(fecha_reclamo,''1900-01-01'')::date='''||get_json('FECHA_SII_MER',json2)||'''::date ';
        end if;
	if aux_filtro_fechas<>'' then
		v_parametro_adicional1:=v_parametro_adicional1|| ' and ('||aux_filtro_fechas||') ';
	end if;
	--/DAO 20171219
	
	--GAC DAO 20170412
	/*
	v_parametro_sucursales:=null;
        v_parametro_sucursales:=obtiene_filtro_perfilamiento_usuario_6000(rut1::bigint,v_rut_usuario::bigint,'sucursales',aux, 'RECIBIDOS');
	if v_parametro_sucursales='' then
	        v_parametro_sucursales:=obtiene_filtro_perfilamiento_usuario_6000(rut1::bigint,v_rut_usuario::bigint,'PARAMETRO1',aux, 'RECIBIDOS');
	end if;
        v_parametro_sucursales:=coalesce(v_parametro_sucursales,'');
	*/
	v_parametro_sucursales:='';
        json2:=logjson(json2,'PARAMETRO v_parametro_adicional1='||coalesce(v_parametro_adicional1,'vacio'));

        --TIPO_DTE--
	--Si viene un * son Todos los DTE menos las boletas
	if (vin_tipo_dte='*' or vin_tipo_dte='') then
		v_tipo_dte:=(select '('||string_agg(codigo,',')||')' from detalle_parametros where id_parametro = 31 and codigo not in ('39','41'));
		v_tipo_dte_com:=(select '('||string_agg(quote_literal(codigo),',')||')' from detalle_parametros where id_parametro = 31 and codigo not in ('39','41'));
       		v_parametro_tipo_dte:=coalesce(obtiene_filtro_perfilamiento_usuario_6000(rut1::bigint,v_rut_usuario::bigint,'tipo_dte',vin_tipo_dte, 'RECIBIDOS'),'');
	elsif(get_json('LISTA_TIPO_DTE',json2)<>'') then
		v_tipo_dte:='('||get_json('LISTA_TIPO_DTE',json2)||')';
		v_tipo_dte_com:='('||get_json('LISTA_TIPO_DTE',json2)||')';
		v_parametro_tipo_dte:=' and tipo_dte in ('||get_json('LISTA_TIPO_DTE',json2)||')';
	else
		if(vin_tipo_dte in ('39','41'))then
			flag_boleta1:='SI';
		end if;
		v_tipo_dte:='('||vin_tipo_dte||')';
		v_tipo_dte_com:='('||quote_literal(vin_tipo_dte)||')';
		json2:=logjson(json2,'vin_tipo_dte='||vin_tipo_dte::varchar);
       		v_parametro_tipo_dte:=coalesce(obtiene_filtro_perfilamiento_usuario_6000(rut1::bigint,v_rut_usuario::bigint,'tipo_dte',vin_tipo_dte, 'RECIBIDOS'),'');
	end if;
	json2:=logjson(json2,'v_parametro_tipo_dte='||v_parametro_tipo_dte);
	
	if (is_number(rut_emisor1) is true and rut_emisor1 is not null) then
                if (flag_prea1) then
                        v_emisor := ' and split_part(rut_emisor::varchar,''-'',1)::integer=' || rut_emisor1||' ';
                else
                        v_emisor := ' and rut_emisor=' || rut_emisor1||' ';
                end if;
        else
                v_emisor := ' and 1=1 ';
        end if;

	if get_json('ESTADO_R42',json2)<>'' then
		ret1=get_json('ESTADO_R42',json2);
		--v_emisor := v_emisor ||' and rut_emisor in ('||(select string_agg(''''||rut_emisor::varchar||'''',',') from contribuyentes where resolucion_42=ret1)||') ';
		v_emisor := v_emisor ||' and rut_emisor in ('||(select string_agg(''''||rut_emisor::varchar||'''',',') from retenidos_retenedores_por_cliente where empresa=rut1 and resolucion_42=ret1)||') ';
        end if;

	--Folio
        if (is_number(vin_folio) is true and vin_folio is not null) then
                if (flag_prea1 or vin_tipo_dte='801') then
                        v_emisor := v_emisor||' and folio=' ||quote_literal(vin_folio)||' ';
                else
                        v_emisor := v_emisor||' and folio=' || vin_folio||' ';
                end if;
	else
		--Se limpia para que indique que no tiene folio
		vin_folio:='';
        end if;


	json2:=logjsonfunc(json2,'v_tipo_dte='||v_tipo_dte);
	--/TIPO_DTE--


	--Busco el codigo entrante
        select * into stEstado from estado_dte where codigo=vin_estado;
        if not found then
               select * into stEstado from estado_dte where codigo='REMI';
               if not found then
                      json2:=response_requests_6000('2','Error en estado de entrada','',json2);
                       return json2;
               end if;
        end if;

        --Si consulta por DOCUMENTOS CEDIDOS y no tiene habilitado el contacto sii en el maestro de clientes...
        if (stEstado.codigo='RACS' and get_json('flag_contacto_sii',json2)<>'SI') then
                --json2:=response_requests_6000('2','<h4>Para habilitar esta busqueda:<br>Es necesario activar la casilla de contacto SII en Acepta.<br>Por favor comuniquese con su Ejecutivo Comercial para activar esta funcionalidad.</h4>','',json2);
		json2:=put_json(json2,'MENSAJE_RESPUESTA','<h4>Para habilitar esta busqueda:<br>Es necesario activar la casilla de contacto SII en Acepta<br>Por favor comuniquese con su Ejecutivo Comercial para activar esta funcionalidad.</h4>');
		json2:=put_json(json2,'CODIGO_RESPUESTA','2');
		json2:=put_json(json2,'__SECUENCIAOK__','0');
		json2:=responde_pantalla_15102(json2);
                return json2;
        end if;
	--Si el estado es RADJ y el usuario busca con fecha < 2017-12-01 le decimos que no aplica
	if (stEstado.codigo='RADJ' and v_in_fecha_inicio::varchar::date<'2017-12-01'::date) then
		json2:=put_json(json2,'MENSAJE_RESPUESTA','<h4>La busqueda con este filtro "Documento Incluye Adjuntos (Traza)" es válido desde el 2017-12-01.</h4>');
		json2:=put_json(json2,'CODIGO_RESPUESTA','2');
		json2:=put_json(json2,'__SECUENCIAOK__','0');
		json2:=responde_pantalla_15102(json2);
                return json2;
	end if; 
        if(vin_tipo_fecha='recepcion_sii') then
                select * into rc from rc_no_autorizados where rut_emisor=rut1::integer;
                if found and get_json('gubernamental',json2)<>'S' then
			--Verificamos si tiene la nueva recepcion consolidada
			select flag_rcv_rconsolidao into rc from maestro_clientes where rut_emisor=rut1::integer and flag_rcv_rconsolidao is not null;
			if not found then
				json2:=put_json(json2,'MENSAJE_RESPUESTA','<h4>Para habilitar esta busqueda:<br>Es necesario que usted actualice los permisos para Acepta en el SII.</h4><a href="https://escritorio.acepta.com/documentos_publicos/CambioPermisosUsuariosAceptaSII.pdf" target="_blank">Manual</a>');
				json2:=put_json(json2,'CODIGO_RESPUESTA','2');
				json2:=put_json(json2,'__SECUENCIAOK__','0');
				json2:=responde_pantalla_15102(json2);
                        	--json2:=response_requests_6000('2','<h4>Para habilitar esta busqueda:<br>Es necesario que usted actualice los permisos para Acepta en el SII.</h4><a href="https://escritorio.acepta.com/documentos_publicos/CambioPermisosUsuariosAceptaSII.pdf" target="_blank">Manual</a>','',json2);
                        	return json2;
			end if;
                end if;
        end if;
	v_glosa_estado:=stEstado.glosa;
        v_estado:=stEstado.filtro_detalle;

	--Estado Revisado
        if get_json('ESTADO_REVISADO',json2)<>'' then
               	select * into stEstado1 from estado_dte where codigo=get_json('ESTADO_REVISADO',json2);
                if found then
                        v_estado:=v_estado||' and '||stEstado1.filtro_detalle;
                end if;
        end if;

	if get_json('__BOTONES_TABLA__',json2)='' then
		app1:=get_json('app_dinamica',json2);
		if(get_json('TIPO_DTE',json2) in ('39','41') or get_json('v_lista_errores',json2)='SI') then
			--Las boletas no tienen boton cesion
			select array_to_json(array_agg(row_to_json(sql))) from (select titulo as nombre,tx,labels_no_editable,funcion,funcion_modal,titulo,tipo, reemplaza_regex_valores_json(reemplaza_regex_json(href,json2),json2) as href,glyphicon from acciones_grillas where id_pantalla=app1 and categoria='BOTONERA_GRILLA' and check_funcionalidad_6000(json2,valor) and case when valor='ExportarPDF' then case when get_json('parametro_pdf_masivo',json2)='SI' then true else false end else true end and valor not in ('CESION','ExportarPDF') order by orden) sql into json_out2;
		else
			select array_to_json(array_agg(row_to_json(sql))) from (select titulo as nombre,tx,labels_no_editable,funcion,funcion_modal,titulo,tipo, reemplaza_regex_valores_json(reemplaza_regex_json(href,json2),json2) as href,glyphicon from acciones_grillas where id_pantalla=app1 and categoria='BOTONERA_GRILLA' and check_funcionalidad_6000(json2,valor) and case when valor='ExportarPDF' then case when get_json('parametro_pdf_masivo',json2)='SI' then true else false end else true end order by orden) sql into json_out2;
		end if;
		-- NBV 201705
		if(get_json('TIPO_DTE',json2)='801') then
			json_oc:='[]';
			select row_to_json(sql) from (select titulo as nombre,tx,labels_no_editable,funcion,funcion_modal,titulo,tipo,reemplaza_regex_valores_json(reemplaza_regex_json(href,json2),json2) as href,glyphicon from acciones_grillas where id_pantalla=app1 and categoria='BOTONERA_GRILLA' and valor='ExportarPDF') sql into json_pdf1;
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

	id_masivo1:=get_json('parametro5',json2);
	--Campos QUERY
	-- Casos en q no se cuenta en el RS son PREA, OC (de_recibidos), Por FOLIO
	if (flag_prea1 or vin_folio<>'' or vin_tipo_dte='801') then
		json2:=logjsonfunc(json2,'PREA o vin_folio o 801');
		---PREA
		--Armamor Query Data
                aux:=' LEFT JOIN (select * from dte_pagado where canal=''RECIBIDOS'')dp on dp.codigo_txel=d.codigo_txel ';
                where_folio1:=' FROM (select *,split_part(rut_emisor::varchar,''-'',1)::integer as rut_emisor_int from '||chr(36)||chr(36)||chr(36)||'TABLA'||chr(36)||chr(36)||chr(36)||' where '||v_parametro_rut_receptor||v_emisor||' and tipo_dte in '||v_tipo_dte_com||' and '||v_estado||' LIMIT ' || v_in_cant_reg_fijo::varchar || ' OFFSET ' || v_in_offset1::varchar||' ) d LEFT JOIN contribuyentes m ON m.rut_emisor=d.rut_emisor_int LEFT JOIN tipo_dte t ON t.codigo = d.tipo_dte::integer '||aux;

                if(get_json('TIPO_DTE',json2)='801') then
			--Se limpia el estado para que no vaya al if de PREA
			json2:=put_json(json2,'ESTADO','');
			consulta1:=get_campos_recibidos_avanzada_6000_2(json2,flag_rut_receptor1);
			where_folio1:=replace(where_folio1,chr(36)||chr(36)||chr(36)||'TABLA'||chr(36)||chr(36)||chr(36),'de_recibidos');
                        where1:=where_folio1;
			--Contamos local
                	execute 'select count(*) FROM de_recibidos where '||v_parametro_rut_receptor||v_emisor||' and tipo_dte in '||v_tipo_dte_com||' and '||v_estado||' and dia'||tipo_dia1||' in '||fecha_in1 into total1;
			json2:=logjson(json2,'select count(*) FROM de_recibidos where '||v_parametro_rut_receptor||v_emisor||' and tipo_dte in '||v_tipo_dte_com||' and '||v_estado||' and dia'||tipo_dia1||' in '||fecha_in1);
			json2:=put_json(json2,'v_total_registros',total1::varchar);
		elsif (vin_folio<>'' and flag_prea1 is false) then
			--Se limpia el estado para que no vaya al if de PREA
			json2:=put_json(json2,'ESTADO','');
			consulta1:=get_campos_recibidos_avanzada_6000_2(json2,flag_rut_receptor1);
                        where1:=where_folio1;
		else
			consulta1:=get_campos_recibidos_avanzada_6000_2(json2,flag_rut_receptor1);
                	aux:=' LEFT JOIN (select * from dte_pagado where canal=''RECIBIDOS'')dp on dp.codigo_txel=d.id ';
			if aux_filtro_fechas<>'' then
                		where1:=' FROM (select *,split_part(rut_emisor::varchar,''-'',1)::integer as rut_emisor_int,id as codigo_txel from dte_pendientes_recibidos where '||v_parametro_rut_receptor||v_emisor||' and tipo_dte in '||v_tipo_dte_com||' and '||v_estado||' and dia'||tipo_dia1||' in '||fecha_in1||' and ('||aux_filtro_fechas||') LIMIT ' || v_in_cant_reg_fijo::varchar || ' OFFSET ' || v_in_offset1::varchar||' ) d LEFT JOIN contribuyentes m ON m.rut_emisor=d.rut_emisor_int LEFT JOIN tipo_dte t ON t.codigo = d.tipo_dte::integer '||aux;
			else
                		where1:=' FROM (select *,split_part(rut_emisor::varchar,''-'',1)::integer as rut_emisor_int,id as codigo_txel from dte_pendientes_recibidos where '||v_parametro_rut_receptor||v_emisor||' and tipo_dte in '||v_tipo_dte_com||' and '||v_estado||' and dia'||tipo_dia1||' in '||fecha_in1||' LIMIT ' || v_in_cant_reg_fijo::varchar || ' OFFSET ' || v_in_offset1::varchar||' ) d LEFT JOIN contribuyentes m ON m.rut_emisor=d.rut_emisor_int LEFT JOIN tipo_dte t ON t.codigo = d.tipo_dte::integer '||aux;
			end if;
			--Contamos local
			if aux_filtro_fechas<>'' then
                		execute 'select count(*) FROM dte_pendientes_recibidos where '||v_parametro_rut_receptor||v_emisor||' and tipo_dte in '||v_tipo_dte_com||' and '||v_estado||' and dia'||tipo_dia1||' in '||fecha_in1||' and ('||aux_filtro_fechas||') ' into total1;
			else
                		execute 'select count(*) FROM dte_pendientes_recibidos where '||v_parametro_rut_receptor||v_emisor||' and tipo_dte in '||v_tipo_dte_com||' and '||v_estado||' and dia'||tipo_dia1||' in '||fecha_in1 into total1;
			end if;
			json2:=put_json(json2,'v_total_registros',total1::varchar);
                end if;
	else
		--raise notice 'select get_campos_recibidos_avanzada_6000_2(''%'');',json2;
		consulta1:=get_campos_recibidos_avanzada_6000_2(json2,flag_rut_receptor1);
		json2:=put_json(json2,'tabla_mes','SI');
		json2:=put_json(json2,'id_masivo',id_masivo1::varchar);

		--where_importados1:=' FROM (select * from '||chr(36)||chr(36)||chr(36)||'TABLA'||chr(36)||chr(36)||chr(36)||' where '||chr(36)||chr(36)||chr(36)||'CODIGO_TXEL'||chr(36)||chr(36)||chr(36)||' ) d LEFT JOIN contribuyentes m ON m.rut_emisor=d.rut_emisor LEFT JOIN tipo_dte t ON t.codigo = d.tipo_dte  LEFT JOIN (select * from dte_pagado where canal=''RECIBIDOS'')dp on dp.codigo_txel=d.codigo_txel ';
		
		--where_importados_folio1:=' FROM (select * from '||tabla_importados1||' where '||v_parametro_rut_receptor||v_emisor||' and tipo_dte in ('||tipo1||') and '||v_estado||' LIMIT ' || limit1 || ' OFFSET ' || pagina12||' ) d LEFT JOIN contribuyentes m ON m.rut_emisor=d.rut_emisor LEFT JOIN tipo_dte t ON t.codigo = d.tipo_dte LEFT JOIN (select * from dte_pagado where canal=''RECIBIDOS'')dp on dp.codigo_txel=d.codigo_txel';	

		where1:=' FROM (select * from '||chr(36)||chr(36)||chr(36)||'TABLA'||chr(36)||chr(36)||chr(36)||' where '||chr(36)||chr(36)||chr(36)||'CODIGO_TXEL'||chr(36)||chr(36)||chr(36)||' ) d LEFT JOIN contribuyentes m ON m.rut_emisor=d.rut_emisor LEFT JOIN tipo_dte t ON t.codigo = d.tipo_dte LEFT JOIN (select * from dte_pagado where canal=''RECIBIDOS'')dp on dp.codigo_txel=d.codigo_txel ';	
		if (vin_estado='RIMP') then
                        json2:=logjsonfunc(json2,'Cuenta en cuenta_offset_indexer_estadisticas');
                        flag_codigo_txel:='SI';
			json2:=put_json(json2,'flag_importado','SI');
			json2:=put_json(json2,'flag_recibidos','SI');
                        --json2:=genera_querys_cuenta_codigo_txel_15102(json2,v_in_offset1,v_in_cant_reg_fijo,v_emisor,v_tipo_dte,'','',fecha_in1,v_estado,tipo_dia_ind1, ' and '||v_parametro_rut_receptor,v_parametro_referencias1,v_parametro_adicional1|| ' ' ||v_parametro_sucursales,get_json('__hash__',json2),v_in_fecha_inicio::varchar);
                        json2:=genera_querys_cuenta_codigo_txel_15102(json2,v_in_offset1,v_in_cant_reg_fijo,v_emisor,v_tipo_dte,v_parametro_tipo_dte,'',fecha_in1,v_estado,tipo_dia_ind1, ' and '||v_parametro_rut_receptor,v_parametro_referencias1,v_parametro_adicional1|| ' ' ||v_parametro_sucursales,get_json('__hash__',json2),v_in_fecha_inicio::varchar);
                else
                        flag_codigo_txel:='SI';
                        json2:=logjsonfunc(json2,'Cuenta en cuenta_offset_redshift_emitidos');
			json2:=put_json(json2,'flag_recibidos','SI');
			--Cuenta local y genera query para contar en el RS que corresponda
                        --json2:=genera_querys_cuenta_codigo_txel_15102(json2,v_in_offset1,v_in_cant_reg_fijo,v_emisor,v_tipo_dte,'','',fecha_in1,v_estado,tipo_dia_ind1, ' and '||v_parametro_rut_receptor,v_parametro_referencias1,v_parametro_adicional1|| ' ' ||v_parametro_sucursales,get_json('__hash__',json2),v_in_fecha_inicio::varchar);
                        json2:=genera_querys_cuenta_codigo_txel_15102(json2,v_in_offset1,v_in_cant_reg_fijo,v_emisor,v_tipo_dte,v_parametro_tipo_dte,'',fecha_in1,v_estado,tipo_dia_ind1, ' and '||v_parametro_rut_receptor,v_parametro_referencias1,v_parametro_adicional1|| ' ' ||v_parametro_sucursales,get_json('__hash__',json2),v_in_fecha_inicio::varchar);
                end if;
	end if;
	--raise notice 'consulta1=% where1=% order_excel1=%',consulta1,where1,order_excel1;
	--query_data1:=replace(replace('SELECT array_to_json(array_agg(row_to_json(sql))) FROM (' ||  consulta1 || where1 || order_excel1||') sql',chr(10),''),chr(13),'');
	query_data1:=replace(replace('/*'||chr(36)||chr(36)||chr(36)||'TABLA'||chr(36)||chr(36)||chr(36)||'*/  SELECT array_to_json(array_agg(row_to_json(sql))) FROM (' ||  consulta1 || where1 || order_excel1||') sql',chr(10),''),chr(13),'');
	if query_data1 is null then
		json2:=logjson(json2,'query_data1 nulo '||substring(coalesce(consulta1,'consulta1_nulo'),1,20)||' '||substring(coalesce(where1,'where1_nulo'),1,20)||' '||coalesce(order_excel1,'order_excel1_nulo'));
		json2:=put_json(json2,'MENSAJE_RESPUESTA','Falla Consultar Data. Reintente');
		json2:=put_json(json2,'CODIGO_RESPUESTA','2');
		json2:=put_json(json2,'__SECUENCIAOK__','0');
		json2:=responde_pantalla_15102(json2);
		return json2;
	end if;
	json2:=put_json(json2,'PATRON_QUERY',encode_hex(query_data1));	
	json2:=put_json(json2,'JSON5_QUERY',encode_hex('{}'));
	json2:=logjsonfunc(json2,'QUERY_RS='||get_json('QUERY_RS',json2));
	json2:=logjsonfunc(json2,'TABLAS_HOY='||get_json('TABLAS_HOY',json2));
	json2:=logjsonfunc(json2,'flag_contar='||get_json('flag_contar',json2));

	json2:=put_json(json2,'CAT_EST','');
	if (vin_folio<>'') then
		query_data1:=replace(query_data1,chr(36)||chr(36)||chr(36)||'TABLA'||chr(36)||chr(36)||chr(36),'dte_recibidos');
		--perform logfile('CUENTA '||query_data1);
		--json2:=logjsonfunc(json2,'query_data1='||query_data1);
		json2:=put_json(json2,'QUERY_DATA',query_data1);
		json2:=put_json(json2,'CAT_EST','FOLIO_NORMAL_REC');
		--json2:=put_json(json2,'__TOTAL_RESP_ESPERADAS__','2');
                --json2:=put_json(json2,'__SECUENCIAOK__','30');
		json2:=put_json(json2,'__SECUENCIAOK__','90');
		json2:=put_json(json2,'__CONTADOR__','0');
		if get_json('rutUsuario',json2)='17597643' then
		json2:=logjson(json2,'QUERY17597643 '||query_data1);
		end if;
	elsif (flag_prea1) then
		--json2:=logjsonfunc(json2,'query_data1='||query_data1);
		json2:=put_json(json2,'CAT_EST','PREA');
		json2:=put_json(json2,'__SECUENCIAOK__','70');
		json2:=put_json(json2,'QUERY_DATA',query_data1);
		json2:=put_json(json2,'__CONTADOR__','0');
		json2:=put_json(json2,'__aux_tablas','[]');
	elsif (vin_tipo_dte='801') then
		json2:=put_json(json2,'CAT_EST','OC_REC');
		json2:=put_json(json2,'__SECUENCIAOK__','70');
		json2:=put_json(json2,'QUERY_DATA',query_data1);
		if get_json('rutUsuario',json2)='17597643' then
		json2:=logjson(json2,'QUERY17597643 '||query_data1);
		end if;
		json2:=logjsonfunc(json2,query_data1);
		json2:=put_json(json2,'__CONTADOR__','0');
		json2:=put_json(json2,'__aux_tablas','[]');
	elsif (get_json('QUERY_RS',json2)='') then
		json2:=put_json(json2,'__SECUENCIAOK__','50');
	elsif (get_json('BASE_RS',json2)='BASE_REDSHIFT_RECIBIDOS') then
		json2:=put_json(json2,'CAT_EST','CUENTA_RECIBIDOS');
		json2:=put_json(json2,'__TOTAL_RESP_ESPERADAS__','2');
		json2:=put_json(json2,'__SECUENCIAOK__','30');
		if get_json('rutUsuario',json2)='17597643' then
		json2:=logjson(json2,'QUERY17597643 '||replace(query_data1,chr(10),' '));
		end if;
	else
		json2:=logjsonfunc(json2,'ERROR: Viene QUERY_RS pero no viene BASE_RS Reconocida');
		json2:=put_json(json2,'__SECUENCIAOK__','50');
	end if;
	--Criterios de busqueda para reportes
	crit_busq1:='<b>Desde=</b>' || vin_fstart || ' <b>Hasta=</b>' || vin_fend || ' <b>Tipo Fecha=</b>' || vin_tipo_fecha || '<br>';
	if(v_tipo_dte<>'') then
		crit_busq1:=crit_busq1||' <b>Tipo Doc=</b>' || v_tipo_dte;
	end if;
	if(v_estado<>'') then
		crit_busq1:=crit_busq1||' <b>Estado=</b>' || v_estado;
	end if;
	if(v_in_rut_receptor<>'') then
		crit_busq1:=crit_busq1||' <b>Receptor=</b>' || v_in_rut_receptor;
	end if;

	crit_busq1:='<b>Desde=</b>'||vin_fstart||'  <b>Hasta=</b>'||vin_fend||', <b>Tipo Fecha=</b>'||vin_tipo_fecha||'<br>';
	crit_busq1:=crit_busq1||'<b>Estado=</b>'||v_glosa_estado||'<b>Tipo Doc=</b>'||replace(vin_tipo_dte,'*','Todos(menos Boletas)')||'<br>';
	crit_busq1:=crit_busq1||'<b>Receptor=</b>'||vin_rut_receptor||'<b>Emisor=</b>'||vin_rut_emisor||'<b>Folio=</b>'||vin_folio||'<br>';
	 if (is_number(get_json('PARAMETRO5',json2))) then
                crit_busq1:=crit_busq1||'<b>Nomina=</b>'||get_json('PARAMETRO5',json2)||'<br>';
        end if;
	crit_busq1:=crit_busq1||'<b>Parametros </b>'||texto_filtro_params;
	json2:=put_json(json2,'criterio_busqueda_excel',crit_busq1);
	json2:=put_json(json2,'criterio_busqueda',texto_filtro);
	return json2;
end;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION resultado_cuenta_pivote_query_15102(json)
returns json as $$
declare
        json1           alias for $1;
	json2	json;
        aux     varchar;
        lista1  varchar;
        json4   varchar;
        total_base1     integer;
        total_rs1       integer;
        flag_contar     boolean;
        v_total_registros       varchar;
        tablas1         json;
        tablas_hoy1     varchar;
        tablas_hoy_json1        json;
	hash varchar;
	v_out_resultado	varchar;
	v_parametro_referencias1	varchar;
	v_parametro_adicional1	varchar;
        json3                   json;
        campo1           record;
        query1  varchar;
        query2  varchar;
        vin_estado      varchar;
        json5           json;
        j1      json;
        fecha_c1        varchar;
        i       integer;
        tabla1  varchar;
	par1	varchar;
	lista_new	json;
	flagx	boolean;
	total_rut_emisores	integer;
	flag_agrega_tabla	boolean;
	
	
	campo	record;
BEGIN
	json2:=json1;
	json2:=put_json(json2,'__FUNC__','resultado_cuenta');
	--Es el resultado local
	json4:=null;
	tablas_hoy1:=get_json('TABLAS_HOY',json2);
	v_total_registros:=get_json('v_total_registros',json2);
	total_base1:=0;
	flag_contar:=get_json('flag_contar',json2)::boolean;
	if get_json('RESULTADO_CUENTA_LOCAL',json2)<>'' then
		json2:=logjsonfunc(json2,'Hay Resultado Local');
		json2:=logjsonfunc(json2,'RESULTADO_CUENTA_LOCAL='||get_json('RESULTADO_CUENTA_LOCAL',json2));
		json4:=get_json('RESULTADO_CUENTA_LOCAL',json2);
		total_base1:=get_json('TOTAL_BASE_LOCAL',json2)::integer;
	end if;

	hash:=get_json('__hash__',json2);
	json2:=logjsonfunc(json2,'TOTAL_RES_JSON='||get_json('TOTAL_RES_JSON',json2)||' total_base1='||get_json('TOTAL_BASE_LOCAL',json2));
	--Si hay QUERY_RS, entoces deben venir resultados del Redshift
	if (get_json('QUERY_RS',json2)<>'')  then
		if (get_json('TOTAL_RES_JSON',json2)<>'2') then
			--Si falla el count en el RS guardo en las estadisticas
			perform graba_estadisticas_busqueda(json2,get_json('CAT_EST',json2),'NK');
			json2:=logjsonfunc(json2,'No hay resultados en RS');
			json2:=put_json(json2,'MENSAJE_RESPUESTA','Falla Conexión Base Datos.');
			json2:=put_json(json2,'CODIGO_RESPUESTA','2');
			json2:=put_json(json2,'__SECUENCIAOK__','0');
			json2:=responde_pantalla_15102(json2);
			return json2;
		end if;
		json3:=get_json('RES_JSON_2',json2)::json;
                if (get_json('STATUS',json3)='SIN_DATA') then
                        json3:=put_json('{}','STATUS','NK');
                else
                        json3:=put_json('{}','STATUS','OK');
                end if;
		--json3:=put_json('{}','STATUS','OK');
		--Si falla el count en el RS guardo en las estadisticas
		perform graba_estadisticas_busqueda(json2,get_json('CAT_EST',json2),'OK');
	end if;

	--Si no hay OK es porque no hay resultados en el RS
        if (get_json('STATUS',json3)<>'OK') then
		--Si no hay resultado local
                if (json4 is null) then
			json2:=logjsonfunc(json2,'No se encontraron registros.');
			json2:=put_json(json2,'MENSAJE_RESPUESTA','No se encontraron registros.');
			json2:=put_json(json2,'CODIGO_RESPUESTA','2');
			json2:=put_json(json2,'__SECUENCIAOK__','0');
			json2:=responde_pantalla_15102(json2);
			return json2;
                else
			json2:=logjsonfunc(json2,'Solo resultados locales');
                        --Solo quedan los emitidiso del dia de hoy
                        lista1:='('||json4||')';
                        --Si solo hay registros en la base local, se agrega el mes actual
                        if(tablas_hoy1<>'') then
                                BEGIN
                                        tablas1:=('['||tablas_hoy1||']')::json;
                                EXCEPTION WHEN OTHERS THEN
                                        tablas1:=put_json_list('[]','"'||to_char(now(),'YYMM')||'"');
                                END;
                        else
                                tablas1:=put_json_list('[]','"'||to_char(now(),'YYMM')||'"');
                        end if;
			v_total_registros:=(total_base1)::varchar;
                        json2:=put_json(json2,'__aux_hash',encripta_hash_evento_VDC('total_registros='||(total_base1)::varchar||'&'||'total_base1='||total_base1::varchar||'&'||'total_base2=0&'));
			
                end if;
        else
                --tablas1:='[]';
                --Separo resultados
		json2:=logjsonfunc(json2,'Junto Resultados');
                json5:=get_json('RES_JSON_1',json2)::json;
                if (flag_contar) then
                        total_rs1:=get_json('count',json5)::integer;
			v_total_registros:=(total_rs1+total_base1)::varchar;
                        json2:=put_json(json2,'__aux_hash',encripta_hash_evento_VDC('total_registros='||(total_rs1+total_base1)::varchar||'&'||'total_base1='||total_base1::varchar||'&'||'total_base2='||total_rs1::varchar||'&'));
			json2:=logjsonfunc(json2,'__aux_hash='||get_json('__aux_hash',json2));
                else
                        json2:=put_json(json2,'__aux_hash',hash);
                end if;
		json2:=logjsonfunc(json2,'Total RS1='||coalesce(total_rs1::varchar,'')||' TotalLocal='||coalesce(total_base1::varchar,'')|| ' TotalRegistros='||coalesce(v_total_registros::varchar,''));

		tablas1:='[]';
		json3:=get_json('RES_JSON_2',json2)::json;
		if (get_json('STATUS',json3)='SIN_DATA') then
			json2:=logjsonfunc(json2,'RS responde SIN_DATA');
		elsif (get_json('TOTAL_REGISTROS',json3)='1') then
			json2:=logjsonfunc(json2,'TOTAL_REGISTROS=1	'||json3::varchar);
			lista1:=get_json('c',json3);
			if (lista1<>'-1') then
				tablas1:=('["'||substring(get_json('m',json3),3,4)||'"]')::json;
			end if;
		else
			json2:=logjsonfunc(json2,'TOTAL_REGISTROS= LISTA');
			--Recorro la lista de codigos txel
			json5:=get_json('LISTA',json3);
			lista1:='';
			i:=0;
			aux:=get_json_index(json5,i);
			while (aux<>'') loop
				--Agrega a la lista el codigo txel
				if (lista1='') then
					lista1:=get_json('c',aux::json);
				else
					lista1:=lista1||','||get_json('c',aux::json);
                                end if;
                                if (strpos(tablas1::varchar,'"'||substring(get_json('m',aux::json),3,4)||'"')=0) then
                                       tablas1:=put_json_list(tablas1,'"'||substring(get_json('m',aux::json),3,4)||'"');
                                end if;
                                i:=i+1;
                	        aux:=get_json_index(json5,i);
                        end loop;
                end if;
                --Si tengo resagados el dia de hoy con fecha de emision anterior
                if (json4 is not null and json4<>'') then
                        lista1:='('||json4::varchar||','||lista1||')';
                        --Si ya esta el mes actual en la lista, mo lo agregue
                        if (tablas1 is null) then
                                tablas1:=put_json_list('[]','"'||to_char(now(),'YYMM')||'"');
                        elsif (strpos(tablas1::varchar,'"'||to_char(now(),'YYMM')||'"')=0) then
                                tablas1:=put_json_list(tablas1,'"'||to_char(now(),'YYMM')||'"');
                        end if;
                else
                        --tablas1:=put_json_list(tablas1,to_char(now(),'YYMM'));
                        lista1:='('||lista1||')';
                end if;
                if(tablas_hoy1 is not null and tablas_hoy1<>'') then
                        BEGIN
                                tablas_hoy_json1:=('['||tablas_hoy1||']')::json;
                                i:=0;
                                aux:=get_json_index(tablas_hoy_json1,i);
                                while (aux<>'')loop
                                        if(strpos(tablas1::varchar,'"'||aux||'"')=0) then
                                                tablas1:=put_json_list(tablas1,'"'||aux||'"');
                                        end if;
                                        i:=i+1;
                                        aux:=get_json_index(tablas_hoy_json1,i);
                                end loop;
                        EXCEPTION WHEN OTHERS THEN
                                perform logfile('TABLA_HOY1 no est JSON');
                        END;
                end if;
        end if;

	--Cuando solo se requiere contar...
        if get_json('FLAG_SOLO_COUNT',json2)='SI' then
                json3:='{}';
                json3:=put_json(json3,'TOTAL',v_total_registros::varchar);
                json3:=put_json(json3,'criterio_busqueda_excel',get_json('criterio_busqueda_excel',json2));
                json2:=put_json(json2,'__SECUENCIAOK__','0');
                return response_requests_6000('1','TEST',json3::varchar,json2);
        end if;

	if (count_array_json(tablas1)=0) then
		json2:=logjsonfunc(json2,'No se encontraron registros.');
		json2:=put_json(json2,'MENSAJE_RESPUESTA','No se encontraron registros.');
		json2:=put_json(json2,'CODIGO_RESPUESTA','2');
		json2:=put_json(json2,'__SECUENCIAOK__','0');
		json2:=responde_pantalla_15102(json2);
		return json2;
	end if;

        --perform logfile('cuenta_offset_redshift_emitidos_codigo_txel tablas1='||tablas1::varchar);
        json2:=put_json(json2,'v_total_registros',v_total_registros::varchar);
        json2:=put_json(json2,'__aux_tablas',tablas1::varchar);
        json2:=put_json(json2,'__aux_filtro_codigos',lista1);
	json2:=put_json(json2,'__hash__',get_json('__aux_hash',json2));

	json2:=logjsonfunc(json2,'TABLAS= '||tablas1::varchar||' filtro_codigos='||lista1::varchar||' __hash__='||get_json('__aux_hash',json2)||' Total='||get_json('v_total_registros',json2));	

	
        --BUSQUEDA--
        --QUERY--
        --NOMINAS--
	query2:=decode_hex(get_json('PATRON_QUERY',json2));
	json5:=decode_hex(get_json('JSON5_QUERY',json2))::varchar::json;
	json5:=put_json(json5,'CODIGO_TXEL',' codigo_txel in '|| get_json('__aux_filtro_codigos',json2));
	vin_estado:=get_json('ESTADO',json2);

	json2:=logjsonfunc(json2,'Busca Generico');

	--Inserto valores en el json para hacer el loop con las tablas
	json2:=put_json(json2,'QUERY2',encode_hex(query2::varchar));
	json2:=put_json(json2,'JSON5',encode_hex(json5::varchar));
	if (vin_estado='RIMP') then
		json2:=put_json(json2,'PREFIJO_TABLA','dte_recibidos_importados_');
		json2:=put_json(json2,'TABLA_CONFIGURACION','config_tabla_recibidos_importados');
	else
		json2:=put_json(json2,'PREFIJO_TABLA','dte_recibidos_');
                json2:=put_json(json2,'TABLA_CONFIGURACION','config_tabla_recibidos');
	end if;

	--Saco la primera tabla
	i:=0;
	j1:=get_json('__aux_tablas',json2);
	aux:=get_json_index(j1,i);
	
	fecha_c1:=get_json_index(j1,i);
	tabla1:=get_json('PREFIJO_TABLA',json2)||fecha_c1;
	json5:=put_json(json5,'TABLA',tabla1);	
	json2:=put_json(json2,'TABLA',tabla1);	
	query1:=remplaza_tags_json_c(json5,query2);
	json2:=put_json(json2,'QUERY_DATA',query1);
	execute 'select parametro_motor from '||get_json('TABLA_CONFIGURACION',json2)||' where periodo_desde<='||split_part(fecha_c1,'_',1)||' and periodo_hasta>='||split_part(fecha_c1,'_',1) into par1;
		
	json2:=put_json(json2,'PARAMETRO_TABLA',par1::varchar);
	json2:=put_json(json2,'CAT_EST',coalesce(par1::varchar,'LOCAL')||'__'||tabla1);
	if (par1='BASE_RECIBIDOS_HISTORICOS') then
		json2:=logjson(json2,'query='||query1);
		json2:=put_json(json2,'__SECUENCIAOK__','72');
	elsif (par1='BASE_AMAZON_IMPORTADOS') then
		json2:=put_json(json2,'__SECUENCIAOK__','74');
	else
		json2:=logjson(json2,'QUERY '||query1);
                --DAO 20201001 para los reportes vamos a la base de replica
		--if get_json('rutUsuario',json2)='17597643' then
		if is_number(get_json('id_reporte',json2)) then
                        json2:=put_json(json2,'__SECUENCIAOK__','71');
                else
                        json2:=put_json(json2,'__SECUENCIAOK__','70');
                end if;

		if get_json('rutUsuario',json2)='17597643' then
			json2:=logjson(json2,'QUERY17597643 '||query1);
		end if;
	end if;
	json2:=put_json(json2,'__TOTAL_RESP_ESPERADAS__','1');
	json2:=put_json(json2,'__CONTADOR__','1');
	json2:=put_json(json2,'RES_JSON_1','');
	json2:=logjsonfunc(json2,'Ejecuta '||coalesce(par1,'LOCAL')||' Tabla '||tabla1);
	return json2;

	--Vamos a hacer el loop
end;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION genera_querys_cuenta_codigo_txel_15102(json,integer,varchar,varchar,varchar,varchar,varchar,varchar,varchar,varchar,varchar,varchar,varchar,varchar,varchar)
returns json as
$$
declare
        json1           alias for $1;
        v_in_offset1    alias for $2;
        v_in_cant_reg   alias for $3;
        v_parametro_rut_emisor  alias for $4;
        v_tipo_dte      alias for $5;
        v_parametro_tipo_dte    alias for $6;
        v_parametro_var alias for $7;
        fecha_in1       alias for $8;
        v_estado        alias for $9;
        tipo_dia_ind1   alias for $10;
        v_in_rut_receptor       alias for $11;
        v_parametro_referencias alias for $12;
        v_parametro_adicional alias for $13;
        hash    alias for $14;
        fecha_desde1    alias for $15;
        json2           json;
        --v_total1        integer;
        --flag_offset     boolean;
        desde1          integer;
        hasta1          integer;
        limit1          integer;
        of1             integer;
        --flag_ok boolean;
        campo   record;
        query1  varchar;
        query2  varchar;
        rut1    varchar;
        dia1    integer;
        i       integer;
        aux     varchar;
        tipo_dia1       varchar;
        flag1   boolean;
        json4   varchar;
        total_base1     integer;
        total_rs1       integer;
        paginas_base1   integer;
        sobra_base1     integer;
        flag_contar     boolean;
        json_hash1      json;
        v_total_registros       varchar;
        total_base2     integer;
        tabla_base1             varchar;
        tabla_base2             varchar;
        campo1          varchar;
        campo2          varchar;
        group2          varchar;
        tablas1         json;
        filtro_dia1     varchar;
        filtro_tipo_dte1        varchar;
        cod_act1        varchar;
        count_act1      bigint;
        filtro_rs1      varchar;
        tabla_rs1       varchar;
        flag_saca_tablas_hoy boolean;
        campo_hoy1      record;
        campo_tabla_rut record;
        tablas_hoy1     varchar;
        tablas_hoy_json1        json;
	flag_nomina1	boolean;
	join1	varchar;
begin
        json2:=json1;
	json2:=put_json(json2,'__FUNC__','genera_query');
        json4:=null;
        tabla_rs1:=null;
	v_total_registros:='0';
        filtro_rs1:='';
        count_act1:=0;
        total_rs1:=0;
        tablas_hoy1:=null;
        flag_saca_tablas_hoy:=false;
	flag_nomina1:=false;
        --perform logfile('cuenta_offset_redshift_emitidos_codigo_txel json1='||json1::varchar||' v_in_offset1='||v_in_offset1::varchar||' v_in_cant_reg='||v_in_cant_reg::varchar||' v_parametro_rut_emisor='||v_parametro_rut_emisor||' v_tipo_dte='||v_tipo_dte||' v_parametro_tipo_dte='||v_parametro_tipo_dte||' v_parametro_var='||v_parametro_var||' fecha_in1='||fecha_in1||' v_estado='||v_estado||' tipo_dia_ind1='||tipo_dia_ind1||' v_in_rut_receptor='||v_in_rut_receptor||' v_parametro_referencias='||v_parametro_referencias||' v_parametro_adicional='||v_parametro_adicional||' hash='||hash);

        if (v_in_offset1<0) then
                json2:=put_json(json2,'__aux_filtro_codigos',' (0) and 1=0 ');
                json2:=put_json(json2,'v_total_registros','0');
		json2:=put_json(json2,'QUERY_RS','');
        	flag_contar:=false;
		json2:=put_json(json2,'flag_contar',flag_contar::varchar);
                return json2;
        end if;

        --Si viene el hash, uso los datos que calcule previamente
        flag_contar:=true;
        if (hash<>'') then
		json2:=logjsonfunc(json2,'Viene HASH no cuenta '||coalesce(hash,'HASH NULO'));
                json_hash1:='{}';
                json_hash1:=put_json(json_hash1,'QUERY_STRING',desencripta_hash_evento_VDC(hash));
                json_hash1:=get_parametros_get_json(json_hash1);
                if (get_json('total_base1',json_hash1)='') then
                        json2:=put_json(json2,'__aux_filtro_codigos',' (0) and 1=0 ');
                        json2:=put_json(json2,'__aux_v_total','0');
			json2:=put_json(json2,'flag_contar',flag_contar::varchar);
                        return json2;
                end if;
                total_base1:=get_json('total_base1',json_hash1);
                total_base2:=get_json('total_base2',json_hash1);
                v_total_registros:=get_json('total_registros',json_hash1);
                flag_contar:=false;
                --Si me paso de paginas
                if (v_in_offset1>v_total_registros::integer) then
                        json2:=put_json(json2,'__aux_filtro_codigos',' (0) and 1=0 ');
                        json2:=put_json(json2,'__aux_v_total',v_total_registros);
			json2:=put_json(json2,'flag_contar',flag_contar::varchar);
                        return json2;
                end if;
                json2:=put_json(json2,'__aux_tablas',get_json('tablas',json_hash1));
        end if;
	

        rut1:='';
        rut1:=replace(v_parametro_rut_emisor,'rut_emisor',' and rut');
        --v_total1:=0;
        --flag_offset:=true;
        desde1:=v_in_offset1;
        hasta1:=v_in_offset1+v_in_cant_reg::integer;
        limit1:=0;
        of1:=0;
        --flag_ok:=false;
        if (tipo_dia_ind1='E') then
                tipo_dia1:='_emision';
        elsif (tipo_dia_ind1='R') then
                tipo_dia1:='_recepcion_sii';
        else
                tipo_dia1:='';
        end if;
        dia1:=to_char(now(),'YYYYMMDD');

        --Si tiene dia de hoy
        execute 'select to_char(now(),''YYYYMMDD'')::integer in '||fecha_in1 into flag1;
        --Contamos en redshift
        if (get_json('flag_recibidos',json1)='SI' and get_json('flag_importado',json1)<>'SI') then
                tabla_base1:='dte_recibidos';
                tabla_base2:='dte_recibidos';
		json2:=put_json(json2,'BASE_RS','BASE_REDSHIFT_RECIBIDOS');
                if(get_json('tabla_mes',json2)='SI') then
                        campo1:='codigo_txel';
                        campo2:=',mes as m ';
                        group2:=' group by 2 order by 2';
                else
                        campo1:='codigo_txel';
                        campo2:='';
                        group2:='';
                end if;
                tabla_rs1:='dte_recibidos_actualizacion2';
		json2:=put_json(json2,'flag_act','SI');
        elsif (get_json('flag_recibidos',json1)='SI' and get_json('flag_importado',json1)='SI') then
                tabla_base1:='dte_recibidos_importados_generica';
                tabla_base2:='dte_recibidos_importados';
		json2:=put_json(json2,'BASE_RS','BASE_REDSHIFT_RECIBIDOS');
                campo1:='codigo_txel';
                campo2:=',mes as m ';
                group2:=' group by 2 order by 2';
        elsif (get_json('flag_importado',json1)='SI' and (strpos(v_tipo_dte,'39')=0 and strpos(v_tipo_dte,'41')=0)) then
                tabla_base1:='dte_emitidos_importados_generica';
                tabla_base2:='dte_emitidos_importados';
		json2:=put_json(json2,'BASE_RS','BASE_REDSHIFT_EMITIDOS');
                campo1:='codigo_txel';
                campo2:=',mes as m ';
                group2:=' group by 2 order by 2';
        elsif (get_json('flag_importado',json1)='SI' and (strpos(v_tipo_dte,'39')>0 or strpos(v_tipo_dte,'41')>0)) then
                tabla_base1:='dte_boletas_importadas_generica';
                tabla_base2:='dte_boletas_importadas';
		json2:=put_json(json2,'BASE_RS','BASE_REDSHIFT_BOLETAS');
                campo1:='codigo_txel';
                campo2:=',mes as m ';
                group2:=' group by 2 order by 2';
        elsif (strpos(v_tipo_dte,'39')>0 or strpos(v_tipo_dte,'41')>0) then
                flag_saca_tablas_hoy:=true;
                tabla_base1:='dte_boletas_diarias';
                select * into campo_tabla_rut from rut_tabla_boletas_rs where rut_boleta=get_json('rutCliente',json2)::integer;
                if found then
                        tabla_base2:='dte_boletas_'||get_json('rutCliente',json2);
                else
                        tabla_base2:='dte_boletas';
                end if;
		json2:=put_json(json2,'BASE_RS','BASE_REDSHIFT_BOLETAS');
                campo1:='codigo_txel';
                campo2:=',mes_emision as m ';
                group2:=' group by 2 order by 2';
        elsif (get_json('flag_errores',json1)='SI') then
                tabla_base1:='dte_emitidos_errores';
                tabla_base2:='dte_emitidos_errores';
		json2:=put_json(json2,'BASE_RS','BASE_REDSHIFT_RECIBIDOS');
                campo1:='codigo_txel';
                campo2:='';
                group2:='';
        else
                tabla_base1:='dte_emitidos';
                --tabla_base2:='dte_emitidos';
                select * into campo_tabla_rut from rut_tabla_boletas_rs where rut_boleta=get_json('rutCliente',json2)::integer;
                if found then
                        tabla_base2:='dte_emitidos_'||get_json('rutCliente',json2);
                else
                        tabla_base2:='dte_emitidos';
                end if;
		json2:=put_json(json2,'BASE_RS','BASE_REDSHIFT_EMITIDOS');
                campo1:='codigo_txel';
                campo2:=',mes as m ';
                group2:=' group by 2 order by 2';
                tabla_rs1:='dte_emitidos_actualizacion';
        end if;
        if(is_number(get_json('id_masivo',json2)) and get_json('id_masivo',json2)<>'0')then
		flag_nomina1:=true;
		tabla_base1:=' (select a.* from (select * from busqueda_masiva_detalle where id='||get_json('id_masivo',json2)||') b left join '||tabla_base1||' a on a.rut_emisor=b.rut_emisor and a.tipo_dte=b.tipo_dte and a.folio=b.folio) '||tabla_base1;
		join1:=' ) a join (select * from busqueda_masiva_detalle where id='||get_json('id_masivo',json2)||') b on a.rut_emisor=b.rut_emisor and a.tipo_dte=b.tipo_dte and a.folio=b.folio ) '||tabla_base2;
	end if;

        --Si viene folio solo para emitidos
        if(strpos(v_parametro_rut_emisor,'folio')>0) then
                if (get_json('flag_recibidos',json1)='SI') then
                        --Para los recibidos, noo se consdera la fecha si viene l folio, pero si el tipo_dte
                        filtro_dia1:='';
                        filtro_tipo_dte1:=' and tipo_dte in '||v_tipo_dte||' ';
                else
                        filtro_dia1:='';
                        filtro_tipo_dte1:='';
                end if;
        else
                filtro_dia1:=' and dia'||tipo_dia1||' in '||fecha_in1;
                filtro_tipo_dte1:=' and tipo_dte in '||v_tipo_dte||' ';
        end if;

        if(get_json('flag_act',json2)='SI' and tabla_rs1 is not null and (v_estado <>'' and v_estado is not null and strpos(v_estado,'1=1')=0)) then
                --Sacamos los codigos txel que estan en la tabla de actualizacion de la busqueda q va al redshift
                --perform logfile('DAO_ACT select string_agg(codigo_txel::varchar,'',''),count(*) from '||tabla_rs1||' where 1=1 '||v_in_rut_receptor);
		if get_json('rutUsuario',json2)='17597643' then
			perform logfile('cuenta_offset_redshift_emitidos_codigo_txel v_in_offset1='||v_in_offset1::varchar||' v_in_cant_reg='||v_in_cant_reg::varchar||' v_parametro_rut_emisor='||v_parametro_rut_emisor||' v_tipo_dte='||v_tipo_dte||' v_parametro_tipo_dte='||v_parametro_tipo_dte||' v_parametro_var='||v_parametro_var||' fecha_in1='||fecha_in1||' v_estado='||v_estado||' tipo_dia_ind1='||tipo_dia_ind1||' v_in_rut_receptor='||v_in_rut_receptor||' v_parametro_referencias='||v_parametro_referencias||' v_parametro_adicional='||v_parametro_adicional||' hash='||hash);
		end if;
                execute 'select string_agg(codigo_txel::varchar,'','') from '||tabla_rs1||' where 1=1 '||v_in_rut_receptor into cod_act1;
                execute 'select count(*) from '||tabla_rs1||' where '||v_estado||v_parametro_rut_emisor||' and tipo_dte in '||v_tipo_dte||' '||filtro_dia1||' '||v_in_rut_receptor||' '||v_parametro_tipo_dte||' '||v_parametro_var||v_parametro_referencias||v_parametro_adicional||' and codigo_txel is not null' into count_act1;
                if (cod_act1 is not null) then
                        filtro_rs1:=' and codigo_txel not in ('||cod_act1||')';
                end if;
                --perform logfile('DAO_ACT '||' v_estado='||v_estado||' '||filtro_rs1||' '||coalesce(count_act1,0));
        end if;
	--Siempre cuento arriba para el total
	if (flag_contar) then
		if (tipo_dia1 in ('_emision','_recepcion_sii') or (flag1 and tipo_dia1='')) then
			if flag_nomina1 then
				query1:='select count(*) from ((select * from '||tabla_base1||' where dia='||dia1||' and '||v_estado||v_parametro_rut_emisor||' and tipo_dte in '||v_tipo_dte||' '||filtro_dia1||' '||v_in_rut_receptor||' '||v_parametro_tipo_dte||' '||v_parametro_var||v_parametro_referencias||v_parametro_adicional||' and codigo_txel is not null '||join1;
			else
				query1:='select count(*) from '||tabla_base1||' where dia='||dia1||' and '||v_estado||v_parametro_rut_emisor||' and tipo_dte in '||v_tipo_dte||' '||filtro_dia1||' '||v_in_rut_receptor||' '||v_parametro_tipo_dte||' '||v_parametro_var||v_parametro_referencias||v_parametro_adicional||' and codigo_txel is not null';
			end if;
--			json2:=logjson(json2,'QUERY_LOCAL '||query1);
			execute query1 into total_base1;
		else
			total_base1:=0;
		end if;
		--Se va a ejcutar en el RS
		if(get_json('flag_act',json2)='SI') then
			--Agregamos el filtro para que no cuente los q estan actualizados abajo y no en el RS
			if flag_nomina1 then
				query1:='select count(*) from ((select * from '||tabla_base2||' where '||v_estado||v_parametro_rut_emisor||filtro_tipo_dte1||filtro_dia1||' '||v_in_rut_receptor||' '||v_parametro_tipo_dte||' '||v_parametro_var||v_parametro_referencias||v_parametro_adicional||filtro_rs1||join1;
			else
				query1:='select count(*) from '||tabla_base2||' where '||v_estado||v_parametro_rut_emisor||filtro_tipo_dte1||filtro_dia1||' '||v_in_rut_receptor||' '||v_parametro_tipo_dte||' '||v_parametro_var||v_parametro_referencias||v_parametro_adicional||filtro_rs1;
			end if;
			--perform logfile('DAO_ACT '||query1);
		else
			if flag_nomina1 then
				query1:='select count(*) from ((select * from '||tabla_base2||' where '||v_estado||v_parametro_rut_emisor||filtro_tipo_dte1||filtro_dia1||' '||v_in_rut_receptor||' '||v_parametro_tipo_dte||' '||v_parametro_var||v_parametro_referencias||v_parametro_adicional||join1;
				perform logfile('QUERY_NOMINA '||query1);
			else
				query1:='select count(*) from '||tabla_base2||' where '||v_estado||v_parametro_rut_emisor||filtro_tipo_dte1||filtro_dia1||' '||v_in_rut_receptor||' '||v_parametro_tipo_dte||' '||v_parametro_var||v_parametro_referencias||v_parametro_adicional;
			end if;
		end if;
	else
		query1:='select 0 as count';
	end if;
	if(get_json('flag_act',json2)='SI') then
		--Sumamos los de la tabla de actualizacion como base1
		total_base1:=total_base1+count_act1;
		--perform logfile('DAO_ACT '||total_base1::varchar);
	end if;

	limit1:=v_in_cant_reg::integer;
	of1:=v_in_offset1::integer;

	paginas_base1:=total_base1/limit1;
	sobra_base1:=total_base1%limit1;
	--offset 0-100-200
	--limit 100
	--Dependiendo de cuantos mostrar, hay que ir o no a buscar codigos al redshift
	--Alcanza con lo que tenemos en la base1
	if get_json('FLAG_SOLO_COUNT',json2)='SI' then
                query1:=query1||';select -1 as c';
                json4:='[]';
	elsif (of1+limit1<=paginas_base1*limit1) then
		--Solo cuento el total
		--Solo muestro los del dia
		if(get_json('flag_act',json2)='SI' and filtro_rs1<>'') then
			--Hacemos un union entre la base local y los actualizados para sacar el limit y offset
			query2:='select string_agg('||campo1||'::varchar,'','') from (
			(select '||campo1||' from '||tabla_base1||' where dia='||dia1||' and '||v_estado||v_parametro_rut_emisor||' and tipo_dte in '||v_tipo_dte||' '||filtro_dia1||' '||v_in_rut_receptor||' '||v_parametro_tipo_dte||' '||v_parametro_var||v_parametro_referencias||v_parametro_adicional|| ' and codigo_txel is not null)union
			(select '||campo1||' from '||tabla_rs1||' where '||v_estado||v_parametro_rut_emisor||' and tipo_dte in '||v_tipo_dte||' '||filtro_dia1||' '||v_in_rut_receptor||' '||v_parametro_tipo_dte||' '||v_parametro_var||v_parametro_referencias||v_parametro_adicional|| ' and codigo_txel is not null) order by 1 desc offset '||v_in_offset1||' limit '||v_in_cant_reg||'
			) sql';
			execute query2 into json4;
		else
			if(flag_saca_tablas_hoy) then
				query2:='select string_agg('||campo1||'::varchar,'','') as c,''"''||string_agg(distinct mes_emision::varchar,''","'')||''"'' as m from (select '||campo1||',substring(mes_emision::varchar,3) as mes_emision from '||tabla_base1||' where dia='||dia1||' and '||v_estado||v_parametro_rut_emisor||' and tipo_dte in '||v_tipo_dte||' '||filtro_dia1||' '||v_in_rut_receptor||' '||v_parametro_tipo_dte||' '||v_parametro_var||v_parametro_referencias||v_parametro_adicional|| ' and codigo_txel is not null order by 1 desc offset '||v_in_offset1||' limit '||v_in_cant_reg||') sql';
				if(get_json('rutUsu',json2)='7621836') then
					perform logfile('DAO_BOL Paso1');
				end if;
				execute query2 into campo_hoy1;
				if(get_json('rutUsu',json2)='7621836') then
					perform logfile('DAO_BOL Paso2');
				end if;
				json4:=campo_hoy1.c;
				tablas_hoy1:=campo_hoy1.m;
				json2:=put_json(json2,'TABLAS_HOY',tablas_hoy1);
				
			else
				query2:='select string_agg('||campo1||'::varchar,'','') from (select '||campo1||' from '||tabla_base1||' where dia='||dia1||' and '||v_estado||v_parametro_rut_emisor||' and tipo_dte in '||v_tipo_dte||' '||filtro_dia1||' '||v_in_rut_receptor||' '||v_parametro_tipo_dte||' '||v_parametro_var||v_parametro_referencias||v_parametro_adicional|| ' and codigo_txel is not null order by 1 desc offset '||v_in_offset1||' limit '||v_in_cant_reg||') sql';
				execute query2 into json4;
			end if;
		end if;
		query1:=query1||';select -1 as c';
	else
		--Se sacan los id de la base1 siempre y cuando sea la pagina intermedia
		if (sobra_base1>0 and of1=(paginas_base1)*limit1) then
			--Hacemos un union entre la base local y los actualizados para sacar el limit y offset
			if(get_json('flag_act',json2)='SI' and filtro_rs1<>'') then
				query2:='select string_agg('||campo1||'::varchar,'','') from ((select '||campo1||' from '||tabla_base1||' where dia='||dia1||' and '||v_estado||v_parametro_rut_emisor||' and tipo_dte in '||v_tipo_dte||' '||filtro_dia1||' '||v_in_rut_receptor||' '||v_parametro_tipo_dte||' '||v_parametro_var||v_parametro_referencias||v_parametro_adicional|| ' and codigo_txel is not null) union (select '||campo1||' from '||tabla_rs1||' where '||v_estado||v_parametro_rut_emisor||' and tipo_dte in '||v_tipo_dte||' '||filtro_dia1||' '||v_in_rut_receptor||' '||v_parametro_tipo_dte||' '||v_parametro_var||v_parametro_referencias||v_parametro_adicional|| ' and codigo_txel is not null) order by 1 desc offset '||v_in_offset1||' limit '||sobra_base1||' ) sql';
				execute query2 into json4;
			else
				if(flag_saca_tablas_hoy) then
					query2:='select string_agg('||campo1||'::varchar,'','') as c,''"''||string_agg(distinct mes_emision::varchar,''","'')||''"'' as m from (select '||campo1||',substring(mes_emision::varchar,3) as mes_emision from '||tabla_base1||' where dia='||dia1||' and '||v_estado||v_parametro_rut_emisor||' and tipo_dte in '||v_tipo_dte||' '||filtro_dia1||' '||v_in_rut_receptor||' '||v_parametro_tipo_dte||' '||v_parametro_var||v_parametro_referencias||v_parametro_adicional|| ' and codigo_txel is not null order by 1 desc offset '||v_in_offset1||' limit '||sobra_base1||') sql';
					execute query2 into campo_hoy1;
					json4:=campo_hoy1.c;
					tablas_hoy1:=campo_hoy1.m;
					json2:=put_json(json2,'TABLAS_HOY',tablas_hoy1);
				else
					query2:='select string_agg('||campo1||'::varchar,'','') from (select '||campo1||' from '||tabla_base1||' where dia='||dia1||' and '||v_estado||v_parametro_rut_emisor||' and tipo_dte in '||v_tipo_dte||' '||filtro_dia1||' '||v_in_rut_receptor||' '||v_parametro_tipo_dte||' '||v_parametro_var||v_parametro_referencias||v_parametro_adicional|| ' and codigo_txel is not null order by 1 desc offset '||v_in_offset1||' limit '||sobra_base1||') sql';
					execute query2 into json4;
				end if;
			end if;
			limit1:=limit1-sobra_base1;
			of1:=0;
			if flag_nomina1 then
				query1:=query1||';select '||campo1||' as c'||campo2||' from ((select * from '||tabla_base2||' where '||v_estado||v_parametro_rut_emisor||filtro_tipo_dte1||filtro_dia1||' '||v_in_rut_receptor||' '||v_parametro_tipo_dte||' '||v_parametro_var||v_parametro_referencias||v_parametro_adicional||join1|| ' order by 1 desc offset '||of1::varchar||' limit '||limit1::varchar;
			else
				query1:=query1||';select '||campo1||' as c'||campo2||' from '||tabla_base2||' where '||v_estado||v_parametro_rut_emisor||filtro_tipo_dte1||filtro_dia1||' '||v_in_rut_receptor||' '||v_parametro_tipo_dte||' '||v_parametro_var||v_parametro_referencias||v_parametro_adicional|| ' order by 1 desc offset '||of1::varchar||' limit '||limit1::varchar;
			end if;
		else
			--Se saca los id solo de la base2
			limit1:=v_in_cant_reg::integer;
			of1:=v_in_offset1::integer-paginas_base1::integer*v_in_cant_reg::integer-sobra_base1;
			if(get_json('flag_act',json2)='SI') then
				--Agregamos el filtro para que no cuente los q estan actualizados abajo y no en el RS
				if flag_nomina1 then
					query1:=query1||';select '||campo1||' as c'||campo2||' from ((select * from '||tabla_base2||' where '||v_estado||v_parametro_rut_emisor||filtro_tipo_dte1||filtro_dia1||' '||v_in_rut_receptor||' '||v_parametro_tipo_dte||' '||v_parametro_var||v_parametro_referencias||v_parametro_adicional||join1|| ' order by 1 desc offset '||of1::varchar||' limit '||limit1::varchar;
				else
					query1:=query1||';select '||campo1||' as c'||campo2||' from '||tabla_base2||' where '||v_estado||v_parametro_rut_emisor||filtro_tipo_dte1||filtro_dia1||' '||v_in_rut_receptor||' '||v_parametro_tipo_dte||' '||v_parametro_var||v_parametro_referencias||v_parametro_adicional|| ' order by 1 desc offset '||of1::varchar||' limit '||limit1::varchar;
				end if;
			else
				if flag_nomina1 then
					query1:=query1||';select '||campo1||' as c'||campo2||' from ((select * from '||tabla_base2||' where '||v_estado||v_parametro_rut_emisor||filtro_tipo_dte1||filtro_dia1||' '||v_in_rut_receptor||' '||v_parametro_tipo_dte||' '||v_parametro_var||v_parametro_referencias||v_parametro_adicional||filtro_rs1||join1|| ' order by 1 desc offset '||of1::varchar||' limit '||limit1::varchar;
				else
					query1:=query1||';select '||campo1||' as c'||campo2||' from '||tabla_base2||' where '||v_estado||v_parametro_rut_emisor||filtro_tipo_dte1||filtro_dia1||' '||v_in_rut_receptor||' '||v_parametro_tipo_dte||' '||v_parametro_var||v_parametro_referencias||v_parametro_adicional||filtro_rs1|| ' order by 1 desc offset '||of1::varchar||' limit '||limit1::varchar;
				end if;
			end if;
		end if;
	end if;

	json2:=logjsonfunc(json2,'RESULTADO_CUENTA_LOCAL='||coalesce(json4::varchar,'JSON4 NULO')||' TOTAL_BASE_LOCAL='||coalesce(total_base1::varchar,'TOTAL_BASE_LOCAL NULO'));
	json2:=put_json(json2,'RESULTADO_CUENTA_LOCAL',json4::varchar);
	json2:=put_json(json2,'TOTAL_BASE_LOCAL',total_base1::varchar);
	json2:=put_json(json2,'flag_contar',flag_contar::varchar);
	json2:=put_json(json2,'v_total_registros',v_total_registros::varchar);
	json2:=logjsonfunc(json2,'v_total_registros='||v_total_registros::varchar);

	if(fecha_desde1::integer<dia1::integer or tipo_dia_ind1='E' or strpos(v_parametro_rut_emisor,'folio')>0)then
		json2:=put_json(json2,'QUERY_RS',query1);
		--perform logfile('CUENTA_REC '||query1);
		--json2:=put_json(json2,'IP_RS',get_json('__IP_CONEXION_CLIENTE__',json_par1));
		--json2:=put_json(json2,'PORT_RS',get_json('__IP_PORT_CLIENTE__',json_par1));
	else
		json2:=put_json(json2,'QUERY_RS','');
	end if;
        return json2;
end;
$$
LANGUAGE plpgsql;


