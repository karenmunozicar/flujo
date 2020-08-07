delete from isys_querys_tx where llave='15200';
CREATE or replace FUNCTION pivote_busqueda_15200(json) RETURNS json AS $$
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
        json2:=put_json(json2,'__SECUENCIAOK__','15200');
        return json2;
END;
$$ LANGUAGE plpgsql;

insert into isys_querys_tx values ('15200','10',9,1,'select query_local__arma_query_rds_15200(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('15200','20',42,1,'$$QUERY_DATA$$',0,0,0,9,1,30,30);
insert into isys_querys_tx values ('15200','22',44,1,'$$QUERY_DATA$$',0,0,0,9,1,30,30);
insert into isys_querys_tx values ('15200','30',9,1,'select armo_resultado_15200(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);


CREATE OR REPLACE FUNCTION public.respondo_pantalla_15200(json)
 RETURNS json
AS $$
declare
        json1                alias for $1;
        json2                   json;
        json3                   json;
BEGIN
        json2:=json1;
        if(get_json('CODIGO_RESPUESTA',json2)<>'1') then
                json3:='{}';
                json3:=put_json(json3,'datos_tabla','[]');
                json3:=put_json(json3,'total_regs','0');
                json3:=put_json(json3,'criterio_busqueda_excel',get_json('criterio_busqueda_excel',json2));
                json2:=response_requests_6000('1', get_json('MENSAJE_RESPUESTA',json2), json3::varchar,json2);
                return json2;
        end if;
        if get_json('v_out_resultado',json2)='' then
                json2:=logjsonfunc(json2,'Busqueda no exitosa, v_out_resultado es nulo');
                json2:=put_json(json2,'MENSAJE_RESPUESTA','No se encontraron registros..');
                json3:=put_json(json3,'datos_tabla','[]');
                json3:=put_json(json3,'total_regs','0');
                json3:=put_json(json3,'criterio_busqueda_excel',get_json('criterio_busqueda_excel',json2));
                json2:=response_requests_6000('1', get_json('MENSAJE_RESPUESTA',json2), json3::varchar,json2);
                return json2;
        end if;

        json3:=put_json(json3,'MENSAJE',get_json('MENSAJE',json2));
        json3:=put_json(json3,'cantregs','100');
        json3:=put_json(json3,'datos_tabla',get_json('v_out_resultado',json2));

        if get_json('COUNT_RDS',json2)<>'' then
                json3:=put_json(json3,'mostrar_mensaje_paginacion','SI');
                json3:=put_json(json3,'mostrar_paginador','false'); -- no muestra el paginador del datatables
                json3:=put_json(json3,'mostrar_info','false'); -- no muestra el mensaje de paginación
                json3:=put_json(json3,'mostrar_filtros','false'); -- no muestra el buscar del plugin
                json3:=put_json(json3,'flag_paginacion','NO');
                json3:=put_json(json3,'flag_paginacion_manual','SI');

                json2:=put_json(json2,'v_total_registros',(get_json('COUNT_RDS',json2)::bigint + get_json('COUNT_LOCAL',json2)::bigint)::varchar);

                json3:=put_json(json3,'offset',get_json('v_in_offset',json2));
                json3:=put_json(json3,'total_regs',get_json('v_total_registros',json2));
                json3:=put_json(json3,'cantidad_paginas',(get_json('v_total_registros',json2)::integer/100 + case when get_json('v_total_registros',json2)::integer%100>0 then 1 else 0 end)::varchar);
                json3:=put_json(json3,'cant_paginas',(get_json('v_total_registros',json2)::integer/100 + case when get_json('v_total_registros',json2)::integer%100>0 then 1 else 0 end)::varchar);
        else
                json3:=put_json(json3,'mostrar_mensaje_paginacion','SI');
                json3:=put_json(json3,'mostrar_paginador','true'); -- no muestra el paginador del datatables
                json3:=put_json(json3,'mostrar_info','true'); -- no muestra el mensaje de paginación
                json3:=put_json(json3,'mostrar_filtros','true'); -- no muestra el buscar del plugin
                json3:=put_json(json3,'flag_paginacion','SI');
                json3:=put_json(json3,'flag_paginacion_manual','NO');
        end if;

        json3:=put_json(json3,'flag_tipo_cuadro','GRILLA');
        json3:=put_json(json3,'caption_buscar','Buscar en esta pagina :');

        if get_json('__BOTONES_TABLA__',json2)<>'' then
                json3:=put_json(json3,'botones_tabla',get_json('__BOTONES_TABLA__',json2));
        end if;

        json2:=response_requests_6000('1', 'OK', json3::varchar,json2);

        return json2;
end
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION public.armo_resultado_15200(json)
 RETURNS json
AS $$
declare
        json1                alias for $1;
        json2                   json;
	v_out_resultado	varchar;
BEGIN
	json2:=json1;
	--json2:=logjson(json2,'json2='||json2::varchar);
	if (get_json('__TOTAL_RESP_ESPERADAS__',json2)<>get_json('TOTAL_RES_JSON',json2)) then
		json2:=put_json(json2,'CODIGO_RESPUESTA','2');
		json2:=put_json(json2,'MENSAJE_RESPUESTA','Falla Busqueda');
		return respondo_pantalla_15200(json2);
        else
		if get_json('__TOTAL_RESP_ESPERADAS__',json2)='2' then
                	v_out_resultado:=get_json('array_to_json',get_json('RES_JSON_2',json2)::json);
			json2:=put_json(json2,'COUNT_RDS',get_json('count',get_json('RES_JSON_1',json2)::json));
		else
                	v_out_resultado:=get_json('array_to_json',get_json('RES_JSON_1',json2)::json);
		end if;
                --Si viene data
                if v_out_resultado<>'' then
                        if get_json('v_out_resultado',json2)='' then
                                json2:=put_json(json2,'v_out_resultado',v_out_resultado::varchar);
                        else
                                json2:=put_json(json2,'v_out_resultado',json_merge_lists(get_json('v_out_resultado',json2),v_out_resultado::varchar));
                        end if;
                end if;
        end if;
	json2:=put_json(json2,'CODIGO_RESPUESTA','1');
	json2:=put_json(json2,'MENSAJE_RESPUESTA','OK');
	return respondo_pantalla_15200(json2);
        return json2;
end
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION public.query_local__arma_query_rds_15200(json)
 RETURNS json
AS $$
declare
        json1                alias for $1;
        json2                   json;
        app1    varchar;
        campo   record;
        campos1 varchar;
        from_where_local1       varchar;
        from_where_rds1         varchar;
        json_local      json;
        json3   json;
BEGIN
        json2:=json1;
        app1:=get_json('app_dinamica',json2);
        
        select * into campo from config_busqueda_generica_rds where id_pantalla=app1;
        if not found then
                json2:=put_json(json2,'CODIGO_RESPUESTA','2');
                json2:=put_json(json2,'MENSAJE_RESPUESTA','Falla Configuracion');
                return respondo_pantalla_15200(json2);
        end if;

        json2:=put_json(json2,'periodo_hasta_rds',campo.periodo_hasta_rds::varchar);
        --Ejecutamos funcion personalizada para sacar campos y el from
        execute 'select '||campo.funcion||'(' || chr(39) || json2 || chr(39) || '::json)' into json2;

        --Revisamos si la funcion respondio Error
        if get_json('CODIGO_RESPUESTA',json2)='2' then
                json2:=put_json(json2,'CODIGO_RESPUESTA','2');
                json2:=put_json(json2,'MENSAJE_RESPUESTA','Falla Configuracion.');
                return respondo_pantalla_15200(json2);
        end if;
        
        --Vemos a que secuencia nos iremos a ejecutar despues de ejecutar local
        if campo.parametro_rds='CD_BITACORA_HIST' then
                json2:=put_json(json2,'__SECUENCIAOK__','20');
	elsif campo.parametro_rds='BASE_DEC' then
                json2:=put_json(json2,'__SECUENCIAOK__','22');
        else
                json2:=put_json(json2,'CODIGO_RESPUESTA','2');
                json2:=put_json(json2,'MENSAJE_RESPUESTA','Falla Configuracion.');
                return respondo_pantalla_15200(json2);
        end if;

        campos1:=decode_hex(get_json('__CAMPOS_BUSQUEDA__',json2));
        if get_json('EJECUTO_LOCAL',json2)='SI' then
                from_where_local1:=decode_hex(get_json('__FW_LOCAL__',json2));
                execute 'SELECT array_to_json(array_agg(row_to_json(sql))) FROM (select '||campos1||' from '||from_where_local1||')sql ' into json_local;
                json2:=put_json(json2,'v_out_resultado',json_local::varchar);
        end if;

        if get_json('EJECUTO_RDS',json2)='SI' then
                from_where_rds1:=decode_hex(get_json('__FW_RDS__',json2));
                if get_json('QUERY_COUNT_RDS',json2)<>'' then
                        json2:=put_json(json2,'QUERY_DATA',decode_hex(get_json('QUERY_COUNT_RDS',json2))||';SELECT array_to_json(array_agg(row_to_json(sql))) FROM (select '||campos1||' from '||from_where_rds1||')sql ');
                        json2:=put_json(json2,'__TOTAL_RESP_ESPERADAS__','2');
                else
                        json2:=put_json(json2,'QUERY_DATA','SELECT array_to_json(array_agg(row_to_json(sql))) FROM (select '||campos1||' from '||from_where_rds1||')sql ');
                        json2:=put_json(json2,'__TOTAL_RESP_ESPERADAS__','1');
                end if;
                json2:=put_json(json2,'__TIMEOUT_SERV_PXML__','10');
        else
                json2:=put_json(json2,'__SECUENCIAOK__','30');
                json2:=put_json(json2,'TOTAL_RES_JSON','1');
                json2:=put_json(json2,'RES_JSON_1','[]');
                json2:=put_json(json2,'__TOTAL_RESP_ESPERADAS__','1');
        end if;

        return json2;
end
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION public.busqueda_bitacora_10k_15200(json)
 RETURNS json
AS $$
declare
        json1                alias for $1;
        json2                   json;
        rut_cliente1              varchar;
        rut_usuario1              varchar;
        offset1  integer;
        v_in_offset1  integer;
        cant_regs1 integer;
        total_regs1 varchar;
        cantidad_paginas1 varchar;
        fecha1    varchar;
        fecha2    varchar;
        filtro1  varchar;
        query1    varchar;
        json_out1   json;
        rol1            varchar;
        funcionalidad1 varchar;
        categoria1        varchar;
        app10k            varchar; --A.A 21/02/2017
        vin_offset      varchar;
        v_in_cant_reg   varchar;
        count_local1    bigint;
        jcount  json;
	aux1	varchar;
	mes1	record;
	fecha_in1	varchar;
BEGIN
        json2:=json1;
        rut_cliente1:=get_json('rutCliente',json2);
        rut_usuario1:=get_json('rutUsuario',json2);
        rol1:=replace(get_json('rol_usuario',json2),'_QA','');
        categoria1:=get_json('CATEGORIA_BITACORA',json2);
        fecha1:=replace(get_json('FECHA_INI',json2),'-','');
        app10k:=get_json('aplicacion',json2); --A.A 21/02/2017
        if (fecha1='') then
                fecha1:=to_char(now(),'YYYYMMDD');
        end if;
        fecha2:=replace(get_json('FECHA_FIN',json2),'-','');
        if (fecha2='') then
                fecha2:=to_char(now(),'YYYYMMDD');
        end if;
        --verificamos funcionalidad
        if (check_funcionalidad_6000(json2,'bitacora')) then
                rut_usuario1:=get_json('RUT_USUARIO_BITACORA',json2);
                if (rut_usuario1 in ('*','')) then
                        filtro1:=' empresa='||quote_literal(rut_cliente1);
                else
                        filtro1:=' empresa='||quote_literal(rut_cliente1)||' and rut_usuario='||quote_literal(rut_usuario1);
                end if;
        else
                filtro1:=' empresa='||quote_literal(rut_cliente1)||' and rut_usuario='||quote_literal(rut_usuario1);
        end if;
        if (categoria1 not in ('*','')) then
                filtro1:=filtro1||' and accion='||quote_literal(categoria1);
        end if;

        json2:=put_json(json2,'__CAMPOS_BUSQUEDA__',encode_hex(' to_char(bit.fecha,''YYYY-MM-DD HH24:MI:SS'') as INFO__FECHA__ON,bit.rut_usuario as INFO__RUT_USUARIO__ON,bit.empresa as INFO__RUT_EMPRESA__ON,bit.perfil as INFO__PERFIL__ON,bit.accion as INFO__CATEGORIA__ON,bit.descripcion as INFO__DESCRIPCION__ON,bit.aplicacion as INFO__APP__ON,bit.ip_cliente as INFO__IP_CLIENTE__ON '));
        
        vin_offset:=get_json('offset',json2);
        if(vin_offset is null or vin_offset='' or vin_offset='undefined')then
                vin_offset=1;
        end if;
        v_in_cant_reg:='100';
        v_in_offset1:=(vin_offset::integer-1)*v_in_cant_reg::integer;
        json2:=put_json(json2,'v_in_offset',vin_offset::varchar);

--select distinct to_char(generate_series,'YYMM') as fecha from generate_series(date_trunc('day','20180401'::date),date_trunc('day','20180512'::Date),'1 day') order by fecha
        --Solo local 
        if fecha1::integer>get_json('periodo_hasta_rds',json2)::integer then
                json2:=logjson(json2,'Solo busqueda Local offset ='||v_in_offset1::varchar||' limit '||v_in_cant_reg);
                json2:=put_json(json2,'EJECUTO_LOCAL','SI');
                json2:=put_json(json2,'EJECUTO_RDS','NO');
                json2:=put_json(json2,'COUNT_RDS','0');
		fecha_in1:=genera_in_fechas(fecha1::integer,fecha2::integer);
                --json2:=put_json(json2,'__FW_LOCAL__',encode_hex(' (select * from bitacora_10k where dia>='||fecha1||' and dia<='||fecha2||' and '||filtro1||' ) bit inner join (select * from detalle_parametros where aplicacion@@'''||app10k||''') det on bit.accion=det.codigo order by bit.fecha offset '||v_in_offset1::varchar||' limit '||v_in_cant_reg));
                json2:=put_json(json2,'__FW_LOCAL__',encode_hex(' (select * from bitacora_10k where dia in '||fecha_in1||' and '||filtro1||' ) bit inner join (select * from detalle_parametros where aplicacion@@'''||app10k||''') det on bit.accion=det.codigo order by bit.fecha offset '||v_in_offset1::varchar||' limit '||v_in_cant_reg));
		if get_json('rutUsuario',json2)='17597643' then
                	perform logfile('BITACORA (select * from bitacora_10k where dia in '||fecha_in1||' and '||filtro1||' ) bit inner join (select * from detalle_parametros where aplicacion@@'''||app10k||''') det on bit.accion=det.codigo order by bit.fecha offset '||v_in_offset1::varchar||' limit '||v_in_cant_reg);
		end if;
                --execute 'select count(*) from (select * from bitacora_10k where dia>'||get_json('periodo_hasta_rds',json2)||' and dia<='||fecha2||' and '||filtro1||') bit inner join (select * from detalle_parametros where aplicacion@@'''||app10k||''') det on bit.accion=det.codigo ' into count_local1; 
		fecha_in1:=genera_in_fechas(fecha1::integer,fecha2::integer);
                execute 'select count(*) from (select * from bitacora_10k where dia in '||fecha_in1||' and '||filtro1||') bit inner join (select * from detalle_parametros where aplicacion@@'''||app10k||''') det on bit.accion=det.codigo ' into count_local1; 
		if get_json('rutUsuario',json2)='17597643' then
                	perform logfile('BITACORA COUNT2');
		end if;
                json2:=logjson(json2,'Solo busqueda Local offset ='||v_in_offset1::varchar||' limit '||v_in_cant_reg||' count='||count_local1::varchar);
                json2:=put_json(json2,'COUNT_LOCAL',coalesce(count_local1,0)::varchar);
        --Solo RDS
        elsif fecha2::integer<=get_json('periodo_hasta_rds',json2)::integer then
                json2:=put_json(json2,'EJECUTO_LOCAL','NO');
                json2:=put_json(json2,'EJECUTO_RDS','SI');
		aux1:='';
		for mes1 in select distinct to_char(generate_series,'YYMM') as fecha from generate_series(date_trunc('day',fecha1::date),date_trunc('day',fecha2::Date),'1 day') order by fecha loop
			if aux1<>'' then
				aux1:=aux1||' union ';
			end if;	
			fecha_in1:=genera_in_fechas(fecha1::integer,fecha2::integer);	
			aux1:=aux1||' select * from bitacora_10k_'||mes1.fecha||' where dia in '||fecha_in1||' and '||filtro1||' ';
		end loop;
                json2:=put_json(json2,'__FW_RDS__',encode_hex(' ('||aux1||' ) bit inner join (select * from detalle_parametros where aplicacion@@'''||app10k||''') det on bit.accion=det.codigo order by bit.fecha offset '||v_in_offset1::varchar||' limit '||v_in_cant_reg));
                --json2:=put_json(json2,'__FW_RDS__',encode_hex(' (select * from bitacora_10k where dia>='||fecha1||' and dia<='||fecha2||' and '||filtro1||' ) bit inner join (select * from detalle_parametros where aplicacion@@'''||app10k||''') det on bit.accion=det.codigo order by bit.fecha offset '||v_in_offset1::varchar||' limit '||v_in_cant_reg));
                json2:=put_json(json2,'COUNT_LOCAL','0');
		fecha_in1:=genera_in_fechas(fecha1::integer,fecha2::integer);
                json2:=logjson(json2,'Query Count= select count(*) from (select * from bitacora_10k where dia in '||fecha_in1||' and '||filtro1||') bit inner join (select * from detalle_parametros where aplicacion@@'''||app10k||''') det on bit.accion=det.codigo ');
                json2:=logjson(json2,'Solo busqueda RDS offset ='||v_in_offset1::varchar||' limit '||v_in_cant_reg||' ');
                json2:=put_json(json2,'QUERY_COUNT_RDS',encode_hex('select count(*) from (select * from bitacora_10k where dia in '||fecha_in1||' and '||filtro1||') bit inner join (select * from detalle_parametros where aplicacion@@'''||app10k||''') det on bit.accion=det.codigo '));
        --Mix Local-RDS
        else
                json2:=put_json(json2,'EJECUTO_LOCAL','SI');
		fecha_in1:=genera_in_fechas(get_json('periodo_hasta_rds',json2)::integer,fecha2::integer);
                json2:=put_json(json2,'__FW_LOCAL__',encode_hex(' (select * from bitacora_10k where dia in '||fecha_in1||' and '||filtro1||' ) bit inner join (select * from detalle_parametros where aplicacion@@'''||app10k||''') det on bit.accion=det.codigo order by bit.fecha offset '||v_in_offset1::varchar||' limit '||v_in_cant_reg));
                json2:=put_json(json2,'EJECUTO_RDS','SI');
                --Contamos
                execute 'select count(*) from (select * from bitacora_10k where dia in '||fecha_in1||' and '||filtro1||') bit inner join (select * from detalle_parametros where aplicacion@@'''||app10k||''') det on bit.accion=det.codigo ' into count_local1;
                jcount:=get_offset_limit_2_bases(v_in_offset1,v_in_cant_reg::integer,count_local1::bigint,0);
                json2:=put_json(json2,'COUNT_LOCAL',coalesce(count_local1,0)::varchar);
		
		aux1:='';
		for mes1 in select distinct to_char(generate_series,'YYMM') as fecha from generate_series(date_trunc('day',fecha1::date),date_trunc('day',get_json('periodo_hasta_rds',json2)::date),'1 day') order by fecha loop
			if aux1<>'' then
				aux1:=aux1||' union ';
			end if;	
			fecha_in1:=genera_in_fechas(fecha1::integer,fecha2::integer);
			aux1:=aux1||' select * from bitacora_10k_'||mes1.fecha||' where dia in '||fecha_in1||' and '||filtro1||' ';
		end loop;
                
                json2:=put_json(json2,'__FW_RDS__',encode_hex(' ('||aux1||') bit inner join (select * from detalle_parametros where aplicacion@@'''||app10k||''') det on bit.accion=det.codigo order by bit.fecha offset '||get_json('OFFSET_BASE_2',jcount)||' limit '||get_json('LIMIT_BASE_2',jcount)));
                json2:=put_json(json2,'QUERY_COUNT_RDS',encode_hex('select count(*) from ('||aux1||') bit inner join (select * from detalle_parametros where aplicacion@@'''||app10k||''') det on bit.accion=det.codigo '));
                --json2:=put_json(json2,'__FW_RDS__',encode_hex(' (select * from bitacora_10k where dia>='||fecha1||' and dia<='||fecha2||' and '||filtro1||' ) bit inner join (select * from detalle_parametros where aplicacion@@'''||app10k||''') det on bit.accion=det.codigo order by bit.fecha offset '||get_json('OFFSET_BASE_2',jcount)||' limit '||get_json('LIMIT_BASE_2',jcount)));
                --json2:=put_json(json2,'QUERY_COUNT_RDS',encode_hex('select count(*) from (select * from bitacora_10k where dia>='||fecha1||' and dia<='||fecha2||' and '||filtro1||') bit inner join (select * from detalle_parametros where aplicacion@@'''||app10k||''') det on bit.accion=det.codigo '));
                
		fecha_in1:=genera_in_fechas(fecha1::integer,fecha2::integer);
                json2:=logjson(json2,'Query Count= select count(*) from (select * from bitacora_10k where dia in '||fecha_in1||' and '||filtro1||') bit inner join (select * from detalle_parametros where aplicacion@@'''||app10k||''') det on bit.accion=det.codigo ');
                json2:=logjson(json2,'busqueda Local-RDS offset ='||v_in_offset1::varchar||' limit '||v_in_cant_reg||' count_local='||count_local1::varchar);
        end if;
        RETURN json2;

end
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.busqueda_casilla_15200(json)
 RETURNS json
AS $$
declare
        json1                alias for $1;
        json2                   json;
        json3   json;
        json4   json;
        boton_edita1    json;
        json_drop1      json;
        json_but1       json;
        id_pantalla1    varchar;
        rutUsuario1     varchar;
        rut1    bigint;
        categoria1      varchar;
        titulo1 varchar;
        tipo1   varchar;
        uri_visualizador1       varchar;
        uri_google1     varchar;
        wpantalla_int1  integer;
        ids1    varchar;
        uri_visualizador_html   varchar;
        uri_visualizador_pdf    varchar;
        campo   record;
        empresa1        varchar;
	accion_compartir        json;
        accion_reenviar         json;
        accion_ver_doc          json;
BEGIN
        json2:=json1;
        rutUsuario1:=get_json('rutUsuario',json2);
        id_pantalla1:=get_json('app_dinamica',json2);
        uri_visualizador1:= coalesce((select remplaza_tags_6000(href,json2) from menu_info_10k_cd where id2='visualizador'),'');
        uri_visualizador_html:= coalesce((select remplaza_tags_6000(href,json2) from menu_info_10k_cd where id2='visualizador_html'),'');
        uri_visualizador_pdf:= uri_visualizador1;
        uri_google1:='http://drive.google.com/viewerng/viewer?embedded=true&url=';
        --select array_to_json(array_agg(row_to_json(sql))) from (select * from acciones_grillas where id_pantalla='grilla_cd' order by orden) sql into json_drop1;
	-- NBV 20180228
        if get_json('flag_compartir',json2)='SI' then
                accion_compartir:=coalesce((select row_to_json(sql) from (select * from acciones_grillas where id_pantalla='grilla_cd' and valor='Compartir a otra casilla digital') sql),'{}');
        else
                accion_compartir:='{}';
        end if;
        if get_json('flag_reenviar',json2)='SI' then
                accion_reenviar:=coalesce((select row_to_json(sql) from (select * from acciones_grillas where id_pantalla='grilla_cd' and valor='Reenviar a otro correo') sql),'{}');
        else
                accion_reenviar:='{}';
        end if;
        accion_ver_doc:=coalesce((select row_to_json(sql) from (select *,'_blank' as target from acciones_grillas where id_pantalla='grilla_cd' and valor='Ver Documento') sql),'{}');

        --Generico
        select * into campo from cd_categorias where id=get_json('awsfec',json2)::integer;
        if not found then
                return response_requests_6000('1', 'Sin Registros', json3::varchar,json2);
        end if;
        categoria1:=campo.categoria;
        rut1:=get_json('rutUsuario',json2)::bigint;

        json2:=put_json(json2,'EJECUTO_LOCAL','SI');
        json2:=put_json(json2,'EJECUTO_RDS','SI');
        if(categoria1='BOLETAS_DIRECTV') then
                json2:=put_json(json2,'__CAMPOS_BUSQUEDA__',encode_hex(' desde as info__from__off,to_char(fecha,''YYYY/MM/DD HH24:MI'') as info__fecha__on,case when fecha_vencimiento is null or fecha_vencimiento='''' then ''Sin Fecha'' else to_char(cd_format_fecha_vcto(fecha_vencimiento),''YYYY/MM/DD HH24:MI'') end as info__fecha_vcto__on, nro_cliente as info__nro_de_suscriptor__on,case when strpos(monto,''$'')>0 then replace(monto,''$'',''$ '') else formatea_monto(monto) end as info__monto_a_pagar__on, categoria as info__categoria__off,id as info__id__off,(''[''||'''||accion_reenviar::varchar||','||accion_compartir::varchar||',''||replace('''||accion_ver_doc::varchar||''',''###HREF###'',replace(coalesce(url,url_html,url_text),''\\/'',''/''))||'']'')::json as DROPDOWN__ACCIONES__ON '));
                json2:=put_json(json2,'__FW_LOCAL__',encode_hex(' (select * from  cd_lista_mail where rut='''||rut1::varchar||''') x where dia>='||get_json('periodo_hasta_rds',json2)||' and categoria='''||categoria1||''' and size>300 order by fecha desc'));
                --json2:=put_json(json2,'__FW_LOCAL__',encode_hex(' cd_lista_mail where dia>='||get_json('periodo_hasta_rds',json2)||' and rut='''||rut1::varchar||''' and categoria='''||categoria1||''' and size>300 order by fecha desc'));
                json2:=put_json(json2,'__FW_RDS__',encode_hex(' cd_lista_mail where rut='''||rut1::varchar||''' and categoria='''||categoria1||''' and size>300 order by fecha desc'));
        else
                --Para DEMO en empresa ACEPTA
                if get_json('rutCliente',json2)='1096919050' then 
                        json2:=put_json(json2,'__FW_LOCAL__',encode_hex(' cd_lista_mail where dia>='||get_json('periodo_hasta_rds',json2)||' and rut='''||rut1::varchar||''' and categoria='''||categoria1||''' and size>300 order by fecha desc'));
                        json2:=put_json(json2,'__FW_RDS__',encode_hex(' cd_lista_mail where rut='''||rut1::varchar||''' and categoria='''||categoria1||''' and size>300 order by fecha desc'));
                        json2:=put_json(json2,'__CAMPOS_BUSQUEDA__',encode_hex(' desde as info__from__off,to_char(fecha,''YYYY/MM/DD HH24:MI'') as info__fecha__on,periodo as info__periodo__off, fecha_vencimiento as info__fecha_pago__off,categoria as info__categoria__off,''Acepta'' as info__de__on, coalesce(caption_usuario,replace(replace(replace(filename,chr(10),''''),chr(13),''''),chr(9),''''),subject) as info__doc__off,'''' as info__nuevo_nombre__off,titulo as info__empresa__off,id as info__id__off, (''[''||'''||accion_reenviar::varchar||','||accion_compartir::varchar||',''||replace('''||accion_ver_doc::varchar||''',''###HREF###'',replace(coalesce(url,url_html,url_text),''\\/'',''/''))||'']'')::json as DROPDOWN__ACCIONES__ON '));
                -- para la carpeta legal el usuario esta en el campo nro_cliente y se busca en otra tabla y la empresa viene en cpt
                elsif get_json('host_canal',json2)='legal.casilladigital.cl' then       
                        empresa1:=get_json('cpt',json2);
                        json2:=put_json(json2,'__FW_LOCAL__',encode_hex(' cd_lista_carpeta_legal where dia>='||get_json('periodo_hasta_rds',json2)||' and rut='''||empresa1::varchar||''' and nro_cliente='''||rut1::varchar||''' and categoria='''||categoria1||''' and size>300 order by fecha desc'));
                        json2:=put_json(json2,'__FW_RDS__',encode_hex(' cd_lista_carpeta_legal where rut='''||empresa1::varchar||''' and nro_cliente='''||rut1::varchar||''' and categoria='''||categoria1||''' and size>300 order by fecha desc'));
                        json2:=put_json(json2,'__CAMPOS_BUSQUEDA__',encode_hex(' desde as info__from__off,to_char(fecha,''YYYY/MM/DD HH24:MI'') as info__fecha__on,periodo as info__periodo__off, fecha_vencimiento as info__fecha_pago__off,categoria as info__categoria__off,desde as info__de__on, coalesce(decode_utf8_amz(subject),caption_usuario,''-'')||''__''||coalesce(url,url_html,url_text) as urlbutton__documento__on,coalesce(caption_usuario,replace(replace(replace(filename,chr(10),''''),chr(13),''''),chr(9),''''),subject) as info__doc__off,'''' as info__nuevo_nombre__off,titulo as info__empresa__off,id as info__id__off '));
                else
			if get_json('rutCliente',json2)='89802200' then
				json2:=put_json(json2,'__FW_LOCAL__',encode_hex(' cd_lista_mail where dia>='||get_json('periodo_hasta_rds',json2)||' and rut='''||rut1::varchar||''' and categoria='''||categoria1||''' and size>300 order by fecha desc'));
				json2:=put_json(json2,'__FW_RDS__',encode_hex(' cd_lista_mail where rut='''||rut1::varchar||''' and categoria='''||categoria1||''' and size>300 order by fecha desc'));
			else
				json2:=put_json(json2,'__FW_LOCAL__',encode_hex(' cd_lista_mail where dia>='||get_json('periodo_hasta_rds',json2)||' and rut='''||rut1::varchar||''' and categoria='''||categoria1||''' and size>300 order by fecha desc'));
				json2:=put_json(json2,'__FW_RDS__',encode_hex(' cd_lista_mail where rut='''||rut1::varchar||''' and categoria='''||categoria1||''' and size>300 order by fecha desc'));
			end if;
                        json2:=put_json(json2,'__CAMPOS_BUSQUEDA__',encode_hex(' desde as info__from__off,to_char(fecha,''YYYY/MM/DD HH24:MI'') as info__fecha__on,periodo as info__periodo__off, fecha_vencimiento as info__fecha_pago__off,categoria as info__categoria__off,desde as info__de__on, coalesce(caption_usuario,replace(replace(replace(filename,chr(10),''''),chr(13),''''),chr(9),''''),subject) as info__doc__off,'''' as info__nuevo_nombre__off,titulo as info__empresa__off,id as info__id__off , (''[''||'''||accion_reenviar::varchar||','||accion_compartir::varchar||',''||replace('''||accion_ver_doc::varchar||''',''###HREF###'',replace(coalesce(url,url_html,url_text),''\\/'',''/''))||'']'')::json as DROPDOWN__ACCIONES__ON '));
                end if;
        end if;
        return json2;
end
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION public.busqueda_compartidos_cd_15200(json)
 RETURNS json
AS $$
declare
        json1                alias for $1;
        json2                   json;
        json3   json;
        json4   json;
        boton_edita1    json;
        json_drop1      json;
        json_but1       json;
        id_pantalla1    varchar;
        rutUsuario1     varchar;
        rut1    bigint;
        categoria1      varchar;
        titulo1 varchar;
        tipo1   varchar;
        uri_visualizador1       varchar;
        uri_google1     varchar;
        wpantalla_int1  integer;
        ids1    varchar;
        uri_visualizador_html   varchar;
        uri_visualizador_pdf    varchar;
        campo   record;
        empresa1        varchar;
        accion_compartir        json;
        accion_reenviar         json;
        accion_ver_doc          json;
BEGIN
        json2:=json1;
        rutUsuario1:=get_json('rutUsuario',json2);
        id_pantalla1:=get_json('app_dinamica',json2);
        rut1:=get_json('rutUsuario',json2)::bigint;
	uri_visualizador1:= coalesce((select remplaza_tags_6000(href,json2) from menu_info_10k_cd where id2='visualizador'),'');
        uri_visualizador_html:= coalesce((select remplaza_tags_6000(href,json2) from menu_info_10k_cd where id2='visualizador_html'),'');
        uri_visualizador_pdf:= uri_visualizador1;
        uri_google1:='http://drive.google.com/viewerng/viewer?embedded=true&url=';

        json2:=put_json(json2,'EJECUTO_LOCAL','SI');
        json2:=put_json(json2,'EJECUTO_RDS','SI');
        
	if id_pantalla1='grilla_compartidos_cd_enviados' then
		json2:=put_json(json2,'__FW_LOCAL__',encode_hex(' cd_lista_mail join cd_compartidos b on cd_lista_mail.id=b.id_mail and cd_lista_mail.rut=b.casilla_from  where b.casilla_from='''||rut1::varchar||''' order by fecha desc '));
		json2:=put_json(json2,'__FW_RDS__',encode_hex(' cd_lista_mail join cd_compartidos b on cd_lista_mail.id=b.id_mail and cd_lista_mail.rut=b.casilla_from  where b.casilla_from='''||rut1::varchar||''' order by fecha desc '));
		json2:=put_json(json2,'__CAMPOS_BUSQUEDA__',encode_hex(' to_char(fecha,''YYYY/MM/DD HH24:MI'') as info__fecha__off,periodo as info__periodo__off, fecha_vencimiento as info__fecha_pago__off,rut_empresa||''-''||modulo11(rut_empresa::varchar) as info__empresa__off,cd_lista_mail.categoria as info__categoria__off,desde as info__de__off, case when categoria_envio=''COMPARTIR'' then b.casilla_to||''-''||modulo11(b.casilla_to::varchar) else lista_mail end as info__compartido_para__on,to_char(b.dia_desde,''YYYY/MM/DD HH24:MI'') as info__compartido_desde__on,to_char(b.dia_hasta,''YYYY/MM/DD HH24:MI'') as info__compartido_hasta__on,b.categoria as info__categoria_documento__off, categoria_envio as info__categoria__on, coalesce(decode_utf8_amz(subject),caption_usuario,''-'')||''__''||coalesce(url,url_html,url_text) as urlbutton__documento__on, coalesce(caption_usuario,replace(replace(replace(filename,chr(10),''''),chr(13),''''),chr(9),''''),subject) as info__doc__off,'''' as info__nuevo_nombre__off,titulo as info__empresa__off,cd_lista_mail.id as info__id__off '));
	else
		json2:=put_json(json2,'__FW_LOCAL__',encode_hex(' cd_lista_mail join cd_compartidos b on cd_lista_mail.id=b.id_mail and cd_lista_mail.rut=b.casilla_from and now()>=b.dia_desde and now()<=b.dia_hasta where b.casilla_to='''||rut1::varchar||''' order by fecha desc'));
		json2:=put_json(json2,'__FW_RDS__',encode_hex(' cd_lista_mail join cd_compartidos b on cd_lista_mail.id=b.id_mail and cd_lista_mail.rut=b.casilla_from and now()>=b.dia_desde and now()<=b.dia_hasta where b.casilla_to='''||rut1::varchar||''' order by fecha desc'));
		json2:=put_json(json2,'__CAMPOS_BUSQUEDA__',encode_hex(' to_char(fecha,''YYYY/MM/DD HH24:MI'') as info__fecha__off,periodo as info__periodo__off, fecha_vencimiento as info__fecha_pago__off,rut_empresa||''-''||modulo11(rut_empresa::varchar) as info__empresa__off,cd_lista_mail.categoria as info__categoria__off,desde as info__de__off,b.casilla_from||''-''||modulo11(b.casilla_from::varchar) as info__compartido_por__on,to_char(b.dia_desde,''YYYY/MM/DD HH24:MI'') as info__compartido_desde__on,to_char(b.dia_hasta,''YYYY/MM/DD HH24:MI'') as info__compartido_hasta__on,b.categoria as info__categoria_documento__off, coalesce(decode_utf8_amz(subject),caption_usuario,''-'')||''__''||coalesce(url,url_html,url_text) as urlbutton__documento__on, coalesce(caption_usuario,replace(replace(replace(filename,chr(10),''''),chr(13),''''),chr(9),''''),subject) as info__doc__off,'''' as info__nuevo_nombre__off,titulo as info__empresa__off,cd_lista_mail.id as info__id__off '));
	end if;
        return json2;
end
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION public.busqueda_bitacora_dec_15200(json)
 RETURNS json
AS $$
declare
        json1                alias for $1;
        json2                   json;
        rut_cliente1              varchar;
        rut_usuario1              varchar;
        offset1  integer;
        v_in_offset1  integer;
        cant_regs1 integer;
        total_regs1 varchar;
        cantidad_paginas1 varchar;
        fecha1    varchar;
        fecha2    varchar;
        filtro1  varchar;
        query1    varchar;
        json_out1   json;
        rol1            varchar;
        funcionalidad1 varchar;
        categoria1        varchar;
        app10k            varchar; --A.A 21/02/2017
        vin_offset      varchar;
        v_in_cant_reg   varchar;
        count_local1    bigint;
        jcount  json;
	aux1	varchar;
	mes1	record;
	fecha_in1	varchar;
BEGIN
        json2:=json1;
        rut_cliente1:=get_json('rutCliente',json2);
        rut_usuario1:=get_json('rutUsuario',json2);
        rol1:=replace(get_json('rol_usuario',json2),'_QA','');
        categoria1:=get_json('CATEGORIA_BITACORA',json2);
        fecha1:=replace(get_json('FECHA_INI',json2),'-','');
        app10k:=get_json('aplicacion',json2); --A.A 21/02/2017
        if (fecha1='') then
                fecha1:=to_char(now(),'YYYYMMDD');
        end if;
        fecha2:=replace(get_json('FECHA_FIN',json2),'-','');
        if (fecha2='') then
                fecha2:=to_char(now(),'YYYYMMDD');
        end if;
        --verificamos funcionalidad
	rut_usuario1:=replace(get_json('RUT_USUARIO_BITACORA',json2),'.','');
	filtro1:=' empresa in ('||quote_literal(get_json('razon_social',json2))||',(select inst_base from grupo_rut where institucion='||quote_literal(get_json('razon_social',json2))||')) ';
	if (rut_usuario1 not in ('*','')) then
		filtro1:=filtro1||' and rut_usuario in ('||quote_literal(lpad(rut_usuario1,12,'0'))||','||quote_literal(rut_usuario1)||')';
	end if;
        if (categoria1 not in ('*','')) then
                filtro1:=filtro1||' and accion='||quote_literal(categoria1);
        end if;

        json2:=put_json(json2,'__CAMPOS_BUSQUEDA__',encode_hex(' to_char(bit.fecha,''YYYY-MM-DD HH24:MI:SS'') as INFO__FECHA__ON,ltrim(bit.rut_usuario,''0'') as INFO__RUT_USUARIO__ON,bit.empresa as INFO__RUT_EMPRESA__ON,bit.perfil as INFO__PERFIL__ON,bit.accion as INFO__CATEGORIA__ON,bit.descripcion as INFO__DESCRIPCION__ON '));
        
        vin_offset:=get_json('offset',json2);
        if(vin_offset is null or vin_offset='' or vin_offset='undefined')then
                vin_offset=1;
        end if;
        v_in_cant_reg:='100';
        v_in_offset1:=(vin_offset::integer-1)*v_in_cant_reg::integer;
        json2:=put_json(json2,'v_in_offset',vin_offset::varchar);

--select distinct to_char(generate_series,'YYMM') as fecha from generate_series(date_trunc('day','20180401'::date),date_trunc('day','20180512'::Date),'1 day') order by fecha
        --Solo local 
        if fecha1::integer>get_json('periodo_hasta_rds',json2)::integer then
                json2:=logjson(json2,'Solo busqueda Local offset ='||v_in_offset1::varchar||' limit '||v_in_cant_reg);
                json2:=put_json(json2,'EJECUTO_LOCAL','SI');
                json2:=put_json(json2,'EJECUTO_RDS','NO');
                json2:=put_json(json2,'COUNT_RDS','0');
		fecha_in1:=genera_in_fechas(fecha1::integer,fecha2::integer);
                json2:=put_json(json2,'__FW_LOCAL__',encode_hex(' (select * from bitacora_dec where dia in '||fecha_in1||' and '||filtro1||' ) bit order by bit.fecha offset '||v_in_offset1::varchar||' limit '||v_in_cant_reg));
		fecha_in1:=genera_in_fechas(fecha1::integer,fecha2::integer);
                execute 'select count(*) from (select * from bitacora_dec where dia in '||fecha_in1||' and '||filtro1||') bit ' into count_local1; 
		if get_json('rutUsuario',json2)='17597643' then
                	perform logfile('BITACORA COUNT2');
		end if;
                json2:=logjson(json2,'Solo busqueda Local offset ='||v_in_offset1::varchar||' limit '||v_in_cant_reg||' count='||count_local1::varchar);
                json2:=put_json(json2,'COUNT_LOCAL',coalesce(count_local1,0)::varchar);
        --Solo RDS
        elsif fecha2::integer<=get_json('periodo_hasta_rds',json2)::integer then
                json2:=put_json(json2,'EJECUTO_LOCAL','NO');
                json2:=put_json(json2,'EJECUTO_RDS','SI');
		aux1:='';
		for mes1 in select distinct to_char(generate_series,'YYMM') as fecha from generate_series(date_trunc('day',fecha1::date),date_trunc('day',fecha2::Date),'1 day') order by fecha loop
			if aux1<>'' then
				aux1:=aux1||' union ';
			end if;	
			fecha_in1:=genera_in_fechas(fecha1::integer,fecha2::integer);	
			aux1:=aux1||' select * from bitacora_dec_'||mes1.fecha||' where dia in '||fecha_in1||' and '||filtro1||' ';
		end loop;
                json2:=put_json(json2,'__FW_RDS__',encode_hex(' ('||aux1||' ) bit order by bit.fecha offset '||v_in_offset1::varchar||' limit '||v_in_cant_reg));
                json2:=put_json(json2,'COUNT_LOCAL','0');
		fecha_in1:=genera_in_fechas(fecha1::integer,fecha2::integer);
                json2:=logjson(json2,'Query Count= select count(*) from (select * from bitacora_dec where dia in '||fecha_in1||' and '||filtro1||') bit ');
                json2:=logjson(json2,'Solo busqueda RDS offset ='||v_in_offset1::varchar||' limit '||v_in_cant_reg||' ');
                json2:=put_json(json2,'QUERY_COUNT_RDS',encode_hex('select count(*) from (select * from bitacora_dec where dia in '||fecha_in1||' and '||filtro1||') bit '));
        --Mix Local-RDS
        else
                json2:=put_json(json2,'EJECUTO_LOCAL','SI');
		fecha_in1:=genera_in_fechas(get_json('periodo_hasta_rds',json2)::integer,fecha2::integer);
                json2:=put_json(json2,'__FW_LOCAL__',encode_hex(' (select * from bitacora_dec where dia in '||fecha_in1||' and '||filtro1||' ) bit order by bit.fecha offset '||v_in_offset1::varchar||' limit '||v_in_cant_reg));
                json2:=put_json(json2,'EJECUTO_RDS','SI');
                --Contamos
                execute 'select count(*) from (select * from bitacora_dec where dia in '||fecha_in1||' and '||filtro1||') bit ' into count_local1;
                jcount:=get_offset_limit_2_bases(v_in_offset1,v_in_cant_reg::integer,count_local1::bigint,0);
                json2:=put_json(json2,'COUNT_LOCAL',coalesce(count_local1,0)::varchar);
		
		aux1:='';
		for mes1 in select distinct to_char(generate_series,'YYMM') as fecha from generate_series(date_trunc('day',fecha1::date),date_trunc('day',get_json('periodo_hasta_rds',json2)::date),'1 day') order by fecha loop
			if aux1<>'' then
				aux1:=aux1||' union ';
			end if;	
			fecha_in1:=genera_in_fechas(fecha1::integer,fecha2::integer);
			aux1:=aux1||' select * from bitacora_dec_'||mes1.fecha||' where dia in '||fecha_in1||' and '||filtro1||' ';
		end loop;
                
                json2:=put_json(json2,'__FW_RDS__',encode_hex(' ('||aux1||') bit order by bit.fecha offset '||get_json('OFFSET_BASE_2',jcount)||' limit '||get_json('LIMIT_BASE_2',jcount)));
                json2:=put_json(json2,'QUERY_COUNT_RDS',encode_hex('select count(*) from ('||aux1||') bit '));
                
		fecha_in1:=genera_in_fechas(fecha1::integer,fecha2::integer);
                json2:=logjson(json2,'busqueda Local-RDS offset ='||v_in_offset1::varchar||' limit '||v_in_cant_reg||' count_local='||count_local1::varchar);
        end if;
        RETURN json2;

end
$$ LANGUAGE plpgsql;

