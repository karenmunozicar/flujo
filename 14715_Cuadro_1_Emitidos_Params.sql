delete from isys_querys_tx where llave='14715';
delete from isys_querys_tx where llave='14716';
delete from isys_querys_tx where llave='14717';

CREATE or replace FUNCTION cuadro1emitidos_params(json) RETURNS json AS $$
DECLARE
        json2   json;
        json1   alias for $1;
    	v_fecha_inicio      integer;
    	v_fecha_fin         integer;
	fecha_in1	varchar;
	json3       json;
    	json4       json;
    	json5       json;
	texto_ref1	varchar;
BEGIN
        json2:=json1;
	json2:=put_json(json2,'__CUADRO__','1');

	--Ingresamos las variables como las esperan las funciones
	json2:=put_json(json2,'tipoFecha',get_json('TIPO_FECHA',json2));
        json2:=put_json(json2,'fstart',get_json('FSTART',json2));
        json2:=put_json(json2,'fend',get_json('FEND',json2));
        json2:=put_json(json2,'tipo_dte_filtro',get_json('TIPO_DTE',json2));
        json2:=put_json(json2,'rut_emisor_filtro',get_json('RUT_EMISOR',json2));
	----------------------------------------------------------------------
        json2:=corrige_fechas(json2);

        v_fecha_inicio:=get_json('fstart',json2)::integer;
        v_fecha_fin:=get_json('fend',json2)::integer;
	fecha_in1:=genera_in_fechas(v_fecha_inicio::integer,v_fecha_fin::integer);
        if (fecha_in1 is null) then
                json2:=response_requests_6000('2', 'Rango de Fechas Invalida','',json2);
                return json2;
        end if;
        	
	json2:=put_json(json2,'LLAMA_FLUJO','SI');
	json2:=put_json(json2,'__SECUENCIAOK__','14715');
	return json2;
END;
$$ LANGUAGE plpgsql;

insert into isys_querys_tx values ('14715',10,1,9,'<EJECUTA0>14716</EJECUTA0><EJECUTA1>14717</EJECUTA1><TIMEOUT>15</TIMEOUT>',0,0,0,1,1,20,20);

insert into isys_querys_tx values ('14715',20,9,1,'select respuesta_cuadro_emitidosv2_14715(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('14716',10,9,1,'select select_cuadro_emitidosv2_redshift_14716(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('14717',10,9,1,'select select_cuadro_emitidosv2_local_14717(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

CREATE or replace FUNCTION respuesta_cuadro_emitidosv2_14715(json) RETURNS json AS $$
DECLARE
        json2   json;
        json1   alias for $1;
	campo	record;
	lista_emi_rs	json;
	lista_emi_local	json;
	lista_suma	json;
        json_resp       json;
        json5           json;
        json_aux1       json;
        json_pend       json;
	json_patron	json;
        tipo_dte1       varchar;
        texto1          varchar;
        select_1        varchar;
        json3   json;
        json4   json;
        v_fecha_inicio  integer;
        v_fecha_fin     integer;
        i       integer;
        j       integer;
        aux     varchar;
        aux1     varchar;
	jaux	json;
	jaux2	json;
        estado1 varchar;
	sufijo1	varchar;
	param1	varchar;
	alias1	varchar;
       	jg	json; 
       	jgg	json; 
	lista_final1	json;
BEGIN
        json2:=json1;
        json2 := put_json(json2,'__SECUENCIAOK__','0');

        json2:=corrige_fechas(json2);
        v_fecha_inicio:=get_json('fstart',json2)::integer;
        v_fecha_fin:=get_json('fend',json2)::integer;

        begin
                lista_emi_rs:=get_json('EMITIDOS_RS',json2);
        exception when others then
		lista_emi_rs:='[]';
        end;
        begin
                lista_emi_local:=get_json('EMITIDOS_LOCAL',json2);
        exception when others then
                lista_emi_local:='[]';
        end;
	
	perform logfile('DAO_14715 JSON RS  '||lista_emi_rs::varchar);
	perform logfile('DAO_14715 JSON LOCAL  '||lista_emi_local::varchar);
	perform logfile('DAO_14715 JSON sum_groups '||get_json('sum_groups',json2));

	param1:=get_json('PARAM',json2);	
	alias1:=get_json('alias_param',json2);
	lista_suma:=suma_json2(lista_emi_rs,lista_emi_local,get_json('sum_groups',json2),'["'||lower(alias1)||'"]');
	perform logfile('DAO_14715 JSON SUMA '||lista_suma::varchar);
	/*if count_array_json(lista_suma)=0 then
        	return response_requests_6000('2', 'Sin Registros', '[]', json2);
	end if;*/
       	/* 
	lista_final1:='[]';
	for campo in execute 'select row_to_json(sql) as data from (select distinct '||param1||' from indexer_hash where rut_emisor='||get_json('rutCliente',json2)||' order by 1)sql' loop
		i:=0;
		aux:=get_json_index(lista_suma,i);
		while(aux<>'') loop
			perform logfile('DAO_14715 '||get_json(alias1,aux::json)||' '||get_json(param1,campo.data));
			if get_json(alias1,aux::json)=get_json(param1,campo.data) then
				perform logfile('DAO_14715 igual');
			end if;
			i:=i+1;
			aux:=get_json_index(lista_suma,i);
		end loop;	
	end loop;
	*/
	jg:=get_json('sum_groups',json2)::json;
	jgg:=get_json('glosa_groups',json2)::json;
	lista_final1:='[]';
	i:=0;
	aux:=get_json_index(lista_suma,i);
	while(aux<>'') loop
		jaux:=aux::json;
		jaux2:='{}';
		j:=0;
		aux1:=get_json_index(jg,j);
		jaux2:=put_json(jaux2,alias1,get_json(lower(alias1),jaux));
		while(aux1<>'') loop
			--jaux2:=put_json(jaux2,aux1,get_json(aux1,jaux)||'____'||get_json(lower(alias1),jaux));
			jaux2:=put_json(jaux2,get_json(upper(aux1),jgg)||'__info__'||aux1||'__link',get_json(lower(aux1),jaux)||'____'||get_json(lower(alias1),jaux));
			--jaux:=put_json(jaux,get_json(aux1,jgg)||'__info__'||aux1||'__link',get_json(aux1,jaux)||'____'||get_json(lower(alias1),jaux));
			j:=j+1;
			aux1:=get_json_index(jg,j);
		end loop;
		--jaux2:=put_json(jaux2,'total',get_json('total',jaux));
		--lista_final1:=put_json_list(lista_final1,jaux::varchar);
		lista_final1:=put_json_list(lista_final1,jaux2::varchar);
		i:=i+1;
		aux:=get_json_index(lista_suma,i);
	end loop;
	perform logfile('DAO_14715 '||lista_final1::varchar);
	json4:='[]';
        json5:='{}';
        json5:=put_json(json5,'criterio_busqueda',get_json('criterio_busqueda',json2));
        json5:=put_json(json5,'id','Cuadro2');
        json5:=put_json(json5,'tipo','2');
        json5:=put_json(json5,'data',lista_final1::varchar);
        json5:=put_json(json5,'uri',coalesce((select replace(remplaza_tags_6000(href,json2),'NO_BUSCAR','') from menu_info_10k where id2='buscarNEW_emitidos'),''));
        json5:=put_json(json5,'uri_ant','');
        json5:=put_json(json5,'criterio_busqueda',get_json('criterio_busqueda',json2));
        json4:=put_json_list(json4,json5);
        return response_requests_6000('1', '', json4::varchar, json2);

END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION select_cuadro_emitidosv2_local_14717(json) RETURNS json AS $$
DECLARE
    json2                json;
    json1                alias for $1;
BEGIN
	json2:=json1;
	json2:=put_json(json2,'_ORIGEN_','LOCAL');
	return select_cuadro_emitidosv2_14715(json2);

END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION select_cuadro_emitidosv2_redshift_14716(json) RETURNS json AS $$
DECLARE
    json2                json;
    json1                alias for $1;
BEGIN
	json2:=json1;
	json2:=put_json(json2,'_ORIGEN_','RS');
	return select_cuadro_emitidosv2_14715(json2);
END;
$$ LANGUAGE plpgsql;

--Recibidos de RES
CREATE or replace FUNCTION select_cuadro_emitidosv2_14715(json) RETURNS json AS $$
DECLARE
    json2                json;
    json1                alias for $1;
	j1	json;
	json4	json;
	v_fecha_inicio	integer;
	v_fecha_fin	integer;
	v_rut_emisor	integer;
	v_rut_usuario	integer;
	now1	varchar;
	now_int	integer;
	tipo_dia1	varchar;
	json_rut1	json;
	query1	varchar;
	fecha_in1	varchar;
	v_parametro_rut_emisor	varchar;
	tipoFecha1	varchar;
	v_parametro_tipo_dte	varchar;
	filtro_cuadro2	varchar;
	json_par1	json;
	v_parametro_rut_emisor_com	varchar;
	aux varchar;
	monto_cantidad1	varchar;
	monto_cantidad2	varchar;
	json3	json;
	lista1	varchar;
	filtro_fecha1	varchar;
	param1	varchar;
	alias1	varchar;
BEGIN
        json2:=json1;
	json4:='{}';
	--json4:=put_json(json4,'sum_groups','["total","ACEPTADO_POR_EL_SII","ACEPTADO_CON_REPAROS_POR_EL_SII","RECHAZADO_POR_EL_SII","ACEPTADOS_POR_EL_RECEPTOR","RECLAMADOS_POR_EL_RECEPTOR","ACEPTADOS_9_DIAS_RECEPTOR"]');
	json4:=put_json(json4,'sum_groups','["ACEPTADO_POR_EL_SII","ACEPTADO_CON_REPAROS_POR_EL_SII","RECHAZADO_POR_EL_SII","ACEPTADOS_POR_EL_RECEPTOR","RECLAMADOS_POR_EL_RECEPTOR","ACEPTADOS_9_DIAS_RECEPTOR"]');
	json4:=put_json(json4,'glosa_groups','{"ACEPTADO_POR_EL_SII":"ACEPTADOS_SII","ACEPTADO_CON_REPAROS_POR_EL_SII":"ACEPTADOS_CON_REPAROS_SII","RECHAZADO_POR_EL_SII":"RECHAZADOS_SII","ACEPTADOS_POR_EL_RECEPTOR":"ACEPTADOS_RECEPTOR","RECLAMADOS_POR_EL_RECEPTOR":"RECLAMADOS_RECEPTOR","ACEPTADOS_9_DIAS_RECEPTOR":"ACEPTADOS_9_DIAS_RECEPTOR"}');

	json2:=logjson(json2,'Ejecuta select_cuadro_1_emitidos_redshift_14711');
        v_rut_emisor:=get_json('rutCliente',json2)::integer;
        v_rut_usuario:=get_json('rutUsuario',json2)::integer;

        json2:=corrige_fechas(json2);
        v_fecha_inicio:=get_json('fstart',json2)::integer;
        v_fecha_fin:=get_json('fend',json2)::integer;

        fecha_in1:=genera_in_fechas(v_fecha_inicio::integer,v_fecha_fin::integer);
        if (fecha_in1 is null) then
                json2:=response_requests_6000('2', 'Rango de Fechas Invalida','',json2);
                return json2;
        end if;

	now_int:=to_char(now(),'YYYYMMDD')::integer;
	if get_json('_ORIGEN_',json2)='RS' and v_fecha_inicio=now_int then
		perform logfile('14715 solo local');	
		json4:=put_json(json4,'_LOG_',get_json('_LOG_',json2));
		json4:=put_json(json4,'__FLUJO_ACTUAL__',get_json('__FLUJO_ACTUAL__',json2));
		json4:=put_json(json4,'EMITIDOS_RS','[]');
		return json4;
	end if;
	if get_json('_ORIGEN_',json2)='LOCAL' and v_fecha_fin<now_int then
		perform logfile('14715 solo rs');	
		json4:=put_json(json4,'_LOG_',get_json('_LOG_',json2));
		json4:=put_json(json4,'__FLUJO_ACTUAL__',get_json('__FLUJO_ACTUAL__',json2));
		json4:=put_json(json4,'EMITIDOS_LOCAL','[]');
		return json4;
	end if;
		
        tipo_dia1:=get_json('TIPO_FECHA',json2);
        if (tipo_dia1='Emision') then
                tipo_dia1:='E';
                tipoFecha1:='_emision ';
	elsif(tipo_dia1='recepcion_sii') then
                tipo_dia1:='R';
                tipoFecha1:='_recepcion_sii';
        else
                tipo_dia1:='A';
                tipoFecha1:=' ';
        end if;
	--now1:=to_char(now(),'YYYYMMDD')::varchar;
	now1:=now();

	filtro_fecha1:='';
	if get_json('_ORIGEN_',json2)='RS' then
		filtro_fecha1:=' dia'||tipoFecha1||' in '||fecha_in1||' ';
	else
		filtro_fecha1:=' dia'||tipoFecha1||'='||now_int||' ';
	end if;

	--PERFILAMIENTO--
	json_rut1:=obtiene_filtro_perfilamiento_rut_emisor_6000(get_json('rutCliente',json2)::bigint,get_json('rutUsuario',json2)::bigint,'rut_emisor',get_json('RUT_EMISOR',json2));
        v_parametro_rut_emisor:=get_json('TAG_RUT_EMISOR',json_rut1);
        v_parametro_rut_emisor_com:=get_json('TAG_RUT_EMISOR_COMILLAS',json_rut1);
        json2:=logjson(json2,'v_parametro_rut_emisor='||v_parametro_rut_emisor);

        v_parametro_tipo_dte:='';
        aux:=get_json('tipo_dte_filtro',json2);
        v_parametro_tipo_dte:=obtiene_filtro_perfilamiento_usuario_6000(v_rut_emisor::bigint,get_json('rutUsuario',json2)::bigint,'tipo_dte',aux);
        json2:=logjson(json2,'PARAMETRO v_parametro_tipo_dte='||coalesce(v_parametro_tipo_dte,'vacio'));
	--PERFILAMIENTO--


        --Saco los estados del redshift
	filtro_cuadro2:='';
	if (get_json('MONTO_CANTIDAD',json2)='MONTO') then
                monto_cantidad1:=' sum(monto_total) as total ';
		monto_cantidad2:=' monto_total ';
        else
                monto_cantidad1:=' count(*) as total ';
		monto_cantidad2:=' 1 ';
        end if;
	param1:=get_json('PARAM',json2);
	alias1:=(select alias_web from filtros_rut where rut_emisor=v_rut_emisor and parametro=param1 and canal='EMITIDOS');
	if alias1 is null then
		json4:=put_json(json4,'_LOG_',get_json('_LOG_',json2));
		json4:=put_json(json4,'__FLUJO_ACTUAL__',get_json('__FLUJO_ACTUAL__',json2));
		json4:=put_json(json4,'EMITIDOS_LOCAL','[]');
		json4:=put_json(json4,'EMITIDOS_RS','[]');
		return json4;
	end if;
		
	json4:=put_json(json4,'alias_param',alias1);
	--factura_electronica__info__33__link
	query1:='select '||monto_cantidad1||','||get_json('PARAM',json2)||' as '||alias1||',

			sum(case when estado_sii in (''ACEPTADO_POR_EL_SII'') then '||monto_cantidad2||' else 0 end) as ACEPTADO_POR_EL_SII,
			sum(case when estado_sii in (''ACEPTADO_CON_REPAROS_POR_EL_SII'') then '||monto_cantidad2||' else 0 end) as ACEPTADO_CON_REPAROS_POR_EL_SII,
			sum(case when estado_sii=''RECHAZADO_POR_EL_SII'' then '||monto_cantidad2||' else 0 end) as RECHAZADO_POR_EL_SII,
			sum(case when coalesce(estado_reclamo,'''')=''OTORGA_RECIBO_DE_MERCADERIA_O_SERVICIO'' or coalesce(estado_nar,'''')=''ACEPTACION_DE_CONTENIDO_DE_DOCUMENTO'' then '||monto_cantidad2||' else 0 end) as ACEPTADOS_POR_EL_RECEPTOR,
			sum(case when coalesce(estado_reclamo,'''') in (''RECLAMO_FALTA_PARCIAL_DE_MERCADERIA'',''RECLAMO_FALTA_TOTAL_DE_MERCADERIA'') or coalesce(estado_nar,'''')=''RECHAZO_DE_CONTENIDO_DE_DOCUMENTO'' then '||monto_cantidad2||' else 0 end) as RECLAMADOS_POR_EL_RECEPTOR,
			sum(case when coalesce(estado_reclamo,'''')='''' and coalesce(estado_nar,'''')='''' and '''||now1||'''::timestamp -fecha_sii>interval ''8 days'' then 1 else 0 end) as ACEPTADOS_9_DIAS_RECEPTOR
			
			from dte_emitidos where '||v_parametro_rut_emisor||' '||v_parametro_tipo_dte||'  and '||filtro_fecha1||' '||filtro_cuadro2||' group by 2 order by 2';

	query1:=replace(query1,chr(10),'');

	json4:=put_json(json4,'_LOG_',get_json('_LOG_',json2));
	json4:=put_json(json4,'__FLUJO_ACTUAL__',get_json('__FLUJO_ACTUAL__',json2));
	if get_json('_ORIGEN_',json2)='RS' then
        	json_par1:=get_parametros_motor_json('{}','BASE_REDSHIFT_EMITIDOS');

		perform logfile('DAO_14715 QUERY RS ='||query1);

		json3:=query_db_json(get_json('__IP_CONEXION_CLIENTE__',json_par1),get_json_int('__IP_PORT_CLIENTE__',json_par1)::integer,query1);
		json4:=logjson(json4,'Busqueda Recibidos '||json3::varchar);
		if (get_json('STATUS',json3)='SIN_DATA') then
			json4:=logjson(json4,'Sin Datos');
			json4:=put_json(json4,'EMITIDOS_RS','[]');
			return json4;
		end if;
		if (get_json('STATUS',json3)<>'OK') then
			json4:=logjson(json4,'Query ='||query1);
			json4:=logjson(json4,'Falla Busqueda de Emitidos BASE_REDSHIFT_EMITIDOS');
			json4:=put_json(json4,'EMITIDOS_RS','[]');
			return json4;
		end if;
		if (get_json('TOTAL_REGISTROS',json3)='1') then
			json3:=json_remove_json(json3::varchar,'["STATUS","TOTAL_REGISTROS"]');
			lista1:='['||json3||']';
		else
			lista1:=get_json('LISTA',json3);
		end if;	
		json4:=put_json(json4,'EMITIDOS_RS',lista1);
	else
		perform logfile('DAO_14715 QUERY LOCAL ='||query1);

		execute 'select array_to_json(array_agg(row_to_json(sql))) from ('||query1||')sql' into j1;
		if j1 is null then
			json4:=put_json(json4,'EMITIDOS_LOCAL','[]');
		else
			json4:=put_json(json4,'EMITIDOS_LOCAL',j1::varchar);
		end if;
	end if;

	return json4;
END;
$$ LANGUAGE plpgsql;

