delete from isys_querys_tx where llave='15100';
CREATE or replace FUNCTION pivote_busqueda_15100(json) RETURNS json AS $$
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
	cli1	varchar;
	campo record;
	link1	varchar;
BEGIN
        json2:=json1;
        json2:=put_json(json2,'LLAMA_FLUJO','SI');
	if (get_json('FOLIO',json2)<>'') then
	        json2:=put_json(json2,'__SECUENCIAOK__','15101');
	elsif (get_json('aplicacion',json2) in ('FINANCIAMIENTO','FINANCIAMIENTO_FIN')) then
		--Si el cliente no ha aceptado el TyC no le mostramos nada
		if get_json('aplicacion',json2)='FINANCIAMIENTO' then
			cli1:=get_json('rutCliente',json2);
			select * into campo from mensaje_usuario where rut_empresa =cli1 and categoria='FIN_TyC_v2' and leido='SI' limit 1;
			if not found then
				--			json_aux:=(select row_to_json(sql) from (select 'aplicacion' as tipo,'badge_'||campo.id::varchar as id,campo.icon as icon,caption,badge1 as badge,remplaza_tags_6000(href,json_data) as href from menu_info_10k where id2=campo.id2) sql);
				link1:=(select remplaza_tags_6000(href,json2)||'%26abre_acordeon=ac17_fin_tyc' as href from menu_info_10k where id2='configDinamico' limit 1);
				json2:=put_json(json2,'MENSAJE_RESPUESTA','Usted no ha aceptado el Términos y Condiciones de la Plataforma de Financiamiento, <a href="'||link1||'">Revise Terminos y Condiciones</a>');
				json2:=put_json(json2,'CODIGO_RESPUESTA','2');
				json2:=put_json(json2,'__SECUENCIAOK__','0');
				json2:=responde_pantalla_15100(json2);
				return json2;
			end if;

		end if;
		--En el caso de Financiamiento no se va al flujo por folio
        	json2:=put_json(json2,'__SECUENCIAOK__','15100');
		if (get_json('folio_desde',json2)<>'' or get_json('folio_hasta',json2)<>'') then
			if get_json('folio_desde',json2)='' then
				json2:=put_json(json2,'folio_desde',get_json('folio_hasta',json2));
				json2:=put_json(json2,'FSTART',(get_json('FSTART',json2)::date - interval '4 months')::date::varchar);
			elsif get_json('folio_hasta',json2)='' then
				json2:=put_json(json2,'FSTART',(get_json('FSTART',json2)::date - interval '4 months')::date::varchar);
				json2:=put_json(json2,'folio_hasta',get_json('folio_desde',json2));
			end if;
		end if;
	elsif (get_json('folio_desde',json2)<>'' or get_json('folio_hasta',json2)<>'') then
		if get_json('TIPO_DTE',json2)='801' then
			json2:=put_json(json2,'__SECUENCIAOK__','15100');
                elsif get_json('folio_desde',json2)='' then
                        json2:=put_json(json2,'__SECUENCIAOK__','15101');
                        json2:=put_json(json2,'FOLIO',get_json('folio_hasta',json2));
                elsif get_json('folio_hasta',json2)='' then
                        json2:=put_json(json2,'__SECUENCIAOK__','15101');
                        json2:=put_json(json2,'FOLIO',get_json('folio_desde',json2));
                elsif get_json('folio_hasta',json2)=get_json('folio_desde',json2) then
                        json2:=put_json(json2,'__SECUENCIAOK__','15101');
                        json2:=put_json(json2,'FOLIO',get_json('folio_desde',json2));
                else
                        json2:=put_json(json2,'__SECUENCIAOK__','15100');
                end if;
	else
	        json2:=put_json(json2,'__SECUENCIAOK__','15100');
	end if;
        return json2;
END;
$$ LANGUAGE plpgsql;

insert into isys_querys_tx values ('15100','10',9,1,'select arma_filtros_15100(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0); 

--Cuenta Boletas Redshift
insert into isys_querys_tx values ('15100','20',35,1,'$$QUERY_RS$$',0,0,0,9,1,50,50);
--Cuenta Recibidos Redshift
insert into isys_querys_tx values ('15100','30',22,1,'$$QUERY_RS$$',0,0,0,9,1,50,50);
--Cuenta Emiutidos Redshift Tipo9 = Salida json
insert into isys_querys_tx values ('15100','40',23,1,'$$QUERY_RS$$',0,0,0,9,1,50,50); 

--Junto resultados del contado
insert into isys_querys_tx values ('15100','50',9,1,'select resultado_cuenta_pivote_query_15100(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('15100','60',9,1,'select responde_pantalla_15100(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,0,0);

--LOOP de tablas
--Base Normal
insert into isys_querys_tx values ('15100','70',9,1,'$$QUERY_DATA$$',0,0,0,9,1,80,80);
--Base Replica - Solo Reportes
insert into isys_querys_tx values ('15100','71',11,1,'$$QUERY_DATA$$',0,0,0,9,1,80,80);
--Base Emitidos Historicos
insert into isys_querys_tx values ('15100','72',25,1,'$$QUERY_DATA$$',0,0,0,9,1,80,80);
--Base Importados
insert into isys_querys_tx values ('15100','74',26,1,'$$QUERY_DATA$$',0,0,0,9,1,80,80);
--Boletas 2014
insert into isys_querys_tx values ('15100','76',17,1,'$$QUERY_DATA$$',0,0,0,9,1,80,80);
--Boletas Historicas
insert into isys_querys_tx values ('15100','77',39,1,'$$QUERY_DATA$$',0,0,0,9,1,80,80);

insert into isys_querys_tx values ('15100','80',9,1,'select arma_next_query1_15100(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

CREATE or replace FUNCTION arma_next_query1_15100(json) RETURNS json AS $$
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
begin
	json2:=json1;
	json2:=put_json(json2,'__FUNC__','arma_next_query1_15100');
	if (get_json('RES_JSON_1',json2)='') then
		perform graba_estadisticas_busqueda(json1,get_json('CAT_EST',json2),'NK');	
		json2:=logjsonfunc(json2,'Falla obtener resultado de '||get_json('TABLA',json2)|| ' Base '||get_json('PARAMETRO_TABLA',json2));
		json2:=put_json(json2,'MENSAJE_RESPUESTA','Reintente por favor (FT)');
		json2:=put_json(json2,'CODIGO_RESPUESTA','2');
		json2:=put_json(json2,'__SECUENCIAOK__','0');
		json2:=responde_pantalla_15100(json2);
		return json2;
	end if;
	v_out_resultado:=get_json('v_out_resultado',json2);
	if (v_out_resultado<>'') then
		v_out_resultado:=json_merge_lists(v_out_resultado::varchar,get_json('array_to_json',get_json('RES_JSON_1',json2)::json));
	else
		v_out_resultado:=get_json('array_to_json',get_json('RES_JSON_1',json2)::json);
	end if;
	json2:=put_json(json2,'v_out_resultado',v_out_resultado);

	perform graba_estadisticas_busqueda(json1,get_json('CAT_EST',json2),'OK');

	i:=get_json('__CONTADOR__',json2)::integer;
	j1:=get_json('__aux_tablas',json2);
	--Si el contador es mayor que el total de tablas..
	if (i=count_array_json(j1)) then
		json2:=logjsonfunc(json2,'Termino de hacer querys');
		json2:=put_json(json2,'MENSAJE_RESPUESTA','OK');
		json2:=put_json(json2,'CODIGO_RESPUESTA','1');
		json2:=put_json(json2,'__SECUENCIAOK__','0');
		json2:=responde_pantalla_15100(json2);
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
		json2:=responde_pantalla_15100(json2);
		return json2;
	end if;
	*/
	json5:=decode_hex(get_json('JSON5',json2));
	query2:=decode_hex(get_json('QUERY2',json2));
	fecha_c1:=get_json_index(j1,i);
	tabla1:=get_json('PREFIJO_TABLA',json2)||fecha_c1;
	if tabla1='dte_boletas_no_borrar_fay' then
		par1:=null;
		tabla1='dte_boletas_no_borrar_fay ';
	elsif tabla1='dte_boletas_amazon2014_no_borrar_fay' then
		par1:='BASE_AMAZON_BOLETAS_2014';
		tabla1='dte_boletas_amazon2014_no_borrar_fay';
	elsif tabla1='dte_boletas_amazon2016_no_borrar_fay' then
		par1:='BASE_AMAZON_BOLETAS_HISTORICAS';
		tabla1='dte_boletas_amazon2016_no_borrar_fay';
	else
		execute 'select parametro_motor from '||get_json('TABLA_CONFIGURACION',json2)||' where periodo_desde<='||split_part(fecha_c1,'_',1)||' and periodo_hasta>='||split_part(fecha_c1,'_',1) into par1;
	end if;
	json5:=put_json(json5,'TABLA',tabla1);	
	json2:=put_json(json2,'TABLA',tabla1);	
	query1:=remplaza_tags_json_c(json5,query2);
	json2:=put_json(json2,'QUERY_DATA',query1);
		
	json2:=put_json(json2,'PARAMETRO_TABLA',par1::varchar);
	json2:=put_json(json2,'CAT_EST',coalesce(par1::varchar,'LOCAL')||'__'||tabla1);
	if (par1='BASE_EMITIDOS_HISTORICOS') then
		json2:=put_json(json2,'__SECUENCIAOK__','72');
	elsif (par1='BASE_AMAZON_IMPORTADOS') then
		json2:=put_json(json2,'__SECUENCIAOK__','74');
	elsif (par1='BASE_AMAZON_BOLETAS_2014') then
		json2:=put_json(json2,'__SECUENCIAOK__','76');
	elsif (par1='BASE_AMAZON_BOLETAS_HISTORICAS') then
		json2:=put_json(json2,'__SECUENCIAOK__','77');
	else
		--if get_json('rutUsuario',json2)='17597643' then
		--DAO 20201001 para los reportes vamos a la base de replica
		if is_number(get_json('id_reporte',json2)) then
			json2:=put_json(json2,'__SECUENCIAOK__','71');
		else
			json2:=put_json(json2,'__SECUENCIAOK__','70');
		end if;
		--perform logfile('FLUJO_15100 QUERY1 '||query1);
	end if;
	json2:=put_json(json2,'__TOTAL_RESP_ESPERADAS__','1');
	json2:=put_json(json2,'__CONTADOR__',(i+1)::Varchar);
	json2:=logjsonfunc(json2,'Ejecuta '||coalesce(par1,'LOCAL')||' Tabla '||tabla1);
	return json2;
end;
$$
LANGUAGE plpgsql;

CREATE or replace FUNCTION responde_pantalla_15100(json) RETURNS json AS $$
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
	json2:=put_json(json2,'__FUNC__','responde_pantalla_15100');
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
                json3:=put_json(json3,'informacion',json_info::varchar);
        else
                json3:=put_json(json3,'criterio_busqueda_excel',get_json('criterio_busqueda_excel',json2));
                json3:=put_json(json3,'total_regs','0');
                json3:=put_json(json3,'cantidad_paginas','0');
        end if;

        estado1:=get_json('ESTADO',json2);
        if(estado1<>'') then
                select glosa from estado_dte where descripcion=estado1 into evento1;
                json2:=put_json(json2,'EVENTO_CUAD',evento1);
        end if;
        if(tipo_dte1<>'') then
                if(tipo_dte1 in ('39','41')) then
                        json2:=put_json(json2,'GRUPO_CUAD','documentos_boletas');
                elsif(tipo_dte1 in ('110','111','112')) then
                        json2:=put_json(json2,'GRUPO_CUAD','documentos_exportacion');
                else
                        json2:=put_json(json2,'GRUPO_CUAD','documentos_nacionales');
                end if;
        end if;

        json3:=put_json(json3,'titulo','Búsqueda Emitidos');
        json3:=put_json(json3,'flag_paginacion','NO');
        json3:=put_json(json3,'flag_paginacion_manual','SI');
        json3:=put_json(json3,'flag_tipo_cuadro','GRILLA');
        json3:=put_json(json3,'caption_buscar','Buscar en esta pagina :');

	if(tipo_dte1 in ('39','41') or get_json('v_lista_errores',json2)='SI') then
		--Las boletas no tienen boton cesion
		select array_to_json(array_agg(row_to_json(sql))) from (select titulo as nombre,tx,labels_no_editable,funcion,funcion_modal,titulo,tipo, reemplaza_regex_valores_json(reemplaza_regex_json(href,json2),json2) as href,glyphicon,caption from acciones_grillas where id_pantalla=app1 and categoria='BOTONERA_GRILLA' and check_funcionalidad_6000(json2,valor) and case when valor='ExportarPDF' then case when get_json('parametro_pdf_masivo',json2)='SI' then true else false end else true end and valor not in ('CESION') order by orden) sql into json_out2;
	else
		if get_json('ESTADO',json2)='EN_PROCESO' then
                        select array_to_json(array_agg(row_to_json(sql))) from (select titulo as nombre,tx,labels_no_editable,funcion,funcion_modal,titulo,tipo, reemplaza_regex_valores_json(reemplaza_regex_json(href,json2),json2) as href,glyphicon,informacion,caption from acciones_grillas where id_pantalla=app1 and categoria='BOTONERA_GRILLA' and labels_editable='EN_PROCESO' and (case when get_json('rutCliente',json2) in ('97018000','96919050','96722460') and valor='AnularNC' then true else check_funcionalidad_6000(json2,valor) end) and case when valor='ExportarPDF' then case when get_json('parametro_pdf_masivo',json2)='SI' then true else false end else true end order by orden) sql into json_out2;
                else
			select array_to_json(array_agg(row_to_json(sql))) from (select titulo as nombre,tx,labels_no_editable,funcion,funcion_modal,titulo,tipo, reemplaza_regex_valores_json(reemplaza_regex_json(href,json2),json2) as href,glyphicon,informacion,caption from acciones_grillas where id_pantalla=app1 and categoria='BOTONERA_GRILLA' and coalesce(labels_editable,'')<>'EN_PROCESO' and check_funcionalidad_6000(json2,valor) and case when valor='ExportarPDF' then case when get_json('parametro_pdf_masivo',json2)='SI' then true else false end else true end order by orden) sql into json_out2;
		end if;
	end if;
        -- NBV 201705
        if(tipo_dte1='801') then
                json_oc:='[]';
                select row_to_json(sql) from (select titulo as nombre,tx,labels_no_editable,funcion,funcion_modal,titulo,tipo,reemplaza_regex_valores_json(reemplaza_regex_json(href,json2),json2) as href,glyphicon,caption from acciones_grillas where id_pantalla=app1 and categoria='BOTONERA_GRILLA' and valor='Exportar') sql into json_pdf1;
                json_oc:=put_json_list(json_oc,json_pdf1);
                json3:=put_json(json3,'botones_tabla',json_oc);
        else
        -- NBV 201705
                json3:=put_json(json3,'botones_tabla',json_out2);
        end if;
	if get_json('__BOTONES_TABLA__',json2)<>'' then
                json3:=put_json(json3,'botones_tabla',get_json('__BOTONES_TABLA__',json2));
	end if;

        json2:=response_requests_6000('1', 'OK', json3::varchar,json2);
        RETURN json2;
end;
$$
LANGUAGE plpgsql;


CREATE or replace FUNCTION arma_filtros_15100(json) RETURNS json AS $$
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
	order1	varchar;
	ret1	varchar;	
	jaux1	json;
	jaux2	json;
	j	integer;
	aux1	varchar;
	-- FGE - 20200225 - Para el filtro de tipo de busqueda OC Dipres
        v_tipo_institucion  varchar;

	pag_base_colas1	integer;
	sobra_base_colas1 integer;
	total_base_colas1 integer;
	total_base_colas2 integer;
BEGIN
        json2:=json1;
	json2:=put_json(json2,'__FUNC__','arma_filtro');
        flag_boleta1:='NO';
        flag_cuenta_estado:=false;
        v_in_rut_emisor:=get_json('rutCliente',json2)::integer;
        v_rut_usuario:=get_json('rutUsuario',json2)::integer;
        flag_codigo_txel:='NO';

	json2:=put_json(json2,'__SECUENCIAOK__','0');

        --VARIABLES DE ENTRADA--
        vin_fstart:=get_json('FSTART',json2);
        vin_fend:=get_json('FEND',json2);
        vin_estado:=get_json('ESTADO',json2);
        vin_rut_receptor:=split_part(replace(get_json('RUT_RECEPTOR',json2),'.',''),'-',1);
        vin_rut_emisor:=split_part(replace(get_json('RUT_EMISOR',json2),'.',''),'-',1);
        vin_folio:=get_json('FOLIO',json2);
        vin_tipo_fecha:=get_json('TIPO_FECHA',json2);
        vin_tipo_dte:=get_json('TIPO_DTE',json2);

        vin_offset:=get_json('offset',json2);
        vin_rol:=get_json('rol_usuario',json2);
        vin_count_table:=replace(get_json('count_table',json2),'.','');

        json2:=logjsonfunc(json2,'Entro a select_detalle_dte_emitidos_6000 VARIABLES_ENTRADA= vin_fstart->'||vin_fstart||', vin_fend->'||vin_fend||', vin_estado->'||vin_estado||', vin_rut_receptor->'||vin_rut_receptor||', vin_rut_emisor->'||vin_rut_emisor||', vin_folio->'||vin_folio||', vin_tipo_fecha->'||vin_tipo_fecha||', vin_tipo_dte->'||vin_tipo_dte||', vin_offset->'||vin_offset||', vin_rol->'||vin_rol||', vin_count_table->'||vin_count_table);
        --/VARIABLES DE ENTRADA--
        rol1:=vin_rol;

        --FECHAS--
        json2:=corrige_fechas(json2);
        v_in_fecha_inicio:=get_json('fstart',json2)::integer;
        v_in_fecha_fin:=get_json('fend',json2)::integer;

	-- FGE - 20200225 - Para el filtro de tipo de busqueda OC Dipres
        v_tipo_institucion := get_json('TIPO_OC', json2);

	tipo_dia1:=vin_tipo_fecha;

	if (tipo_dia1='Emision') then
		tipo_dia1:='_emision';
		tipo_dia_ind1:='E';
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
		json2:=logjsonfunc(json2,'Total de Pag '||total_pag1);
	end if;

	--Si el offset es 0, debo contar
	if (v_in_offset1=0) then
		total_pag1:='-1';
	end if;

	json2:=logjsonfunc(json2,'Entro a select_detalle_dte_emitidos_6000 ID='||id_reporte1);
	--/OFFSET/EXCEL--

	--PARAMETOS--
	--Agrega parametro tipo_dte
	v_parametro_tipo_dte:='';
	v_parametro_tipo_dte:=obtiene_filtro_perfilamiento_usuario_6000(v_in_rut_emisor,v_rut_usuario,'tipo_dte',vin_tipo_dte);
	json2:=logjsonfunc(json2,'PARAMETRO v_parametro_tipo_dte='||coalesce(v_parametro_tipo_dte,'vacio'));

	--rut_emisor
	if get_json('aplicacion',json2)='FINANCIAMIENTO_FIN' then
		vin_rut_emisor:=trim(split_part(get_json('RUT_EMISOR',json2),' -- ',1));
		vin_rut_emisor:=trim(vin_rut_emisor);
		vin_rut_emisor:=trim(vin_rut_emisor);
                if strpos(get_json('lista_rut_dato',json2),vin_rut_emisor)>0 and vin_rut_emisor<>'' then
                        json2:=put_json(json2,'__SECUENCIAOK__','0');
                        json2:=response_requests_6000('2', 'El filtro rut emisor no puede estar contenido en el filtro rut a excluir', '',json2);
                        return json2;
                end if;
                json_rut1:=obtiene_filtro_perfilamiento_rut_emisor_6000(v_in_rut_emisor,v_rut_usuario,'rut_emisor','FINANCIADOR__'||vin_rut_emisor||'__'||encode_hex(get_json('lista_rut_dato',json2)));
                --json_rut1:=obtiene_filtro_perfilamiento_rut_emisor_6000(v_in_rut_emisor,v_rut_usuario,'rut_emisor','FINANCIADOR__'||vin_rut_emisor);
		--json_rut1:=obtiene_filtro_perfilamiento_rut_emisor_6000(v_in_rut_emisor,v_rut_usuario,'rut_emisor','FINANCIADOR');
	elsif(get_json('aplicacion',json2)='CONSULTA_BBVA_SCOTIABANK') then
                json_rut1:=obtiene_filtro_perfilamiento_rut_emisor_6000(v_in_rut_emisor,v_rut_usuario,'rut_emisor','CONSULTA_BBVA_SCOTIABANK__'||vin_rut_emisor);
	elsif get_json('tipo_tx',json2)='buscar_boletas_por_rango' and get_json('PARAMETRO_RUT_EMISOR_BOLETA',json2)<>'' then
		json_rut1:=put_json('{}','TAG_RUT_EMISOR',get_json('PARAMETRO_RUT_EMISOR_BOLETA',json2));
		json_rut1:=put_json(json_rut1,'TAG_RUT_EMISOR_COMILLAS',get_json('PARAMETRO_RUT_EMISOR_BOLETA',json2));
		json_rut1:=put_json(json_rut1,'TAG_LISTA_RUT_EMISORES',get_json('LISTA_PARAMETRO_RUT_EMISOR_BOLETA',json2));
	else
		json_rut1:=obtiene_filtro_perfilamiento_rut_emisor_6000(v_in_rut_emisor,v_rut_usuario,'rut_emisor',vin_rut_emisor);
	end if;
	-- FGE - 20200225 - Para la busqueda Cenabast e Institucion
	v_parametro_rut_emisor:=get_json('TAG_RUT_EMISOR',json_rut1);
	v_parametro_rut_emisor_com:=get_json('TAG_RUT_EMISOR_COMILLAS',json_rut1);
	if v_tipo_institucion in ('1') then
		v_parametro_rut_emisor:=get_json('TAG_RUT_EMISOR',json_rut1);
	elsif v_tipo_institucion = '2' then
		v_parametro_rut_emisor:= ' rut_emisor=61608700 ';
	elsif v_tipo_institucion = '3' then
		v_parametro_rut_emisor:= ' rut_emisor in (' || split_part(get_json('TAG_RUT_EMISOR', json_rut1), '=', 2) ||', 61608700)'; 
	end if;

	flag_rut_emisor1:=get_json('FLAG_RUT_EMISOR',json_rut1);
	json2:=logjsonfunc(json2,'v_parametro_rut_emisor='||v_parametro_rut_emisor ||' v_in_rut_emisor=' ||v_in_rut_emisor||' rut_emisor_filtro='||vin_rut_emisor);
	json2:=put_json(json2,'v_parametro_rut_emisor',v_parametro_rut_emisor);
	json2:=put_json(json2,'lista_rut_emisores',get_json('TAG_LISTA_RUT_EMISORES',json_rut1));

	--Si tiene parametros adicionales, los usamos para filtrar la query
	--parametro1='E512' and parametro2='ERP'
	v_parametro_var:='';
	v_nombres_parametros_var:='';
	v_nombres_parametros_var_vacio:='';
	texto_filtro_params:='';
	--FAY 2019-02-19 Se saca el PARAMETRO5 de los filtros ya que se usa para el codigo unico del cliente
	for campo in select lower(parametro) as parametro,alias_web from filtros_rut where rut_emisor=v_in_rut_emisor and parametro<>'PARAMETRO5' loop
		aux:=get_json(campo.parametro,json2);
		tmp1:=obtiene_filtro_perfilamiento_usuario_6000(v_in_rut_emisor,v_rut_usuario,upper(campo.parametro),aux);
		json2:=logjsonfunc(json2,'PARAMETRO '||campo.parametro||'='||coalesce(tmp1,'vacio') || ' aux[' || aux ||']');
		texto_filtro_params:=texto_filtro_params||' <b>'||campo.alias_web||'=</b>'||replace(aux,'*','TODOS');

		v_parametro_var:=v_parametro_var|| ' ' ||tmp1;

		--Tenemos que construir los nombre para la grilla de los parametros adicionales
		v_nombres_parametros_var:=v_nombres_parametros_var||',coalesce('||trim(campo.parametro)||','''') as INFO__'||trim(campo.alias_web)||'__ON';
		v_nombres_parametros_var_vacio:=v_nombres_parametros_var_vacio||','''' as INFO__'||trim(campo.alias_web)||'__ON';
	end loop;
--/PARAMETOS--

	if (get_json('FORMA_PAGO',json2)<>'') then
		if (get_json('FORMA_PAGO',json2)='-1') then
			v_parametro_var:=v_parametro_var|| ' and strpos(data_dte,''<FmaPago>'')=0 ';
		--Para Finaciamiento Necesitamos todas las q no son al Contado
		elsif (get_json('FORMA_PAGO',json2)='-Contado') then
			v_parametro_var:=v_parametro_var|| ' and strpos(data_dte,''<FmaPago>1</FmaPago>'')=0 ';
		else
			v_parametro_var:=v_parametro_var|| ' and strpos(data_dte,''<FmaPago>'||get_json('FORMA_PAGO',json2)||'</FmaPago>'')>0 ';
		end if;
	end if;

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
	if get_json('ESTADO_R42',json2)<>'' then
                ret1=get_json('ESTADO_R42',json2);
		--v_in_rut_receptor:= v_in_rut_receptor ||' and rut_receptor in ('||(select string_agg(''''||rut_emisor::varchar||'''',',') from contribuyentes where resolucion_42=ret1)||') ';
		v_in_rut_receptor:= v_in_rut_receptor ||' and rut_receptor in ('||(select string_agg(''''||rut_emisor::varchar||'''',',') from retenidos_retenedores_por_cliente where empresa=v_in_rut_emisor and resolucion_42=ret1)||') ';
        end if;
--/RUT_RECEPTOR--

	--Armo la busqueda para las refernecias
	v_parametro_referencias1:='';
	if (get_json('TIPO_REFERENCIA',json2)='SIN_61') then
		v_parametro_referencias1:=' and strpos(data_dte,''<CON_NC>'')=0 ';
	elsif (get_json('TIPO_REFERENCIA',json2)<>'*' and get_json('TIPO_REFERENCIA',json2)<>'' and get_json('VALOR_REFERENCIA',json2)<>'') then
		v_parametro_referencias1:=' and strpos(referencias::varchar,''"Tipo":"'||get_json('TIPO_REFERENCIA',json2)||'","Folio":"'||get_json('VALOR_REFERENCIA',json2)||'"'')>0 ';
	elsif (get_json('TIPO_REFERENCIA',json2)<>'*' and get_json('TIPO_REFERENCIA',json2)<>'') then
		v_parametro_referencias1:=' and strpos(referencias::varchar,''"Tipo":"'||get_json('TIPO_REFERENCIA',json2)||'"'')>0 ';
	elsif (get_json('VALOR_REFERENCIA',json2)<>'') then
                v_parametro_referencias1:=v_parametro_referencias1||' and strpos(referencias::varchar,''"Folio":"'||get_json('VALOR_REFERENCIA',json2)||'"'')>0 ';
        end if;

        v_parametro_adicional1:='';
        --Parametros Adicionales PARAMETRO_ADICIONAL, VALOR_PARAMETRO_ADICIONAL
        if (get_json('PARAMETRO_ADICIONAL',json2)<>'' and get_json('VALOR_PARAMETRO_ADICIONAL',json2)<>'') then
                v_parametro_adicional1:=' and strpos(data_dte,''<'||get_json('PARAMETRO_ADICIONAL',json2)||'>'||split_part(get_json('VALOR_PARAMETRO_ADICIONAL',json2),'-_-',1)||'</'||get_json('PARAMETRO_ADICIONAL',json2)||'>'')>0 ';
        elsif(get_json('PARAMETRO_ADICIONAL',json2)<>'') then
                v_parametro_adicional1:=' and strpos(data_dte,''<'||get_json('PARAMETRO_ADICIONAL',json2)||'>'')>0 ';
        elsif(get_json('VALOR_PARAMETRO_ADICIONAL',json2)<>'') then
                v_parametro_adicional1:=' and strpos(data_dte,''>'||split_part(get_json('VALOR_PARAMETRO_ADICIONAL',json2),'-_-',1)||'</'')>0 ';
        end if;
        if is_json_list(get_json('LISTA_PARAMETRO_ADICIONAL',json2)) then
                jaux1:=get_json('LISTA_PARAMETRO_ADICIONAL',json2)::json;
                j:=0;
                aux1:=get_json_index(jaux1,j);
                while (aux1<>'') loop
                        v_parametro_adicional1:=v_parametro_adicional1||' and strpos(data_dte,''<'||get_json('campo',aux1::json)||'>'||get_json('valor',aux1::json)||'</'||get_json('campo',aux1::json)||'>'')>0 ';
                        j:=j+1;
                        aux1:=get_json_index(jaux1,j);
                end loop;
        end if;
        --CONTROLLER
        if(get_json('CONTROLLER',json2)<>'') then
                --v_parametro_adicional1:=' and strpos(data_dte,''"'||replace(get_json('CONTROLLER',json2),'---',' ')||'"'')>0 ';
                v_parametro_adicional1:=' and strpos(data_dte,''"'||decode_hex(get_json('CONTROLLER',json2))||'"'')>0 ';
        end if;

        if is_number(get_json('folio_desde',json2)) and is_number(get_json('folio_hasta',json2)) then
		v_parametro_adicional1:=v_parametro_adicional1||' and folio>='||get_json('folio_desde',json2)||' and folio<='||get_json('folio_hasta',json2)||' ';
        end if;
        --20190220 mvillanueva
        if get_json('TIPO_DTE',json2)='801' then
                --if is_number(get_json('folio_desde',json2)) then
                if get_json('folio_desde',json2)<>'' then
                        v_parametro_adicional1:=v_parametro_adicional1||' and folio='''||get_json('folio_desde',json2)||'''';
                end if;
                if get_json('ESTADO_OC',json2)<>'' then
                        v_parametro_adicional1:=v_parametro_adicional1||' and estado='''||get_json('ESTADO_OC',json2)||'''';
                end if;
                if get_json('MODELO_PAGO',json2)<>'' then
                        v_parametro_adicional1:=v_parametro_adicional1||' and get_xml(''Modelopago'',data_dte)='''||get_json('MODELO_PAGO',json2)||'''';
                end if;
                if get_json('FCH_EFE_PAGO',json2)<>'' and get_json('FCH_EFE_PAGO',json2) similar to '[0-9]{4}\-[0-9]{2}\-[0-9]{2}' then
                        v_parametro_adicional1:=v_parametro_adicional1||' and get_xml(''FechaEstimadaPago'',data_dte)='''||replace(get_json('FCH_EFE_PAGO',json2),'-','')||''
'';
                end if;
                if get_json('FCH_PAGO',json2)<>'' and get_json('FCH_PAGO',json2) similar to '[0-9]{4}\-[0-9]{2}\-[0-9]{2}' then
                        v_parametro_adicional1:=v_parametro_adicional1||' and get_xml(''FechaPago'',data_dte)='''||replace(get_json('FCH_PAGO',json2),'-','')||'''';
                end if;
		json2:=logjsonfunc(json2,'Filtro OC --> '||v_parametro_adicional1);	
        end if;
        --TIPO_DTE--
	--Si viene un * son Todos los DTE menos las boletas
	if (vin_tipo_dte='*' or vin_tipo_dte='') then
		v_tipo_dte:=(select '('||string_agg(codigo,',')||')' from detalle_parametros where id_parametro = 31 and codigo not in ('39','41'));
		v_tipo_dte_com:=(select '('||string_agg(quote_literal(codigo),',')||')' from detalle_parametros where id_parametro = 31 and codigo not in ('39','41'));

	elsif(get_json('LISTA_TIPO_BOLETA',json2)<>'') then
                flag_boleta1:='SI';
                v_tipo_dte:='('||get_json('LISTA_TIPO_BOLETA',json2)||')';
                v_tipo_dte_com:='('||get_json('LISTA_TIPO_BOLETA',json2)||')';
                v_parametro_tipo_dte:=' and tipo_dte in ('||get_json('LISTA_TIPO_BOLETA',json2)||')';
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
	end if;

	json2:=logjsonfunc(json2,'v_tipo_dte='||v_tipo_dte);
	--/TIPO_DTE--

        --ESTADOS--
	v_in_evento:=vin_estado;
	v_lista_errores:='';
	lista_cesion1:='';
	v_glosa_estado := '';
	v_estado := '';
	v_estado_indexer:=' estado=''EMI''';
	--Solo setea si no es busqueda por folio
	if(v_in_evento<>'') then
		select * into stEstadoDte from estado_dte where descripcion=v_in_evento and length(codigo)=3;
		if found then
			v_glosa_estado:=stEstadoDte.glosa;
			v_lista_errores:=stEstadoDte.flag_lista_errores;
			if (v_lista_errores='SI') then
				json2:=put_json(json2,'v_lista_errores','SI');
			end if;
			if (stEstadoDte.codigo='CES') then
				flag_cuenta_estado:=true;
				lista_cesion1:='SI';
			end if;

			if(v_in_evento='EMITIDO')then
				v_estado := '';
				v_estado_indexer:=' estado=''EMI''';
			elsif (stEstadoDte.codigo in ('ACD','RCD','RFP','RFT','ERM','RNC','RSC','APR','RPR','APR9')) then
				v_estado_indexer:=stEstadoDte.filtro_estado_indexer;
				v_estado :=replace(stEstadoDte.filtro_detalle,'##NOW##',''''||now()::varchar||'''');
			elsif (v_in_evento='PENDIENTE_INTER') then
				v_estado_indexer:=' estado in (''ERE'',''ERF'',''PRE'',''RER'') ';
				v_estado := ' estado_inter in (''ENVIADO_POR_INTERCAMBIO'',''ENTREGA_DE_DTE_POR_INTERCAMBIO_EXITOSA'') and ';
			--Si estan pendientes del SII
			elsif (v_in_evento='PENDIENTE_SII') then
				v_estado_indexer:=' estado in (''PPI'') ';
				v_estado := ' estado_sii in (''ENVIADO_AL_SII'') and ';
			elsif (v_in_evento='ACEPTADO_POR_EL_SII_TODOS') then
				v_estado_indexer:=' estado in (''AST'') ';
				v_estado := ' estado_sii in (''ACEPTADO_POR_EL_SII'',''ACEPTADO_CON_REPAROS_POR_EL_SII'') and ';
			--Encolados (colas_motor_generica)
			elsif (v_in_evento='EN_PROCESO') then
				json2:=put_json(json2,'flag_encolados','SI');
				--v_estado:=' categoria in (''DTE_NORMAL'',''BOLETA'') and ';
				v_estado:=' 1=1 and ';
				v_estado_indexer:=' estado='||quote_literal(stEstadoDte.codigo)||' ';
				v_parametro_rut_emisor:=v_parametro_rut_emisor_com;
				v_tipo_dte:=v_tipo_dte_com;
				v_in_rut_receptor:=v_in_rut_receptor_com;
				aux:=stEstadoDte.tabla_detalle;
				aux:=replace(aux,chr(36)||chr(36)||'RUT_EMISOR'||chr(36)||chr(36),v_parametro_rut_emisor);
				aux:=replace(aux,chr(36)||chr(36)||'TIPO_DTE'||chr(36)||chr(36),v_tipo_dte);
				aux:=replace(aux,chr(36)||chr(36)||'RUT_RECEPTOR'||chr(36)||chr(36),v_in_rut_receptor);
				--aux:=replace(aux,chr(36)||chr(36)||'FOLIO'||chr(36)||chr(36),'');
				aux:=replace(aux,chr(36)||chr(36)||'FOLIO'||chr(36)||chr(36),' and categoria in (''DTE_NORMAL'',''BOLETA'',''WINDTE'',''DTE'') ');
				stEstadoDte.tabla_detalle:=aux;
				flag_cuenta_estado:=true;
			else
				v_estado_indexer:=' estado='||quote_literal(stEstadoDte.codigo)||' ';
				if (lista_cesion1='SI') then
					v_estado:=' 1=1 and ';
				elsif(stEstadoDte.flag_lista_errores='SI') then
					v_estado:=' estado='||quote_literal(v_in_evento)||' and ';
				elsif(stEstadoDte.update_dte_emitidos='INTER')then
					v_estado:='estado_inter='||quote_literal(v_in_evento)||' and ';
				else
					if(get_json('TIPO_DTE',json2)<>'801') then
						v_estado:='estado_sii='||quote_literal(v_in_evento)||' and ';
					else
                                                v_estado:='estado='||quote_literal(stEstadoDte.descripcion)||' and ';
                                                flag_cuenta_estado:=true;
                                        end if;
                                end if;
                        end if;
                        tabla_defecto1:=' '||stEstadoDte.tabla_detalle||' ';
               else
			-- NBV 201705
			if(get_json('TIPO_DTE',json2)<>'801') then
				tabla_defecto1:=' dte_emitidos ';
			else
				flag_cuenta_estado:=true;
				tabla_defecto1:=' de_emitidos ';
			end if;
			-- NBV 201705
			--tabla_defecto1:=' dte_emitidos ';
		end if;
	else
		-- NBV 201705
		if(get_json('TIPO_DTE',json2)<>'801') then
			tabla_defecto1:=' dte_emitidos ';
		else
			flag_cuenta_estado:=true;
			tabla_defecto1:=' de_emitidos ';
		end if;
		-- NBV 201705
		--tabla_defecto1:=' dte_emitidos ';
        end if;
        json2:=logjsonfunc(json2,'ESTADOS--v_estado='||v_estado||' v_estado_indexer='||v_estado_indexer);
	json2:=put_json(json2,'flag_cuenta_estado',flag_cuenta_estado::varchar);
        --/ESTADOS--
	
	json2:=put_json(json2,'flag_boleta',flag_boleta1);

        --FAY,DAO si viene PARAMETRO5, solo busca en busqueda masiva y hace join con dte_emitidos
        id_masivo1:=get_json('parametro5',json2);
        tabla_dinamica1:=' ##TABLA## ';
        tabla_dinamica1:=replace(tabla_dinamica1,'##TABLA##',tabla_defecto1);
        json2:=logjsonfunc(json2,'Tabla Dinamica '||tabla_dinamica1);

        json5:='{}';
        json5:=put_json(json5,'ESTADO_ACEPTA','EMITIDO');
        json5:=put_json(json5,'IMPUESTOS','');
        json5:=put_json(json5,'REFERENCIAS','');
        json5:=put_json(json5,'ESTADO_CESION','');
        json5:=put_json(json5,'ALIAS','');
        json5:=put_json(json5,'TABLA',tabla_dinamica1);
        json2:=put_json(json2,'TABLA',tabla_dinamica1);

        json5:=put_json(json5,'TIPO_DTE',v_tipo_dte);
        json5:=put_json(json5,'RUT_EMISOR',v_parametro_rut_emisor);
        json5:=put_json(json5,'RUT_RECEPTOR',v_in_rut_receptor);
	--DAO 20180228 Se cambia el get campos para que siempre retorne un json
	json2:=get_campos_generica_6000_3(json2,flag_rut_emisor1,v_nombres_parametros_var,flag_boleta1);
	select_vars1:=get_json('__campos_busqueda__',json2);
	if (get_json('rutUsuario',json2)='17597643') then
		perform logfile('QUERY '||replace(select_vars1,chr(10),''));
	end if;
        json5:=put_json(json5,'CAMPOS',select_vars1);
        json5:=put_json(json5,'FILTRO_REF','');
        json5:=put_json(json5,'FILTRO_ADIC','');
	
	--DAO 20180228
	--Ordernamos siempre por codigo_txel (parametro 1) a menos que venga un order como parametro
	order1:=' order by codigo_txel desc ';
	if get_json('__ORDERBY__',json2)<>'' then
		order1:=' order by '||get_json('__ORDERBY__',json2);
		perform logfile('CAMBIA ORDEN '||order1);
	end if; 
	--DAO 20180228 Se agregan al filtro adicional alguna condicion adicional que se haya generado en forma particular cuando se definio el set de datos a mostrar
	if get_json('__FILTRO_ADD__',json2)<>'' then
		json2:=logjson(json2,'__FILTRO_ADD__='||get_json('__FILTRO_ADD__',json2));
		v_parametro_adicional1:=v_parametro_adicional1||' '||get_json('__FILTRO_ADD__',json2); 
	end if;

        query_data1:='SELECT array_to_json(array_agg(row_to_json(sql))) FROM ( '||chr(36)||chr(36)||chr(36)||'CAMPOS'||chr(36)||chr(36)||chr(36)||' from (select *,tipo_dte as tipo_dte_cesion,'''||chr(36)||chr(36)||chr(36)||'ESTADO_ACEPTA'||chr(36)||chr(36)||chr(36)||'''::varchar as estado_acepta '||chr(36)||chr(36)||chr(36)||'IMPUESTOS'||chr(36)||chr(36)||chr(36)||' '||chr(36)||chr(36)||chr(36)||'REFERENCIAS'||chr(36)||chr(36)||chr(36)||' '||chr(36)||chr(36)||chr(36)||'ESTADO_CESION'||chr(36)||chr(36)||chr(36)||' '||chr(36)||chr(36)||chr(36)||'ALIAS'||chr(36)||chr(36)||chr(36)||' from '||chr(36)||chr(36)||chr(36)||'TABLA'||chr(36)||chr(36)||chr(36)||' where '||chr(36)||chr(36)||chr(36)||'CODIGO_TXEL'||chr(36)||chr(36)||chr(36)||' '||order1||' ) x left join contribuyentes m ON m.rut_emisor=x.rut_receptor LEFT JOIN tipo_dte t ON t.codigo = x.tipo_dte LEFT JOIN (select * from dte_pagado where canal=''EMITIDOS'') dp on dp.codigo_txel=x.codigo_txel) sql';
	json2:=put_json(json2,'PATRON_QUERY',encode(query_data1::bytea,'hex'));
	json2:=put_json(json2,'JSON5_QUERY',encode_hex(json5::varchar));
        json2:=put_json(json2,'FILTRO_REF',v_parametro_referencias1);
        json2:=put_json(json2,'FILTRO_ADIC',v_parametro_adicional1);

        --CUENTA--
        --Saco el TOTAL
        --Si viene rut_receptor debo contar desde la tabla
        if get_json('TIPO_DTE',json2)='801' then
        	query1:='SELECT count(*) as total from '||tabla_dinamica1||' where '||v_estado||v_parametro_rut_emisor||'  and tipo_dte in '||v_tipo_dte||' and dia'||tipo_dia1||' in '||fecha_in1||' '||v_in_rut_receptor||' '||v_parametro_tipo_dte||' '||v_parametro_var||v_parametro_adicional1;
	else
        	query1:='SELECT count(*) as total from '||tabla_dinamica1||' where '||v_estado||v_parametro_rut_emisor||'  and tipo_dte in '||v_tipo_dte||' and dia'||tipo_dia1||' in '||fecha_in1||' '||v_in_rut_receptor||' '||v_parametro_tipo_dte||' '||v_parametro_var;
	end if;
        --Solo para encolados y documentos cedidos
        if (flag_cuenta_estado) then
        	query_data1:='SELECT array_to_json(array_agg(row_to_json(sql))) FROM ( '||chr(36)||chr(36)||chr(36)||'CAMPOS'||chr(36)||chr(36)||chr(36)||' from (select *,tipo_dte as tipo_dte_cesion,'''||chr(36)||chr(36)||chr(36)||'ESTADO_ACEPTA'||chr(36)||chr(36)||chr(36)||'''::varchar as estado_acepta '||chr(36)||chr(36)||chr(36)||'IMPUESTOS'||chr(36)||chr(36)||chr(36)||' '||chr(36)||chr(36)||chr(36)||'REFERENCIAS'||chr(36)||chr(36)||chr(36)||' '||chr(36)||chr(36)||chr(36)||'ESTADO_CESION'||chr(36)||chr(36)||chr(36)||' '||chr(36)||chr(36)||chr(36)||'ALIAS'||chr(36)||chr(36)||chr(36)||' from '||chr(36)||chr(36)||chr(36)||'TABLA'||chr(36)||chr(36)||chr(36)||' where '||v_estado||''||chr(36)||chr(36)||chr(36)||'RUT_EMISOR'||chr(36)||chr(36)||chr(36)||' and tipo_dte in '||chr(36)||chr(36)||chr(36)||'TIPO_DTE'||chr(36)||chr(36)||chr(36)||' and dia'||tipo_dia1||' in '||fecha_in1||' '||chr(36)||chr(36)||chr(36)||'RUT_RECEPTOR'||chr(36)||chr(36)||chr(36)||' '||v_parametro_tipo_dte||' '||v_parametro_var||' '||chr(36)||chr(36)||chr(36)||'FILTRO_REF'||chr(36)||chr(36)||chr(36)||' '||chr(36)||chr(36)||chr(36)||'FILTRO_ADIC'||chr(36)||chr(36)||chr(36)||' '||order_excel1||' offset '||v_in_offset1::varchar||' limit '||v_in_cant_reg::varchar||') x left join contribuyentes m ON m.rut_emisor=x.rut_receptor LEFT JOIN tipo_dte t ON t.codigo = x.tipo_dte LEFT JOIN (select * from dte_pagado where canal=''EMITIDOS'') dp on dp.codigo_txel=x.codigo_txel ) sql';
                json2:=logjsonfunc(json2,'Encolados y Cedidos');
                --if (total_pag1='-1') then
                        if (vin_estado='EN_PROCESO') then
				if get_json('rutUsuario',json2)='17597643' then
					perform logfile('EN_PROCESO '||query1);
				end if;
                                json_par1:=get_parametros_motor_json('{}','BASE_COLAS_CH-ADX-P-Colas-motor13');
                                json3:=query_db_json(get_json('__IP_CONEXION_CLIENTE__',json_par1),get_json_int('__IP_PORT_CLIENTE__',json_par1)::integer,query1);
                                if (get_json('STATUS',json3)<>'OK') then
                                        json2:=logjsonfunc(json2,'Falla Obtener Datos En Proceso');
                                        json2:=response_requests_6000('2', 'Falla Obtener Datos En Proceso','',json2);
                                        return json2;
                                end if;
                                v_total1:=get_json('total',json3);
				total_base_colas1:=get_json('total',json3);
                                json_par1:=get_parametros_motor_json('{}','BASE_COLAS_motor14');
                                json3:=query_db_json(get_json('__IP_CONEXION_CLIENTE__',json_par1),get_json_int('__IP_PORT_CLIENTE__',json_par1)::integer,query1);
                                if (get_json('STATUS',json3)<>'OK') then
                                        json2:=logjsonfunc(json2,'Falla Obtener Datos En Proceso');
                                        json2:=response_requests_6000('2', 'Falla Obtener Datos En Proceso','',json2);
                                        return json2;
                                end if;
                                v_total1:=v_total1+get_json('total',json3)::integer;
				total_base_colas2:=get_json('total',json3);
                        else
				--if (get_json('rutUsuario',json2)='17597643') then
					json2:=logjsonfunc(json2,'QUERY_COUNT '||replace(query1,chr(10),''));
				--end if;
                                EXECUTE query1 into v_total1;
                        end if;
			json2:=put_json(json2,'v_total_registros',v_total1::varchar);
                        json2:=logjsonfunc(json2,'Total Contados ='||v_total1::varchar);
                --else
                --        json2:=logjsonfunc(json2,'No cuenta Total pagina='||total_pag1::varchar);
                --        v_total:=total_pag1;
                --end if;
		if (vin_estado='EN_PROCESO') then
			json2:=logjsonfunc(json2,'Busca en Colas EN_PROCESO');
			query1:=remplaza_tags_json_c(json5,query_data1);
			json5:=put_json(json5,'TIPO_DTE',v_tipo_dte_com);
			json5:=put_json(json5,'RUT_EMISOR',v_parametro_rut_emisor_com);
			--json5:=put_json(json5,'RUT_RECEPTOR',v_in_rut_receptor_com);
			json5:=put_json(json5,'RUT_RECEPTOR',v_in_rut_receptor_com||' and categoria in (''DTE_NORMAL'',''BOLETA'',''WINDTE'',''DTE'') ');
			json2:=put_json(json2,'flag_encolados','SI');
			pag_base_colas1:=total_base_colas1/v_in_cant_reg_fijo::integer;
			sobra_base_colas1:=total_base_colas1%v_in_cant_reg_fijo::integer;	
			--DAO 20200930
			--if get_json('rutUsuario',json2)='17597643' then
				--perform logfile('EN_PROCESO pag_base_colas1='||pag_base_colas1::varchar||' sobra_base_colas1='||sobra_base_colas1||' '||' v_in_offset1='||v_in_offset1::varchar);	
				--perform logfile('EN_PROCESO '||replace(query1,chr(10),' '));
				if (v_in_offset1::integer+v_in_cant_reg_fijo::integer<=pag_base_colas1*v_in_cant_reg_fijo::integer) then
					--perform logfile('EN_PROCESO solo base moto13 ');
                                	json_par1:=get_parametros_motor_json('{}','BASE_COLAS_CH-ADX-P-Colas-motor13');
					json4:=query_db_json(get_json('__IP_CONEXION_CLIENTE__',json_par1),get_json_int('__IP_PORT_CLIENTE__',json_par1)::integer,query1);
				elsif sobra_base_colas1>0 and v_in_offset1::integer=pag_base_colas1*v_in_cant_reg_fijo::integer then
					--perform logfile('EN_PROCESO base moto13 off='||v_in_offset1::varchar||' limit '||sobra_base_colas1::varchar);
                                	json_par1:=get_parametros_motor_json('{}','BASE_COLAS_CH-ADX-P-Colas-motor13');
					jaux1:=query_db_json(get_json('__IP_CONEXION_CLIENTE__',json_par1),get_json_int('__IP_PORT_CLIENTE__',json_par1)::integer,split_part(query1,'offset ',1)||'offset '||v_in_offset1::varchar||' limit '||sobra_base_colas1::varchar||' ) x left '||split_part(query1,') x left',2));
					if get_json('STATUS',jaux1)<>'OK' then
						json2:=put_json(json2,'MENSAJE_RESPUESTA','Falla Conexión Base Datos1.-.');
						json2:=put_json(json2,'CODIGO_RESPUESTA','2');
						json2:=put_json(json2,'__SECUENCIAOK__','0');
						json2:=responde_pantalla_15100(json2);
						return json2;
					end if;
					--perform logfile('EN_PROCESO base moto14 off=0 limit '||(v_in_cant_reg_fijo::integer-sobra_base_colas1)::varchar);
					json_par1:=get_parametros_motor_json('{}','BASE_COLAS_motor14');
					jaux2:=query_db_json(get_json('__IP_CONEXION_CLIENTE__',json_par1),get_json_int('__IP_PORT_CLIENTE__',json_par1)::integer,split_part(query1,'offset ',1)||'offset 0 limit '||(v_in_cant_reg_fijo::integer-sobra_base_colas1)::varchar||' ) x left '||split_part(query1,') x left',2));
					if get_json('STATUS',jaux2)<>'OK' then
						json2:=put_json(json2,'MENSAJE_RESPUESTA','Falla Conexión Base Datos1.-.');
						json2:=put_json(json2,'CODIGO_RESPUESTA','2');
						json2:=put_json(json2,'__SECUENCIAOK__','0');
						json2:=responde_pantalla_15100(json2);
						return json2;
					end if;
					json4:=put_json(put_json('{}','array_to_json',json_merge_lists(get_json('array_to_json',jaux1)::varchar,get_json('array_to_json',jaux2)::varchar)::varchar),'STATUS','OK');
				else
					--perform logfile('EN_PROCESO solo base moto14 off='||(v_in_offset1::integer-pag_base_colas1*v_in_cant_reg_fijo::integer-sobra_base_colas1)::varchar||' limit '||v_in_cant_reg_fijo::varchar);
					json_par1:=get_parametros_motor_json('{}','BASE_COLAS_motor14');
					json4:=query_db_json(get_json('__IP_CONEXION_CLIENTE__',json_par1),get_json_int('__IP_PORT_CLIENTE__',json_par1)::integer,split_part(query1,'offset ',1)||'offset '||(v_in_offset1::integer-pag_base_colas1*v_in_cant_reg_fijo::integer-sobra_base_colas1)::varchar||' limit '||v_in_cant_reg_fijo::varchar||' ) x left '||split_part(query1,') x left',2));
				end if;
			/*else
				json4:=merge_lista_query_bd_colas(query1,'array_to_json');
			end if;*/
			/*
			json_par1:=get_parametros_motor_json('{}','BASE_COLAS');
			json4:=query_db_json(get_json('__IP_CONEXION_CLIENTE__',json_par1),get_json_int('__IP_PORT_CLIENTE__',json_par1)::integer,query1);
			json2:=logjsonfunc(json2,'json4='||json4::varchar);*/
			if (get_json('STATUS',json4)='OK' and get_json('array_to_json',json4)<>'') then
				v_out_resultado:=get_json('array_to_json',json4);
				json2:=put_json(json2,'v_out_resultado',v_out_resultado::varchar);
				json2:=put_json(json2,'CODIGO_RESPUESTA','1');
				json2:=put_json(json2,'__SECUENCIAOK__','60');
				RETURN json2;
			elsif (get_json('STATUS',json4)='SIN_DATA' or (get_json('STATUS',json4)='OK' and get_json('array_to_json',json4)='')) then
				json2:=put_json(json2,'MENSAJE_RESPUESTA','No se encontraron registros.-.');
				json2:=put_json(json2,'CODIGO_RESPUESTA','2');
				json2:=put_json(json2,'__SECUENCIAOK__','0');
				json2:=responde_pantalla_15100(json2);
				return json2;
			else
				json2:=put_json(json2,'MENSAJE_RESPUESTA','Falla Conexión Base Datos.-.');
				json2:=put_json(json2,'CODIGO_RESPUESTA','2');
				json2:=put_json(json2,'__SECUENCIAOK__','0');
				json2:=responde_pantalla_15100(json2);
				return json2;
			end if;
		--Solo para encolados y documentos cedidos
		else
			--20190220 mvillanueva
                        if get_json('TIPO_DTE',json2)='801' then
                                json5:=put_json(json5,'FILTRO_ADIC',v_parametro_adicional1);
                        end if;
			json2:=logjsonfunc(json2,'Busca en Colas Dte Cedidos');
			query1:=remplaza_tags_json_c(json5,query_data1);
			--if (get_json('rutUsuario',json2)='17597643') then
				json2:=logjsonfunc(json2,'query1='||replace(query1,chr(10),''));
			--end if;
			execute query1 into v_out_resultado;
			json2:=put_json(json2,'v_out_resultado',v_out_resultado::varchar);
			json2:=put_json(json2,'CODIGO_RESPUESTA','1');
			json2:=put_json(json2,'__SECUENCIAOK__','60');
			RETURN json2;
		end if;	
		json2:=put_json(json2,'__SECUENCIAOK__','60');
		json2:=put_json(json2,'RESULTADO_FINAL',v_out_resultado);
		return json2;
        --GENERICO--
        else
                --Saca el total de las estadisticas del indexer
                --BEGIN
                --Para los Pendiente_Intercambio tiene otra logica
                --Si la fecha_fin es menor a 20140101 las estadisticas estan en respaldo_cuadratura
                json2:=logjsonfunc(json2,'EMITIDOS'||v_in_fecha_fin||vin_estado);
		json2:=put_json(json2,'flag_errores',v_lista_errores);
		json2:=put_json(json2,'id_masivo',id_masivo1::varchar);
		
                if (vin_estado='IMPORTADO') then
                        json2:=logjsonfunc(json2,'Cuenta en cuenta_offset_indexer_estadisticas');
                        flag_codigo_txel:='SI';
			json2:=put_json(json2,'flag_importado','SI');
                        json2:=genera_querys_cuenta_codigo_txel_15100(json2,v_in_offset1,v_in_cant_reg_fijo,v_parametro_rut_emisor,v_tipo_dte,v_parametro_tipo_dte,v_parametro_var,fecha_in1,v_estado,tipo_dia_ind1,v_in_rut_receptor,v_parametro_referencias1,v_parametro_adicional1,get_json('__hash__',json2),v_in_fecha_inicio::varchar);

                else
                        flag_codigo_txel:='SI';
                        json2:=logjsonfunc(json2,'Cuenta en cuenta_offset_redshift_emitidos');
			--Cuenta local y genera query para contar en el RS que corresponda
                        json2:=genera_querys_cuenta_codigo_txel_15100(json2,v_in_offset1,v_in_cant_reg_fijo,v_parametro_rut_emisor,v_tipo_dte,v_parametro_tipo_dte,v_parametro_var,fecha_in1,v_estado,tipo_dia_ind1,v_in_rut_receptor,v_parametro_referencias1,v_parametro_adicional1,get_json('__hash__',json2),v_in_fecha_inicio::varchar);
                end if;

		--json2:=put_json(json2,'BASE_RS',get_json('BASE_RS',json3));
		json2:=logjsonfunc(json2,'QUERY_RS='||get_json('QUERY_RS',json2));
		json2:=logjsonfunc(json2,'TABLAS_HOY='||get_json('TABLAS_HOY',json2));
		json2:=logjsonfunc(json2,'flag_contar='||get_json('flag_contar',json2));

		json2:=put_json(json2,'CAT_EST','');
		if (get_json('QUERY_RS',json2)='') then
			json2:=put_json(json2,'__SECUENCIAOK__','50');
		elsif (get_json('BASE_RS',json2)='BASE_REDSHIFT_BOLETAS') then
			json2:=put_json(json2,'CAT_EST','CUENTA_BOLETAS');
			json2:=put_json(json2,'__TOTAL_RESP_ESPERADAS__','2');
			json2:=put_json(json2,'__SECUENCIAOK__','20');
		elsif (get_json('BASE_RS',json2)='BASE_REDSHIFT_RECIBIDOS') then
			json2:=put_json(json2,'CAT_EST','CUENTA_RECIBIDOS');
			json2:=put_json(json2,'__TOTAL_RESP_ESPERADAS__','2');
			json2:=put_json(json2,'__SECUENCIAOK__','30');
		elsif (get_json('BASE_RS',json2)='BASE_REDSHIFT_EMITIDOS') then
			json2:=put_json(json2,'CAT_EST','CUENTA_EMITIDOS');
			json2:=put_json(json2,'__TOTAL_RESP_ESPERADAS__','2');
			json2:=put_json(json2,'__SECUENCIAOK__','40');
		else
			json2:=logjsonfunc(json2,'ERROR: Viene QUERY_RS pero no viene BASE_RS Reconocida');
			json2:=put_json(json2,'__SECUENCIAOK__','50');
		end if;
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

CREATE OR REPLACE FUNCTION resultado_cuenta_pivote_query_15100(json)
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
	flag_agrega_amazon2016	boolean;
	flag_agrega_amazon2014	boolean;
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
			json2:=responde_pantalla_15100(json2);
			return json2;
		end if;
		json3:=get_json('RES_JSON_2',json2)::json;
		if (get_json('STATUS',json3)='SIN_DATA') then
			json3:=put_json('{}','STATUS','NK');
		--Si se cancelo el query contestamos falla
		elsif (get_json('STATUS',json3)='FALLA_TIMEOUT_QUERY_CANCELADO') then
	                perform graba_estadisticas_busqueda(json2,get_json('CAT_EST',json2),'NK');
                        json2:=logjsonfunc(json2,'Falla Timeout Query Count en RS');
                        json2:=put_json(json2,'MENSAJE_RESPUESTA','Reintente Por favor.');
                        json2:=put_json(json2,'CODIGO_RESPUESTA','2');
                        json2:=put_json(json2,'__SECUENCIAOK__','0');
                        json2:=responde_pantalla_15100(json2);
                        return json2;
		else
			json3:=put_json('{}','STATUS','OK');
		end if;
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
			json2:=responde_pantalla_15100(json2);
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
		json2:=responde_pantalla_15100(json2);
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
	query2:=decode(get_json('PATRON_QUERY',json2),'hex');
	json5:=decode_hex(get_json('JSON5_QUERY',json2))::varchar::json;
	json5:=put_json(json5,'CODIGO_TXEL',' codigo_txel in '|| get_json('__aux_filtro_codigos',json2));
        if (get_json('v_lista_errores',json2)='SI') then
                json2:=logjsonfunc(json2,'Busca en Nominas');
                query1:=remplaza_tags_json_c(json5,query2);
                execute query1 into v_out_resultado;
		json2:=put_json(json2,'v_out_resultado',v_out_resultado::varchar);
		json2:=put_json(json2,'CODIGO_RESPUESTA','1');
		json2:=put_json(json2,'__SECUENCIAOK__','60');
	        RETURN json2;
	end if;
	vin_estado:=get_json('ESTADO',json2);

	json2:=logjsonfunc(json2,'Busca Generico');
	v_parametro_referencias1:=get_json('FILTRO_REF',json2);
	v_parametro_adicional1:=get_json('FILTRO_ADIC',json2);
        json5:=put_json(json5,'FILTRO_REF',v_parametro_referencias1);
        json5:=put_json(json5,'FILTRO_ADIC',v_parametro_adicional1);
	--json2:=logjsonfunc(json2,'query2='||query2);
        query1:=remplaza_tags_json_c(json5,query2);

	--Inserto valores en el json para hacer el loop con las tablas
	json2:=put_json(json2,'QUERY2',encode_hex(query2::varchar));
	json2:=put_json(json2,'JSON5',encode_hex(json5::varchar));
	if (vin_estado='IMPORTADO') then
		if (get_json('flag_boleta',json2)='SI') then
			json2:=put_json(json2,'PREFIJO_TABLA','dte_boletas_importer_');
			json2:=put_json(json2,'TABLA_CONFIGURACION','config_tabla_boletas_importados');
		else
			json2:=put_json(json2,'PREFIJO_TABLA','dte_emitidos_importados_');
			json2:=put_json(json2,'TABLA_CONFIGURACION','config_tabla_emitidos_importados');
		end if;
	else
		if (get_json('flag_boleta',json2)='SI') then
			json2:=put_json(json2,'PREFIJO_TABLA','dte_boletas_');
			json2:=put_json(json2,'TABLA_CONFIGURACION','config_tabla_boletas');
		else
			json2:=put_json(json2,'PREFIJO_TABLA','dte_emitidos_');
                        json2:=put_json(json2,'TABLA_CONFIGURACION','config_tabla_emitidos');
		end if;
	end if;

	--Antes de empezar la recursion, verificamos si tiene tabla de boletas especifica
	if (strpos(get_json('PREFIJO_TABLA',json2),'dte_boletas')>0) then
		i:=0;
		j1:=get_json('__aux_tablas',json2);

		total_rut_emisores:=count_array_json(get_json('lista_rut_emisores',json2)::json);
		aux:=get_json_index(j1,i);
		lista_new:='[]';
		flag_agrega_tabla:=false;
		--Flag para ejecutar solo 1 vez
		flagx:=true;
		flag_agrega_amazon2014=false;
		flag_agrega_amazon2016=false;
		json2:=logjsonfunc(json2,'XXXXX '||aux);
		while (aux<>'') loop	
			if is_number(aux) then
				if aux::integer<1601 then 
					flag_agrega_amazon2014=true;
				end if;
				if aux::integer>=1601 and aux::integer<1609 then 
					flag_agrega_amazon2016=true;
				end if;
				if aux::integer<1510 then 
					lista_new:=put_json_list(lista_new,aux);
					i:=i+1;
					aux:=get_json_index(j1,i);
					continue;
				end if;
			end if;
			json2:=logjsonfunc(json2,'XXXXX Entre al LOOP '||get_json('v_parametro_rut_emisor',json2));
			 for campo in execute 'select rut_boleta from rut_tabla_boletas where strpos('''||get_json('v_parametro_rut_emisor',json2)||''',rut_boleta::varchar)>0' loop
				json2:=logjsonfunc(json2,'XXXXX Paso1 '||campo.rut_boleta::varchar);
				lista_new:=put_json_list(lista_new,('"'||aux||'_'||campo.rut_boleta::varchar||'"')::varchar);
				json2:=logjsonfunc(json2,'XXXXX Paso2 '||lista_new::varchar);
			end loop;

			--Si hay que agregar la tabla, lo hacemos
			if flag_agrega_tabla then
				lista_new:=put_json_list(lista_new,aux);
			--Si los ingresado en un periodo son igual a total de ruts, no se ingresa la tabla generica
			elsif flagx and total_rut_emisores<>count_array_json(lista_new) then
				flag_agrega_tabla:=true;
				lista_new:=put_json_list(lista_new,aux);
			end if;
			flagx:=false;
			--if (flagx is false) then
			--end if;
			i:=i+1;
			aux:=get_json_index(j1,i);
		end loop;
		--lista_new:=put_json_list(lista_new,'no_borrar_fay');
		if flag_agrega_amazon2014 then
			if strpos(lista_new::varchar,'amazon2014_no_borrar_fay')=0 then
				lista_new:=put_json_list(lista_new,'amazon2014_no_borrar_fay');
			end if;
		end if;
		if flag_agrega_amazon2016 then
			if strpos(lista_new::varchar,'amazon2016_no_borrar_fay')=0 then
				lista_new:=put_json_list(lista_new,'amazon2016_no_borrar_fay');
			end if;
		end if;
		json2:=put_json(json2,'__aux_tablas',lista_new::varchar);
		json2:=logjsonfunc(json2,'XXXXX Lista con Rut Especificos de Boleta '||lista_new::varchar);
	end if;

	--Saco la primera tabla
	i:=0;
	j1:=get_json('__aux_tablas',json2);
	json2:=logjsonfunc(json2,'__aux_tablas '||j1::varchar);
	aux:=get_json_index(j1,i);
	/*
	if(is_number(get_json_index(j1,i)) is false) then
		json2:=logjsonfunc(json2,'Falla periodo de la tabla');
		json2:=put_json(json2,'MENSAJE_RESPUESTA','Reintente por favor (FP)');
		json2:=put_json(json2,'CODIGO_RESPUESTA','2');
		json2:=put_json(json2,'__SECUENCIAOK__','0');
		json2:=responde_pantalla_15100(json2);
		return json2;
	end if;
	*/
	
	fecha_c1:=get_json_index(j1,i);
	tabla1:=get_json('PREFIJO_TABLA',json2)||fecha_c1;
	if tabla1='dte_boletas_no_borrar_fay' then
		par1:=null;
		tabla1='dte_boletas_no_borrar_fay ';
	elsif tabla1='dte_boletas_amazon2014_no_borrar_fay' then
		par1:='BASE_AMAZON_BOLETAS_2014';
		tabla1='dte_boletas_amazon2014_no_borrar_fay';
	elsif tabla1='dte_boletas_amazon2016_no_borrar_fay' then
		par1:='BASE_AMAZON_BOLETAS_HISTORICAS';
		tabla1='dte_boletas_amazon2016_no_borrar_fay';
	else
		execute 'select parametro_motor from '||get_json('TABLA_CONFIGURACION',json2)||' where periodo_desde<='||split_part(fecha_c1,'_',1)||' and periodo_hasta>='||split_part(fecha_c1,'_',1) into par1;
	end if;
	json5:=put_json(json5,'TABLA',tabla1);	
	json2:=put_json(json2,'TABLA',tabla1);	
	query1:=remplaza_tags_json_c(json5,query2);
	json2:=put_json(json2,'QUERY_DATA',query1);
		
	json2:=put_json(json2,'PARAMETRO_TABLA',par1::varchar);
	json2:=put_json(json2,'CAT_EST',coalesce(par1::varchar,'LOCAL')||'__'||tabla1);
	if (par1='BASE_EMITIDOS_HISTORICOS') then
		json2:=put_json(json2,'__SECUENCIAOK__','72');
	elsif (par1='BASE_AMAZON_IMPORTADOS') then
		json2:=put_json(json2,'__SECUENCIAOK__','74');
	elsif (par1='BASE_AMAZON_BOLETAS_2014') then
		json2:=put_json(json2,'__SECUENCIAOK__','76');
	elsif (par1='BASE_AMAZON_BOLETAS_HISTORICAS') then
		json2:=put_json(json2,'__SECUENCIAOK__','77');
	else
		--if get_json('rutUsuario',json2)='17597643' then
		--DAO 20201001 para los reportes vamos a la base de replica
		if is_number(get_json('id_reporte',json2)) then
			json2:=put_json(json2,'__SECUENCIAOK__','71');
		else
			json2:=put_json(json2,'__SECUENCIAOK__','70');
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

CREATE OR REPLACE FUNCTION genera_querys_cuenta_codigo_txel_15100(json,integer,varchar,varchar,varchar,varchar,varchar,varchar,varchar,varchar,varchar,varchar,varchar,varchar,varchar)
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
	flag_nomina1    boolean;
        join1   varchar;
	order1 varchar;
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
                else
                        campo1:='codigo_txel';
                        campo2:='';
                end if;
                tabla_rs1:='dte_recibidos_actualizacion2';
        elsif (get_json('flag_recibidos',json1)='SI' and get_json('flag_importado',json1)='SI') then
                tabla_base1:='dte_recibidos_importados_generica';
                tabla_base2:='dte_recibidos_importados';
		json2:=put_json(json2,'BASE_RS','BASE_REDSHIFT_RECIBIDOS');
                campo1:='codigo_txel';
                campo2:=',mes as m ';
        elsif (get_json('flag_importado',json1)='SI' and (strpos(v_tipo_dte,'39')=0 and strpos(v_tipo_dte,'41')=0)) then
                tabla_base1:='dte_emitidos_importados_generica';
                tabla_base2:='dte_emitidos_importados';
		json2:=put_json(json2,'BASE_RS','BASE_REDSHIFT_EMITIDOS');
                campo1:='codigo_txel';
                campo2:=',mes as m ';
        elsif (get_json('flag_importado',json1)='SI' and (strpos(v_tipo_dte,'39')>0 or strpos(v_tipo_dte,'41')>0)) then
                tabla_base1:='dte_boletas_importadas_generica';
                tabla_base2:='dte_boletas_importadas';
		json2:=put_json(json2,'BASE_RS','BASE_REDSHIFT_BOLETAS');
                campo1:='codigo_txel';
                campo2:=',mes as m ';
        elsif (strpos(v_tipo_dte,'39')>0 or strpos(v_tipo_dte,'41')>0) then
                flag_saca_tablas_hoy:=true;
                tabla_base1:='dte_boletas_diarias';
                --tabla_base1:='dte_boletas_no_borrar_fay';
                select * into campo_tabla_rut from rut_tabla_boletas_rs where rut_boleta=get_json('rutCliente',json2)::integer;
                if found then
                        tabla_base2:='dte_boletas_'||get_json('rutCliente',json2);
                else
                        tabla_base2:='dte_boletas';
                end if;
		--RME 20200828 para buscar boletas con COD_SAP repetido 
		if (get_json('flag_errores',json1)='SI') then
                        tabla_base1:='dte_emitidos_errores';
                        tabla_base2:='dte_emitidos_errores';
                end if;
		json2:=put_json(json2,'BASE_RS','BASE_REDSHIFT_BOLETAS');
                campo1:='codigo_txel';
                --campo2:=',mes_emision as m ';
                campo2:=',mes as m ';
        elsif (get_json('flag_errores',json1)='SI') then
                tabla_base1:='dte_emitidos_errores';
                tabla_base2:='dte_emitidos_errores';
		json2:=put_json(json2,'BASE_RS','BASE_REDSHIFT_RECIBIDOS');
                campo1:='codigo_txel';
                campo2:='';
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
			if (get_json('rutUsuario',json2)='17597643') then
				perform logfile('QUERY_'||query1);
			end if;
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
		else
			if flag_nomina1 then
				query1:='select count(*) from ((select * from '||tabla_base2||' where '||v_estado||v_parametro_rut_emisor||filtro_tipo_dte1||filtro_dia1||' '||v_in_rut_receptor||' '||v_parametro_tipo_dte||' '||v_parametro_var||v_parametro_referencias||v_parametro_adicional||join1;
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

	--DAO 20180228
	--Ordernamos siempre por codigo_txel (parametro 1) a menos que venga un order como parametro
	order1:=' order by 1 desc ';
	if get_json('__ORDERBY__',json2)<>'' then
		order1:=' order by '||get_json('__ORDERBY__',json2);
		perform logfile('CAMBIA ORDEN '||order1);
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
			(select '||campo1||' from '||tabla_rs1||' where '||v_estado||v_parametro_rut_emisor||' and tipo_dte in '||v_tipo_dte||' '||filtro_dia1||' '||v_in_rut_receptor||' '||v_parametro_tipo_dte||' '||v_parametro_var||v_parametro_referencias||v_parametro_adicional|| ' and codigo_txel is not null) '||order1||' offset '||v_in_offset1||' limit '||v_in_cant_reg||'
			) sql';
			if (get_json('rutUsuario',json2)='7621836') then
				perform logfile('QUERY_LOCAL '||query2);
			end if;	
			execute query2 into json4;
		else
			if(flag_saca_tablas_hoy) then
				query2:='select string_agg('||campo1||'::varchar,'','') as c,''"''||string_agg(distinct mes_emision::varchar,''","'')||''"'' as m from (select '||campo1||',substring(mes_emision::varchar,3) as mes_emision from '||tabla_base1||' where dia='||dia1||' and '||v_estado||v_parametro_rut_emisor||' and tipo_dte in '||v_tipo_dte||' '||filtro_dia1||' '||v_in_rut_receptor||' '||v_parametro_tipo_dte||' '||v_parametro_var||v_parametro_referencias||v_parametro_adicional|| ' and codigo_txel is not null '||order1||' offset '||v_in_offset1||' limit '||v_in_cant_reg||') sql';
				if(get_json('rutUsu',json2)='7621836') then
					perform logfile('DAO_BOL Paso1');
				end if;
				execute query2 into campo_hoy1;
				if (get_json('rutUsuario',json2)='7621836') then
					perform logfile('QUERY_LOCAL '||query2);
				end if;	
				json4:=campo_hoy1.c;
				tablas_hoy1:=campo_hoy1.m;
				json2:=put_json(json2,'TABLAS_HOY',tablas_hoy1);
				
			else
				query2:='select string_agg('||campo1||'::varchar,'','') from (select '||campo1||' from '||tabla_base1||' where dia='||dia1||' and '||v_estado||v_parametro_rut_emisor||' and tipo_dte in '||v_tipo_dte||' '||filtro_dia1||' '||v_in_rut_receptor||' '||v_parametro_tipo_dte||' '||v_parametro_var||v_parametro_referencias||v_parametro_adicional|| ' and codigo_txel is not null '||order1||' offset '||v_in_offset1||' limit '||v_in_cant_reg||') sql';
				execute query2 into json4;
				if (get_json('rutUsuario',json2)='7621836') then
					perform logfile('QUERY_LOCAL '||query2);
				end if;	
			end if;
		end if;
		query1:=query1||';select -1 as c';
	else
		if (get_json('rutUsuario',json2)='7621836') then
			perform logfile('Paso2');
		end if;	
		--Se sacan los id de la base1 siempre y cuando sea la pagina intermedia
		if (sobra_base1>0 and of1=(paginas_base1)*limit1) then
			--Hacemos un union entre la base local y los actualizados para sacar el limit y offset
			if(get_json('flag_act',json2)='SI' and filtro_rs1<>'') then
				query2:='select string_agg('||campo1||'::varchar,'','') from ((select '||campo1||' from '||tabla_base1||' where dia='||dia1||' and '||v_estado||v_parametro_rut_emisor||' and tipo_dte in '||v_tipo_dte||' '||filtro_dia1||' '||v_in_rut_receptor||' '||v_parametro_tipo_dte||' '||v_parametro_var||v_parametro_referencias||v_parametro_adicional|| ' and codigo_txel is not null) union (select '||campo1||' from '||tabla_rs1||' where '||v_estado||v_parametro_rut_emisor||' and tipo_dte in '||v_tipo_dte||' '||filtro_dia1||' '||v_in_rut_receptor||' '||v_parametro_tipo_dte||' '||v_parametro_var||v_parametro_referencias||v_parametro_adicional|| ' and codigo_txel is not null) '||order1||' offset '||v_in_offset1||' limit '||sobra_base1||' ) sql';
				if (get_json('rutUsuario',json2)='7621836') then 
					perform logfile(' QUERY2 '||query2);
				end if;
				execute query2 into json4;
			else
				if(flag_saca_tablas_hoy) then
					query2:='select string_agg('||campo1||'::varchar,'','') as c,''"''||string_agg(distinct mes_emision::varchar,''","'')||''"'' as m from (select '||campo1||',substring(mes_emision::varchar,3) as mes_emision from '||tabla_base1||' where dia='||dia1||' and '||v_estado||v_parametro_rut_emisor||' and tipo_dte in '||v_tipo_dte||' '||filtro_dia1||' '||v_in_rut_receptor||' '||v_parametro_tipo_dte||' '||v_parametro_var||v_parametro_referencias||v_parametro_adicional|| ' and codigo_txel is not null '||order1||' offset '||v_in_offset1||' limit '||sobra_base1||') sql';
					if (get_json('rutUsuario',json2)='7621836') then 
						perform logfile(' QUERY2.1 '||query2);
					end if;
					execute query2 into campo_hoy1;
					json4:=campo_hoy1.c;
					tablas_hoy1:=campo_hoy1.m;
					json2:=put_json(json2,'TABLAS_HOY',tablas_hoy1);
				else
					query2:='select string_agg('||campo1||'::varchar,'','') from (select '||campo1||' from '||tabla_base1||' where dia='||dia1||' and '||v_estado||v_parametro_rut_emisor||' and tipo_dte in '||v_tipo_dte||' '||filtro_dia1||' '||v_in_rut_receptor||' '||v_parametro_tipo_dte||' '||v_parametro_var||v_parametro_referencias||v_parametro_adicional|| ' and codigo_txel is not null '||order1||' offset '||v_in_offset1||' limit '||sobra_base1||') sql';
					if (get_json('rutUsuario',json2)='7621836') then 
						perform logfile(' QUERY2.2 '||query2);
					end if;
					execute query2 into json4;
				end if;
			end if;
			limit1:=limit1-sobra_base1;
			of1:=0;
			if flag_nomina1 then
				query1:=query1||';select '||campo1||' as c'||campo2||' from ((select * from '||tabla_base2||' where '||v_estado||v_parametro_rut_emisor||filtro_tipo_dte1||filtro_dia1||' '||v_in_rut_receptor||' '||v_parametro_tipo_dte||' '||v_parametro_var||v_parametro_referencias||v_parametro_adicional||join1||' '||order1||' offset '||of1::varchar||' limit '||limit1::varchar;
			else
				query1:=query1||';select '||campo1||' as c'||campo2||' from '||tabla_base2||' where '||v_estado||v_parametro_rut_emisor||filtro_tipo_dte1||filtro_dia1||' '||v_in_rut_receptor||' '||v_parametro_tipo_dte||' '||v_parametro_var||v_parametro_referencias||v_parametro_adicional|| ' '||order1||' offset '||of1::varchar||' limit '||limit1::varchar;
			end if;
		else
			--Se saca los id solo de la base2
			limit1:=v_in_cant_reg::integer;
			of1:=v_in_offset1::integer-paginas_base1::integer*v_in_cant_reg::integer-sobra_base1;
			if(get_json('flag_act',json2)='SI') then
				--Agregamos el filtro para que no cuente los q estan actualizados abajo y no en el RS
				if flag_nomina1 then
					query1:=query1||';select '||campo1||' as c'||campo2||' from ((select * from '||tabla_base2||' where '||v_estado||v_parametro_rut_emisor||filtro_tipo_dte1||filtro_dia1||' '||v_in_rut_receptor||' '||v_parametro_tipo_dte||' '||v_parametro_var||v_parametro_referencias||v_parametro_adicional||join1|| ' '||order1||' offset '||of1::varchar||' limit '||limit1::varchar;
				else
					query1:=query1||';select '||campo1||' as c'||campo2||' from '||tabla_base2||' where '||v_estado||v_parametro_rut_emisor||filtro_tipo_dte1||filtro_dia1||' '||v_in_rut_receptor||' '||v_parametro_tipo_dte||' '||v_parametro_var||v_parametro_referencias||v_parametro_adicional|| ' '||order1||' offset '||of1::varchar||' limit '||limit1::varchar;
				end if;
			else
				if flag_nomina1 then
					query1:=query1||';select '||campo1||' as c'||campo2||' from ((select * from '||tabla_base2||' where '||v_estado||v_parametro_rut_emisor||filtro_tipo_dte1||filtro_dia1||' '||v_in_rut_receptor||' '||v_parametro_tipo_dte||' '||v_parametro_var||v_parametro_referencias||v_parametro_adicional||filtro_rs1||join1|| ' '||order1||' offset '||of1::varchar||' limit '||limit1::varchar;
				else
					query1:=query1||';select '||campo1||' as c'||campo2||' from '||tabla_base2||' where '||v_estado||v_parametro_rut_emisor||filtro_tipo_dte1||filtro_dia1||' '||v_in_rut_receptor||' '||v_parametro_tipo_dte||' '||v_parametro_var||v_parametro_referencias||v_parametro_adicional||filtro_rs1|| ' '||order1||' offset '||of1::varchar||' limit '||limit1::varchar;
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
		--json2:=put_json(json2,'IP_RS',get_json('__IP_CONEXION_CLIENTE__',json_par1));
		--json2:=put_json(json2,'PORT_RS',get_json('__IP_PORT_CLIENTE__',json_par1));
	else
		json2:=put_json(json2,'QUERY_RS','');
	end if;
        return json2;
end;
$$
LANGUAGE plpgsql;


