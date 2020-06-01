--Publica documento
delete from isys_querys_tx where llave='12709';
insert into isys_querys_tx values ('12709',10,1,1,'select proc_procesa_cuadratura_12709(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

--Agrega Parametro al Filtro
CREATE or replace FUNCTION agrega_filtro_12709(varchar,varchar,varchar) RETURNS varchar AS $$
declare
	par1 alias for $1; --PARAMETRO1,PARAMETRO2
	filtro1	alias for $2; --PARAMETRO1='SUC' and PARAMETRO2='12'
	xml2	alias for $3;
	filtro2	varchar;
	aux1	varchar;
	rut_cliente1	varchar;
	rut_cliente2	bigint;
	rut_empresa1	varchar;
	rut_empresa2	bigint;
	perfil1	varchar;
begin
	filtro2:=filtro1;
	rut_empresa1:=get_campo('rut_empresa',xml2);
    	if (is_number(rut_empresa1) is false) then
        	xml2:=logapp(xml2,'12709: Rut Empresa no numerico '||rut_empresa1);
	        return xml2;
	    end if;
	rut_empresa2:=rut_empresa1::bigint;
	rut_cliente1:=get_campo('rut_cliente',xml2);
    	if (is_number(rut_cliente1) is false) then
        	xml2:=logapp(xml2,'12709: Rut Cliente no numerico '||rut_cliente1);
	        return xml2;
	end if;
        rut_cliente2:=rut_cliente1::bigint;

	--Verifico los filtros de perfilamiento para este cliente
	perfil1:=obtiene_condicion_perfilamiento_12709(rut_empresa2,rut_cliente2,par1);	
	
	aux1:=get_campo(par1,xml2);
	filtro2:=filtro2||' '||perfil1;
	if (length(aux1)>0 and aux1<>'TODOS') then
		filtro2:=filtro2||' AND '||par1||'='||quote_literal(aux1);
	end if;
	return filtro2;
end
$$ LANGUAGE plpgsql;


--Cambia los estados por Texto
CREATE or replace FUNCTION estado_dte_12709(varchar) RETURNS varchar AS $$
declare
	estado alias for $1;
begin
	return case when estado in ('ACEPTADO_POR_EL_SII','ACEPTADO_CON_REPAROS_POR_EL_SII') then 'Aprobado SII' 
		when estado in ('RECHAZADO_POR_EL_SII') then 'Rechazado SII' 
		when estado='ENVIADO_AL_SII' then 'Enviado SII' 
		when estado='ENVIADO_POR_INTERCAMBIO' then 'Enviado' 
		when estado='ACEPTADO_CON_RECEPCION_TECNICA' then 'Aceptado' 
		when estado='RECHAZADO_CON_NOTIFICACION_COMERCIAL' then 'Rechazado' 
		when estado='RECHAZADO_POR_SERVIDOR_CORREO' then 'Rechazado' 
		when estado='ACEPTADO_CON_NOTIFICACION_COMERCIAL' then 'Aceptado' 
		when estado='ENVIADO_POR_INTERCAMBIO' then 'Enviado' 
		when estado='DTE_REPETIDO' then 'Repetido'
		when estado='REPROCESA_DTE_RECHAZADA_SII' then 'Reemplazado'
		when estado='DTE_YA_ACEPTADO_SII' then 'Folio Duplicado'
		when estado='DTE_EN_ESPERA' then 'Pendiente'
		when estado='DTE_EN_ESPERA*' then 'Pendiente'
		when estado='REPROCESA_DTE_NO_PROCESADO' then 'Reintentado'
		else '' end;
end
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION procesa_exportar_datos_12709(varchar) RETURNS varchar AS $$
declare
	xml1 alias for $1;
	xml2	varchar;
	query1	varchar;
	fecha_ini1	varchar;
	fecha_fin1	varchar;
	respuesta1	varchar;
	filtro1		varchar;
	filtro_adicionales1	varchar;
	campo	RECORD;
	fila2	varchar;
	columna2	varchar;
	aux1	varchar;
	tipo_tabla1	varchar;
	pagina1	varchar;
	stEstado	estado_dte%ROWTYPE;
	header1	varchar;
	rut_empresa1	varchar;
	rut_empresa2	bigint;
	total_reg1	integer;
	tipo_fecha1	varchar;
	dia1		varchar;
	parametros2	varchar;
	folio1		varchar;
	tabla1		varchar;
	campo_adicional	varchar;
	tabla_boletas1	varchar;
	periodo1	varchar;
        rut_receptor1   varchar;
        rut_receptor2   bigint;
        busqueda_avanzada       varchar;

begin
	--Fechas
	xml2:=xml1;
	fila2:=get_campo('fila',xml2);
	columna2:=get_campo('columna',xml2);
	fecha_ini1:=get_campo('dia',xml2);
	tipo_tabla1:=get_campo('tipo_tabla',xml2);
	folio1:=get_campo('avFolio',xml2);
	respuesta1:='';

	rut_empresa1:=get_campo('rut_empresa',xml2);
    	if (is_number(rut_empresa1) is false) then
        	xml2:=logapp(xml2,'12709: Rut Empresa no numerico '||rut_empresa1);
	        return xml2;
	    end if;
	rut_empresa2:=rut_empresa1::bigint;

        rut_receptor1:=get_campo('avRutReceptor',xml2);
        if (is_number(rut_receptor1) is false) then
                xml2:=logapp(xml2,'12709: Rut rut_receptor no numerico '||rut_receptor1);
                rut_receptor2:=0;
        else
                rut_receptor2:=rut_receptor1::bigint;
        end if;
	
	tipo_fecha1:=get_campo('tipoFecha',xml2);
	if (tipo_fecha1<>'E') then
                tipo_fecha1:='A';
		dia1:='dia';
	else
		dia1:='dia_emision';
	end if;
	--Aplico Filtros
	filtro1:='';
	for campo in select parametro from filtros_rut where rut_emisor=rut_empresa2 union select 'tipo_dte' as parametro loop
		filtro1:=agrega_filtro_12709(campo.parametro,filtro1,xml2);
	end loop;
	/*
	filtro1:=agrega_filtro_12709('tipo_dte',filtro1,xml2);
	filtro1:=agrega_filtro_12709('PARAMETRO1',filtro1,xml2);
	filtro1:=agrega_filtro_12709('PARAMETRO2',filtro1,xml2);
	filtro1:=agrega_filtro_12709('PARAMETRO3',filtro1,xml2);
	filtro1:=agrega_filtro_12709('PARAMETRO4',filtro1,xml2);
	filtro1:=agrega_filtro_12709('PARAMETRO5',filtro1,xml2);
	*/
	xml2:=logapp(xml2,'12709: filtros '||filtro1);
	
	--Setea la busqueda avanzada
	busqueda_avanzada:='';
        if (is_number(folio1)) then
                busqueda_avanzada :=' and folio='||folio1;
        end if;
        if (rut_receptor2>0) then
                busqueda_avanzada := busqueda_avanzada||' and rut_receptor='||rut_receptor2;
        end if;
	
	--Obtengo filtros de sseleccion
	--En particular este saca los tipo_dte
	filtro_adicionales1:='';
	if (length(columna2)>0) then
		filtro_adicionales1:=(select a.columna||' in ('||string_agg(quote_literal(c.codigo),',')||')' from (select * from tipo_tabla_cuadratura where tipo_tabla=tipo_tabla1) a left join parametros b on b.parametro=a.parametro_columna left join detalle_parametros c on c.id_parametro=b.id_parametro and codigo_grupo=columna2::integer group by a.columna);
		if (length(filtro_adicionales1)>0) then
			filtro_adicionales1:=' AND '||filtro_adicionales1;
		end if;
	end if;

	--En particular este saaca los estados
	if (length(fila2)>0) then
		aux1:='';
		aux1:=(select c.codigo from (select * from tipo_tabla_cuadratura where tipo_tabla=tipo_tabla1) a left join parametros b on b.parametro=a.parametro_fila left join detalle_parametros c on c.id_parametro=b.id_parametro and codigo_grupo=fila2::integer limit 1);
		xml2:=logapp(xml2,'12709: Evento Detalle '||aux1);
		--Si son estados de error, cambiamos de tabla
		if (aux1 in ('YRE','YRR','YAS','YEE','YNP','YE1')) then
			tabla1:='dte_emitidos_errores';
		else
			tabla1:='dte_emitidos';
		end if;
		xml2:=logapp(xml2,'12709: Tabla Elegida '||tabla1);
				
		if (aux1='ASI') then 
			aux1:=' estado_sii=''ACEPTADO_POR_EL_SII''';
		elsif (aux1='RSI') then
			aux1:=' estado_sii=''RECHAZADO_POR_EL_SII''';
		elsif (aux1='CSI') then
			aux1:=' estado_sii=''ACEPTADO_CON_REPAROS_POR_EL_SII''';
		elsif (aux1='PPI') then
			aux1:=' estado_sii in (''ENVIADO_AL_SII'')';
		elsif (aux1='ESI') then
			aux1:=' estado_sii in (''ENVIADO_AL_SII'',''ACEPTADO_POR_EL_SII'',''RECHAZADO_POR_EL_SII'',''ACEPTADO_CON_REPAROS_POR_EL_SII'')';
		elsif (aux1='ERE') then
			aux1:=' estado_inter in (''ENVIADO_POR_INTERCAMBIO'',''ACEPTADO_CON_RECEPCION_TECNICA'',''ACEPTADO_CON_NOTIFICACION_COMERCIAL'',''RECHAZADO_CON_NOTIFICACION_COMERCIAL'',''RECHAZADO_POR_SERVIDOR_CORREO'')';
		--Pendiente
		elsif (aux1='PPR') then
			aux1:=' estado_inter in (''ENVIADO_POR_INTERCAMBIO'')';
		else
			--Necesito la traduccion del estado Acepta por el guardado en dte_emitidos
			select * into stEstado from estado_dte where codigo=aux1;
			if found then
				if (stEstado.update_dte_emitidos='INTER') then
					aux1:=' estado_inter in ('||quote_literal(stEstado.descripcion)||')';
				elsif (tabla1='dte_emitidos_errores') then
					aux1:=' estado in ('||quote_literal(stEstado.descripcion)||')';
				else
					aux1:='';
				end if;
			else
				aux1:='';
			end if;
		end if;
		if (length(aux1)>0) then
			filtro_adicionales1:=filtro_adicionales1||' AND '||aux1;
		end if;
	end if;

	if (is_number(fecha_ini1) is false) then
		parametros2:=(select ';'||string_agg(alias_web,';') from filtros_rut where rut_emisor=rut_empresa2);
		--Hay que mandar el header
		xml2:=logapp(xml2,'12709: Envio Header File');
		if (tabla1='dte_emitidos_errores') then
			respuesta1:='Tipo Doc;Folio;R.U.T. Receptor;Publicacion;Emision;Monto Total'||coalesce(parametros2,'')||';Estado SII;Estado Intercambio;url';
		else
			--RME 20140729 excepcion shopping center
			if rut_empresa2=94226000 then
				respuesta1:='Tipo Doc;Folio;R.U.T. Receptor;Publicacion;Emision;Monto Total'||coalesce(parametros2,'')||';Estado;veces;trx;url';
			else
				respuesta1:='Tipo Doc;Folio;R.U.T. Receptor;Publicacion;Emision;Monto Total'||coalesce(parametros2,'')||';Estado;veces;url';
			end if;

		end if;
		--respuesta1:='{"funcion":"DETALLE","tipo_tabla":"'||tipo_tabla1||'","DATA":"'||respuesta1||'"}';
	        --xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK'||chr(10)||'Content-type: application/json;charset=UTF-8;'||chr(10)||'Content-length: '||length(respuesta1)||chr(10)||chr(10)||respuesta1);
		--Si viene Folio, contestamos todo inmediatamente
		if (busqueda_avanzada='') then
        		xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK'||chr(10)||'Content-type: text;'||chr(10)||'Content-length: '||length(respuesta1)||chr(10)||chr(10)||respuesta1);
			return xml2;
		else
			respuesta1:=respuesta1||'\n';
		end if;
	end if;

	--Agrego parametros
	parametros2:=(select '||'';''||'||string_agg('coalesce('||parametro||','''')','||'';''||') from filtros_rut where rut_emisor=rut_empresa2);

	--RME 20140729 excepcion shopping center
	if rut_empresa2=94226000 then
		campo_adicional:='||get_xml(''TRX'',data_dte)||'';''';
	else
		campo_adicional:='';
	end if;

	tabla_boletas1:='dte_boletas_generica';
	/*
	periodo1:=substring(fecha_ini1,3,4);
        if (periodo1::integer>=1408) then
                tabla_boletas1:='dte_boletas_'||periodo1;
        else
                tabla_boletas1:='dte_boletas';
        end if;
        xml2:=logapp(xml2,'12709: Periodo '||periodo1||' Tabla Boleta='||tabla_boletas1);
	*/

	busqueda_avanzada:='';
        if (is_number(folio1)) then
                busqueda_avanzada :=' and folio='||folio1;
        end if;
        if (rut_receptor2>0) then
                busqueda_avanzada := busqueda_avanzada||' and rut_receptor='||rut_receptor2;
        end if;

        if (busqueda_avanzada<>'') then
	        --Si viene rut receptor, se ocupan las fechas avanzadas
                if (rut_receptor2>0) then
                        fecha_ini1:=get_campo('avDesde',xml2);
                        if (is_number(fecha_ini1) is false) then
                                xml2:=logapp(xml2,'12709: Fecha ini avanzada no es numerico = '||fecha_ini1);
                        end if;
                        fecha_fin1:=get_campo('avHasta',xml2);
                        if (is_number(fecha_fin1) is false) then
                                xml2:=logapp(xml2,'12709: Fecha Fin avanzada no es numerico = '||fecha_fin1);
                        end if;
                        tabla1:='dte_emitidos';
                        --Si viene rut receptor, no busca en boletas
			query1:='select string_agg(tipo_dte::varchar||'';''||folio::varchar||'';''||rut_receptor::varchar||''-''||modulo11(rut_receptor::varchar)||'';''||to_char(fecha_ingreso,''YYYY-MM-DD HH24:MI:SS'')||'';''||fecha_emision::varchar||'';''||monto_total::varchar'||coalesce(parametros2,'')||'||'';''||estado_dte_12709(estado_sii)||'';''||estado_dte_12709(estado_inter)||'';''' ||campo_adicional||'||uri,''\n'') from (select * from '||tabla1||' where '||dia1||'>=$1::integer and '||dia1||'<=$2::integer and rut_emisor=$3 and tipo_dte in (33,34,46,56,61,111,110,112,39,41,43,52) '||busqueda_avanzada||' '||filtro1||' '||filtro_adicionales1||') a' ;
		else
		 	--Para buscar una boleta especifica, recorro todas las tablas de boletas de adelante hacia atras
	                --La idea es determinar en que tabla de boletas esta el rut tipo folio
        	        --tabla_boletas1:=sp_busca_tabla_boleta_folio(rut_empresa2,folio1::bigint);

			query1:='select string_agg(tipo_dte::varchar||'';''||folio::varchar||'';''||rut_receptor::varchar||''-''||modulo11(rut_receptor::varchar)||'';''||to_char(fecha_ingreso,''YYYY-MM-DD HH24:MI:SS'')||'';''||fecha_emision::varchar||'';''||monto_total::varchar'||coalesce(parametros2,'')||'||'';''||estado_dte_12709(estado_sii)||'';''||estado_dte_12709(estado_inter)||'';''' ||campo_adicional||'||uri,''\n'') from (select codigo_txel , fecha_ingreso , mes , dia , tipo_dte , folio , fecha_emision , mes_emision , dia_emision , fecha_vencimiento , rut_emisor , rut_receptor , monto_neto , monto_total , fecha_ult_modificacion , estado , hash_md5 , uri , estado_sii , parametro1 , parametro2 , parametro3 , parametro4 , parametro5 , fecha_sii , dia_sii , digest , data_dte , estado_inter , estado_mandato , monto_excento , monto_iva , fecha_inter , mensaje_inter , mensaje_sii  from dte_boletas_generica where rut_emisor=$3 and tipo_dte in (33,34,46,56,61,111,110,112,39,41,43,52) '||busqueda_avanzada||' '||filtro1||' '||filtro_adicionales1||' union select codigo_txel , fecha_ingreso , mes , dia , tipo_dte , folio , fecha_emision , mes_emision , dia_emision , fecha_vencimiento , rut_emisor , rut_receptor , monto_neto , monto_total , fecha_ult_modificacion , estado , hash_md5 , uri , estado_sii , parametro1 , parametro2 , parametro3 , parametro4 , parametro5 , fecha_sii , dia_sii , digest , data_dte , estado_inter , estado_mandato , monto_excento , monto_iva , fecha_inter , mensaje_inter , mensaje_sii from dte_emitidos where rut_emisor=$3 and tipo_dte in (33,34,46,56,61,111,110,112,39,41,43,52) '||busqueda_avanzada||' '||filtro1||' '||filtro_adicionales1||') a' ;
			xml2:=logapp(xml2,'12709: Exporta Detalle Folio');
		end if;

	--query1:='select array_to_json(array_agg(row_to_json(rds))) from 
	--Si viene algun filtro con boletas, sacamos de otra tabla
	elsif (strpos(filtro_adicionales1,'tipo_dte in (''39'')')>0) then
		query1:='select string_agg(tipo_dte::varchar||'';''||folio::varchar||'';''||rut_receptor::varchar||''-''||modulo11(rut_receptor::varchar)||'';''||to_char(fecha_ingreso,''YYYY-MM-DD HH24:MI:SS'')||'';''||fecha_emision::varchar||'';''||monto_total::varchar'||coalesce(parametros2,'')||'||'';''||estado_dte_12709(estado_sii)||'';''||estado_dte_12709(estado_inter)||'';''' ||campo_adicional||'||uri,''\n'') from (select * from '||tabla_boletas1||' where '||dia1||'=$1::integer and rut_emisor=$3 '||filtro1||' '||filtro_adicionales1||') a';
		xml2:=logapp(xml2,'12709: Extrae Boletas Rut='||rut_empresa2::varchar||' Dia='||fecha_ini1::varchar);
	elsif (tabla1='dte_emitidos_errores') then
		query1:='select string_agg(tipo_dte::varchar||'';''||folio::varchar||'';''||rut_receptor::varchar||''-''||modulo11(rut_receptor::varchar)||'';''||to_char(fecha_ingreso,''YYYY-MM-DD HH24:MI:SS'')||'';''||fecha_emision::varchar||'';''||monto_total::varchar'||coalesce(parametros2,'')||'||'';''||estado_dte_12709(estado)||'';''||veces||'';''||uri,''\n'') from (select * from '||tabla1||' where '||dia1||'=$1::integer and rut_emisor=$3 '||filtro1||' '||filtro_adicionales1||') a';
		xml2:=logapp(xml2,'12709: Extrae DTE Rut='||rut_empresa2::varchar||' Dia='||fecha_ini1::varchar);
	else
		query1:='select string_agg(tipo_dte::varchar||'';''||folio::varchar||'';''||rut_receptor::varchar||''-''||modulo11(rut_receptor::varchar)||'';''||to_char(fecha_ingreso,''YYYY-MM-DD HH24:MI:SS'')||'';''||fecha_emision::varchar||'';''||monto_total::varchar'||coalesce(parametros2,'')||'||'';''||estado_dte_12709(estado_sii)||'';''||estado_dte_12709(estado_inter)||'';''' ||campo_adicional||'||uri,''\n'') from (select * from '||tabla1||' where '||dia1||'=$1::integer and rut_emisor=$3 '||filtro1||' '||filtro_adicionales1||') a';
		xml2:=logapp(xml2,'12709: Extrae DTE Rut='||rut_empresa2::varchar||' Dia='||fecha_ini1::varchar);
	end if;
	execute query1 using fecha_ini1,fecha_fin1,rut_empresa2 into aux1;
	respuesta1:=respuesta1||aux1||'\n';
	
	xml2:=logapp(xml2,query1);
        xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK'||chr(10)||'Content-type: xml;'||chr(10)||'Content-length: '||length(coalesce(respuesta1,''))||chr(10)||chr(10)||coalesce(respuesta1,''));
	return xml2;
end
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION procesa_detalle_12709(varchar) RETURNS varchar AS $$
declare
	xml1 alias for $1;
	xml2	varchar;
	query1	varchar;
	fecha_ini1	varchar;
	fecha_fin1	varchar;
	respuesta1	varchar;
	filtro1		varchar;
	filtro_adicionales1	varchar;
	campo	RECORD;
	fila2	varchar;
	columna2	varchar;
	aux1	varchar;
	tipo_tabla1	varchar;
	pagina1	varchar;
	stEstado	estado_dte%ROWTYPE;
	header1	varchar;
	rut_empresa1	varchar;
	rut_empresa2	bigint;
	tabla1		varchar;
	total_reg1	integer;
	tipo_fecha1	varchar;
	dia1		varchar;
	folio1		varchar;
	tabla_boletas1	varchar;
        rut_receptor1   varchar;
        rut_receptor2   bigint;
        busqueda_avanzada       varchar;
	emirec1	varchar;
	fecha_in1	varchar;


begin
	--Fechas
	xml2:=xml1;
	fila2:=get_campo('fila',xml2);
	columna2:=get_campo('columna',xml2);
	fecha_ini1:=get_campo('fDesde',xml2);
	tipo_tabla1:=get_campo('tipo_tabla',xml2);
	if (is_number(fecha_ini1) is false) then
		xml2:=logapp(xml2,'12709: Fecha Ini no es numerico = '||fecha_ini1);
		return xml2;
	end if;
	fecha_fin1:=get_campo('fHasta',xml2);
	if (is_number(fecha_fin1) is false) then
		xml2:=logapp(xml2,'12709: Fecha fin no es numerico = '||fecha_fin1);
		return xml2;
	end if;
	rut_empresa1:=get_campo('rut_empresa',xml2);
    	if (is_number(rut_empresa1) is false) then
        	xml2:=logapp(xml2,'12709: Rut Empresa no numerico '||rut_empresa1);
	        return xml2;
	    end if;
	rut_empresa2:=rut_empresa1::bigint;

        rut_receptor1:=get_campo('avRutReceptor',xml2);
        if (is_number(rut_receptor1) is false) then
                xml2:=logapp(xml2,'12709: Rut rut_receptor no numerico '||rut_receptor1);
                rut_receptor2:=0;
        else
                rut_receptor2:=rut_receptor1::bigint;
        end if;


	tipo_fecha1:=get_campo('tipoFecha',xml2);
	if (tipo_fecha1<>'E') then
                tipo_fecha1:='A';
		dia1:='dia';
	else
		dia1:='dia_emision';
	end if;

	emirec1:=get_campo('emirec',xml2);

	--Aplico Filtros
	filtro1:='';
	for campo in select parametro from filtros_rut where rut_emisor=rut_empresa2 union select 'tipo_dte' as parametro loop
		filtro1:=agrega_filtro_12709(campo.parametro,filtro1,xml2);
	end loop;
	/*
	filtro1:=agrega_filtro_12709('tipo_dte',filtro1,xml2);
	filtro1:=agrega_filtro_12709('PARAMETRO1',filtro1,xml2);
	filtro1:=agrega_filtro_12709('PARAMETRO2',filtro1,xml2);
	filtro1:=agrega_filtro_12709('PARAMETRO3',filtro1,xml2);
	filtro1:=agrega_filtro_12709('PARAMETRO4',filtro1,xml2);
	filtro1:=agrega_filtro_12709('PARAMETRO5',filtro1,xml2);
	*/
	xml2:=logapp(xml2,'12709: filtros '||filtro1);
	
	--Obtengo filtros de sseleccion
	--En particular este saca los tipo_dte
	filtro_adicionales1:='';
	if (length(columna2)>0) then
		filtro_adicionales1:=(select a.columna||' in ('||string_agg(quote_literal(c.codigo),',')||')' from (select * from tipo_tabla_cuadratura where tipo_tabla=tipo_tabla1) a left join parametros b on b.parametro=a.parametro_columna left join detalle_parametros c on c.id_parametro=b.id_parametro and codigo_grupo=columna2::integer group by a.columna);
		if (length(filtro_adicionales1)>0) then
			filtro_adicionales1:=' AND '||filtro_adicionales1;
		end if;
	end if;

	--En particular este saaca los estados
	if (length(fila2)>0) then
		aux1:='';
		aux1:=(select c.codigo from (select * from tipo_tabla_cuadratura where tipo_tabla=tipo_tabla1) a left join parametros b on b.parametro=a.parametro_fila left join detalle_parametros c on c.id_parametro=b.id_parametro and codigo_grupo=fila2::integer limit 1);
		xml2:=logapp(xml2,'12709: Evento Detalle '||aux1);
		--Si son estados de error, cambiamos de tabla
		if (aux1 in ('YRE','YRR','YAS','YEE','YNP','YE1')) then
			tabla1:='dte_emitidos_errores';
		elsif (emirec1='RECIBIDOS') then
			tabla1:='dte_recibidos';
		else
			tabla1:='dte_emitidos';
		end if;
		xml2:=logapp(xml2,'12709: Tabla Elegida '||tabla1);
				
		--Emitidos
		if (aux1='ASI') then 
			aux1:=' estado_sii=''ACEPTADO_POR_EL_SII''';
		elsif (aux1='RSI') then
			aux1:=' estado_sii=''RECHAZADO_POR_EL_SII''';
		elsif (aux1='CSI') then
			aux1:=' estado_sii=''ACEPTADO_CON_REPAROS_POR_EL_SII''';
		elsif (aux1='PPI') then
			aux1:=' estado_sii in (''ENVIADO_AL_SII'')';
		elsif (aux1='ESI') then
			aux1:=' estado_sii in (''ENVIADO_AL_SII'',''ACEPTADO_POR_EL_SII'',''RECHAZADO_POR_EL_SII'',''ACEPTADO_CON_REPAROS_POR_EL_SII'')';
		elsif (aux1='ERE') then
			aux1:=' estado_inter in (''ENVIADO_POR_INTERCAMBIO'',''ACEPTADO_CON_RECEPCION_TECNICA'',''ACEPTADO_CON_NOTIFICACION_COMERCIAL'',''RECHAZADO_CON_NOTIFICACION_COMERCIAL'',''RECHAZADO_POR_SERVIDOR_CORREO'')';
		--Eventos de Recibidos
		elsif (aux1='RCSI') then
			aux1:=' estado_sii=''ACEPTADO_CON_REPAROS_POR_EL_SII''';
		elsif (aux1='RASI') then
			aux1:=' estado_sii=''ACEPTADO_POR_EL_SII''';
		elsif (aux1='REMI') then
			aux1:=' estado_sii in (''ACEPTADO_CON_REPAROS_POR_EL_SII'',''ACEPTADO_POR_EL_SII'','''')';
		--Pendiente
		elsif (aux1='PPR') then
			aux1:=' estado_inter in (''ENVIADO_POR_INTERCAMBIO'')';
		else
			--Necesito la traduccion del estado Acepta por el guardado en dte_emitidos
			select * into stEstado from estado_dte where codigo=aux1;
			if found then
				if (stEstado.update_dte_emitidos='INTER') then
					aux1:=' estado_inter in ('||quote_literal(stEstado.descripcion)||')';
				elsif (tabla1='dte_emitidos_errores') then
					aux1:=' estado in ('||quote_literal(stEstado.descripcion)||')';
				else
					aux1:='';
				end if;
			else
				aux1:='';
			end if;
		end if;
		if (length(aux1)>0) then
			filtro_adicionales1:=filtro_adicionales1||' AND '||aux1;
		end if;
	end if;
	--Tomamos el numero de pagina y hacemos el offsett
	pagina1:=get_campo('pos',xml2);
	if (is_number(pagina1) is false) then
		xml2:=logapp(xml2,'12709: Numero de Pagina no es numerico, vamos a pagina 1 tabla='||tabla1);
		pagina1:='0';
		if (tabla1='dte_emitidos_errores') then
			respuesta1:='<Detalle><DetalleHeader> <id>DETALLE</id> <columnas> <columna> <nombre>Tipo Doc</nombre> <tipo>str</tipo><estilo>centro</estilo> </columna> <columna> <nombre>Folio</nombre> <estilo>izquierdo</estilo> <tipo>url</tipo> </columna> <columna> <nombre>R.U.T. Receptor</nombre> <tipo>str</tipo> <estilo>derecho</estilo> </columna><columna> <nombre>Publicacion</nombre> <tipo>str</tipo> </columna> <columna> <nombre>Emision</nombre> <tipo>str</tipo> </columna> <columna> <nombre>Monto Total</nombre> <tipo>str</tipo> <estilo>derecho</estilo></columna> <columna> <nombre>Estado </nombre> <tipo>str</tipo> </columna><columna> <nombre>Veces</nombre> <tipo>str</tipo> </columna> <columna> <nombre>Eventos</nombre> <tipo>url_img</tipo> <estilo>centro</estilo></columna> </columnas> </DetalleHeader></Detalle>';	
		elsif (emirec1='RECIBIDOS') then
			respuesta1:='<Detalle><DetalleHeader> <id>DETALLE</id> <columnas> <columna> <nombre>Tipo Doc</nombre> <tipo>str</tipo><estilo>centro</estilo> </columna> <columna> <nombre>Folio</nombre> <estilo>izquierdo</estilo> <tipo>url</tipo> </columna> <columna> <nombre>R.U.T. Emisor</nombre> <tipo>str</tipo> <estilo>derecho</estilo> </columna><columna> <nombre>Publicacion</nombre> <tipo>str</tipo> </columna> <columna> <nombre>Emision</nombre> <tipo>str</tipo> </columna> <columna> <nombre>Monto Total</nombre> <tipo>str</tipo> <estilo>derecho</estilo></columna> <columna> <nombre>Estado SII</nombre> <tipo>str</tipo> </columna> <columna> <nombre>Eventos</nombre> <tipo>url_img</tipo> <estilo>centro</estilo></columna> </columnas> </DetalleHeader></Detalle>';	
		else
			--RME 20140729 se hace excepcion para Shopping Center
			if (rut_empresa1='94226000') then
				respuesta1:='<Detalle><DetalleHeader> <id>DETALLE</id> <columnas> <columna> <nombre>Tipo Doc</nombre> <tipo>str</tipo><estilo>centro</estilo> </columna> <columna> <nombre>Folio</nombre> <estilo>izquierdo</estilo> <tipo>url</tipo> </columna> <columna> <nombre>R.U.T. Receptor</nombre> <tipo>str</tipo> <estilo>derecho</estilo> </columna><columna> <nombre>Publicacion</nombre> <tipo>str</tipo> </columna> <columna> <nombre>Emision</nombre> <tipo>str</tipo> </columna> <columna> <nombre>Monto Total</nombre> <tipo>str</tipo> <estilo>derecho</estilo></columna> <columna> <nombre>Estado SII</nombre> <tipo>str</tipo> </columna> <columna> <nombre>Estado Intercambio</nombre> <tipo>str</tipo> </columna><columna> <nombre>TRX</nombre> <tipo>str</tipo> </columna> <columna> <nombre>Eventos</nombre> <tipo>url_img</tipo> <estilo>centro</estilo></columna> </columnas> </DetalleHeader></Detalle>';	
			else
				respuesta1:='<Detalle><DetalleHeader> <id>DETALLE</id> <columnas> <columna> <nombre>Tipo Doc</nombre> <tipo>str</tipo><estilo>centro</estilo> </columna> <columna> <nombre>Folio</nombre> <estilo>izquierdo</estilo> <tipo>url</tipo> </columna> <columna> <nombre>R.U.T. Receptor</nombre> <tipo>str</tipo> <estilo>derecho</estilo> </columna><columna> <nombre>Publicacion</nombre> <tipo>str</tipo> </columna> <columna> <nombre>Emision</nombre> <tipo>str</tipo> </columna> <columna> <nombre>Monto Total</nombre> <tipo>str</tipo> <estilo>derecho</estilo></columna> <columna> <nombre>Estado SII</nombre> <tipo>str</tipo> </columna> <columna> <nombre>Estado Intercambio</nombre> <tipo>str</tipo> </columna> <columna> <nombre>Eventos</nombre> <tipo>url_img</tipo> <estilo>centro</estilo></columna> </columnas> </DetalleHeader></Detalle>';	
			end if;
		end if;
		respuesta1:='{"funcion":"DETALLE","tipo_tabla":"'||tipo_tabla1||'","DATA":"'||respuesta1||'"}';
	        --xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK'||chr(10)||'Content-type: application/json;charset=UTF-8;'||chr(10)||'Content-length: '||length(respuesta1)||chr(10)||chr(10)||respuesta1);
        	xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK'||chr(10)||'Content-type: xml;'||chr(10)||'Content-length: '||length(respuesta1)||chr(10)||chr(10)||respuesta1);
		xml2:=logapp(xml2,'12709: '||respuesta1);
		return xml2;
	end if;

	total_reg1:=0;
	--query1:='select array_to_json(array_agg(row_to_json(rds))) from 
	respuesta1:='<Detalle><DetalleData><filas>';

	--Si viene avFolio hacemos consulta directa
	folio1:=get_campo('avFolio',xml2);
	
	busqueda_avanzada:='';
	if (is_number(folio1)) then
                busqueda_avanzada :=' and folio='||folio1;
        end if;
        if (rut_receptor2>0) then
                busqueda_avanzada := busqueda_avanzada||' and rut_receptor='||rut_receptor2;
        end if;

		
	--FAY 20160321
	fecha_in1:=genera_in_fechas(fecha_ini1::integer,fecha_fin1::integer);

	if (busqueda_avanzada<>'') then	
		filtro1:=coalesce(filtro1,'');
		filtro_adicionales1:=coalesce(filtro_adicionales1,'');
                	
		fecha_ini1:=get_campo('avDesde',xml2);
                if (is_number(fecha_ini1) is false) then
               	        xml2:=logapp(xml2,'12709: Fecha ini avanzada no es numerico = '||fecha_ini1);
	        end if;
                fecha_fin1:=get_campo('avHasta',xml2);
               	if (is_number(fecha_fin1) is false) then
                      	xml2:=logapp(xml2,'12709: Fecha Fin avanzada no es numerico = '||fecha_fin1);
	        end if;
	
		--Defino la tabla de obtencion de detalles segun EMITIDOS o RECIBIDOS
		if (emirec1='RECIBIDOS') then
			tabla1:='dte_recibidos';
		else
			tabla1:='dte_emitidos';
		end if;	
	
		--Si viene rut receptor, se ocupan las fechas avanzadas
		if (rut_receptor2>0) then
			--tabla1:='dte_emitidos';
			--Si viene rut receptor, no busca en boletas
			query1:='select *,0 as veces from '||tabla1||' where '||dia1||'>=$1::integer and '||dia1||'<=$2::integer and rut_emisor=$3 '||filtro1||' '||filtro_adicionales1||' '||busqueda_avanzada||' offset '||pagina1||' limit 100';
		else
			--Si son recibidos, no miramos las boletas
			if (tabla1='dte_recibidos') then
				query1:='select *,0 as veces from (select * from '||tabla1||' where 1=1 '||filtro1||' '||filtro_adicionales1||' and tipo_dte in (33,34,46,56,61,111,110,112,39,41,43,52) '||busqueda_avanzada||' and '||dia1||'>=$1::integer and '||dia1||'<=$2::integer) a' ;
			else
				query1:='select *,0 as veces from (select codigo_txel , fecha_ingreso , mes , dia , tipo_dte , folio , fecha_emision , mes_emision , dia_emision , fecha_vencimiento , rut_emisor , rut_receptor, monto_neto , monto_total , fecha_ult_modificacion , estado , hash_md5 , uri , estado_sii , parametro1 , parametro2 , parametro3 , parametro4 , parametro5 , fecha_sii , dia_sii , digest , data_dte , estado_inter , estado_mandato , monto_excento , monto_iva , fecha_inter , mensaje_inter , mensaje_sii from dte_boletas_generica where rut_emisor=$3 '||filtro1||' '||filtro_adicionales1||' and tipo_dte in (33,34,46,56,61,111,110,112,39,41,43,52) '||busqueda_avanzada||' union select codigo_txel , fecha_ingreso , mes , dia , tipo_dte , folio , fecha_emision , mes_emision , dia_emision , fecha_vencimiento , rut_emisor , rut_receptor , monto_neto , monto_total , fecha_ult_modificacion , estado , hash_md5 , uri , estado_sii , parametro1 , parametro2 , parametro3 , parametro4 , parametro5 , fecha_sii , dia_sii , digest , data_dte , estado_inter , estado_mandato , monto_excento , monto_iva , fecha_inter , mensaje_inter , mensaje_sii from '||tabla1||' where rut_emisor=$3 '||filtro1||' '||filtro_adicionales1||' and tipo_dte in (33,34,46,56,61,111,110,112,39,41,43,52) '||busqueda_avanzada||') a' ;
			 end if;
			xml2:=logapp(xml2,'12709: Query Busqueda Avanzada');
			xml2:=logapp(xml2,'12709: '||query1);
       		end if;

	--Si viene algun filtro con boletas, sacamos de otra tabla
	elsif (strpos(filtro_adicionales1,'tipo_dte in (''39'')')>0) then
		query1:='select *,0 as veces from dte_boletas_generica where '||dia1||'>=$1::integer and '||dia1||'<=$2::integer and rut_emisor=$3 '||filtro1||' '||filtro_adicionales1||' offset '||pagina1||' limit 100';
		
		xml2:=logapp(xml2,'12709: Query Boletas');
		tabla1:='dte_emitidos';
	--Si es de dte_recibidos cambiar el rut_emisor por rut_receptor
	elsif (emirec1='RECIBIDOS') then
		query1:='select *,0 as veces from '||tabla1||' where '||dia1||'>=$1::integer and '||dia1||'<=$2::integer and rut_receptor=$3 '||filtro1||' '||filtro_adicionales1||' offset '||pagina1||' limit 100';
	else
		query1:='select *,0 as veces from '||tabla1||' where '||dia1||'>=$1::integer and '||dia1||'<=$2::integer and rut_emisor=$3 '||filtro1||' '||filtro_adicionales1||' offset '||pagina1||' limit 100';
	end if;
	xml2:=logapp(xml2,coalesce(query1,'query nulo'));
	--raise notice 'tabla1=% query=%',tabla1,query1;
	for campo in execute query1 using fecha_ini1,fecha_fin1,rut_empresa2  loop
		respuesta1:=respuesta1||'<fila>'||
		'<cont>'||campo.tipo_dte::varchar||'</cont>'||chr(10)||
		'<cont>'||campo.folio::varchar||'##'||campo.uri||'</cont>'||chr(10)||
		case when tabla1='dte_recibidos' then
		'<cont>'||campo.rut_emisor::varchar||'-'||modulo11(campo.rut_emisor::varchar)||'</cont>'||chr(10)
		else
		'<cont>'||campo.rut_receptor::varchar||'-'||modulo11(campo.rut_receptor::varchar)||'</cont>'||chr(10)
		end||
		'<cont>'||to_char(campo.fecha_ingreso,'YYYY-MM-DD HH24:MI:SS')||'</cont>'||chr(10)||
		'<cont>'||campo.fecha_emision::varchar||'</cont>'||chr(10)||
		'<cont>'||'$'||edita_numero(campo.monto_total::varchar)||'</cont>'||chr(10)||
		case when tabla1='dte_emitidos_errores' then
			'<cont>'||estado_dte_12709(campo.estado)||'</cont>'||chr(10)||
			'<cont>'||coalesce(campo.veces::varchar,'')||'</cont>'||chr(10)
		when tabla1='dte_recibidos' then
			'<cont>'||estado_dte_12709(campo.estado_sii)||'</cont>'||chr(10)
		else
			'<cont>'||estado_dte_12709(campo.estado_sii)||'</cont>'||chr(10)||
			'<cont>'||estado_dte_12709(campo.estado_inter)||'</cont>'||chr(10)
		end||
		--RME 20140729 se hace excepcion para shopping center
		case when campo.rut_emisor=94226000 then
			'<cont>'||get_xml('TRX',campo.data_dte)||'</cont>'||chr(10)
		else
			''
		end||
		'<cont>'||'lupa.gif##'||replace(campo.uri,'/v01/','/traza/')||'</cont>'||chr(10)||
		'</fila>';
		total_reg1:=total_reg1+1;
	end loop;
	respuesta1:=respuesta1||'</filas></DetalleData></Detalle>';
	--{"tipo_dte":"1##Cant. Doc. Nacionales","estado":"3##Emitidos","total":137341}
	
	xml2:=logapp(xml2,'12709: Fecha_ini='||fecha_ini1||'Fecha_Fin='||fecha_fin1);
	xml2:=logapp(xml2,'12709: rut='||rut_empresa2::varchar);
	xml2:=logapp(xml2,'12709: Total_Reg='||total_reg1::varchar);
        
	--EXECUTE query1 into respuesta1 using fecha_ini1,fecha_fin1,rut_empresa2;
	respuesta1:='{"funcion":"DETALLE","tipo_tabla":"'||tipo_tabla1||'","DATA":"'||respuesta1||'"}';
        --xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK'||chr(10)||'Content-type: application/json;charset=UTF-8;'||chr(10)||'Content-length: '||length(respuesta1)||chr(10)||chr(10)||respuesta1);
        xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK'||chr(10)||'Content-type: xml;'||chr(10)||'Content-length: '||length(respuesta1)||chr(10)||chr(10)||respuesta1);
	return xml2;
end
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION procesa_consulta_cuadratura_12709(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2    varchar;
        respuesta1      varchar;
        query1  varchar;
        input1  varchar;
        rut_empresa1    varchar;
        rut_empresa2    bigint;
        filtro1 varchar;
        aux     varchar;
	tipo_tabla1	varchar;
	stTabla	tipo_tabla_cuadratura%ROWTYPE;
	stNavegacion	navegacion_tabla_cuadratura%ROWTYPE;
	fila1	varchar;
	columna1	varchar;
	emirec1		varchar;
BEGIN
        xml2:=xml1;
	rut_empresa1:=get_campo('rut_empresa',xml2);
	tipo_tabla1:=get_campo('tipo_tabla',xml2);
	fila1:=get_campo('fila',xml2);
	columna1:=get_campo('columna',xml2);
	

	--Si no viene tipo_tabla, vamos a la tabla por defecto para ese RUT
	if (length(tipo_tabla1)=0) then
	
		--FAY 2014-10-20 Definimos el tipo de tabla segun el tipo de documento a Mostrar
		--EMITIDOS o RECIBIDOS, EMITIDOS = CUADRATURA2, RECIBIDOS=CUADRATURA_REC
		emirec1:=get_campo('emirec',xml2);
		xml2:=logapp(xml2,'12709: Tipo Pantalla Inicial = '||emirec1);
		if (emirec1='RECIBIDOS') then
			tipo_tabla1:='CUADRATURA_REC';
		else
			tipo_tabla1:='CUADRATURA2';
		end if;

		select * into stTabla from tipo_tabla_cuadratura where tipo_tabla=tipo_tabla1;
		if not found then
			xml2:=logapp(xml2,'12709: Tabla '||tipo_tabla1||' no definida para ese RUT');
			return xml2;
		end if;
		xml2:=put_campo(xml2,'tipo_tabla',stTabla.tipo_tabla);
		xml2:=put_campo(xml2,'tipo_tabla_anterior',stTabla.tipo_tabla);
		--xml2:=procesa_tabla_12709('tipo_dte','estado','cantidad','documentos_cuadratura','estados_cuadratura',xml2);
		xml2:=procesa_tabla_12709(stTabla.columna,stTabla.fila,stTabla.agrupador,stTabla.parametro_columna,stTabla.parametro_fila,xml2,stTabla.agrupado2);
		return xml2;	
	end if;
	
		
	--Veo si tiene navegacion especifica
	select * into stNavegacion from navegacion_tabla_cuadratura where tipo_tabla=tipo_tabla1 and codigo_grupo_columna=columna1::integer and codigo_grupo_fila=fila1::integer;
	if found then
	
		--Si la pantalla es de detalle, contesto el detalle
		if (stNavegacion.tipo_tabla_destino='DETALLE') then
			xml2:=procesa_detalle_12709(xml2);
			return xml2;
		end if;
	
		select * into stTabla from tipo_tabla_cuadratura where tipo_tabla=stNavegacion.tipo_tabla_destino;
		if found then
			xml2:=put_campo(xml2,'tipo_tabla',stNavegacion.tipo_tabla_destino);
			xml2:=put_campo(xml2,'tipo_tabla_anterior',stNavegacion.tipo_tabla);
			xml2:=procesa_tabla_12709(stTabla.columna,stTabla.fila,stTabla.agrupador,stTabla.parametro_columna,stTabla.parametro_fila,xml2,stTabla.agrupado2);
			return xml2;
		end if;
	else
		select * into stNavegacion from navegacion_tabla_cuadratura where tipo_tabla=tipo_tabla1 and codigo_grupo_columna=-1 and codigo_grupo_fila=-1;
		if found then
			--Si la pantalla es de detalle, contesto el detalle
			if (stNavegacion.tipo_tabla_destino='DETALLE') then
				xml2:=procesa_detalle_12709(xml2);
				return xml2;
			end if;
			select * into stTabla from tipo_tabla_cuadratura where tipo_tabla=stNavegacion.tipo_tabla_destino;
			if found then
				xml2:=put_campo(xml2,'tipo_tabla',stNavegacion.tipo_tabla_destino);
				xml2:=put_campo(xml2,'tipo_tabla_anterior',stNavegacion.tipo_tabla);
				xml2:=procesa_tabla_12709(stTabla.columna,stTabla.fila,stTabla.agrupador,stTabla.parametro_columna,stTabla.parametro_fila,xml2,stTabla.agrupado2);
				return xml2;
			end if;
		end if;
	end if;
	--Si no esta definida la navegacion vuelve al principal
	select * into stTabla from tipo_tabla_cuadratura where tipo_tabla='CUADRATURA2';
	if not found then
		xml2:=logapp(xml2,'12709: Tabla CUADRATURA2 no definida para ese RUT');
		return xml2;
	end if;
	xml2:=put_campo(xml2,'tipo_tabla',stTabla.tipo_tabla);
	xml2:=put_campo(xml2,'tipo_tabla_anterior',stNavegacion.tipo_tabla);
	--xml2:=procesa_tabla_12709('tipo_dte','estado','cantidad','documentos_cuadratura','estados_cuadratura',xml2);
	xml2:=procesa_tabla_12709(stTabla.columna,stTabla.fila,stTabla.agrupador,stTabla.parametro_columna,stTabla.parametro_fila,xml2,stTabla.agrupado2);
	return xml2;
end
$$ LANGUAGE plpgsql;


--Esta funcion recibe como parametro fila y columna para armar una json de respuesta
CREATE or replace FUNCTION procesa_tabla_12709(varchar,varchar,varchar,varchar,varchar,varchar,varchar) RETURNS varchar AS $$
DECLARE
    fila1	alias for $1;
    columna1	alias for $2;
    agrupador1	alias for $3;
    parametro_fila1	alias for $4;
    parametro_columna1	alias for $5;
    xml1        alias for $6;
    agrupador2        alias for $7;
    xml2    varchar;
        respuesta1      varchar;
        respuesta2      varchar;
	resp_final1	varchar;
	query1	varchar;
	first1	boolean;
	grilla1	smallint;
	query2	varchar;
	misma_tabla1	boolean;
        input1  varchar;
	rut_empresa1	varchar;
	rut_empresa2	bigint;
	filtro1	varchar;
	aux1	varchar;
	fecha_ini1	varchar;
	fecha_fin1	varchar;
	filtro_adicionales1	varchar;	
	fila2	varchar;
	columna2	varchar;
	tipo_tabla1	varchar;
	tipo_fecha1	varchar;
	campo	RECORD;
	version1	varchar;
BEGIN
	xml2:=xml1;
	--tipo_tx=Consulta&rut_cliente=108324996&rut_empresaLogin=81201000K&fDesde=20140301&fHasta=20140321&rut_empresa=96671750&tipo_dte=TODOS&PARAMETRO1=TODOS&PARAMETRO2=TODOS&tipo_tabla=CUADRATURA1&columna=33&fila=1&

--	xml2:=put_campo(xml2,'fecha_ini','20140301');
--	xml2:=put_campo(xml2,'fecha_fin','20140331');
	query2:=null;
	version1:=get_campo('version',xml2);
	fila2:=get_campo('fila',xml2);
	columna2:=get_campo('columna',xml2);
	tipo_tabla1:=get_campo('tipo_tabla_anterior',xml2);
 	tipo_fecha1:=get_campo('tipoFecha',xml2);
	resp_final1:='';
	if (tipo_fecha1<>'E') then 
		tipo_fecha1:='A';
	end if;

	rut_empresa1:=get_campo('rut_empresa',xml2);
	if (is_number(rut_empresa1) is false) then
		xml2:=logapp(xml2,'12709: Rut Empresa no numerico = '||rut_empresa1);
		return xml2;
	end if;
	rut_empresa2:=rut_empresa1::bigint;

	--Fechas
	fecha_ini1:=get_campo('fDesde',xml2);
	if (is_number(fecha_ini1) is false) then
		xml2:=logapp(xml2,'12709: Fecha Ini no es numerico = '||fecha_ini1);
		return xml2;
	end if;
	fecha_fin1:=get_campo('fHasta',xml2);
	if (is_number(fecha_fin1) is false) then
		xml2:=logapp(xml2,'12709: Fecha fin no es numerico = '||fecha_fin1);
		return xml2;
	end if;

	--Aplico Filtros
	filtro1:='';
	for campo in select parametro from filtros_rut where rut_emisor=rut_empresa2 union select 'tipo_dte' as parametro loop
		filtro1:=agrega_filtro_12709(campo.parametro,filtro1,xml2);
	end loop;
	/*
	filtro1:=agrega_filtro_12709('tipo_dte',filtro1,xml2);
	filtro1:=agrega_filtro_12709('PARAMETRO1',filtro1,xml2);
	filtro1:=agrega_filtro_12709('PARAMETRO2',filtro1,xml2);
	filtro1:=agrega_filtro_12709('PARAMETRO3',filtro1,xml2);
	filtro1:=agrega_filtro_12709('PARAMETRO4',filtro1,xml2);
	filtro1:=agrega_filtro_12709('PARAMETRO5',filtro1,xml2);
	*/
	xml2:=logapp(xml2,'12709: filtros '||filtro1);
	xml2:=put_campo(xml2,'FILTROS',filtro1);

	--Obtengo filtros de sseleccion
	filtro_adicionales1:='';
	if (length(columna2)>0) then
		filtro_adicionales1:=(select a.columna||' in ('||string_agg(quote_literal(c.codigo),',')||')' from (select * from tipo_tabla_cuadratura where tipo_tabla=tipo_tabla1) a left join parametros b on b.parametro=a.parametro_columna left join detalle_parametros c on c.id_parametro=b.id_parametro and codigo_grupo=columna2::integer group by a.columna);
		if (length(filtro_adicionales1)>0) then
			filtro_adicionales1:=' WHERE '||filtro_adicionales1;
		end if;
	end if;

	if (length(fila2)>0) then
		aux1:='';
		aux1:=(select a.fila||' in ('||string_agg(quote_literal(c.codigo),',')||')' from (select * from tipo_tabla_cuadratura where tipo_tabla=tipo_tabla1) a left join parametros b on b.parametro=a.parametro_fila left join detalle_parametros c on c.id_parametro=b.id_parametro and codigo_grupo=fila2::integer group by a.fila);
		if (length(aux1)>0) then
			if (length(filtro_adicionales1)=0) then
				filtro_adicionales1:=' WHERE '||aux1;
			elsif (length(filtro_adicionales1)>0) then
				filtro_adicionales1:=filtro_adicionales1||' AND '||aux1;
			end if;
		end if;
	end if;
	xml2:=logapp(xml2,'12709: filtros_adicionales='||filtro_adicionales1);
	xml2:=put_campo(xml2,'FILTROS_ADICIONALES',filtro_adicionales1);

	xml2:=logapp(xml2,'12709: tipo_tabla='||get_campo('tipo_tabla',xml2));

			
	resp_final1:='{"tipo_tabla":"'||get_campo('tipo_tabla',xml2)||'","DATA": ';

	--Definimos la primera pantalla
	--Columna Tipo_DTE
	--Filas Estados
	--Hacia abajo
	if (agrupador2 is null) then
		perform logfile('INDEXER F_12709 1.0 '||xml2);
		
		--Se hacen 2 conjuntos, primero el relleno que es el cruce entre las filas y las columnas
		-- y luego se igualan por los codigos de grupo y los que vengan en null son los que tienen valor 0
		query1:='
			select codigo_grupo_fila as fila_codigo,descripcion_relleno_fila as fila_descr,codigo_grupo_columna as col_codigo,descripcion_relleno_columna as col_descr ,orden_fila,orden_columna,grilla,coalesce(sum(xx.total),0) as total,nombre_grilla 
					   from
					(select *,(coalesce(grilla_fila,0)::varchar||coalesce(grilla_columna,0)::varchar)::smallint as grilla,coalesce(nombre_grilla_fila,'''')||coalesce(nombre_grilla_columna,'''') as nombre_grilla from 
						(select distinct codigo_grupo as codigo_grupo_fila,descripcion as descripcion_relleno_fila,grilla as grilla_fila,nombre_grilla as nombre_grilla_fila,codigo as codigo_fila,orden as orden_fila from detalle_parametros where id_parametro in (select id_parametro from parametros where parametro='||quote_literal(parametro_fila1)||'))  relleno_fila,
						(select distinct codigo_grupo as codigo_grupo_columna,descripcion as descripcion_relleno_columna,grilla as grilla_columna,nombre_grilla as nombre_grilla_columna, codigo as codigo_columna,orden as orden_columna from detalle_parametros where id_parametro in (select id_parametro from parametros where parametro='||quote_literal(parametro_columna1)||')) relleno_columna) relleno 
						left join
						(select '||fila1||' as fila,'||columna1||' as columna,sum('||agrupador1||') as total from
							(select * from
								(select * from indexer_hash where rut_emisor=$1 '||filtro1||') a join indexer_estadisticas_generica b on a.id=b.id and dia>='||fecha_ini1||' and dia<='||fecha_fin1||' and tipo_dia=$2) ff '||coalesce(filtro_adicionales1,'')||' group by 1,2
						) xx on
						xx.fila::varchar=codigo_fila and xx.columna::varchar=codigo_columna
						group by 1,2,3,4,5,6,7,9
						order by grilla,orden_columna,fila_descr desc';
		resp_final1:=resp_final1||'[';
		--ACA
		grilla1:=0;
		first1:=true;
		for campo in execute query1 using rut_empresa2,tipo_fecha1 loop
			--La primera vez igualo la grilla
			if (first1) then
				resp_final1:=resp_final1||'[';
				grilla1:=coalesce(campo.grilla,0);
				first1:=false;
			elsif (grilla1<>campo.grilla) then
				--Le saco la coma al final
				resp_final1:=substring(resp_final1,1,length(resp_final1)-1);
				resp_final1:=resp_final1||'], [';
				grilla1:=coalesce(campo.grilla,0);
			end if;
			 --Si viene nombre grilla, lo cambiamos por columna1 para que aparezca en el titulo
                        if (length(campo.nombre_grilla)>0) then
				resp_final1:=resp_final1||'{"'||fila1||'":"'||campo.fila_codigo||'##'||rpad(campo.fila_descr,30,'*')||'","'||rpad(campo.nombre_grilla,30,'*')||'":"'||campo.col_codigo||'##'||rpad(campo.col_descr,30,' ')||'","total":'||campo.total||'} ,';
			else
				resp_final1:=resp_final1||'{"'||fila1||'":"'||campo.fila_codigo||'##'||rpad(campo.fila_descr,30,'*')||'","'||rpad(columna1,30,'*')||'":"'||campo.col_codigo||'##'||rpad(campo.col_descr,30,' ')||'","total":'||campo.total||'} ,';
			end if;
		end loop;
		xml2:=logapp(xml2,query1);
		
		resp_final1:=substring(resp_final1,1,length(resp_final1)-1);
		resp_final1:=resp_final1||']]}';
	        xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK'||chr(10)||'Content-type: application/json;charset=UTF-8;'||chr(10)||'Content-length: '||length(coalesce(resp_final1,''))||chr(10)||chr(10)||coalesce(resp_final1,''));
		return xml2;
		
	else
	
	first1:=true;
	perform logfile('INDEXER F_12709 2.0 '||xml2);
	--Hago loop por el tipo de doc
	--query1:='select array_to_json(array_agg(row_to_json(rds))) from 
	query1:='select descrp_filas.codigo_grupo as fila_codigo,descrp_filas.descripcion as fila_descr,descrp_columnas.codigo_grupo as col_codigo,descrp_columnas.descripcion as col_descr,xx.total,xx.monto_total from 
		(select '||fila1||' as fila,'||columna1||' as columna,sum('||agrupador1||') as total,sum('||agrupador2||') as monto_total from 
		(select * from 
			(select * from indexer_hash where rut_emisor=$1 '||filtro1||') a join indexer_estadisticas_generica b on a.id=b.id and dia>='||fecha_ini1||' and dia<='||fecha_fin1||' and tipo_dia=$2) ff '||  
		coalesce(filtro_adicionales1,'')||' group by 1,2
		) xx join
	(select codigo,descripcion,codigo_grupo,orden from (select id_parametro from parametros where parametro='||quote_literal(parametro_fila1)||') a join detalle_parametros b on a.id_parametro=b.id_parametro) descrp_filas on descrp_filas.codigo=xx.fila::varchar join
	(select codigo,descripcion,codigo_grupo,orden from (select id_parametro from parametros where parametro='||quote_literal(parametro_columna1)||') a join detalle_parametros b on a.id_parametro=b.id_parametro) descrp_columnas on descrp_columnas.codigo=xx.columna::varchar order by 1';

	xml2:=logapp(xml2,query1);
	--Para recordar el parametro y separar las tablas
	aux1:='';
	misma_tabla1:=false;
	resp_final1:=resp_final1||'[';
	for campo in execute query1 using rut_empresa2,tipo_fecha1 loop
		xml2:=logapp(xml2,'12709: Recursion query1');
		xml2:=logapp(xml2,campo::varchar);
		--Verificamos que este en la misma tabla
		--Como estamos poniendo una tabla por tipo dte (fila), ponemos el nombre de la fila donde dice estado (columna)
		if (aux1=campo.fila_codigo::varchar) then
			resp_final1:=resp_final1||' ,{"'||fila1||'":"'||campo.fila_codigo||'##'||center_text(15,'Cantidad','*')||'","'||rpad(campo.fila_descr,52,'*')||'":"'||campo.col_codigo||'##'||rpad(campo.col_descr,52,' ')||'","total":'||campo.total||'} ,';
			resp_final1:=resp_final1||'{"'||fila1||'":"##'||center_text(30,'Total','*')||'","'||rpad(campo.fila_descr,52,'*')||'":"'||campo.col_codigo||'##'||rpad(campo.col_descr,52,' ')||'","total":'||campo.monto_total||'}';
			misma_tabla1:=true;
		else
			--Si estoy cerrando una tabla..
			--Tabla nueva
			--[{"tipo_dte":"33##Factura","estado":"1##Aprobado SII","total":1116},
			--Si pase por la misma tabla, tengo que cerrar la tabla anterior
			if (misma_tabla1) then
				resp_final1:=resp_final1||'],';
				misma_tabla1:=false;
			elsif (first1) then
				first1:=false;
			else
				resp_final1:=resp_final1||'],';
			end if;
			--XXX
			resp_final1:=resp_final1||'[{"'||fila1||'":"'||campo.fila_codigo||'##'||center_text(15,'Cantidad','*')||'","'||rpad(campo.fila_descr,52,'*')||'":"'||campo.col_codigo||'##'||rpad(campo.col_descr,52,' ')||'","total":'||campo.total||'} ,';
			resp_final1:=resp_final1||'{"'||fila1||'":"##'||center_text(30,'Total','*')||'","'||rpad(campo.fila_descr,52,'*')||'":"'||campo.col_codigo||'##'||rpad(campo.col_descr,52,' ')||'","total":'||campo.monto_total||'}';
			aux1:=campo.fila_codigo::varchar;
		end if;
		
	end loop;

	--End loop
	resp_final1:=resp_final1||']]}';
        xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK'||chr(10)||'Content-type: application/json;charset=UTF-8;'||chr(10)||'Content-length: '||length(coalesce(resp_final1,''))||chr(10)||chr(10)||coalesce(resp_final1,''));
	return xml2;
	end if;
	
	xml2:=logapp(xml2,query1);

        
	EXECUTE query1 into respuesta1 using rut_empresa2,tipo_fecha1;
	if (query2 is not null) then
		EXECUTE query2 into respuesta2 using rut_empresa2,tipo_fecha1;
		xml2:=logapp(xml2,query2);
	end if;
	if (version1<>'') then
		if (respuesta2 is not null) then
			respuesta1:='{"tipo_tabla":"'||get_campo('tipo_tabla',xml2)||'","DATA": ['||coalesce(respuesta1,'')||' ,'||coalesce(respuesta2,'')||']}';
		else
			respuesta1:='{"tipo_tabla":"'||get_campo('tipo_tabla',xml2)||'","DATA": ['||coalesce(respuesta1,'')||' ]}';
		end if;
	else
		respuesta1:='{"tipo_tabla":"'||get_campo('tipo_tabla',xml2)||'","DATA":'||coalesce(respuesta1,'[{"f":"0##","Sin Datos":"0##","total":0}]')||'}';
	end if;
        xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK'||chr(10)||'Content-type: application/json;charset=UTF-8;'||chr(10)||'Content-length: '||length(respuesta1)||chr(10)||chr(10)||respuesta1);

	--select array_to_json(array_agg(row_to_json(xx))) into respuesta1 from (select c.descripcion1 as "Tipo DTE",a."Estado",a."Total" from (select a.tipo_dte as "Tipo DTE",estado_dte_12709(b.estado) as "Estado",sum(cantidad) as "Total",sum(total) as "Monto" from (select * from indexer_hash where rut_emisor=96671750) a join indexer_estadisticas_generica b on a.id=b.id  group by 1,2) a join traza.config c on a."Tipo DTE"::varchar=c.evento where a."Estado" is not null) xx;
   return xml2;
END;
$$ LANGUAGE plpgsql;


--Recibe 3 parametros y entrega una condicion para buscar si el rut_cliente puede obtener datos de filtros
CREATE or replace FUNCTION obtiene_condicion_perfilamiento_12709(bigint,bigint,varchar) RETURNS varchar AS $$
DECLARE
	rut_empresa1	alias for $1;
	rut_cliente1	alias for $2;
	parametro1	alias for $3;
	salida1		varchar;
	stPerfil	perfil_cuadratura%ROWTYPE;
	aux1	varchar;
BEGIN
	--Si esta definido el *, no hay condicion, saco todos
	select * into stPerfil from perfil_cuadratura where rut_empresa=rut_empresa1 and rut_cliente=rut_cliente1 and parametro=parametro1 and valor='*';
	if found then
		return '';
	end if;
	aux1:=(select string_agg(quote_literal(valor),',') from perfil_cuadratura where rut_empresa=rut_empresa1 and rut_cliente=rut_cliente1 and parametro=parametro1 );
	if (length(aux1)>0 and aux1 is not null) then
		return 'AND '||parametro1||' in ('||aux1||')';
	else
		--Para que no cargue nada
		return 'AND 1=0 ';
	end if;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION procesa_parametros_cuadratura_12709(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2    varchar;
        respuesta1      varchar;
        input1  varchar;
	rut_empresa1	varchar;
	rut_empresa2	bigint;
	nombre_empresa1 varchar;
	rut_cliente1	varchar;
	rut_cliente2	bigint;
	campo	RECORD;
	campo1	RECORD;
	sql1	varchar;
	first1	boolean;
	filtro_perfil1	varchar;
	rut_empresa_login1	varchar;
	rut_empresa_login2	bigint;
	
BEGIN
    xml2:=xml1;
    rut_empresa1:=get_campo('rut_empresa',xml2);
    if (is_number(rut_empresa1) is false) then
	xml2:=logapp(xml2,'12709: Rut Empresa no numerico '||rut_empresa1);
	return xml2;
    end if;
    rut_empresa2:=rut_empresa1::bigint;

    rut_empresa_login1:=get_campo('rut_login',xml2);
    if (is_number(rut_empresa_login1) is false) then
	xml2:=logapp(xml2,'12709: Rut Empresa Login no numerico '||rut_empresa_login1);
    	rut_empresa_login2:=rut_empresa2;
    else
    	rut_empresa_login2:=rut_empresa_login1::bigint;
    end if;

    rut_cliente1:=get_campo('rut_cliente',xml2);
    if (is_number(rut_cliente1) is false) then
	xml2:=logapp(xml2,'12709: Rut Cliente no numerico '||rut_cliente1);
	return xml2;
    end if;
    rut_cliente2:=rut_cliente1::bigint;

    select name into nombre_empresa1 from recipient_traza_historico where rut = rut_empresa2;

    respuesta1:='<Cuadratura>
	<ParametrosConfiguracion>
		<UsuarioConectado>
			<RutUsr>'||rut_cliente1||'</RutUsr>
			<NombreUsr>Sin Nombre</NombreUsr>
		</UsuarioConectado>
		<Empresa>
			<NombreEmp>'||nombre_empresa1||'</NombreEmp>
			<RutEmp>'||rut_empresa1||'</RutEmp>
		</Empresa>
	</ParametrosConfiguracion>
	<ParametrosBusqueda>';


	respuesta1:=respuesta1||'<Parametros>';

	--El perfilamiento de las empresas habilitadas para el rut cliente, se basa en el rut de login
	filtro_perfil1:=obtiene_condicion_perfilamiento_12709(rut_empresa_login2,rut_cliente2,'rut_emisor');
	xml2:=logapp(xml2,'12709: Perfil '||filtro_perfil1);
	--Todos las consultas tienen TIPO_DTEy RUT_EMPRESA
	respuesta1:=respuesta1||'<Parametro><IdPar>rut_empresa</IdPar><NomPar>Rut Empresa</NomPar><Dominante>true</Dominante><Opciones>';
	--Sacamos los valores posibles del parametro en el indexer hash
	first1:=true;
	sql1:='select a.parametro,b.name as descripcion,a.orden from 
		(select distinct rut_emisor as parametro,1 as orden from indexer_hash where 1=1 '||filtro_perfil1||' and rut_emisor<>$1) a left join recipient_traza_historico b on a.parametro=b.rut union
	       select $1,(select name from recipient_traza_historico where rut=$1 limit 1),0 as orden
               order by orden,1';
	--raise notice 'sql1=%',sql1;

	for campo1 in execute sql1 using rut_empresa2 loop
		--raise notice 'campo1.parametro=% campo1.descripcion=%',campo1.parametro,campo1.descripcion;
		--El primer rut siempre es el seleccionado
		if (first1) then
			first1:=false;
			--respuesta1:=respuesta1||'<Opcion><IdOpc>campo1.parTODOS</IdOpc><Texto>TODOS</Texto></Opcion>';
			respuesta1:=respuesta1||'<Opcion><IdOpc>'||campo1.parametro||'</IdOpc><Texto>'||coalesce(campo1.descripcion,campo1.parametro::varchar)||'</Texto></Opcion>';
		else
			respuesta1:=respuesta1||'<Opcion><IdOpc>'||campo1.parametro||'</IdOpc><Texto>'||coalesce(campo1.descripcion,campo1.parametro::varchar)||'</Texto></Opcion>';
		end if;
	end loop;
	respuesta1:=respuesta1||'</Opciones></Parametro>';
	--raise notice 'respuesta1=%',respuesta1;

	--FAY 2014-10-20 se agrega la condicion de Tipo de Documento EMITIDO o RECIBIDO
	respuesta1:=respuesta1||'<Parametro><IdPar>emirec</IdPar><NomPar>Documentos</NomPar><Opciones>';
	for campo1 in select valor as parametro,valor as descripcion from perfil_cuadratura where rut_empresa=rut_empresa2 and rut_cliente=rut_cliente2 and parametro='emirec' order by 1 loop
		respuesta1:=respuesta1||'<Opcion><IdOpc>'||campo1.parametro||'</IdOpc><Texto>'||campo1.descripcion||'</Texto></Opcion>';
	end loop;
	respuesta1:=respuesta1||'</Opciones></Parametro>';

	respuesta1:=respuesta1||'<Parametro><IdPar>tipo_dte</IdPar><NomPar>Tipo DTE</NomPar><Opciones>';
	filtro_perfil1:=obtiene_condicion_perfilamiento_12709(rut_empresa2,rut_cliente2,'tipo_dte');
	xml2:=logapp(xml2,'12709: Perfil '||filtro_perfil1);
	--Sacamos los valores posibles del parametro en el indexer hash
	first1:=true;
	--raise notice 'filtro_perfil1=% rut_empresa2=%',filtro_perfil1,rut_empresa2;
	sql1:='select a.parametro::varchar,b.descripcion1 as descripcion from (select distinct tipo_dte as parametro from indexer_hash where rut_emisor=$1 '||filtro_perfil1||') a left join traza.config b on a.parametro::varchar=b.evento order by 2';
	for campo1 in execute sql1 using rut_empresa2 loop
		if (first1) then
			first1:=false;
			respuesta1:=respuesta1||'<Opcion><IdOpc>TODOS</IdOpc><Texto>TODOS</Texto></Opcion>';
		end if;
		respuesta1:=respuesta1||'<Opcion><IdOpc>'||campo1.parametro||'</IdOpc><Texto>'||campo1.descripcion||'</Texto></Opcion>';
	end loop;
	respuesta1:=respuesta1||'</Opciones></Parametro>';


	for campo in select * from filtros_rut where rut_emisor=rut_empresa2 order by parametro loop
		xml2:=logapp(xml2,'12709: campo='||campo);
		respuesta1:=respuesta1||'<Parametro><IdPar>'||campo.parametro||'</IdPar><NomPar>'||campo.alias_web||'</NomPar><Opciones>';
		filtro_perfil1:=obtiene_condicion_perfilamiento_12709(rut_empresa2,rut_cliente2,campo.parametro);
		--Sacamos los valores posibles del parametro en el indexer hash
		sql1:='select distinct '||campo.parametro||' as parametro from indexer_hash where rut_emisor=$1 '||filtro_perfil1||' order by 1';
		--xml2:=logapp(xml2,'12709: sql1='||sql1);
		--for campo1 in execute 'select distinct $1 as parametro from indexer_hash where rut_emisor=$2' using campo.parametro,rut_empresa2 loop
		first1:=true;
		for campo1 in execute sql1 using rut_empresa2 loop
			--xml2:=logapp(xml2,'12709: campo1='||campo1);
			if (first1) then
				first1:=false;
				respuesta1:=respuesta1||'<Opcion><IdOpc>TODOS</IdOpc><Texto>TODOS</Texto></Opcion>';
			end if;
			respuesta1:=respuesta1||'<Opcion><IdOpc>'||campo1.parametro||'</IdOpc><Texto>'||campo1.parametro||'</Texto></Opcion>';
		end loop;
		respuesta1:=respuesta1||'</Opciones></Parametro>';
	end loop;
	respuesta1:=respuesta1||'</Parametros></ParametrosBusqueda></Cuadratura>';

    xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK'||chr(10)||'Content-type: text/html'||chr(10)||'Content-length: '||length(respuesta1)||chr(10)||chr(10)||respuesta1);

   return xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION procesa_consulta_boletas_12709(varchar) RETURNS varchar AS $$
declare
   rut_emisor1 	integer;
   xml1        alias for $1;
   xml2    varchar;
   tipo_dte1	integer;
   folio1	integer;
   monto_total1 integer;
   fechaemi1	integer;
   uri1		varchar;
	campo	record;

BEGIN

        xml2:=xml1;
        xml2:=get_parametros_get(xml2);
	rut_emisor1:=get_campo('emisor',xml2);	
	tipo_dte1:=get_campo('tipoDte',xml2);
	folio1:=get_campo('folio',xml2);
	monto_total1:=get_campo('total',xml2);
	fechaemi1:=get_campo('fechaEmi',xml2);


	--select uri into uri1 from dte_boletas_generica where rut_emisor = rut_emisor1 and tipo_dte = tipo_dte1 and folio = folio1 and dia_emision = fechaemi1 and monto_total = monto_total1;
	perform logfile('F_12709 0');
	select * into campo from dte_boletas_generica where rut_emisor = rut_emisor1 and tipo_dte = tipo_dte1 and folio = folio1;
 	if found then
		perform logfile('F_12709 '||campo.uri);
 		if(campo.dia_emision = fechaemi1 and campo.monto_total = monto_total1) then
			perform logfile('F_12709 2 '||campo.uri);
			uri1:=campo.uri;
			xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK'||chr(10)||'Content-type: text/html'||chr(10)||'Content-length: '||length(uri1)||chr(10)||chr(10)||uri1);
		else
			perform logfile('F_12709 3 ');
			uri1:='NO DATA';
		   	xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK'||chr(10)||'Content-type: text/html'||chr(10)||'Content-length: '||length(uri1)||chr(10)||chr(10)||uri1);
		end if;
	else
	   perform logfile('F_12709 34 ');
	   uri1:='NO DATA';
	   xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK'||chr(10)||'Content-type: text/html'||chr(10)||'Content-length: '||length(uri1)||chr(10)||chr(10)||uri1);
	end if;

   return xml2;
END;
$$ LANGUAGE plpgsql;




CREATE or replace FUNCTION proc_procesa_cuadratura_12709(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2    varchar;
	input1	varchar;
	tx1	varchar;
BEGIN
    xml2:=xml1;
	
    --tipo_tx=Parametros&rut_cliente=123456&rut_empresa=111111
    --Identificamos la tx entrante
    --xml2:=get_parametros_get(xml2);
    xml2:=get_parametros(xml2);
    tx1:=get_campo('tipo_tx',xml2);
		
	
    --Determinamos la tx a procesar
    if (tx1='Parametros') then
	xml2:=logapp(xml2,'12709: Tx Parametros');
	xml2:=procesa_parametros_cuadratura_12709(xml2);
    elsif (tx1='Consulta') then
	xml2:=logapp(xml2,'12709: Tx Consulta');
	--Si viene avfolio es una busqueda avanzada
        if (length(get_campo('avFolio',xml2))>0 or length(get_campo('avRutReceptor',xml2))>0) then
		xml2:=procesa_detalle_12709(xml2);
                return xml2;
        else
		xml2:=procesa_consulta_cuadratura_12709(xml2);	
        end if;

    elsif (tx1='Export') then
	xml2:=logapp(xml2,'12709: Tx Exportar');
	xml2:=procesa_exportar_datos_12709(xml2);	
    elsif (tx1='ConsultaBoletas') then
	 xml2:=logapp(xml2,'12709: Tx ConsultaBoletas');
	xml2:=procesa_consulta_boletas_12709(xml2);
    elsif (tx1='ConsultaBoletaUsuario') then
	 xml2:=logapp(xml2,'12709: Tx ConsultaBoletaUsuario');
	xml2:=procesa_consulta_boletas_rut_12700(xml2);
    	--procesamiento de lista de contribuyentes obtenida desde SII.
    elsif(tx1='ContribuyentesElectronicos') then
         xml2:=logapp(xml2,'12709: Tx contribuyentes');
         xml2:=procesa_contribuyente(xml2);
    elsif (tx1='comienza_contribuyentes') then
         xml2:=logapp(xml2,'12709: Tx comienza proceso contribuyentes');
         xml2:=comienza_proceso_contribuyentes(xml2);
    elsif (tx1='fin_contribuyentes') then
         xml2:=logapp(xml2,'12709: Tx finaliza proceso contribuyentes');
         xml2:=fin_proceso_contribuyentes(xml2);
    elsif (tx1='total_contribuyentes') then
         xml2:=logapp(xml2,'12709: Tx total registros de contribuyentes');
         xml2:=get_total_contribuyentes(xml2);
    elsif (tx1='lista_contribuyentes') then
         xml2:=logapp(xml2,'12709: Tx listado contribuyentes');
	 xml2:=get_lista_contribuyentes(xml2);
    --elsif (tx1:='lista_RCF') then
	-- xml2:=logapp(xml2,'12709: RCOF');
        
    elsif (tx1='lista_RC') then
	 xml2:=logapp(xml2,'12709: Recepcion consolidada');
	 xml2:=get_lista_recepcion_consolidada(xml2);
    elsif (tx1='lista_RC2') then
         xml2:=logapp(xml2,'12709: Recepcion consolidada custom');
         xml2:=get_lista_recepcion_consolidada_custom(xml2);
    elsif (tx1='notificaciones_acm') then
	 --xml2:=logapp(xml2,'12709: notificaciones push para acm***********');
	 xml2:=logapp(xml2,'12709: INPUT PUSH ****');
	 xml2:=notificaciones_amazon(xml2);
    elsif (tx1='reporte_acm') then
	 xml2:=logapp(xml2,'12709: Reporte ACM');
	 xml2:=reporte_acm(xml2);
    else		
	xml2:=logapp(xml2,'12709: Tx Desconocida Cuadratura');
    end if;
    return xml2;
 
END;
$$ LANGUAGE plpgsql;
