delete from isys_querys_tx where llave='12708';

insert into isys_querys_tx values ('12708',10,1,1,'select proc_procesa_doc_recibido_sii_12708(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

--Envia a Cuadratura
--insert into isys_querys_tx values ('12708',20,1,2,'Cuadratura',4011,100,101,0,0,100,100);

--insert into isys_querys_tx values ('12708',100,1,1,'select proc_respuesta_doc_recibido_sii_12708(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);


CREATE or replace FUNCTION proc_procesa_doc_recibido_sii_12708(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
        xml2    varchar;
	stDND	traza.rut_dnd%ROWTYPE;
	rut1	varchar;
	datos1 varchar;
	rutEmisor1 varchar;
	rutEmiSDV varchar;
	tipodoc1 varchar;
	folio1 varchar;
	evento1 varchar;
	montototal1 varchar;
	fechaEmitido1 varchar;
	fechaRecepcion1 varchar;
	rutReceptor1 varchar;
	rutRecSDV varchar;
	consulta1 varchar;
	stDteRecibido	dte_recibidos%ROWTYPE;
	stDtePendiente  dte_pendientes_recibidos%ROWTYPE;
	data_hex2 varchar;
	data1 varchar;
	part1	integer;
	param1	varchar;
	respuesta1 varchar;
	status1 varchar;
	server1 varchar;
	rut_aux1	integer;
	trackid1  varchar;
	id_pend1	bigint;

	uri1	varchar;
	xml3	varchar;

	campo record;
	rut_receptor1	bigint;
	fecha_cola1	timestamp;
	nombre_tabla1	varchar;
	tx1		varchar;
	cola1	varchar;
	json_par1	json;
	json3	json;
	query1	varchar;
	id1	bigint;
BEGIN
    xml2:=xml1; 
    --POr defecto paramos la ejecucion
    xml2 := put_campo(xml2,'__SECUENCIAOK__','0');


      perform logfile_rc('Reporte_consolidado');
	--parseamos data
--	xml2:=logapp(xml2,'entrada_hex '||get_campo('INPUT',xml2));
	 if length(get_campo('INPUT',xml2))=0 then
        	data1:=get_campo('QUERY_STRING',xml2);
   	 else
        	data_hex2:=get_campo('INPUT',xml2);
        	data1:=decode(data_hex2,'hex');
   	 end if;

	--tipo_tx=ReporteConsolidado&_DOREMPRUT=85541900&_TIPDOC=33&_DORFOL=1240824&_CODEVE=EMI&_RUTREC=96919050&_CICLO=&_DORMNTNTO=&_DORMNTTOT=457817&_FCHEVE=2014-03-07 12:29:00.0
	--raise notice 'data1=(%)',ltrim(data1,' ');
    	part1 :=1;
        param1 := split_part(data1,'&',part1);
        --raise notice 'param1=(%)',param1;
        while param1 <> '' loop
             xml2 := put_campo(xml2,split_part(param1,'=',1),split_part(param1,'=',2));
             part1 := part1 + 1;
             param1 := split_part(data1,'&',part1);
        end loop;

    --Parsear datos entrante que llegan en INPUT para consultar en tabla docemitidos
	rutEmisor1 := get_campo('RUT_EMISOR',xml2);
    	rutEmiSDV :=split_part(rutEmisor1,'-',1);	
	tipodoc1 := get_campo('TIPO_DTE',xml2);
	folio1 := get_campo('FOLIO',xml2);
	montototal1 := get_campo('MONTO_TOTAL',xml2);
	fechaEmitido1 := get_campo('FECHA_EMI',xml2);
	fechaRecepcion1 := get_campo('FECHA_REC_SII',xml2);
	rutReceptor1 := get_campo('RUT_RECEPTOR',xml2);
	rutRecSDV :=split_part(rutReceptor1,'-',1);
	trackid1 := '<trackId>'||get_campo('TRACKID',xml2)||'</trackId>';
 	xml2:=logapp(xml2,'Reporte_consolidado_datos_recibidos '||rutEmisor1||' '||tipodoc1||' '||folio1||' '||montototal1||' '||fechaEmitido1||' '||fechaRecepcion1||' '||rutReceptor1);
	perform logfile_rc('Reporte_consolidado_datos_recibidos '||rutEmisor1||' - '||tipodoc1||' - '||folio1||' - '||montototal1||' - '||fechaEmitido1||' - '||fechaRecepcion1||' - '||rutReceptor1);
	--Validar si son integer
	if( is_number(rutEmiSDV) is false or is_number(rutRecSDV) is false or is_number(tipodoc1) is false or is_number(folio1) is false) then
		--xml2:=logapp(xml2,'Reporte consolidado _DOEMPRUT '||rutEmisor1);
 	        --xml2:=logapp(xml2,'Reporte consolidado _TIPDOC '||tipodoc1);
       		--xml2:=logapp(xml2,'Reporte consolidado _DORFOL '||folio1);
        	--xml2:=logapp(xml2,'Reporte consolidado _DORMNTTOT '||montototal1);
        	--xml2:=logapp(xml2,'Reporte consolidado _RUTREC '||rutReceptor1);
		--xml2:=logapp(xml2,'Falla en datos ingresados, se envia mail de error.');
		--insert into aviso_mail values (now(),0,'marcos.donoso@acepta.com','Falla Recepcion de Registro en SII Reporte Consolidado', 'Falla al procesar datos de documentos recibidos en SII, revisar datos '||'rut_emisor='||rutEmisor1||', tipodoc='|| tipodoc1||', folio='|| folio1||', rutemisor='|| rutEmisor1);
        	respuesta1:='OK graba datos en tabla pendientes';
	           status1:='Status: 200 OK'||chr(10)||
                 'Content-type: application/json;charset=UTF-8;'||chr(10)||
                 'Content-length: '||length(respuesta1)||chr(10)||chr(10)||respuesta1;
        	 xml2 := put_campo(xml2,'__SOCKET_RESPONSE__','RESPUESTA');
	         xml2:=put_campo(xml2,'RESPUESTA',status1);
		 xml2:=sp_procesa_respuesta_cola_motor(xml2);
		return xml2;
	end if;
	--xml2:=logapp(xml2,'RC rut_emisor='||rutEmiSDV||' folio='||folio1||' tipo_dte='||tipodoc1||' rut_receptor='||rutRecSDV);
	--Se normaliza el folio para q no queden ceros a la izquierda
	folio1 := folio1::bigint::varchar;
	perform logfile_rc('RC rut_emisor='||rutEmiSDV||' folio='||folio1||' tipo_dte='||tipodoc1||' rut_receptor='||rutRecSDV);
	
	--consulta si el documento esta informado en cuadratura
	--FAY-DAO2018-0419 Si el dte detectado tiene ademas la misma fecha de emision y el mismo monto, entonces podemos marcar el ASI
	select * into stDteRecibido from dte_recibidos where rut_emisor=rutEmiSDV::integer and folio=folio1::bigint and tipo_dte=tipodoc1::integer and rut_receptor=rutRecSDV::integer and fecha_emision=split_part(fechaEmitido1,' ',1) and monto_total=montototal1::bigint;
	if not found then
		--xml2:=logapp(xml2,'RC No existe en dte_recibidos');
		perform logfile_rc('RC No existe en dte_recibidos');
    		--si el documento no esta se mandan los eventos EMI e ISI a cuadratura indexer
		--antes de ir a cuadratura se ingresa el registro en dte_recibido_pendiente
		select * into stDtePendiente from dte_pendientes_recibidos where rut_emisor=rutEmisor1 and folio=folio1 and tipo_dte=tipodoc1 and rut_receptor=split_part(rutReceptor1,'-',1)::bigint;
		if found then
			xml2:=logapp(xml2,'RC dte ya registrado en dte_pendientes_recibidos');
			perform logfile_rc('RC dte ya registrado en dte_pendientes_recibidos, salgo del flujo');

			--Salgo
        		respuesta1:='DTE ya se encuentra procesado';
		        status1:='Status: 200 OK'||chr(10)||
        		         'Content-type: application/json;charset=UTF-8;'||chr(10)||
	                 'Content-length: '||length(respuesta1)||chr(10)||chr(10)||respuesta1;
        		 xml2 := put_campo(xml2,'__SOCKET_RESPONSE__','RESPUESTA');
	        	 xml2:=put_campo(xml2,'RESPUESTA',status1);
			xml2:=sp_procesa_respuesta_cola_motor(xml2);
			return xml2;
		END IF;

		--Generamos la URI del DTE REcibido
		uri1:='http://'||get_campo('DOMINIO',xml2)||to_char(now(),'YYMM')||'.acepta.com/v01/'||genera_uri2(split_part(rutEmisor1,'-',1),tipodoc1,folio1,split_part(fechaEmitido1,' ',1),montototal1,'R');
	
		--si no esta se inserta en la tabla
		xml2:=logapp(xml2,'RC se registra en dte_pendientes_recibidos');
		perform logfile_rc('RC se registra en dte_pendientes_recibidos datos= '||tipodoc1||'-'||folio1||'-'||fechaEmitido1||'-'||fechaRecepcion1||'-'||rutEmisor1||'-'||rutReceptor1||'-'||montototal1||'-'||fechaEmitido1||'-'||trackid1||'-'||fechaRecepcion1);

		--Grabo Evento de 'Intercambio Recibido con Atraso'
		xml3:='';
        	xml3:=put_campo(xml3,'FECHA_EMISION',split_part(fechaEmitido1,' ',1));
	        xml3:=put_campo(xml3,'RUT_EMISOR',split_part(rutEmisor1,'-',1));
        	xml3:=put_campo(xml3,'RUT_OWNER',split_part(rutReceptor1,'-',1));
	        xml3:=put_campo(xml3,'FECHA_EVENTO',now()::varchar);
        	xml3:=put_campo(xml3,'RUT_RECEPTOR',split_part(rutReceptor1,'-',1));
	        xml3:=put_campo(xml3,'FOLIO',folio1);
        	xml3:=put_campo(xml3,'TIPO_DTE',tipodoc1);
	        xml3:=put_campo(xml3,'CANAL','RECIBIDOS');
        	xml3:=put_campo(xml3,'URI_IN',uri1);
        	xml3:=put_campo(xml3,'FECHA_RECEPCION_SII',fechaRecepcion1::varchar);
		xml3:=put_campo(xml3,'MONTO_TOTAL',montototal1::varchar);	
	        xml3:=put_campo(xml3,'COMENTARIO_TRAZA','Documento Existe en SII y no Recibido en Acepta.'||chr(10)||'Recepción SII:'||fechaRecepcion1);
		insert into dte_pendientes_recibidos (fecha_ingreso,tipo_dte,folio,fecha_emision,fecha_recepcion_sii,dia_recepcion_sii,rut_emisor,rut_receptor,nombre_emisor,monto_total,dia,dia_emision,data_dte,uri) values (now(),tipodoc1::integer,folio1::bigint,fechaEmitido1,fechaRecepcion1,to_char(fechaRecepcion1::timestamp,'YYYYMMDD')::integer,rutEmisor1,split_part(rutReceptor1,'-',1)::bigint,(select nombre from contribuyentes where rut_emisor=rutEmiSDV::bigint),montototal1::bigint,to_char(now(),'YYYYMMDD')::integer,split_part(replace(fechaEmitido1,'-',''),' ',1)::integer,trackid1,uri1) returning id into id_pend1;
		xml3:=put_campo(xml3,'ID_PENDIENTE',id_pend1::varchar);
        	xml3:=put_campo(xml3,'EVENTO','DNR');
		xml3:=graba_bitacora(xml3,'DNR');
		xml2:=logapp(xml2,get_campo('_LOG_',xml3));
		perform logfile_rc('Graba Evento DNR URI='||uri1);
		
		--Graba un evento para la fecha SII
		xml3:=put_campo(xml3,'FECHA_EVENTO',fechaRecepcion1::varchar);
		xml3:=put_campo(xml3,'EVENTO','FSII');	
		xml3:=put_campo(xml3,'COMENTARIO_TRAZA','Se obtiene Fecha en Reporte Consolidado del SII.');
		xml3:=graba_bitacora(xml3,'FSII');
		xml2:=logapp(xml2,get_campo('_LOG_',xml3));
		 perform logfile_rc('Graba Evento FSII URI='||uri1);

		--Revisamos si existe al menos 1 regla con filtro fecha_sii e insertamos en las colas hacia el futuro
		rut_receptor1:=split_part(rutReceptor1,'-',1)::bigint;
		--FAY-DAO 2018-11-20 REcorremos todas las reglas que tengan ese filtro para evaluarlas de forma individual en el futuro
		for campo in select * from controller_detalle_regla_10k where id_cabecera in (select id from controller_cabecera_regla_10k where canal='RECIBIDOS' and rut_empresa=rut_receptor1) and filtro_xml='FECHA_RECEPCION_SII' loop
		--select * into campo from controller_detalle_regla_10k where id_cabecera in (select id from controller_cabecera_regla_10k where canal='RECIBIDOS' and rut_empresa=rut_receptor1) and filtro_xml='FECHA_RECEPCION_SII';
                        execute 'select '''||fechaRecepcion1::varchar||'''::timestamp + interval '''||campo.valor::varchar||' days''' into fecha_cola1;
                        xml2:=logapp(xml2,'FECHA_RECEPCION_SII_CONTROLLER');
                        --Insertamos para que se ejecute en el futuro
                        xml3:='';
                        xml3:=put_campo(xml3,'TX','6001');
                        xml3:=put_campo(xml3,'_ORIGEN_CONTROLLER_','FECHA_SII_RC');
                        xml3:=put_campo(xml3,'tipo_tx','valida_reglas_cabecera_controller_6001');
                        xml3:=put_campo(xml3,'URI_IN',uri1);
                        xml3:=put_campo(xml3,'CANAL','RECIBIDOS');
                        xml3:=put_campo(xml3,'RUT_EMISOR',split_part(rutEmisor1,'-',1));
                        xml3:=put_campo(xml3,'RUT_RECEPTOR',rut_receptor1::varchar);
                        xml3:=put_campo(xml3,'TIPO_DTE',tipodoc1::varchar);
                        xml3:=put_campo(xml3,'FOLIO',folio1::varchar);
			xml3:=put_campo(xml3,'ID_PENDIENTE',id_pend1::varchar);
                        xml3:=put_campo(xml3,'FECHA_INGRESO_COLA',fecha_cola1::varchar);
                        --xml3:=put_campo(xml3,'REFERENCIAS_JSON',referencias1::varchar);
                        xml3:=put_campo(xml3,'DATA_DTE',trackid1);
                        xml3:=put_campo(xml3,'MONTO_TOTAL',montototal1::varchar);
                        xml3:=put_campo(xml3,'FECHA_RECEPCION_SII',fechaRecepcion1::varchar);
                        xml3:=put_campo(xml3,'ID_REGLA_CONTROLLER',campo.id_cabecera::varchar);
                        cola1:=nextval('id_cola_procesamiento');
                        tx1:='30';
                        nombre_tabla1:='cola_motor_'||cola1::varchar;
                        query1:='insert into ' || nombre_tabla1 || ' (fecha,uri,reintentos,data,tx,rut_emisor,reproceso,categoria, nombre_cola) values ( '''||fecha_cola1||'''::timestamp,'||quote_literal(uri1)||',0,'||quote_literal(xml3)||','||tx1||','||quote_literal(split_part(rutEmisor1,'-',1)::varchar)||',''NO'',''CONTROLLER_FECHA'','|| quote_literal(nombre_tabla1) ||') returning id';
			execute query1 into id1;
                        xml2:=logapp(xml2,'Se ingresa en colas con fecha='||fecha_cola1::varchar||' para procesar regla con fecha de recepcion '||uri1||' idcola='||id1::varchar||' Regla='||campo.id_cabecera::Varchar);
		end loop;

        	respuesta1:='DTE registrado ok';
	        status1:='Status: 200 OK'||chr(10)||
        	         'Content-type: application/json;charset=UTF-8;'||chr(10)||
	                'Content-length: '||length(respuesta1)||chr(10)||chr(10)||respuesta1;
        	 xml2 := put_campo(xml2,'__SOCKET_RESPONSE__','RESPUESTA');
	       	 xml2:=put_campo(xml2,'RESPUESTA',status1);
		 xml2:=sp_procesa_respuesta_cola_motor(xml2);
		return xml2;

   ELSE
		--Si el DTE esta en el reporte consolidado, significa que esta aprobado por el SII
		--Si tenemos el DTE marcado como no aprobado, debemos generar el evento ASI
		if (coalesce(stDteRecibido.estado_sii,'') not in ('ACEPTADO_POR_EL_SII','ACEPTADO_CON_REPAROS_POR_EL_SII')) then
			xml3:='';
			xml3:=put_campo(xml3,'RUT_EMISOR',stDteRecibido.rut_emisor::varchar);
			xml3:=put_campo(xml3,'RUT_OWNER',stDteRecibido.rut_receptor::varchar);
			xml3:=put_campo(xml3,'FECHA_EMISION',stDteRecibido.fecha_emision::varchar);
			xml3:=put_campo(xml3,'RUT_RECEPTOR',stDteRecibido.rut_receptor::varchar);
			xml3:=put_campo(xml3,'FECHA_EVENTO',now()::varchar);
			xml3:=put_campo(xml3,'FOLIO',stDteRecibido.folio::varchar);
			xml3:=put_campo(xml3,'TIPO_DTE',stDteRecibido.tipo_dte::varchar);
			xml3:=put_campo(xml3,'CANAL','RECIBIDOS');
			xml3:=put_campo(xml3,'URI_IN',stDteRecibido.uri);
			xml3:=put_campo(xml3,'COMENTARIO_TRAZA','Glosa: DTE Recibido (DOK)*');
			xml3:=put_campo(xml3,'EVENTO','ASI');
			xml3:=actualiza_estado_dte(xml3);
			xml3:=graba_bitacora(xml3,'ASI');
			xml2:=logapp(xml2,get_campo('_LOG_',xml3));
		end if;
		xml2:=logapp(xml2,'RC dte ya existe en dte_recibidos '||stDteRecibido.fecha_emision||' '||stDteRecibido.monto_total::varchar);
		--Guardamos la fecha de recepcion en el sii en dte_recibidos
		rut_aux1:=split_part(rutEmisor1,'-',1);
		perform logfile_rc('RC dte ya existe en dte_recibidos, se actualiza '||rut_aux1||folio1||tipodoc1||rutReceptor1||fechaRecepcion1);
		update dte_recibidos set fecha_recepcion_sii=replace(decodifica_url(fechaRecepcion1),'+',' ')::timestamp,dia_recepcion_sii=to_char(replace(decodifica_url(fechaRecepcion1),'+',' ')::timestamp,'YYYYMMDD')::integer,fecha_ult_modificacion=now()::varchar,data_dte=coalesce(data_dte||trackid1,trackid1) where rut_emisor=rut_aux1::integer and folio=folio1::bigint and tipo_dte=tipodoc1::integer and rut_receptor=split_part(rutReceptor1,'-',1)::bigint and fecha_recepcion_sii is null;
		--si se encuentra el registro se devuelve ok, el documento ya esta informado en cuadratura
		xml2:=logapp(xml2,'Actualiza FechaSii en wf. de controller='||inserta_campo_workflow_controller(rut_aux1::integer,tipodoc1::integer,folio1::bigint,'FECHA_RECEPCION_SII',replace(decodifica_url(fechaRecepcion1),'+',' ')));
		respuesta1:='OK,no fui a cuadratura ';
        	status1:='Status: 200 OK'||chr(10)||
                	 'Content-type: application/json;charset=UTF-8;'||chr(10)||
	                 'Content-length: '||length(respuesta1)||chr(10)||chr(10)||respuesta1;
 		 xml2 := put_campo(xml2,'__SOCKET_RESPONSE__','RESPUESTA');
		 xml2:=put_campo(xml2,'RESPUESTA',status1);
   END IF;	
	xml2:=sp_procesa_respuesta_cola_motor(xml2);

   return xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION proc_respuesta_doc_recibido_sii_12708(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2    varchar;
	resp1	varchar;
	respuesta1	varchar;
	status1	varchar;
	datos1	varchar;
	rutEmiSDV varchar;
	server1 varchar;
BEGIN
    xml2:=xml1;
     --Leemos la respuesta
    resp1:=get_campo('RESPUESTA',xml2);
    if (strpos(resp1,'200 OK')>0) then

		--Pregunto si falta el evento ISI (Solo envie el EMI)
		if (get_campo('ESTADO_RECEPCION_CUADRATURA',xml2)<>'ISI') then	
	
		--verifico cliente cge	
		xml2:=logapp(xml2,'rut_receptor '||split_part(get_campo('RUT_RECEPTOR',xml2),'-',1));
		xml2:=put_campo(xml2,'RUT_CGE',split_part(get_campo('RUT_RECEPTOR',xml2),'-',1));
		xml2:=verifica_evento_cge(xml2);
		xml2:=logapp(xml2,'rut es cge--- '||get_campo('EVENTO_CGE',xml2));
		if (get_campo('EVENTO_CGE',xml2)='SI') then
                	server1:='cge-cuadindexer.acepta.com';
	        else
                	server1:='cuadraturav2.custodium.com';
        	end if;
		datos1:='_DOREMPRUT='||get_campo('RUT_RECEPTOR',xml2)||'&_RUTREC='||get_campo('RUT_EMISOR',xml2)||'&_TIPDOC='||get_campo('TIPO_DTE',xml2)||'&_DORFOL='||get_campo('FOLIO',xml2)||'&_CODEVE=ISI&_DORMNTTOT='||get_campo('MONTO_TOTAL',xml2)||'&_FCHEVE='||codifica_url(to_char(get_campo('FECHA_REC_SII',xml2)::timestamp,'DD/MM/YYYY+HH24:MI'));
        	--Se pone en duro cuadratura.
        	xml2:=put_campo(xml2,'INPUT','POST /cuadratura-indexer/ HTTP/1.1'||chr(10)||'Host: '||server1||chr(10)||'Content-Type: application/x-www-form-urlencoded'||chr(10)||'User-Agent: Apache-HttpClient/4.2.1.(java.1.5)'||chr(10)||'Content-Length: '||length(datos1)||chr(10)||chr(10)||datos1);
			xml2:=put_campo(xml2,'ESTADO_RECEPCION_CUADRATURA','ISI');
    			xml2:=put_campo(xml2,'__IP_CONEXION_CLIENTE__',server1);
			--xml2 := put_campo(xml2,'__SECUENCIAOK__','20');
			return xml2;
		end if;

		--se inserta en la tabla cuando fue exitoso el ultimo evento ISI
		rutEmiSDV:=split_part(get_campo('RUT_EMISOR',xml2),'-',1);
		insert into dte_pendientes_recibidos (fecha_ingreso,tipo_dte,folio,fecha_emision,fecha_recepcion_sii,rut_emisor,rut_receptor,nombre_emisor,monto_total,dia,dia_emision) select now(),get_campo('TIPO_DTE',xml2),get_campo('FOLIO',xml2),get_campo('FECHA_EMI',xml2),get_campo('FECHA_REC_SII',xml2),get_campo('RUT_EMISOR',xml2),split_part(get_campo('RUT_RECEPTOR',xml2),'-',1)::bigint,name,get_campo('MONTO_TOTAL',xml2),replace(get_campo('FECHA_EMI',xml2),'-','')::integer,to_char(now(),'YYYYMMDD')::integer from recipient_traza_historico where rut=rutEmiSDV::bigint limit 1;
	   respuesta1:='<b>OK</b>';
	   status1:='Status: 200 OK'||chr(10)||
                 'Content-type: text/html'||chr(10)||
                 'Content-length: '||length(respuesta1)||chr(10)||chr(10)||respuesta1;
    else
	   respuesta1:='<b>FALLA</b>';
	   status1:='Status: 400 NK'||chr(10)||
                 'Content-type: text/html'||chr(10)||
                 'Content-length: '||length(respuesta1)||chr(10)||chr(10)||respuesta1;
    end if;

	xml2:=put_campo(xml2,'RESPUESTA',status1||chr(10)||respuesta1);
	RETURN xml2;
END;
$$ LANGUAGE plpgsql;


