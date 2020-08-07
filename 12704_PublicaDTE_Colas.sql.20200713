--Publica documento
delete from isys_querys_tx where llave='112704';

--Consultamos en la base de traza si el DTE ya esta publicado
insert into isys_querys_tx values ('112704',10,1,1,'select proc_consulta_publicacion_112704(''__FLUJO_ACTUAL__[]=$$__FLUJO_ACTUAL__$$###REQUEST_METHOD[]=$$REQUEST_METHOD$$###HTTP_USER_AGENT[]=$$HTTP_USER_AGENT$$###QUERY_STRING[]=$$$QUERY_STRING$$###URI_IN[]=$$URI_IN$$###RUT_EMISOR[]=$$RUT_EMISOR$$###TIPO_DTE[]=$$TIPO_DTE$$###FOLIO[]=$$FOLIO$$###MONTO_TOTAL[]=$$MONTO_TOTAL$$###SCRIPT_NAME[]=$$SCRIPT_NAME$$###CONTENIDO[]=$$CONTENIDO$$###XML_FLAGS[]=$$XML_FLAGS$$###__FLAG_PUB_10K__[]=$$__FLAG_PUB_10K__$$###'') as __xml__',0,0,0,1,1,-1,0);

--20200110 proxy en Amazon
--Traza 2014
insert into isys_querys_tx values ('112704',2014,38,1,'select proxy_traza_amazon_112704(''__FLUJO_ACTUAL__[]=$$__FLUJO_ACTUAL__$$###URI_IN[]=$$URI_IN$$###TABLA_TRAZA[]=$$TABLA_TRAZA$$###'') as __xml__',0,0,0,1,1,-1,0);
--Traza 2015
insert into isys_querys_tx values ('112704',2015,37,1,'select proxy_traza_amazon_112704(''__FLUJO_ACTUAL__[]=$$__FLUJO_ACTUAL__$$###URI_IN[]=$$URI_IN$$###TABLA_TRAZA[]=$$TABLA_TRAZA$$###'') as __xml__',0,0,0,1,1,-1,0);
--Traza 2016
insert into isys_querys_tx values ('112704',2016,36,1,'select proxy_traza_amazon_112704(''__FLUJO_ACTUAL__[]=$$__FLUJO_ACTUAL__$$###URI_IN[]=$$URI_IN$$###TABLA_TRAZA[]=$$TABLA_TRAZA$$###'') as __xml__',0,0,0,1,1,-1,0);
--Traza 2017
insert into isys_querys_tx values ('112704',2017,33,1,'select proxy_traza_amazon_112704(''__FLUJO_ACTUAL__[]=$$__FLUJO_ACTUAL__$$###URI_IN[]=$$URI_IN$$###TABLA_TRAZA[]=$$TABLA_TRAZA$$###'') as __xml__',0,0,0,1,1,-1,0);
--Traza 2018
insert into isys_querys_tx values ('112704',2018,46,1,'select proxy_traza_amazon_112704(''__FLUJO_ACTUAL__[]=$$__FLUJO_ACTUAL__$$###URI_IN[]=$$URI_IN$$###TABLA_TRAZA[]=$$TABLA_TRAZA$$###'') as __xml__',0,0,0,1,1,-1,0);
--Traza 2019
insert into isys_querys_tx values ('112704',2019,49,1,'select proxy_traza_amazon_112704(''__FLUJO_ACTUAL__[]=$$__FLUJO_ACTUAL__$$###URI_IN[]=$$URI_IN$$###TABLA_TRAZA[]=$$TABLA_TRAZA$$###'') as __xml__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('112704',20,1,1,'select proc_consulta_publicacion_112704_2(''__FLUJO_ACTUAL__[]=$$__FLUJO_ACTUAL__$$###REQUEST_METHOD[]=$$REQUEST_METHOD$$###HTTP_USER_AGENT[]=$$HTTP_USER_AGENT$$###QUERY_STRING[]=$$$QUERY_STRING$$###URI_IN[]=$$URI_IN$$###RUT_EMISOR[]=$$RUT_EMISOR$$###TIPO_DTE[]=$$TIPO_DTE$$###FOLIO[]=$$FOLIO$$###MONTO_TOTAL[]=$$MONTO_TOTAL$$###SCRIPT_NAME[]=$$SCRIPT_NAME$$###CONTENIDO[]=$$CONTENIDO$$###XML_FLAGS[]=$$XML_FLAGS$$###__FLAG_PUB_10K__[]=$$__FLAG_PUB_10K__$$###'') as __xml__',0,0,0,1,1,-1,0);

--Sacamos el XML del DTE Emitido en caso de ser necesario
--insert into isys_querys_tx values ('112704',20,1,8,'GET XML desde Almacen',12705,0,0,1,1,40,40);
--Ejecuta la Pre-Emision en Controller
insert into isys_querys_tx values ('112704',30,45,1,'select pre_emision_controller_112704(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('112704',35,1,8, 'Firma XML flujo 13795',13795,0,0,1,1,0,0);

--Llamamos a Escribir Directo
insert into isys_querys_tx values ('112704',40,19,1,'select proc_prepara_graba_directo_almacen_colas_112704(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

--Llamada al flujo 8015 idioma motor (Tipo 7= Respuesta Motor)
insert into isys_querys_tx values ('112704',56,1,2,'Llamada a Escribir en Almacen',9017,104,200,0,0,66,66);
--Se eejecuta en la base de colas
insert into isys_querys_tx values ('112704',66,19,1,'select proc_respuesta_almacen_112704_3(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
--Se eejecuta en la base principal
insert into isys_querys_tx values ('112704',70,1,1,'select graba_estado_publicacion_traza_112704(''__FLUJO_ACTUAL__[]=$$__FLUJO_ACTUAL__$$###$$XML3$$'') as __xml__',0,0,0,1,1,-1,0);


CREATE or replace FUNCTION pre_emision_controller_112704(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2    varchar;
	xml_c	varchar;
        xml_referencias                 varchar;
        v_rut_emisor                    varchar;

        v_rut_receptor                  varchar;
        v_nro_cuenta                    varchar;
        v_fecha_emision                 varchar;
        v_id_referencia_facturacion     integer;
        v_periodo1                      varchar;
        nuevas_referencias              varchar;
        record1                         record;
	aux1	varchar;
BEGIN
	xml2:=xml1;
	xml_c := reglas.parseo_datos(xml2);
	--Seteo el canal para las reglas de PRE-EMISION
	xml2 := logapp(xml2,'CONTROLLER Pre-Emision');
	xml_c := put_campo(xml_c,'_ORIGEN_CONTROLLER_','PRE-EMISION');
	xml_c := put_campo(xml_c,'CANAL','PRE-EMISION');
	xml_c:=valida_reglas_cabecera_controller(xml_c);
	xml2 := logapp(xml2,get_campo('_LOG_',xml_c));
        xml2 := put_campo(xml2, 'RUT_USUARIO_ACCION', get_campo('RUT_USUARIO_ACCION', xml_c));
	xml2:=logapp(xml2,'RUT_USUARIO_ACCION='||get_campo('RUT_USUARIO_ACCION', xml2));
	--Si se aplico alguna regla de retencion, aca es donde ponemos el documento a futuro y lo dejamos en las colas
	--if get_campo('_CONTROLLER_RES_',xml_c)='APLICA' and get_campo('_CONTROLLER_ACCION_',xml_c)='retiene' then
	if get_campo('_CONTROLLER_RES_',xml_c)='APLICA' and get_campo('_CONTROLLER_ACCION_',xml_c)='retiene' and get_campo('__FLAG_REFERENCIAS_PRE_EMISION__',xml_c)<>'SI' then
                -- AVC 20180607
		--DAO-FAY 20200203 Se agrega parametro generico en datos_dte_adicionales para aplicar con clientes sin ciclo
		aux1:=get_xml('_PARAMETRO_BUSQUEDA_PRE_EMISION_',get_campo('DATA_DTE',xml_c));
                if get_campo('MANDATO_CICLO', xml_c) != '' or aux1!='' then
--                	xml2 := logapp(xml2, 'MANDATO_CICLO: "' || get_campo('MANDATO_CICLO', xml_c) || '"');
                        xml2 := logapp(xml2, 'Documento con ciclo-->'||get_campo('MANDATO_CICLO', xml_c)||' o _PARAMETRO_BUSQUEDA_PRE_EMISION_='||aux1);
                        v_rut_emisor := get_campo('RUT_EMISOR_DV', xml_c);
                        xml2 := logapp(xml2, 'rut_emisor: ' || v_rut_emisor);

                        v_rut_receptor := get_campo('RUT_RECEPTOR_DV', xml_c);
                        xml2 := logapp(xml2, 'rut_receptor: ' || v_rut_receptor);

			--DAO-FAY 20200203
			if aux1!='' then
				v_nro_cuenta := aux1;
			else
				--ENTEL
                        	v_nro_cuenta := get_campo('NRO_CUENTA', xml_c);
			end if;
                        xml2 := logapp(xml2, 'nro_cuenta: ' || v_nro_cuenta);

                        v_fecha_emision := get_campo('FECHA_EMISION', xml_c);
                        v_periodo1 := split_part(v_fecha_emision, '-', 1) || split_part(v_fecha_emision, '-', 2);
                        xml2 := logapp(xml2, 'periodo: ' || v_periodo1);
                        
			xml2 := put_campo(xml2, 'RUT_USUARIO_ACCION', get_campo('RUT_USUARIO_ACCION', xml_c));
			xml2 := logapp(xml2, 'RUT_USUARIO_ACCION-->>>>>'|| get_campo('RUT_USUARIO_ACCION',xml2));

                        select id_referencia_facturacion into v_id_referencia_facturacion
                        from referencia_facturacion
                        where rut_emisor=v_rut_emisor and rut_receptor=v_rut_receptor and nro_cuenta=v_nro_cuenta and periodo=v_periodo1 limit 1;

                        if found then
                                xml2 := logapp(xml2, 'Tiene referencias cargadas');
                                xml_referencias := '';
                               --AVC 20181002 for record1 in select tipo_ref, folio_ref, fecha_ref from detalle_referencia_facturacion where id_referencia_facturacion = v_id_refencia_facturacion loop
				for record1 in select tipo_ref, folio_ref, fecha_ref, razon_ref from detalle_referencia_facturacion where id_referencia_facturacion = v_id_referencia_facturacion loop
                                        xml_referencias := xml_referencias || '<Referencia><NroLinRef></NroLinRef><TpoDocRef>' || record1.tipo_ref
                                        ||'</TpoDocRef><FolioRef>' || record1.folio_ref || '</FolioRef>';
                                        if(record1.fecha_ref = '') then
                                                xml_referencias := xml_referencias || '<FchRef>' || v_fecha_emision || '</FchRef>';
                                        else
                                                xml_referencias := xml_referencias || '<FchRef>' || record1.fecha_ref || '</FchRef>';
                                        end if;
					--AVC 20180927
                                        if(record1.razon_ref = '') then
                                                 xml_referencias := xml_referencias || '<RazonRef/>';
                                        else
                                                xml_referencias := xml_referencias || '<RazonRef>' || record1.razon_ref || '</RazonRef>';
					end if;
	                                xml_referencias := xml_referencias || '</Referencia>';
                                end loop;
                                xml2 := logapp(xml2, 'Referencias a agregar: ' || xml_referencias);
                                xml2 := put_campo(xml2, 'ADD_REFERENCIAS', xml_referencias);
				--xml2 := put_campo(xml2, 'INPUT_REF',get_campo('INPUT',xml2));
                                xml2 := put_campo(xml2, '__SECUENCIAOK__', '35');
                        else
                                xml2 := logapp(xml2, 'No tiene referencias cargadas, se emite el documento');
                                xml2 := put_campo(xml2,'__SECUENCIAOK__', '40');
                        end if;
                else
			 -- 20180806 AVC
                        xml2 := put_campo(xml2, 'RUT_USUARIO_ACCION', get_campo('RUT_USUARIO_ACCION', xml_c));
                        -- AVC FIN
			xml2 := logapp(xml2,'CONTROLLER Pre-Emision se retiene el documento');
			xml2:=put_campo(xml2,'__FECHA_FUTURO_COLA__',get_campo('FECHA_RETENCION_CONTROLLER',xml_c));
			--Seteamos que ponga el dte a futuro
			xml2 := put_campo(xml2,'__RETIENE_DTE__','SI');
			--Marcamos el xml_flags para evitar que se retenga nuevamente
			xml2 := put_campo(xml2,'MENSAJE_XML_FLAGS','RETENIDO-'||get_campo('FECHA_INGRESO_COLA',xml2));
                        xml2 := put_campo(xml2, '__SECUENCIAOK__', '0');
                end if;
        else
                xml2 := put_campo(xml2,'__SECUENCIAOK__', '40');
        end if;

        return xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION proc_consulta_publicacion_112704(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
        xml2    varchar;
    data1       varchar;
    file1       varchar;
    sts         integer;
    host1       varchar;
    header1     varchar;
   largo1       integer;
    pos_final1 integer;
    pos_inicial1 integer;
    dominio1 varchar;
fecha1  varchar;
directorio1 varchar;
tabla_traza1    varchar;
uri1    varchar;
stTraza traza.traza%ROWTYPE;
	rut_emisor1	bigint;
	tipo_dte1	bigint;
	folio1		bigint;
	monto1		bigint;
	campo		RECORD;
	i1	integer;
	xml3	varchar;
	json_aux1	json;
	rut1		varchar;
BEGIN
    xml2:=xml1;
    xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
    xml2:=logapp(xml2,'URI_IN='||get_campo('URI_IN',xml2));

    --Si es un get salgo altiro
    if (get_campo('REQUEST_METHOD',xml2)='GET') then
        if ((strpos(get_campo('HTTP_USER_AGENT',xml2),'check_http')>0) or (length(get_campo('QUERY_STRING',xml2))=0)) then
                xml2 := logapp(xml2,'Nagios Check o GET sin datos, se ignora');
                --Se Responde OK
                xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK'||chr(10)||
                'Content-type: text/html; charset=iso-8859-1'||chr(10)||
                'Content-length: 0'||chr(10)||
                'Vary: Accept-Encoding'||chr(10)||chr(10));
        	xml2 := put_campo(xml2,'__PUBLICADO_OK__','FALLA');
                --xml2 := sp_procesa_respuesta_cola_motor(xml2);
                --xml2 := put_campo_ctx(xml2,'__ETAPA1__','OK');
                return xml2;
        end if;
    end if;
    uri1:=get_campo('URI_IN',xml2);

    --20150224 FAY Si no viene URI no se puede publicar
    if (length(uri1)=0) then
        xml2 := logapp(xml2,'No viene URI_IN, no se puede publicar');
        xml2 := put_campo(xml2,'__PUBLICADO_OK__','FALLA');
        return xml2;
    end if;


    --FAY 2015-03-26 Solo para EMITIDOS
    if (strpos(uri1,'?k=')>0) then
	    fecha1:=get_fecha_uri(uri1);
	    --Verifico sie debo buscar en trazas antiguas
            tabla_traza1:=get_tabla_traza(uri1);
	    select * into campo from config_tabla_traza where periodo_desde<=fecha1::integer and periodo_hasta>=fecha1::integer;
	    --Si debo hacer lo mismo
	    if (found and campo.parametro is null) then
 	            --Si ya tiene el evento PUB en traza, no se publica
	            begin
        	       execute 'select * from '||tabla_traza1||' where uri=$1 and evento=''PUB''' into stTraza using uri1;
	               --Si no esta el evento..
        	       if stTraza.uri is not null then
                	        xml2 := put_campo(xml2,'__PUBLICADO_OK__','SI');
                        	xml2 := logapp(xml2,'Uri '||uri1||' ya publicado');
	                        return xml2;
        	       end if;
	            exception WHEN OTHERS THEN
        	       select * into stTraza from traza.traza where uri=uri1 and evento='PUB';
	               if found then
        	                --Si no esta el evento PUB vamos a publicar
                        	xml2 := put_campo(xml2,'__PUBLICADO_OK__','SI');
                	        xml2 := logapp(xml2,'Uri '||uri1||' ya publicado*');
                        	return xml2;
	               end if;
        	    end;
	     --Si es de una traza antigua >=2014
	     elsif is_number(split_part(campo.parametro,'_',2)) then
		xml2:=put_campo(xml2,'TABLA_TRAZA',tabla_traza1);
		xml2:=put_campo(xml2,'__SECUENCIAOK__',split_part(campo.parametro,'_',2));
		xml2 := logapp(xml2,'URI='||uri1||' Valida proxy en Amazon');
		return xml2;	
	     else
		xml2 := logapp(xml2,'lee_traza_evento');
		json_aux1:=lee_traza_evento(uri1,'PUB');
                if (get_json('status',json_aux1)='OK') then
                --if (json_aux1::varchar<>'{}') then
                       xml2 := put_campo(xml2,'__PUBLICADO_OK__','SI');
                       xml2 := logapp(xml2,'Uri '||uri1||' ya publicado');
                       return xml2;
                end if;
	     end if;
    end if;

    return proc_consulta_publicacion_112704_2(xml2);
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION proc_consulta_publicacion_112704_2(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
        xml2    varchar;
    data1       varchar;
    file1       varchar;
    sts         integer;
    host1       varchar;
    header1     varchar;
   largo1       integer;
    pos_final1 integer;
    pos_inicial1 integer;
    dominio1 varchar;
fecha1  varchar;
directorio1 varchar;
tabla_traza1    varchar;
uri1    varchar;
stTraza traza.traza%ROWTYPE;
        rut_emisor1     bigint;
        tipo_dte1       bigint;
        folio1          bigint;
        monto1          bigint;
        campo           RECORD;
        i1      integer;
        xml3    varchar;
        json_aux1       json;
        rut1            varchar;
BEGIN
    xml2:=xml1;
    uri1:=get_campo('URI_IN',xml2);
    --Verifico que sea un DTE recibido y el rut Emisor sea de ACEPTA, entonces publicamos con el custodium document
    --Del dte emitido
    --i1:=(select i from flag_tmp_fay limit 1);
    --Solo para los DTE 
    xml2 := put_campo(xml2,'__FLAG_CLIENTE_COMUNIDAD__','');
    if (get_campo('SCRIPT_NAME',xml2) in ('/ca4/ca4rec','/ca4/recmotor') and get_campo('CONTENIDO',xml2)='DTE')  then
		--Solo para CGE
		--xml2:=put_campo(xml2,'RUT_CGE',get_campo('RUT_EMISOR',xml2));
		--xml2:=verifica_evento_cge(xml2);
		--if (get_campo('EVENTO_CGE',xml2)='SI') then
			rut_emisor1:=get_campo_bigint('RUT_EMISOR',xml2);
			tipo_dte1:=get_campo_bigint('TIPO_DTE',xml2);	
			folio1:=get_campo_bigint('FOLIO',xml2);
			monto1:=get_campo_bigint('MONTO_TOTAL',xml2);
			xml2 := logapp(xml2,'COMUNIDAD= busca DTE ');
			--Que exista el DTE en dte_emitidos
			select * into campo from dte_emitidos where rut_emisor=rut_emisor1 and tipo_dte=tipo_dte1 and folio=folio1 and monto_total=monto1;
			if found then
				xml2 := logapp(xml2,'COMUNIDAD=DTE Recibido pertenece a la Comunidad y es el mismo URI_REC='||get_campo('URI_IN',xml2)||' URI_EMITIDA='||campo.uri);
				--Se usa el mismo DTE emitido
				xml2 := put_campo(xml2,'URI_IN_RECIBIDO',get_campo('URI_IN',xml2));
				xml2 := put_campo(xml2,'URI_IN',campo.uri);
				if (get_fecha_uri(uri1)::integer>=1701) then
    					xml2:=get_parametros_motor(xml2,'PUBLICADOR_2017');
				else
    					xml2:=get_parametros_motor(xml2,'PUBLICADOR');
				end if;
				--xml2 := put_campo(xml2,'__SECUENCIAOK__','20');
				--Verificamos que tengo que llenar el campo XML_ALMACEN para la secuencia 40
				xml2 := put_campo(xml2,'__FLAG_CLIENTE_COMUNIDAD__','SI');
				--Se guarda la uri en caso de reproceso
				insert into dte_recibido_comunidad (fecha,uri_emitido,uri) values (now(),campo.uri,get_campo('URI_IN_RECIBIDO',xml2));
				xml2 := put_campo(xml2,'__SECUENCIAOK__','40');
				return xml2;
			else
				 xml2 := logapp(xml2,'COMUNIDAD= Dte no esta en emitidos rut_emisor='||rut_emisor1::varchar||' tipo_dte='||tipo_dte1::varchar||' folio='||folio1::varchar||' monto_total='||monto1::varchar);
			end if;
		--end if;
		--xml2:=put_campo(xml2,'EVENTO_CGE','');
    end if;

    --Saco los parametros del publicador para usarlos posteriormente.
    xml2:=logapp(xml2,'DTE no publicado (o Recibido) URI_IN='||get_campo('URI_IN',xml2));
    if (get_fecha_uri(uri1)::integer>=1701) then
    	xml2:=get_parametros_motor(xml2,'PUBLICADOR_2017');
    else
    	xml2:=get_parametros_motor(xml2,'PUBLICADOR');
    end if;

    xml2 := put_campo(xml2,'__SECUENCIAOK__','40');

    --FAY-DAO 2018-03-12 Solo para Emitidos que no esten retenido previamente
    --FAY-DAO 20200422 si la emision es por escritorio, no se aplican las reglas de PRE-EMISION
    if (strpos(uri1,'?k=')>0 and strpos(get_campo('XML_FLAGS',xml2),'RETENIDO-')=0 and get_campo('__FLAG_PUB_10K__',xml2)<>'SI') then
	--Si tiene el rut controller pre-emision se valida aca
	rut1:=get_campo('RUT_EMISOR',xml2);
	--DAO 20190712 Agregamos que el Tipo Dte sea numerico
	if is_number(rut1) and is_number(get_campo('TIPO_DTE',xml2))then
		select * into campo from controller_cabecera_regla_10k where rut_empresa=rut1::integer and canal='PRE-EMISION';
		if found then
			xml2:=logapp(xml2,'Vamos a Revisar Controller PRE-EMISION '||uri1);
			xml2 := put_campo(xml2,'__SECUENCIAOK__','30');
		end if;
	end if;
    end if; 

    return xml2;
END;
$$ LANGUAGE plpgsql;





CREATE or replace FUNCTION proc_prepara_graba_directo_almacen_colas_112704(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
        xml2    varchar;
    data1       varchar;
    file1       varchar;
    sts         integer;
    host1       varchar;
    header1     varchar;
   largo1	integer;
    pos_final1 integer;	
    pos_inicial1 integer;
    dominio1 varchar;
fecha1	varchar;
directorio1 varchar;
tabla_traza1	varchar;
uri1	varchar;
stTraza	traza.traza%ROWTYPE;
	xml3	varchar;
	campo record;
    
BEGIN
    xml2:=xml1;

    --Vengo a la base de las colas a borrar de la cola
    if (get_campo('__PUBLICADO_OK__',xml2) in ('SI','FALLA')) then
	xml2 := logapp(xml2,'DTE Publicado Ok en SI o FALLA, se borra DTE');
	xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
	xml2 := sp_procesa_respuesta_cola_motor_original(xml2);
	return xml2;
    end if;

    --RME 20181023 solo para pruebas escribo LOG
   xml2 := logapp(xml2,'FLAG RME---->'|| get_campo('REFERENCIA_OK',xml2));	
   if (get_campo('REFERENCIA_OK',xml2)='SI') then
	xml2 := logapp(xml2,'INPUT REFERENCIA-->'||get_campo('INPUT',xml2));
   end if;
  


    if (get_campo('REFERENCIA_OK',xml2)='FALLA') then	
	xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
	xml2 := put_campo(xml2,'RESPUESTA','');  --Para forzar aumento de reintentos.
        xml2 := sp_procesa_respuesta_cola_motor_original(xml2);
    end if;
    --Revisamos si encontramos el DTE emitido y le cambiamos al URI por el recibido
    if (get_campo('__FLAG_CLIENTE_COMUNIDAD__',xml2)='SI') then
	--Si no lee que reintente
	--if (get_campo('FALLA_CUSTODIUM',xml2)='SI') then
	--	xml2 := logapp(xml2,'COMUNIDAD= Falla lectura de DTE del almacen');
--		xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
--		xml2 := sp_procesa_respuesta_cola_motor_original(xml2);
--		return xml2;
--	end if;
  	data1:=get_input_almacen('{"uri":"'||get_campo('URI_IN',xml2)||'"}');
	xml2 := logapp(xml2,'COMUNIDAD Busco URI='||get_campo('URI_IN',xml2));
	--Si no contesta o no es un CUSTODIUM DOCUMENT ...Fallamos
	if (length(data1)=0 or strpos(data1,encode('</Content>','hex'))=0) then				
		xml2 := logapp(xml2,'Falla lectura de DTE '||get_campo('URI_IN',xml2)||' en get_input_almacen');
		xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
		xml2 := put_campo(xml2,'__PUBLICADO_OK__','FALLA');
		xml2 := sp_procesa_respuesta_cola_motor_original(xml2);
		return xml2;
	end if;
	
	--Tenemos que guardar el Custodium 
    	    uri1:=get_campo('URI_IN_RECIBIDO',xml2);
	    --data1:=get_campo('XML_ALMACEN',xml2);
	    xml2:=put_campo(xml2,'INPUT',data1);
	    xml2:=put_campo(xml2,'URI_IN',uri1);
	    --xml2:=put_campo(xml2,'XML_ALMACEN','');
	    largo1:=length(data1);
	    --xml3:=replace(get_campo('__XML_PUBLICADOR__',xml2),'&&&','###');
	    --xml2:=put_campo(xml2,'__IP_PORT_CLIENTE__',get_campo('__IP_PORT_CLIENTE__',xml3));
	    --xml2:=put_campo(xml2,'__IP_CONEXION_CLIENTE__',get_campo('__IP_CONEXION_CLIENTE__',xml3));
	    --xml2:=put_campo(xml2,'PARAMETRO_RUTA',get_campo('PARAMETRO_RUTA',xml3));
	    xml2 := logapp(xml2,'COMUNIDAD');
    else
    	    uri1:=get_campo('URI_IN',xml2);
	    --Solo windte por ahora
	    --Ya se parseo el DTE
	    data1:=get_campo('INPUT',xml2);
	    --Si no viene el CONTENT_LENGTH ..
	    if (get_campo('CONTENT_LENGTH',xml2)='') then
		xml2:=put_campo(xml2,'CONTENT_LENGTH',(length(data1)/2)::varchar);
		xml2:=put_campo(xml2,'ERROR_CONTENT_LENGTH','No venia en Datos');
	    end if;
    	   largo1:=get_campo('CONTENT_LENGTH',xml2)::integer*2;
    end if;

    host1:=split_part(split_part(uri1,'//',2),'/',1);
 
    --if (strpos(host1,'windte')=0) then
	--xml2:=put_campo(xml2,'__SECUENCIAOK__','0');
	--return xml2;
    --end if;
    xml2 := put_campo(xml2,'TX','8015'); 
    --xml2 := put_campo(xml2,'INPUT','3c3f786d6c2076657273696f6e3d'||split_part(split_part(get_campo('INPUT',xml2),'3c3f786d6c2076657273696f6e3d',2),'2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d',1));
 

    --Nuevo Procedimiento
    --largo1:=get_campo('CONTENT_LENGTH',xml2)::integer*2;
    --Busco donde empieza <?xml version
    pos_inicial1:=strpos(data1,'3c3f786d6c2076657273696f6e3d');
    --Buscamos al reves donde esta el primer signo > que en hex es 3e
    --Como se pone un reverse se busca e3
    --xml2:=logapp(xml2,'MAGIA:'||strpos(reverse(data1),'e3')::varchar);
    pos_final1:=largo1-pos_inicial1+4-strpos(reverse(data1),'e3');
    data1:=substring(data1,pos_inicial1,pos_final1);
    xml2 := put_campo(xml2,'INPUT_CUSTODIUM',data1);
    xml2 := put_campo(xml2,'LEN_INPUT_CUSTODIUM',length(data1)::varchar);
    --xml2 := put_campo(xml2,'INPUT',data1);
    xml2 := logapp(xml2,'Largo XML_ALMACEN largo1='||largo1::varchar||' LEN_INPUT_CUSTODIUM='||length(data1)::varchar);

    --TODO DTE PUBLICADO SE VA AL S3
    xml2:=graba_documento_s3(xml2);

    --Si son DTE importados, solo van al S3 y no al almacen
    if (get_campo('SCRIPT_NAME',xml2)='/ca4/ca4importer') or (get_campo('SCRIPT_NAME',xml2)='/ca4/ca4importer_rec') then
                --Si no esta el evento PUB vamos a publicar
                xml2 := put_campo(xml2,'__SECUENCIAOK__','70');
                xml2 := put_campo(xml2,'__PUBLICADO_OK__','SI');
                xml2 := logapp(xml2,'Uri '||uri1||' importado va solo al S3');

		--SAcamos el input para no hacer viajar la data hacia la base principal
		xml3:=xml2;
		xml3:=put_campo(xml3,'INPUT','');
		xml3:=put_campo(xml3,'INPUT_CUSTODIUM','');
		xml3:=put_campo(xml3,'_LOG_','');
		--Insertamos el XML3 en el xml cambiando los ###
                xml2:=put_campo(xml2,'XML3',replace(xml3,'###','&&&'));
                return xml2;
    end if;



    dominio1:=split_part(split_part(uri1,'//',2),'.',1);
    --Los ultimo 4 del dominio1
    if length(dominio1)>4 then
    	fecha1:=substring(dominio1,length(dominio1)-3,4);
        --xml2:=logapp(xml2,'fecha1='||fecha1);
        dominio1:=lower(substring(dominio1,1,length(dominio1)-4));

        --xml2:=logapp(xml2,'dominio1='||dominio1);
        file1:=split_part(uri1,'/',5);
        --xml2:=logapp(xml2,'file1='||file1);
        directorio1:=substring(file1,1,2);
        --xml2:=logapp(xml2,'directorio1='||directorio1);
        file1:=split_part(substring(file1,3,length(file1)),'?',1);
        --xml2:=logapp(xml2,'file1='||file1);
    else
	--2015-03-17FAY para los DTE que vienen sin dominio se graban en cola_motor_sin_uri
       	xml2:=logapp(xml2,'Almacen: DTE sin dominio '||dominio1||' Se graba en cola_motor_sin_uri');
	xml2:=put_campo(xml2,'URI_IN','');
       	xml2 := put_campo(xml2,'__PUBLICADO_OK__','NO');
	return xml2;	
    end if;


    --xml2:=get_parametros_motor(xml2,'PUBLICADOR');
    --MDA 2014-09-11 Se cambia storage almacen por mas espacio
    --xml2:=put_campo(xml2,'ALMACEN','/opt/acepta/custodium/depot/'||dominio1||'/'||fecha1||'/'||directorio1||'/'||file1);
    --xml2:=put_campo(xml2,'ALMACEN',get_campo('PARAMETRO_RUTA',xml2)||dominio1||'/'||fecha1||'/'||directorio1||'/'||file1);
    --xml2:=put_campo(xml2,'__IP_CONEXION_CLIENTE__','172.16.10.131');

    xml2 := put_campo(xml2,'_STS_FILE_','');
    --Borro el certificado que no se usa 
    xml2 := put_campo(xml2,'SSL_SERVER_CERT','');


	--select * into campo from tmp6 limit 1;
	--if found then
    	--	xml2:=put_campo(xml2,'ALMACEN',get_campo('PARAMETRO_RUTA',xml2)||dominio1||'/'||fecha1||'/'||directorio1||'/'||file1);
		--Armo paquete para ir directo al ProcesadorAlmacen
	--	xml2:=put_campo(xml2,'INPUT_CUSTODIUM','02'||encode('<TX=4>9015<INPUT='::bytea,'hex')||encode(((get_campo('LEN_INPUT_CUSTODIUM',xml2)::integer/2)::varchar||'>')::bytea,'hex')||get_campo('INPUT_CUSTODIUM',xml2)||encode(('<ALMACEN='||length(get_campo('ALMACEN',xml2))::varchar||'>'||get_campo('ALMACEN',xml2))::bytea,'hex')||'03');
	--else
	--	insert into tmp6 values (uri1);
    		xml2:=put_campo(xml2,'ALMACEN',get_campo('PARAMETRO_RUTA',xml2)||dominio1||'/'||fecha1||'/'||directorio1||'/'||file1||'.gz');
		data1:=gzip_string_hex(get_campo('INPUT_CUSTODIUM',xml2));
		xml2:=put_campo(xml2,'INPUT_CUSTODIUM','02'||encode('<TX=4>9016<INPUT='::bytea,'hex')||encode((length(data1)::varchar||'>')::bytea,'hex')||encode(data1::bytea,'hex')||encode(('<ALMACEN='||length(get_campo('ALMACEN',xml2))::varchar||'>'||get_campo('ALMACEN',xml2))::bytea,'hex')||'03');
	--end if;
        xml2:=logapp(xml2,'Almacen '||get_campo('ALMACEN',xml2));
        xml2:=logapp(xml2,'Almacen: Envia Data Directo '||get_campo('__IP_CONEXION_CLIENTE__',xml2)||' '||uri1);

	if (get_campo('__FLAG_CLIENTE_COMUNIDAD__',xml2)='SI') then
		xml2:=logapp(xml2,'COMUNIDAD='||get_campo('__IP_CONEXION_CLIENTE__',xml2)||' '||get_campo('__IP_PORT_CLIENTE__',xml2));
	end if;
	xml2:=put_campo(xml2,'__SECUENCIAOK__','56');
    --end if;

	--Limpia el INPUT_CUSTODIUM para que no viaje a la 132 
    --end if;

    return xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION proc_respuesta_almacen_112704_3(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2        varchar;
        data1   varchar;
	sts1	varchar;
	xml3	varchar;
BEGIN
        --Cambio la respuesta de cuadratura por la respuesta original
        xml2:=xml1;
	--Limpio INPUT_CUSTODIUM para que no viaje a la 132
	xml2:=put_campo(xml2,'INPUT_CUSTODIUM','');
        xml2:=logapp(xml2,'RESPUESTA_CUSTODIUM='||get_campo('_STS_FILE_',xml2));
	if (get_campo('_STS_FILE_',xml2)='FILE_YA_EXISTE') then
		xml2 := logapp(xml2,'Almacen:File ya existe en Almacen');	
        	xml2 := put_campo(xml2,'__SECUENCIAOK__','70');
		--SAcamos el input para no hacer viajar la data hacia la base principal
		xml3:=xml2;
		xml3:=put_campo(xml3,'INPUT','');
		xml3:=put_campo(xml3,'INPUT_CUSTODIUM','');
		xml3:=put_campo(xml3,'XML_CUSTODIUM','');
		xml3:=put_campo(xml3,'_LOG_','');
		xml3:=put_campo(xml3,'FCGI_ROLE','');
		xml3:=put_campo(xml3,'SSL_VERSION_INTERFACE','');
		xml3:=put_campo(xml3,'SSL_VERSION_LIBRARY','');
		xml3:=put_campo(xml3,'SSL_PROTOCOL','');
		xml3:=put_campo(xml3,'SSL_CIPHER_ALGKEYSIZE','');
		xml3:=put_campo(xml3,'SSL_CIPHER_USEKEYSIZE','');
		xml3:=put_campo(xml3,'SSL_SERVER_M_SERIAL','');
		xml3:=put_campo(xml3,'SSL_SERVER_V_START','');
		xml3:=put_campo(xml3,'SSL_SERVER_V_END','');
		xml3:=put_campo(xml3,'SSL_SERVER_S_DN','');
		xml3:=put_campo(xml3,'SSL_SERVER_S_DN_OU','');
		xml3:=put_campo(xml3,'SSL_SERVER_S_DN_CN','');
		xml3:=put_campo(xml3,'SSL_SERVER_I_DN','');
		xml3:=put_campo(xml3,'SSL_SERVER_I_DN_OU','');
		xml3:=put_campo(xml3,'SSL_SERVER_A_SIG','');
		xml3:=put_campo(xml3,'SSL_SESSION_ID','');
		xml3:=put_campo(xml3,'SERVER_SIGNATURE','');
		xml3:=put_campo(xml3,'SERVER_SOFTWARE','');
		xml3:=put_campo(xml3,'CONTENT_TYPE','');
		xml3:=put_campo(xml3,'SCRIPT_FILENAME','');
		xml3:=put_campo(xml3,'__FLUJO_ACTUAL__','');
		--Insertamos el XML3 en el xml cambiando los ###
		xml2:=put_campo(xml2,'XML3',replace(xml3,'###','&&&'));
		
	elsif (get_campo('_STS_FILE_',xml2)='OK') then
        	xml2 := put_campo(xml2,'__SECUENCIAOK__','70');
		--SAcamos el input para no hacer viajar la data hacia la base principal
		xml3:=xml2;
		xml3:=put_campo(xml3,'INPUT','');
		xml3:=put_campo(xml3,'INPUT_CUSTODIUM','');
		xml3:=put_campo(xml3,'XML_CUSTODIUM','');
		xml3:=put_campo(xml3,'_LOG_','');
		xml3:=put_campo(xml3,'FCGI_ROLE','');
		xml3:=put_campo(xml3,'SSL_VERSION_INTERFACE','');
		xml3:=put_campo(xml3,'SSL_VERSION_LIBRARY','');
		xml3:=put_campo(xml3,'SSL_PROTOCOL','');
		xml3:=put_campo(xml3,'SSL_CIPHER_ALGKEYSIZE','');
		xml3:=put_campo(xml3,'SSL_CIPHER_USEKEYSIZE','');
		xml3:=put_campo(xml3,'SSL_SERVER_M_SERIAL','');
		xml3:=put_campo(xml3,'SSL_SERVER_V_START','');
		xml3:=put_campo(xml3,'SSL_SERVER_V_END','');
		xml3:=put_campo(xml3,'SSL_SERVER_S_DN','');
		xml3:=put_campo(xml3,'SSL_SERVER_S_DN_OU','');
		xml3:=put_campo(xml3,'SSL_SERVER_S_DN_CN','');
		xml3:=put_campo(xml3,'SSL_SERVER_I_DN','');
		xml3:=put_campo(xml3,'SSL_SERVER_I_DN_OU','');
		xml3:=put_campo(xml3,'SSL_SERVER_A_SIG','');
		xml3:=put_campo(xml3,'SSL_SESSION_ID','');
		xml3:=put_campo(xml3,'SERVER_SIGNATURE','');
		xml3:=put_campo(xml3,'SERVER_SOFTWARE','');
		xml3:=put_campo(xml3,'CONTENT_TYPE','');
		xml3:=put_campo(xml3,'SCRIPT_FILENAME','');
		xml3:=put_campo(xml3,'__FLUJO_ACTUAL__','');
		--Insertamos el XML3 en el xml cambiando los ###
		xml2:=put_campo(xml2,'XML3',replace(xml3,'###','&&&'));
	else
                xml2 := logapp(xml2,'Almacen:Falla Almacen Directo');
        	xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
        end if;
        xml2 := put_campo(xml2,'RESPUESTA_CUSTODIUM','');
        return xml2;
END;
$$ LANGUAGE plpgsql;


--Ahora la respuesta custodium es:
--<__PROCESOXML__=20>PROCESA_XML_ALMACEN1<__IPLOCAL__=0><__IDPROC__=1>1<__PROC_ACTIVOS__=1>1<TX=4>9015<INPUT=0><ALMACEN=14>/tmp/file2.txt<STATUS=2>OK<__FLUJO_ACTUAL__=19>F_9015:SEC_30:BD_1:<_STS_FILE_BYTES_WRITTEN_=1>4<_STS_FILE_=2>OK<FILE=14>/tmp/file2.txt
CREATE or replace FUNCTION proc_respuesta_almacen_112704_2(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2        varchar;
        data1   varchar;
	sts1	varchar;
	xml3	varchar;
BEGIN
        --Cambio la respuesta de cuadratura por la respuesta original
        xml2:=xml1;
	sts1:=decode(get_campo('RESPUESTA_HEX',xml2),'hex');
	--Limpio INPUT_CUSTODIUM para que no viaje a la 132
	xml2:=put_campo(xml2,'INPUT_CUSTODIUM','');
        xml2 := logapp(xml2,sts1);
	if (strpos(sts1,'FILE_YA_EXISTE')>0) then
		xml2 := logapp(xml2,'Almacen:File ya existe en Almacen');	
        	xml2 := put_campo(xml2,'__SECUENCIAOK__','70');
		--SAcamos el input para no hacer viajar la data hacia la base principal
		xml3:=xml2;
		xml3:=put_campo(xml3,'INPUT','');
		xml3:=put_campo(xml3,'INPUT_CUSTODIUM','');
		xml3:=put_campo(xml3,'_LOG_','');
		xml3:=put_campo(xml3,'FCGI_ROLE','');
		xml3:=put_campo(xml3,'SSL_VERSION_INTERFACE','');
		xml3:=put_campo(xml3,'SSL_VERSION_LIBRARY','');
		xml3:=put_campo(xml3,'SSL_PROTOCOL','');
		xml3:=put_campo(xml3,'SSL_CIPHER_ALGKEYSIZE','');
		xml3:=put_campo(xml3,'SSL_CIPHER_USEKEYSIZE','');
		xml3:=put_campo(xml3,'SSL_SERVER_M_SERIAL','');
		xml3:=put_campo(xml3,'SSL_SERVER_V_START','');
		xml3:=put_campo(xml3,'SSL_SERVER_V_END','');
		xml3:=put_campo(xml3,'SSL_SERVER_S_DN','');
		xml3:=put_campo(xml3,'SSL_SERVER_S_DN_OU','');
		xml3:=put_campo(xml3,'SSL_SERVER_S_DN_CN','');
		xml3:=put_campo(xml3,'SSL_SERVER_I_DN','');
		xml3:=put_campo(xml3,'SSL_SERVER_I_DN_OU','');
		xml3:=put_campo(xml3,'SSL_SERVER_A_SIG','');
		xml3:=put_campo(xml3,'SSL_SESSION_ID','');
		xml3:=put_campo(xml3,'SERVER_SIGNATURE','');
		xml3:=put_campo(xml3,'SERVER_SOFTWARE','');
		xml3:=put_campo(xml3,'CONTENT_TYPE','');
		xml3:=put_campo(xml3,'SCRIPT_FILENAME','');
		xml3:=put_campo(xml3,'__FLUJO_ACTUAL__','');
	
		--Insertamos el XML3 en el xml cambiando los ###
		xml2:=put_campo(xml2,'XML3',replace(xml3,'###','&&&'));
		
	elsif (strpos(sts1,'<_STS_FILE_=2>OK')>0) then
        	xml2 := put_campo(xml2,'__SECUENCIAOK__','70');
		--SAcamos el input para no hacer viajar la data hacia la base principal
		xml3:=xml2;
		xml3:=put_campo(xml3,'INPUT','');
		xml3:=put_campo(xml3,'INPUT_CUSTODIUM','');
		xml3:=put_campo(xml3,'_LOG_','');
		xml3:=put_campo(xml3,'FCGI_ROLE','');
		xml3:=put_campo(xml3,'SSL_VERSION_INTERFACE','');
		xml3:=put_campo(xml3,'SSL_VERSION_LIBRARY','');
		xml3:=put_campo(xml3,'SSL_PROTOCOL','');
		xml3:=put_campo(xml3,'SSL_CIPHER_ALGKEYSIZE','');
		xml3:=put_campo(xml3,'SSL_CIPHER_USEKEYSIZE','');
		xml3:=put_campo(xml3,'SSL_SERVER_M_SERIAL','');
		xml3:=put_campo(xml3,'SSL_SERVER_V_START','');
		xml3:=put_campo(xml3,'SSL_SERVER_V_END','');
		xml3:=put_campo(xml3,'SSL_SERVER_S_DN','');
		xml3:=put_campo(xml3,'SSL_SERVER_S_DN_OU','');
		xml3:=put_campo(xml3,'SSL_SERVER_S_DN_CN','');
		xml3:=put_campo(xml3,'SSL_SERVER_I_DN','');
		xml3:=put_campo(xml3,'SSL_SERVER_I_DN_OU','');
		xml3:=put_campo(xml3,'SSL_SERVER_A_SIG','');
		xml3:=put_campo(xml3,'SSL_SESSION_ID','');
		xml3:=put_campo(xml3,'SERVER_SIGNATURE','');
		xml3:=put_campo(xml3,'SERVER_SOFTWARE','');
		xml3:=put_campo(xml3,'CONTENT_TYPE','');
		xml3:=put_campo(xml3,'SCRIPT_FILENAME','');
		xml3:=put_campo(xml3,'__FLUJO_ACTUAL__','');
		--Insertamos el XML3 en el xml cambiando los ###
		xml2:=put_campo(xml2,'XML3',replace(xml3,'###','&&&'));
	else
                xml2 := logapp(xml2,'Almacen:Falla Almacen Directo');
        	xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
        end if;
        xml2 := put_campo(xml2,'RESPUESTA_CUSTODIUM','');
        return xml2;
END;
$$ LANGUAGE plpgsql;



CREATE or replace FUNCTION proc_respuesta_almacen_112704(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2        varchar;
        data1   varchar;
	sts1	varchar;
	xml3	varchar;
BEGIN
        --Cambio la respuesta de cuadratura por la respuesta original
        xml2:=xml1;
	sts1:=get_campo('_STS_FILE_',xml2);
	if (sts1='FILE_YA_EXISTE') then
		xml2 := logapp(xml2,'Almacen:File ya existe en Almacen');	
        	xml2 := put_campo(xml2,'__SECUENCIAOK__','70');
		--SAcamos el input para no hacer viajar la data hacia la base principal
		xml3:=xml2;
		xml3:=put_campo(xml3,'INPUT','');
		xml3:=put_campo(xml3,'INPUT_CUSTODIUM','');
		xml3:=put_campo(xml3,'_LOG_','');
		--Insertamos el XML3 en el xml cambiando los ###
		xml2:=put_campo(xml2,'XML3',replace(xml3,'###','&&&'));
		
	elsif (sts1='OK') then
                xml2 := logapp(xml2,'Almacen:OK Escritos '||get_campo('_STS_FILE_BYTES_WRITTEN_',xml2)||' ContentLength:'||get_campo('CONTENT_LENGTH',xml2)||' Largo Data:'||get_campo('LEN_INPUT_CUSTODIUM',xml2));
        	xml2 := put_campo(xml2,'__SECUENCIAOK__','70');
		--SAcamos el input para no hacer viajar la data hacia la base principal
		xml3:=xml2;
		xml3:=put_campo(xml3,'INPUT','');
		xml3:=put_campo(xml3,'INPUT_CUSTODIUM','');
		xml3:=put_campo(xml3,'_LOG_','');
		--Insertamos el XML3 en el xml cambiando los ###
		xml2:=put_campo(xml2,'XML3',replace(xml3,'###','&&&'));
	else
                xml2 := logapp(xml2,'Almacen:Falla Almacen Directo '||get_campo('_STS_FILE_',xml2));
        	xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
        end if;
        xml2 := put_campo(xml2,'_STS_FILE_','');
        return xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION graba_estado_publicacion_traza_112704(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2        varchar;
        data1   varchar;
	sts1	varchar;
	publicado1	varchar;
	xml3	varchar;
BEGIN
	xml2:=xml1;
	--Volvemos los &&& a los ###
	xml2:=replace(xml2,'&&&','###');
	--xml2 := logapp(xml2,'graba_estado_publicacion_traza_112704 '||replace(xml2,'###','&&&'));
        xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
        --Cambio la respuesta de cuadratura por la respuesta original
	xml2:=put_campo(xml2,'COMENTARIO_TRAZA','');
	xml2:=put_campo(xml2,'COMENTARIO2','');
	--JSE Se agrega canal para guardar evento Publicado.
	if (get_campo('SCRIPT_NAME',xml2) in ('/ca4/ca4rec','/ca4/recmotor')) then
		xml2:=put_campo(xml2,'CANAL','RECIBIDOS');
		xml2:=put_campo(xml2,'RUT_OWNER',get_campo('RUT_RECEPTOR',xml2));
	else 
		 xml2:=put_campo(xml2,'CANAL','EMITIDOS');
		xml2:=put_campo(xml2,'RUT_OWNER',get_campo('RUT_EMISOR',xml2));
	end if;
	xml2 := graba_bitacora(xml2,'PUB');
        --Para no perder los datos del XML2 solo contestamos lo que se necesita saber
	xml3:='';	
        xml3 := put_campo(xml3,'__PUBLICADO_OK__','SI');
        xml3 := put_campo(xml3,'_LOG_',get_campo('_LOG_',xml2));
	--Se necesita devolver la fecha para grabar en cuadratura antigua
	xml3 := put_campo(xml3,'FECHA_EVENTO_PUB',get_campo('FECHA_EVENTO_PUB',xml2));
        return xml3;
END;
$$ LANGUAGE plpgsql;

