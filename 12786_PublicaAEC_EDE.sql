--Publica documento
delete from isys_querys_tx where llave='12786';

--Llamamos a Escribir Directo
insert into isys_querys_tx values ('12786',40,1,1,'select proc_prepara_grabacion_edte_12786(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('12786',50,1,3,'Llamada a Escribir en EDTE',8016,0,0,0,0,60,60);
insert into isys_querys_tx values ('12786',60,1,1,'select proc_respuesta_edte_12786(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);


--Borra en la Base del Motor
insert into isys_querys_tx values ('12786',1000,1,1,'select sp_procesa_respuesta_cola_motor_original(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,0,0);
insert into isys_querys_tx values ('12786',1010,19,1,'select sp_procesa_respuesta_cola_motor_original(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,0,0);

--Genera un XML para ir a borrar, con datos basicos
CREATE or replace FUNCTION limpia_xml_12786(varchar) RETURNS varchar AS $$
DECLARE
        xml2        alias for $1;
        xml3    varchar;
BEGIN
	xml3:=xml2;
        if(get_campo('_CATEGORIA_BD_',xml2)='COLAS')then
                xml3 := put_campo(xml3,'__SECUENCIAOK__','1010');
        else
                xml3 := put_campo(xml3,'__SECUENCIAOK__','1000');
        end if;
        return xml3;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION proc_prepara_grabacion_edte_12786(varchar) RETURNS varchar AS $$
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
	id1	varchar;
	status1	varchar;
	
   	texto_resp1	varchar; 
	rut1	varchar;
	tipo_dte1	varchar;
	folio1	varchar;	
	--stDte 	dte_emitidos%ROWTYPE;
	aux1	varchar;
	json_dte1	json;
	campo	record;
BEGIN
    xml2:=xml1; 

    xml2:=put_campo(xml2,'__SECUENCIAOK__','0');

    --Si es un get salgo altiro
    if (get_campo('REQUEST_METHOD',xml2)='GET') then
        if ((strpos(get_campo('HTTP_USER_AGENT',xml2),'check_http')>0) or (length(get_campo('QUERY_STRING',xml2))=0))
then
                xml2 := logapp(xml2,'Nagios Check o GET sin datos, se ignora');
                --Se Responde OK
		xml2:=respuesta_status_scgi(xml2,'200','');
                xml2 := limpia_xml_12786(xml2);
                xml2 := put_campo_ctx(xml2,'__ETAPA1__','OK');
                return xml2;
        end if;
    end if;

    uri1:=get_campo('URI_IN',xml2);

    --20150224 FAY Si no viene URI no se puede publicar
    if (length(uri1)=0) then
	xml2 := logapp(xml2,'No viene URI_IN, no se puede publicar');
        xml2 := put_campo(xml2,'__EDTE_AEC_OK__','NO');
	xml2:=respuesta_status_scgi(xml2,'400','');
        xml2 := limpia_xml_12786(xml2);
	return xml2;	
    end if;


    texto_resp1:='';
    --Si ya tiene el evento EMA, no envio el mandato
    tabla_traza1:=get_tabla_traza(uri1);
    begin
               execute 'select * from '||tabla_traza1||' where uri=$1 and evento=''ENVIADO_EDTE_AEC''' into stTraza using uri1;
               --Si no esta el evento..
               if stTraza.uri is not null then
        		--xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
                        xml2 := put_campo(xml2,'__EDTE_AEC_OK__','SI');
			xml2 := logapp(xml2,'Uri '||uri1||' AEC ya enviado al EDTE');
			xml2 := put_campo(xml2,'MENSAJE_AEC','Cesion ya enviada');
			xml2:=respuesta_status_scgi(xml2,'200',texto_resp1);
        		xml2 := limpia_xml_12786(xml2);
		        return xml2;
               end if;
    exception WHEN OTHERS THEN
               select * into stTraza from traza.traza where uri=uri1 and evento='ENVIADO_EDTE_AEC';
               if not found then
                        --Si no esta el evento PUB vamos a publicar
                        xml2 := put_campo(xml2,'__EDTE_AEC_OK__','SI');
			xml2 := logapp(xml2,'Uri '||uri1||' AEC ya enviado al EDTE*');
			xml2 := put_campo(xml2,'MENSAJE_AEC','Cesion ya enviada');
			xml2:=respuesta_status_scgi(xml2,'200',texto_resp1);
        		xml2 := limpia_xml_12786(xml2);
		        return xml2;
               end if;
    end;
    --xml2:=put_context(xml2,'CONTEXTO_ALMACEN');
    xml2 := put_campo(xml2,'TX','8016'); 

    --Ya se parseo el DTE
    host1:=split_part(split_part(uri1,'//',2),'/',1);
    data1:=get_campo('INPUT',xml2);


    --Nuevo Procedimiento
    largo1:=get_campo('CONTENT_LENGTH',xml2)::integer*2;
    --Busco donde empieza <?xml version
    pos_inicial1:=strpos(data1,'3c3f786d6c2076657273696f6e3d');
    --Buscamos al reves donde esta el primer signo > que en hex es 3e
    --Como se pone un reverse se busca e3
    --xml2:=logapp(xml2,'MAGIA:'||strpos(reverse(data1),'e3')::varchar);
    pos_final1:=largo1-pos_inicial1+4-strpos(reverse(data1),'e3');
    data1:=substring(data1,pos_inicial1,pos_final1);
    xml2 := put_campo(xml2,'INPUT_CUSTODIUM',data1);
    xml2 := put_campo(xml2,'LEN_INPUT_CUSTODIUM',length(data1)::varchar);

    --Obtenemos el Firmante de la Cesion (Ultima Firma)
    xml2 := put_campo(xml2,'RUT_FIRMA_CESION',split_part(split_part(verifica_certificado(get_firma_cesion(data1)),'serialNumber=',2),'-',1));

    --Obtengo datos del documento
    rut1:=split_part(get_xml_hex1('RUTEmisor',data1),'-',1);
    tipo_dte1:=get_xml_hex1('TipoDTE',data1);
    folio1:=get_xml_hex1('Folio',data1);
    xml2 := put_campo(xml2,'RUT_EMISOR',rut1);
    xml2 := put_campo(xml2,'RUT_RECEPTOR',split_part(get_xml_hex1('RUTRecep',data1),'-',1));
    xml2 := put_campo(xml2,'MONTO_NETO',get_xml_hex1('MntNeto',data1));
    xml2 := put_campo(xml2,'MONTO_TOTAL',get_xml_hex1('MntTotal',data1));
    xml2 := put_campo(xml2,'FOLIO',folio1);
    xml2 := put_campo(xml2,'TIPO_DTE',tipo_dte1);
    xml2 := put_campo(xml2,'RUT_CEDENTE',get_xml_hex1('RutCedente',data1));
    xml2 := put_campo(xml2,'RUT_CESIONARIO',get_xml_hex1('RutCesionario',data1));
    xml2 := put_campo(xml2,'NOMBRE_CONTACTO',get_xml_hex1('NmbContacto',data1));
    xml2 := put_campo(xml2,'FONO_CONTACTO',get_xml_hex1('FonoContacto',data1));
    xml2 := put_campo(xml2,'MAIL_CONTACTO',get_xml_hex1('MailContacto',data1));
    xml2 := put_campo(xml2,'MONTO_CESION',get_xml_hex1('MontoCesion',data1));
    xml2 := put_campo(xml2,'ULTIMO_VENC',get_xml_hex1('UltimoVencimiento',data1));
    aux1:=replace(get_xml_hex1('FchEmis',data1),'-','');
    if (is_number(aux1)) then
	    xml2 := put_campo(xml2,'FECHA_EMISION',aux1);
    else
	    xml2 := put_campo(xml2,'FECHA_EMISION',to_char(now(),'YYYYMMDD'));
    end if;
	

    --Si es numerico
    if (is_number(rut1) is false or is_number(tipo_dte1) is false or is_number(folio1) is false) then
	xml2:=logapp(xml2,'EDTE AEC: Datos Invalidor rut1='||rut1||' tipo_dte1='||tipo_dte1||' folio1='||folio1);
	xml2:=respuesta_status_scgi(xml2,'400',texto_resp1);
   	xml2 := limpia_xml_12786(xml2);
	return xml2;
    end if;

    --FAY-DAO 2019-07-02 la recesion no busca en dte_emitidos
    if (get_campo('RECESION',xml2)='SI') then
	xml2:=logapp(xml2,'EDTE AEC: Recesion no busca en dte_emitidos');
	xml2 := put_campo(xml2,'CODIGO_TXEL','-1');	
    else
	--DAO 20201126 si es factura de compra buscamos en los recibidos
	if tipo_dte1::integer=46 then
    		json_dte1:=lee_dte_recibido(rut1::integer,tipo_dte1::integer,folio1::bigint);
	else
    		json_dte1:=lee_dte(rut1::integer,tipo_dte1::integer,folio1::bigint);
	end if;
	if (get_json('status',json_dte1)='NO_ENCONTRADO') then
		--Error
		xml2:=logapp(xml2,'EDTE AEC: No existe en dte_emitidos rut1='||rut1||' tipo_dte1='||tipo_dte1||' folio1='||folio1);
		xml2 := put_campo(xml2,'MENSAJE_AEC','Cesion referencia DTE que no esta registrado');
		xml2 := put_campo(xml2,'__EDTE_AEC_OK__','NO');
		xml2:=respuesta_status_scgi(xml2,'400','Cesion referencia DTE que no esta registrado');
   		xml2 := limpia_xml_12786(xml2);
		return xml2;
	end if;
		
     --Si no esta aprobado (solo para Documentos no IMPORTADOS)
	if (get_json('estado_sii',json_dte1) not in ('ACEPTADO_POR_EL_SII','ACEPTADO_CON_REPAROS_POR_EL_SII','IMPORTADO')) then
		xml2:=logapp(xml2,'EDTE AEC: No esta aprobado, se rechaza rut1='||rut1||' tipo_dte1='||tipo_dte1||' folio1='||folio1);
		xml2 := put_campo(xml2,'MENSAJE_AEC','Cesion referencia DTE que no esta aprobado por el SII');
		xml2 := put_campo(xml2,'__EDTE_AEC_OK__','NO');
		xml2:=respuesta_status_scgi(xml2,'400','Cesion referencia DTE que no esta aprobado por el SII');
   		xml2 := limpia_xml_12786(xml2);
		return xml2;
	end if;
	--FAY-DAO 2018-04-02 Antes de permitir la cesion, verificamos que no este reclamado el DTE
	if (get_json('estado_nar',json_dte1) in ('RECHAZO_DE_CONTENIDO_DE_DOCUMENTO') or get_json('estado_reclamo',json_dte1) in ('RECLAMO_FALTA_PARCIAL_DE_MERCADERIA','RECLAMO_FALTA_TOTAL_DE_MERCADERIA')) then
		xml2:=logapp(xml2,'EDTE AEC: Dte Reclamado, se rechaza rut1='||rut1||' tipo_dte1='||tipo_dte1||' folio1='||folio1);
		xml2 := put_campo(xml2,'MENSAJE_AEC','Cesion referencia DTE que esta reclamado en el SII');
		xml2 := put_campo(xml2,'__EDTE_AEC_OK__','NO');
		xml2:=respuesta_status_scgi(xml2,'400','Cesion referencia DTE que esta reclamado en el SII');
   		xml2 := limpia_xml_12786(xml2);
		return xml2;
	end if;
	--FAY-DAO Antes de permitir la cesion, verificamos que no este reclamado el DTE
	
	xml2:=logapp(xml2,'EDTE AEC: Existe OK rut1='||rut1||' tipo_dte1='||tipo_dte1||' folio1='||folio1);
	xml2 := put_campo(xml2,'URI_DTE',get_json('uri',json_dte1));
	xml2 := put_campo(xml2,'CODIGO_TXEL',get_json('codigo_txel',json_dte1));
    end if;
		

    --Si ya existe una cesion enviada al SII y aun no tengo el track_id, no proceso la cesion
    --FAY-DAO 20190702 las cesiones deben incorporar el rutcedente y cesionario, ya que pueden existir multiples cesiones del mismo documento, el estado sii no aplica para las recesiones
    --select * into campo from dte_cesiones where rut_emisor=rut1::integer and tipo_dte=tipo_dte1::integer and folio=folio1::bigint and estado_sii is null;
    select * into campo from dte_cesiones where rut_emisor=rut1::integer and tipo_dte=tipo_dte1::integer and folio=folio1::bigint and rut_cesionario=get_campo('RUT_CESIONARIO',xml2) and rut_cedente=get_campo('RUT_CEDENTE',xml2);
    if found then
		--Si ya se encuentra la cesion, validamos el estado sii
		if (campo.estado_sii is null and get_campo('RECESION',xml2)<>'SI') then
			xml2:=logapp(xml2,'EDTE AEC: '||campo::varchar);
			--Tengo una cesion en espera, debe esperar el estado del EDTE
			xml2:=logapp(xml2,'EDTE AEC: Ya existe cesion sin TrackID rut1='||rut1||' tipo_dte1='||tipo_dte1||' folio1='||folio1);
			xml2 := put_campo(xml2,'MENSAJE_AEC','Ya existe cesion sin TrackID, espere por favor');
			xml2 := put_campo(xml2,'__EDTE_AEC_OK__','NO');
			xml2:=respuesta_status_scgi(xml2,'400','Ya existe cesion sin TrackID');
			if (get_campo('FECHA_INGRESO_COLA',xml2)<>'') then
				if(now()-get_campo('FECHA_INGRESO_COLA',xml2)::timestamp>interval '1 month') then
					xml2:=logapp(xml2,'EDTE AEC: No se respondio la cesion, se borra de la cola');
					xml2:=respuesta_status_scgi(xml2,'200','Ya existe cesion sin TrackID, se borra');
				end if;
			end if;
   			xml2 := limpia_xml_12786(xml2);
			return xml2;
		end if;
     end if;

	
	/*
    	--Verifico la existencia del documento a ceder
	select * into stDte from dte_emitidos where rut_emisor=rut1::bigint and tipo_dte=tipo_dte1::integer and folio=folio1::bigint;
	if not found then
		select * into stDte from dte_emitidos_importados_generica where rut_emisor=rut1::bigint and tipo_dte=tipo_dte1::integer and folio=folio1::bigint;
		if not found then
			--Error
			xml2:=logapp(xml2,'EDTE AEC: No existe en dte_emitidos rut1='||rut1||' tipo_dte1='||tipo_dte1||' folio1='||folio1);
			xml2 := put_campo(xml2,'MENSAJE_AEC','Cesion referencia DTE que no esta registrado');
			xml2 := put_campo(xml2,'__EDTE_AEC_OK__','NO');
			xml2:=respuesta_status_scgi(xml2,'400','Cesion referencia DTE que no esta registrado');
		        xml2 := sp_procesa_respuesta_cola_motor(xml2);
			return xml2;
		end if;
	end if;
		
	--else
		--Si no esta aprobado (solo para Documentos no IMPORTADOS)
		if (stDte.estado_sii not in ('ACEPTADO_POR_EL_SII','ACEPTADO_CON_REPAROS_POR_EL_SII','IMPORTADO')) then
			xml2:=logapp(xml2,'EDTE AEC: No esta aprobado, se rechaza rut1='||rut1||' tipo_dte1='||tipo_dte1||' folio1='||folio1);
			xml2 := put_campo(xml2,'MENSAJE_AEC','Cesion referencia DTE que no esta aprobado por el SII');
			xml2 := put_campo(xml2,'__EDTE_AEC_OK__','NO');
			xml2:=respuesta_status_scgi(xml2,'400','Cesion referencia DTE que no esta aprobado por el SII');
		        xml2 := sp_procesa_respuesta_cola_motor(xml2);
			return xml2;
		end if;
		xml2:=logapp(xml2,'EDTE AEC: Existe OK rut1='||rut1||' tipo_dte1='||tipo_dte1||' folio1='||folio1);
		xml2 := put_campo(xml2,'URI_DTE',stDte.uri);
		xml2 := put_campo(xml2,'CODIGO_TXEL',stDte.codigo_txel::varchar);
	--end if;
    end if;
    */


    --http%3A%2F%2Fdcummins1503.acepta.com%2Fv01%2F8747A8B9163F1433677E5676D9E619701998F1AA%3Fk%3D8181b82294071788d29d6992b4caf785
    file1:=replace(replace(replace(replace(uri1,':','%3A'),'/','%2F'),'?','%3F'),'=','%3D');
    xml2:=put_campo(xml2,'ALMACEN','/opt/acepta/enviodte/work/sii/aec/escribiendo_motor/'||file1);
    xml2:=logapp(xml2,'EDTE AEC: '||get_campo('ALMACEN',xml2));

    xml2:=put_campo(xml2,'SCRIPT_EDTE','/bin/mv /opt/acepta/enviodte/work/sii/aec/escribiendo_motor/'||file1||' /opt/acepta/enviodte/work/sii/aec/pendiente/'||file1);
    --xml2:=put_campo(xml2,'SCRIPT_EDTE','echo 1');

    xml2:=logapp(xml2,'EDTE AEC RUT_EMISOR='||get_campo('RUT_EMISOR',xml2)||' Script:'||get_campo('SCRIPT_EDTE',xml2));
    xml2:=logapp(xml2,'EDTE AEC FOLIO='||get_campo('FOLIO',xml2)||' RUT_EMISOR='||get_campo('RUT_EMISOR',xml2));
    
    xml2:=put_campo(xml2,'RUT_CGE',get_campo('RUT_EMISOR',xml2));
    xml2:=verifica_evento_cge(xml2);
    xml2:=logapp(xml2,'EVENTO_CGE='||get_campo('EVENTO_CGE',xml2));
    if (get_campo('EVENTO_CGE',xml2)='SI') then
	xml2:=put_campo(xml2,'__IP_CONEXION_CLIENTE__',get_campo('__IP_CONEXION_CLIENTE__',get_parametros_motor('','EDTE_CGE')));
    else
	xml2:=put_campo(xml2,'__IP_CONEXION_CLIENTE__',get_campo('__IP_CONEXION_CLIENTE__',get_parametros_motor('','EDTE')));
    end if;
	
    xml2:=logapp(xml2,'EDTE AEC: Envia Data Directo '||get_campo('__IP_CONEXION_CLIENTE__',xml2)||' '||uri1);
    --PARA MODO PRUEBA SISTEMAS NO PUEDE CEDER
    if (get_campo('rol_usuario',xml2)='Sistemas' or get_campo('rutUsuario',xml2)='7621836') then
	xml2:=logapp(xml2,'MODO SISTEMAS NO ENVIA AL SII Usuario='||get_campo('rutUsuario',xml2)||' Rol='||get_campo('rol_usuario',xml2));
    	xml2:=put_campo(xml2,'__SECUENCIAOK__','60');
	xml2 := put_campo(xml2,'_STS_FILE_','OK');
	return xml2;
	
    end if;
    xml2:=put_campo(xml2,'__SECUENCIAOK__','50');
    xml2 := put_campo(xml2,'_STS_FILE_','');
    return xml2;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION proc_respuesta_edte_12786(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2        varchar;
        data1   varchar;
	sts1	varchar;
	publicado1	varchar;
	xml3	varchar;
	cola1  bigint;
	nombre_tabla1   varchar;
	        uri1    varchar;
        rut1    varchar;
	        tx1     varchar;
	id1	varchar;
	codigo1	varchar;
	status1	varchar;
	texto_resp1	varchar;
	aux1	varchar;
	monto_neto1	varchar;
	codigo_txel1	varchar;
	rut_receptor1	varchar;
	codigo_cesion1	bigint;
	rut_cesonario	varchar;
	query1		varchar;
	campo		record;
	
BEGIN
        --Cambio la respuesta de cuadratura por la respuesta original
        xml2:=xml1;
       	xml2 := put_campo(xml2,'__EDTE_OK__','NO');
	sts1:=get_campo('_STS_FILE_',xml2);
	if (sts1='FILE_YA_EXISTE') then
		xml2 := logapp(xml2,'EDTE AEC:File ya existe en EDTE');	
        	xml2 := put_campo(xml2,'__EDTE_AEC_OK__','SI');
		xml2 := graba_bitacora(xml2,'ENVIADO_EDTE_AEC');
		--Un documento reprocesado, que se envia al AML, se puede borrar de la cola de procesamiento
	        if (get_campo('_REPROCESO_',xml2)='SI') then
        	        xml2 := put_campo(xml2,'_ESTADO_REPROCESO_','OK');
	        end if;

	elsif (sts1='OK') then
                xml2 := logapp(xml2,'EDTE:OK Escritos '||get_campo('_STS_FILE_BYTES_WRITTEN_',xml2)||' ContentLength:'||get_campo('CONTENT_LENGTH',xml2)||' Largo Data:'||get_campo('LEN_INPUT_CUSTODIUM',xml2));
        	xml2 := put_campo(xml2,'__EDTE_AEC_OK__','SI');
		xml2 := graba_bitacora(xml2,'ENVIADO_EDTE_AEC');
	else
                xml2 := logapp(xml2,'EDTE AEC:Falla EDTE Directo '||get_campo('_STS_FILE_',xml2));
        	xml2 := put_campo(xml2,'__EDTE_AEC_OK__','NO');
		xml2 := graba_bitacora(xml2,'FALLA_ENVIADO_EDTE_AEC');
        end if;
        xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
        xml2 := put_campo(xml2,'_STS_FILE_','');


	--Si me fue bien
	texto_resp1:='';
	if (get_campo('__EDTE_AEC_OK__',xml2)='SI') then	

		codigo_txel1:=get_campo('CODIGO_TXEL',xml2);
		monto_neto1:=get_campo('MONTO_NETO',xml2);
		if (is_number(monto_neto1) is false) then
			monto_neto1:=null;
		end if;
		rut_receptor1:=get_campo('RUT_RECEPTOR',xml2);
		if (is_number(rut_receptor1) is false) then
			rut_receptor1=-1;
		end if;
		--Grabo en la tabla de cesiones
		insert into dte_cesiones (fecha_ingreso,mes,dia,rut_emisor,tipo_dte,folio,rut_receptor,monto_neto,monto_total,monto_cesion,rut_cedente,rut_cesionario,nombre_contacto,fono_contacto,mail_contacto,ultimo_vencimiento,estado,uri,uri_dte,codigo_txel,origen,dia_emision,usuario_cesion) values (now(),to_char(now(),'YYYYMM')::integer,to_char(now(),'YYYYMMDD')::integer,get_campo_bigint('RUT_EMISOR',xml2)::integer,get_campo_bigint('TIPO_DTE',xml2)::integer,get_campo_bigint('FOLIO',xml2)::bigint,rut_receptor1::integer,monto_neto1::bigint,get_campo_bigint('MONTO_TOTAL',xml2)::bigint,get_campo_bigint('MONTO_CESION',xml2)::bigint,get_campo('RUT_CEDENTE',xml2),get_campo('RUT_CESIONARIO',xml2),get_campo('NOMBRE_CONTACTO',xml2),get_campo('FONO_CONTACTO',xml2),get_campo('MAIL_CONTACTO',xml2),get_campo('ULTIMO_VENC',xml2),'INGRESADO',get_campo('URI_IN',xml2),get_campo('URI_DTE',xml2),codigo_txel1::bigint,get_campo('ORIGEN_AEC',xml2),get_campo_bigint('FECHA_EMISION',xml2)::integer,get_campo('RUT_FIRMA_CESION',xml2)) returning codigo_cesion into codigo_cesion1;
		xml2:=logapp(xml2,'Cesion Procesada Ok '||get_campo('app_dinamica',xml2));
		xml2 := put_campo(xml2,'MENSAJE_AEC','Cesion Procesada Ok');
		
		--Relaciona los documentos para la traza
		xml2 := put_campo(xml2,'CODIGO_CESION',codigo_cesion1::varchar);
		if is_number(get_periodo_uri(get_campo('URI_DTE',xml2))) then
			aux1:=graba_documentos_relacionados(get_campo('URI_IN',xml2),'CESION_'||get_campo('TIPO_DTE',xml2),get_campo('FOLIO',xml2),get_campo('URI_DTE',xml2),get_campo('TIPO_DTE',xml2),get_campo('FOLIO',xml2));
			aux1:=graba_documentos_relacionados(get_campo('URI_DTE',xml2),get_campo('TIPO_DTE',xml2),get_campo('FOLIO',xml2),get_campo('URI_IN',xml2),'CESION_'||get_campo('TIPO_DTE',xml2),get_campo('FOLIO',xml2));
		end if;
		
		--Relaciono Dte_emitidos indicando que tiene una cesion
		if codigo_txel1<>'-1' then
			if get_campo('TIPO_DTE',xml2)='46' then
				update dte_recibidos set data_dte=put_data_dte(data_dte,'EstadoCesion','CEDIDO') where codigo_txel=codigo_txel1::bigint;
			else
				update dte_emitidos set estado_cesion='SI' where codigo_txel=codigo_txel1::bigint;
			end if;
		end if;

		status1:='Status: 200 OK'||chr(10)||
                 'Content-type: text/html'||chr(10)||
                 'Content-Location: '||get_campo('URI_IN',xml2)||chr(10)||
                 'Content-length: '||length(texto_resp1)||chr(10);
	        xml2 := logapp(xml2,'CA4AEC: Respuesta Servicio 200 OK URI'||get_campo('URI_IN',xml1));
        	xml2 := logapp(xml2,'CA4AEC: Respuesta'||status1||chr(10)||texto_resp1);
	        xml2 := put_campo(xml2,'RESPUESTA',status1||chr(10)||texto_resp1);
   		xml2 := limpia_xml_12786(xml2);

	        --Si el DTE tiene una solicitud de financiamiento pendiente por parte del cliente, y se cede el dte
        	--Se asume que esta aceptando la oferta
		rut_cesonario:=upper(replace(trim(get_campo('RUT_CESIONARIO',xml2)),'.',''));
		/*
	        for campo in select * from financiamiento_solicitudes where codigo_txel=codigo_txel1::bigint and rut_financiador<>rut_cesonario and coalesce(estado_cliente,'')<>'RECHAZADA' loop
			--Si el financiador es distinto, rechazo
			if campo.estado not in ('CEDIDA') then
				xml2 := logapp(xml2,'CA4AEC: Se rechaza la solicitud de financiamiento id='||campo.id::varchar);
				--Si cede el DTE a otro financiador, se rechaza por defecto
				xml3:='';
				xml3:=put_campo(xml3,'TX','6001');
				xml3:=put_campo(xml3,'tipo_tx','financiamiento_rechazar_oferta');
				xml3:=put_campo(xml3,'ID',campo.id::varchar);
				xml3:=put_campo(xml3,'rutUsuario',campo.rut_usuario_cliente::varchar);
				xml3:=put_campo(xml3,'razon_rechazo','Usuario Cede a Otro Financiador');
				xml3:=put_campo(xml3,'FECHA_INGRESO_COLA',now()::varchar);
				cola1:=nextval('id_cola_procesamiento');
				nombre_tabla1:='cola_motor_'||cola1::varchar;
				query1:='insert into ' || nombre_tabla1 || ' (fecha,uri,reintentos,data,tx,rut_emisor,reproceso,categoria, nombre_cola) values ( now(),'||quote_literal(campo.uri)||',9,'||quote_literal(xml3)||',10,'||quote_literal(campo.rut_emisor::varchar)||',''NO'',''RECHAZO_FINANCIAMIENTO'','|| quote_literal(nombre_tabla1) ||');';
				execute query1;
			end if;
	        end loop;
		*/
	else
		xml2 := put_campo(xml2,'MENSAJE_AEC','Falla Grabado de Cesion, reintente por favor');
	        status1:='Status: 400 Rechazado'||chr(10)||'Content-type: text/html'||chr(10)||'Content-length: '||length(texto_resp1)||chr(10);	
		xml2 := logapp(xml2,'Respuesta Servicio 400 Rechazado (12728) URI'||get_campo('URI_IN',xml2));
		xml2:=put_campo(xml2,'RESPUESTA',status1||chr(10)||'');
   		xml2 := limpia_xml_12786(xml2);
	end if;

        return xml2;
END;
$$ LANGUAGE plpgsql;

