delete from isys_querys_tx where llave='8031';

insert into isys_querys_tx values ('8031',10,45,1,'select ensobra_dte_8031(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,1000);

--Primero que hacemos el publicar DTE Recibido
insert into isys_querys_tx values ('8031',40,1,8,'Publica DTE',112704,0,0,0,0,50,50);

--Proceso el DTE REcibido
insert into isys_querys_tx values ('8031',50,45,1,'select verifica_publicacion_rec_8031(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

--Genera Solicitud de CRT
insert into isys_querys_tx values ('8031',60,19,1,'select genera_solicitud_crt_8031(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,1000);

--Genera Solicitud de Validacion en SII
insert into isys_querys_tx values ('8031',70,19,1,'select genera_solicitud_sii_8031(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,1000);

insert into isys_querys_tx values ('8031',1000,19,1,'select sp_procesa_respuesta_cola_motor88_json(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,0,0);
insert into isys_querys_tx values ('8031',1010,45,1,'select sp_procesa_respuesta_cola_motor_original_json(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,0,0);

CREATE or replace FUNCTION verifica_publicacion_rec_8031(varchar) RETURNS varchar AS $$
DECLARE
        xml1        alias for $1;
        xml2    varchar;
	--json_par1	json;
	json_aux	json;
	json_in		json;
	json_script1	json;
	resp_est	varchar;
	resp_cod	varchar;	
	glosa_es	varchar;
	glosa_er	varchar;
	output1		varchar;
	fecha1		varchar;
	cola1	varchar;
	v_nombre_tabla	VARCHAR;
	rut1	varchar;
	aux1	varchar;
	port            varchar;
	j3	json;
BEGIN
    xml2:=xml1;
    xml2 := put_campo(xml2,'FLAG_NO_LIMPIA','SI');
    xml2 := put_campo(xml2,'FIRMA_DA','8031');
    --xml2:=logapp(xml2,'verifica_publicacion_rec_8031  FECHA_EMISION='||get_campo('FECHA_EMISION',xml2));

    
    --Se guarda Evento RCP (Procesado por el receptor)
    rut1:=get_campo('RUT_EMISOR',xml2);
    aux1:=(select email from contribuyentes where rut_emisor=rut1::integer);
    --perform logfile('F_8031 '||aux1||' '||rut1::varchar);
    xml2:=put_campo(xml2,'MAIL_EMISOR',aux1);

    --Leemos antes de procesar, si el Recibido ya esta emitido
    j3:=lee_traza_evento(get_campo('URI_IN',xml2),'EMI');
    if get_json('status',j3)='SIN_DATA' then
	     xml2 := proc_recibidos_fcgi_12703(xml2);
    	     xml2:=put_campo(xml2,'CERTIFICADO_X509','');
             xml2:=put_campo(xml2,'XML3','');
             xml2 := logapp(xml2,'RESPUESTA proc_recibidos_fcgi_12703 '||get_campo('RESPUESTA',xml2));
	     if (get_campo('FLAG_12703',xml2)='FALLA') then
		--Aca puede o no borrar segun el flujo 12703
		xml2 := logapp(xml2,'Falla proc_recibidos_fcgi_12703');
		xml2 := put_campo(xml2,'__SECUENCIAOK__','1000');
		return xml2;
	    end if;	
	    xml2:=put_campo(xml2,'RESPUESTA','');
	    --Si estaba repetido y ya tiene estado del sii
	    if (get_campo('FLAG_DTE_RECIBIDO_REPETIDO',xml2)='SI') then
		--Si tengo estado del sii, vamos por el CRT
		if (get_campo('ESTADO_SII_DTE_REPETIDO',xml2)<>'') then
			xml2 := logapp(xml2,'DTE ya recibido, se envia CRT Repetido');
			xml2 := put_campo(xml2,'__SECUENCIAOK__','60');
			return xml2;
		end if; 	

		--Si no fue grabado por motor...
		if (get_campo('NUEVO_RECIBIDO',xml2)<>'1' and get_campo('RUT_RECEPTOR',xml2)='81201000') then
			xml2 := logapp(xml2,'DTE ya recibido de cencodud, se envia CRT Repetido');
			xml2 := put_campo(xml2,'__SECUENCIAOK__','60');
			return xml2;
		end if;	

		--Si es del motor y tiene distintas uri, se envia repetido
		 if (get_campo('NUEVO_RECIBIDO',xml2)='1' and get_campo('URI_DTE_REPETIDO',xml2)<>get_campo('URI_IN',xml2)) then
			--repetido	
			xml2 := logapp(xml2,'DTE ya recibido de motor, se envia CRT Repetido');
			xml2 := put_campo(xml2,'__SECUENCIAOK__','60');
			return xml2;
		end if;
	    end if;

	    xml2:=put_campo(xml2,'COMENTARIO_TRAZA','Emite: '||coalesce(aux1,'-'));
	    xml2:=graba_bitacora(xml2,'RCP');                               

	    --Guardo en la traza el sobre de envio
	    xml2:=put_campo(xml2,'COMENTARIO_TRAZA',get_campo('eml',xml2));
	    xml2:=graba_bitacora(xml2,'SOBRE_ENVIO');
		

	    --Antes de ir al sii verificamos que no haya estado en el reporte consolidado..
	    if(get_campo('FLAG_RC_OK',xml2)='SI') then
		xml2:=logapp(xml2,'DTE encontrado en reporte consolidado, se da por validado el DTE en el SII');
		--Damos por validado el DTE en el SII
		xml2:=put_campo(xml2,'COMENTARIO_TRAZA','Glosa: DTE Recibido (DOK)*');
		xml2:=put_campo(xml2,'EVENTO','ASI');
		--xml2:=put_campo(xml2,'FECHA_EVENTO_ASI',get_campo('FECHA_RC_OK',xml2));
		xml2:=actualiza_estado_dte(xml2);
		xml2:=graba_bitacora(xml2,'ASI');
		xml2 := put_campo(xml2,'__SECUENCIAOK__','60');
		return xml2;
    	    end if;
    --Si falla la lectura de la traza
    elsif (get_json('status',j3)<>'OK') then
	xml2 := logapp(xml2,'Falla leer traza');
	xml2:=put_campo(xml2,'RESPUESTA','Status: 400 NK');
	xml2 := put_campo(xml2,'__SECUENCIAOK__','1000');
	return xml2;
    else
	--Si no viene INPUT... Lo sacamos DAO 20180507
	if get_campo('INPUT',xml2)='' then
		xml2:=put_campo(xml2,'INPUT',get_input_almacen('{"uri":"'||get_campo('URI_IN',xml2)||'"}'));
	end if;
	--Parseamos el doc recibido
	xml2 := parseo_doc_recibido(xml2);
        xml2:=put_campo(xml2,'CERTIFICADO_X509','');
        xml2:=put_campo(xml2,'XML3','');
	xml2:=put_campo(xml2,'RESPUESTA','');
	xml2 := logapp(xml2,'Ya existe evento EMI');
    end if;

    --Si llega una boleta, hasta llega la recepcion
    if (get_campo('TIPO_DTE',xml2) in ('39','41')) then
	xml2:=logapp(xml2,'Boletas no van a comprobarse al SII');
	xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
        xml2 := put_campo(xml2,'__SECUENCIAOK__','1000');
	return xml2;
    end if;

    --Si no esta aprobado por el SII vamos 
    j3:=lee_traza_evento(get_campo('URI_IN',xml2),'ASI');
    if get_json('status',j3)='SIN_DATA' then
    	xml2 := put_campo(xml2,'__SECUENCIAOK__','70');
    --Si falla la lectura de la traza
    elsif (get_json('status',j3)<>'OK') then
	xml2 := logapp(xml2,'Falla leer traza');
	xml2:=put_campo(xml2,'RESPUESTA','Status: 400 NK');
	xml2 := put_campo(xml2,'__SECUENCIAOK__','1000');
    else
	xml2 := logapp(xml2,'Ya existe evento ASI, voy directo al enviar el CRT');
	xml2 := put_campo(xml2,'__SECUENCIAOK__','60');
    end if;
    return xml2;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION public.ensobra_dte_8031(json) RETURNS json AS $$
DECLARE
    json1               alias for $1;
    json2               json;
        json3   json;
        patron_dte1     varchar;
        xml_dte1        varchar;
        dte1            varchar;
        esquema         varchar;
        uri1            varchar;
        rut1            varchar;
        campo           record;
        campo1          record;

	rut_emisor1	varchar;
	rut_emisor2	varchar;
	tipo_dte1	varchar;
	folio1		varchar;
	tms1		varchar;
	tms2		timestamp;
	rut_receptor2	bigint;
	certificado_x509	varchar;
	aux			varchar;
	rut_firma1		varchar;
	monto1		varchar;
	fecha1		varchar;

	json_in	json;
BEGIN
        json2:=json1;
	json2:=put_json(json2,'__SECUENCIAOK__','1000');
	json2:=logjson(json2,'URIP='||get_json('URIP',json2)||' ID='||get_json('__ID_DTE__',json2));

	--Sacamos el xml del mail
	xml_dte1:=get_json('XML',json2);
	--Limpio XML
	json2:=put_json(json2,'XML','');
	tipo_dte1:=get_xml_hex1('TipoDTE',xml_dte1);
	--Para corregir Visualizacion
	--Las liquidaciones son distintas
	if (tipo_dte1='43') then
		xml_dte1:=encode('<DTE version="1.0"><Liquidacion ','hex')||split_part(xml_dte1,encode('<Liquidacion ','hex'),2);
	elsif (tipo_dte1 in ('110','111','112')) then
		xml_dte1:=encode('<DTE version="1.0"><Exportaciones ','hex')||split_part(xml_dte1,encode('<Exportaciones ','hex'),2);
	else
		xml_dte1:=encode('<DTE version="1.0"><Documento ','hex')||split_part(xml_dte1,encode('<Documento ','hex'),2);
	end if;
	--2017-0602 Cambiamos el <Signature> por <Signature xmlns="http://www.w3.org/2000/09/xmldsig#">
	xml_dte1:=replace(xml_dte1,encode('<Signature>','hex'),encode('<Signature xmlns="http://www.w3.org/2000/09/xmldsig#">','hex'));

        rut1:=split_part(get_xml_hex1('RUTRecep',xml_dte1),'-',1);
	if (is_number(rut1) is false) then
		json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
		json2:=logjson(json2,'Rut Receptor no es numerico, xml invalido '||rut1::varchar||' URI='||get_json('URI_IN',json2));
		return json2;
	end if;
	--Para la publicacion
	json2:=put_json(json2,'RUT_RECEPTOR',rut1);

        select * into campo from maestro_clientes where rut_emisor=rut1::integer;
	if not found then
		if(now()-get_json('FECHA_INGRESO_COLA',json2)::timestamp>interval '5 days') then
			json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
		else
			json2:=put_json(json2,'RESPUESTA','Status: 444 NK');
			json2:=put_json(json2,'MENSAJE_XML_FLAGS','Rut Receptor no se encuentra en maestro_clientes');
		end if;
		json2:=logjson(json2,'Rut Receptor no se encuentra en maestro_clientes '||rut1::varchar);
		return json2;
	end if;

        --json2:=put_json(json2,'RAZON_SOC_RECEPTOR_HEX',utf82latin1hex(campo.razon_social));
        json2:=put_json(json2,'RAZON_SOC_RECEPTOR_HEX',encode(escape_xml_characters_simple(campo.razon_social)::bytea,'hex'));
        json2:=put_json(json2,'RAZON_SOC_EMISOR_HEX',get_xml_hex(encode('RznSoc'::bytea,'hex'),xml_dte1));
	
	rut_emisor2:=get_xml_hex1('RUTEmisor',xml_dte1);
	rut_emisor1:=split_part(rut_emisor2,'-',1);
	tipo_dte1:=get_xml_hex1('TipoDTE',xml_dte1);
	folio1:=get_xml_hex1('Folio',xml_dte1);
	tms1:=get_xml_hex1('TmstFirma',xml_dte1);
	monto1:=get_xml_hex1('MntTotal',xml_dte1);
	fecha1:=get_xml_hex1('FchEmis',xml_dte1);
	
	if(is_number(rut_emisor1) is false or is_number(tipo_dte1) is false or is_number(folio1) is false) then
		json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
		json2:=logjson(json2,'Datos Inválidos, no numéricos...');
		return json2;
	end if;
	--Se normaliza el folio para q no queden ceros a la izquierda
	folio1:=folio1::bigint::varchar;

	--Solo aceptamos estos tipos de dete
    	select * into campo1 from tipo_dte where codigo=tipo_dte1::integer;
	if not found then
		json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
		json2:=logjson(json2,'Tipo Dte Invalido '||tipo_dte1);
		return json2;
	end if;

	BEGIN
		tms2:=tms1::timestamp;
		rut_receptor2:=split_part(get_xml_hex1('RUTRecep',xml_dte1),'-',1)::bigint;
	EXCEPTION WHEN OTHERS THEN
		json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
		json2:=logjson(json2,'Fecha Firma Inválida o RUTRecep invalido,se borra DTE');
		return json2;
	END;
	json2:=put_json(json2,'TmstFirma',tms1);
        json3:='{}';

	if (campo.dominio is null or campo.dominio='') then
                campo.dominio:='webdte';
	end if;
	
	--Reviso si el DTE ya esta en la tabla de pendientes
	select * into campo1 from dte_pendientes_recibidos where rut_emisor=rut_emisor2 and tipo_dte=tipo_dte1 and folio=folio1 and rut_receptor=rut_receptor2 and monto_total=monto1 and split_part(fecha_emision,' ',1)=fecha1 and coalesce(uri,'')<>'';
	if found then
		--Si se encuentra
		json2:=logjson(json2,'DTE ya registrado en dte_pendientes_recibidos, se usa esta URI='||campo1.uri);	
		uri1:=campo1.uri;
	else
		json2:=logjson(json2,'GENERO URI RUT_EMISOR='||rut_emisor1::varchar||' TIPO_DTE='||tipo_dte1::varchar||' FOLIO='||folio1::varchar||' FECHA_EMISION='||get_xml_hex1('FchEmis',xml_dte1)||' MONTO_TOTAL='||get_xml_hex1('MntTotal',xml_dte1));
        	uri1:='http://'||campo.dominio||to_char(now(),'YYMM')||'.acepta.com/v01/'||genera_uri2(rut_emisor1,tipo_dte1,folio1,get_xml_hex1('FchEmis',xml_dte1),get_xml_hex1('MntTotal',xml_dte1),'R');
		json2:=logjson(json2,'DTE no registrado en dte_pendientes_recibidos URI='||uri1);
	end if;
	
	json3:=put_json(json3,'DominioEmisor',campo.dominio);
	json3:=put_json(json3,'Dominio',campo.dominio);
	json2:=put_json(json2,'DOMINIO_EMISOR',campo.dominio);

	json2:=logjson(json2,'Ensobro DTE Recibido '||uri1||' URI_PY='||get_json('URIP',json2));

	certificado_x509:=replace(get_xml_hex1('X509Certificate',xml_dte1),' ','');
	--json2:=logjson(json2,'X509Certificate='||certificado_x509);
	aux:=verifica_certificado(certificado_x509);
	rut_firma1:=split_part(split_part(aux,'serialNumber=',2),'-',1);
	--Si no podemos sacar el rut correcto, ponemos el rut emisor
	if (is_number(rut_firma1) is false) then
		rut_firma1:=rut_emisor1;
	end if;

        json3:=put_json(json3,'RUTEmisor',get_xml_hex1('RUTEmisor',xml_dte1));
        json2:=put_json(json2,'RUT_EMISOR',split_part(get_xml_hex1('RUTEmisor',xml_dte1),'-',1));
        json2:=put_json(json2,'MONTO_TOTAL',get_xml_hex1('MntTotal',xml_dte1));

        json3:=put_json(json3,'RutFirma',rut_firma1||'-'||modulo11(rut_firma1));
        json3:=put_json(json3,'Folio',folio1);
	json2:=put_json(json2,'FOLIO',folio1);
	json2:=put_json(json2,'TIPO_DTE',tipo_dte1);
        json3:=put_json(json3,'FchEmis',get_xml_hex1('FchEmis',xml_dte1));
        json3:=put_json(json3,'SUCURSAL',get_xml_hex1('CdgSIISucur',xml_dte1));
        json3:=put_json(json3,'CdgSIISucur',get_xml_hex1('CdgSIISucur',xml_dte1));
        json3:=put_json(json3,'URI_IN',uri1);

	json3:=put_json(json3,'fechaVencimiento','');
	json3:=put_json(json3,'NombreEmisor','');
        
	json3:=put_json(json3,'XSL','http://www.custodium.com/docs/otros/dte/dte.xsl');

        patron_dte1:=pg_read_file('./patron_dte_10k/patron_dte_sin_custodium_rec.xml');
        if (patron_dte1='' or patron_dte1 is null) then
		json2:=put_json(json2,'RESPUESTA','Status: 400 NK');
		json2:=logjson(json2,'Falla Insercion no existe patron de DTE');
		return json2;
        end if;
        json3:=escape_xml_characters(json3::varchar)::json;
        dte1:=remplaza_tags_json_c(json3,patron_dte1);
--      dte1:=limpia_tags(dte1);
	
	dte1:=encode(dte1::bytea,'hex');
	dte1:=replace(dte1,'2424245255545265636570242424',get_xml_hex('RUTRecep',xml_dte1));	
	dte1:=replace(dte1,'242424527a6e536f635265636570242424',get_xml_hex('RznSocRecep',xml_dte1));	
	dte1:=replace(dte1,'2424244469725265636570242424',get_xml_hex('DirRecep',xml_dte1));	
	dte1:=replace(dte1,'242424527a6e536f63242424',get_xml_hex('RznSoc',xml_dte1));	

        --xml_dte1:=replace(encode(dte1::bytea,'hex'),encode('#|#|#|#DTE#|#|#|#','hex'),xml_dte1);
        xml_dte1:=replace(dte1,encode('#|#|#|#DTE#|#|#|#','hex'),xml_dte1);

	json2:=put_json(json2,'INPUT',xml_dte1);
	json2:=put_json(json2,'CONTENT_LENGTH',(length(xml_dte1)/2)::varchar);
	json2:=put_json(json2,'URI_IN',uri1);
	json2:=put_json(json2,'__SECUENCIAOK__','40');
	return json2;

END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION public.genera_solicitud_crt_8031(json) RETURNS json AS $$
DECLARE
        json1                   alias for $1;
        json2                   json;
	cola1	bigint;
	nombre_tabla1	varchar;
	xml3	varchar;
	tx1	varchar;
	uri1	varchar;
	campo	record;
	id1 bigint;
BEGIN
        json2:=json1;

	uri1:=get_json('URI_IN',json2);
	--Revisamos si esta en la cola
	
	select * into campo from colas_motor_generica where uri=uri1 and categoria='ENVIO_CRT';
	if found then	
		json2:=logjson(json2,'CRT ya grabado en la cola. No se vuelve a grabar URI_DTE='||uri1);
		json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
		json2:=put_json(json2,'__SECUENCIAOK__','1000');
		return json2;
	end if;
	--Inserta en la cola la solicitud de CRT
	cola1:=nextval('id_cola_procesamiento');
        nombre_tabla1:='cola_motor_'||cola1::varchar;
	
	xml3:=put_campo('','INPUT',encode_hex(json2::varchar));
        xml3:=put_campo(xml3,'FECHA_INGRESO_COLA',now()::varchar);
        xml3:=put_campo(xml3,'TX','8032');
	tx1:='30';
	
        execute 'insert into ' || nombre_tabla1 || ' (fecha,uri,reintentos,data,tx,rut_emisor,reproceso,categoria,nombre_cola,rut_receptor,tipo_dte,folio) values ( now(),'||quote_literal(uri1)||',0,'||quote_literal(xml3)||','||tx1||','||quote_literal(get_json('RUT_EMISOR',json2))||',''NO'','||quote_literal('ENVIO_CRT')||','||quote_literal(nombre_tabla1)||','||quote_literal(get_json('RUT_RECEPTOR',json2))||','||quote_literal(get_json('TIPO_DTE',json2))||','||quote_literal(get_json('FOLIO',json2))||') returning id' into id1;
	if id1 is null then
		json2:=logjson(json2,'Falla encolar Envio CRT URI_DTE='||uri1);
		json2:=put_json(json2,'RESPUESTA','Status: 400 NK');
		json2:=put_json(json2,'__SECUENCIAOK__','1000');
		return json2;
	else
		json2:=logjson(json2,'Se encola Envio CRT URI_DTE='||uri1||' ID='||id1::varchar);
		--json2:=put_json(json2,'RESPUESTA','Status: 200 OK');	
		--json2:=put_json(json2,'__SECUENCIAOK__','1000');
		--return json2;
	end if;

	--json2:=logjson(json2,'DND - Verificamos '||get_json('RUT_RECEPTOR',json2));
	--DAO 20180418 Para RUTs que estan con el DND antiguo
	if (get_json('RUT_RECEPTOR',json2) in ('78549950','90703000','90266000','76041871')) then
		json2:=logjson(json2,'DND - Encolamos el Insert a Mordor'); 
                xml3:=put_campo('','TX','8060');
                xml3:=put_campo(xml3,'CATEGORIA','DND');
                xml3:=put_campo(xml3,'URI_IN',get_json('URI_IN',json2));
                xml3:=put_campo(xml3,'QUERY',encode_hex('select insert_document_dnd_motor('''||get_json('DOMINIO_EMISOR',json2)||''','''||get_json('URI_IN',json2)||''')'));
                execute 'insert into ' || nombre_tabla1 || ' (fecha,uri,reintentos,data,tx,rut_emisor,reproceso,categoria) values ( now()+interval ''2 hours'','||quote_literal(get_json('URI_IN',json2))||',0,'||quote_literal(xml3)||','||'10'||',null,''NO'',''ACT_REMOTO'') returning id' into id1;

		if id1 is null then
			json2:=logjson(json2,'Falla encolar Insert DND URI_DTE='||uri1);
			json2:=put_json(json2,'RESPUESTA','Status: 400 NK');
			json2:=put_json(json2,'__SECUENCIAOK__','1000');
			return json2;
		else
			json2:=logjson(json2,'Se encolar Insert DND URI_DTE='||uri1||' ID='||id1::varchar);
			json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
			json2:=put_json(json2,'__SECUENCIAOK__','1000');
			return json2;
		end if;
	end if;
	json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
	json2:=put_json(json2,'__SECUENCIAOK__','1000');
	return json2;

	return json2;	
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION public.genera_solicitud_sii_8031(varchar) RETURNS varchar AS $$
DECLARE
        xml1                   alias for $1;
        xml2                   varchar;
        cola1   bigint;
        nombre_tabla1   varchar;
        xml3    varchar;
        tx1     varchar;
        uri1    varchar;
        campo   record;
        id1 bigint;
BEGIN
	xml2:=xml1;
        --xml2:=logapp(xml2,'genera_solicitud_sii_8031 FECHA_EMISION='||get_campo('FECHA_EMISION',xml2));

	uri1:=get_campo('URI_IN',xml2);
        --Revisamos si esta en la cola
        select * into campo from colas_motor_generica where uri=uri1 and categoria='ESTADO_SII_REC';
        if found then
		xml2:=logapp(xml2,'Consulta Estado SII ya grabado en la cola. No se vuelve a grabar
URI_DTE='||uri1);
		xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
		xml2:=put_campo(xml2,'__SECUENCIAOK__','1000');
		return xml2;
        end if;
        --Inserta en la cola la solicitud de Consulta SII
	cola1:=nextval('id_cola_sii');
        nombre_tabla1:='cola_sii_'||cola1::varchar;
	
        xml2:=put_campo(xml2,'FECHA_INGRESO_COLA',now()::varchar);
        xml2:=put_campo(xml2,'TX','8033');
        tx1:='20';

        execute 'insert into ' || nombre_tabla1 || ' (fecha,uri,reintentos,data,tx,rut_emisor,reproceso,categoria,nombre_cola,rut_receptor,tipo_dte,folio) values ( now(),'||quote_literal(uri1)||',0,'||quote_literal(xml2)||','||tx1||','||quote_literal(get_campo('RUT_EMISOR',xml2))||',''NO'','||quote_literal('ESTADO_SII_REC')||','||quote_literal(nombre_tabla1)||','||quote_literal(get_campo('RUT_RECEPTOR',xml2))||','||quote_literal(get_campo('TIPO_DTE',xml2))||','||quote_literal(get_campo('FOLIO',xml2))||') returning id' into id1;
        if id1 is null then
		xml2:=logapp(xml2,'Falla encolar Consulta Estado SII URI_DTE='||uri1);
		xml2:=put_campo(xml2,'RESPUESTA','Status: 400 NK');
		xml2:=put_campo(xml2,'__SECUENCIAOK__','1000');
		return xml2;
        else
		xml2:=logapp(xml2,'Se encola Consulta Estado SII URI_DTE='||uri1||' ID='||id1::varchar);
		xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
		xml2:=put_campo(xml2,'__SECUENCIAOK__','1000');
                return xml2;
        end if;
        return xml2;
END;
$$ LANGUAGE plpgsql;

