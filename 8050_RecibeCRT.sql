delete from isys_querys_tx where llave='8050';

insert into isys_querys_tx values ('8050',5,1,14,'{"f":"INSERTA_JSON","p1":{"__SECUENCIAOK__":"10","__SOCKET_RESPONSE__":"RESPUESTA","__TIPO_SOCKET_RESPONSE__":"SCGI","RESPUESTA":"Status: 555 OK\nContent-Type: text/plain\n\n{\"STATUS\":\"Responde sin Espera\",\"__PROC_ACTIVOS__\":\"$$__PROC_ACTIVOS__$$\"}"}}',0,0,0,0,0,10,10);

insert into isys_querys_tx values ('8050',10,19,1,'select ensobra_crt_8050(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,1000);

--Primero que hacemos el publicar el CRT
insert into isys_querys_tx values ('8050',40,1,8,'Publica DTE',112704,0,0,0,0,50,50);

--Proceso el CRT
insert into isys_querys_tx values ('8050',50,19,1,'select verifica_publicacion_crt_8050(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('8050',60,19,1,'select envio_correo_op_venta_8050(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('8050',1000,19,1,'select sp_procesa_respuesta_cola_motor88_json(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,0,0);

CREATE or replace FUNCTION envio_correo_op_venta_8050(json) RETURNS json AS $$
DECLARE
	json1	alias for $1;
	json2	json;
	json4	json;
	jsonsts1	json;	
BEGIN
	json2:=json1;
        json2:=put_json(json2,'__SECUENCIAOK__','1000');
        json4:='{}';
        json4:=put_json(json4,'uri',get_json('URI_IN',json2));
        json4:=put_json(json4,'flag_data_xml','SI'); --Para que no saque el contenido de la URI
	if(get_json('CATEGORIA_MAIL',json2)='NAR') then
	        json4:=put_json(json4,'subject_hex',encode(('Producción - Intercambio - Revisión Comercial de '||get_json('RznSocResponde',json2)||' - RUT '||get_json('RUT_EMISOR',json2))::bytea,'hex'));
	else
	        json4:=put_json(json4,'subject_hex',encode(('Producción - Intercambio - Acuse de Recibo de Mercaderias de '||get_json('RznSocResponde',json2)||' - RUT '||get_json('RUT_EMISOR',json2))::bytea,'hex'));
	end if;
        json4:=put_json(json4,'from_hex',encode((get_json('RznSocResponde',json2)||' <'||get_json('MAIL_RECEPTOR',json2)||'>')::bytea,'hex')::varchar);
        json4:=put_json(json4,'to',translate(get_json('OP_VENTA',json2),'áéíóúÁÉÍÓÚ','aeiouAEIOU'));

        --json4:=put_json(json4,'bcc','fernando.arancibia@acepta.com');
        json4:=put_json(json4,'tipo_envio','HTML');
        json4:=put_json(json4,'content_html',get_json('HTML_OPVENTA',json2));
        --Buscamos el xsl que le corresponde
        --json4:=put_json(json4,'ip_envio','172.16.14.82');
        json4:=put_json(json4,'ip_envio','http://interno.acepta.com:8080/sendmail');
	json4:=put_json(json4,'RUT_OWNER',split_part(get_json('RUT_EMISOR',json2),'-',1));
	json4:=put_json(json4,'CATEGORIA','OPVENTA');
        --perform logfile('F_8030 select send_mail_python2('''||json4::varchar||''')');
        --raise notice 'xml=%',get_json('INPUT_CUSTODIUM',json4);
        --jsonsts1:=send_mail_python2(json4::varchar);
	jsonsts1:=send_mail_python2_colas(json4::varchar);
	if (get_json('status',jsonsts1)='OK') then
                json2:=logjson(json2,'Envio a OP_VENTA Exitoso '||get_json('OP_VENTA',json2)||' retorno='||get_json('retorno_send_mail',jsonsts1)||' msg-id='||get_json('msg-id',jsonsts1));
                json2:=logjson(json2,'Envio a OP_VENTA Exitoso '||get_json('to',json4)||' '||get_json('MAIL_RECEPTOR',json2));
                json2:= put_json(json2,'RESPUESTA','Status: 200 OK');
        else
                json2:=logjson(json2,'Envio a OP_VENTA Fallido jsonsts1='||jsonsts1::varchar||' '||get_json('OP_VENTA',json2)||' '||get_json('URI_IN',json2));
                json2:= put_json(json2,'RESPUESTA','Status: 400 NK');
        end if;

	return json2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION graba_bitacora_8050(varchar) RETURNS varchar AS $$
DECLARE
        xml1        alias for $1;
        xml2    varchar;
	json_detalle1	json;
	detalle_crt	varchar;
	aux1	varchar;
	json_aux	json;
	xml3 	varchar;
	campo 	record;	
	tipo1	varchar;
	folio1	varchar;
	rut_emisor1	varchar;
	rut_receptor1	varchar;
	i	integer;
	aux	varchar;
	categoria1	varchar;
	html1		varchar;
        patron_html1     varchar;
	json3	json;
	mail1	varchar;
BEGIN
    xml2:=xml1;
        categoria1:=get_campo('CATEGORIA_MAIL',xml2);
        xml2:=logapp(xml2,'Se procesa detalle '||categoria1);
        html1:='';
	json_aux:=get_campo('DETALLE_DOC_CRT',xml2)::json;
	rut_emisor1:=split_part(get_json('RUT_EMISOR',json_aux),'-',1);
	tipo1:=get_json('TIPO_DTE',json_aux);
	folio1:=get_json('FOLIO',json_aux);
    	xml2:=logapp(xml2,'Procesa '||i::varchar||' '||rut_emisor1||' '||tipo1||' '||folio1);
	if(is_number(rut_emisor1) is false or is_number(tipo1) is false or is_number(folio1) is false) then
		xml2:=logapp(xml2,'Datos no numericos... rut_emisor1='||rut_emisor1||' tipo1='||tipo1||' folio1='||folio1);
		return xml2;
	end if;
	select * into campo from dte_emitidos where rut_emisor=rut_emisor1::bigint and tipo_dte=tipo1::integer and folio=folio1::bigint;
	if not found then
		xml2:=logapp(xml2,'No se encuentra DTE en dte_emitidos... rut_emisor1='||rut_emisor1||' tipo1='||tipo1||' folio1='||folio1);
		return xml2;
	else
		xml2:=logapp(xml2,'Se encuentra DTE emitido con URI='||campo.uri||' para '||categoria1);
	end if;
	rut_receptor1:=split_part(get_json('RUT_RECEPTOR',json_aux),'-',1);
	--FAY-DAO si es vacio el rut receptor, lo tomo del dte_emitido
	if rut_receptor1='' then
		rut_receptor1:=campo.rut_receptor::Varchar;
	end if;
	xml3:='';
        xml3:=put_campo(xml3,'RUT_EMISOR',rut_emisor1);
        xml3:=put_campo(xml3,'RUT_OWNER',rut_emisor1);
        xml3:=put_campo(xml3,'FECHA_EVENTO',now()::varchar);
	xml3:=put_campo(xml3,'FECHA_EMISION',campo.fecha_emision::timestamp::varchar);
	xml3:=put_campo(xml3,'RUT_RECEPTOR',rut_receptor1);
	xml3:=put_campo(xml3,'FOLIO',folio1);
	xml3:=put_campo(xml3,'TIPO_DTE',tipo1);
	xml3:=put_campo(xml3,'CANAL','EMITIDOS');
	xml3:=put_campo(xml3,'__FLUJO_ACTUAL__',get_campo('__FLUJO_ACTUAL__',xml2));
	xml3:=put_campo(xml3,'URL_GET',get_campo('URI_IN',xml2));
	xml3:=put_campo(xml3,'URI_IN',campo.uri);

	if(categoria1='NAR') then
		html1:=html1||' <tr><td class="url"><a href="'||get_campo('URI_IN',xml2)||'">abrir</a></td><td class="rut">'||rut_emisor1||'-'||modulo11(rut_emisor1)||'</td><td>'||tipo1||'</td><td>'||folio1||'</td><td>'||campo.fecha_emision::timestamp::varchar||'</td><td>'||campo.monto_total::varchar||'</td><td>'||get_json('ESTADO',json_aux)||'</td><td class="detalle">'||escape_xml(get_json('GLOSA',json_aux))::varchar||'</td></tr>';

		if(get_json('ESTADO',json_aux) in ('0','1')) then
			xml3:=put_campo(xml3,'EVENTO','NRE');
		else
			xml3:=put_campo(xml3,'EVENTO','RRE');
		end if;
		mail1:=(select trim(email) from contribuyentes where rut_emisor=rut_receptor1::bigint);
		xml2:=put_campo(xml2,'MAIL_RECEPTOR',mail1);
		xml3:=put_campo(xml3,'COMENTARIO_TRAZA','Emite: '||mail1||chr(10)||'Glosa: '||escape_xml(get_json('GLOSA',json_aux))::varchar||' ('||get_json('ESTADO',json_aux)||')');
	elsif(categoria1='ARM') then
		--2018-02-02 Si se recibe un ARM no es necesrio marcar el dte_Recibidos y se corrige el CANAL que es realmente RECIBIDOS
		xml3:=put_campo(xml3,'CANAL','RECIBIDOS');
		mail1:=(select trim(email) from contribuyentes where rut_emisor=rut_receptor1::bigint);
		xml2:=put_campo(xml2,'MAIL_RECEPTOR',mail1);
		html1:=html1||' <tr><td class="url"><a href="'||get_campo('URI_IN',xml2)||'">abrir</a></td><td class="rut">'||rut_emisor1||'-'||modulo11(rut_emisor1)||'</td><td>'||tipo1||'</td><td>'||folio1||'</td><td>'||campo.fecha_emision::timestamp::varchar||'</td><td>'||campo.monto_total::varchar||'</td><td>APROBADO</td><td class="detalle">'||escape_xml(get_json('RECINTO',json_aux))::varchar||'</td></tr>';
		xml3:=put_campo(xml3,'EVENTO','ARM');
		xml3:=put_campo(xml3,'COMENTARIO_TRAZA','Recibe: '||(select email from contribuyentes where rut_emisor=rut_emisor1::bigint)||chr(10)||'Recinto: '||escape_xml(get_json('RECINTO',json_aux))::varchar);
	elsif(categoria1='CRT') then
		xml3:=put_campo(xml3,'EVENTO','PRE');
		xml3:=put_campo(xml3,'COMENTARIO_TRAZA','Emite: '||(select email from contribuyentes where rut_emisor=rut_receptor1::bigint)||chr(10)||'Glosa: '||escape_xml(get_json('GLOSA',json_aux))::varchar||' ('||get_json('ESTADO',json_aux)||')');
	end if;
	xml3:=graba_bitacora_aws(xml3,get_campo('EVENTO',xml3));
	xml2:=logapp(xml2,get_campo('_LOG_',xml3));
	
	return xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION verifica_publicacion_crt_8050(varchar) RETURNS varchar AS $$
DECLARE
        xml1        alias for $1;
        xml2    varchar;
	json_detalle1	json;
	detalle_crt	varchar;
	aux1	varchar;
	json_aux	json;
	xml3 	varchar;
	xml7 	varchar;
	id1	bigint;
	campo 	record;	
	tipo1	varchar;
	folio1	varchar;
	rut_emisor1	varchar;
	rut_receptor1	varchar;
	i	integer;
	aux	varchar;
	categoria1	varchar;
	html1		varchar;
        patron_html1     varchar;
	json3	json;
	mail1	varchar;
BEGIN
    xml2:=xml1;
    xml2 := put_campo(xml2,'__SECUENCIAOK__','1000');
    if (get_campo('__PUBLICADO_OK__',xml2)<>'SI') then
    	xml2:=logapp(xml2,'Falla la Publicacion CRT en Almacen '||get_campo('URI_IN',xml2));
    	xml2 := put_campo(xml2,'RESPUESTA','Status: 400 NK');
	return xml2;
    end if;

    --Recorremos el detalle del CRT para grabar los respectivos eventos...
    detalle_crt:=get_campo('DETALLE',xml2);
    BEGIN
	json_detalle1:=detalle_crt::json;	
    EXCEPTION WHEN OTHERS THEN
	xml2:=logapp(xml2,'Detalle CRT no es un json valido '||get_campo('URI_IN',xml2));
    	xml2 := put_campo(xml2,'RESPUESTA','Status: 400 NK');
        return xml2;
    END;

    categoria1:=get_campo('CATEGORIA_MAIL',xml2);
    xml2:=logapp(xml2,'Se procesa detalle '||categoria1);
    html1:='';
    i:=0;
    aux:=get_json_index(json_detalle1,i);
    while(aux<>'') loop
	xml3:='';
	xml3:=put_campo(xml3,'CATEGORIA_MAIL',categoria1);
	xml3:=put_campo(xml3,'URI_IN',get_campo('URI_IN',xml2));
	xml3:=put_campo(xml3,'DETALLE_DOC_CRT',aux::varchar);

	--Grabamos en en las colas para q ejecute 8060 yendo a motor por cada registro
	xml7:=put_campo('','TX','8060');
        xml7:=put_campo(xml7,'CATEGORIA','MOTOR');
        xml7:=put_campo(xml7,'SUB_CATEGORIA','GRABA_BITACORA_8050');
        xml7:=put_campo(xml7,'URI_IN',get_campo('URI_IN',xml2));
        xml7:=put_campo(xml7,'QUERY',encode_hex('select graba_bitacora_8050('||quote_literal(xml3)||')'));
        execute 'insert into cola_motor_10 (fecha,uri,reintentos,data,tx,rut_emisor,reproceso,categoria) values ( now(),'||quote_literal(get_campo('URI_IN',xml2))||',0,'||quote_literal(xml7)||','||'10'||',null,''NO'',''ACT_REMOTO'') returning id' into id1;
	xml2:=logapp(xml2,'Encolo GRABA_BITACORA_8050 ID='||id1::varchar);
	
	i:=i+1;
	aux:=get_json_index(json_detalle1,i);
    end loop;
    if(categoria1 in ('NAR','ARM') and trim(get_campo('OP_VENTA',xml2))<>'') then
	patron_html1:=pg_read_file('./patron_dte_10k/patron_op_venta.html');
	if (patron_html1='' or patron_html1 is null) then
		xml2 := put_campo(xml2,'RESPUESTA','Status: 400 NK');
		xml2:=logapp(xml2,'Falla NO existe patron de HTML patron_op_venta.html');
		return xml2;
	end if;
	json3:='{}';
	if(categoria1='NAR') then
		json3:=put_json(json3,'TITULO','Intercambio - Revisi&oacute;n Comercial de DTE');
		json3:=put_json(json3,'TITULO_H1','Producci&oacute;n &middot; Intercambio - Revisi&oacute;n Comercial de DTE');
	else
		json3:=put_json(json3,'TITULO','Intercambio - Acuse de Recibo de Mercaderias');
		json3:=put_json(json3,'TITULO_H1','Producci&oacute;n &middot; Intercambio - Acuse de Recibo de Mercaderias');
	end if;
	json3:=put_json(json3,'TABLA',html1);
	json3:=put_json(json3,'RznSocEmisor',escape_xml(get_campo('RznSocResponde',xml2))::varchar);
	json3:=put_json(json3,'RznSocReceptor',escape_xml(get_campo('RznSocRecibe',xml2))::varchar);
	json3:=put_json(json3,'RutReceptor',rut_emisor1||'-'||modulo11(rut_emisor1));
	json3:=put_json(json3,'RutEmisor',rut_receptor1||'-'||modulo11(rut_receptor1));
	json3:=put_json(json3,'FchRecep',to_char(now(),'DD/MM/YYYY'));
	html1:=remplaza_tags_json_c(json3,patron_html1);
	html1:=limpia_tags(html1);
	xml2 := put_campo(xml2,'HTML_OPVENTA',encode(html1::bytea,'hex'));
	xml2 := put_campo(xml2,'__SECUENCIAOK__','60');
	return xml2;
    end if;
    xml2 := put_campo(xml2,'RESPUESTA','Status: 200 OK');
    xml2 := put_campo(xml2,'__SECUENCIAOK__','1000');

    return xml2;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION public.ensobra_crt_8050(json) RETURNS json AS $$
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
        xml3            varchar;

	rut_emisor1	varchar;
	tipo_dte1	varchar;
	folio1		varchar;
	tms1		varchar;
	tms2		timestamp;
	certificado_x509	varchar;
	aux			varchar;
	rut_firma1		varchar;

	json_in	json;
	categoria1	varchar;
	rz_emi1		varchar;
	rz_rec1		varchar;
BEGIN
        json2:=json1;
	json2:=put_json(json2,'__SECUENCIAOK__','1000');
		

	--Sacamos el xml del mail
	xml_dte1:=get_json('XML',json2);
	--Limpio XML
	json2:=put_json(json2,'XML','');
	categoria1:=get_json('CATEGORIA_MAIL',json2);
	--Para corregir Visualizacion
	json2:=logjson(json2,'Entra un '||categoria1||' rut='||get_json('RUT_EMISOR',json2)||' tipo='||get_json('TIPO_DTE',json2)||' Folio='||get_json('FOLIO',json2)||' eml='||get_json('eml',json2));
	if(categoria1 in ('CRT')) then
		--Verificamos como viene Resultado
		if (strpos(xml_dte1,encode('<Resultado ','hex'))>0) then
			xml_dte1:=encode('<RespuestaDTE xmlns="http://www.sii.cl/SiiDte" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" version="1.0"><Resultado ','hex')||split_part(xml_dte1,encode('<Resultado ','hex'),2);
		else
			xml_dte1:=encode('<RespuestaDTE xmlns="http://www.sii.cl/SiiDte" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" version="1.0"><Resultado>','hex')||split_part(xml_dte1,encode('<Resultado>','hex'),2);
		end if;
		--json2:=logjson(json2,'xml_dte1='||xml_dte1);
		tipo_dte1:=get_xml_hex1('TipoDTE',xml_dte1);
		tms1:=get_xml_hex1('TmstFirmaResp',xml_dte1);
        	rut1:=split_part(get_xml_hex1('RutRecibe',xml_dte1),'-',1);
		--FAY si no viene el RutRecibe lo sacamos de RUTEmisor
		if (rut1='') then
			rut1:=split_part(get_xml_hex1('RUTEmisor',xml_dte1),'-',1);
		end if;
		rut_emisor1:=split_part(get_xml_hex1('RutResponde',xml_dte1),'-',1);
		--FAY si no viene el RutResponde lo sacamos del RUTRecep
		if (rut_emisor1='') then
			rut_emisor1:=split_part(get_xml_hex1('RUTRecep',xml_dte1),'-',1);
		end if;
		json2:=logjson(json2,'CRT tipo_dte1='||tipo_dte1::varchar||' firma='||tms1);
	elsif(categoria1 in ('NAR')) then
		--Verificamos como viene Resultado
		if (strpos(xml_dte1,encode('<Resultado ','hex'))>0) then
			xml_dte1:=encode('<RespuestaDTE xmlns="http://www.sii.cl/SiiDte" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" version="1.0"><Resultado ','hex')||split_part(xml_dte1,encode('<Resultado ','hex'),2);
		elsif (strpos(xml_dte1,encode('<Resultado>','hex'))>0) then
			xml_dte1:=encode('<RespuestaDTE xmlns="http://www.sii.cl/SiiDte" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" version="1.0"><Resultado>','hex')||split_part(xml_dte1,encode('<Resultado>','hex'),2);
		elsif (strpos(xml_dte1,encode('<ResultadoDTE>','hex'))>0) then
			xml_dte1:=encode('<RespuestaDTE xmlns="http://www.sii.cl/SiiDte" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" version="1.0"><ResultadoDTE>','hex')||split_part(xml_dte1,encode('<ResultadoDTE>','hex'),2);
		else
			xml_dte1:='';
		end if;
		--json2:=logjson(json2,'xml_dte1='||xml_dte1);
		tipo_dte1:=get_xml_hex1('TipoDTE',xml_dte1);
		tms1:=get_xml_hex1('TmstFirmaResp',xml_dte1);
        	rut1:=split_part(get_xml_hex1('RutRecibe',xml_dte1),'-',1);
		rut_emisor1:=split_part(get_xml_hex1('RutResponde',xml_dte1),'-',1);
		json2:=logjson(json2,'NAR tipo_dte1='||tipo_dte1::varchar||' firma='||tms1);
	elsif(categoria1='ARM') then
		if (strpos(xml_dte1,encode('<SetRecibos ','hex'))>0) then
			--ARM <EnvioRecibos
			xml_dte1:=encode('<EnvioRecibos xmlns="http://www.sii.cl/SiiDte" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" version="1.0"><SetRecibos ','hex')||split_part(xml_dte1,encode('<SetRecibos ','hex'),2);
        		rut1:=split_part(get_xml_hex1('RutRecibe',xml_dte1),'-',1);
			rut_emisor1:=split_part(get_xml_hex1('RutResponde',xml_dte1),'-',1);
			json2:=logjson(json2,'ARM SetRecibos');
		else
			xml_dte1:=encode('<Recibo xmlns="http://www.sii.cl/SiiDte" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" version="1.0"><DocumentoRecibo ','hex')||split_part(xml_dte1,encode('<DocumentoRecibo ','hex'),2);
			--Viene solo el emisor que es nuestro cliente (rut recibe)
        		rut1:=split_part(get_xml_hex1('RUTEmisor',xml_dte1),'-',1);
			rut_emisor1:=split_part(get_xml_hex1('RUTRecep',xml_dte1),'-',1);
			json2:=logjson(json2,'ARM DocumentoRecibo');
		end if;
		tipo_dte1:=get_xml_hex1('TipoDoc',xml_dte1);
		tms1:=get_xml_hex1('TmstFirmaRecibo',xml_dte1);
		--FAY si no viene TmstFirmaRecibo tomo la firma del sobre
		if (tms1='') then
			tms1:=get_xml_hex1('TmstFirmaEnv',xml_dte1);
		end if;
	else
		json2:=logjson(json2,'Categoria no reconocida '||categoria1);
		json2:=put_json(json2,'RESPUESTA','Status: 444 OK');
		return json2;
	end if;
	--Para corregir la visualizacion
	xml_dte1:=replace(xml_dte1,'1013','10');

        --rut1:=split_part(get_xml_hex1('RutRecibe',xml_dte1),'-',1);
	
	json2:=logjson(json2,'Rut Receptor='||rut1::varchar||' file='||get_json('NmbEnvio',json2)||' adjuntos='||get_json('adjuntos',json2)||' eml='||get_json('eml',json2));
	if(is_number(rut1) is false) then
		json2:=put_json(json2,'RESPUESTA','Status: 444 NK');
		json2:=put_json(json2,'MENSAJE_XML_FLAGS','(CRT) Rut Receptor no numerico');
		json2:=logjson(json2,'Rut Receptor no numerico '||rut1::varchar);
		return json2;
	end if;
	if(is_number(rut_emisor1) is false) then
		json2:=put_json(json2,'RESPUESTA','Status: 444 NK');
		json2:=put_json(json2,'MENSAJE_XML_FLAGS','(CRT) Rut Emisor no numerico');
		json2:=logjson(json2,'Rut Emisor no numerico '||rut_emisor1::varchar);
		return json2;
	end if;
		
        select * into campo from dominios_maestro_clientes where rut_emisor=rut1::integer;
	if not found then
		if(now()-get_json('FECHA_INGRESO_COLA',json2)::timestamp>interval '3 days') then
			json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
			json2:=logjson(json2,'Rut Receptor no se encuentra en maestro_clientes '||rut1::varchar||' Se borra de la cola (5 dias)');
		else
        		select * into campo from dominios_maestro_clientes where rut_emisor=rut_emisor1::integer;
			if not found then
				--Si el otro rut tampoco esta en el MC
				json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
				json2:=logjson(json2,'Ningun rut en el maestro_clientes se borra');
			else
				json2:=put_json(json2,'RESPUESTA','Status: 444 NK');
				json2:=put_json(json2,'MENSAJE_XML_FLAGS','(CRT) Rut Receptor no se encuentra en maestro_clientes');
				json2:=logjson(json2,'Rut Receptor no se encuentra en maestro_clientes '||rut1::varchar);
			end if;
		
		end if;
		return json2;
	end if;
	json2:=put_json(json2,'OP_VENTA',trim(campo.op_venta));

	--Si esta bloqueado lo dejo en las colas
	if (strpos(campo.estado,'BLOQUEADO')>0) then
		if(now()-get_json('FECHA_INGRESO_COLA',json2)::timestamp>interval '30 days') then
			json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
			json2:=logjson(json2,'Cliente Bloqueado en maestro_clientes '||rut1::varchar||' Se borra de la cola (30 dias)');
		else
			--Si el mail del contribuyente no es custodium, el cliente ya no es de acepta
			if (strpos(campo.email_contribuyente,'custodium')=0) then
				json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
				json2:=logjson(json2,'Cliente Bloqueado en maestro_clientes '||rut1::varchar||' y no es correo custodium, se ignora el CRT');
				return json2;
			end if;

			json2:=put_json(json2,'RESPUESTA','Status: 444 NK');
			json2:=logjson(json2,'Cliente bloqueado en maestro de clientes, se mantiene en la cola '||rut1);
			json2:=put_json(json2,'MENSAJE_XML_FLAGS','(CRT) Cliente Bloqueado en maestro de clientes');
		end if;
		return json2;
	end if;

	--Si no tiene dominio, le asigno un webdte
	if (campo.dominio is null) then
		campo.dominio:='webdte';
		json2:=logjson(json2,'Dominio nulo en maestro de clientes, se asigna dominio webdtei RUT='||rut1);
	end if;

	folio1:=get_xml_hex1('Folio',xml_dte1);
	
	if(is_number(rut_emisor1) is false or is_number(tipo_dte1) is false or is_number(folio1) is false) then
		json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
		json2:=logjson(json2,'Datos Inválidos, no numéricos. rut_emisor1='||rut_emisor1||' folio1='||folio1||' tipo_dte1='||tipo_dte1);
		return json2;
	end if;
	BEGIN
		tms2:=tms1::timestamp;
	EXCEPTION WHEN OTHERS THEN
		json2:=put_json(json2,'RESPUESTA','Status: 400 NK');
		json2:=logjson(json2,'Fecha Firma Inválida '||tms1::varchar||' Se asume now()');
		--FAY el tms1 solo sirve para generar la URI, se asume now
		tms2:=now();
		--return json2;
	END;
	json2:=put_json(json2,'TmstFirma',tms1);

        uri1:='http://'||campo.dominio||to_char(now(),'YYMM')||'.acepta.com/v01/'||genera_uri(rut_emisor1,tipo_dte1,folio1,tms1,'R');
	json3:=put_json(json3,'Dominio',campo.dominio);
	json2:=put_json(json2,'DOMINIO_EMISOR',campo.dominio);

        json3:='{}';
        --json3:=put_json(json3,'RUTEmisor',get_xml_hex1('RutResponde',xml_dte1));
        json3:=put_json(json3,'RUTEmisor',rut_emisor1||'-'||modulo11(rut_emisor1::varchar));
        --json3:=put_json(json3,'RutFirma',get_xml_hex1('RutRecibe',xml_dte1));
        json3:=put_json(json3,'RutFirma',rut1||'-'||modulo11(rut1::varchar));
	rz_emi1:=(select nombre from contribuyentes where rut_emisor=rut_emisor1::integer limit 1);
	rz_rec1:=(select nombre from contribuyentes where rut_emisor=rut1::integer limit 1);
        --json3:=put_json(json3,'RznSocResponde',(select nombre from contribuyentes where rut_emisor=rut_emisor1::integer limit 1));
        json3:=put_json(json3,'RznSocResponde',rz_emi1);
        json2:=put_json(json2,'RznSocResponde',rz_emi1);
        --json3:=put_json(json3,'RznSocRecibe',(select nombre from contribuyentes where rut_emisor=rut1::integer limit 1));
        json3:=put_json(json3,'RznSocRecibe',rz_rec1);
        json2:=put_json(json2,'RznSocRecibe',rz_rec1);
        json3:=put_json(json3,'URI_IN',uri1);
	if(categoria1='NAR') then
		json3:=put_json(json3,'XSL','http://www.custodium.com/intercambio/notificacion.xsl');
	elsif(categoria1='ARM') then
		json3:=put_json(json3,'XSL','http://www.custodium.com/docs/arm/arm.xsl');
	elsif(categoria1='CRT') then
		json3:=put_json(json3,'XSL','http://www.custodium.com/docs/otros/comprobanterecepcion/comprobanterecepcion.xsl');
	end if;

        patron_dte1:=pg_read_file('./patron_dte_10k/patron_dte_sin_custodium_crt.xml');
        if (patron_dte1='' or patron_dte1 is null) then
		json2:=put_json(json2,'RESPUESTA','Status: 400 NK');
		json2:=logjson(json2,'Falla Insercion no existe patron de DTE');
		return json2;
        end if;
        json3:=escape_xml_characters(json3::varchar)::json;
        dte1:=remplaza_tags_json_c(json3,patron_dte1);
        dte1:=limpia_tags(dte1);

        xml_dte1:=replace(encode(dte1::bytea,'hex'),encode('#|#|#|#DTE#|#|#|#','hex'),xml_dte1);
	json2:=logjson(json2,'URI_CRT='||uri1);

	json2:=put_json(json2,'INPUT',xml_dte1);
	json2:=put_json(json2,'CONTENT_LENGTH',(length(xml_dte1)/2)::varchar);
	json2:=put_json(json2,'URI_IN',uri1);
	json2:=put_json(json2,'__SECUENCIAOK__','40');
	return json2;

END;
$$ LANGUAGE plpgsql;
