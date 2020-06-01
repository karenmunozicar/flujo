--Publica documento
delete from isys_querys_tx where llave='12762';
--Obtiene el DTE Original con la entrada URI_IN
insert into isys_querys_tx values ('12762',10,1,1,'select xml_firma1_12762(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

--insert into isys_querys_tx values ('12762',20,1,10,'$$SCRIPT$$',0,0,0,1,1,21,21);
insert into isys_querys_tx values ('12762',20,1,2,'Servicio de Firma 192.168.3.17',4013,109,106,0,0,30,30);

insert into isys_querys_tx values ('12762',30,1,1,'select xml_firma2_12762(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

--insert into isys_querys_tx values ('12762',40,1,10,'$$SCRIPT$$',0,0,0,1,1,41,41);
insert into isys_querys_tx values ('12762',40,1,2,'Servicio de Firma 192.168.3.17',4013,109,106,0,0,50,50);

insert into isys_querys_tx values ('12762',50,1,1,'select arm_resp_12762(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

--Se envia al EDTE
insert into isys_querys_tx values ('12762',60,1,8,'Llamada ARM EDTE',12780,0,0,0,0,65,65);

--Publicamos el ARM
insert into isys_querys_tx values ('12762',65,1,8,'Publica DTE',12704,0,0,0,0,70,70);

--Validamos la publicacion
insert into isys_querys_tx values ('12762',70,1,1,'select valida_publicacion_arm_12762(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);



CREATE or replace FUNCTION valida_publicacion_arm_12762(varchar) RETURNS varchar AS $$
declare
	xml1	alias for $1;
	xml2	varchar;
	json2	varchar;
	rut1	varchar;
	stContribuyente	contribuyentes%ROWTYPE;
	mail1	varchar;
begin
	xml2:=xml1;
	xml2:=put_campo(xml2,'__SECUENCIAOK__','0');
        json2:=decode(get_campo('JSON_IN',xml2),'hex')::varchar;
	if (get_campo('__PUBLICADO_OK__',xml2)<>'SI') then
                  xml2:=response_requests_5000('2', 'Falla Publicacion de ARM', '',xml2,json2);
		  return xml2;
	end if;

	--Si lo escribi bien en el EDTE
	if (get_campo('__EDTE_ARM_OK__',xml2)<>'SI') then
                  xml2:=response_requests_5000('2', 'Falla Envio de ARM', '',xml2,json2);
		  return xml2;
	end if;

	--2015-04-30 FAY,RME Se graba inmediatamente el Evento ARM para el DTE recibido
        rut1:=get_campo('RUT_EMISOR',xml2);
        select * into stContribuyente from contribuyentes where rut_emisor=rut1::integer;
        if not found then
                xml2:=logapp(xml2,'ARM: Rut Emisor del DTE Recibido no registrado en contribuyentes');
                mail1:='Sin mail de intercambio';
        else
                mail1:=stContribuyente.email;
        end if;
        xml2:=put_campo(xml2,'COMENTARIO_TRAZA','Recibe: '||mail1||chr(10)||get_campo('RECINTO',xml2)||'.');
        --Se asignan las uris para grabar el evento en traza
        xml2:=put_campo(xml2,'URL_GET',get_campo('URI_IN',xml2));
        xml2:=put_campo(xml2,'URI_IN',get_campo('URI_DTE',xml2));
        xml2:=graba_bitacora2(xml2,'ARM');
        --Vuelo a dejar en uri_in la uri del ARM
        xml2:=put_campo(xml2,'URI_IN',get_campo('URL_GET',xml2));

        xml2:=response_requests_5000('1', 'ARM firmado',get_campo('URI_IN',xml2),xml2,json2);
   	return xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION xml_firma1_12762(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
        xml2    varchar;
	json1	varchar;
	json2	varchar;
	rut1	varchar;
	data1	varchar;
	tipo_dte1	varchar;
	folio1		varchar;
	recinto1	varchar;
	query1		varchar;
	campo1              RECORD;
	EncabezadoCusDoc    varchar;
    	pieFirma            varchar;
    	pieCusDoc           varchar;
    	id1                 varchar;
    	id2                 varchar;
	dominio1            varchar;
    	request1            varchar;
	xml_resp1		varchar;
	data_firma1	varchar;
BEGIN
    xml2:=xml1;
    xml2:=logapp(xml2,'COMIENZA FLUJO 12762');
    json1:=decode(get_campo('JSON_IN',xml2),'hex')::varchar;
    json2:=json1;

	rut1:=json_get('rutDte',json2);
        tipo_dte1:=json_get('tipoDte',json2);
        folio1:=replace(json_get('folioDte',json2),'.','');
        recinto1:=decode(json_get('recinto',json2),'hex');
	xml2:=put_campo(xml2,'RECINTO',recinto1);


        xml2:=logapp(xml2,'Datos para formar xml ARM rut1=' || rut1);
        xml2:=logapp(xml2,'Datos para formar xml ARM tipo_dte1=' || tipo_dte1);
        xml2:=logapp(xml2,'Datos para formar xml ARM folio1=' || folio1);
        xml2:=logapp(xml2,'Datos para formar xml ARM recinto1=' || recinto1);
        xml2:=logapp(xml2,'Datos para formar xml ARM rut_firma=' || json_get('rut_firma',json2));

        if((rut1 || tipo_dte1 ||folio1 ||recinto1 ||json_get('rut_firma',json2))=NULL) then
                xml2:=response_requests_5000('2', 'Alguno de los campos obligatorios es Nulo','', xml2,json2);
                return xml2;
        end if;

        xml2:=logapp(xml2,'rut1= ' || rut1 || ' tipo_dte1= ' || tipo_dte1 || ' folio1= ' || folio1 || ' recinto1=' ||recinto1);

        if(folio1='' or is_number(folio1) is false) then
                xml2:=response_requests_5000('2', 'Folio Incorrecto', '', xml2,json2);
                return xml2;
        end if;

        if(tipo_dte1='-1') then
                xml2:=response_requests_5000('400', 'Tipo DTE Incorrecto', '', xml2,json2);
                return xml2;
        end if;
--      xml2:=logapp(xml2,'select xml= ' || 'SELECT * INTO exists_select1 FROM dte_recibidos WHERE rut_emisor=' || rut1 ||'::integer and tipo_dte=' || tipo_dte1 || '::integer and folio=' || '::integer;');
        xml2:=logapp(xml2,'RUT='||rut1);
        --query1:='SELECT rut_emisor,rut_receptor,tipo_dte,folio,fecha_emision,monto_total,estado, to_char(current_timestamp, ''YYYY-MM-DD HH:MM:SS'') as time FROM dte_recibidos WHERE rut_receptor=' || rut1 ||'::integer and tipo_dte=' || tipo_dte1 || '::integer and folio=' || folio1 || '::integer;';
        SELECT codigo_txel,rut_emisor,rut_receptor,tipo_dte,folio,fecha_emision,monto_total,estado,uri_arm, to_char(current_timestamp, 'YYYY-MM-DD HH:MM:SS') as time,uri into campo1 FROM dte_recibidos WHERE rut_receptor=rut1::integer and tipo_dte=tipo_dte1::integer and folio=folio1::integer;
	if found then
		--Si ya esta firmado
		if campo1.uri_arm is not null then
			xml2:=logapp(xml2,'ARM ya procesado'||campo1.uri_arm);
                        xml2:=response_requests_5000('1', 'ARM ya procesado',campo1.uri_arm,xml2,json2);
			return xml2;
		end if;
		xml2:=put_campo(xml2,'URI_DTE',campo1.uri);
		xml2:=put_campo(xml2,'RUT_EMISOR',campo1.rut_emisor::varchar);
		xml2:=put_campo(xml2,'RUT_RECEPTOR',campo1.rut_receptor::varchar);
		xml2:=put_campo(xml2,'FECHA_EMISION',campo1.fecha_emision::varchar);
		xml2:=put_campo(xml2,'MONTO_TOTAL',campo1.monto_total::varchar);
		xml2:=put_campo(xml2,'TIPO_DTE',campo1.tipo_dte::varchar);
		xml2:=put_campo(xml2,'CODIGO_TXEL_ARM',campo1.codigo_txel::varchar);
		xml2:=put_campo(xml2,'RUT_EMISOR_ARM',campo1.rut_emisor::varchar);
		xml2:=put_campo(xml2,'TIPO_DTE_ARM',campo1.tipo_dte::varchar);
		xml2:=put_campo(xml2,'FOLIO_ARM',campo1.folio::varchar);
                id1:=get_newRespuestaID_2116(campo1.rut_emisor::varchar, campo1.rut_receptor::varchar,campo1.tipo_dte::varchar,campo1.folio::varchar,campo1.fecha_emision::varchar, 'Recibo');
                id2:=replace(id1,'Recibo','SetRecibo');
		xml2:=logapp(xml2,'Rut Emisor='||campo1.rut_receptor::varchar);
                SELECT coalesce(dominio,'') FROM maestro_clientes WHERE rut_emisor = campo1.rut_receptor INTO dominio1;
                if not found then
                        dominio1:='webdte';
                end if;
		--Armamos la URI del ARM
		--http://lg1504.acepta.com/v01/b3dc07b386b63ec6e8d7e334623afd8a77de91fe?k=cd79851b62204f6280ba3eea5893ed89
		xml2:=put_campo(xml2,'URI_IN','http://'||dominio1||to_char(now(),'YYMM')||'.acepta.com/v01/'||lpad('_'||replace(getipserver('eth0'),'.','')||'_'||to_char(now(),'DDHH24MISSMI')||'_'||nextval('correlativo_uri')::varchar||'_',40,'0')||'?k='||md5(coalesce(dominio1,'X')));
	
                EncabezadoCusDoc:='<?xml version="1.0" encoding="ISO-8859-1"?>' || chr(10);
                EncabezadoCusDoc:=EncabezadoCusDoc || '<?xml-stylesheet type="text/xsl" href="http://www.custodium.com/docs/arm/arm.xsl"?> '|| chr(10);
                EncabezadoCusDoc:=EncabezadoCusDoc || '<Document Domain="' ||dominio1|| '" Type="Intercambio">'|| chr(10);
                EncabezadoCusDoc:=EncabezadoCusDoc || '<Issuer><PI Type="Rut">' ||campo1.rut_receptor || '-' || modulo11(campo1.rut_receptor::varchar) ||'</PI></Issuer>' || chr(10)     ;
                EncabezadoCusDoc:=EncabezadoCusDoc || '<Signers><Signer><PI Type="Rut">'||json_get('rut_firma',json2)||'</PI></Signer></Signers>'|| chr(10);
                EncabezadoCusDoc:=EncabezadoCusDoc || '<Recipients><Recipient><PI Type="Rut">' ||campo1.rut_emisor || '-' || modulo11(campo1.rut_emisor::varchar)||'</PI></Recipient></Recipients>'|| chr(10);
                EncabezadoCusDoc:=EncabezadoCusDoc || '<Attributes>'|| chr(10);
                EncabezadoCusDoc:=EncabezadoCusDoc || '<Attribute Type="TIPODTE">' || campo1.tipo_dte || '</Attribute>'|| chr(10);
                EncabezadoCusDoc:=EncabezadoCusDoc || '<Attribute Type="FOLIO">' || campo1.folio || '</Attribute>'|| chr(10);
                EncabezadoCusDoc:=EncabezadoCusDoc || '<Attribute Type="FECHAEMISION">' || campo1.fecha_emision || '</Attribute>'|| chr(10);
                EncabezadoCusDoc:=EncabezadoCusDoc || '<Attribute Type="RUTEMISOR">' || campo1.rut_emisor || '-' || modulo11(campo1.rut_emisor::varchar) || '</Attribute>'|| chr(10);
                EncabezadoCusDoc:=EncabezadoCusDoc || '<Attribute Type="RUTRECEPTOR">' || campo1.rut_receptor || '-' || modulo11(campo1.rut_receptor::varchar) || '</Attribute>'|| chr(10);
                EncabezadoCusDoc:=EncabezadoCusDoc || '<Attribute Type="MONTOTOTAL">' || campo1.monto_total || '</Attribute>'|| chr(10);
                EncabezadoCusDoc:=EncabezadoCusDoc || '</Attributes>';
                EncabezadoCusDoc:=EncabezadoCusDoc || '<Content>';
		
		xml2:=logapp(xml2,'paso1='||EncabezadoCusDoc);

                xml_resp1:= '<EnvioRecibos xmlns="http://www.sii.cl/SiiDte" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.sii.cl/SiiDte EnvioRecibos_v10.xsd" version="1.0">';
                xml_resp1:= xml_resp1 || '<SetRecibos ID="' || id2 || '">';
                xml_resp1:= xml_resp1 || '<Caratula version="1.0">';
                xml_resp1:= xml_resp1 || '<RutResponde>' || campo1.rut_receptor || '-' || modulo11(campo1.rut_receptor::varchar) || '</RutResponde>';
                xml_resp1:= xml_resp1 || '<RutRecibe>' || campo1.rut_emisor || '-' || modulo11(campo1.rut_emisor::varchar) || '</RutRecibe>';
                xml_resp1:= xml_resp1 || '<NmbContacto>x</NmbContacto>';
                xml_resp1:= xml_resp1 || '<FonoContacto>x</FonoContacto>';
                xml_resp1:= xml_resp1 || '<MailContacto>x</MailContacto>';
                xml_resp1:= xml_resp1 || '<TmstFirmaEnv>' || replace(campo1.time,' ','T') || '</TmstFirmaEnv>';
                xml_resp1:= xml_resp1 || '</Caratula>';
                xml_resp1:= xml_resp1 || '<Recibo version="1.0"><DocumentoRecibo ID="' || id1 || '">';
                xml_resp1:= xml_resp1 || '<TipoDoc>' || campo1.tipo_dte || '</TipoDoc>';
                xml_resp1:= xml_resp1 || '<Folio>' || campo1.folio || '</Folio>';
                xml_resp1:= xml_resp1 || '<FchEmis>' || campo1.fecha_emision || '</FchEmis>';
                xml_resp1:= xml_resp1 || '<RUTEmisor>' || campo1.rut_emisor || '-' || modulo11(campo1.rut_emisor::varchar) || '</RUTEmisor>';
                xml_resp1:= xml_resp1 || '<RUTRecep>' || campo1.rut_receptor || '-' || modulo11(campo1.rut_receptor::varchar) || '</RUTRecep>';
                xml_resp1:= xml_resp1 || '<MntTotal>' || campo1.monto_total || '</MntTotal>';
                xml_resp1:= xml_resp1 || '<Recinto>'|| recinto1 ||'</Recinto>';
                xml_resp1:= xml_resp1 || '<RutFirma>'||json_get('rut_firma',json2)||'-'||modulo11(json_get('rut_firma',json2)) ||'</RutFirma>';
                xml_resp1:= xml_resp1 || '<Declaracion>El acuse de recibo que se declara en este acto, de acuerdo a lo dispuesto en la letra b) del Art. 4, y la letra c) del Art. 5 de la Ley 19.983, acredita que la entrega de mercaderias o servicio(s) prestado(s) ha(n) sido recibido(s).</Declaracion>';
                xml_resp1:= xml_resp1 || '<TmstFirmaRecibo>' || replace(campo1.time,' ','T') || '</TmstFirmaRecibo>';
                xml_resp1:= xml_resp1 || '</DocumentoRecibo>';
                xml_resp1:= xml_resp1 || '</Recibo></SetRecibos>';
                xml_resp1:= xml_resp1 || '</EnvioRecibos>';
		xml2:=logapp(xml2,'paso2='||xml_resp1);

                PieCusDoc:='</Content><Log><Process id="motor" version="1.0"><item name="custodium-uri">__REMPLAZA_URI__</item></Process><Process build="" id="MOTOR" version=""><item name="">item</item></Process></Log></Document>';

                xml_resp1:=EncabezadoCusDoc || xml_resp1 || PieCusDoc;


                --request1:='<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ser="http://servicio.firma.acepta.com/"><soapenv:Header/><soapenv:Body><ser:firmarXML><XmlInput>'||encode(xml_resp1::bytea,'base64')::varchar||'</XmlInput><NodoId>'||id1||'</NodoId><RutEmpresa>'||json_get('rutCliente',json2)||'-'||json_get('rut_firma',json2)||'-'||modulo11(json_get('rut_firma',json2))||'</RutEmpresa><RutFirmante>'||json_get('rut_firma',json2) ||'-'||modulo11(json_get('rut_firma',json2))||'</RutFirmante><ClaveAcceso>REEMPLAZA_CLAVE_ACCESO</ClaveAcceso><Entidad>SII</Entidad></ser:firmarXML></soapenv:Body></soapenv:Envelope>';

		--Armamos para disparar directo a el firmador por socket
		data_firma1:=replace('{"documento":"'||encode(xml_resp1::bytea,'base64')::varchar||'","nodoId":"'||id1||'","rutEmpresa":"'||json_get('rut_firma',json2)||'-'||modulo11(json_get('rut_firma',json2))||'","entidad":"SII","rutFirmante":"'||json_get('rut_firma',json2)||'-'||modulo11(json_get('rut_firma',json2))||'","codigoAcceso":"'||replace(corrige_pass(decode(json_get('pass',json2),'hex')::text),chr(92),chr(92)||chr(92))||'"}',chr(10),'');

		--xml2:=get_parametros_motor(xml2,'FIRMADOR');
		xml2:=get_parametros_motor(xml2,get_parametro_firmador(json_get('rut_firma',json2)||'-'||modulo11(json_get('rut_firma',json2))));
		xml2:=put_campo(xml2,'INPUT_FIRMADOR','POST '||get_campo('PARAMETRO_RUTA',xml2)||' HTTP/1.1'||chr(10)||'User-Agent: curl/7.26.0'||chr(10)||'Host: '||get_campo('__IP_CONEXION_CLIENTE__',xml2)||chr(10)||'Accept: */*'||chr(10)||'Content-Type: application/json'||chr(10)||'Content-Length: '||length(data_firma1)::varchar||chr(10)||chr(10)||data_firma1);
		--xml2:=put_campo(xml2,'__IP_CONEXION_CLIENTE__','192.168.3.17');
		--xml2:=put_campo(xml2,'__IP_PORT_CLIENTE__','80');
		/*xml2:=put_campo(xml2,'__SECUENCIAOK__','21');*/
		
		xml2:=put_campo(xml2,'ID_FIRMA2_ARM',id2);

                --xml2:=put_campo(xml2,'SCRIPT','/opt/acepta/motor/scripts/funciones/cuad_recibidos5000/script_firma_simple.sh '||encode(request1::bytea,'hex') || ' ' || json_get('pass',json2));
		--xml2:=logapp(xml2,'script='||request1);
		xml2:=put_campo(xml2,'__SECUENCIAOK__','20');
        else
                xml2:=response_requests_5000('200', 'DTE no encontrado', '', xml2,json2);
		xml2:=put_campo(xml2,'__SECUENCIAOK__','0');
        end if;
    
   return xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION xml_firma2_12762(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
        xml2    varchar;
        json1   varchar;
        json2   varchar;
        rut1    varchar;
        data1   varchar;
	resp1	varchar;
	pos1	integer;
	pos12	integer;
	uri1	varchar;
	request1            varchar;
	json_resp1	varchar;
	data_firma1     varchar;
BEGIN
	xml2:=xml1;
    	json1:=decode(get_campo('JSON_IN',xml2),'hex')::varchar;
    	json2:=json1;		
	xml2:=put_campo(xml2,'__SECUENCIAOK__','0');
 
	--Limpiamos
	xml2:=put_campo(xml2,'SCRIPT','');

	--resp1:=get_campo('RESPUESTA_SYSTEM',xml2);
	--xml2:=put_campo(xml2,'RESPUESTA_SYSTEM','');
	--Corrige la respuesta si viene chunked
	xml2:=respuesta_no_chunked(xml2);
	resp1:=decode(get_campo('RESPUESTA_HEX',xml2),'hex');
	--xml2:=logapp(xml2,'resp1 ARM='||resp1);
	json_resp1:=split_part(resp1,'\012\012',2);
	--xml2:=logapp(xml2,'json_resp1='||json_resp1);
	xml2:=put_campo(xml2,'RESPUESTA_HEX','');
	xml2:=put_campo(xml2,'INPUT_FIRMADOR','');
        --xml2:=logapp(xml2,'1Respuesta_HEX ARM='||decode(get_campo('RESPUESTA_HEX',xml2),'hex')::varchar);
	if (strpos(resp1,'HTTP/1.1 200 ')>0) then
		--uri1:=get_xml('urlDocumento',resp1);	
		--xml2:=logapp(xml2,'json_resp1='||json_resp1);
		--xml2:=logapp(xml2,'doc_firmado='||replace(json_get('documentoFirmado',json_resp1),chr(13)||chr(10),''));
		data1:=json_get('documentoFirmado',json_resp1);
		--xml2:=logapp(xml2,'documentoFirmado='||data1);
		--data1:=get_xml('documentoFirmado',resp1);
                if (length(data1)=0) then
			xml2:=response_requests_5000('2', 'ARM no firmado', '',xml2,json2);
			insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),json_get('rut_firma',json2),'FALLA','','ARM',get_campo('URI_IN',xml2));
			return xml2;
		else
			--Firme Correctamente
			insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),json_get('rut_firma',json2),'OK1','','ARM',get_campo('URI_IN',xml2));
			
        	end if;
	elsif (strpos(resp1,'HTTP/1.1 500 ')>0) then
		   xml2:=logapp(xml2,'Error Cesion Respuesta '||resp1);
		   resp1:=json_get('ERROR',json_resp1);
                   --resp1:='Error: '||split_part(split_part(resp1,'<faultstring>com.acepta.custodiafirma.servicios.ServicioFirmaXMLException:',2),'</faultstring>',1);
                   xml2:=response_requests_5000('2', resp1, '',xml2,json2);
		   insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),json_get('rut_firma',json2),'FALLA1',resp1,'ARM',get_campo('URI_IN',xml2));
		   return xml2;
        else
		   xml2:=logapp(xml2,'No responde Cesion Respuesta '||resp1);
                   xml2:=response_requests_5000('2', 'Servicio de Firma no responde', '',xml2,json2);
		   insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),json_get('rut_firma',json2),'FALLA2','Servicio de Firma no responde','ARM',get_campo('URI_IN',xml2));
		   return xml2;
        end if;

	--request1:='<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ser="http://servicio.firma.acepta.com/"><soapenv:Header/><soapenv:Body><ser:firmarXML><XmlInput>'||data1::varchar||'</XmlInput><NodoId>'||get_campo('ID_FIRMA2_ARM',xml2)||'</NodoId><RutEmpresa>'||json_get('rut_firma',json2)||'-'||modulo11(json_get('rut_firma',json2))||'</RutEmpresa><RutFirmante>'||json_get('rut_firma',json2)||'-'||modulo11(json_get('rut_firma',json2)) ||'</RutFirmante><ClaveAcceso>REEMPLAZA_CLAVE_ACCESO</ClaveAcceso><Entidad>SII</Entidad></ser:firmarXML></soapenv:Body></soapenv:Envelope>';
	--Armamos para disparar directo a el firmador por socket
	data_firma1:=replace('{"documento":"'||data1||'","nodoId":"'||get_campo('ID_FIRMA2_ARM',xml2) ||'","rutEmpresa":"'||json_get('rut_firma',json2)||'-'||modulo11(json_get('rut_firma',json2))||'","entidad":"SII","rutFirmante":"'||json_get('rut_firma',json2)||'-'||modulo11(json_get('rut_firma',json2))||'","codigoAcceso":"'||replace(corrige_pass(decode(json_get('pass',json2),'hex')::text),chr(92),chr(92)||chr(92))||'"}',chr(10),'');
	

	--xml2:=get_parametros_motor(xml2,'FIRMADOR');
	xml2:=get_parametros_motor(xml2,get_parametro_firmador(json_get('rut_firma',json2)||'-'||modulo11(json_get('rut_firma',json2))));

	xml2:=put_campo(xml2,'INPUT_FIRMADOR','POST '||get_campo('PARAMETRO_RUTA',xml2)||' HTTP/1.1'||chr(10)||'User-Agent: curl/7.26.0'||chr(10)||'Host: '||get_campo('__IP_CONEXION_CLIENTE__',xml2)||chr(10)||'Accept: */*'||chr(10)||'Content-Type: application/json'||chr(10)||'Content-Length: '||length(data_firma1)::varchar||chr(10)||chr(10)||data_firma1);
	--xml2:=put_campo(xml2,'__IP_CONEXION_CLIENTE__','192.168.3.17');
	--xml2:=put_campo(xml2,'__IP_PORT_CLIENTE__','80');

	--xml2:=put_campo(xml2,'SCRIPT','/opt/acepta/motor/scripts/funciones/cuad_recibidos5000/script_firma_simple.sh '||encode(request1::bytea,'hex') || ' ' || json_get('pass',json2));
	xml2:=put_campo(xml2,'__SECUENCIAOK__','40');
	return xml2;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION arm_resp_12762(varchar)
 RETURNS varchar AS $$
DECLARE
    xml1                alias for $1;
    xml2                varchar;
    resp1               varchar;
    uri1        varchar;
    data1	varchar;
	json2	varchar;
	aux1	varchar;
	json_resp1	varchar;
BEGIN
        xml2:=xml1;

	json2:=decode(get_campo('JSON_IN',xml2),'hex');
	xml2:=put_campo(xml2,'__SECUENCIAOK__','0');
	xml2:=respuesta_no_chunked(xml2);
	resp1:=decode(get_campo('RESPUESTA_HEX',xml2),'hex');
	--resp1:=get_campo('RESPUESTA_SYSTEM',xml2);
	--xml2:=logapp(xml2,'resp1 ARM='||resp1);
	json_resp1:=split_part(resp1,'\012\012',2);
	xml2:=put_campo(xml2,'RESPUESTA_HEX','');
	xml2:=put_campo(xml2,'INPUT_FIRMADOR','');

       if (strpos(resp1,'HTTP/1.1 200 ')>0) then
		--aux1:=get_xml('documentoFirmado',resp1);
		aux1:=json_get('documentoFirmado',json_resp1);
                if (length(aux1)>0) then
			--Obtengo el documento para enviarlo al EDTE
			data1:=base642hex(aux1);
			--Armo la URI segun el dominio del dte
			data1:=replace(data1,encode('__REMPLAZA_URI__','hex')::varchar,encode(get_campo('URI_IN',xml2)::bytea,'hex')::varchar);
			xml2:=put_campo(xml2,'INPUT',data1);
	                xml2:=put_campo(xml2,'CONTENT_LENGTH',length(data1)::varchar);
			xml2:=put_campo(xml2,'__IP_CONEXION_CLIENTE__',get_campo('__IP_CONEXION_CLIENTE__',get_parametros_motor('','EDTE')));
			xml2:=put_campo(xml2,'__SECUENCIAOK__','60');
			insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),json_get('rut_firma',json2),'OK2','','ARM',get_campo('URI_IN',xml2));
                else
                        xml2:=response_requests_5000('2', 'ARM no firmado', '',xml2,json2);
			insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),json_get('rut_firma',json2),'FALLA1','ARM no firmado','ARM',get_campo('URI_IN',xml2));
                end if;
       elsif (strpos(resp1,'HTTP/1.1 500 ')>0) then
		   resp1:=json_get('ERROR',json_resp1);
                   --resp1:='Error: '||split_part(split_part(resp1,'<faultstring>com.acepta.custodiafirma.servicios.ServicioFirmaXMLException:',2),'</faultstring>',1);
                   xml2:=response_requests_5000('2', resp1, '',xml2,json2);
		   insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),json_get('rut_firma',json2),'FALLA2',resp1,'ARM',get_campo('URI_IN',xml2));
        else
                   xml2:=response_requests_5000('2', 'Servicio de Firma no responde', '',xml2,json2);
		   insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),json_get('rut_firma',json2),'FALLA3','Servicio de Firma no responde','ARM',get_campo('URI_IN',xml2));
        end if;
	return xml2;

END;
$$ LANGUAGE plpgsql;



