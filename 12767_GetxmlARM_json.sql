--Publica documento
delete from isys_querys_tx where llave='12767';
--Obtiene el DTE Original con la entrada URI_IN
insert into isys_querys_tx values ('12767',10,9,1,'select xml_firma1_12767(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

--insert into isys_querys_tx values ('12767',20,1,10,'$$SCRIPT$$',0,0,0,1,1,21,21);
insert into isys_querys_tx values ('12767',20,1,2,'Servicio de Firma 192.168.3.17',4013,109,106,0,0,30,30);

insert into isys_querys_tx values ('12767',30,9,1,'select xml_firma2_12767(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

--insert into isys_querys_tx values ('12767',40,1,10,'$$SCRIPT$$',0,0,0,1,1,41,41);
insert into isys_querys_tx values ('12767',40,1,2,'Servicio de Firma 192.168.3.17',4013,109,106,0,0,50,50);

insert into isys_querys_tx values ('12767',50,9,1,'select arm_resp_12767(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

--Se envia al EDTE
insert into isys_querys_tx values ('12767',60,1,8,'Llamada ARM EDTE',12780,0,0,0,0,65,65);
--insert into isys_querys_tx values ('12767',62,1,8,'Llamada ARM EDTE',112780,0,0,0,0,65,65);

--Publicamos el ARM
insert into isys_querys_tx values ('12767',65,1,8,'Publica DTE',1127043,0,0,0,0,70,70);

--Validamos la publicacion
insert into isys_querys_tx values ('12767',70,9,1,'select valida_publicacion_arm_12767(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);


CREATE or replace FUNCTION valida_publicacion_arm_12767(json) RETURNS json AS $$
declare
	json1	alias for $1;
	json2	json;
	rut1	varchar;
	stContribuyente	contribuyentes%ROWTYPE;
	mail1	varchar;
	xml4	varchar;
begin
	json2:=json1;
	json2:=put_json(json2,'__SECUENCIAOK__','0');
        --json2:=decode(get_json_upper('JSON_IN',json2),'hex')::varchar;
	if (get_json_upper('__PUBLICADO_OK__',json2)<>'SI') then
                  json2:=response_requests_6000_upper('2', 'Falla Publicacion de ARM', '',json2);
		  return json2;
	end if;

	--Si lo escribi bien en el EDTE
	if (get_json('RECINTO',json2)<>'TEST') then
		if (get_json_upper('__EDTE_ARM_OK__',json2)<>'SI') then
			if (get_json('FLAG_RECLAMO',json2)='SI') then
        	        	json2:=response_requests_6000_upper('2',get_json('__MENSAJE_10K__',json2), '',json2);
			else
        	        	json2:=response_requests_6000_upper('2', 'Falla Envio de ARM', '',json2);
			end if;
			return json2;
		end if;
	end if;

	--2015-04-30 FAY,RME Se graba inmediatamente el Evento ARM para el DTE recibido
        rut1:=get_json_upper('RUT_EMISOR',json2);
        select * into stContribuyente from contribuyentes where rut_emisor=rut1::integer;
        if not found then
                json2:=logjson(json2,'ARM: Rut Emisor del DTE Recibido no registrado en contribuyentes');
                mail1:='Sin mail de intercambio';
        else
                mail1:=stContribuyente.email;
        end if;
        json2:=put_json(json2,'COMENTARIO_TRAZA','Recibe: '||mail1||chr(10)||get_json_upper('RECINTO',json2)||'.');
        --Se asignan las uris para grabar el evento en traza
        json2:=put_json(json2,'URL_GET',get_json_upper('URI_IN',json2));
        json2:=put_json(json2,'URI_IN',get_json_upper('URI_DTE',json2));

	xml4:='';
	xml4:=put_campo(xml4,'BORRADOR',get_json_upper('BORRADOR',json2));
	xml4:=put_campo(xml4,'FECHA_SERVER',now()::varchar);
	xml4:=put_campo(xml4,'FECHA_EVENTO',get_json_upper('FECHA_EVENTO',json2));
	xml4:=put_campo(xml4,'CODIGO_TXEL',get_json_upper('CODIGO_TXEL',json2));
	xml4:=put_campo(xml4,'FECHA_EMISION',get_json_upper('FECHA_EMISION',json2));
	xml4:=put_campo(xml4,'URL_GET',get_json_upper('URL_GET',json2));
	xml4:=put_campo(xml4,'RUT_EMISOR',get_json_upper('RUT_EMISOR',json2));
	xml4:=put_campo(xml4,'RUT_RECEPTOR',get_json_upper('RUT_RECEPTOR',json2));
	xml4:=put_campo(xml4,'URI_IN',get_json_upper('URI_IN',json2));
	xml4:=put_campo(xml4,'TIPO_OPERACION',get_json_upper('TIPO_OPERACION',json2));
	xml4:=put_campo(xml4,'FECHA_EMISION',get_json_upper('FECHA_EMISION',json2));
	xml4:=put_campo(xml4,'COMENTARIO_TRAZA',get_json_upper('COMENTARIO_TRAZA',json2));
	xml4:=put_campo(xml4,'COMENTARIO2',get_json_upper('COMENTARIO2',json2));
	xml4:=put_campo(xml4,'FOLIO',get_json_upper('FOLIO',json2));
	xml4:=put_campo(xml4,'TIPO_DTE',get_json_upper('TIPO_DTE',json2));
	xml4:=put_campo(xml4,'FECHA_EVENTO_ARM',get_json_upper('FECHA_EVENTO_ARM',json2));
	xml4:=put_campo(xml4,'CANAL','EMITIDOS');
	xml4:=graba_bitacora(xml4,'ARM');

        --Vuelo a dejar en uri_in la uri del ARM
        json2:=put_json(json2,'URI_IN',get_json_upper('URL_GET',json2));
	if (get_json('FLAG_RECLAMO',json2)='SI') then
       	       	json2:=response_requests_6000_upper('1',get_json('__MENSAJE_10K__',json2),get_json_upper('URI_IN',json2) ,json2);
	else
        	json2:=response_requests_6000_upper('1', 'ARM firmado',get_json_upper('URI_IN',json2),json2);
	end if;
   	return json2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION xml_firma1_12767(json) RETURNS json AS $$
DECLARE
    json1        alias for $1;
        json2   json;
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
	pass1	varchar;
	rut_cliente1	varchar;
BEGIN
    json2:=json1;
    json2:=logjson(json2,'COMIENZA FLUJO 12767');
	json2:=put_json(json2,'__ORIGEN__','ESCRITORIO');
	json2:=put_json(json2,'__FLAG_PUB_10K__','SI');
    --json1:=decode(get_json_upper('JSON_IN',json2),'hex')::varchar;
    --json2:=json1;
        json2:=logjson(json2,'Json in='||json2);

	rut1:=trim(replace(split_part(get_json_upper('rutDte',json2),'-',1),'.',''));
	rut_cliente1:=get_json('rutCliente',json2);
	
	--Si el rut es no numerico
	if (is_number(rut1) is false) then
		--Si tiene activado el complemento de skype
		if (strpos(rut1,'skype')>0) then
			json2:=response_requests_6000_upper('2', 'Por favor, desactive Skype como complemento','', json2);
		else
			json2:=response_requests_6000_upper('2', 'Rut Emisor no Numerico','', json2);
		end if;	
		return json2;
	end if;
        tipo_dte1:=get_json_upper('tipoDte',json2);
        folio1:=replace(get_json_upper('folioDte',json2),'.','');
        recinto1:=decode(get_json_upper('recinto',json2),'hex');
	json2:=put_json(json2,'RECINTO',recinto1);


        json2:=logjson(json2,'Datos para formar xml ARM rut1=' || rut1);
        json2:=logjson(json2,'Datos para formar xml ARM tipo_dte1=' || tipo_dte1);
        json2:=logjson(json2,'Datos para formar xml ARM folio1=' || folio1);
        json2:=logjson(json2,'Datos para formar xml ARM recinto1=' || recinto1);
        json2:=logjson(json2,'Datos para formar xml ARM rut_firma=' || get_json_upper('rut_firma',json2));

        if((rut1 || tipo_dte1 ||folio1 ||recinto1 ||get_json_upper('rut_firma',json2))=NULL) then
                json2:=response_requests_6000_upper('2', 'Alguno de los campos obligatorios es Nulo','', json2);
                return json2;
        end if;

        json2:=logjson(json2,'rut1= ' || rut1 || ' tipo_dte1= ' || tipo_dte1 || ' folio1= ' || folio1 || ' recinto1=' ||recinto1);

        if(folio1='' or is_number(folio1) is false) then
                json2:=response_requests_6000_upper('2', 'Folio Incorrecto', '', json2);
                return json2;
        end if;

        if(tipo_dte1='-1') then
                json2:=response_requests_6000_upper('400', 'Tipo DTE Incorrecto', '', json2);
                return json2;
        end if;
--      json2:=logjson(json2,'select xml= ' || 'SELECT * INTO exists_select1 FROM dte_recibidos WHERE rut_emisor=' || rut1 ||'::integer and tipo_dte=' || tipo_dte1 || '::integer and folio=' || '::integer;');
        json2:=logjson(json2,'RUT='||rut1);
        --query1:='SELECT rut_emisor,rut_receptor,tipo_dte,folio,fecha_emision,monto_total,estado, to_char(current_timestamp, ''YYYY-MM-DD HH:MM:SS'') as time FROM dte_recibidos WHERE rut_receptor=' || rut1 ||'::integer and tipo_dte=' || tipo_dte1 || '::integer and folio=' || folio1 || '::integer;';
        --SELECT codigo_txel,rut_emisor,rut_receptor,tipo_dte,folio,fecha_emision,monto_total,estado,uri_arm, to_char(current_timestamp, 'YYYY-MM-DD HH:MM:SS') as time,uri into campo1 FROM dte_recibidos WHERE rut_receptor=rut1::integer and tipo_dte=tipo_dte1::integer and folio=folio1::integer;
	--Desde la pagina se envia en rut1 el rut emisor del DTE recibido, con esto aseguro la busqueda unica delñ DTE en la tabla
	--Se agrega que el ARM sea de nuestro cliente
        SELECT codigo_txel,rut_emisor,rut_receptor,tipo_dte,folio,fecha_emision,monto_total,estado,uri_arm, to_char(current_timestamp, 'YYYY-MM-DD HH:MM:SS') as time,uri,estado_reclamo into campo1 FROM (select * from dte_recibidos WHERE rut_emisor=rut1::integer and tipo_dte=tipo_dte1::integer and folio=folio1::integer) x where x.rut_receptor=rut_cliente1::integer;
	if not found then
		--Lo buscamos en los importados
        	SELECT codigo_txel,rut_emisor,rut_receptor,tipo_dte,folio,fecha_emision,monto_total,estado,uri_arm, to_char(current_timestamp, 'YYYY-MM-DD HH:MM:SS') as time,uri,estado_reclamo into campo1 FROM (select * from dte_recibidos_importados_generica WHERE rut_emisor=rut1::integer and tipo_dte=tipo_dte1::integer and folio=folio1::integer) x where x.rut_receptor=rut_cliente1::integer;
	end if;
	if found then
		--Si ya esta firmado
		if campo1.uri_arm is not null then
			json2:=logjson(json2,'ARM ya procesado'||campo1.uri_arm);
                        json2:=response_requests_6000_upper('1', 'ARM ya procesado',campo1.uri_arm,json2);
			return json2;
		end if;

		json2:=put_json(json2,'URI_DTE',campo1.uri);
		json2:=put_json(json2,'RUT_EMISOR',campo1.rut_emisor::varchar);
		json2:=put_json(json2,'RUT_RECEPTOR',campo1.rut_receptor::varchar);
		json2:=put_json(json2,'FECHA_EMISION',campo1.fecha_emision::varchar);
		json2:=put_json(json2,'MONTO_TOTAL',campo1.monto_total::varchar);
		json2:=put_json(json2,'TIPO_DTE',campo1.tipo_dte::varchar);
		json2:=put_json(json2,'CODIGO_TXEL_ARM',campo1.codigo_txel::varchar);
		json2:=put_json(json2,'RUT_EMISOR_ARM',campo1.rut_emisor::varchar);
		json2:=put_json(json2,'TIPO_DTE_ARM',campo1.tipo_dte::varchar);
		json2:=put_json(json2,'FOLIO_ARM',campo1.folio::varchar);
                id1:=get_newRespuestaID_2116(campo1.rut_emisor::varchar, campo1.rut_receptor::varchar,campo1.tipo_dte::varchar,campo1.folio::varchar,campo1.fecha_emision::varchar, 'Recibo');
                id2:=replace(id1,'Recibo','SetRecibo');
		json2:=put_json(json2,'ID1_DOC',id1);
		json2:=put_json(json2,'ID2_DOC',id2);
		json2:=logjson(json2,'Rut Emisor='||campo1.rut_receptor::varchar);
                SELECT coalesce(dominio,'') FROM maestro_clientes WHERE rut_emisor = campo1.rut_receptor INTO dominio1;
                if not found then
                        dominio1:='webdte';
                end if;
		--Armamos la URI del ARM
		--http://lg1504.acepta.com/v01/b3dc07b386b63ec6e8d7e334623afd8a77de91fe?k=cd79851b62204f6280ba3eea5893ed89
		json2:=put_json(json2,'URI_IN','http://'||dominio1||to_char(now(),'YYMM')||'.acepta.com/v01/'||lpad('_'||replace(getipserver('eth0'),'.','')||'_'||to_char(now(),'DDHH24MISSMI')||'_'||nextval('correlativo_uri')::varchar||'_',40,'0')||'?k='||md5(coalesce(dominio1,'X')));
	
                EncabezadoCusDoc:='<?xml version="1.0" encoding="ISO-8859-1"?>' || chr(10);
                EncabezadoCusDoc:=EncabezadoCusDoc || '<?xml-stylesheet type="text/xsl" href="http://www.custodium.com/docs/arm/arm.xsl"?> '|| chr(10);
                EncabezadoCusDoc:=EncabezadoCusDoc || '<Document Domain="' ||dominio1|| '" Type="Intercambio">'|| chr(10);
                EncabezadoCusDoc:=EncabezadoCusDoc || '<Issuer><PI Type="Rut">' ||campo1.rut_receptor || '-' || modulo11(campo1.rut_receptor::varchar) ||'</PI></Issuer>' || chr(10)     ;
                EncabezadoCusDoc:=EncabezadoCusDoc || '<Signers><Signer><PI Type="Rut">'||get_json_upper('rut_firma',json2)||'</PI></Signer></Signers>'|| chr(10);
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
		
		json2:=logjson(json2,'paso1='||EncabezadoCusDoc);

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
                xml_resp1:= xml_resp1 || '<RutFirma>'||get_json_upper('rut_firma',json2)||'-'||modulo11(get_json_upper('rut_firma',json2)) ||'</RutFirma>';
                xml_resp1:= xml_resp1 || '<Declaracion>El acuse de recibo que se declara en este acto, de acuerdo a lo dispuesto en la letra b) del Art. 4, y la letra c) del Art. 5 de la Ley 19.983, acredita que la entrega de mercaderias o servicio(s) prestado(s) ha(n) sido recibido(s).</Declaracion>';
                xml_resp1:= xml_resp1 || '<TmstFirmaRecibo>' || replace(campo1.time,' ','T') || '</TmstFirmaRecibo>';
                xml_resp1:= xml_resp1 || '</DocumentoRecibo>';
                xml_resp1:= xml_resp1 || '</Recibo></SetRecibos>';
                xml_resp1:= xml_resp1 || '</EnvioRecibos>';
		json2:=logjson(json2,'paso2='||xml_resp1);

                PieCusDoc:='</Content><Log><Process id="motor" version="1.0"><item name="custodium-uri">__REMPLAZA_URI__</item></Process><Process build="" id="MOTOR" version=""><item name="">item</item></Process></Log></Document>';

                xml_resp1:=EncabezadoCusDoc || xml_resp1 || PieCusDoc;


                --request1:='<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ser="http://servicio.firma.acepta.com/"><soapenv:Header/><soapenv:Body><ser:firmarXML><XmlInput>'||encode(xml_resp1::bytea,'base64')::varchar||'</XmlInput><NodoId>'||id1||'</NodoId><RutEmpresa>'||get_json_upper('rutCliente',json2)||'-'||get_json_upper('rut_firma',json2)||'-'||modulo11(get_json_upper('rut_firma',json2))||'</RutEmpresa><RutFirmante>'||get_json_upper('rut_firma',json2) ||'-'||modulo11(get_json_upper('rut_firma',json2))||'</RutFirmante><ClaveAcceso>REEMPLAZA_CLAVE_ACCESO</ClaveAcceso><Entidad>SII</Entidad></ser:firmarXML></soapenv:Body></soapenv:Envelope>';

		--Armamos para disparar directo a el firmador por socket
		if(get_json_upper('flag_tx_buscar',json2)<>'SI')then
			pass1:=corrige_pass(decode(get_json_upper('pass',json2),'hex')::text);
		else
			pass1:=get_json_upper('pass',json2);
		end if;
		pass1:=replace(pass1,chr(92),chr(92)||chr(92));
		data_firma1:=replace('{"documento":"'||encode(xml_resp1::bytea,'base64')::varchar||'","nodoId":"'||id1||'","rutEmpresa":"'||get_json_upper('rut_firma',json2)||'-'||modulo11(get_json_upper('rut_firma',json2))||'","entidad":"SII","rutFirmante":"'||get_json_upper('rut_firma',json2)||'-'||modulo11(get_json_upper('rut_firma',json2))||'","codigoAcceso":"'||pass1||'"}',chr(10),'');
		
		--json2:=get_parametros_motor_json(json2,'FIRMADOR');
		json2:=get_parametros_motor_json(json2,get_parametro_firmador(get_json_upper('rut_firma',json2)||'-'||modulo11(get_json_upper('rut_firma',json2))));
		json2:=put_json(json2,'INPUT_FIRMADOR','POST '||get_json('PARAMETRO_RUTA',json2)||' HTTP/1.1'||chr(10)||'User-Agent: curl/7.26.0'||chr(10)||'Host: '||get_json('__IP_CONEXION_CLIENTE__',json2)||chr(10)||'Accept: '||chr(42)||'/'||chr(42)||chr(10)||'Content-Type: application/json'||chr(10)||'Content-Length: '||length(data_firma1)::varchar||chr(10)||chr(10)||data_firma1);
		--json2:=put_json(json2,'__IP_CONEXION_CLIENTE__','192.168.3.17');
		--json2:=put_json(json2,'__IP_PORT_CLIENTE__','80');
		/*json2:=put_json(json2,'__SECUENCIAOK__','21');*/
		
		json2:=put_json(json2,'ID_FIRMA2_ARM',id2);

                --json2:=put_json(json2,'SCRIPT','/opt/acepta/motor/scripts/funciones/cuad_recibidos5000/script_firma_simple.sh '||encode(request1::bytea,'hex') || ' ' || get_json_upper('pass',json2));
		--json2:=logjson(json2,'script='||request1);
		json2:=put_json(json2,'__SECUENCIAOK__','20');
        else
                --json2:=response_requests_6000_upper('200', 'DTE no encontrado.', '', json2);
                json2:=response_requests_6000_upper('2', 'Refresque su pagina, Por favor haga CRTl+F5 o CRTL+R', '', json2);
		json2:=put_json(json2,'__SECUENCIAOK__','0');
        end if;
    
   return json2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION xml_firma2_12767(json) RETURNS json AS $$
DECLARE
    json1        alias for $1;
        json2   json;
        rut1    varchar;
        data1   varchar;
	resp1	varchar;
	pos1	integer;
	pos12	integer;
	uri1	varchar;
	request1            varchar;
	json_resp1	varchar;
	data_firma1     varchar;
	pass1     varchar;
BEGIN
	json2:=json1;
    	--json1:=decode(get_json_upper('JSON_IN',json2),'hex')::varchar;
    	--json2:=json1;		
	json2:=put_json(json2,'__SECUENCIAOK__','0');
 
	--Limpiamos
	json2:=put_json(json2,'SCRIPT','');

	--resp1:=get_json_upper('RESPUESTA_SYSTEM',json2);
	--json2:=put_json(json2,'RESPUESTA_SYSTEM','');
	--Corrige la respuesta si viene chunked
	json2:=respuesta_no_chunked_json(json2);
	resp1:=decode(get_json_upper('RESPUESTA_HEX',json2),'hex');
	--json2:=logjson(json2,'resp1 ARM='||resp1);
	json_resp1:=split_part(resp1,'\012\012',2);
	--json2:=logjson(json2,'json_resp1='||json_resp1);
	json2:=put_json(json2,'RESPUESTA_HEX','');
	json2:=put_json(json2,'INPUT_FIRMADOR','');
        --json2:=logjson(json2,'1Respuesta_HEX ARM='||decode(get_json_upper('RESPUESTA_HEX',json2),'hex')::varchar);
	if (strpos(resp1,'HTTP/1.1 200 ')>0) then
		data1:=json_get('documentoFirmado',json_resp1);
                if (length(data1)=0) then
			json2:=response_requests_6000_upper('2', 'ARM no firmado', '',json2);
			--insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json_upper('rut_firma',json2),'FALLA','','ARM',get_json_upper('URI_IN',json2));
			json2:=bitacora10k(json2,'FIRMA','Falla firma ARM.');
			return json2;
		else
			--Verifico el Documento firmado
			if (verifica_doc_firmado(data1,get_json('ID1_DOC',json2)) is false) then
		   		json2:=logjson(json2,'Error en HSM, no devuelve doc original');
                   		json2:=response_requests_6000_upper('2', 'Falla firma ARM', '',json2);
	    	   		json2:=bitacora10k(json2,'FIRMA','Falla firma ARM. ');
				return json2;
			end if;
			--Firme Correctamente
			--insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json_upper('rut_firma',json2),'OK1','','ARM',get_json_upper('URI_IN',json2));
			
        	end if;
	elsif (strpos(resp1,'HTTP/1.1 500 ')>0) then
		   json2:=logjson(json2,'Error Cesion Respuesta '||resp1);
		   resp1:=json_get('ERROR',json_resp1);
                   --resp1:='Error: '||split_part(split_part(resp1,'<faultstring>com.acepta.custodiafirma.servicios.ServicioFirmaXMLException:',2),'</faultstring>',1);
                   json2:=response_requests_6000_upper('2', resp1, '',json2);
		   --insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json_upper('rut_firma',json2),'FALLA1',resp1,'ARM',get_json_upper('URI_IN',json2));
	    	   json2:=bitacora10k(json2,'FIRMA','Falla firma ARM. '||resp1);
		   return json2;
        else
		   json2:=logjson(json2,'No responde Cesion Respuesta '||resp1);
                   json2:=response_requests_6000_upper('2', 'Servicio de Firma no responde', '',json2);
		   --insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json_upper('rut_firma',json2),'FALLA2','Servicio de Firma no responde','ARM',get_json_upper('URI_IN',json2));
	    	   json2:=bitacora10k(json2,'FIRMA','Firma de ARM, Servicio de Firma no responde');
		   return json2;
        end if;

	--request1:='<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ser="http://servicio.firma.acepta.com/"><soapenv:Header/><soapenv:Body><ser:firmarXML><XmlInput>'||data1::varchar||'</XmlInput><NodoId>'||get_json_upper('ID_FIRMA2_ARM',json2)||'</NodoId><RutEmpresa>'||get_json_upper('rut_firma',json2)||'-'||modulo11(get_json_upper('rut_firma',json2))||'</RutEmpresa><RutFirmante>'||get_json_upper('rut_firma',json2)||'-'||modulo11(get_json_upper('rut_firma',json2)) ||'</RutFirmante><ClaveAcceso>REEMPLAZA_CLAVE_ACCESO</ClaveAcceso><Entidad>SII</Entidad></ser:firmarXML></soapenv:Body></soapenv:Envelope>';
	--Armamos para disparar directo a el firmador por socket
	if(get_json_upper('flag_tx_buscar',json2)<>'SI')then
		pass1:=corrige_pass(decode(get_json_upper('pass',json2),'hex')::text);
	else
		pass1:=get_json_upper('pass',json2);
	end if;
	pass1:=replace(pass1,chr(92),chr(92)||chr(92));
	data_firma1:=replace('{"documento":"'||data1||'","nodoId":"'||get_json_upper('ID_FIRMA2_ARM',json2) ||'","rutEmpresa":"'||get_json_upper('rut_firma',json2)||'-'||modulo11(get_json_upper('rut_firma',json2))||'","entidad":"SII","rutFirmante":"'||get_json_upper('rut_firma',json2)||'-'||modulo11(get_json_upper('rut_firma',json2))||'","codigoAcceso":"'||pass1||'"}',chr(10),'');

	--json2:=get_parametros_motor_json(json2,'FIRMADOR');
	json2:=get_parametros_motor_json(json2,get_parametro_firmador(get_json_upper('rut_firma',json2)||'-'||modulo11(get_json_upper('rut_firma',json2))));
	json2:=put_json(json2,'INPUT_FIRMADOR','POST '||get_json('PARAMETRO_RUTA',json2)||' HTTP/1.1'||chr(10)||'User-Agent: curl/7.26.0'||chr(10)||'Host: '||get_json('__IP_CONEXION_CLIENTE__',json2)||chr(10)||'Accept: '||chr(42)||'/'||chr(42)||chr(10)||'Content-Type: application/json'||chr(10)||'Content-Length: '||length(data_firma1)::varchar||chr(10)||chr(10)||data_firma1);
	--json2:=put_json(json2,'__IP_CONEXION_CLIENTE__','192.168.3.17');
	--json2:=put_json(json2,'__IP_PORT_CLIENTE__','80');

	--json2:=put_json(json2,'SCRIPT','/opt/acepta/motor/scripts/funciones/cuad_recibidos5000/script_firma_simple.sh '||encode(request1::bytea,'hex') || ' ' || get_json_upper('pass',json2));
	json2:=put_json(json2,'__SECUENCIAOK__','40');
	return json2;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION arm_resp_12767(json) RETURNS json AS $$
DECLARE
    json1                alias for $1;
    json2                json;
    resp1               varchar;
    uri1        varchar;
    data1	varchar;
	aux1	varchar;
	json_resp1	varchar;
BEGIN
        json2:=json1;

	--json2:=decode(get_json_upper('JSON_IN',json2),'hex');
	json2:=put_json(json2,'__SECUENCIAOK__','0');
	json2:=respuesta_no_chunked_json(json2);
	--raise notice 'RESPUESTA_HEX=%',get_json_upper('RESPUESTA_HEX',json2);
	--return json2;
	resp1:=decode(get_json_upper('RESPUESTA_HEX',json2),'hex');
	--resp1:=get_json_upper('RESPUESTA_SYSTEM',json2);
	--json2:=logjson(json2,'resp1 ARM='||resp1);
	json_resp1:=split_part(resp1,'\012\012',2);
	json2:=put_json(json2,'RESPUESTA_HEX','');
	json2:=put_json(json2,'INPUT_FIRMADOR','');

       if (strpos(resp1,'HTTP/1.1 200 ')>0) then
		aux1:=json_get('documentoFirmado',json_resp1);
                if (length(aux1)>0) then
			--Obtengo el documento para enviarlo al EDTE
			data1:=base642hex(aux1);
			--Armo la URI segun el dominio del dte
			data1:=replace(data1,encode('__REMPLAZA_URI__','hex')::varchar,encode(get_json_upper('URI_IN',json2)::bytea,'hex')::varchar);
			json2:=put_json(json2,'INPUT',data1);
	                json2:=put_json(json2,'CONTENT_LENGTH',length(data1)::varchar);
			json2:=put_json(json2,'__IP_CONEXION_CLIENTE__',get_campo('__IP_CONEXION_CLIENTE__',get_parametros_motor('','EDTE')));
			if (get_json('RECINTO',json2)='TEST') then
				json2:=put_json(json2,'__SECUENCIAOK__','65');
			else
				json2:=put_json(json2,'__SECUENCIAOK__','60');
			end if;
			--insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json_upper('rut_firma',json2),'OK2','','ARM',get_json_upper('URI_IN',json2));
			--Verifico el Documento firmado
			if (verifica_doc_firmado(aux1,get_json('ID2_DOC',json2)) is false) then
		   		json2:=logjson(json2,'Error en HSM, no devuelve doc original');
                   		json2:=response_requests_6000_upper('2', 'Falla firma ARM', '',json2);
	    	   		json2:=bitacora10k(json2,'FIRMA','Falla firma ARM. ');
				return json2;
			end if;
	    	   	json2:=bitacora10k(json2,'FIRMA','ARM firmado OK URI='||get_json_upper('URI_IN',json2));
                else
                        json2:=response_requests_6000_upper('2', 'ARM no firmado', '',json2);
			--insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json_upper('rut_firma',json2),'FALLA1','ARM no firmado','ARM',get_json_upper('URI_IN',json2));
	    	   	json2:=bitacora10k(json2,'FIRMA','Falla firma de ARM.');
                end if;
       elsif (strpos(resp1,'HTTP/1.1 500 ')>0) then
		   resp1:=json_get('ERROR',json_resp1);
                   --resp1:='Error: '||split_part(split_part(resp1,'<faultstring>com.acepta.custodiafirma.servicios.ServicioFirmaXMLException:',2),'</faultstring>',1);
                   json2:=response_requests_6000_upper('2', resp1, '',json2);
		  --insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json_upper('rut_firma',json2),'FALLA2',resp1,'ARM',get_json_upper('URI_IN',json2));
	    	   json2:=bitacora10k(json2,'FIRMA','Falla firma de ARM. '||resp1);
        else
                   json2:=response_requests_6000_upper('2', 'Servicio de Firma no responde', '',json2);
		   --insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json_upper('rut_firma',json2),'FALLA3','Servicio de Firma no responde','ARM',get_json_upper('URI_IN',json2));
	    	   json2:=bitacora10k(json2,'FIRMA','Falla firma de ARM. Servicio de Firma no responde');
        end if;
	return json2;

END;
$$ LANGUAGE plpgsql;



