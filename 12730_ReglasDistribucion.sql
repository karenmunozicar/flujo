delete from isys_querys_tx where llave='12730';
---Nuevo flujo para envio XML y PDF.-
insert into isys_querys_tx values ('12730',5,1,8,'GET XML desde Almacen',12705,0,0,1,1,10,10);
--Procesa XML descargado
insert into isys_querys_tx values ('12730',10,1,1,'select proc_procesa_get_xml_12730(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
--Descarga PDF
insert into isys_querys_tx values ('12730',20,1,8,'Obtiene PDF Almacen',12714,0,0,0,0,30,30);
--Valida PDF
insert into isys_querys_tx values ('12730',30,1,1,'select proc_procesa_get_pdf_12730(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,40,40);
--ProcesaRespuesta.
insert into isys_querys_tx values ('12730',40,19,1,'select proc_procesa_12730(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
--Envia Post Correo
--insert into isys_querys_tx values ('12750',50,1,2,'GENERICO',4013,103,101,0,0,0,0);
--Generico Llamada de SCRIPT
insert into isys_querys_tx values ('12730',90,1,10,'$$SCRIPT$$',0,0,0,1,1,100,100);
--insert into isys_querys_tx values ('12730',90,1,1,'select test(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,100,100);
--Respuesta de correo.
insert into isys_querys_tx values ('12730',100,1,1,'select resp_procesa_12730(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);


CREATE or replace FUNCTION proc_procesa_get_xml_12730(varchar) RETURNS varchar AS $$
DECLARE
        xml1        alias for $1;
        xml2    varchar;
        v_nombre_xml    varchar;
        v_file_xml      varchar;
        sts     integer;
        v_almacen       varchar;
        --PDF
        v_pdf   boolean;
	uri_pdf varchar;

--Modificacion Caratula XML
        v_xml_final     varchar;
        v_caratula      varchar;
        v_dte           varchar;
--DatosResolucion       
        v_fecha_resol   varchar;
        v_num_resol     varchar;
        v_rut_emisor    varchar;
--FechaFirma
        --v_tabla_traza   varchar;
        v_fecha_firma   varchar;
	json_aux	json;
BEGIN
        xml2:='';
        xml2:=xml1;
--      insert into jsprueba select xml2;
        v_pdf := get_campo('ENVIA_PDF',xml2)::boolean;
--      insert into jsprueba select'inicio 12730 '|| v_pdf::varchar;
        xml2:= logapp(xml2,'12730 - Valida XML');
        if (get_campo('FALLA_CUSTODIUM',xml2)='SI') then
		--perform logfile('F_12730 Falla Custodium');
                xml2:=logapp(xml2,'Falla Custodium 12730: DTE no leido desde almacen URI='||get_campo('URI_IN',xml2));
                xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
        	xml2 := sp_procesa_respuesta_cola_motor(xml2);
                return xml2;
        end if;
	--perform logfile('F_12730 ENVIA_XML='||get_campo('ENVIA_XML',xml2));
        if (get_campo('ENVIA_XML',xml2)::boolean) then
--****************************Construccion DTE Ensobrado************************************************
                v_almacen := get_campo('XML_ALMACEN',xml2);
                v_rut_emisor:= split_part(decode(get_xml_hex(encode('RUTEmisor','hex'),get_campo('XML_ALMACEN',xml2)),'hex')::varchar,'-',1);
                select nro_resolucion, fecha_resolucion into v_num_resol, v_fecha_resol from contribuyentes where rut_emisor=v_rut_emisor::integer;
		if not found then
			v_num_resol:='0';
			v_fecha_resol:='2000-01-01';
		end if;
		json_aux:=lee_traza_evento(get_campo('URI_IN',xml2),'PUB');
		--Si no esta el evento Publicado...Error
		if (get_json('uri',json_aux)='') then
                	--perform logfile('F_12730 Falla Custodium');
	                xml2:=logapp(xml2,'Falla Leer Traza Pub 12730: URI='||get_campo('URI_IN',xml2));
        	        xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
                	xml2 := sp_procesa_respuesta_cola_motor(xml2);
	                return xml2;
	        end if;
		--v_fecha_firma:=get_json('fecha',json_aux);
		v_fecha_firma:=to_char(now(),'YYYY-MM-DD HH24:MI:SS');
		
                --v_tabla_traza:= get_tabla_traza(get_campo('URI_IN',xml2));
        --FechaFirma
                --if (v_tabla_traza<>'') then
                --        execute 'select fecha from '||v_tabla_traza||' where uri=$1 and evento=$2' into v_fecha_firma using get_campo('URI_IN',xml2),'PUB';
                --end if;
        --Construimos Caratula
                v_caratula := encode('<EnvioDTE version="1.0" xmlns="http://www.sii.cl/SiiDte" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.sii.cl/SiiDte EnvioDTE_v10.xsd">','hex');
		

                v_caratula := v_caratula || '0a' || encode('<SetDTE ID="ID','hex') ||get_xml_hex(encode('RUTEmisor','hex'),v_almacen) || encode('_','hex') || get_xml_hex(encode('TipoDTE','hex'),v_almacen) || encode('_','hex') || get_xml_hex(encode('Folio','hex'),v_almacen)|| encode('">','hex');
                xml2:=logapp(xml2,'caratula1='||coalesce(v_caratula,'NULA'));
                v_caratula := v_caratula || '0a' || encode('<Caratula version="1.0">','hex') || '0a';
                v_caratula := v_caratula || '0a' || encode('<RutEmisor>','hex') || get_xml_hex(encode('RUTEmisor','hex'),v_almacen) || encode('</RutEmisor>','hex');
                v_caratula := v_caratula || '0a' || encode('<RutEnvia>','hex') || get_xml_hex(encode('RUTEmisor','hex'),v_almacen) || encode('</RutEnvia>','hex');
                v_caratula := v_caratula || '0a' || encode('<RutReceptor>','hex') || get_xml_hex(encode('RUTRecep','hex'),v_almacen) || encode('</RutReceptor>','hex');
                v_caratula := v_caratula || '0a' || encode('<FchResol>','hex') || encode(v_fecha_resol::bytea,'hex') || encode('</FchResol>','hex');
                xml2:=logapp(xml2,'caratula2='||coalesce(v_caratula,'NULA'));
                v_caratula := v_caratula || '0a' || encode('<NroResol>','hex') || encode(v_num_resol::bytea,'hex') || encode('</NroResol>','hex');
                xml2:=logapp(xml2,'caratula2.1='||coalesce(v_caratula,'NULA'));
                v_caratula := v_caratula || '0a' || encode('<TmstFirmaEnv>','hex') || encode(v_fecha_firma::bytea,'hex') || encode('</TmstFirmaEnv>','hex');
                xml2:=logapp(xml2,'caratula2.2='||coalesce(v_caratula,'NULA'));
--		insert into jsprueba select v_caratula;
                --insert into jsprueba select 'caratula8: ' ||  v_fecha_firma;

                v_caratula := v_caratula || '0a' || encode('<SubTotDTE><TpoDTE>','hex') || get_xml_hex(encode('TipoDTE','hex'),v_almacen) || encode('</TpoDTE><NroDTE>1</NroDTE></SubTotDTE></Caratula>','hex');

                xml2:=logapp(xml2,'caratula3='||coalesce(v_caratula,'NULA'));

        --Sacamos <DTE
                v_dte := encode('<DTE ','hex')||split_part(split_part(get_campo('XML_ALMACEN',xml2),encode('<DTE ','hex'),2),encode('</DTE>','hex')::varchar,1)||encode('</DTE>','hex');
                v_xml_final:= encode('<?xml version="1.0" encoding="ISO-8859-1"?>','hex');
                v_xml_final := v_xml_final || '0a' || v_caratula || '0a' || v_dte || '0a' || encode('</SetDTE></EnvioDTE>','hex');
                xml2:=logapp(xml2,'caratula='||coalesce(v_caratula,'NULA'));
                
		if (v_xml_final is null) then
	                xml2:=logapp(xml2,'Falla XML_ENVIO nulo, no se envia URI='||get_campo('URI_IN',xml2));
        	        xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
                	xml2 := sp_procesa_respuesta_cola_motor(xml2);
	                return xml2;
		end if;
		xml2 := put_campo(xml2,'XML_ENVIO',coalesce(v_xml_final,'NULO'));
		--perform logfile('F_12730 v_caratula='||coalesce(v_caratula,'NULO'));
		--perform logfile('F_12730 v_dte='||coalesce(v_dte,'NULO'));

--*****************************************************************************

                --v_nombre_xml := nextval('correlativo_xml')::varchar||'.xml';
                --v_file_xml := '/opt/acepta/mail/file/'||v_nombre_xml;
		--xml2:=logapp(xml2,'Escribe XML en carpeta /opt/acepta/mail/file/'|| v_nombre_xml);
                --xml2:=logapp(xml2,'XML COMPLETO: ' || v_xml_final);
                --sts := write_file_hex(v_file_xml,v_xml_final);
                --if (sts<>1) then
                --        xml2:=logapp(xml2,'Mandato: Falla escribir XML en disco carpeta /opt/acepta/mail/file/');
                --        xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
                --        return xml2;
                --end if;
                --xml2 := put_campo(xml2,'NOMBRE_XML',v_nombre_xml);
        end if;
        xml2 := put_campo(xml2,'__SECUENCIAOK__','40');
	return xml2;

        if (v_pdf) then
                xml2 := put_campo(xml2,'URI_ORIGINAL',get_campo('URI_IN',xml2));
		uri_pdf := get_campo('URI_IN',xml2);
		uri_pdf := uri_pdf || '&xsl=http://www.custodium.com/docs/otros/dte/dte-xslfo.xsl';
		xml2 := put_campo(xml2,'URI_IN',uri_pdf);
		xml2 := put_campo(xml2,'__SECUENCIAOK__','20');
        else
                xml2 := put_campo(xml2,'__SECUENCIAOK__','40');
        end if;
        return xml2;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION proc_procesa_get_pdf_12730(varchar) RETURNS varchar AS $$
DECLARE
        xml1    alias for $1;
        xml2    varchar;
        v_nombre_pdf    varchar;
        v_file_pdf      varchar;
        sts     integer;

        --Corta PDF
        --data          varchar;
        --largo         integer;
        --pos_inicial   integer;
        --pos_final     integer;
BEGIN

        xml2:='';
        xml2:=xml1;
        xml2:= logapp(xml2,'12730 - VALIDA PDF');
	xml2 := put_campo(xml2,'URI_IN',get_campo('URI_ORIGINAL',xml2));
     -- insert into jsprueba select 'PDF:'||xml2;
        if (get_campo('FALLA_PDF_CUSTODIUM',xml2)='SI') then
                xml2:=logapp(xml2,'Mandato: Falla Obtencion de PDF desde el Almacen');
                xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
		xml2 := sp_procesa_respuesta_cola_motor(xml2);
                return xml2;
        end if;
        v_nombre_pdf := nextval('correlativo_pdf')::varchar||'.pdf';
        v_file_pdf := '/opt/acepta/mail/file/'||v_nombre_pdf;
        --sts := write_file_hex(v_file_pdf,get_campo('PDF_ALMACEN',xml2));
        sts := write_file_hex(v_file_pdf,encode('%PDF-1.4','hex')||split_part(get_campo('PDF_ALMACEN',xml2),encode('%PDF-1.4','hex'),2));
        if (sts<>1) then
                xml2:=logapp(xml2,'Mandato: Falla escribir pdf en disco carpeta /opt/acepta/mail/file/');
                xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
                return xml2;
        end if;
	
        xml2 := put_campo (xml2,'NOMBRE_PDF',v_nombre_pdf);
        return xml2;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION proc_procesa_12730(varchar) RETURNS varchar AS $$
DECLARE
        xml1    alias for $1;
        xml2    varchar;
        --Correo
        --id_message varchar;
        subject varchar;
        --Data Envia Correo
        v_data  varchar;
        --v_nombre_pdf varchar;
        v_pdf   varchar;
        v_xml   varchar;


        --CUERPO
        v_cuerpo varchar;
        v_tipo_dte      varchar;
        v_desc_dte      varchar;
        v_razon_social  varchar;
        ----
        v_almacen       varchar;
        v_monto         varchar;
	rut_owner1	varchar;
	rutemi1		varchar;
	rutrecep1	varchar;
	jsonsts1	json;
	json4		json;
		v_cuerpo1	varchar;

BEGIN
        xml2:='';
        xml2:=xml1;
        v_almacen := get_campo('XML_ALMACEN',xml2);
        v_pdf:= get_campo ('NOMBRE_PDF',xml2);
        v_xml:=get_campo('NOMBRE_XML',xml2);
        v_tipo_dte:= get_campo ('TIPO_DTE',xml2);
        v_monto := decode(get_xml_hex(encode('MntTotal','hex'),v_almacen),'hex');
        v_razon_social := decode(get_xml_hex(encode('RznSoc','hex'),v_almacen),'hex');

        subject:= codifica_url(get_campo ('SUBJECT',xml2)|| v_tipo_dte || ' - Emisor : ' || decode(get_xml_hex(encode('RUTEmisor','hex'),v_almacen),'hex') || ' - Folio : ' || get_campo('FOLIO',xml2));
        --return xml2;
---     if (get_campo('CANAL',xml2)<>'RECIBIDOS') then
--      else
--      end if;


        select initcap(replace(descripcion,'_',' ')) into v_desc_dte from tipo_dte where codigo = v_tipo_dte::integer;
        v_cuerpo := 'SE&#209;OR(ES):'|| chr(10) ||'Se adjunta '|| v_desc_dte || '.' || chr(10)|| 'Emitida por: ' || v_razon_social;
        v_cuerpo1 := 'SE&#209;OR(ES):<br>Se adjunta '|| v_desc_dte || '.<br>Emitida por: ' || v_razon_social;

        v_cuerpo:= v_cuerpo || chr(10) || 'Folio : ' || get_campo('FOLIO',xml2) || chr(10) || 'Monto : ' || v_monto || chr(10) || 'URI:' || get_campo('URI_IN',xml2);
        v_cuerpo1:= v_cuerpo1 ||'<br>Folio : ' || get_campo('FOLIO',xml2) ||'<br>Monto : ' || v_monto ||'<br>URI:' || get_campo('URI_IN',xml2);
        v_cuerpo:= codifica_url(v_cuerpo);
        if (v_pdf is null) then
                v_pdf:='';
        end if;
        if (v_xml is null ) then
                v_xml:='';
        end if;
        v_data:='mailfrom='||'noreply@acepta.com'||'&mailto='||replace(get_campo('MAIL_TO',xml2),';',',')|| '&subject='||subject||'&pdf='||v_pdf||'&xml='||v_xml||'&nombre_adjunto='||get_campo('RUT_EMISOR',xml2)||'_'||get_campo('TIPO_DTE',xml2)||'_'||get_campo('FOLIO',xml2)||'&cuerpo='||v_cuerpo;

        json4:='{}';
        if (get_campo('ENVIA_XML',xml2)::boolean) then
        	json4:=put_json(json4,'adjunta_xml','SI');
	        json4:=put_json(json4,'nombre_xml',get_campo('RUT_EMISOR',xml2)||'_'||get_campo('TIPO_DTE',xml2)||'_'||get_campo('FOLIO',xml2));
		json4:=put_json(json4,'flag_data_xml','SI');
	end if;
	if (get_campo('ENVIA_PDF',xml2)::boolean) then
        	json4:=put_json(json4,'adjunta_pdf','SI');
	        json4:=put_json(json4,'nombre_pdf',get_campo('RUT_EMISOR',xml2)||'_'||get_campo('TIPO_DTE',xml2)||'_'||get_campo('FOLIO',xml2));
	end if;
        json4:=put_json(json4,'uri',get_campo('URI_IN',xml2));
        json4:=put_json(json4,'INPUT_CUSTODIUM',get_campo('XML_ENVIO',xml2));
	--perform logfile('F_12730 XML_ENVIO.='||get_campo('XML_ENVIO',xml2));
        json4:=put_json(json4,'subject_hex',encode((get_campo ('SUBJECT',xml2)|| v_tipo_dte || ' - Emisor : ' || decode(get_xml_hex(encode('RUTEmisor','hex'),v_almacen),'hex') || ' - Folio : ' || get_campo('FOLIO',xml2))::bytea,'hex'));
        json4:=put_json(json4,'from_hex',encode('Distribución Acepta <noreply@acepta.com>'::bytea,'hex'));
	json4:=put_json(json4,'to',replace(get_campo('MAIL_TO',xml2),';',' '));
	json4:=put_json(json4,'msg_id','<ACP'||encripta_hash_evento_VDC(get_campo('RUT_EMISOR',xml2)||'##'||get_campo('TIPO_DTE',xml2)||'##'||get_campo('FOLIO',xml2)||'##'||to_char(now(),'YYYY-MM-DD')||'##'||get_campo('URI_IN',xml2)||'####'||get_campo('CANAL',xml2)||'##'||get_campo('RUT_RECEPTOR',xml2)||'##ENVIO_MAIL_EXITOSO##ENVIO_MAIL_FALLIDO')||'@motor2.acepta.com>');
	json4:=put_json(json4,'return_path','confirmacion_envio@custodium.com');
	json4:=put_json(json4,'ip_envio','172.16.14.82');
	json4:=put_json(json4,'tipo_envio','HTML');
	json4:=put_json(json4,'content_html',encode(v_cuerpo1::bytea,'hex'));
	
	rutemi1:=get_campo('RUT_EMISOR',xml2)||'-'||modulo11(get_campo('RUT_EMISOR',xml2));
	rutrecep1:=get_campo('RUT_RECEPTOR',xml2)||'-'||modulo11(get_campo('RUT_RECEPTOR',xml2));
	--Envia a la traza el evento de envio
	if (get_campo('CANAL',xml2)='EMITIDOS') then
		rut_owner1:=rutemi1;
	else
		rut_owner1:=rutrecep1;
	end if;
	
	json4:=put_json(json4,'url_traza','http://motor-prod.acepta.com:8082/motor/traza.fcgi');
	json4:=put_json(json4,'evento_ema','<trace source="SEND_MAIL" version="1.1"><node name="SEND_MAIL" stamp="'||to_char(now(),'YYYY-MM-DD')||'T'||to_char(now(),'HH24:MI:SS')||'" owner="'||rut_owner1||'"><keys><key name="rutEmisor" value="'||rutemi1||'"/><key name="tipoDTE" value="'||get_campo('TIPO_DTE',xml2)||'"/><key name="folio" value="'||get_campo('FOLIO',xml2)||'"/><key name="fchEmis" value=""/></keys><attrs><attr key="code">'||get_campo('TIPO_DTE',xml2)||'</attr><attr key="url">'||get_campo('URI_IN',xml2)||'</attr><attr key="relatedUrl"></attr><attr key="orig">'||rutrecep1||'</attr><attr key="dest">'||rutrecep1||'</attr><attr key="tag">'||get_campo('FOLIO',xml2)||'</attr><attr key="data"></attr><attr key="comment">Distribucion a:'||chr(10)||replace(get_campo('MAIL_TO',xml2),';',chr(10))||'</attr></attrs></node></trace>');
	
	xml2:=logapp(xml2,'json4='||json4::varchar);
	--perform logfile('F_12730 select send_mail_python2('''||json4::varchar||''')');
	--insert into jsprueba select json4::varchar;
	jsonsts1:=send_mail_python2(json4::varchar);
	if (get_json('status',jsonsts1)='OK') then
		xml2:=logapp(xml2,'Regla Distribucion Enviada OK retorno='||get_json('retorno_send_mail',jsonsts1));
	        xml2 := put_campo(xml2,'RESPUESTA','Status: 200 OK');
	else
	        xml2 := put_campo(xml2,'RESPUESTA','Status: 400 NK');
		xml2:=logapp(xml2,'Regla Distribucion Enviada Fallida'||jsonsts1::varchar);
	end if;
	
	--xml2 := sp_procesa_respuesta_cola_motor(xml2);
	--xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
	--return xml2;
        --POST
        --xml2:=put_campo(xml2,'SCRIPT','/opt/acepta/motor/Procesos/envio_mail_reglas_distribucion.sh "' || v_data || '"');
        --xml2:=logapp(xml2,'SCRIPT MAILJS'||get_campo('SCRIPT',xml2));
	xml2 := put_campo(xml2,'__SECUENCIAOK__','100');
        return xml2;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION resp_procesa_12730(varchar) RETURNS varchar AS $$
DECLARE
        xml1    alias for $1;
        xml2    varchar;
	v_destinatarios	varchar;
BEGIN
        xml2:='';
        xml2:=xml1;
	--v_destinatarios := replace(get_campo('MAIL_TO',xml2),';',chr(10));
	--xml2 := put_campo(xml2,'COMENTARIO_TRAZA',v_destinatarios);
        --xml2 := put_campo(xml2,'RESPUESTA','Status: 200 OK');
        xml2 := sp_procesa_respuesta_cola_motor(xml2);
        xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
        --xml2 := graba_bitacora(xml2,'SEND_MAIL');
        return xml2;
END;
$$ LANGUAGE plpgsql;

