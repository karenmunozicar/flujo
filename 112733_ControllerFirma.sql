delete from isys_querys_tx where llave='112733';

insert into isys_querys_tx values ('112733',5,19,1,'select control_flujo_80101(''$$__JSONCOMPLETO__["__PROC_ACTIVOS__","TX","REQUEST_URI","__ARGV__","__CATEGORIA_COLA__","__FLUJO_ACTUAL__"]$$''::json) as __json__',0,0,0,1,1,-1,10);
insert into isys_querys_tx values ('112733',10,19,1,'select prepara_mail_controller_112733(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,1000);
insert into isys_querys_tx values ('112733',15,1,2,'Servicio de Firma 192.168.3.17',4013,109,106,0,0,20,20);
insert into isys_querys_tx values ('112733',20,19,1,'select analiza_resp_firmador_112733(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,1000);
insert into isys_querys_tx values ('112733',30,19,1,'select envia_mail_controller_112733(''$$__XMLCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,1000);

insert into isys_querys_tx values ('112733',1000,19,1,'select borra_colas_112733(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

CREATE or replace FUNCTION analiza_resp_firmador_112733(varchar) RETURNS varchar AS $$
DECLARE
	text_all		alias for $1;
	text_input	varchar;
        json2        json;
        v_adjuntos      varchar;
	json3	json;
	sts1	integer;
	sts2	varchar;
	patron_correo1	varchar;
	json4	json;
	content1	varchar;
	correos_to1     varchar;
	campo	record;
	jsonsts1	json;	
	json_par1	json;
	data_lma	varchar;
	j3	json;
	v_caratula	varchar;
	flag_pendiente1	boolean;
	
	-- DAO-NBV 20180320 - Plantillas
        json_plantilla  json;
        json5           json;
        json6           json;
        content2        varchar;
        mensaje_plantilla       varchar;
        link_plantilla          varchar;
        nombre_plantilla        varchar;	
	json_msg	json;
	data_firma1	varchar;
	data1		varchar;
	j1	json;
	nodo_firma1	varchar;
	resp1	varchar;
	json_resp1	json;
	aux1	varchar;
BEGIN
        --text_input:=decode(get_campo('INPUT',text_all),'hex');
        text_input:=decode_hex(get_campo('INPUT',text_all));
	json2 := text_input::json;
        json2:=respuesta_no_chunked_json(json2);
        resp1:=decode_hex(get_campo('RESPUESTA_HEX',text_all));
        if (strpos(resp1,'HTTP/1.1 200 ')>0) then
        	--json_resp1:=split_part(resp1,'\012\012',2);
        	json_resp1:=split_part(resp1,chr(10)||chr(10),2);
        	aux1:=get_json('documentoFirmado',json_resp1);
                if (length(aux1)>0) then
                        data1:=base642hex(aux1);
			json3:=get_json('JSON3',json2);
			json3:=put_json(json3,'adjunta_xml','enviar_mail_con_xml_custodium');
			json3:=put_json(json3,'INPUT_CUSTODIUM',data1);
			--encode_hex('<EnvioDTE')||split_part(data1,encode_hex('<EnvioDTE'),2));	
			json2:=put_json(json2,'JSON3',json3::varchar);
	   		text_input:=put_campo(text_input,'__SECUENCIAOK__','30');
		else
			text_input:=logapp(text_input,'Falla Firma '||resp1);
	   		text_input:=put_campo(text_input,'__SECUENCIAOK__','1000');
			return text_input;
		end if;
	else
		text_input:=logapp(text_input,'..Falla Firma '||resp1||chr(10)||get_campo('INPUT_FIRMADOR',text_all));
		text_input:=put_campo(text_input,'__SECUENCIAOK__','1000');
		return text_input;
	end if;

	text_input:=put_campo(text_input,'INPUT',encode_hex(json2::varchar));
        return text_input;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION prepara_mail_controller_112733(varchar) RETURNS varchar AS $$
DECLARE
	text_all		alias for $1;
	text_input	varchar;
        json2        json;
        v_adjuntos      varchar;
	json3	json;
	sts1	integer;
	sts2	varchar;
	patron_correo1	varchar;
	json4	json;
	content1	varchar;
	correos_to1     varchar;
	campo	record;
	jsonsts1	json;	
	json_par1	json;
	data_lma	varchar;
	j3	json;
	v_caratula	varchar;
	flag_pendiente1	boolean;
	
	-- DAO-NBV 20180320 - Plantillas
        json_plantilla  json;
        json5           json;
        json6           json;
        content2        varchar;
        mensaje_plantilla       varchar;
        link_plantilla          varchar;
        nombre_plantilla        varchar;	
	json_msg	json;
	data_firma1	varchar;
	data1		varchar;
	j1	json;
	nodo_firma1	varchar;

BEGIN
        --text_input:=decode(get_campo('INPUT',text_all),'hex');
        text_input:=decode_hex(get_campo('INPUT',text_all));
	json2 := text_input::json;
	--Para que se vea bien el LOG
	json2:=put_json(json2,'__FLUJO_ACTUAL__',get_campo('__FLUJO_ACTUAL__',text_all));
	correos_to1:=get_json('C_MAILTO',json2);
	--Si no viene C_MAILTO, no se envia correo
	if (correos_to1='') then
		--Borramos de la cola
		json2:=logjson(json2,'CONTROLLER: No se envia correo, sin destinatario ');
		json2:= put_json(json2,'RESPUESTA','Status: 200 OK');
        	json2:=put_json(json2,'__SECUENCIAOK__','1000');
	        return json2;
	end if;
	
	if get_json('FLAG_PENDIENTE',json2)='SI' then
		flag_pendiente1:=true;
	else
		flag_pendiente1:=false;
	end if;	
	json2:=logjson(json2,'CONTROLLER: '||get_json('URI_IN',json2));

	json3:='{}';
	json3:=put_json(json3,'subject','ACEPTA-CONTROLLER: '|| get_json('C_SUBJECT',json2));
	json3:=put_json(json3,'to',get_json('C_MAILTO',json2));
	json3:=put_json(json3,'from','Avisos Acepta<noreply@acepta.com>');
	json3:=put_json(json3,'uri',get_json('URI_IN',json2));
	json2:=logjson(json2,'CONTROLLER: flag_pendiente1='||flag_pendiente1::varchar);
	json2:=logjson(json2,'CONTROLLER: ADJUNTO_CORREO='||get_json('ADJUNTO_CORREO',json2));
        if (strpos(get_json('ADJUNTO_CORREO',json2),'enviar_mail_con_xml')>0 and flag_pendiente1 is false) then
		if strpos(get_json('ADJUNTO_CORREO',json2),'enviar_mail_con_xml_custodium')>0 then
			json3:=put_json(json3,'adjunta_xml','enviar_mail_con_xml_custodium');
		elsif strpos(get_json('ADJUNTO_CORREO',json2),'enviar_mail_con_xml_sii')>0 then
			json3:=put_json(json3,'adjunta_xml','enviar_mail_con_xml_sii');
		else
			json3:=put_json(json3,'adjunta_xml','enviar_mail_con_xml');
		end if;
		json3:=put_json(json3,'nombre_xml',get_json('C_NOMBRE_ADJUNTO',json2));
		--Si necesito enviar con SetDTE armamos la caratula aca
		--if get_json('ADJUNTO_CORREO',json2)='enviar_mail_con_xml_sii' then
		if strpos(get_json('ADJUNTO_CORREO',json2),'enviar_mail_con_xml_sii')>0 then
			nodo_firma1:='ID'||get_json('RUT_EMISOR',json2)||'_'||get_json('TIPO_DTE',json2)||'_'||get_json('FOLIO',json2);
			if get_json('TIPO_DTE',json2) in ('39','41') then
				v_caratula := '<EnvioBOLETA version="1.0" xmlns="http://www.sii.cl/SiiDte" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.sii.cl/SiiDte EnvioDTE_v10.xsd">';
				v_caratula:=v_caratula||chr(10)||'<SetDTE ID="ID'||get_json('RUT_EMISOR',json2)||'_'||get_json('TIPO_DTE',json2)||'_'||get_json('FOLIO',json2)||'">'||chr(10)||'<Caratula version="1.0">'||chr(10)||'<RutEmisor>'||get_json('RUT_EMISOR',json2)||'-'||modulo11(get_json('RUT_EMISOR',json2))||'</RutEmisor>'||chr(10);
				v_caratula:=v_caratula||'<RutEnvia>'||get_json('RUT_EMISOR',json2)||'-'||modulo11(get_json('RUT_EMISOR',json2))||'</RutEnvia>'||chr(10);
				v_caratula:=v_caratula||'<RutReceptor>'||get_json('RUT_RECEPTOR',json2)||'-'||modulo11(get_json('RUT_RECEPTOR',json2))||'</RutReceptor>'||chr(10);
				v_caratula:=v_caratula||'<FchResol>'||get_json('FECHA_RESOLUCION',json2)||'</FchResol>'||chr(10);
				v_caratula:=v_caratula||'<NroResol>'||get_json('NRO_RESOLUCION',json2)||'</NroResol>'||chr(10);
				v_caratula:=v_caratula||'<TmstFirmaEnv>'||replace(to_char(now(),'YYYY-MM-DD HH24:MI:SS'),' ','T')||'</TmstFirmaEnv>'||chr(10);	
				v_caratula:=v_caratula||'<SubTotDTE><TpoDTE>'||get_json('TIPO_DTE',json2)||'</TpoDTE><NroDTE>1</NroDTE></SubTotDTE></Caratula>'||chr(10);
			else
				v_caratula := '<EnvioDTE version="1.0" xmlns="http://www.sii.cl/SiiDte" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.sii.cl/SiiDte EnvioDTE_v10.xsd">';
				v_caratula:=v_caratula||chr(10)||'<SetDTE ID="ID'||get_json('RUT_EMISOR',json2)||'_'||get_json('TIPO_DTE',json2)||'_'||get_json('FOLIO',json2)||'">'||chr(10)||'<Caratula version="1.0">'||chr(10)||'<RutEmisor>'||get_json('RUT_EMISOR',json2)||'-'||modulo11(get_json('RUT_EMISOR',json2))||'</RutEmisor>'||chr(10);
				v_caratula:=v_caratula||'<RutEnvia>'||get_json('RUT_EMISOR',json2)||'-'||modulo11(get_json('RUT_EMISOR',json2))||'</RutEnvia>'||chr(10);
				v_caratula:=v_caratula||'<RutReceptor>'||get_json('RUT_RECEPTOR',json2)||'-'||modulo11(get_json('RUT_RECEPTOR',json2))||'</RutReceptor>'||chr(10);
				v_caratula:=v_caratula||'<FchResol>'||get_json('FECHA_RESOLUCION',json2)||'</FchResol>'||chr(10);	
				v_caratula:=v_caratula||'<NroResol>'||get_json('NRO_RESOLUCION',json2)||'</NroResol>'||chr(10);
				v_caratula:=v_caratula||'<TmstFirmaEnv>'||replace(to_char(now(),'YYYY-MM-DD HH24:MI:SS'),' ','T')||'</TmstFirmaEnv>'||chr(10);	
				v_caratula:=v_caratula||'<SubTotDTE><TpoDTE>'||get_json('TIPO_DTE',json2)||'</TpoDTE><NroDTE>1</NroDTE></SubTotDTE></Caratula>'||chr(10);
			end if;
			json3:=put_json(json3,'caratula_hex_ini',encode(v_caratula::bytea,'hex')::varchar);
		end if;
	end if;
        if (strpos(get_json('ADJUNTO_CORREO',json2),'enviar_mail_con_pdf')>0 and flag_pendiente1 is false) then
		json3:=put_json(json3,'adjunta_pdf','SI');
		json3:=put_json(json3,'nombre_pdf',get_json('C_NOMBRE_ADJUNTO',json2));
	end if;
	--Nombre del xsl en la base de datos
	--json3:=put_json(json3,'tipo_envio_xsl','NO');
	--json3:=put_json(json3,'file_xsl','/opt/postgresql/9.2/colas_motor/mailcontroller.xsl');
	json3:=put_json(json3,'tipo_envio','HTML');
	
	json4:='{}';
	json4:=put_json(json4,'RAZON_EMPRESA',get_json('RUT_OWNER',json2));
	json4:=put_json(json4,'RUT_EMPRESA',get_json('RUT_OWNER',json2)||'-'||modulo11(get_json('RUT_OWNER',json2)));
	json4:=put_json(json4,'RUT_EMISOR',get_json('RUT_EMISOR',json2)||'-'||modulo11(get_json('RUT_EMISOR',json2)));	
	json4:=put_json(json4,'RUT_RECEPTOR',get_json('RUT_RECEPTOR',json2)||'-'||modulo11(get_json('RUT_RECEPTOR',json2)));	
	json4:=put_json(json4,'CANAL',get_json('CANAL',json2));
	json4:=put_json(json4,'CREADOR_REGLA','');
	json4:=put_json(json4,'ACCIONES',get_json('ACCIONES',json2));
        json4:=put_json(json4,'CORREOS_TO','<li>'||replace(correos_to1,' ','</li><li>')||'</li>');
	json4:=put_json(json4,'MOTIVO','Regla '||get_json('C_NOMBRE',json2));
	json4:=put_json(json4,'NOMBRE_REGLA',get_json('C_NOMBRE',json2));
        json4:=put_json(json4,'TIPO_DTE',get_json('TIPO_DTE',json2));
        json4:=put_json(json4,'FOLIO',get_json('FOLIO',json2));
	json4:=put_json(json4,'NOMBRE_CLIENTE','');
--	json4:=put_json(json4,'MENSAJE',get_json('C_MENSAJE',json2));
	json4:=put_json(json4,'URI_DOC',get_json('URI_IN',json2));

	if get_json('PLANTILLA_CONTROLLER',json2) not in ('','{}') then
		json_plantilla:=get_json('PLANTILLA_CONTROLLER',json2);
		if get_json('FLAG_PLANTILLA',json_plantilla)='true' then
			if get_json('NOMBRE_PLANTILLA',json_plantilla)='correo_generico_controller.html' then
				patron_correo1:=pg_read_file('./patron_correos/'||get_json('NOMBRE_PLANTILLA',json_plantilla));
			else
				patron_correo1:=pg_read_file('./patron_correos/patron_personalizado/'||get_json('NOMBRE_PLANTILLA',json_plantilla));
			end if;
			if get_json('MENSAJE_PLANTILLA',json_plantilla) <> '' then
				json_msg:=json4;
				json_msg:=put_json(json_msg,'ROL','<li>'||replace(get_json('ROLES',json_plantilla),',','</li><li>')||'</li>');
				json_msg:=put_json(json_msg,'NOMBRE_ROL','<li>'||replace(get_json('USUARIOS',json_plantilla),',','</li><li>')||'</li>');
				json_msg:=put_json(json_msg,'TIPO_DOCUMENTO',get_json('TIPO_DTE',json2));
				json_msg:=put_json(json_msg,'FOLIO_DOCUMENTO',get_json('FOLIO',json2));
				json_msg:=put_json(json_msg,'URI_DOC',get_json('URI_IN',json2));
				json_msg:=put_json(json_msg,'CARGO','<li>'||replace(get_json('CARGOS',json_plantilla),',','</li><li>')||'</li>');
				--mensaje_plantilla:=remplaza_tags_json_c(json_msg,get_json('MENSAJE_PLANTILLA',json_plantilla));
				mensaje_plantilla:=remplaza_tags_json_c(json_msg,decode(get_json('MENSAJE_PLANTILLA',json_plantilla),'hex')::varchar);

				json4:=put_json(json4,'INFORMACION_REGLA',get_json('C_NOMBRE',json2));
				json4:=put_json(json4,'MENSAJE',mensaje_plantilla);
				json4:=put_json(json4,'LINK',get_json('LINK_PLANTILLA',json_plantilla));
				json4:=put_json(json4,'NOMBRE_LINK',get_json('NOMBRE_LINK_PLANTILLA',json_plantilla));
			end if;
		else
			patron_correo1:=pg_read_file('./patron_correos/patron_correo_controller.html');
			json4:=put_json(json4,'MENSAJE',replace(replace(replace(get_json('C_MENSAJE',json2),chr(10),'<br>'),'Regla [[','<br><b>Regla [['),']]',']]</b>'));
		end if;
	else
		patron_correo1:=pg_read_file('./patron_correos/patron_correo_controller.html');
        	json4:=put_json(json4,'MENSAJE',replace(replace(replace(get_json('C_MENSAJE',json2),chr(10),'<br>'),'Regla [[','<br><b>Regla [['),']]',']]</b>'));
	end if;
	--json4:=put_json(json4,'CONDICIONES',get_json('C_MENSAJE_HTML',json2));
	json4:=put_json(json4,'CONDICIONES',replace(get_json('C_MENSAJE_HTML',json2),'Aplica:',''));
	--json4:=put_json(json4,'MENSAJE_ERROR','');

	json3:=put_json(json3,'CATEGORIA','CONTROLLER');
	json3:=put_json(json3,'RUT_OWNER',get_json('RUT_OWNER',json2));
	json3:=put_json(json3,'ip_envio','http://interno.acepta.com:8080/sendmail');	

	
	json_par1:=get_parametros_motor_json('{}','SERVIDOR_CORREO');
	json3:=put_json(json3,'return_path',get_json('PARAMETRO_RUTA',json_par1));
	
	--DAO 20180227 En el caso de los DNR viene la traza 
	json2:=put_json(json2,'URI_IN',replace(get_json('URI_IN',json2),'/traza/','/v01/'));

	json2:=put_json(json2,'JSON4',json4::varchar);
	json2:=put_json(json2,'patron_correo1',encode_hex(patron_correo1));
	--json2:=put_json(json2,'INPUT',encode_hex(json2::varchar));
        --json2:=put_json(json2,'__SECUENCIAOK__','20');

	--Vamos a Firmar el sobre en caso de que haya
	if get_json('caratula_hex_ini',json3)<>'' then
	   j1:=put_json('{}','uri',get_json('URI_IN',json2));
	   data1:=get_input_almacen(j1::varchar);
	   if (length(data1)=0) then
		json2 := logjson(json2,'Falla Lectura de la uri en el almacen');
        	json2:=put_json(json2,'__SECUENCIAOK__','1000');
		return json2;
	   end if;
	   json2:=put_json(json2,'rut_firma','13272698');
	   json2:=put_json(json2,'pass','aeqPaS3Mk3');
	   data_firma1:=encode_hex('<?xml version="1.0" encoding="ISO-8859-1"?>'||chr(10))||get_json('caratula_hex_ini',json3)||split_part(split_part(data1,encode_hex('<Content>'),2),encode_hex('</Content>'),1)||encode_hex(chr(10)||'</SetDTE></EnvioDTE>');	
	   --perform logfile('PDF='||hex2base64(data_firma1));
	   --data_firma1:=replace('{"documento":"'||hex2base64(data_firma1)||'","nodoId":"'||nodo_firma1||'","rutEmpresa":"'||get_json('rut_firma',json2)||'-'||modulo11(get_json('rut_firma',json2))||'","entidad":"SII","rutFirmante":"'||get_json('rut_firma',json2)||'-'||modulo11(get_json('rut_firma',json2))||'","codigoAcceso":"'||replace(get_json('pass',json2),chr(92),chr(92)||chr(92))||'"}',chr(10),'');
	   --DAO 20200303 sacamos los datos adjuntos...
	   data_firma1:=REGEXP_REPLACE(data_firma1,'3c4461746f7341646a756e746f73.*4461746f7341646a756e746f733e','','g'); -- <DatosAdjuntos.*DatosAdjuntos\>
           data_firma1:=REGEXP_REPLACE(data_firma1,'3c4461746f41646a756e746f.*4461746f41646a756e746f3e','','g'); -- <DatoAdjunto.*DatoAdjunto\>
	   data_firma1:=replace('{"documento":"'||hex2ascii2base64(data_firma1)||'","nodoId":"'||nodo_firma1||'","rutEmpresa":"'||get_json('rut_firma',json2)||'-'||modulo11(get_json('rut_firma',json2))||'","entidad":"SII","rutFirmante":"'||get_json('rut_firma',json2)||'-'||modulo11(get_json('rut_firma',json2))||'","codigoAcceso":"'||replace(get_json('pass',json2),chr(92),chr(92)||chr(92))||'"}',chr(10),'');
	   json2:=get_parametros_motor_json(json2,'FIRMADOR');
           text_input:=put_campo(text_input,'INPUT_FIRMADOR','POST '||get_json_upper('PARAMETRO_RUTA',json2)||' HTTP/1.1'||chr(10)||'User-Agent: curl/7.26.0'||chr(10)||'Host: '||get_json_upper('__IP_CONEXION_CLIENTE__',json2)||chr(10)||'Accept: '||chr(42)||'/'||chr(42)||chr(10)||'Content-Type: application/json; charset=ISO-8859-1'||chr(10)||'Content-Length: '||length(data_firma1)::varchar||chr(10)||chr(10)||data_firma1);
           --text_input:=put_campo(text_input,'INPUT_FIRMADOR','POST '||get_json_upper('PARAMETRO_RUTA',json2)||' HTTP/1.1'||chr(13)||chr(10)||'User-Agent: curl/7.26.0'||chr(13)||chr(10)||'Host: '||get_json_upper('__IP_CONEXION_CLIENTE__',json2)||chr(13)||chr(10)||'Accept: '||chr(42)||'/'||chr(42)||chr(13)||chr(10)||'Content-Type: application/json'||chr(13)||chr(10)||'Content-Length: '||length(data_firma1)::varchar||chr(10)||chr(10)||data_firma1);
	   json3:=put_json(json3,'flag_data_xml','SI');
  	   text_input:=put_campo(text_input,'__SECUENCIAOK__','15');
	   text_input:=put_campo(text_input,'__IP_CONEXION_CLIENTE__',get_json('__IP_CONEXION_CLIENTE__',json2));
           text_input:=put_campo(text_input,'__IP_PORT_CLIENTE__',get_json('__IP_PORT_CLIENTE__',json2));
	else
	   --Para el caso de que no se va a adjuntar nada, no tiene q ir al almacen a buscar el XML
	   if ((strpos(get_json('ADJUNTO_CORREO',json2),'enviar_mail_con_xml')=0 and strpos(get_json('ADJUNTO_CORREO',json2),'enviar_mail_con_pdf')=0) or flag_pendiente1) then
		json3:=put_json(json3,'flag_data_xml','NO');
	   end if;
	   text_input:=put_campo(text_input,'__SECUENCIAOK__','30');
	end if;
	json2:=put_json(json2,'JSON3',json3::varchar);
	text_input:=put_campo(text_input,'INPUT',encode_hex(json2::varchar));
        return text_input;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION envia_mail_controller_112733(varchar) RETURNS json AS $$
DECLARE
	text_all alias for $1;
	text_input	varchar;
        json2        json;
        v_adjuntos      varchar;
	json3	json;
	sts1	integer;
	sts2	varchar;
	patron_correo1	varchar;
	json4	json;
	content1	varchar;
	correos_to1     varchar;
	campo	record;
	jsonsts1	json;	
	json_par1	json;
	data_lma	varchar;
	j3	json;
	v_caratula	varchar;
	flag_pendiente1	boolean;
	
	-- DAO-NBV 20180320 - Plantillas
        json_plantilla  json;
        json5           json;
        json6           json;
        content2        varchar;
        mensaje_plantilla       varchar;
        link_plantilla          varchar;
        nombre_plantilla        varchar;	
	json_msg	json;
BEGIN
	text_input:=decode_hex(get_campo('INPUT',text_all));
	json2 := text_input::json;
	json3:=get_json('JSON3',json2)::json;
	json4:=get_json('JSON4',json2)::json;
	patron_correo1:=decode_hex(get_json('patron_correo1',json2));
        --text_input:=decode(get_campo('INPUT',text_all),'hex');
	--Grabo tantos correos como destinatarios tengo
	for campo in select trim(mail) as mail from (select * from regexp_split_to_table(get_json('to',json3),'[\,,\;, ]') mail) x where length(mail)>0 loop
		--Para la lectura de mail
        	data_lma := encripta_hash_evento_VDC('uri='||get_json('URI_IN',json2)||'&owner='||get_json('RUT_OWNER',json2)||'&rutEmisor='||get_json('RUT_EMISOR',json2)||'&tipoDTE='||get_json('TIPO_DTE',json2)||'&folio='||get_json('FOLIO',json2)||'&mail='||campo.mail||'&type=LMC'||'&rutRecep='||get_json('RUT_RECEPTOR',json2)||'&fchEmis='||get_json('FECHA_EMISION',json2)||'&relatedUrl=&comment=<b>Regla '||get_json('NOMBRE_REGLA',json2)||'</b>--<CHR10>--Mail Leído por '||trim(campo.mail)|| '&');
                json4:=put_json(json4,'LMA',get_json('__VALOR_PARAM__',json_par1)||'?hash='||data_lma);
	
		content1:=encode(remplaza_tags_json_c(json4,patron_correo1)::bytea,'hex');
	        json3:=put_json(json3,'content_html',coalesce(content1,''));
		
		j3=put_json('{}','E',get_json('RUT_EMISOR',json2));
                j3=put_json(j3,'T',get_json('TIPO_DTE',json2));
                j3=put_json(j3,'F',get_json('FOLIO',json2));
                j3=put_json(j3,'FE',get_json('FECHA_EMISION',json2));
                j3=put_json(j3,'C',get_json('CANAL',json2));
                j3=put_json(j3,'U',get_json('URI_IN',json2));
                j3=put_json(j3,'R',get_json('RUT_RECEPTOR',json2));
                j3=put_json(j3,'EO','CONTROLLER_ENVIO_MAIL_EXITOSO');
                j3=put_json(j3,'EN','CONTROLLER_ENVIO_MAIL_FALLIDO');
                j3=put_json(j3,'CO','<b>Regla '||get_json('NOMBRE_REGLA',json2)||'</b>');
                json3:=put_json(json3,'msg_id','<JCP'||encripta_hash_evento_VDC2(j3::varchar)||'@motor2.acepta.com>');

		--json3:=put_json(json3,'msg_id','<ACP'||encripta_hash_evento_VDC(get_json('RUT_EMISOR',json2)||'##'||get_json('TIPO_DTE',json2)||'##'||get_json('FOLIO',json2)||'##'||get_json('FECHA_EMISION',json2)||'##'||get_json('URI_IN',json2)||'####'||get_json('CANAL',json2)||'##'||get_json('RUT_RECEPTOR',json2)||'##CONTROLLER_ENVIO_MAIL_EXITOSO##CONTROLLER_ENVIO_MAIL_FALLIDO##<b>Regla '||get_json('NOMBRE_REGLA',json2)||'</b>')||'@motor2.acepta.com>');


		json3:=put_json(json3,'to',campo.mail);
		--perform logfile('send_mail_python2_colas='||json3::varchar);
		json2:=logjson(json2,'CONTROLLER: flag_data_xml= '||get_json('flag_data_xml',json3)||' adjunta_xml='||get_json('adjunta_xml',json3)||' adjunta_pdf='||get_json('adjunta_pdf',json3));
		jsonsts1:=send_mail_python2_colas(json3::varchar);
		if (get_json('status',jsonsts1)='OK') then
			--Borramos de la cola
			json2:=logjson(json2,'CONTROLLER: Correo Enviado OK '||campo.mail||' URI='||get_json('URI_IN',json2));
			json2:= put_json(json2,'RESPUESTA','Status: 200 OK');
		else
			json2:=logjson(json2,'CONTROLLER: Falla Envio de Correo '||campo.mail||' URI='||get_json('URI_IN',json2)||' jsonsts1='||jsonsts1::varchar);
			--perform logfile('send_mail_python1=Falla Envio Correo '||json3::varchar);
		end if;
	end loop;

        json2:=put_json(json2,'__SECUENCIAOK__','1000');
        return json2;
END;
$$ LANGUAGE plpgsql;




--Valida si necesita PDF - XML 
CREATE or replace FUNCTION ini_controller_112733(varchar) RETURNS json AS $$

DECLARE
	text_all	alias for $1;
        text_input      varchar;
        json2  	     json;
	v_adjuntos	varchar;
BEGIN
	json2:=put_json(json2,'__SECUENCIAOK__','0');
        text_input:=decode(get_campo('INPUT',text_all),'hex');
	json2 := text_input::json;
	if (strpos(get_json('ADJUNTO_CORREO',json2),'enviar_mail_con_xml')>0) then
		json2:= put_json(json2,'ADJUNTA_XML','SI');
	end if;
	if (strpos(get_json('ADJUNTO_CORREO',json2),'enviar_mail_con_pdf')>0) then
		json2:= put_json(json2,'ADJUNTA_PDF','SI');
	end if;

	json2:=put_json(json2,'__SECUENCIAOK__','15');

        return json2;
END;
$$ LANGUAGE plpgsql;
CREATE or replace FUNCTION valida_xml_controller_112733(json) RETURNS json AS $$

DECLARE
        json1        alias for $1;
        json2        json;
	
	--Archivo
	v_nombre_archivo	varchar;
	
BEGIN

        json2 := json1;
	
	if (get_json('FALLA_CUSTODIUM',json2)='SI') then
		json2:=logjson(json2,'Falla Custodium 112733: DTE no leido desde almacen URI=' || get_json('URI_IN',json2));
		json2:=put_json(json2,'__SECUENCIAOK__','1000');
		return json2;
	end if;	
	json2:=logjson(json2,'112733: DTE leido OK URI=' || get_json('URI_IN',json2));

	if (get_json('ADJUNTA_PDF',json2)='SI') then
		json2:=put_json(json2,'__SECUENCIAOK__','16');
	else
		json2:=put_json(json2,'__SECUENCIAOK__','30');
	end if;
	return json2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION borra_colas_112733(varchar) RETURNS varchar AS $$

DECLARE
        xml1           alias for $1;
        xml2           varchar;
	v_respuesta	varchar;
BEGIN
	xml2:=xml1;
	xml2 := logapp(xml2,'CONTROLLER: va a sp_procesa_respuesta_cola_motor');
	xml2 := sp_procesa_respuesta_cola_motor(xml2);

        return xml2;
END;
$$ LANGUAGE plpgsql;

