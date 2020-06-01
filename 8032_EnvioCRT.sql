delete from isys_querys_tx where llave='8032';

insert into isys_querys_tx values ('8032',60,45,1,'select genera_crt_8032(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,1000);

insert into isys_querys_tx values ('8032',70,1,2,'Servicio de Firma 192.168.3.17',4013,109,106,0,0,80,80);

insert into isys_querys_tx values ('8032',80,45,1,'select verifica_firma_crt_8032(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);
--Publica CRT
insert into isys_querys_tx values ('8032',90,1,8,'Publica DTE',112704,0,0,0,0,100,100);

--Envia mail CRT
insert into isys_querys_tx values ('8032',100,19,1,'select send_mail_crt_8032(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);
--Graba evento
insert into isys_querys_tx values ('8032',120,45,1,'select graba_evento_crt_8032(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('8032',1000,19,1,'select sp_procesa_respuesta_cola_motor88_json(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,0,0);


CREATE OR REPLACE FUNCTION public.genera_crt_8032(json) RETURNS json AS $$
DECLARE
        json1                   alias for $1;
        json2                   json;
        json3                   json;
        patron_dte1             varchar;
        xml_dte1                varchar;
        dte1                    varchar;
        campo                   record;
        campo1                  record;

        razonReceptor           varchar;
        razonEmisor             varchar;

        RecepEnvGlosa           varchar;
        estadoRecepcionEnvio    varchar;
        detallesEnvio           json;

        i                       integer;
        j                       integer;
        aux                     varchar;
        aux2                    json;
        dte                     varchar;
        detalleDte              json;
	rut_emisor1		varchar;
	rut_receptor1		varchar;
	detalle1		varchar;
	tms1			varchar;
	rut_firma1		varchar;
	pass1			varchar;
	id1			varchar;
	data_firma1		varchar;
	folio1			varchar;
	tipo_dte1		varchar;
	uri1		varchar;
	html1		varchar;
	json_aux1	json;
BEGIN
	--perform logfile('JSON1='||json1::varchar);
	--El input viene encodeado, pero es un json
	--json2:=decode_hex(get_json('INPUT',json1))::json;
	json2:=json1;
	--Limpio el INPUT de la memoria del procesador
	json2:=put_json(json2,'INPUT','');
	

	json2:=logjson(json2,'Se genera CRT');
	rut_emisor1:=get_json('RUT_EMISOR',json2);
	rut_receptor1:=get_json('RUT_RECEPTOR',json2);
	json3:='{}';
        json3:=put_json(json3,'RUTEmisor',rut_emisor1||'-'||modulo11(rut_emisor1));
	--Alvaro Gonzalez
	select rut,clave from token_hsm_sii where rut='13272698-1' into rut_firma1,pass1;
        json3:=put_json(json3,'RutFirma',rut_firma1);
        --Caratula
        json3:=put_json(json3,'RutResponde',rut_receptor1||'-'||modulo11(rut_receptor1));
        json3:=put_json(json3,'RutRecibe',rut_emisor1||'-'||modulo11(rut_emisor1));

	--secuencia (numero de identificacion de respuesta generado por quien responde)
        json3:=put_json(json3,'IdRespuesta',get_json('CODIGO_TXEL',json2));
        json3:=put_json(json3,'NroDetalles','1');
	tms1:=to_char(now(),'YYYY-MM-DD')||'T'||to_char(now(),'HH24:MM:SS');
        -- Fecha de la firma
        json3:=put_json(json3,'TmstFirmaResp',tms1);
        --RecepcionEnvio
        json3:=put_json(json3,'NmbEnvio',decode_latin1(get_json('NmbEnvio',json2)));
        json3:=put_json(json3,'FchRecep',tms1);
        json3:=put_json(json3,'FchRecepDis',to_char(now(),'DD/MM/YYYY'));
        -- secuencia (Codigo unico de envio generado por el receptor)
        json3:=put_json(json3,'CodEnvio',get_json('CODIGO_TXEL',json2));
        json3:=put_json(json3,'EnvioDTEID',decode_latin1(split_part(get_json('NmbEnvio',json2),'.',1)));
        json3:=put_json(json3,'Digest',get_json('DigestSobre',json2));
        json3:=put_json(json3,'RutEmisor',rut_emisor1||'-'||modulo11(rut_emisor1));
        json3:=put_json(json3,'RutReceptor',rut_receptor1||'-'||modulo11(rut_receptor1));
        json3:=put_json(json3,'NroDTE','1');

	if (get_json('FLAG_DTE_RECIBIDO_REPETIDO',json2)='SI') then
		json2:=logjson(json2,'FLAG_DTE_RECIBIDO_REPETIDO URI_DTE_REPETIDO='||get_json('URI_DTE_REPETIDO',json2));
		--Si esta repetido, verificamos que realmente este el PRE en la traza
		--Del DTE encontrado en recibidos
		json_aux1:=lee_traza_evento(get_json('URI_DTE_REPETIDO',json2),'PRE');
		--Si ya se envio con dTe original, entonces es un repetido
		if (strpos(get_json('comentario1',json_aux1),'DTE Recibido (0)')>0) then
			json2:=logjson(json2,'PRE ya enviado');
			--Si ya envie 5 veces, no lo hago mas
			if (get_json('veces',json_aux1)::integer>5) then
				json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
				json2:=logjson(json2,'PRE enviado muchas veces, se ignora el DTE');
				json2:=put_json(json2,'__SECUENCIAOK__','1000');
				return json2;
			end if;
			estadoRecepcionEnvio:='4';
			RecepEnvGlosa:='DTE No Recibido - DTE Repetido';
			json3:=put_json(json3,'EstadoRecepEnvM','RECHAZADO');
			json3:=put_json(json3,'RecepEnvGlosaM','EnvioDTE Recibido');
			json3:=put_json(json3,'DetalleGlosa','DTE No Recibido - DTE Repetido');
			json2:=logjson(json2,'Se envia CRT Repetido');
		else
			--No es un repetido, si el DTe original no tiene PRE Recibido
			estadoRecepcionEnvio:='0';
			RecepEnvGlosa:='Envio Recibido Conforme';
			json3:=put_json(json3,'EstadoRecepEnvM','RECIBIDO');
			json3:=put_json(json3,'RecepEnvGlosaM','EnvioDTE Recibido');
			json3:=put_json(json3,'DetalleGlosa','DTE Recibido');
			--Desmarcamos el FLAG
			json2:=put_json(json2,'FLAG_DTE_RECIBIDO_REPETIDO','');
			json2:=logjson(json2,'PRE no enviado, se envia OK');
		end if;
        else
		estadoRecepcionEnvio:='0';
		RecepEnvGlosa:='Envio Recibido Conforme';
		json3:=put_json(json3,'EstadoRecepEnvM','RECIBIDO');
		json3:=put_json(json3,'RecepEnvGlosaM','EnvioDTE Recibido');
		json3:=put_json(json3,'DetalleGlosa','DTE Recibido');
		json2:=logjson(json2,'PRE no enviado, se envia OK.');
	end if;
        json3:=put_json(json3,'EstadoRecepEnv',estadoRecepcionEnvio);
        json3:=put_json(json3,'RecepEnvGlosa',RecepEnvGlosa);
	
	folio1:=get_json('FOLIO',json2);
	tipo_dte1:=get_json('TIPO_DTE',json2);
        json3:=put_json(json3,'TipoDte',tipo_dte1);
        json3:=put_json(json3,'Folio',folio1);
        json3:=put_json(json3,'DominioEmisor',get_json('DOMINIO_EMISOR',json2));
	json3:=put_json(json3,'FechaEmision',to_char(to_timestamp(get_json('FECHA_EMISION',json2),'YYYY-MM-DD'),'DD/MM/YYYY'));
	--FAY-DAO 2018-03-01 El monto viene con caracteres especiales (salto de libeas), para que no se caiga
	begin
		json3:=put_json(json3,'MontoTotal',edita_monto(get_json('MONTO_TOTAL',json2)));
	exception when others then
		json3:=put_json(json3,'MontoTotal',get_json('MONTO_TOTAL',json2));
	end;
        
	uri1:='http://'||get_json('DOMINIO_EMISOR',json2)||to_char(now(),'YYMM')||'.acepta.com/v01/'||genera_uri(rut_emisor1,tipo_dte1,'CRT_'||folio1,tms1,'R');
	json2:=logjson(json2,'URI_CRT='||uri1);
	json3:=put_json(json3,'custodium-uri',uri1);
	json2:=put_json(json2,'URI_CRT',uri1);

        patron_dte1:=pg_read_file('./patron_dte_10k/patron_crt.xml');
        if (patron_dte1='' or patron_dte1 is null) then
		json2:=put_json(json2,'RESPUESTA','Status: 400 NK');
		json2:=logjson(json2,'Falla Insercion no existe patron de DTE');
		return json2;
        end if;
        json3:=escape_xml_characters(json3::varchar)::json;
	detalle1:='<RecepcionDTE><TipoDTE>'||get_json('TIPO_DTE',json2)||'</TipoDTE><Folio>'||get_json('FOLIO',json2)||'</Folio><FchEmis>'||get_json('FECHA_EMISION',json2)||'</FchEmis><RUTEmisor>'||rut_emisor1||'-'||modulo11(rut_emisor1)||'</RUTEmisor><RUTRecep>'||rut_receptor1||'-'||modulo11(rut_receptor1)||'</RUTRecep><MntTotal>'||get_json('MONTO_TOTAL',json2)||'</MntTotal><EstadoRecepDTE>'||estadoRecepcionEnvio||'</EstadoRecepDTE><RecepDTEGlosa>'||RecepEnvGlosa||'</RecepDTEGlosa></RecepcionDTE>';
	json3:=put_json(json3,'Detalle_RecepcionDTE',detalle1);

	id1:='RespuestaDTE-'||rut_receptor1||'-'||modulo11(rut_receptor1)||'-'||rut_emisor1||'-'||modulo11(rut_emisor1)||'-'||to_char(now(),'YYYYMMDD')||'-'||get_json('IdRespuesta',json3);
	json3:=put_json(json3,'NODO_ID',id1);
	json2:=put_json(json2,'ID_MAIL',id1);
	--Patron CRT
        dte1:=remplaza_tags_json_c(json3,patron_dte1);
	dte1:=encode(dte1::bytea,'hex');
	--RznSocResponde
	dte1:=replace(dte1,'242424527a6e536f63526573706f6e6465242424',get_json('RAZON_SOC_RECEPTOR_HEX',json2));
	--RznSocRecibe
	dte1:=replace(dte1,'242424527a6e536f63526563696265242424',get_json('RAZON_SOC_EMISOR_HEX',json2));
	--json3:=put_json(json3,'RznSocResponde',get_json('RAZON_SOC_RECEPTOR',json2));
        --json3:=put_json(json3,'RznSocRecibe',get_json('RAZON_SOC_EMISOR',json2));
        --dte1:=limpia_tags(dte1);

	--Genero Html para el mail
        patron_dte1:=pg_read_file('./patron_dte_10k/patron_mail_acuse_recibo_inter.html');
        if (patron_dte1='' or patron_dte1 is null) then
		json2:=put_json(json2,'RESPUESTA','Status: 400 NK');
		json2:=logjson(json2,'Falla Insercion no existe patron de mail de acuse DTE');
		return json2;
        end if;
	html1:=remplaza_tags_json_c(json3,patron_dte1);
	html1:=encode(html1::bytea,'hex');
	html1:=replace(html1,'242424527a6e536f63526573706f6e6465242424',get_json('RAZON_SOC_RECEPTOR_HEX',json2));
	html1:=replace(html1,'242424527a6e536f63526563696265242424',get_json('RAZON_SOC_EMISOR_HEX',json2));
	--html1:=limpia_tags(html1);
	--json2:=put_json(json2,'html_mail_crt',encode(html1::bytea,'hex')::varchar);
	json2:=put_json(json2,'html_mail_crt',html1);
        
	--data_firma1:=replace('{"documento":"'||str2latin12base64(dte1)||'","nodoId":"'||id1||'","rutEmpresa":"'||rut_firma1||'","entidad":"SII","rutFirmante":"'||rut_firma1||'","codigoAcceso":"'||replace(pass1,chr(92),chr(92)||chr(92))||'"}',chr(10),'');
	data_firma1:=replace('{"documento":"'||hex2ascii2base64(dte1)||'","nodoId":"'||id1||'","rutEmpresa":"'||rut_firma1||'","entidad":"SII","rutFirmante":"'||rut_firma1||'","codigoAcceso":"'||replace(pass1,chr(92),chr(92)||chr(92))||'"}',chr(10),'');
	--json2:=logjson(json2,'data_firma1='||data_firma1);
	
	json2:=get_parametros_motor_json(json2,'FIRMADOR_OFFLINE');
	json2:=put_json(json2,'IP_PORT_CLIENTE',get_json('__IP_PORT_CLIENTE__',json2));
	json2:=put_json(json2,'IP_CONEXION_CLIENTE',get_json('__IP_CONEXION_CLIENTE__',json2));
	--Para el reproceso
	json2:=put_json(json2,'IPPORT_SII',get_json('__IP_CONEXION_CLIENTE__',json2)||':'||get_json('__IP_PORT_CLIENTE__',json2));
        json2:=put_json(json2,'INPUT_FIRMADOR','POST '||get_json_upper('PARAMETRO_RUTA',json2)||' HTTP/1.1'||chr(10)||'User-Agent: curl/7.26.0'||chr(10)||'Host: '||get_json_upper('__IP_CONEXION_CLIENTE__',json2)||chr(10)||'Accept: '||chr(42)||'/'||chr(42)||chr(10)||'Content-Type: application/json; charset=ISO-8859-1'||chr(10)||'Content-Length: '||length(data_firma1)::varchar||chr(10)||chr(10)||data_firma1);

	--json2:=logjson(json2,'CRT:'||data_firma1);

	json2:=put_json(json2,'__SECUENCIAOK__','70');

	return json2;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION public.verifica_firma_crt_8032(json) RETURNS json AS $$
DECLARE
        json1                   alias for $1;
        json2                   json;
        json3                   json;
	resp1	varchar;
	json_resp1	varchar;	
	aux1	varchar;
	data1	varchar;
        campo           record;
BEGIN
	json2:=json1;
	json2 :=put_json(json2,'__SECUENCIAOK__','1000');
	json2:=respuesta_no_chunked_json(json2);
        resp1:=decode(get_json_upper('RESPUESTA_HEX',json2),'hex');
        json_resp1:=split_part(resp1,'\012\012',2);
        json2:=put_json(json2,'RESPUESTA_HEX','');

	--json2:=logjson(json2,get_json('INPUT_FIRMADOR',json2));

       	if (strpos(resp1,'HTTP/1.1 200 ')>0) then
                aux1:=get_json('documentoFirmado',json_resp1::json);
                if (length(aux1)>0) then
        		json2:=put_json(json2,'INPUT_FIRMADOR','');
			json2:=logjson(json2,'CRT Firmado OK');	
		        data1:=base642hex(aux1);
                        json2:=put_json(json2,'INPUT',data1);
                        json2:=put_json(json2,'CONTENT_LENGTH',(length(data1)/2)::varchar);
                        --Se procesa por el 8010
                        json2:=put_json(json2,'SCRIPT_NAME','/ca4/ca4dte');
                        json2:=put_json(json2,'__SECUENCIAOK__','90');
                        json2:=put_json(json2,'RESPUESTA','');
			--Guardamos la URI del dte
			json2:=logjson(json2,'URI_IN='||get_json('URI_IN',json2));
			json2:=logjson(json2,'URI_CRT='||get_json('URI_CRT',json2));

			json2:=put_json(json2,'URI_REC',get_json('URI_IN',json2));
			json2:=put_json(json2,'URI_IN',get_json('URI_CRT',json2));

			json2:=logjson(json2,'URI_IN='||get_json('URI_IN',json2));
			json2:=logjson(json2,'URI_CRT='||get_json('URI_CRT',json2));
			--Limpiamos el publicado ok
			json2:=put_json(json2,'__PUBLICADO_OK__','');
			json2:=put_json(json2,'__FLAG_CLIENTE_COMUNIDAD__','');
			return json2;
		else
			json2:=logjson(json2,'Respuesta Firmador '||json_resp1::varchar);
			json2:=logjson(json2,'Servicio de Firma no responde documento Firmado');
			json2 := put_json(json2,'RESPUESTA','Status: 400 NK');
			return json2;
		end if;
	else
		json2:=logjson(json2,'Respuesta Firmador '||json_resp1::varchar);
		json2:=logjson(json2,'Servicio de Firma Falla');
	 	json2:= put_json(json2,'RESPUESTA','Status: 400 NK');
		return json2;
	end if;
        return json2;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.graba_evento_crt_8032(varchar) RETURNS varchar AS $$
DECLARE
        xml1                   alias for $1;
        xml2                   varchar;
	rut1	varchar;
	aux1	varchar;
	lista1	json;
	i	integer;
	file1	varchar;
	j3	json;
	cod1	bigint;
BEGIN
        xml2:=xml1;
	xml2 := put_campo(xml2,'__SECUENCIAOK__','1000');
	--RME Agrega Canal y Rut Owner.
	xml2 := put_campo(xml2,'CANAL','RECIBIDOS');
	xml2 := put_campo(xml2,'RUT_OWNER',get_campo('RUT_RECEPTOR',xml2));
	xml2:=put_campo(xml2,'URI_IN',get_campo('URI_REC',xml2));
	xml2:=put_campo(xml2,'URL_GET',get_campo('URI_CRT',xml2));
	aux1:=get_campo('MAIL_EMISOR',xml2);
	if (get_campo('FLAG_DTE_RECIBIDO_REPETIDO',xml2)='SI') then
		rut1:=get_campo('RUT_EMISOR',xml2);
		aux1:=(select email from contribuyentes where rut_emisor=rut1::integer);
		xml2:=put_campo(xml2,'COMENTARIO_TRAZA','Recibe: '||coalesce(aux1,'-')||chr(10)||'Glosa: DTE Repetido (4)');
		xml2:=logapp(xml2,'URI_REC='||get_campo('URI_REC',xml2)||' URI_DTE_REPETIDO='||get_campo('URI_DTE_REPETIDO',xml2));
		--Si tienen uris distintas los relaciono
		if (get_campo('URI_REC',xml2)<>get_campo('URI_DTE_REPETIDO',xml2)) then
			--Relaciono el DTE repetido con el encontrado en dte_recibidos
			xml2:=logapp(xml2,'Se relaciona URI='||get_campo('URI_REC',xml2)||' URI_DTE_REPETIDO='||get_campo('URI_DTE_REPETIDO',xml2));
			aux1:=graba_documentos_relacionados(get_campo('URI_REC',xml2),get_campo('TIPO_DTE',xml2),get_campo('FOLIO',xml2),get_campo('URI_DTE_REPETIDO',xml2),get_campo('TIPO_DTE',xml2),get_campo('FOLIO',xml2));
		end if;
	else
		xml2:=put_campo(xml2,'COMENTARIO_TRAZA','Recibe: '||coalesce(aux1,'-')||chr(10)||'Glosa: DTE Recibido (0)');
	end if;
        xml2:=put_campo(xml2,'EVENTO','PRE');
        xml2:=graba_bitacora(xml2,'PRE');

	--Grabamos los archivos adjuntos en la traza si existen
	i:=0;
	xml2:=logapp(xml2,'Adjuntos '||get_campo('adjuntos',xml2));
	if(is_json_list(get_campo('adjuntos',xml2))) then
		lista1:=get_campo('adjuntos',xml2)::json;
		aux1:=get_json_index(lista1,i);
		--Marcamos el DTE que tiene Adj
		if aux1<>'' then
			cod1:=get_campo('CODIGO_TXEL',xml2);
			xml2:=logapp(xml2,'Marco dte_recibidos '||cod1::varchar);
			update dte_recibidos set data_dte=coalesce(data_dte,'')||'<ADJ>SI</ADJ>' where codigo_txel=cod1::bigint;
		end if;

		xml2:=logapp(xml2,'Entra a grabar ADJ');
		while (aux1<>'') loop
			xml2:=logapp(xml2,'Graba ADJ '||aux1);
			--Grabo Evento con Documento Adjunto
			j3:=aux1::json;	
			xml2:=put_campo(xml2,'COMENTARIO_TRAZA','PDF Adjunto en Sobre de Envío ('||get_json('nombre',j3)||')');
			xml2:=put_campo(xml2,'URL_GET',split_part(get_json('uri',j3),'.gz',1));
			xml2:=put_campo(xml2,'COMENTARIO2','PDF Adjunto');
			xml2:=graba_bitacora(xml2,'ADJ');	
			i:=i+1;
			aux1:=get_json_index(lista1,i);
		end loop;
	else
		xml2:=logapp(xml2,'Sin Adjuntos '||get_campo('adjuntos',xml2));
	end if;

        xml2 := put_campo(xml2,'RESPUESTA','Status: 200 OK');
        return xml2;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.send_mail_crt_8032(json) RETURNS json AS $$
DECLARE
        json1                   alias for $1;
        json2        json;
	json4	json;

	--jsonsts1	json;
	input1	varchar;
	aux1	varchar;
	rut1	varchar;
	sts1	varchar;
BEGIN
	json2:=json1;

        if (get_json('__PUBLICADO_OK__',json2)<>'SI') then
                json2:=logjson(json2,'Falla la Publicacion en Almacen del CRT '||get_json('URI_IN',json2)||' '||get_json('URI_CRT',json2));
                json2 := put_json(json2,'RESPUESTA','Status: 400 NK');
                json2 := put_json(json2,'__SECUENCIAOK__','1000');
                return json2;
        end if;

	json2:=put_json(json2,'__SECUENCIAOK__','1000');
	input1:=get_json('INPUT',json2);

	rut1:=get_json('RUT_RECEPTOR',json2);
	aux1:=(select email from contribuyentes where rut_emisor=rut1::integer);
	json2:=put_json(json2,'MAIL_RECEPTOR',aux1);
        json4:='{}';
        json4:=put_json(json4,'uri',get_json('URI_IN',json2));
	--perform logfile('INPUT_CUSTODIUM ANTES='||input1);
        json4:=put_json(json4,'INPUT_CUSTODIUM',encode('<RespuestaDTE','hex')||split_part(split_part(input1,encode('<RespuestaDTE','hex'),2),encode('</RespuestaDTE>','hex'),1)||encode('</RespuestaDTE>','hex'));
	json4:=put_json(json4,'flag_data_xml','SI'); --Para que no saque el contenido de la URI
	--perform logfile('INPUT_CUSTODIUM='||get_json('INPUT_CUSTODIUM',json4));
        json4:=put_json(json4,'subject_hex',encode(('Acuse de Recibo de DTEs -'||chr(10)||get_json('ID_MAIL',json2))::bytea,'hex'));
        json4:=put_json(json4,'from_hex',get_json('RAZON_SOC_RECEPTOR_HEX',json2)||encode((' <'||aux1||'>')::bytea,'hex')::varchar);
        json4:=put_json(json4,'to',trim(get_json('MAIL_EMISOR',json2)));
        --json4:=put_json(json4,'to','fernando.arancibia@acepta.com');
        json4:=put_json(json4,'tipo_envio','HTML');
	json4:=put_json(json4,'content_html',get_json('html_mail_crt',json2));
	json2:=put_json(json2,'html_mail_crt','');
        --Buscamos el xsl que le corresponde
        json4:=put_json(json4,'file_xsl','/opt/acepta/motor/xsl/CRT/comprobanterecepcion.xsl');
        --json4:=put_json(json4,'ip_envio','172.16.14.82');
	json4:=put_json(json4,'adjunta_xml','SI');
	json4:=put_json(json4,'nombre_xml','crt_'||get_json('FOLIO',json2)||'_'||get_json('TIPO_DTE',json2));
	--perform logfile('F_8032 select send_mail_python2('''||json4::varchar||''')');
	--raise notice 'xml=%',get_json('INPUT_CUSTODIUM',json4);

	json4:=put_json(json4,'CATEGORIA','CRT');
        json4:=put_json(json4,'RUT_OWNER',rut1::varchar);
       	json4:=put_json(json4,'ip_envio','http://interno.acepta.com:8080/sendmail');
	if (strpos(graba_mail_cola(json4),'OK')>0) then
        --jsonsts1:=send_mail_python2_colas(json4::varchar);
	--jsonsts1:=send_mail_python2(json4::varchar);
        --if (get_json('status',jsonsts1)='OK') then
		json2:=logjson(json2,'Envio CRT Exitoso');

		json2:=put_json(json2,'INPUT','');
		json2:=put_json(json2,'__SECUENCIAOK__','120');
		if (get_json('TIPO_DTE',json2) in ('33','34','43')) then
			--si envio correctamente el CRT, grabo en las colas para buscar la fecha de recepcion real del sii
			sts1:=insert_cola_fecha_rec_sii_16103(get_json('CODIGO_TXEL',json2),get_json('RUT_EMISOR',json2),get_json('TIPO_DTE',json2),get_json('FOLIO',json2),get_json('RUT_RECEPTOR',json2),get_json('URI_REC',json2));
			json2:=logjson(json2,'Se graba Busqueda de fecha de recepcion '||sts1);
		end if;
	else
		json2:=logjson(json2,'Envio CRT Fallido ');
		json2:= put_json(json2,'RESPUESTA','Status: 400 NK');
	end if;

	return json2;
END;
$$ LANGUAGE plpgsql;

