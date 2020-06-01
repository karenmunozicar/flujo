--Publica documento
delete from isys_querys_tx where llave='112779';

insert into isys_querys_tx values ('112779',10,1,1,'select busco_dte_nar_112779 (''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('112779',12,1,2,'Servicio de SII 172.16.14.88',4013,100,101,0,0,14,14);

--Valida respuesta sii
insert into isys_querys_tx values ('112779',14,1,1,'select valida_respuesta_sii_112779(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('112779',15,19,1,'select envia_nar_112779 (''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('112779',20,1,1,'select actualizo_dte_112779 (''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('112779',30,19,1,'select revisa_respuesta_nar_112779(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

CREATE or replace FUNCTION revisa_respuesta_nar_112779(varchar) RETURNS varchar AS $$
DECLARE
        xml1   alias for $1;
        xml2  	varchar; 
BEGIN
	xml2:=xml1;
	xml2:=put_campo(xml2,'__SECUENCIAOK__','0');
        --xml2:=logapp(xml2,'1__EDTE_NAR_OK__='||get_campo('__EDTE_NAR_OK__',xml2));
	if (get_campo('__EDTE_NAR_OK__',xml2)='SI') then
		xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
        	xml2:=logapp(xml2,'RESPUESTA 200 OK');
	else
		xml2:=put_campo(xml2,'RESPUESTA','Status: 400 FALLA NAR');
	end if;
	xml2 := put_campo(xml2,'__SOCKET_RESPONSE__','RESPUESTA');
	xml2 := sp_procesa_respuesta_cola_motor88(xml2);
        return xml2;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION  busco_dte_nar_112779(json) RETURNS json AS $$
DECLARE
        json1   alias for $1;
        json2   json;
        total1  integer;
        uri1    varchar;
        hash1   varchar;
        ver_dcto_attrib varchar;
        html1   varchar;
        data1   varchar;
        largo1  integer;
        pos_inicial1    integer;
        pos_final1      integer;
        mail1   varchar;
        campo   record;
        campo1   record;
        json4   json;
        sts1    varchar;
        pos     integer;
        subject1        varchar;
        json_out1       json;
        comentario1     varchar;
        xml3    varchaR;
        cola1   varchar;
        nombre_tabla1   varchar;
        tx1     varchar;
        id1     varchar;
        aux1    varchar;
        comentario_traza1       varchar;
        json3   json;
        jsonsts1        json;
        i       integer;
        xml4    varchar;
        url_get1        varchar;
        data_lma        varchar;
        json_par1       json;
        rut1    varchar;
        rut_receptor1   varchar;
        folio1  varchar;
        data2   varchar;
        tipo_dte1       varchar;
        uri_dte1        varchar;
        fecha_emi1      varchar;
        sts2            varchar;
        estado_nar1     varchar;
        codigo_txel1    varchar;
	json_reclamo	json;
	flag_reclamo	varchar;
	input1	varchar;
	evento1	varchar;
	json_in	json;
	port varchar;
BEGIN
        json2:=json1;
	/*if (get_json('rutUsuario',json2)<>'7621836') then
        	json2:=put_json(json2,'__SECUENCIAOK__','40');
		return json2;
	end if;
	*/

	if (get_json('__FLAG_PUB_10K__',json2)='SI') then
        	json2:=put_json(json2,'__SECUENCIAOK__','0');
	else
        	json2:=put_json(json2,'__SECUENCIAOK__','30');
	end if;
	json2:=logjson(json2,'URI_NAR='||get_json('URI_IN',json2));
        --NAR a ser enviado
        data1:=get_json('INPUT',json2);
        rut1:=split_part(get_xml_hex1('RUTEmisor',data1),'-',1);
        folio1:=get_xml_hex1('Folio',data1);
        tipo_dte1:=get_xml_hex1('TipoDTE',data1);
        rut_receptor1:=split_part(get_xml_hex1('RUTRecep',data1),'-',1);
        fecha_emi1:=get_xml_hex1('FchEmis',data1);
        if (length(folio1)=0) then
                json2:=logjson(json2,'Si no hay Folio , no se acepta el NAR');
		json2:=put_json(json2,'__MENSAJE_10K__','Folio Invalido');
		json2:=put_json(json2,'__EDTE_NAR_OK__','SI');
                return json2;
        end if;
	--Si el RUT_EMISOR no es numerico borre el DTE
   	if (is_number(rut1) is false) then
                json2:=logjson(json2,'Se borra DTE, rut_emisor no numerico (NAR)');
		json2:=put_json(json2,'__MENSAJE_10K__','Rut Emisor no numerico');
		json2:=put_json(json2,'__EDTE_NAR_OK__','SI');
                return json2;
   	end if;

        --Valido si trae URI_DTE, si viene es porque viene des escritoriom, sino validamos el DTE para contestar
        if (get_json('URI_DTE',json2)='') then
                --Buscamos el dte recibido
                SELECT * into campo1 FROM dte_recibidos WHERE rut_emisor=rut1::integer and tipo_dte=tipo_dte1::integer and folio=folio1::integer;
                if not found then
                        json2:=logjson(json2,'Si no hay DTE recibido, no se acepta el NAR rut_emisor='||rut1||' tipo_dte='||tipo_dte1||' folio='||folio1);
			json2:=put_json(json2,'__MENSAJE_10K__','Dte no recibido aun.');
			json2:=put_json(json2,'__EDTE_NAR_OK__','SI');
                        --json2:=logjson(json2,'__EDTE_NAR_OK__='||get_json('__EDTE_NAR_OK__',json2));
                        return json2;
                end if;
                uri_dte1:=campo1.uri;
		json2:=put_json(json2,'URI_DTE',uri_dte1);
                codigo_txel1:=campo1.codigo_txel;
		json2:=put_json(json2,'CODIGO_TXEL_NAR',codigo_txel1);

		--Si es estado NAR ya esta marcado OK, no se envia NAR
		if (coalesce(campo1.estado_nar,'')<>'') then
			--No se puede realizar el NAR
                	json2:=logjson(json2,'DTE ya reclamado '||campo1.estado_nar||' '||campo1.uri);
			json2:=put_json(json2,'__EDTE_NAR_OK__','SI');
			json2:=put_json(json2,'__MENSAJE_10K__','Accion ya completada ('||campo1.mensaje_nar||')');
	                return json2;
		end if;
        end if;

	-- NBV 20170405 DAO
        if(codigo_txel1 is null) then
                codigo_txel1:=get_json('CODIGO_TXEL_NAR',json2);
        end if;

        uri1:=get_json('URI_IN',json2);
        if (length(uri1)=0) then
                json2:=logjson(json2,'Uri NAR vacia');
		json2:=put_json(json2,'__EDTE_NAR_OK__','SI');
		json2:=put_json(json2,'__MENSAJE_10K__','Uri invalida');
                return json2;
        end if;

        select * into campo from contribuyentes where rut_emisor=rut1::integer;
        if not found then
                json2:=logjson(json2,'Sin mail de intercambio RUT_EMISOR');
                --return json2;
        end if;
	json2:=put_json(json2,'CORREO_EMISOR',campo.email);
	json2:=put_json(json2,'NOMBRE_EMISOR',campo.nombre);
        select * into campo1 from contribuyentes where rut_emisor=rut_receptor1::integer;
        if not found then
                json2:=logjson(json2,'Sin mail de intercambio RUT_RECEPTOR');
                --return json2;
        end if;
	json2:=put_json(json2,'CORREO_RECEPTOR',campo1.email);
	json2:=put_json(json2,'NOMBRE_RECEPTOR',campo1.nombre);

        json2:=logjson(json2,'Mail NAR='||campo.email);
        --Validamos el correo
        if (valida_email(campo.email) is false) then
                json2:=logjson(json2,'Mail Invalido '||campo.email);
                --return json2;
        end if;

	flag_reclamo:='NO';
        sts2:=get_xml_hex1('EstadoDTE',data1);
	if (strpos(uri1,'cencosud')>0 or rut_receptor1 in ('78703410','87845500','90635000')) then
		input1:=decode(data1,'hex');
	        flag_reclamo:=split_part(split_part(split_part(input1,'<NombreDA>ReclamarDTE</NombreDA>',2),'<ValorDA>',2),'</ValorDA>',1);
	else
		flag_reclamo:='SI';
	end if;
	json2:=put_json(json2,'FLAG_RECLAMO',flag_reclamo);

	--DAO-2017-11-20 Si no es un DTE reclamable en el SII, solo hace el NAR
        if(flag_reclamo='NO' or flag_reclamo='' or tipo_dte1::varchar not in ('33','34','43')) then
        	json2:=put_json(json2,'__SECUENCIAOK__','15');
		json2:=logjson(json2,'No va al SII el NAR');
		return json2;
	end if;
	
	json2:=logjson(json2,'Vamos al SII');
	if (sts2 in ('0','1')) then
       		evento1:='ACD';
        else
                evento1:='RCD';
        end if;
	json2:=put_json(json2,'EVENTO_RECLAMO',evento1);

	json_in:='{"rutEmisor":"'||rut1||'","dvEmisor":"'||modulo11(rut1)||'","tipoDoc":"'||tipo_dte1||'","folio":"'||folio1||'","accionDoc":"'||evento1||'"}';

        json2:=logjson(json2,'Vamos a Reclamar/Aceptar al SII '||json_in::varchar);
        json2:=put_json(json2,'__SECUENCIAOK__','12');
        --json2:=get_parametros_motor_json(json2,'SERVICIO_SII_JSON');
	port:=get_ipport_sii();
	--Si no hay puertos libres ...
	if (port='') then
		--Si no hay puertos libres...
		json2:=logjson(json2,'No hay puertos libres ');
		--json2:=put_json(json2,'__SECUENCIAOK__','900');
		if (get_json('__FLAG_PUB_10K__',json2)='SI') then
			json2:=put_json(json2,'__SECUENCIAOK__','0');
		else
			json2:=put_json(json2,'__SECUENCIAOK__','30');
		end if;
		json2:=put_json(json2,'RESPUESTA','Status: 400 NK');
		return json2;
	end if;
	json2:=put_json(json2,'__IP_PORT_CLIENTE__',split_part(port,':',2));
	json2:=put_json(json2,'__IP_CONEXION_CLIENTE__',split_part(port,':',1));
	json2:=put_json(json2,'IPPORT_SII',port);

	--json2:=put_json(json2,'IP_PORT_CLIENTE',port);
	--json2:=put_json(json2,'__IP_PORT_CLIENTE__',port);
        json2:=put_json(json2,'INPUT','POST /reclamo_aceptacion HTTP/1.1'||chr(10)||'User-Agent: curl/7.26.0'||chr(10)||'Host: '||get_json('__IP_CONEXION_CLIENTE__',json2)||':'||get_json('__IP_PORT_CLIENTE__',json2)||chr(10)||'Accept: '||chr(42)||'/'||chr(42)||chr(10)||'Content-Type: application/json; charset=ISO-8859-1'||chr(10)||'Content-Length: '||length(json_in::varchar)::varchar||chr(10)||chr(10)||json_in::varchar);
        return json2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION valida_respuesta_sii_112779(json) RETURNS json AS $$
DECLARE
	json1	alias for $1;
	json2	json;
	resp1	varchar;
	json_out	json;	
	evento1	varchar;
	v_tipo_dte	varchar;
	v_folio		varchar;
	v_rutEmisor	varchar;
	campoE	record;
	campo1	record;
	ws_codResp	varchar;
	ws_descResp	varchar;
	xml3	varchar;
	v_cod_txel	bigint;
BEGIN
	json2:=json1;
	if (get_json('__FLAG_PUB_10K__',json2)='SI') then
        	json2:=put_json(json2,'__SECUENCIAOK__','0');
	else
        	json2:=put_json(json2,'__SECUENCIAOK__','30');
	end if;
	
	resp1:=get_json('RESPUESTA',json2);
        if(strpos(resp1,'HTTP/1.0 200')=0) then
		perform libera_ipport_sii(get_json('IPPORT_SII',json2),'FALLA');
		json2:=put_json(json2,'__EDTE_NAR_OK__','NK');
		json2:=put_json(json2,'__EDTE_ARM_OK__','NK');
		json2:=put_json(json2,'__MENSAJE_10K__','Falla Comunicación SII');
                return json2;
        end if;

	perform libera_ipport_sii(get_json('IPPORT_SII',json2),'OK');
        json_out:=split_part(resp1,chr(10)||chr(10),2)::json;

        ws_codResp:=get_json('codResp',json_out::json);
        ws_descResp:=replace_unicode(get_json('descResp',json_out::json));

	evento1:=get_json('EVENTO_RECLAMO',json2);
	v_cod_txel:=get_json('CODIGO_TXEL_NAR',json2);
	
	v_tipo_dte:=get_json('TIPO_DTE',json2);
	v_folio:=get_json('FOLIO',json2);
	v_rutEmisor:=get_json('RUT_EMISOR',json2);

	xml3:='';
	xml3:=put_campo(xml3,'__FLUJO_ACTUAL__',get_json('__FLUJO_ACTUAL__',json2));
	xml3:=put_campo(xml3,'FECHA_EMISION',get_json('FECHA_EMISION',json2));
	xml3:=put_campo(xml3,'RUT_EMISOR',v_rutEmisor);
	xml3:=put_campo(xml3,'RUT_OWNER',get_json('RUT_RECEPTOR',json2));
	xml3:=put_campo(xml3,'FECHA_EVENTO',now()::varchar);
	xml3:=put_campo(xml3,'RUT_RECEPTOR',get_json('RUT_RECEPTOR',json2));
	xml3:=put_campo(xml3,'FOLIO',v_folio);
	xml3:=put_campo(xml3,'TIPO_DTE',v_tipo_dte);
	xml3:=put_campo(xml3,'CANAL','RECIBIDOS');
	xml3:=put_campo(xml3,'URI_IN',get_json('URI_DTE',json2));
	xml3:=put_campo(xml3,'COMENTARIO_TRAZA','Glosa: '||ws_descResp||'('||ws_codResp||')');
	
	json2:=logjson(json2,'Respuesta SII COD='||ws_codResp||' DESC='||ws_descResp);
	if (ws_codResp in ('5','6','8','10','11')) then 
		xml3:=graba_bitacora(xml3,evento1||'_FALLA');
		json2:=logjson(json2,get_campo('_LOG_',xml3));
		-- TRAIGO ESTADO DEL EVENTO
		select * from estado_dte where codigo='R'||evento1 into campoE;
		if not found then
			select * from estado_dte where codigo=evento1 into campoE;
		end if;
		if(get_json('FLAG_PREA',json2)='SI') then
                        v_cod_txel:=get_json('COD_TXEL',json2);
                	update dte_pendientes_recibidos set estado_nar=campoE.descripcion||'_ERROR',fecha_nar=now(),mensaje_nar='Glosa: '||ws_descResp||' ('||ws_codResp||')' where id=v_cod_txel::bigint and coalesce(estado_nar,'')<>campoE.descripcion; 
		else
                	update dte_recibidos set estado_nar=campoE.descripcion||'_ERROR',fecha_nar=now(),mensaje_nar='Glosa: '||ws_descResp||' ('||ws_codResp||')' where codigo_txel=v_cod_txel::bigint and coalesce(estado_nar,'')<>campoE.descripcion; 
		end if;
		json2:=put_json(json2,'__MENSAJE_10K__','Acción Fallida '||ws_descResp);
	
        elsif(ws_codResp in ('0','7')) then
		xml3:=graba_bitacora(xml3,evento1);
		json2:=logjson(json2,get_campo('_LOG_',xml3));
		-- TRAIGO ESTADO DEL EVENTO
		select * from estado_dte where codigo='R'||evento1 into campoE;
		if not found then
			select * from estado_dte where codigo=evento1 into campoE;
		end if;
		if(get_json('FLAG_PREA',json2)='SI') then
                        v_cod_txel:=get_json('COD_TXEL',json2);
                	update dte_pendientes_recibidos set estado_nar=campoE.descripcion,fecha_nar=now(),mensaje_nar='Glosa: '||ws_descResp||' ('||ws_codResp||')' where id=v_cod_txel::bigint and coalesce(estado_nar,'')<>campoE.descripcion;
		else
                	update dte_recibidos set estado_nar=campoE.descripcion,fecha_nar=now(),mensaje_nar='Glosa: '||ws_descResp||' ('||ws_codResp||')' where codigo_txel=v_cod_txel::bigint and coalesce(estado_nar,'')<>campoE.descripcion;
		end if;
                -- Valido si existe el documento en emitidos !!!!!!!!!!!!!
                select * from dte_emitidos where rut_emisor=v_rutEmisor::integer and tipo_dte=v_tipo_dte::integer and folio=v_folio::bigint into campo1;
                if found then
                	select * from estado_dte where codigo=evento1 into campoE;
                        if not found then
                        	select * from estado_dte where codigo='R'||evento1 into campoE;
                        end if;
                        update dte_emitidos set estado_nar=campoE.descripcion,fecha_nar=now(),mensaje_nar='Glosa: '||ws_descResp||' ('||ws_codResp||')' where rut_emisor=v_rutEmisor::integer and tipo_dte=v_tipo_dte::integer and folio=v_folio::bigint and coalesce(estado_nar,'')<>campoE.descripcion; 
			xml3:=put_campo(xml1,'RUT_OWNER',campo.rut_emisor::varchar);
			xml3:=put_campo(xml1,'CANAL','EMITIDOS');
			xml3:=put_campo(xml1,'URI_IN',campo1.uri::varchar);
			xml3:=graba_bitacora(xml3,evento1);
			json2:=logjson(json2,get_campo('_LOG_',xml3));
		else
			json2:=logjson(json2,'No existe el emitido para este recibidos :P');
		end if;
		json2:=put_json(json2,'__MENSAJE_10K__','Acción Realizada OK');
		json2:=put_json(json2,'__SECUENCIAOK__','15');	
	else
		json2:=put_json(json2,'__EDTE_NAR_OK__','NK');
		json2:=put_json(json2,'__EDTE_ARM_OK__','NK');
		json2:=put_json(json2,'__MENSAJE_10K__','SII responde error '||ws_descResp);
                return json2;
	end if;
	return json2;
END;$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION  envia_nar_112779(json) RETURNS json AS $$
DECLARE
        json1   alias for $1;
        json2   json;
        total1  integer;
        uri1    varchar;
        hash1   varchar;
        ver_dcto_attrib varchar;
        html1   varchar;
        data1   varchar;
        largo1  integer;
        pos_inicial1    integer;
        pos_final1      integer;
        mail1   varchar;
        campo   record;
        campo1   record;
        json4   json;
        sts1    varchar;
        pos     integer;
        subject1        varchar;
        json_out1       json;
        comentario1     varchar;
        xml3    varchaR;
        cola1   varchar;
        nombre_tabla1   varchar;
        tx1     varchar;
        id1     varchar;
        aux1    varchar;
        comentario_traza1       varchar;
        json3   json;
        jsonsts1        json;
        i       integer;
        xml4    varchar;
        url_get1        varchar;
        data_lma        varchar;
        json_par1       json;
        rut1    varchar;
        rut_receptor1   varchar;
        folio1  varchar;
        data2   varchar;
        tipo_dte1       varchar;
        uri_dte1        varchar;
        fecha_emi1      varchar;
        sts2            varchar;
        estado_nar1     varchar;
        codigo_txel1    varchar;
	mail_emisor1	varchar;
	mail_receptor1	varchar;
	nombre_emisor1	varchar;
	nombre_receptor1	varchar;
	tmp1	varchar;
	tmp2	varchar;
BEGIN
        json2:=json1;
	if (get_json('__FLAG_PUB_10K__',json2)='SI') then
        	json2:=put_json(json2,'__SECUENCIAOK__','0');
	else
        	json2:=put_json(json2,'__SECUENCIAOK__','30');
	end if;

        --NAR a ser enviado
        data1:=get_json('INPUT',json2);
        rut1:=split_part(get_xml_hex1('RUTEmisor',data1),'-',1);
        folio1:=get_xml_hex1('Folio',data1);
        tipo_dte1:=get_xml_hex1('TipoDTE',data1);
        rut_receptor1:=split_part(get_xml_hex1('RUTRecep',data1),'-',1);
        fecha_emi1:=get_xml_hex1('FchEmis',data1);
	uri_dte1:=get_json('URI_DTE',json2);
	codigo_txel1:=get_json('CODIGO_TXEL_NAR',json2);
	uri1:=get_json('URI_IN',json2);	
	mail_emisor1:=get_json('CORREO_EMISOR',json2);
	mail_receptor1:=get_json('CORREO_RECEPTOR',json2);
	nombre_emisor1:=get_json('NOMBRE_EMISOR',json2);
	nombre_receptor1:=get_json('NOMBRE_RECEPTOR',json2);

        json2:=logjson(json2,'Casilla NAR --> '||mail_emisor1||' URI='||uri1||' URI_DTE='||uri_dte1);
        sts2:=get_xml_hex1('EstadoDTE',data1);
        json4:='{}';
        if (sts2 in ('0','1')) then
                hash1 := encripta_hash_evento_VDC('uri='||uri_dte1||'&owner='||rut_receptor1||'&rutEmisor='||rut1||'&tipoDTE='||tipo_dte1||'&folio='||folio1||'&mail='||trim(mail_emisor1)||'&type=LNRE'||'&rutRecep='||rut_receptor1||'&fchEmis='||fecha_emi1||'&relatedUrl=&comment=Mail Leído por '||trim(mail_emisor1)||'&');
                if (sts2='0') then
                        json4:=put_json(json4,'ESTADO_DTE','ACEPTADO OK');
                else
                        json4:=put_json(json4,'ESTADO_DTE','ACEPTADA CON DISCREPANCIA');
                end if;
        else
                hash1 := encripta_hash_evento_VDC('uri='||uri_dte1||'&owner='||rut_receptor1||'&rutEmisor='||rut1||'&tipoDTE='||tipo_dte1||'&folio='||folio1||'&mail='||trim(mail_emisor1)||'&type=LRRE'||'&rutRecep='||rut_receptor1||'&fchEmis='||fecha_emi1||'&relatedUrl=&comment=Mail Leído por '||trim(mail_emisor1)||'&');
                json4:=put_json(json4,'ESTADO_DTE','RECHAZADO');
        end if;

        --Enviamos el NAR
        json4:=put_json(json4,'GLOSA_DTE',get_xml_hex1('EstadoDTEGlosa',data1));
        json4:=put_json(json4,'RAZON_EMISOR',nombre_emisor1);
        json4:=put_json(json4,'RAZON_RECEPTOR',nombre_receptor1);
        --Case
        json_par1:=get_parametros_motor_json('{}','SERVIDOR_CORREO');

        json4:=put_json(json4,'uri',uri1);
        json4:=put_json(json4,'FECHA_EMISION',fecha_emi1);
        json4:=put_json(json4,'TIPO_DTE',tipo_dte1);
        json4:=put_json(json4,'FOLIO',folio1);
        json4:=put_json(json4,'flag_data_xml','NO');
        json4:=put_json(json4,'RUT_EMISOR_DV',rut1||'-'||modulo11(rut1));
        json4:=put_json(json4,'RUT_RECEPTOR_DV',rut_receptor1||'-'||modulo11(rut_receptor1));
        json4:=put_json(json4,'IMG_LECTURA','<img style="display: none;" src="'||get_json('__VALOR_PARAM__',json_par1)||'?hash='||hash1||'&"/>');
        --json4:=put_json(json4,'IMG_LECTURA','<img style="display: none;" src="http://servicios.acepta.com/traza?hash='||hash1||'&"/>');
        json4:=put_json(json4,'TITULO','Producción Intercambio Revisión Comercial de DTE');
        json4:=put_json(json4,'MONTO_TOTAL','$ '||get_xml_hex1('MntTotal',data1));

        html1:=pg_read_file('./patron_dte_10k/patron_nar.html');
        html1:=remplaza_tags_json_c(json4,html1);
        json4:=put_json(json4,'content_html',encode(html1::bytea,'hex'));
        --Solo se envia el RespuestaDTE
	tmp1:=encode('<RespuestaDTE','hex');
	tmp2:=encode('</RespuestaDTE>','hex');
        --data2:=encode(('<?xml version="1.0" encoding="ISO-8859-1"?>'||chr(10))::bytea,'hex')||get_xml_hex(encode('Content','hex'),data1);
        data2:=encode(('<?xml version="1.0" encoding="ISO-8859-1"?>'||chr(10))::bytea,'hex')||tmp1||split_part(split_part(data1,tmp1,2),tmp2,1)||tmp2;
        json4:=put_json(json4,'INPUT_CUSTODIUM',data2);
        --json4:=put_json(json4,'adjunta_xml','SI');
        json4:=put_json(json4,'adjunta_attach','SI');
        json4:=put_json(json4,'nombre_xml','Notificacion_Aprobacion_o_Rechazo_'||folio1::varchar);
        json4:=put_json(json4,'RUT_RECEPTOR',rut_receptor1);
        json4:=put_json(json4,'subject_hex',encode('Revisión Comercial de DTEs -','hex'));
        json4:=put_json(json4,'from',mail_receptor1);
        json4:=put_json(json4,'to',mail_emisor1);
        --json4:=put_json(json4,'bcc','fernando.arancibia@acepta.com');
        json4:=put_json(json4,'tipo_envio','HTML');

        --json4:=put_json(json4,'return_path','confirmacion_envio@custodium.com');
        --json4:=put_json(json4,'ip_envio','172.16.10.185');
        json4:=put_json(json4,'return_path',get_json('PARAMETRO_RUTA',json_par1));
        json4:=put_json(json4,'ip_envio',get_json('__IP_CONEXION_CLIENTE__',json_par1));
        comentario_traza1:='Recibe: '||mail_emisor1||chr(10)||get_xml_hex1('EstadoDTEGlosa',data1);
	json4:=put_json(json4,'url_traza',get_json('__VALOR_PARAM__',json_par1));

        --json4:=put_json(json4,'url_traza','http://servicios.acepta.com/traza');
        json4:=put_json(json4,'uri_dte',uri_dte1);
        json4:=put_json(json4,'CANAL','RECIBIDOS');

        if (get_xml_hex1('EstadoDTE',data1) in ('0','1')) then
                json4:=put_json(json4,'evento_ema','<trace source="ENVIA_NAR" version="1.1"><node name="NRE" stamp="'||to_char(now(),'YYYY-MM-DD')||'T'||to_char(now(),'HH24:MI:SS')||'" owner="' ||get_json('RUT_RECEPTOR_DV',json4)||'"><keys><key name="rutEmisor" value="'||get_json('RUT_EMISOR_DV',json4)||'"/><key name="tipoDTE" value="'||tipo_dte1||'"/><key name="folio" value="'||folio1||'"/><key name="fchEmis" value="'||fecha_emi1||'"/></keys><attrs><attr key="code">'||tipo_dte1||'</attr><attr key="url">'||uri_dte1||'</attr><attr key="relatedUrl">'||uri1||'</attr><attr key="orig">'||get_json('RUT_EMISOR_DV',json4)||'</attr><attr key="dest">'||get_json('RUT_RECEPTOR_DV',json4)||'</attr><attr key="tag">'||folio1||'</attr><attr key="data"></attr><attr key="comment">'||comentario_traza1||'</attr></attrs></node></trace>');
                json4:=put_json(json4,'evento_confirmacion','ENRE');
		--Se envian los 2 eventos para guardar en la conf de la traza
                json4:=put_json(json4,'eok','ONRE');
                json4:=put_json(json4,'enk','FNRE');
                json4:=put_json(json4,'evento_confirmacion','');
                estado_nar1:='NAR_APROBADO';
        else
                json4:=put_json(json4,'evento_ema','<trace source="ENVIA_NAR" version="1.1"><node name="RRE" stamp="'||to_char(now(),'YYYY-MM-DD')||'T'||to_char(now(),'HH24:MI:SS')||'" owner="' ||get_json('RUT_RECEPTOR_DV',json4)||'"><keys><key name="rutEmisor" value="'||get_json('RUT_EMISOR_DV',json4)||'"/><key name="tipoDTE" value="'||tipo_dte1||'"/><key name="folio" value="'||folio1||'"/><key name="fchEmis" value="'||fecha_emi1||'"/></keys><attrs><attr key="code">'||tipo_dte1||'</attr><attr key="url">'||uri_dte1||'</attr><attr key="relatedUrl">'||uri1||'</attr><attr key="orig">'||get_json('RUT_EMISOR_DV',json4)||'</attr><attr key="dest">'||get_json('RUT_RECEPTOR_DV',json4)||'</attr><attr key="tag">'||folio1||'</attr><attr key="data"></attr><attr key="comment">'||comentario_traza1||'</attr></attrs></node></trace>');
                json4:=put_json(json4,'eok','ORRE');
                json4:=put_json(json4,'enk','FRRE');
                json4:=put_json(json4,'evento_confirmacion','');
                --json4:=put_json(json4,'evento_confirmacion','ERRE');
                estado_nar1:='NAR_RECHAZADO';
        end if;
	id1:='ACP'||encripta_hash_evento_VDC(rut1||'##'||tipo_dte1||'##'||folio1||'##'||fecha_emi1||'##'||uri_dte1||'####RECIBIDOS##'||rut_receptor1||'##'||get_json('eok',json4)||'##'||get_json('enk',json4));
       	--Generamos un id para confirmar la lectura de correo
        --id1:=md5(now()::varchar)||nextval('id_confirmacion_mail');
        json4:=put_json(json4,'msg_id','<'||id1||'@motor2.acepta.com>');

        jsonsts1:=send_mail_python2(json4::varchar);
        if (get_json('status',jsonsts1)='OK') then
                json2:=put_json(json2,'__EDTE_NAR_OK__','SI');
                --perform logfile('send_mail_python1 html2='||get_json('html2',jsonsts1));
                --perform logfile('send_mail_python1 html='||get_json('html',jsonsts1));
                --Si envie correctamente, inserto en confirmacion_mail para generar el evento de envio
                json4:=put_json(json4,'INPUT_CUSTODIUM','');
                json4:=put_json(json4,'evento_ema','');
                json4:=put_json(json4,'content_html','');
                json4:=put_json(json4,'IMG_LECTURA','');

		json2:=put_json(json2,'ESTADO_NAR',estado_nar1);
		json2:=put_json(json2,'MENSAJE_NAR',comentario_traza1);
		json2:=put_json(json2,'URI_NAR',uri1);
                --insert into confirmacion_mail (id,json_data) values (id1,json4);
		if (strpos(uri1,'cencosud')>0 or rut_receptor1 in ('78703410','87845500','90635000')) then
        		json2:=put_json(json2,'__SECUENCIAOK__','20');
		else
			json2:=put_json(json2,'__EDTE_OK__','SI');
			json2:=put_json(json2,'__EDTE_NAR_OK__','SI');
			if (get_json('__FLAG_PUB_10K__',json2)='SI') then
				json2:=put_json(json2,'__SECUENCIAOK__','0');
			else
				json2:=put_json(json2,'__SECUENCIAOK__','30');
			end if;
		end if;
                --update dte_recibidos set uri_nar=uri1,mensaje_nar=comentario_traza1,estado_nar=estado_nar1,fecha_nar=now() where codigo_txel=codigo_txel1::bigint;
        else
                json2:=logjson(json2,'Falla Mail');
                json2:=put_json(json2,'__EDTE_OK__','NO');
                json2:=put_json(json2,'__EDTE_NAR_OK__','NO');
        end if;
    return json2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION  actualizo_dte_112779(json) RETURNS json AS $$
DECLARE
        json1   alias for $1;
        json2   json;
        id1     varchar;
        codigo_txel1    varchar;
BEGIN
        json2:=json1;
	if (get_json('__FLAG_PUB_10K__',json2)='SI') then
        	json2:=put_json(json2,'__SECUENCIAOK__','0');
	else
        	json2:=put_json(json2,'__SECUENCIAOK__','30');
	end if;
	codigo_txel1:=get_json('CODIGO_TXEL_NAR',json2);
        update dte_recibidos set uri_nar=get_json('URI_NAR',json2),mensaje_nar=get_json('MENSAJE_NAR',json2),estado_nar=get_json('ESTADO_NAR',json2),fecha_nar=now() where codigo_txel=codigo_txel1::bigint and coalesce(estado_nar,'')='';
        json2:=put_json(json2,'__EDTE_OK__','SI');
        json2:=put_json(json2,'__EDTE_NAR_OK__','SI');
    return json2;
END;
$$ LANGUAGE plpgsql;

