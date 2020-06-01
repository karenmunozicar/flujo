--Consulta Estado Reclamo SII
delete from isys_querys_tx where llave='16101';

insert into isys_querys_tx values ('16101',10,1,1,'select check_flag_sii_16101(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('16101',20,1,2,'Servicio de SII 172.16.14.88',4013,100,101,0,0,30,30);
insert into isys_querys_tx values ('16101',30,1,1,'select proceso_resp_reclamo_sii_16101(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,1000,1000);

--Flujo de los CA4ARM
insert into isys_querys_tx values ('16101',150,1,8,'Flujo ARM',112718,0,0,1,1,0,0);
--Flujo de los CA4RESP
insert into isys_querys_tx values ('16101',160,1,8,'Flujo RESP',112779,0,0,1,1,0,0);

--Colas
insert into isys_querys_tx values ('16101',1000,19,1,'select sp_procesa_respuesta_cola_motor88_json(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,0,0);

--Responde a Pantalla
insert into isys_querys_tx values ('16101',2000,1,1,'select responde_pantalla_16101(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,0,0);

CREATE or replace FUNCTION responde_pantalla_16101(json) RETURNS json AS $$
DECLARE
	json1	alias for $1;
	json2	json;
BEGIN
        json2:=json1;
	return response_requests_6000('2',get_json('__MENSAJE_10K__',json2),'',json2);
END;
$$ LANGUAGE plpgsql;



CREATE or replace FUNCTION check_flag_sii_16101(varchar) RETURNS varchar AS $$
DECLARE
        xml1   alias for $1;
        xml2   varchar;

        json_in json;

	flag_reclamo	varchar;
	sts2		varchar;
	data1		varchar;
	input1		varchar;
	evento1		varchar;
	categoria	varchar;

	v_rutEmisor	varchar;
	v_folio		varchar;
	v_tipo_dte	varchar;
	v_cod_txel	bigint;
BEGIN
	xml2:=xml1;

	xml2:=logapp(xml2,'Entro a check_flag_sii_16101');

	data1:=get_campo('INPUT',xml2);
	input1:=decode(data1,'hex');
	
	v_rutEmisor:=get_campo('RUT_EMISOR',xml2);
	v_folio:=get_campo('FOLIO',xml2);
	--v_tipo_dte:=get_campo('TIPO_DTE',xml2);
	v_tipo_dte:=get_xml('TipoDoc',input1);
	xml2:=put_campo(xml2,'TIPO_DOC',v_tipo_dte);

	if(get_campo('__FLAG_PUB_10K__',xml2)='SI') then
		xml2 := put_campo(xml2,'__SECUENCIAOK__','2000');
	else
		xml2 := put_campo(xml2,'__SECUENCIAOK__','1000');
	end if;
	--Verificamos si es un NAR o ARM
	if(get_campo('SCRIPT_NAME',xml2)='/ca4/ca4arm') then
		categoria='ARM';
    	elsif (get_campo('SCRIPT_NAME',xml2)='/ca4/ca4resp') then
		categoria='NAR';
	else
		xml2:=put_campo(xml2,'RESPUESTA','Status: 400 NK');
		xml2:=logapp(xml2,'No reconozco ni un NAR, ni ARM');
		xml2:=put_campo(xml2,'__MENSAJE_10K__','Falla realizar acción');
		return xml2;
	end if;
	xml2:=logapp(xml2,'CATEGORIA '||categoria);
	if(is_number(v_rutEmisor) is false or is_number(v_folio) is false or is_number(v_tipo_dte) is false) then
		xml2:=put_campo(xml2,'RESPUESTA','Status: 444 NK');
		xml2:=logapp(xml2,'Datos no numéricos rut_emisor='||v_rutEmisor||' tipo_dte='||v_tipo_dte||' folio='||v_folio);
		xml2:=put_campo(xml2,'__MENSAJE_10K__','Falla realizar acción.');
		return xml2;
	end if;
	select codigo_txel into v_cod_txel from dte_recibidos where rut_emisor=v_rutEmisor::bigint and tipo_dte=v_tipo_dte::integer and folio=v_folio::bigint;
	if not found then
		xml2:=put_campo(xml2,'RESPUESTA','Status: 444 NK');
		xml2:=logapp(xml2,'No se encuentra DTE en dte_recibidos');
		xml2:=put_campo(xml2,'__MENSAJE_10K__','Falla realizar acción, no se encuentra documento');
		return xml2;
	end if;
	
	--Chequeamos si viene la doble detonacion...
	flag_reclamo:=split_part(split_part(split_part(input1,'<NombreDA>ReclamarDTE</NombreDA>',2),'<ValorDA>',2),'</ValorDA>',1);
	if(flag_reclamo='SI') then
		xml2 := put_campo(xml2,'_CODIGO_TXEL_REC_',v_cod_txel::varchar);
		xml2:=logapp(xml2,'XML '||categoria||' VA_AL_SII');
		if(categoria='NAR') then
			sts2:=get_xml_hex1('EstadoDTE',data1);	
			if (sts2 in ('0','1')) then
				evento1:='ACD';
			else
				evento1:='RCD';
			end if;
		else
			evento1:='ERM';
		end if;
	else
		--Nos vamos al Flujo Normal... que no va al SII
		if(categoria='NAR') then
			xml2 := logapp(xml2,'NAR no va al SII. Seguimos el Flujo NAR');
			xml2 := put_campo(xml2,'__SECUENCIAOK__','160');
			return xml2;	
		else
			xml2 := logapp(xml2,'ARM no va al SII. Seguimos el Flujo ARM');
			xml2 := put_campo(xml2,'__SECUENCIAOK__','150');
			return xml2;	
		end if;
	end if;

	xml2 := put_campo(xml2,'__ACCION__',evento1);
	xml2 := put_campo(xml2,'__CATEGORIA__',categoria);

	--json_in:='{"rutEmisor":"'||get_campo('RUT_EMISOR',xml2)||'","dvEmisor":"'||modulo11(get_campo('RUT_EMISOR',xml2))||'","tipoDoc":"'||get_campo('TIPO_DTE',xml2)||'","folio":"'||get_campo('FOLIO',xml2)||'","accionDoc":"'||evento1||'"}';
	json_in:='{"rutEmisor":"'||get_campo('RUT_EMISOR',xml2)||'","dvEmisor":"'||modulo11(get_campo('RUT_EMISOR',xml2))||'","tipoDoc":"'||v_tipo_dte||'","folio":"'||get_campo('FOLIO',xml2)||'","accionDoc":"'||evento1||'"}';

	xml2 := logapp(xml2,'Vamos a Reclamar/Aceptar al SII '||json_in::varchar);
	xml2 := put_campo(xml2,'__SECUENCIAOK__','20');
        xml2:=get_parametros_motor(xml2,'SERVICIO_SII_JSON');
        xml2:=put_campo(xml2,'INPUT','POST /reclamo_aceptacion HTTP/1.1'||chr(10)||'User-Agent: curl/7.26.0'||chr(10)||'Host: '||get_campo('__IP_CONEXION_CLIENTE__',xml2)||':'||get_campo('__IP_PORT_CLIENTE__',xml2)||chr(10)||'Accept: '||chr(42)||'/'||chr(42)||chr(10)||'Content-Type: application/json; charset=ISO-8859-1'||chr(10)||'Content-Length: '||length(json_in::varchar)::varchar||chr(10)||chr(10)||json_in::varchar);
	return xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION proceso_resp_reclamo_sii_16101(varchar) RETURNS varchar AS $$
DECLARE
        xml1   alias for $1;
        xml2 varchar;
        xml3 varchar;

        json_in json;

        v_rutEmisor     varchar;
        v_rutReceptor     varchar;
        v_tipo_dte      varchar;
        v_folio         varchar;
	v_uri		varchar;

        v_canal         varchar;
        v_cod_txel      bigint;
        v_flag_escritorio       varchar;
        json_out        json;
        json_script1    json;
        ws_codResp      varchar;
        ws_descResp     varchar ;
        ws_json         json;

        i integer;
        aux     varchar;
        aux2    json;

        ws_codEvento    varchar;
        ws_descEvento   varchar;
        te_estado       varchar;
        te_fecha        varchar;
	resp1	varchar;
	v_accion	varchar;
	campoE		record;
	campo1		record;
BEGIN
	xml2:=xml1;
	resp1:=get_campo('RESPUESTA',xml2);
	perform logfile('DAO_SII '||resp1);
	if(get_campo('__FLAG_PUB_10K__',xml2)='SI') then
		xml2 := put_campo(xml2,'__SECUENCIAOK__','2000');
	else
		xml2 := put_campo(xml2,'__SECUENCIAOK__','1000');
	end if;
	xml2:=logapp(xml2,'ENTRO a proceso_resp_reclamo_sii_16101');
	if(strpos(resp1,'HTTP/1.0 200')=0) then
                xml2:=put_campo(xml2,'RESPUESTA','Status: 400 NK');
		xml2:=put_campo(xml2,'__MENSAJE_10K__','Falla comunicación con el SII');
		return xml2;
	end if;
	
	v_accion:=get_campo('__ACCION__',xml2);
	v_rutEmisor:=get_campo('RUT_EMISOR',xml2);
	v_rutReceptor:=get_campo('RUT_RECEPTOR',xml2);
	v_folio:=get_campo('FOLIO',xml2);
	--v_tipo_dte:=get_campo('TIPO_DTE',xml2);
	v_tipo_dte:=get_campo('TIPO_DOC',xml2);
	v_uri:=get_campo('URI',xml2);

	v_cod_txel=get_campo('_CODIGO_TXEL_REC_',xml2);

	json_out:=split_part(resp1,chr(10)||chr(10),2)::json;
	ws_codResp:=get_json('codResp',json_out::json);
	ws_descResp:=replace_unicode(get_json('descResp',json_out::json));

	xml2:=logapp(xml2,'*************** RESPUESTA WS *****************');
	xml2:=logapp(xml2,'*************** COD_RESPUESTA_WS => '||ws_codResp::varchar);
	xml2:=logapp(xml2,'*************** DESC_RESPUESTA_WS => '||ws_descResp::varchar);

	--Si se realizo OK la accion...
	if(ws_codResp='0') then
		-- GUARDO EVENTO EN TRAZA
		xml3:='';
		xml3:=put_campo(xml3,'FECHA_EMISION',get_campo('FECHA_EMISION',xml2));
		xml3:=put_campo(xml3,'RUT_EMISOR',v_rutEmisor::varchar);
		xml3:=put_campo(xml3,'RUT_OWNER',v_rutEmisor::varchar);
		xml3:=put_campo(xml3,'FECHA_EVENTO',now()::varchar);
		xml3:=put_campo(xml3,'RUT_RECEPTOR',v_rutReceptor);
		xml3:=put_campo(xml3,'FOLIO',v_folio);
		xml3:=put_campo(xml3,'TIPO_DTE',v_tipo_dte);
		xml3:=put_campo(xml3,'CANAL','RECIBIDOS');
		xml3:=put_campo(xml3,'URI_IN',v_uri);
		xml3:=put_campo(xml3,'COMENTARIO_TRAZA','Glosa: '||ws_descResp::varchar);
		xml3:=graba_bitacora(xml3,v_accion);
		xml2:=logapp(xml2,get_campo('_LOG_',xml3));

		-- TRAIGO ESTADO DEL EVENTO
		select * from estado_dte where codigo='R'||v_accion into campoE;
		if not found then
			select * from estado_dte where codigo=v_accion into campoE;
		end if;
		-- UPDATE DTE_RECIBIDOS
		if(v_accion in ('ACD','RCD')) then
			update dte_recibidos set estado_nar=campoE.descripcion,fecha_nar=now(),mensaje_nar='Glosa: '||ws_descResp||' ('||ws_codResp||')' where codigo_txel=v_cod_txel::bigint;
		else
			update dte_recibidos set estado_reclamo=campoE.descripcion,fecha_reclamo=now(),mensaje_reclamo='Glosa: '||ws_descResp||' ('||ws_codResp||')' where codigo_txel=v_cod_txel::bigint;
		end if;
		-- Valido si existe el documento en emitidos !!!!!!!!!!!!!
		select * from dte_emitidos where rut_emisor=v_rutEmisor::integer and tipo_dte=v_tipo_dte::integer and folio=v_folio::bigint into campo1;
		if found then
			select * from estado_dte where codigo=v_accion into campoE;
			if not found then
				select * from estado_dte where codigo='R'||v_accion into campoE;
			end if;

			-- actualizo estado de emitido
			if(v_accion in ('ACD','RCD')) then
			-- NAR / ACD - RCD
				update dte_emitidos set estado_nar=campoE.descripcion,fecha_nar=now(),mensaje_nar='Glosa: '||ws_descResp||' ('||ws_codResp||')' where rut_emisor=v_rutEmisor::integer and tipo_dte=v_tipo_dte::integer and folio=v_folio::bigint;
			else
			-- ERM - RFP - RFT
				update dte_emitidos set estado_reclamo=campoE.descripcion,fecha_reclamo=now(),mensaje_reclamo='Glosa: '||ws_descResp||' ('||ws_codResp||')' where rut_emisor=v_rutEmisor::integer and tipo_dte=v_tipo_dte::integer and folio=v_folio::bigint;
			end if;
			-- GUARDO EVENTO EN TRAZA
			xml3:='';
			xml3:=put_campo(xml3,'FECHA_EMISION',campo.fecha_emision::varchar);
			xml3:=put_campo(xml3,'RUT_EMISOR',v_rutEmisor::varchar);
			xml3:=put_campo(xml3,'RUT_OWNER',v_rutEmisor::varchar);
			xml3:=put_campo(xml3,'FECHA_EVENTO',now()::varchar);
			xml3:=put_campo(xml3,'RUT_RECEPTOR',campo1.rut_receptor::varchar);
			xml3:=put_campo(xml3,'FOLIO',v_folio::varchar);
			xml3:=put_campo(xml3,'TIPO_DTE',v_tipo_dte::varchar);
			xml3:=put_campo(xml3,'CANAL','EMITIDOS');
                        xml3:=put_campo(xml3,'URI_IN',campo1.uri::varchar);
                        xml3:=put_campo(xml3,'COMENTARIO_TRAZA','Glosa: '||ws_descResp::varchar);
                        xml3:=graba_bitacora(xml3,v_accion);
			xml2:=logapp(xml2,get_campo('_LOG_',xml3));
		else
                        xml2:=logapp(xml2,'No existe el emitido para este recibidos');
                end if;
		
		if(get_campo('__CATEGORIA__',xml2)='NAR') then
			xml2 := logapp(xml2,'SII reponde OK. Seguimos el Flujo NAR');
			xml2 := put_campo(xml2,'__SECUENCIAOK__','160');
			return xml2;	
		else
			xml2 := logapp(xml2,'SII reponde OK. Seguimos el Flujo ARM');
			xml2 := put_campo(xml2,'__SECUENCIAOK__','150');
			return xml2;	
		end if;
		return xml2;
	else
		--Si el SII respondio error...
		xml2:=put_campo(xml2,'RESPUESTA','Status: 400 NK');
		xml2:=logapp(xml2,'El SII respondio ERROR');
		xml2:=put_campo(xml2,'__MENSAJE_10K__','SII Responde Error, '||ws_descResp);
		return xml2;
	end if;
	return xml2;
END;
$$ LANGUAGE plpgsql;

