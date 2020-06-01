--Consulta Estado Reclamo SII
delete from isys_querys_tx where llave='16106';

insert into isys_querys_tx values ('16106',10,1,1,'select armo_reclamo_sii_16106(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('16106',20,1,2,'Servicio de SII 172.16.14.88',4013,100,101,0,0,30,30);
insert into isys_querys_tx values ('16106',30,1,1,'select proceso_reclamo_sii_16106(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,1000,1000);
insert into isys_querys_tx values ('16106',1000,19,1,'select procesa_respuesta_16106(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,0,0);

CREATE or replace FUNCTION pivote_consulta_reclamo_16106(json) RETURNS json AS $$
DECLARE
        json2   json;
        json1   alias for $1;
	campo record;
	cod1	bigint;
BEGIN
        json2:=json1;
	json2:=put_json(json2,'RUT_EMISOR',trim(split_part(replace(get_json('EMISOR',json2),'.',''),'-',1)));
	json2:=put_json(json2,'TIPO_DTE',trim(get_json('TIPO',json2)));
	json2:=put_json(json2,'FOLIO',replace(get_json('FOLIO',json2),'.',''));
	json2:=put_json(json2,'CODIGO_TXEL',replace(get_json('COD_TXEL',json2),'.',''));
	if is_number(get_json('CODIGO_TXEL',json2)) is false then
		return response_requests_6000_upper('2', 'Falla busqueda DTE', '',json2);
	end if;
	cod1:=get_json('CODIGO_TXEL',json2)::bigint;
	--Verificamos que el codigo txel sea del cliente
	select * into campo from dte_recibidos where codigo_txel=cod1;
	if not found then
		return response_requests_6000_upper('2', 'Falla busqueda DTE.', '',json2);
	end if;
	if campo.rut_receptor<>get_json('rutCliente',json2)::bigint then
                return response_requests_6000_upper('2', 'Receptor no corresponde', '',json2);
        end if;
	json2:=put_json(json2,'URI_IN',campo.uri);
	
        json2:=put_json(json2,'LLAMA_FLUJO','SI');
        json2:=put_json(json2,'__SECUENCIAOK__','16106');
	return json2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION procesa_respuesta_16106(json) RETURNS json  AS $$
DECLARE
	json1 alias for $1;
	json2	json;
BEGIN
	json2:=json1;
	if get_json('__COLA_MOTOR__',json2)<>'' then
		return sp_procesa_respuesta_cola_motor88_json(json2);
	else
		if (strpos(get_json('RESPUESTA',json2),'Status: 200 OK')>0) then
			return response_requests_6000_upper('1', 'Estado Actualizado', '',json2);
		else
			return response_requests_6000_upper('2', 'Falla. Reintente', '',json2);
		end if;
	end if;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION armo_reclamo_sii_16106(varchar) RETURNS varchar AS $$
DECLARE
        xml1   alias for $1;
        xml2   varchar;

        json_in json;

        v_rutEmisor     varchar;
        v_dvEmisor      varchar;
        v_tipo_dte      varchar;
        v_folio         varchar;

        v_canal         varchar;
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
	port		varchar;
BEGIN
	xml2:=xml1;

        v_rutEmisor:=get_campo('RUT_EMISOR',xml2);
        v_dvEmisor:=modulo11(v_rutEmisor);
        v_tipo_dte:=get_campo('TIPO_DTE',xml2);
        v_folio:=get_campo('FOLIO',xml2);
        v_canal:=get_campo('CANAL',xml2);

	--port:=nextval('correlativo_servicio_sii')::varchar;
	port:=get_ipport_sii();
	--Si no hay puertos libres ...
        if (port='') then
	        --Si no hay puertos libres...
               xml2:=logapp(xml2,'No hay puertos libres');
	       xml2:=put_campo(xml2,'RESPUESTA','Status: 400 NK');
	       xml2 := put_campo(xml2,'__SECUENCIAOK__','1000');
               return xml2;
        end if;
	
        json_in:='{"rutEmisor":"'||v_rutEmisor::varchar||'","dvEmisor":"'||v_dvEmisor::varchar||'","tipoDoc":"'||v_tipo_dte::varchar||'","folio":"'||v_folio::varchar||'","RUT_OWNER":"'||v_rutEmisor::varchar||'"}';
	--perform logfile('F_16106 '||json_in::varchar);
	xml2:=logapp(xml2,'armo_reclamo_sii json_in='||json_in::varchar);
	xml2 := put_campo(xml2,'__SECUENCIAOK__','20');
	xml2:=put_campo(xml2,'__IP_CONEXION_CLIENTE__',split_part(port,':',1));
	xml2:=put_campo(xml2,'__IP_PORT_CLIENTE__',split_part(port,':',2));
	xml2:=put_campo(xml2,'__PORT_SII__',port);

	xml2:=logapp(xml2,'DATA_DTE='||get_campo('DATA_DTE',xml2));
	if (get_xml('FmaPago',get_campo('DATA_DTE',xml2))='1') then
		xml2 := put_campo(xml2,'__SECUENCIAOK__','30');
		xml2:=logapp(xml2,'DTE con FmaPago al contado, no se va al SII');
		xml2 := put_campo(xml2,'RESPUESTA','HTTP/1.0 200'||chr(10)||chr(10)||'{"listaEventosDoc": [{"codEvento": "PAG", "fechaEvento": "'||to_char(now(),'DD-MM-YYYY HH24:MI:SS')||'", "dvResponsable": "", "rutResponsable": "", "descEvento": "DTE Pagado al Contado**"}], "codResp": 15, "descResp": "Listado de eventos del documento"}');
		return xml2;
	end if;

        xml2:=put_campo(xml2,'INPUT','POST /lista_eventos_docto HTTP/1.1'||chr(10)||'User-Agent: curl/7.26.0'||chr(10)||'Host: '||get_campo('__IP_CONEXION_CLIENTE__',xml2)||':'||port||chr(10)||'Accept: '||chr(42)||'/'||chr(42)||chr(10)||'Content-Type: application/json; charset=ISO-8859-1'||chr(10)||'Content-Length: '||length(json_in::varchar)::varchar||chr(10)||chr(10)||json_in::varchar);
	return xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION proceso_reclamo_sii_16106(varchar) RETURNS varchar AS $$
DECLARE
        xml1   alias for $1;
        xml2 varchar;
        xml3 varchar;

        json_in json;

        v_rutEmisor     varchar;
        v_dvEmisor      varchar;
        v_tipo_dte      varchar;
        v_folio         varchar;

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
	evento1	varchar;
BEGIN
	xml2:=xml1;
	resp1:=get_campo('RESPUESTA',xml2);
	--perform logfile('DAO_SII '||resp1);
	xml2 :=put_campo(xml2,'__SECUENCIAOK__','1000');	
	xml2:=logapp(xml2,'ENTRO a proceso_reclamo_sii_16106');
	if(strpos(resp1,'HTTP/1.0 200')=0) then
		perform libera_ipport_sii(get_campo('__PORT_SII__',xml2),'FALLA');
                xml2:=put_campo(xml2,'RESPUESTA','Status: 400 NK');
		return xml2;
	end if;
	
	xml2:=logapp(xml2,'SII_RESPONDE F_16106 '||split_part(resp1,chr(10)||chr(10),2));
	BEGIN
		json_out:=split_part(resp1,chr(10)||chr(10),2)::json;
	EXCEPTION WHEN OTHERS THEN
		xml2:=logapp(xml2,'SII_RESPONDE F_16106 Falla JSON '||resp1);
		xml2:=put_campo(xml2,'RESPUESTA','Status: 400 NK');
		perform libera_ipport_sii(get_campo('__PORT_SII__',xml2),'FALLA');
		return xml2;
	END;
	xml2 :=put_campo(xml2,'JSON_OUT',json_out::varchar);
	perform libera_ipport_sii(get_campo('__PORT_SII__',xml2),'OK');
	return parseo_respuesta_sii_16106(xml2);
	
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION parseo_respuesta_sii_16106(varchar) RETURNS varchar AS $$
DECLARE
        xml1   alias for $1;
        xml2 varchar;
        xml3 varchar;

        json_in json;

        v_rutEmisor     varchar;
        v_dvEmisor      varchar;
        v_tipo_dte      varchar;
        v_folio         varchar;

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
        resp1   varchar;
        evento1 varchar;
BEGIN
        xml2:=xml1;
	json_out:=get_campo('JSON_OUT',xml2)::json;
        ws_codResp:=get_json('codResp',json_out::json);
        ws_descResp:=get_json('descResp',json_out::json);
        v_cod_txel:=get_campo('CODIGO_TXEL',xml2);

        xml2:=logapp(xml2,'*************** RESPUESTA WS ***************** '||json_out::varchar);
        xml2:=logapp(xml2,'*************** COD_RESPUESTA_WS => '||ws_codResp::varchar);
        xml2:=logapp(xml2,'*************** DESC_RESPUESTA_WS => '||ws_descResp::varchar);
        --Si no tiene reclamo
        if(ws_codResp in ('18')) then
		xml2 :=logapp(xml2,'Se borra de las colas. El SII responde = Documento no ha sido recibido');
		xml2 :=put_campo(xml2,'RESPUESTA','Status: 200 OK');
		return xml2;
        elsif(ws_codResp in ('16','10')) then
                xml2 :=put_campo(xml2,'RESPUESTA','Status: 200 OK');
                --Se graba evento de que no tiene reclamo...por ahora
                xml3:='';
                xml3:=put_campo(xml3,'FECHA_EMISION',get_campo('FECHA_EMISION',xml2));
                xml3:=put_campo(xml3,'RUT_EMISOR',get_campo('RUT_EMISOR',xml2));
                xml3:=put_campo(xml3,'RUT_OWNER',get_campo('RUT_EMISOR',xml2));
                xml3:=put_campo(xml3,'FECHA_EVENTO',now()::varchar);
                xml3:=put_campo(xml3,'RUT_RECEPTOR',get_campo('RUT_RECEPTOR',xml2));
                xml3:=put_campo(xml3,'FOLIO',get_campo('FOLIO',xml2));
                xml3:=put_campo(xml3,'TIPO_DTE',get_campo('TIPO_DTE',xml2));
                xml3:=put_campo(xml3,'CANAL','RECIBIDOS');
                xml3:=put_campo(xml3,'URI_IN',get_campo('URI_IN',xml2));
                xml3:=put_campo(xml3,'COMENTARIO_TRAZA','Glosa SII: '||ws_descResp::varchar);
                xml3:=graba_bitacora(xml3,'SIN_RECLAMO_SII');
                xml2:=logapp(xml2,get_campo('_LOG_',xml3));

                update dte_recibidos set fecha_ult_modificacion=now(),estado_reclamo='SIN_RECLAMO_SII',fecha_reclamo=now(),mensaje_reclamo=ws_descResp where codigo_txel=v_cod_txel::bigint and coalesce(estado_reclamo,'') in ('','SIN_RECLAMO_SII');
        	xml2:=logapp(xml2,'Marca sin reclamo');
                return xml2;
        --Si no tiene permisos
        elsif(ws_codResp='14') then
                xml2 :=put_campo(xml2,'RESPUESTA','Status: 200 OK');
                --Se graba evento de que no tiene reclamo...por ahora
                xml3:='';
                xml3:=put_campo(xml3,'FECHA_EMISION',get_campo('FECHA_EMISION',xml2));
                xml3:=put_campo(xml3,'RUT_EMISOR',get_campo('RUT_EMISOR',xml2));
                xml3:=put_campo(xml3,'RUT_OWNER',get_campo('RUT_EMISOR',xml2));
                xml3:=put_campo(xml3,'FECHA_EVENTO',now()::varchar);
                xml3:=put_campo(xml3,'RUT_RECEPTOR',get_campo('RUT_RECEPTOR',xml2));
                xml3:=put_campo(xml3,'FOLIO',get_campo('FOLIO',xml2));
                xml3:=put_campo(xml3,'TIPO_DTE',get_campo('TIPO_DTE',xml2));
                xml3:=put_campo(xml3,'CANAL','RECIBIDOS');
                xml3:=put_campo(xml3,'URI_IN',get_campo('URI_IN',xml2));
                xml3:=put_campo(xml3,'COMENTARIO_TRAZA','Consulta Reclamos: '||ws_descResp::varchar);
                xml3:=graba_bitacora(xml3,'SIN_PERMISOS_SII');
                xml2:=logapp(xml2,get_campo('_LOG_',xml3));

                update dte_recibidos set fecha_ult_modificacion=now(),estado_reclamo='SIN_PERMISOS_SII',fecha_reclamo=now(),mensaje_reclamo=ws_descResp where codigo_txel=v_cod_txel::bigint and coalesce(estado_reclamo,'') in ('','SIN_PERMISOS_SII');
                return xml2;

        elsif(ws_codResp='15') then
                ws_json:=get_json('listaEventosDoc',json_out::json);
                --json2:=logjson(json2,'*************** JSON_RESPUESTA_WS => '||ws_json::varchar);
                -- cuento los eventos
                i:=0;
                aux:=get_json_index(ws_json::json,i);
                -- recorro los eventos
                while length(aux) > 0 loop
                        aux2:=aux::json;
                        i:=i+1;
                        aux:=get_json_index(ws_json::json,i);
                        ws_codEvento:=get_json('codEvento',aux2::json);
                        ws_descEvento:=get_json('descEvento',aux2::json);
                        -- Solo respondo desc del reclamo/aceptacion
                        if(ws_codEvento in ('ACD','ERM','ERG')) then
                                evento1:=ws_codEvento;
                                if(ws_codEvento='ACD') then
                                        te_estado:='ACEPTACION_DE_CONTENIDO_DE_DOCUMENTO';
                                        te_fecha:=get_json('fechaEvento',aux2::json);
                                        --Se actualiza si no estaba actualizado
                                        update dte_recibidos set estado_nar=te_estado,fecha_nar=to_timestamp(te_fecha,'DD-MM-YYYY HH24:MI:SS'),mensaje_nar=aux2::varchar where codigo_txel=v_cod_txel::bigint and coalesce(estado_nar,'')<>te_estado;
				elsif (ws_codEvento='ERG') then
					te_estado:='ACUSE_RECIBO_MERCADERIA_GD';
                                        te_fecha:=get_json('fechaEvento',aux2::json);
                                        update dte_recibidos set fecha_ult_modificacion=now(),estado_reclamo=te_estado,fecha_reclamo=to_timestamp(te_fecha,'DD-MM-YYYY HH24:MI:SS'),mensaje_reclamo=aux2::varchar where codigo_txel=v_cod_txel::bigint and coalesce(estado_reclamo,'')<>te_estado;
                                else
                                        te_estado:='OTORGA_RECIBO_DE_MERCADERIA_O_SERVICIO';
                                        te_fecha:=get_json('fechaEvento',aux2::json);
                                        update dte_recibidos set fecha_ult_modificacion=now(),estado_reclamo=te_estado,fecha_reclamo=to_timestamp(te_fecha,'DD-MM-YYYY HH24:MI:SS'),mensaje_reclamo=aux2::varchar where codigo_txel=v_cod_txel::bigint and coalesce(estado_reclamo,'')<>te_estado;
                                end if;
                                -- ACTUALIZAMOS EL ESTADO DEL DOCUMENTO EN DTE_EMITIDOS
                        elsif (ws_codEvento in ('RCD','RFP','RFT')) then
                                evento1:=ws_codEvento;
                                if(ws_codEvento='RCD') then
                                        te_estado:='RECHAZO_DE_CONTENIDO_DE_DOCUMENTO';
                                        te_fecha:=get_json('fechaEvento',aux2::json);
                                        update dte_recibidos set estado_nar=te_estado,fecha_nar=to_timestamp(te_fecha,'DD-MM-YYYY HH24:MI:SS'),mensaje_nar=aux2::varchar where codigo_txel=v_cod_txel::bigint and coalesce(estado_nar,'')<>te_estado;
                                elsif(ws_codEvento='RFP') then
                                        te_estado:='RECLAMO_FALTA_PARCIAL_DE_MERCADERIA';
                                        te_fecha:=get_json('fechaEvento',aux2::json);
                                        update dte_recibidos set fecha_ult_modificacion=now(),estado_reclamo=te_estado,fecha_reclamo=to_timestamp(te_fecha,'DD-MM-YYYY HH24:MI:SS'),mensaje_reclamo=aux2::varchar where codigo_txel=v_cod_txel::bigint and coalesce(estado_reclamo,'')<>te_estado;
                                else
                                        te_estado:='RECLAMO_FALTA_TOTAL_DE_MERCADERIA';
                                        te_fecha:=get_json('fechaEvento',aux2::json);
                                        update dte_recibidos set fecha_ult_modificacion=now(),estado_reclamo=te_estado,fecha_reclamo=to_timestamp(te_fecha,'DD-MM-YYYY HH24:MI:SS'),mensaje_reclamo=aux2::varchar where codigo_txel=v_cod_txel::bigint and coalesce(estado_reclamo,'')<>te_estado;
                                end if;
			elsif (ws_codEvento in ('NCA','ENC','PAG')) then
				te_estado:=ws_codEvento;
				evento1:=ws_codEvento;
                                te_fecha:=get_json('fechaEvento',aux2::json);
                                update dte_recibidos set fecha_ult_modificacion=now(),estado_reclamo=te_estado,fecha_reclamo=to_timestamp(te_fecha,'DD-MM-YYYY HH24:MI:SS'),mensaje_reclamo=aux2::varchar where codigo_txel=v_cod_txel::bigint and coalesce(estado_reclamo,'')<>te_estado;
			--Si el dte esta cedido, solo se graba en la traza
			elsif (ws_codEvento in ('CED')) then	
				evento1:=ws_codEvento;
				te_estado:=ws_codEvento;
				te_fecha:=get_json('fechaEvento',aux2::json);
                        end if;
                        if(evento1 is not null) then
                                xml3:='';
                                xml3:=put_campo(xml3,'FECHA_EMISION',get_campo('FECHA_EMISION',xml2));
                                xml3:=put_campo(xml3,'RUT_EMISOR',get_campo('RUT_EMISOR',xml2));
                                xml3:=put_campo(xml3,'RUT_OWNER',get_campo('RUT_EMISOR',xml2));
                                xml3:=put_campo(xml3,'FECHA_EVENTO',now()::varchar);
                                xml3:=put_campo(xml3,'RUT_RECEPTOR',get_campo('RUT_RECEPTOR',xml2));
                                xml3:=put_campo(xml3,'FOLIO',get_campo('FOLIO',xml2));
                                xml3:=put_campo(xml3,'TIPO_DTE',get_campo('TIPO_DTE',xml2));
                                xml3:=put_campo(xml3,'CANAL','RECIBIDOS');
                                xml3:=put_campo(xml3,'URI_IN',get_campo('URI_IN',xml2));
                                xml3:=put_campo(xml3,'COMENTARIO_TRAZA','Glosa SII: '||ws_descEvento::varchar);
                                xml3:=graba_bitacora(xml3,evento1);
                                xml2:=logapp(xml2,get_campo('_LOG_',xml3));
                               	xml2 :=put_campo(xml2,'RESPUESTA','Status: 200 OK');
                        else
                                xml2 :=put_campo(xml2,'MENSAJE_XML_FLAGS','-COD='||ws_codEvento||' DESC='||ws_descEvento);
                                xml2 :=put_campo(xml2,'RESPUESTA','Status: 444 NK');
        			xml2:=logapp(xml2,'Pone reintentos 10');
                        end if;
                end loop;
        else
                xml2 :=put_campo(xml2,'MENSAJE_XML_FLAGS','COD='||ws_codResp||' DESC='||ws_descResp);
                xml2 :=put_campo(xml2,'RESPUESTA','Status: 444 NK');
        	xml2:=logapp(xml2,'Pone reintentos 10');
        end if;
        return xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION insert_cola_reclamo_sii_emi_16106(varchar,varchar,varchar,varchar,varchar,varchar,varchar) RETURNS varchar AS $$
DECLARE
	codigo_txel1	alias for $1;
	rut_emisor1	alias for $2;
	tipo_dte1	alias for $3;
	folio1		alias for $4;
	rut_receptor1	alias for $5;
	uri1		alias for $6;
	data_dte1	alias for $7;
	xml3	varchar;
	id1	bigint;
	tx1	varchar;
	nombre_tabla1	varchar;
	query1	varchar;
	cola1	varchar;
BEGIN
                xml3:='';
                xml3:=put_campo(xml3,'TX','16106');
                xml3:=put_campo(xml3,'URI_IN',uri1);
                xml3:=put_campo(xml3,'CANAL','EMITIDOS');
                xml3:=put_campo(xml3,'RUT_EMISOR',rut_emisor1);
                xml3:=put_campo(xml3,'RUT_RECEPTOR',rut_receptor1);
                xml3:=put_campo(xml3,'TIPO_DTE',tipo_dte1);
                xml3:=put_campo(xml3,'FOLIO',folio1);
                xml3:=put_campo(xml3,'CODIGO_TXEL',codigo_txel1);
                xml3:=put_campo(xml3,'DATA_DTE',data_dte1);
		xml3:=put_campo(xml3,'FECHA_INGRESO_COLA',now()::varchar);
                cola1:=nextval('id_cola_sii');
                tx1:='30';
                nombre_tabla1:='cola_sii_'||cola1::varchar;
                query1:='insert into ' || nombre_tabla1 || ' (fecha,uri,reintentos,data,tx,rut_emisor,reproceso,categoria, nombre_cola) values ( now(),'||quote_literal(uri1)||',0,'||quote_literal(xml3)||','||tx1||','||quote_literal(rut_emisor1::varchar)||',''NO'',''ESTADO_RECLAMO_SII'','|| quote_literal(nombre_tabla1) ||') returning id';
		execute query1 into id1;
		if id1 is not null then
                        return 'URI='||uri1||' se graba Evento para consultar Reclamo';
		else
			return 'FALLA';
                end if;
END;
$$ LANGUAGE plpgsql;

