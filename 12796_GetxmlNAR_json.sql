delete from isys_querys_tx where llave='12796';

insert into isys_querys_tx values ('12796',10,1,1,'select get_xml_NAR_12796(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('12796',20,1,2,'Servicio de Firma 192.168.3.17',4013,109,106,0,0,30,30);

insert into isys_querys_tx values ('12796',30,1,1,'select get_xml_NAR_resp_12796(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

--Se envia al EDTE
insert into isys_querys_tx values ('12796',50,1,8,'Llamada NAR EDTE',12779,0,0,0,0,65,65);
--insert into isys_querys_tx values ('12796',55,1,8,'Llamada NAR',112779,0,0,0,0,65,65);
--Publicamos el NAR
insert into isys_querys_tx values ('12796',65,1,8,'Publica DTE',1127043,0,0,0,0,70,70);

--Validamos la publicacion
insert into isys_querys_tx values ('12796',70,1,1,'select valida_publicacion_nar_12796(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

CREATE or replace FUNCTION get_xml_NAR_12796(json) RETURNS json AS $$
DECLARE
    json1                alias for $1;
    json2               json;
    json3               json;
    json_par1		json;
    exists_select1      varchar;
    rut1                varchar;
    tipo_dte1           varchar;
    folio1              varchar;
    xml_resp1           varchar;
    query1              varchar;
    campo1              RECORD;
    EncabezadoCusDoc    varchar;
    pieFirma            varchar;
    pieCusDoc           varchar;
    id1                 varchar;
    dominio1            varchar;
    estadoDte1          varchar;
    glosaEstado1        varchar;
    request1            varchar;
    aux1                varchar;
        data_firma1     varchar;
        pass1     varchar;
	campo2	record;
	rut2	varchar;
BEGIN
        json2:=json1;
        rut1:=trim(replace(split_part(get_json_upper('rutDte',json2),'-',1),'.',''));
        tipo_dte1:=get_json_upper('tipoDte',json2);
        folio1:=replace(get_json_upper('folioDte',json2),'.','');
        estadoDte1:=get_json_upper('estadoDte',json2);
        --glosaEstado1:=decode_utf8(decode(get_json_upper('glosaEstado',json2),'hex')::varchar);
        glosaEstado1:=decode(get_json_upper('glosaEstado',json2),'hex');

        json2:=put_json(json2,'__SECUENCIAOK__','0');
	--Se setea que el NAR es por pantalla
	json2:=put_json(json2,'__FLAG_PUB_10K__','SI');
	json2:=put_json(json2,'__ORIGEN__','ESCRITORIO');

        json2:=logjson(json2,'Datos para formar xml NAR rut1=' || rut1);
        json2:=logjson(json2,'Datos para formar xml NAR tipo_dte1=' || tipo_dte1);
        json2:=logjson(json2,'Datos para formar xml NAR folio1=' || folio1);
        json2:=logjson(json2,'Datos para formar xml NAR estadoDte1=' || estadoDte1);
        json2:=logjson(json2,'Datos para formar xml NAR glosaEstado1=' || glosaEstado1);
        json2:=logjson(json2,'Datos para formar xml NAR rut_firma=' || get_json_upper('rut_firma',json2));
        json2:=put_json(json2,'RUT_FIRMA',get_json_upper('rut_firma',json2));

        if((rut1 || tipo_dte1 ||folio1 ||estadoDte1 ||glosaEstado1 ||get_json_upper('rut_firma',json2))=NULL) then
                json2:=response_requests_6000('2', 'Alguno de los campos obligatorios es Nulo','', json2);
                return json2;
        end if;

        if(folio1='' or is_number(folio1) is false) then
                json2:=response_requests_6000('2', 'Folio Incorrecto','', json2);
                return json2;
        end if;

        if(tipo_dte1='-1') then
                json2:=response_requests_6000('2','Tipo DTE Incorrecto', '',json2);
                return json2;
        end if;
        
	--Buscamos en base local
	SELECT codigo_txel,uri,rut_emisor,rut_receptor,tipo_dte,folio,fecha_emision,uri_nar,monto_total,estado, to_char(current_timestamp, 'YYYY-MM-DD HH:MM:SS') as time into campo1 FROM dte_recibidos WHERE rut_emisor=rut1::integer and tipo_dte=tipo_dte1::integer and folio=folio1::integer;
	if not found then
		json2:=logjson(json2,'Busco en IMPORTADOS');
		--Buscamos en importados
		SELECT codigo_txel,uri,rut_emisor,rut_receptor,tipo_dte,folio,fecha_emision,uri_nar,monto_total,estado, to_char(current_timestamp, 'YYYY-MM-DD HH:MM:SS') as time into campo1 FROM dte_recibidos_importados_generica WHERE rut_emisor=rut1::integer and tipo_dte=tipo_dte1::integer and folio=folio1::integer;
		if not found then
			json2:=logjson(json2,'Busco en BASE_RESPALDO_CUADRATURA');
			--Buscamos en respaldo cuadratura
			json_par1:=get_parametros_motor_json('{}','BASE_RECIBIDOS_HISTORICOS');
                        json3:=query_db_json(get_json('__IP_CONEXION_CLIENTE__',json_par1),get_json_int('__IP_PORT_CLIENTE__',json_par1)::integer,'SELECT codigo_txel,uri,rut_emisor,rut_receptor,tipo_dte,folio,fecha_emision,uri_nar,monto_total,estado, to_char(current_timestamp, ''YYYY-MM-DD HH:MM:SS'') as time FROM dte_recibidos WHERE rut_emisor='||rut1||' and tipo_dte='||tipo_dte1||' and folio='||folio1);
                        if (get_json('STATUS',json3)='SIN_DATA') then
				json2:=logjson(json2,'STATUS SIN_DATA');
				--Buscamos en los no recibidos
				rut2:=rut1||'-'||modulo11(rut1);
				select * into campo2 from dte_pendientes_recibidos where rut_emisor=rut2 and tipo_dte=tipo_dte1 and folio=folio1; 
				if not found then
					json2:=logjson(json2,'STATUS no encotnrado en dte_pendientes_recibidos');
                			json2:=response_requests_6000('2', 'DTE no encontrado','', json2);
					return json2;
				end if;
				campo1.uri:=campo2.uri;
				campo1.rut_emisor:=split_part(campo2.rut_emisor,'-',1)::integer;
				campo1.rut_receptor:=campo2.rut_receptor::integer;
				campo1.tipo_dte:=campo2.tipo_dte::integer;
				campo1.folio:=campo2.folio::bigint;
				campo1.fecha_emision:=substring(campo2.fecha_emision,1,10);
				campo1.uri_nar:=null;
				campo1.monto_total:=campo2.monto_total::bigint;
				campo1.estado:='NO RECIBIDO';
				campo1.time:=to_char(now(), 'YYYY-MM-DD HH:MM:SS');
				json2:=put_json(json2,'__FLAG_BASE_CUADRATURA__','SI');
                        else
				if(get_json('uri',json3)='') then
					json2:=logjson(json2,'URI vacia');
                			json2:=response_requests_6000('2', 'DTE no encontrado'||json3::varchar,'', json2);
					return json2;
				end if;
				campo1.uri:=get_json('uri',json3);
				campo1.rut_emisor:=get_json('rut_emisor',json3)::integer;
				campo1.rut_receptor:=get_json('rut_receptor',json3)::integer;
				campo1.tipo_dte:=get_json('tipo_dte',json3)::integer;
				campo1.folio:=get_json('folio',json3)::integer;
				campo1.fecha_emision:=get_json('fecha_emision',json3);
				campo1.uri_nar:=get_json('uri_nar',json3);
				campo1.monto_total:=get_json('monto_total',json3);
				campo1.estado:=get_json('estado',json3);
				campo1.time:=get_json('time',json3);
				json2:=put_json(json2,'__FLAG_BASE_CUADRATURA__','SI');
                        end if;
		end if;	
	end if;
        if found or get_json('__FLAG_BASE_CUADRATURA__',json2)='SI' then
		--FAY-RME-ILB 2015-10-21 Si el cliente quiere hacer otro NAR, lo permite aunque tenga otro realizado
		/*
                --Si ya tiene NAR enviado, no lo procesa
                if campo1.uri_nar is not null then
                        json2:=response_requests_6000('1', 'NAR ya procesado',campo1.uri_nar, json2);
                        return json2;
                end if;
		*/

                --Agrego la uri para uso posterior
                json2:=put_json(json2,'CODIGO_TXEL_NAR',campo1.codigo_txel::varchar);
                json2:=put_json(json2,'RUT_EMISOR',campo1.rut_emisor::varchar);
		json2:=put_json(json2,'TIPO_DTE',campo1.tipo_dte::varchar);
		json2:=put_json(json2,'FOLIO',campo1.folio::varchar);
                --Saco el dominio
                aux1:=split_part(split_part(campo1.uri,'//',2),'.',1);
                json2:=put_json(json2,'URI_IN','http://'||substring(aux1,1,length(aux1)-4)||to_char(now(),'YYMM')||'.acepta.com/v01/'||lpad('_'||replace(getipserver('eth0'),'.','')||'_'||to_char(now(),'DDHH24MISSMI')||'_'||nextval('correlativo_uri')::varchar||'_',40,'0')||'?k='||md5(coalesce(substring(aux1,1,length(aux1)-4),'X')));
                json2:=put_json(json2,'URI_DTE',campo1.uri);
                id1:=get_newRespuestaID_2116(campo1.rut_emisor::varchar, campo1.rut_receptor::varchar,campo1.tipo_dte::varchar,campo1.folio::varchar,campo1.fecha_emision::varchar, 'RespuestaDTE');
		
                SELECT dominio FROM maestro_clientes WHERE rut_emisor = campo1.rut_receptor::integer INTO dominio1;
                if not found then
                        dominio1:='';
                end if;
                EncabezadoCusDoc:='<?xml version="1.0" encoding="ISO-8859-1"?>';
                EncabezadoCusDoc:=EncabezadoCusDoc || '<?xml-stylesheet type="text/xsl" href="http://www.custodium.com/intercambio/notificacion.xsl"?> ';
                EncabezadoCusDoc:=EncabezadoCusDoc || '<Document Domain="' ||dominio1|| '" Type="Intercambio">';
                EncabezadoCusDoc:=EncabezadoCusDoc || '<Issuer><PI Type="Rut">' ||campo1.rut_receptor || '-' || modulo11(campo1.rut_receptor::varchar) || '</PI></Issuer>'      ;
                EncabezadoCusDoc:=EncabezadoCusDoc || '<Signers><Signer><PI Type="Rut">'||get_json_upper('rut_firma',json2)||'</PI></Signer></Signers>';
                EncabezadoCusDoc:=EncabezadoCusDoc || '<Recipients><Recipient><PI Type="Rut">' ||campo1.rut_emisor || '-' || modulo11(campo1.rut_emisor::varchar) || '</PI></Recipient></Recipients>';
                EncabezadoCusDoc:=EncabezadoCusDoc || '<Attributes>';
                EncabezadoCusDoc:=EncabezadoCusDoc || '<Attribute Type="TIPODTE">' || campo1.tipo_dte || '</Attribute>';
                EncabezadoCusDoc:=EncabezadoCusDoc || '<Attribute Type="FOLIO">' || campo1.folio || '</Attribute>';
                EncabezadoCusDoc:=EncabezadoCusDoc || '<Attribute Type="FECHAEMISION">' || campo1.fecha_emision || '</Attribute>';
                EncabezadoCusDoc:=EncabezadoCusDoc || '<Attribute Type="RUTEMISOR">' || campo1.rut_emisor || '-' || modulo11(campo1.rut_emisor::varchar) ||'</Attribute>';
                EncabezadoCusDoc:=EncabezadoCusDoc || '<Attribute Type="RUTRECEPTOR">' || campo1.rut_receptor || '-' || modulo11(campo1.rut_receptor::varchar) || '</Attribute>';
                EncabezadoCusDoc:=EncabezadoCusDoc || '<Attribute Type="MONTOTOTAL">' || campo1.monto_total || '</Attribute>';
                EncabezadoCusDoc:=EncabezadoCusDoc || '<Attribute Type="ESTADODTE"> ' || estadoDte1 || '</Attribute>';
                EncabezadoCusDoc:=EncabezadoCusDoc || '<Attribute Type="ESTADODTEGLOSA">' || glosaEstado1 || '</Attribute>';
                EncabezadoCusDoc:=EncabezadoCusDoc || '</Attributes>';
                EncabezadoCusDoc:=EncabezadoCusDoc || '<Content>';

                --xml_resp1:= '<?xml version="1.1"?>';
                xml_resp1:= '<RespuestaDTE xmlns="http://www.sii.cl/SiiDte" xmlns:sii="http://www.sii.cl/SiiDte" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.sii.cl/SiiDte RespuestaEnvioDTE_v10.xsd"  version="1.0">';
                xml_resp1:= xml_resp1 || '<Resultado ID="' || id1 || '">';
                xml_resp1:= xml_resp1 || '<Caratula version="1.0">';
                xml_resp1:= xml_resp1 || '<RutResponde>' || campo1.rut_receptor || '-' || modulo11(campo1.rut_receptor::varchar) || '</RutResponde>';
                xml_resp1:= xml_resp1 || '<RutRecibe>' || campo1.rut_emisor || '-' || modulo11(campo1.rut_emisor::varchar) || '</RutRecibe>';
                xml_resp1:= xml_resp1 || '<IdRespuesta>' || 1 || '</IdRespuesta>';
                xml_resp1:= xml_resp1 || '<NroDetalles>1</NroDetalles>';
                xml_resp1:= xml_resp1 || '<TmstFirmaResp>' || replace(campo1.time,' ','T') || '</TmstFirmaResp>';
                xml_resp1:= xml_resp1 || '</Caratula>';
                xml_resp1:= xml_resp1 || '<ResultadoDTE>';
                xml_resp1:= xml_resp1 || '<TipoDTE>' || campo1.tipo_dte || '</TipoDTE>';
                xml_resp1:= xml_resp1 || '<Folio>' || campo1.folio || '</Folio>';
                xml_resp1:= xml_resp1 || '<FchEmis>' || campo1.fecha_emision || '</FchEmis>';
                xml_resp1:= xml_resp1 || '<RUTEmisor>' || campo1.rut_emisor|| '-' || modulo11(campo1.rut_emisor::varchar) || '</RUTEmisor>';
                xml_resp1:= xml_resp1 || '<RUTRecep>' || campo1.rut_receptor || '-' || modulo11(campo1.rut_receptor::varchar) ||'</RUTRecep>';
                xml_resp1:= xml_resp1 || '<MntTotal>' || campo1.monto_total || '</MntTotal>';
                xml_resp1:= xml_resp1 || '<CodEnvio>' || 1 || '</CodEnvio>';
                xml_resp1:= xml_resp1 || '<EstadoDTE>' || estadoDte1 || '</EstadoDTE>';
                xml_resp1:= xml_resp1 || '<EstadoDTEGlosa>' || glosaEstado1 || '</EstadoDTEGlosa>';
                xml_resp1:= xml_resp1 || '</ResultadoDTE>';
                xml_resp1:= xml_resp1 || '</Resultado>';
                xml_resp1:= xml_resp1 || '</RespuestaDTE>';

                PieCusDoc:='</Content><Log><Process id="motor" version="1.0"><item name="custodium-uri">__REMPLAZA_URI__</item></Process><Process build="" id="MOTOR" version=""><item name="">item</item></Process></Log></Document>';

                xml_resp1:=EncabezadoCusDoc || xml_resp1 || PieCusDoc;

                json2:=logjson(json2,'id='|| id1);
                json2:=logjson(json2,'rut_firma='|| get_json_upper('rut_firma',json2));

                --Armamos para disparar directo a el firmador por socket
		if(get_json_upper('flag_tx_buscar',json2)<>'SI')then
                        pass1:=corrige_pass(decode(get_json_upper('pass',json2),'hex')::text);
                else
                        pass1:=get_json_upper('pass',json2);
                end if;
		pass1:=replace(pass1,chr(92),chr(92)||chr(92));
		
                data_firma1:=replace('{"documento":"'||encode(xml_resp1::bytea,'base64')::varchar||'","nodoId":"'||id1||'","rutEmpresa":"'||get_json_upper('rut_firma',json2)||'-'||modulo11(get_json_upper('rut_firma',json2))||'","entidad":"SII","rutFirmante":"'||get_json_upper('rut_firma',json2)||'-'||modulo11(get_json_upper('rut_firma',json2))||'","codigoAcceso":"'||pass1||'"}',chr(10),'');

		--perform logfile('FIRMADOR: '||data_firma1);
		--perform logfile('FIRMADOR: '||xml_resp1);

                --json2:=get_parametros_motor_json(json2,'FIRMADOR');
		json2:=get_parametros_motor_json(json2,get_parametro_firmador(get_json_upper('rut_firma',json2)||'-'||modulo11(get_json_upper('rut_firma',json2))));
                json2:=put_json(json2,'INPUT_FIRMADOR','POST '||get_json_upper('PARAMETRO_RUTA',json2)||' HTTP/1.1'||chr(10)||'User-Agent: curl/7.26.0'||chr(10)||'Host: '||get_json_upper('__IP_CONEXION_CLIENTE__',json2)||chr(10)||'Accept: '||chr(42)||'/'||chr(42)||chr(10)||'Content-Type: application/json'||chr(10)||'Content-Length: '||length(data_firma1)::varchar||chr(10)||chr(10)||data_firma1);

        	json2:=put_json(json2,'__SECUENCIAOK__','20');
        else
                --json2:=response_requests_6000('2', 'DTE no encontrado','', json2);
                json2:=response_requests_6000('2', 'Por favor actulize la pagina con CRTL-F5 o CRTL-R.','', json2);
        end if;

        RETURN json2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION get_xml_NAR_resp_12796(json) RETURNS json AS $$
DECLARE
    json1                alias for $1;
    json2                json;
    resp1               varchar;
    uri1        varchar;
    pos1        integer;
    pos12       integer;
        data1   varchar;
        aux1    varchar;
        json_resp1      varchar;

BEGIN
	json2:=json1;
        json2:=respuesta_no_chunked_json(json2);
        resp1:=decode(get_json_upper('RESPUESTA_HEX',json2),'hex');
        json_resp1:=split_part(resp1,'\012\012',2);
        json2:=put_json(json2,'RESPUESTA_HEX','');
        json2:=put_json(json2,'INPUT_FIRMADOR','');
        json2:=put_json(json2,'__SECUENCIAOK__','0');

	json_resp1:=split_part(resp1,'\012\012',2);

       --Si viene un 200 OK
       if (strpos(resp1,'HTTP/1.1 200 ')>0) then
                --aux1:=get_xml('documentoFirmado',resp1);
                aux1:=json_get('documentoFirmado',json_resp1);
                if (length(aux1)>0) then
                   --Log para medir el servicio de firma
                   insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json_upper('RUT_FIRMA',json2),'OK','','NAR',get_json_upper('URI_IN',json2));
                   --Obtengo el documento para enviarlo al EDTE
                   data1:=base642hex(aux1);
                   uri1:=get_json_upper('URI_IN',json2);
                   data1:=replace(data1,encode('__REMPLAZA_URI__','hex')::varchar,encode(uri1::bytea,'hex')::varchar);
                   json2:=put_json(json2,'INPUT',data1);
                   json2:=put_json(json2,'CONTENT_LENGTH',length(data1)::varchar);
                   --Lo envio a la secuencia 50 para que vaya al EDTE
                   json2:=put_json(json2,'__SECUENCIAOK__','50');
               else
                   json2:=logjson(json2,'*Respuesta NAR '||resp1);
		   json2:=bitacora10k(json2,'FIRMA','Falla firma NAR. '||coalesce(resp1,''));
                   json2:=response_requests_6000('2', 'NAR no firmado', '',json2);
                   insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json_upper('RUT_FIRMA',json2),'FALLA','NAR no firmado','NAR',get_json_upper('URI_IN',json2));
               end if;
        elsif (strpos(resp1,'HTTP/1.1 500 ')>0) then
		   json2:=bitacora10k(json2,'FIRMA','Falla firma NAR. '||coalesce(resp1,''));
                   json2:=logjson(json2,'Respuesta NAR '||resp1);
                   resp1:=json_get('ERROR',json_resp1);
                   json2:=response_requests_6000('2', resp1, '',json2);
                   insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json_upper('RUT_FIRMA',json2),'FALLA1',resp1,'NAR',get_json_upper('URI_IN',json2));

        else
		   json2:=bitacora10k(json2,'FIRMA','Servicio de Firma no responde NAR. ');
                   json2:=response_requests_6000('2', 'Servicio de Firma no responde', '',json2);
                   insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json_upper('RUT_FIRMA',json2),'FALLA2','Servicio de Firma no responde','NAR',get_json_upper('URI_IN',json2));
        end if;
        return json2;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION valida_publicacion_nar_12796(json) RETURNS json AS $$
declare
        json1    alias for $1;
        json2    json;
        rut1     varchar;
        stContribuyente contribuyentes%ROWTYPE;
        mail1    varchar;
begin
        json2:=json1;
        json2:=put_json(json2,'__SECUENCIAOK__','0');
        if (get_json_upper('__PUBLICADO_OK__',json2)<>'SI') then
		   json2:=bitacora10k(json2,'FIRMA','Falla Publicacion de NAR. ');
			
                  json2:=response_requests_6000('2', 'Falla Publicacion de NAR', '',json2);
                  return json2;
        end if;

        --Si lo escribi bien en el EDTE
        if (get_json_upper('__EDTE_NAR_OK__',json2)<>'SI') then
		if (get_json('FLAG_RECLAMO',json2)='SI') then
			json2:=bitacora10k(json2,'FIRMA',get_json('__MENSAJE_10K__',json2));
                	json2:=response_requests_6000('2',get_json('__MENSAJE_10K__',json2), '',json2);
		else
			json2:=bitacora10k(json2,'FIRMA','Falla Envio de NAR. ');
                	json2:=response_requests_6000('2', 'Falla Envio de NAR', '',json2);
		end if;
                return json2;
        end if;

	if (get_json('FLAG_RECLAMO',json2)='SI') then
		json2:=bitacora10k(json2,'FIRMA',get_json('__MENSAJE_10K__',json2)||' '||get_json_upper('URI_IN',json2));
               	json2:=response_requests_6000('1',get_json('__MENSAJE_10K__',json2),get_json_upper('URI_IN',json2),json2);
	else
		json2:=bitacora10k(json2,'FIRMA','NAR firmado' ||get_json_upper('URI_IN',json2));
	        json2:=response_requests_6000('1', 'NAR firmado',get_json_upper('URI_IN',json2),json2);
	end if;
        return json2;
END;
$$ LANGUAGE plpgsql;

