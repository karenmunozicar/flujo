--Reclamar DTE SII
delete from isys_querys_tx where llave='16201';

insert into isys_querys_tx values ('16201',10,9,1,'select armo_llamada_sii_16201(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('16201',15,1,2,'Servicio de SII 172.16.14.88',4013,100,101,0,0,20,20);

--Valida respuesta sii
insert into isys_querys_tx values ('16201',20,9,1,'select valida_respuesta_sii_16201(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('16201',900,19,1,'select respondo_16201(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

--insert into isys_querys_tx values ('16201',1010,19,1,'select sp_procesa_respuesta_cola_motor88_json(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,0,0);

CREATE or replace FUNCTION respondo_16201(json) RETURNS json AS $$
DECLARE
        json1   alias for $1;
        json2   json;
BEGIN
        json2:=json1;
        json2:=put_json(json2,'__SECUENCIAOK__','0');
        --Si viene desde el 16210 no borramos de la cola
        if(get_json('__COLA_MOTOR__',json2)<>'' and get_json('__FLAG_16210__',json2)<>'NO_BORRAR') then
                return sp_procesa_respuesta_cola_motor88_json(json2);
        end if;
        if(get_json('RESPUESTA',json2)='Status: 200 OK') then
                return response_requests_6000('1',get_json('__MENSAJE_10K__',json2), '',json2);
        --Si fallo el SII
        elsif (get_json('FALLA_COMM_SII',json2)='SI' and get_json('__FLAG_16210__',json2)='NO_BORRAR') then
                return response_requests_6000('3',get_json('__MENSAJE_10K__',json2), '',json2);
        else
                return response_requests_6000('2',get_json('__MENSAJE_10K__',json2), '',json2);
        end if;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION valida_respuesta_sii_16201(json) RETURNS json AS $$
DECLARE
        json1   alias for $1;
        json2   json;
BEGIN
        json2:=json1;
        json2:=put_json(json2,'__SECUENCIAOK__','900');
        if(get_json('CATEGORIA',json2)='MERCADERIA') then
                json2:=logjson(json2,'validamos como lor arm');
                json2:=valida_respuesta_sii_mercaderia_16201(json2);
        else
                json2:=logjson(json2,'validamos como lor nar');
                json2:=valida_respuesta_sii_contenido_16201(json2);
        end if;
        if(get_json('__ESTADO_RECLAMO__',json2) in ('SI','BORRAR')) then
                json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
        else
                json2:=put_json(json2,'RESPUESTA','Status: 400 NK');
                json2:=put_json(json2,'MENSAJE_XML_FLAGS','Falla Realizar Acción '||get_json('__MENSAJE_10K__',json2));
                json2:=logjson(json2,'Falla Realizar Acción '||get_json('__MENSAJE_10K__',json2));
        end if;
        --Si viene con Nomina se pone mensaje para los usuarios no saben que hacen
        if (is_number(get_json('NOMINA',json2))) then
                json2:=bitacora10k(json2,'SII',get_json('__BITACORA_10K__',json2)||' '||get_json('__MENSAJE_10K__',json2)||' URI='||get_json('URI_DTE',json2)||' (Nomina '||get_json('NOMINA',json2)||')');
        else
                json2:=bitacora10k(json2,'SII',get_json('__BITACORA_10K__',json2)||' '||get_json('__MENSAJE_10K__',json2)||' URI='||get_json('URI_DTE',json2)||' (SN)');
        end if;
        return json2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION armo_llamada_sii_16201(json) RETURNS json AS $$
DECLARE
        json1   alias for $1;
        json2   json;
        flag_reclamo    varchar;
        input1  varchar;
        json_in         json;
        evento1         varchar;
        campo   record;
        aux1    varchar;

        v_data varchar;
        v_rutEmisor     varchar;
        v_folio varchar;
        v_tipo_dte      varchar;
        v_rutReceptor   varchar;
        v_accion        varchar;
        v_dvEmisor      varchar;
        port varchar;

        uri1    varchar;
        v_reg_devengo     record;
BEGIN
        json2:=json1;
        json2:=put_json(json2,'__SECUENCIAOK__','900');

        v_rutEmisor:=get_json('RUT_EMISOR',json2);
        v_folio:=get_json('FOLIO',json2);
        v_tipo_dte:=get_json('TIPO_DTE',json2);
        v_accion:=get_json('EVENTO_RECLAMO',json2);
        v_rutReceptor:=get_json('RUT_RECEPTOR',json2);


        if(get_json('EVENTO_RECLAMO',json2) in ('ACD','RCD')) then
                json2:=put_json(json2,'CATEGORIA','DOC');
        elsif(get_json('EVENTO_RECLAMO',json2) in ('ERM','RFP','RFT','ERG')) then
                json2:=put_json(json2,'CATEGORIA','MERCADERIA');
        else
                json2:=put_json(json2,'RESPUESTA','Status: 444 NK');
                json2:=put_json(json2,'MENSAJE_XML_FLAGS','Accion no identifacada');
                json2:=put_json(json2,'__MENSAJE_10K__','Acción no identifacada');
                json2:=logjson(json2,'Accion no identifacada '||v_accion);
                return json2;
        end if;
        json2:=put_json(json2,'NOMINA',replace(get_json('NOMINA',json2),'.',''));

        if(is_number(v_rutEmisor) is false or is_number(v_tipo_dte) is false or is_number(v_folio) is false ) then
                json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
                json2:=put_json(json2,'__MENSAJE_10K__','Datos Inválidos');
                json2:=logjson(json2,'Datos no numericos v_rutEmisor='||v_rutEmisor||' v_tipo_dte='||v_tipo_dte||' folio='||v_folio);
                return json2;
        end if;
        -- FGE - 20200820 - No reclamar si tiene un devengo finalizado sin errores
        -- FGE - 20201030 - Solo se debe verificar si es RCD, RFP, RFT
        if v_accion in ('RCD', 'RFP', 'RFT') then
                select estado, codigo_dv, codigo_oc, codigo_rc, rut_emisor, rut_receptor from dp_devengo where dte_codigo_txel = (select codigo_txel::varchar from dte_recibidos where rut_emisor=v_rutEmisor::integer and tipo_dte=v_tipo_dte::integer and folio=v_folio::bigint) and tipo_dte = v_tipo_dte::integer into v_reg_devengo;
                if found then
                        if v_reg_devengo.estado in ('FINALIZADO_SIN_ERRORES', 'MANUAL_SIGFE') then
                                json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
                                json2:=put_json(json2,'__MENSAJE_10K__','No es posible reclamar un documento que tiene un devengo con estado Finalizado Sin Errores o Manual Sigfe');
                                json2:=logjson(json2,'El DTE v_rutEmisor='||v_rutEmisor||' v_tipo_dte='||v_tipo_dte||' folio='||v_folio||' tiene un devengo FINALIZADO SIN ERRORES, imposible reclamar');
                                return json2;
                        end if;
                        -- FGE - 20201111 - Si es gubernamental y tiene un devengo, entonces liberar la RC.
                        if v_reg_devengo.codigo_rc <> '' then
                                update token_de_emitidos set codigo_dv = null, estado = 'ACEPTADA' where token = v_reg_devengo.codigo_rc and rut_receptor = v_reg_devengo.rut_receptor;
                                update dp_devengo set codigo_rc = '' where codigo_dv = v_reg_devengo.codigo_dv;
                        end if;
                end if;
        end if;

        select uri,data_dte,codigo_txel,estado_nar,estado_reclamo,rut_receptor::bigint,fecha_emision::varchar into campo from dte_recibidos where rut_emisor=v_rutEmisor::integer and tipo_dte=v_tipo_dte::integer and folio=v_folio::bigint;
        if not found then
                aux1:=v_rutEmisor||'-'||modulo11(v_rutEmisor);
                select uri,data_dte,id::bigint as codigo_txel,estado_nar,estado_reclamo,rut_receptor,split_part(fecha_emision,' ',1)::varchar as fecha_emision  into campo from dte_pendientes_recibidos where rut_emisor=aux1 and tipo_dte=v_tipo_dte and folio=v_folio;
                if not found then
                        --Si esta en alguna cola, y lleva mas de 10 dias se borra el reclamo
                        if (get_json('FECHA_INGRESO_COLA',json2)<>'') then
                                if (now()-get_json('FECHA_INGRESO_COLA',json2)::timestamp>interval '10 days') then
                                        json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
                                        json2:=logjson(json2,'Documento no encontrado en dte_recibidos v_rutEmisor='||v_rutEmisor||' v_tipo_dte='||v_tipo_dte||' folio='||v_folio||' Se borra de las colas por estar mas de 10 dias');
                                        return json2;
                                end if;
                        end if;
                        /*
                        json2:=put_json(json2,'MENSAJE_XML_FLAGS','Documento no encontrado');
                        json2:=put_json(json2,'RESPUESTA','Status: 444 NK');
                        json2:=put_json(json2,'__MENSAJE_10K__','Documento no encontrado');
                        json2:=logjson(json2,'Documento no encontrado en dte_recibidos v_rutEmisor='||v_rutEmisor||' v_tipo_dte='||v_tipo_dte||' folio='||v_folio);
                        return json2;
                        */
                        --FAY-DAO 20190507 si no se encuentra pendiente recibido vamos al reclamo o aceptacion igual, y se graba despues de la respuesta del sii
                        json2:=put_json(json2,'FLAG_PREA','SI');
                        json2:=put_json(json2,'CODIGO_TXEL_RECLAMO','-1');
                        if is_number(v_rutReceptor) is false then
                                json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
                                json2:=put_json(json2,'__MENSAJE_10K__','Datos Inválidos');
                                json2:=logjson(json2,'Receptor no numerico '||v_rutReceptor);
                                return json2;
                        end if;
                        uri1:='http://'||(select dominio from maestro_clientes where rut_emisor=v_rutReceptor::bigint)||to_char(now(),'YYMM')||'.acepta.com/v01/'||genera_uri2(v_rutEmisor,v_tipo_dte,v_folio,get_json('FECHA_EMISION',json2),get_json('MONTO_TOTAL',json2),'R');
                        json2:=put_json(json2,'URI_DTE',uri1);
                        json2:=logjson(json2,'DTE no existe en dte_pendientes_recibidos se crea URI='||uri1||' '||v_rutEmisor||' '||v_tipo_dte||' '||v_folio||' '||get_json('FECHA_EMISION',json2)||' '||get_json('MONTO_TOTAL',json2));
                        --Insertamos en los pendientes a responsabilidad del cliente
                        insert into dte_pendientes_recibidos (fecha_ingreso,tipo_dte,folio,fecha_emision,rut_emisor,rut_receptor,monto_total,dia,data_dte,uri) values (now(),v_tipo_dte,v_folio,get_json('FECHA_EMISION',json2),aux1,v_rutReceptor::bigint,get_json('MONTO_TOTAL',json2),to_char(now(),'YYYYMMDD')::integer,put_data_dte('','SAR_USUARIO','Este Dte fue registrado por el cliente'),uri1) returning uri,data_dte,id as codigo_txel,estado_nar,estado_reclamo,rut_receptor,fecha_emision into campo;
                else
                        json2:=put_json(json2,'FLAG_PREA','SI');
                        json2:=put_json(json2,'CODIGO_TXEL_RECLAMO',campo.codigo_txel::varchar);
                        json2:=put_json(json2,'URI_DTE',campo.uri::varchar);
                        json2:=logjson(json2,'DTE en dte_pendientes_recibidos');
                end if;
        else
                json2:=put_json(json2,'FLAG_PREA','NO');
                json2:=put_json(json2,'CODIGO_TXEL_RECLAMO',campo.codigo_txel::varchar);
                json2:=put_json(json2,'URI_DTE',campo.uri::varchar);
                json2:=put_json(json2,'DATA_DTE',campo.data_dte::varchar);
        end if;

        --Si el DTE Recibido ya esta marcado OK, no lo reprocesamos
        if(get_json('EVENTO_RECLAMO',json2) in ('ACD','RCD')) then
                --Si esta en un estado final...
                if (campo.estado_nar='ACEPTACION_DE_CONTENIDO_DE_DOCUMENTO') then
                        json2:=logjson(json2,'DTE ya esta Aceptado en el SII URI='||campo.uri);
                        json2:=put_json(json2,'__MENSAJE_10K__','DTE ya esta Aceptado en el SII');
                        json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
                        json2:=put_json(json2,'__SECUENCIAOK__','900');
                        return json2;
                elsif (campo.estado_nar='RECHAZO_DE_CONTENIDO_DE_DOCUMENTO') then
                        json2:=logjson(json2,'DTE ya esta Reclamado en el SII URI='||campo.uri);
                        json2:=put_json(json2,'__MENSAJE_10K__','DTE ya esta Reclamado en el SII');
                        json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
                        json2:=put_json(json2,'__SECUENCIAOK__','900');
                        return json2;
                end if;
        elsif(get_json('EVENTO_RECLAMO',json2) in ('ERM','RFP','RFT','ERG')) then
                if (campo.estado_reclamo='OTORGA_RECIBO_DE_MERCADERIA_O_SERVICIO') then
                        json2:=logjson(json2,'DTE ya esta Aceptado con Mercaderia en el SII URI='||campo.uri);
                        json2:=put_json(json2,'__MENSAJE_10K__','DTE ya esta Aceptado con Mercaderia en el SII');
                        json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
                        json2:=put_json(json2,'__SECUENCIAOK__','900');
                        return json2;
                elsif (campo.estado_reclamo in ('RECLAMO_FALTA_PARCIAL_DE_MERCADERIA','RECLAMO_FALTA_TOTAL_DE_MERCADERIA')) then
                        json2:=logjson(json2,'DTE ya esta Reclamado por Mercaderia en el SII URI='||campo.uri);
                        json2:=put_json(json2,'__MENSAJE_10K__','DTE ya esta Reclamado por Mercaderia en el SII');
                        json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
                        json2:=put_json(json2,'__SECUENCIAOK__','900');
                        return json2;
                end if;
        end if;


        json2:=put_json(json2,'RUT_RECEPTOR',campo.rut_receptor::varchar);
        json2:=put_json(json2,'FECHA_EMISION',campo.fecha_emision::varchar);
        v_dvEmisor:=modulo11(v_rutEmisor::varchar);
        json_in:=('{"rutEmisor":"'||v_rutEmisor::varchar||'","dvEmisor":"'||v_dvEmisor::varchar||'","tipoDoc":"'||v_tipo_dte::varchar||'","folio":"'||v_folio::varchar||'","accionDoc":"'||v_accion||'","RUT_OWNER":"'||campo.rut_receptor::varchar||'"}')::json;
        json2:=logjson(json2,'Envio al SII '||json_in::varchar);
        json2:=put_json(json2,'__SECUENCIAOK__','15');

        --DAO 20171031 Buscamos puerto libre. En caso de no existir libre y ser desde Escritorio vamos al 2025
        --json2:=get_parametros_motor_json(json2,'SERVICIO_SII_JSON');
        --if (get_json('__FLAG_PUB_10K__',json2)<>'SI') then
        /*
                port:=get_ipport_sii();
                --Si no hay puertos libres ...
                if (port='') then
                        if get_json('__FLAG_PUB_10K__',json2)='SI' then
                                json2:=get_parametros_motor_json(json2,'SERVICIO_SII_JSON');
                        else
                                --Si no hay puertos libres...
                                json2:=logjson(json2,'No hay puertos libres ');
                                json2:=put_json(json2,'MENSAJE_XML_FLAGS','No hay puertos libres');
                                json2:=put_json(json2,'__SECUENCIAOK__','900');
                                json2:=put_json(json2,'RESPUESTA','Status: 400 NK');
                                return json2;
                        end if;
                else
                        json2:=put_json(json2,'__IP_PORT_CLIENTE__',split_part(port,':',2));
                        json2:=put_json(json2,'__IP_CONEXION_CLIENTE__',split_part(port,':',1));
                end if;
        */
        --end if;
        json2:=put_json(json2,'__IP_PORT_CLIENTE__','8080');
        json2:=put_json(json2,'__IP_CONEXION_CLIENTE__','interno.acepta.com');
        --json2:=put_json(json2,'IP_PORT_CLIENTE',get_json('__IP_CONEXION_CLIENTE__',json2)||':'||get_json('__IP_PORT_CLIENTE__',json2));
        json2:=logjson(json2,'PORT=8080 IP_PORT_CLIENTE=interno.acepta.com');

        if (get_xml('FmaPago',get_json('DATA_DTE',json2))='1') then
                --perform libera_ipport_sii(port,'OK');
                json2:=put_json(json2,'__SECUENCIAOK__','20');
                json2:=logjson(json2,'DTE con FmaPago al contado, no se va al SII');
                json2:=put_json(json2,'RESPUESTA','HTTP/1.0 200'||chr(10)||chr(10)||'{"codResp":"27","descResp":"No se puede registrar un evento (acuse de recibo, reclamo o aceptación de contenido) de un DTE pagado al contado**"}');
                return json2;
        end if;
        --No se puede registrar un evento (acuse de recibo, reclamo o aceptación de contenido) de un DTE pagado al contado.

        json2:=put_json(json2,'INPUT','POST /sii/reclamo_aceptacion HTTP/1.1'||chr(10)||'User-Agent: curl/7.26.0'||chr(10)||'Host: '||get_json('__IP_CONEXION_CLIENTE__',json2)||':'||get_json('__IP_PORT_CLIENTE__',json2)||chr(10)||'Accept: '||chr(42)||'/'||chr(42)||chr(10)||'Content-Type: application/json; charset=ISO-8859-1'||chr(10)||'Content-Length: '||length(json_in::varchar)::varchar||chr(10)||chr(10)||json_in::varchar);
        return json2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION valida_respuesta_sii_contenido_16201(json) RETURNS json AS $$
DECLARE
        json1   alias for $1;
        json2   json;
        resp1   varchar;
        json_out        json;
        evento1 varchar;
        v_tipo_dte      varchar;
        v_folio         varchar;
        v_rutEmisor     varchar;
        campoE  record;
        campo1  record;
        ws_codResp      varchar;
        ws_descResp     varchar;
        xml3    varchar;
        v_cod_txel      bigint;

        desc_bitacora1  varchar;
BEGIN
        json2:=json1;

        resp1:=get_json('RESPUESTA',json2);
        json2:=logjson(json2,'Respuesta='||replace(resp1,chr(10),''));
        if(strpos(resp1,'HTTP/1.0 200')=0 and strpos(resp1,'HTTP/1.1 200')=0) then
                --perform libera_ipport_sii(get_json('IP_PORT_CLIENTE',json2),'FALLA');
                json2:=logjson(json2,'Falla el SII. Reintentamos.3');
                --Si viene de pantalla le doy otra oportunidad
                if (get_json('__FLAG_PUB_10K__',json2)='SI'  and get_json('_REINTENTO_SII_',json2)='') then
                        json2:=logjson(json2,'Reintento una  vez');
                        json2:=put_json(json2,'_REINTENTO_SII_','1');
                        json2:=put_json(json2,'__SECUENCIAOK__','10');
                        json2:=put_json(json2,'FALLA_COMM_SII','SI');
                        return json2;
                end if;
                json2:=put_json(json2,'__MENSAJE_10K__','Falla Comunicación SII');
                json2:=put_json(json2,'FALLA_COMM_SII','SI');
                return json2;
        end if;

        --perform libera_ipport_sii(get_json('IP_PORT_CLIENTE',json2),'OK');
        BEGIN
                json_out:=split_part(resp1,chr(10)||chr(10),2)::json;
        EXCEPTION WHEN OTHERS THEN
                json2:=logjson(json2,'Falla el SII. Reintentamos.4 '||split_part(resp1,chr(10)||chr(10),2));
                --Si viene de pantalla le doy otra oportunidad
                if (get_json('__FLAG_PUB_10K__',json2)='SI'  and get_json('_REINTENTO_SII_',json2)='') then
                        json2:=logjson(json2,'Reintento una  vez');
                        json2:=put_json(json2,'_REINTENTO_SII_','1');
                        json2:=put_json(json2,'__SECUENCIAOK__','10');
                        json2:=put_json(json2,'FALLA_COMM_SII','SI');
                        return json2;
                end if;
                json2:=put_json(json2,'__MENSAJE_10K__','Falla Comunicación SII');
                json2:=put_json(json2,'FALLA_COMM_SII','SI');
                return json2;
        END;
        json2:=put_json(json2,'FALLA_COMM_SII','NO');

        ws_codResp:=get_json('codResp',json_out::json);
        ws_descResp:=replace_unicode(get_json('descResp',json_out::json));

        evento1:=get_json('EVENTO_RECLAMO',json2);
        v_cod_txel:=get_json('CODIGO_TXEL_RECLAMO',json2);

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
        if(get_json('rutUsuario',json2)<>'') then
                if (is_number(get_json('NOMINA',json2))) then
                        xml3:=put_campo(xml3,'COMENTARIO_TRAZA','Glosa SII: '||ws_descResp||'('||ws_codResp||')'||chr(10)||'Acción Realizada por '||get_json('rutUsuario',json2)||'-'||modulo11(get_json('rutUsuario',json2))||' Nomina='||get_json('NOMINA',json2));
                else
                        xml3:=put_campo(xml3,'COMENTARIO_TRAZA','Glosa SII: '||ws_descResp||'('||ws_codResp||')'||chr(10)||'Acción Realizada por '||get_json('rutUsuario',json2)||'-'||modulo11(get_json('rutUsuario',json2)));
                end if;
        elsif(get_json('DESC_ORIGEN',json2)<>'') then
                xml3:=put_campo(xml3,'COMENTARIO_TRAZA','Glosa SII: '||ws_descResp||'('||ws_codResp||')'||chr(10)||get_json('DESC_ORIGEN',json2));
        else
                xml3:=put_campo(xml3,'COMENTARIO_TRAZA','Glosa SII: '||ws_descResp||'('||ws_codResp||')');
        end if;

        json2:=logjson(json2,'Respuesta SII COD='||ws_codResp||' DESC='||ws_descResp);
        if (ws_codResp in ('2','5','6','8','10','11','27','13','17','9')) then
                -- TRAIGO ESTADO DEL EVENTO
                select * from estado_dte where codigo='R'||evento1 into campoE;
                if not found then
                        select * from estado_dte where codigo=evento1 into campoE;
                end if;
                --Para pintar inmediatamente el FLAG
                json2:=put_json(json2,'__COLOR__','RED');
                desc_bitacora1:=campoE.glosa;
                if(get_json('FLAG_PREA',json2)='SI') then
                        update dte_pendientes_recibidos set estado_nar=campoE.descripcion||'_ERROR',fecha_nar=now(),mensaje_nar='Glosa SII: '||ws_descResp||' ('||ws_codResp||')' where id=v_cod_txel::bigint and coalesce(estado_nar,'')<>campoE.descripcion;
                else
                        update dte_recibidos set estado_nar=campoE.descripcion||'_ERROR',fecha_nar=now(),mensaje_nar='Glosa SII: '||ws_descResp||' ('||ws_codResp||')' where codigo_txel=v_cod_txel::bigint and coalesce(estado_nar,'')<>campoE.descripcion;
                end if;
                xml3:=graba_bitacora(xml3,evento1||'_FALLA');
                json2:=logjson(json2,get_campo('_LOG_',xml3));
                json2:=put_json(json2,'__ESTADO_RECLAMO__','BORRAR');
                json2:=put_json(json2,'__MENSAJE_10K__','Acción Fallida '||ws_descResp);

        elsif(ws_codResp in ('0','7')) then
                -- TRAIGO ESTADO DEL EVENTO
                select * from estado_dte where codigo='R'||evento1 into campoE;
                if not found then
                        select * from estado_dte where codigo=evento1 into campoE;
                end if;
                desc_bitacora1:=campoE.glosa;
                json2:=put_json(json2,'__COLOR__','GREEN');
                if(get_json('FLAG_PREA',json2)='SI') then
                        update dte_pendientes_recibidos set estado_nar=campoE.descripcion,fecha_nar=now(),mensaje_nar='Glosa SII: '||ws_descResp||' ('||ws_codResp||')' where id=v_cod_txel::bigint and coalesce(estado_nar,'')<>campoE.descripcion;
                else
                        update dte_recibidos set estado_nar=campoE.descripcion,fecha_nar=now(),mensaje_nar='Glosa SII: '||ws_descResp||' ('||ws_codResp||')' where codigo_txel=v_cod_txel::bigint and coalesce(estado_nar,'')<>campoE.descripcion;
                end if;

                xml3:=graba_bitacora(xml3,evento1);
                json2:=logjson(json2,get_campo('_LOG_',xml3));

                -- Valido si existe el documento en emitidos !!!!!!!!!!!!!
                select * from dte_emitidos where rut_emisor=v_rutEmisor::integer and tipo_dte=v_tipo_dte::integer and folio=v_folio::bigint into campo1;
                if found then
                        select * from estado_dte where codigo=evento1 into campoE;
                        if not found then
                                select * from estado_dte where codigo='R'||evento1 into campoE;
                        end if;
                        update dte_emitidos set estado_nar=campoE.descripcion,fecha_nar=now(),mensaje_nar='Glosa SII: '||ws_descResp||' ('||ws_codResp||')' where rut_emisor=v_rutEmisor::integer and tipo_dte=v_tipo_dte::integer and folio=v_folio::bigint and coalesce(estado_nar,'')<>campoE.descripcion;
                        xml3:=put_campo(xml3,'RUT_OWNER',campo1.rut_emisor::varchar);
                        xml3:=put_campo(xml3,'CANAL','EMITIDOS');
                        xml3:=put_campo(xml3,'URI_IN',campo1.uri::varchar);
                        xml3:=graba_bitacora(xml3,evento1);
                        json2:=logjson(json2,get_campo('_LOG_',xml3));
                else
                        json2:=logjson(json2,'No existe el emitido para este recibidos');
                end if;
                json2:=put_json(json2,'__ESTADO_RECLAMO__','SI');
                json2:=put_json(json2,'__BITACORA_10K__',desc_bitacora1);
                json2:=put_json(json2,'__MENSAJE_10K__','Acción Realizada OK');
        else
                json2:=put_json(json2,'__MENSAJE_10K__','SII responde error '||ws_descResp);
                return json2;
        end if;
        return json2;
END;$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION valida_respuesta_sii_mercaderia_16201(json) RETURNS json AS $$
DECLARE
        json1   alias for $1;
        json2   json;
        resp1   varchar;
        json_out        json;
        evento1 varchar;
        v_tipo_dte      varchar;
        v_folio         varchar;
        v_rutEmisor     varchar;
        campoE  record;
        campo1  record;
        ws_codResp      varchar;
        ws_descResp     varchar;
        xml3    varchar;
        v_cod_txel      bigint;
        desc_bitacora1  varchar;
BEGIN
        json2:=json1;

        resp1:=get_json('RESPUESTA',json2);
        json2:=logjson(json2,'Respuesta='||replace(resp1,chr(10),''));
        if(strpos(resp1,'HTTP/1.0 200')=0 and strpos(resp1,'HTTP/1.1 200')=0) then
                --perform libera_ipport_sii(get_json('IP_PORT_CLIENTE',json2),'FALLA');
                json2:=logjson(json2,'Falla el SII. Reintentamos.1');
                --Si viene de pantalla le doy otra oportunidad
                if (get_json('__FLAG_PUB_10K__',json2)='SI'  and get_json('_REINTENTO_SII_',json2)='') then
                        json2:=logjson(json2,'Reintento una  vez');
                        json2:=put_json(json2,'_REINTENTO_SII_','1');
                        json2:=put_json(json2,'__SECUENCIAOK__','10');
                        json2:=put_json(json2,'FALLA_COMM_SII','SI');
                        return json2;
                end if;
                json2:=put_json(json2,'__MENSAJE_10K__','Falla Comunicación SII');
                json2:=put_json(json2,'FALLA_COMM_SII','SI');
                return json2;
        end if;
        --perform libera_ipport_sii(get_json('IP_PORT_CLIENTE',json2),'OK');

        BEGIN
                json_out:=split_part(resp1,chr(10)||chr(10),2)::json;
        EXCEPTION WHEN OTHERS THEN
                json2:=logjson(json2,'Falla el SII. Reintentamos.2 '||split_part(resp1,chr(10)||chr(10),2));
                --Si viene de pantalla le doy otra oportunidad
                if (get_json('__FLAG_PUB_10K__',json2)='SI'  and get_json('_REINTENTO_SII_',json2)='') then
                        json2:=logjson(json2,'Reintento una  vez');
                        json2:=put_json(json2,'_REINTENTO_SII_','1');
                        json2:=put_json(json2,'__SECUENCIAOK__','10');
                        json2:=put_json(json2,'FALLA_COMM_SII','SI');
                        return json2;
                end if;
                json2:=put_json(json2,'__MENSAJE_10K__','Falla Comunicación SII');
                json2:=put_json(json2,'FALLA_COMM_SII','SI');
                return json2;
        END;
        json2:=put_json(json2,'FALLA_COMM_SII','NO');

        ws_codResp:=get_json('codResp',json_out::json);
        ws_descResp:=replace_unicode(get_json('descResp',json_out::json));

        evento1:=get_json('EVENTO_RECLAMO',json2);
        v_cod_txel:=get_json('CODIGO_TXEL_RECLAMO',json2);

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
        --xml3:=put_campo(xml3,'COMENTARIO_TRAZA','Glosa: '||ws_descResp||'('||ws_codResp||')');
        if(get_json('rutUsuario',json2)<>'' ) then
                if (is_number(get_json('NOMINA',json2))) then
                        xml3:=put_campo(xml3,'COMENTARIO_TRAZA','Glosa SII: '||ws_descResp||'('||ws_codResp||')'||chr(10)||'Acción Realizada por '||get_json('rutUsuario',json2)||'-'||modulo11(get_json('rutUsuario',json2))||' Nomina='||get_json('NOMINA',json2));
                else
                        xml3:=put_campo(xml3,'COMENTARIO_TRAZA','Glosa SII: '||ws_descResp||'('||ws_codResp||')'||chr(10)||'Acción Realizada por '||get_json('rutUsuario',json2)||'-'||modulo11(get_json('rutUsuario',json2)));
                end if;
        elsif(get_json('DESC_ORIGEN',json2)<>'') then
                json2:=logjson(json2,'DESC_ORIGEN='||get_json('DESC_ORIGEN',json2)||' '||get_json('URI_DTE',json2));
                xml3:=put_campo(xml3,'COMENTARIO_TRAZA','Glosa SII: '||ws_descResp||'('||ws_codResp||')'||chr(10)||get_json('DESC_ORIGEN',json2));
        else
                xml3:=put_campo(xml3,'COMENTARIO_TRAZA','Glosa SII: '||ws_descResp||'('||ws_codResp||')');
        end if;

        json2:=logjson(json2,'Respuesta SII COD='||ws_codResp||' DESC='||ws_descResp);
        if (ws_codResp in ('5','6','8','10','11','3','27','13','9')) then
                -- TRAIGO ESTADO DEL EVENTO
                select * from estado_dte where codigo='R'||evento1 into campoE;
                if not found then
                        select * from estado_dte where codigo=evento1 into campoE;
                end if;
                desc_bitacora1:=campoE.glosa;
                json2:=put_json(json2,'__COLOR__','RED');
                if(get_json('FLAG_PREA',json2)='SI') then
                        update dte_pendientes_recibidos set estado_reclamo=campoE.descripcion||'_ERROR',fecha_reclamo=now(),mensaje_reclamo='Glosa SII: '||ws_descResp||' ('||ws_codResp||')' where id=v_cod_txel::bigint and coalesce(estado_reclamo,'')<>campoE.descripcion;
                else
                        update dte_recibidos set estado_reclamo=campoE.descripcion||'_ERROR',fecha_reclamo=now(),mensaje_reclamo='Glosa SII: '||ws_descResp||' ('||ws_codResp||')' where codigo_txel=v_cod_txel::bigint and coalesce(estado_reclamo,'')<>campoE.descripcion;
                end if;
                xml3:=graba_bitacora(xml3,evento1||'_FALLA');
                json2:=logjson(json2,get_campo('_LOG_',xml3));
                json2:=put_json(json2,'__MENSAJE_10K__','Acción Fallida '||ws_descResp);
                json2:=put_json(json2,'__ESTADO_RECLAMO__','BORRAR');

        elsif(ws_codResp in ('0','7')) then
                -- TRAIGO ESTADO DEL EVENTO
                select * from estado_dte where codigo='R'||evento1 into campoE;
                if not found then
                        select * from estado_dte where codigo=evento1 into campoE;
                end if;
                json2:=put_json(json2,'__COLOR__','GREEN');
                desc_bitacora1:=campoE.glosa;
                if(get_json('FLAG_PREA',json2)='SI') then
                        update dte_pendientes_recibidos set estado_reclamo=campoE.descripcion,fecha_reclamo=now(),mensaje_reclamo='Glosa SII: '||ws_descResp||' ('||ws_codResp||')' where id=v_cod_txel::bigint and coalesce(estado_reclamo,'')<>campoE.descripcion;
                else
                        update dte_recibidos set estado_reclamo=campoE.descripcion,fecha_reclamo=now(),mensaje_reclamo='Glosa SII: '||ws_descResp||' ('||ws_codResp||')' where codigo_txel=v_cod_txel::bigint and coalesce(estado_reclamo,'')<>campoE.descripcion;
                end if;
                xml3:=graba_bitacora(xml3,evento1);
                json2:=logjson(json2,get_campo('_LOG_',xml3));
                -- Valido si existe el documento en emitidos !!!!!!!!!!!!!
                select * from dte_emitidos where rut_emisor=v_rutEmisor::integer and tipo_dte=v_tipo_dte::integer and folio=v_folio::bigint into campo1;
                if found then
                        select * from estado_dte where codigo=evento1 into campoE;
                        if not found then
                                select * from estado_dte where codigo='R'||evento1 into campoE;
                        end if;
                        update dte_emitidos set estado_reclamo=campoE.descripcion,fecha_reclamo=now(),mensaje_reclamo='Glosa SII: '||ws_descResp||' ('||ws_codResp||')' where rut_emisor=v_rutEmisor::integer and tipo_dte=v_tipo_dte::integer and folio=v_folio::bigint and coalesce(estado_reclamo,'')<>campoE.descripcion;
                        xml3:=put_campo(xml3,'RUT_OWNER',campo1.rut_emisor::varchar);
                        xml3:=put_campo(xml3,'CANAL','EMITIDOS');
                        xml3:=put_campo(xml3,'URI_IN',campo1.uri::varchar);
                        xml3:=graba_bitacora(xml3,evento1);
                        json2:=logjson(json2,get_campo('_LOG_',xml3));
                else
                        json2:=logjson(json2,'No existe el emitido para este recibidos');
                end if;
                json2:=put_json(json2,'__BITACORA_10K__',desc_bitacora1);
                json2:=put_json(json2,'__MENSAJE_10K__','Acción Realizada OK');
                json2:=put_json(json2,'__ESTADO_RECLAMO__','SI');
        else
                json2:=put_json(json2,'__MENSAJE_10K__','SII responde error '||ws_descResp);
                return json2;
        end if;
        return json2;
END;$$ LANGUAGE plpgsql;

