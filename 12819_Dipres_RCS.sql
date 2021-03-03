delete from isys_querys_tx where llave='12819';
insert into isys_querys_tx values ('12819',10,1,2,'Consulta dipres rcs',4013,300,101,0,0,20,20);
insert into isys_querys_tx values ('12819',20,1,1,'select procesa_resp_ms_12819(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

create or replace function pivote_rcs_12819(json)
    returns json
    as $function$
declare
    json1          alias for $1;
    json2          json;
    id1	           bigint;
    idp1           bigint;
    xml2           varchar;

    v_codigo_txel  bigint;
    v_campo        record;
    v_json_post    json;
    v_uuid         varchar;
    datos_wf       json;
    v_rut_emisor   integer;
    v_rut_receptor integer;
    v_folio        bigint;
    v_tipo_dte     integer;

    mensaje1       varchar;
    mensaje2       varchar;
    mensaje_html   varchar;
    acciones1      varchar;
    evento_ori1    varchar;
    v_uri          varchar;
    
    v_referencias  json;
    v_index        integer;
    v_referencia   varchar;
    v_codigo_oc    varchar;
	v_reg_dte      record;
	aux1	varchar;
begin
    json2:=json1;
    id1:=nullif(get_json('id_solicitud',json2),'')::bigint;
    if id1 is null then
	    return response_requests_6000('2', 'Error al leer id solicitud.', '', json2);
    end if;

    select id_pendiente into idp1 from wf_pendiente_10k where id_solicitud=id1::bigint;

    select * into v_campo from workflow_controller where id_solicitud=id1;
    if not found then 
	    return response_requests_6000('2', 'Error en workflow controller', '', json2);
    end if;
    -- FGE - 20200414 - Validamos que el DTE no esta reclamado
    aux1:=get_campo('CODIGO_TXEL', v_campo.xml2);

    select codigo_txel, estado_nar, estado_reclamo, fecha_sii from dte_recibidos where codigo_txel = aux1::bigint into v_reg_dte;

    -- FGE - 20210118 - Validacion 192 Horas
    if extract(epoch from now() - v_reg_dte.fecha_sii)/3600 < 192 then
        json2:=put_json(json2, 'RESPUESTA', 'Status: 444 NK');
        json2:=logjson(json2, 'La solicitud no alcanza las 192 horas requeridas, tiene ' || (extract(epoch from now() - v_reg_dte.fecha_sii)/3600)::varchar);
    end if;

    if coalesce(v_reg_dte.estado_nar,'') = 'RECHAZO_DE_CONTENIDO_DE_DOCUMENTO' or coalesce(v_reg_dte.estado_reclamo,'') in ('RECLAMO_FALTA_PARCIAL_DE_MERCADERIA','RECLAMO_FALTA_TOTAL_DE_MERCADERIA') then
         datos_wf:=wf_avanza_solicitud(('{"wf_id_solicitud":"'||id1::varchar||'","rutCliente":"'||get_campo('RUT_RECEPTOR',v_campo.xml2)||'","rutUsuario":"99999999","perfil":"Automatico","decision":"dte_reclamado","aplicacion":"DTE","wf_id_pendiente":"'||idp1::varchar||'"}')::json);
         if (datos_wf is not null) then
             update dte_recibidos set data_dte=put_data_dte(data_dte,'WF_TAREA_ACTUAL',get_json('wf_desc_tarea_actual',datos_wf)) where codigo_txel=v_reg_dte.codigo_txel;
         end if;
         return response_requests_6000('2', 'DTE Reclamado con anterioridad', '', json2);
    end if;


    -- FGE - 20191005 - Buscamos la referencia
    v_codigo_oc:='';
    v_referencias:=get_campo('REFERENCIAS_JSON', v_campo.xml2)::json;
    v_index:=0;
    v_referencia:=get_json_index(v_referencias, v_index);
    while length(v_referencia)>0 loop
        if get_json('Tipo', v_referencia::json) = '801' then
            v_codigo_oc := upper(get_json('Folio', v_referencia::json));
        end if;
        v_index:=v_index+1;
        v_referencia:=get_json_index(v_referencias, v_index);
    end loop;
    --if get_campo('TIPO_DTE_REF_1', v_campo.xml2) = '801' then
    if v_codigo_oc <> '' then

        --- FGE - 20190910 - Vamos a dejar traza para la gente de Dipres!
        xml2 := ''; --v_campo.xml2;
        v_uri:=get_campo('URI_IN', v_campo.xml2);
	xml2:=put_campo(xml2,'URI_IN',v_uri);
        mensaje_html:='';
        acciones1:='Ninguna';
        evento_ori1:=get_campo('EVENTO', v_campo.xml2);
        mensaje1:=mensaje1||'Aplica Regla [[no_llega_rc]]  Detalle='||get_campo('MENSAJE_VALIDA_DETALLE', v_campo.xml2);
        mensaje2:=mensaje2||'Aplica Regla [[no_llega_rc]]  Detalle='||get_campo('MENSAJE_VALIDA_DETALLE', v_campo.xml2);
        mensaje_html:=mensaje_html||get_campo('MENSAJE_VALIDA_DETALLE_HTML', v_campo.xml2);
	xml2 := put_campo(xml2,'COMENTARIO2','Aplica Regla EVENTO_RECIBIDOS - [genera_rcs]');
	xml2 := put_campo(xml2,'COMENTARIO_TRAZA','<b>FILTROS:</b><br>'||mensaje_html||'<b>ACCIONES:</b>'||acciones1);
	--Para que no grabe la url_get
	--Si no aplica, solo grabamos en la traza si el flag_visualiza_no_aplica_traza esta en si
 
       
	--if (get_campo('__GRABA_TRAZA__',v_campo.xml2) in ('SI','SOLO_OK','')) then
		--json2 := logjson(json2,'----- FGE - traza: ' || xml2);
		xml2 := graba_bitacora_aws(xml2,'CONTROLLER');
	--end if;

        v_uuid:=uuid_in(md5(random()::text || clock_timestamp()::text)::cstring)::varchar;
        v_json_post:='{}';
        v_json_post:=put_json(v_json_post, 'FechaEnvio', now()::varchar );
        v_json_post:=put_json(v_json_post, 'CodigoTicket', v_uuid);
        --v_json_post:=put_json(v_json_post, 'CodigoOC', get_campo('FOLIO_REF_1',v_campo.xml2));
	v_json_post:=put_json(v_json_post, 'CodigoOC', v_codigo_oc);  --get_campo('FOLIO_REF_1',v_campo.xml2));
        v_json_post:=put_json(v_json_post, 'FechaEmisionDTE',get_campo('FECHA_EMISION',v_campo.xml2) );
        v_json_post:=put_json(v_json_post, 'NumeroFactura', get_campo('FOLIO',v_campo.xml2));
        v_json_post:=put_json(v_json_post, 'MontoDTE', get_campo('MONTO_TOTAL',v_campo.xml2));
        v_json_post:=put_json(v_json_post, 'RutProveedor', replace(replace(to_char(split_part(get_campo('RUT_EMISOR_DV',v_campo.xml2), '-', 1)::integer, '99,999,999'), ',', '.') || '-' || split_part(get_campo('RUT_EMISOR_DV',v_campo.xml2), '-', 2), ' ', ''));
        v_json_post:=put_json(v_json_post, 'RutComprador', replace(replace(to_char(split_part(get_campo('RUT_RECEPTOR_DV',v_campo.xml2), '-', 1)::integer, '99,999,999'), ',', '.') || '-' || split_part(get_campo('RUT_RECEPTOR_DV',v_campo.xml2), '-', 2), ' ', '')); 
        v_json_post:=put_json(v_json_post, 'FechaAceptacionSII', get_campo('FECHA_RECEPCION_SII', v_campo.xml2));

        v_rut_emisor:=split_part(get_campo('RUT_EMISOR_DV',v_campo.xml2), '-', 1)::integer;
        v_rut_receptor:=split_part(get_campo('RUT_RECEPTOR_DV',v_campo.xml2), '-', 1)::integer;
        v_folio:=get_campo('FOLIO',v_campo.xml2)::bigint;
        v_tipo_dte:=get_campo('TIPO_DTE',v_campo.xml2)::integer;

        --select codigo_txel into v_codigo_txel from dte_recibidos where rut_emisor = split_part(get_campo('RUT_EMISOR_DV',v_campo.xml2), '-', 1)::integer and rut_receptor = split_part(get_campo('RUT_RECEPTOR_DV',v_campo.xml2), '-', 1)::integer and folio = get_campo('FOLIO',v_campo.xml2)::bigint and tipo_dte = get_campo('TIPO_DTE',v_campo.xml2)::integer;
        select codigo_txel into v_codigo_txel from dte_recibidos where rut_emisor = v_rut_emisor and rut_receptor = v_rut_receptor and folio = v_folio and tipo_dte = v_tipo_dte;        

        if not found then
            json2:=response_requests_6000('2', 'Existe un problema con los servicios de consulta. '||v_rut_emisor::varchar||' '||v_rut_receptor::varchar||' '||v_folio::varchar||' '||v_tipo_dte::varchar, '', json2);
        else
            update dte_recibidos set data_dte = put_data_dte(data_dte, 'RCS', v_uuid) where codigo_txel = v_codigo_txel;

            -- Cargo los parametros para el flujo y paso el json del devengo
            json2:=put_json(json2,'LLAMA_FLUJO','SI');
            json2:=put_json(json2,'__SECUENCIAOK__','12819');
            json2:=get_parametros_motor_json(json2,'DIPRES_RCS');
            json2:=put_json(json2,'HOST_MS','servicios.acepta.com');
            json2:=put_json(json2,'URI_MS','chilecompra/rcs');
            json2:=put_json(json2,'DATA_JSON',encode_hex(v_json_post::varchar));
            json2:=put_json(json2,'LARGO_JSON',(length(encode_hex(v_json_post::varchar))/2)::varchar);
        end if;
    else
	 json2:=response_requests_6000('2', 'Referencia 801 Codigo OC no encontrada.', '', json2);
        --json2:=response_requests_6000('2', 'Existe un problema con los servicios de consulta. No existe Referencia 801', '', json2);
    end if;

    return json2;

end;
$function$ language plpgsql;

create or replace function procesa_resp_ms_12819(json)
    returns json
    as $function$
declare
    json1               alias for $1;
    json2               json;
    json3               json;
    datos_wf            json;
    
    v_respuesta         varchar;
    v_id_solicitud      bigint;
    v_codigo_txel       bigint;
    idp1                bigint;
    v_rut_emisor        integer;
    v_rut_receptor      integer;
    v_codigo_oc         varchar;
    v_codigo_rc         varchar;
    v_folio_compromiso  varchar;
    v_monto_total       numeric;
    v_encontrado        varchar;

    v_campo             record;
	xml3	varchar;
	v_resp_oc           json;
begin
    json2:=json1;
    v_id_solicitud:=get_json('id_solicitud', json2)::bigint;
    select * into v_campo from workflow_controller where id_solicitud=v_id_solicitud;
    select id_pendiente into idp1 from wf_pendiente_10k where id_solicitud=v_id_solicitud::bigint;
    

    v_respuesta:=get_json('RESPUESTA',json2);
    if(strpos(v_respuesta,'HTTP/1.1 200')=0) then
        json3:=put_json(json3,'MENSAJE_VACIO','Error conexion servicio');
        json2:=response_requests_6000('1', 'Error conexion servicio', json3::varchar, json2);
        return json2;
    end if;

    BEGIN
        v_respuesta:=split_part(v_respuesta,chr(10)||chr(10),2);
    EXCEPTION WHEN OTHERS THEN
        json3:=put_json(json3,'MENSAJE_VACIO','Error al leer respuesta servicio');
        json2:=response_requests_6000('1', 'Error al leer respuesta servicio.', json3::varchar, json2);
        return json2;
    END;

    v_codigo_txel:=get_campo('CODIGO_TXEL',v_campo.xml2)::bigint;

    --update dte_recibidos set data_dte = put_data_dte(data_dte, 'RCS_RESP', v_respuesta) where codigo_txel = v_codigo_txel;

    if lower(get_json('estado', v_respuesta::json)) = 'nok' then
        v_encontrado:='SI';
	xml3:=v_campo.xml2;
        xml3:=put_campo(xml3,'EVENTO','CONTROLLER');
        xml3:=put_campo(xml3,'COMENTARIO_TRAZA','<b>RESPUESTA RCS</b><br><li>' || get_json('mensaje', v_respuesta::json) || '</li><b>');
        xml3:=put_campo(xml3,'COMENTARIO2','Aplica Regla RCS');
	xml3:=graba_bitacora_aws(xml3,'CONTROLLER');
    else
        --perform mp_ingresa_oc(v_respuesta::json);
	v_resp_oc:=mp_ingresa_oc(v_respuesta::json);

        ---  PONER LOGICA DE RC ---
        v_rut_emisor:=replace(split_part(get_campo('RUT_RECEPTOR_DV', v_campo.xml2), '-', 1), '.', '')::integer;
        v_rut_receptor:=replace(split_part(get_campo('RUT_EMISOR_DV',v_campo.xml2), '-', 1), '.', '')::integer;
        v_monto_total:=get_campo('MONTO_TOTAL',v_campo.xml2)::numeric;
	--v_codigo_oc:=get_campo('FOLIO_REF_1',v_campo.xml2);
	v_codigo_oc:=dp_obtiene_oc_flujo(v_id_solicitud::varchar);
        select folio_compromiso into v_folio_compromiso from de_emitidos where rut_emisor = v_rut_emisor and rut_receptor = v_rut_receptor and folio = v_codigo_oc limit 1;
        select token into v_codigo_rc from token_de_emitidos where rut_emisor = v_rut_emisor and rut_receptor = v_rut_receptor and estado = 'ACEPTADA' and monto_rc = v_monto_total and folio = v_codigo_oc limit 1;
        if found then
            v_encontrado:='SI';
            update dte_recibidos set data_dte = put_data_dte(data_dte, 'CONTROLLER', put_json(get_xml('CONTROLLER', data_dte)::json, 'ok_rc', 'SI')::varchar) where codigo_txel = v_codigo_txel;
            update workflow_controller set xml2 = put_campo(put_campo(put_campo(xml2, 'DP_COD_OC', v_codigo_oc), 'DP_COD_RC', v_codigo_rc), 'DP_FOLIO_COMPROMISO', v_folio_compromiso) where id_solicitud = v_id_solicitud;
        else
            v_encontrado:='NO';
        end if;
    end if;

    if v_encontrado <> '' then
        datos_wf:=wf_avanza_solicitud(('{"wf_id_solicitud":"'||v_id_solicitud::varchar||'","rutCliente":"'||get_campo('RUT_RECEPTOR',v_campo.xml2)||'","rutUsuario":"99999999","perfil":"Automatico","decision":"ok_rcs_generado","aplicacion":"DTE","wf_id_pendiente":"'||idp1::varchar||'"}')::json);
    /*else
        datos_wf:=wf_avanza_solicitud(('{"wf_id_solicitud":"'||v_id_solicitud::varchar||'","rutCliente":"'||get_campo('RUT_RECEPTOR',v_campo.xml2)||'","rutUsuario":"99999999","perfil":"Automatico","decision"
:"nk_rcs","aplicacion":"DTE","wf_id_pendiente":"'||idp1::varchar||'"}')::json);*/
    end if;

    json2:=put_json(json2,'__SECUENCIAOK__','0');

    if (datos_wf is not null) then
        update dte_recibidos set data_dte=put_data_dte(data_dte,'WF_TAREA_ACTUAL',get_json('wf_desc_tarea_actual',datos_wf)) where codigo_txel=v_codigo_txel;
    end if;
    json2:=response_requests_6000('1', 'Completado Exitosamente, OC Recibidas: ' || get_json('num_oc', v_resp_oc) || ', RC Recibidas: ' || get_json('num_rc', v_resp_oc), '', json2);
    --json2:=response_requests_6000('1', '', '', json2);
    return json2;
end;
$function$ language plpgsql;






