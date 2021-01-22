delete from isys_querys_tx where llave='12821';
insert into isys_querys_tx values ('12821',10,1,2,'Consulta OC',4013,300,101,0,0,20,20);
insert into isys_querys_tx values ('12821',20,1,1,'select procesa_resp_ms_12821(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

create or replace function pivote_rcs_12821(json)
    returns json
    as $function$
declare
    json1           alias for $1;
    json2           json;
    v_codigo_txel   bigint;
    v_id_solicitud  bigint;
    v_id_solicitud1 varchar;
    v_oc            varchar;
    v_json_post     json;

/*    id1	           bigint;
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

    v_referencias  json;
    v_index        integer;
    v_referencia   varchar;
    v_codigo_oc    varchar;

   emensaje1       varchar;
    mensaje2       varchar;
    mensaje_html   varchar;
    acciones1      varchar;
    evento_ori1    varchar;
    v_uri          varchar;
*/
    
begin
    json2:=json1;
    v_codigo_txel := replace(get_json('COD_TXEL', json2), '.', '')::bigint;
    select get_xml('ID_SOLICITUD_WF', data_dte) into v_id_solicitud1 from dte_recibidos where codigo_txel = v_codigo_txel;
    if is_number(v_id_solicitud1) then
	v_id_solicitud:=v_id_solicitud1::bigint;
    else
        return response_requests_6000('2', 'Falla ID_SOLICITUD_WF.', '', json2);
    end if;
    select dp_obtiene_oc_flujo(v_id_solicitud::varchar) into v_oc;
    v_json_post:=put_json('{}', 'OC', v_oc);

    if coalesce(v_oc, '') <> '' then
        -- Cargo los parametros para el flujo y paso el json del devengo
        json2:=put_json(json2,'LLAMA_FLUJO','SI');
        json2:=put_json(json2,'__SECUENCIAOK__','12821');
        json2:=get_parametros_motor_json(json2,'DIPRES_OC');
        json2:=put_json(json2,'HOST_MS','servicios.acepta.com');
        json2:=put_json(json2,'URI_MS','chilecompra/consultaoc');
        json2:=put_json(json2,'DATA_JSON',encode_hex(v_json_post::varchar));
        json2:=put_json(json2,'LARGO_JSON',(length(encode_hex(v_json_post::varchar))/2)::varchar);
    else
        json2:=response_requests_6000('2', 'Referencia 801 Codigo OC no encontrada.', '', json2);
    end if;

    return json2;

end;
$function$ language plpgsql;

create or replace function procesa_resp_ms_12821(json)
    returns json
    as $function$
declare
    json1               alias for $1;
    json2               json;
    json3               json;
    
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
    v_resp_oc           json;
begin
    json2:=json1;
    
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

    if length(get_json('Listado', v_respuesta::json)) > 3 then
        --perform mp_ingresa_oc(v_respuesta::json);
	v_resp_oc:=mp_ingresa_oc(v_respuesta::json);
	json2:=response_requests_6000('1', 'Se Recibió información desde MP.  OC Recibidas: ' || get_json('num_oc', v_resp_oc) || ', RC Recibidas: ' || get_json('num_rc', v_resp_oc), '', json2);
	--json2:=response_requests_6000('1', '', '', json2);
    else
        json2:=response_requests_6000('1', 'No se Encontró la OC.', '', json2);
    end if;

    return json2;
end;
$function$ language plpgsql;





