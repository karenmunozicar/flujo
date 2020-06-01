delete from isys_querys_tx where llave='12816';
insert into isys_querys_tx values ('12816',10,1,2,'Consulta de Estado de un devengo',4013,300,101,0,0,20,20);
insert into isys_querys_tx values ('12816',20,1,1,'select procesa_resp_ms_12816(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

create or replace function dp_consulta_ticket(json)
    returns json
    as $function$
declare
    json1                  alias for $1;
    json2                  json;

    v_codigo_dv            varchar;
    v_reg_devengo          record;
    v_json_ticket          json;
begin
    json2:=json1;
    v_codigo_dv:=get_json('codigo_dv', json2);
   
 
    if v_codigo_dv = '' then
        json2:=response_requests_6000('2', 'El devengo no existe.', '', json2);
        return json2;
    end if;

    select codigo_dv, ticket_id, ejercicio, area_transaccional, estado from dp_devengo where codigo_dv = v_codigo_dv::bigint into v_reg_devengo;

    if v_reg_devengo.estado = 'BORRADOR' then
        json2:=response_requests_6000('2', 'El devengo todavía no está generado.', '', json2);
        return json2;
    end if;

    v_json_ticket:='{}';
    v_json_ticket:=put_json(v_json_ticket, 'ticketEnvio', v_reg_devengo.ticket_id);
    v_json_ticket:=put_json(v_json_ticket, 'codigoAreaTx', v_reg_devengo.area_transaccional);
    v_json_ticket:=put_json(v_json_ticket, 'ejercicio', v_reg_devengo.ejercicio::varchar);

 
    -- Cargo los parametros para el flujo y paso el json del devengo
    json2:=put_json(json2,'LLAMA_FLUJO','SI');
    json2:=put_json(json2,'__SECUENCIAOK__','12816');
    json2:=get_parametros_motor_json(json2,'CONSULTATICKET_CHC');
    json2:=put_json(json2,'HOST_MS','servicios.acepta.com');
    json2:=put_json(json2,'URI_MS','chilecompra/estadoticket');
    json2:=put_json(json2,'DATA_JSON',encode_hex(v_json_ticket::varchar));
    json2:=put_json(json2,'LARGO_JSON',(length(encode_hex(v_json_ticket::varchar))/2)::varchar);

    json2:=response_requests_6000('2', 'Existe un problema con los servicios de consulta.', '', json2);
    return json2;

end;
$function$ language plpgsql;

create or replace function procesa_resp_ms_12816(json)
    returns json
    as $function$
declare
    json1            alias for $1;
    json2            json;
    json3            json;
    v_codigo_dv      varchar;
    v_respuesta      varchar;
    v_estado_sigfe   varchar;
    v_detalle_error  varchar;
    v_mensaje_error  varchar;

    v_reg_devengo  record;
begin
    json2:=json1;
    v_codigo_dv:=get_json('codigo_dv', json2);

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

    v_estado_sigfe:=get_json('estado', v_respuesta::json);

    --update dp_devengo set estado = v_estado_sigfe where codigo_dv = v_codigo_dv::bigint;

    if v_estado_sigfe = 'FINALIZADO_CON_ERRORES' then
        v_detalle_error:=get_json('transaccionVerticalesType', v_respuesta::json);
        v_detalle_error:=get_json_index(v_detalle_error::json, 0);
        v_detalle_error:=get_json('detalleErrorVerticalesType', v_detalle_error::json);
        v_detalle_error:=get_json_index(v_detalle_error::json, 0);
        v_mensaje_error:='Validación: ' || get_json('validacion', v_detalle_error::json) || '. Descripción: ' || get_json('descripcion', v_detalle_error::json);
        --update dp_devengo set error_externo = v_mensaje_error where codigo_dv = v_codigo_dv::bigint;
    end if;
    
    if v_estado_sigfe = 'FINALIZADO_SIN_ERRORES' then
        v_detalle_error:=get_json('transaccionVerticalesType', v_respuesta::json);
        v_detalle_error:=get_json_index(v_detalle_error::json, 0);
        v_mensaje_error:='Folio: ' || get_json('folio', v_detalle_error::json) || '. Tranferencia: ' || get_json('idTransferenia', v_detalle_error::json); 
    end if;

    if v_estado_sigfe<>'' then
    	perform dp_act_estado_devengo(v_codigo_dv::bigint, v_estado_sigfe, v_mensaje_error);
    end if;

    json2:=response_requests_6000('1', 'Estado: ' || v_estado_sigfe || '. ' || v_mensaje_error, '', json2);
    return json2;
end;
$function$ language plpgsql;




