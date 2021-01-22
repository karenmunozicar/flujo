delete from isys_querys_tx where llave='12822';
insert into isys_querys_tx values ('12822',10,1,2,'Arma el Json de consulta y lo envia',4013,300,101,0,0,20,20);
insert into isys_querys_tx values ('12822',20,1,1,'select procesa_resp_ms_12822(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);


CREATE OR REPLACE FUNCTION public.dp_consulta_contable_12822(json)
    RETURNS json
AS $function$
declare
    json1                     alias for $1;
    json2                     json;
    v_codigo_dv               bigint;
    v_codigo_txel             varchar;
    v_reg_devengo             record;
    v_reg_referencia          record;

    v_json_consulta           json;
begin
    json2:=json1;
    v_codigo_dv:=coalesce(nullif(get_json('codigo_dv', json2), ''), '0')::bigint;

    if v_codigo_dv = 0 then
        v_codigo_txel:=get_json('codigo_txel', json2);
        select codigo_dv into v_codigo_dv from dp_devengo where dte_codigo_txel = v_codigo_txel and tipo_dte <> 0 limit 1;
    end if;
    
    select codigo_dv, estado, area_transaccional, folio, ejercicio, ref_codigo_dv, tipo_dte, tipo_devengo from dp_devengo where codigo_dv = v_codigo_dv into v_reg_devengo;

    json2:=put_json(json2, 'codigo_dv', v_reg_devengo.codigo_dv::varchar);

    if v_reg_devengo.tipo_devengo in ('56', '61') then
        select codigo_dv, estado, area_transaccional, folio, ejercicio, ref_codigo_dv, tipo_dte, tipo_devengo from dp_devengo where codigo_dv = v_reg_devengo.ref_codigo_dv into v_reg_devengo;
    end if;

    if not found then
        json2:=response_requests_6000('2', 'Error en encontrar el devengo', '', json2);
        return json2;
    end if;

    if v_reg_devengo.estado <> 'FINALIZADO_SIN_ERRORES' then
        json2:=response_requests_6000('2', 'El devengo no esta FSE', '', json2);
        return json2;
    end if;

    if (v_reg_devengo.folio <> '') is not true then
        json2:=response_requests_6000('2', 'No se encuentra el folio SIGFE', '', json2);
        return json2;
    end if;

    v_json_consulta:=put_json('{}'::json, 'partida', substring(v_reg_devengo.area_transaccional, 1, 2));
    v_json_consulta:=put_json(v_json_consulta, 'capitulo', substring(v_reg_devengo.area_transaccional, 3, 2));
    v_json_consulta:=put_json(v_json_consulta, 'areaTransaccional', substring(v_reg_devengo.area_transaccional, 5, 3));
    v_json_consulta:=put_json(v_json_consulta, 'ejercicio', v_reg_devengo.ejercicio::varchar);
    v_json_consulta:=put_json(v_json_consulta, 'folio', v_reg_devengo.folio);

    -- Cargo los parametros para el flujo y paso el json del devengo
    json2:=put_json(json2,'LLAMA_FLUJO','SI');
    json2:=put_json(json2,'__SECUENCIAOK__','12822');
    json2:=get_parametros_motor_json(json2,'CONSULTA_CONTABLE_DIPRES_CHC');
    json2:=put_json(json2,'HOST_MS',get_json('__IP_CONEXION_CLIENTE__', json2));
    json2:=put_json(json2,'URI_MS',get_json('PARAMETRO_RUTA', json2));
    json2:=put_json(json2,'DATA_JSON',encode_hex(v_json_consulta::varchar));
    json2:=put_json(json2,'LARGO_JSON',(length(encode_hex(v_json_consulta::varchar))/2)::varchar);

    return json2;
end;
$function$ language plpgsql;


create or replace function procesa_resp_ms_12822(json)
    returns json
as $function$
declare
    json1              alias for $1;
    json2              json;

    v_codigo_dv        varchar;
    v_respuesta        varchar;
    json3              json;
    v_asiento          json;

begin
    json2:=json1;
    v_codigo_dv:=get_json('codigo_dv', json2);

    json3:='{}'::json;

    v_respuesta:=get_json('RESPUESTA',json2);
    if(strpos(v_respuesta,'HTTP/1.1 200')=0) then
        json3:=put_json(json3,'MENSAJE_VACIO','Error conexion servicio');
        json2:=response_requests_6000('1', 'OK', json3::varchar, json2);
        return json2;
    end if;

    BEGIN
        v_respuesta:=split_part(v_respuesta,chr(10)||chr(10),2);
    EXCEPTION WHEN OTHERS THEN
        json3:=put_json(json3,'MENSAJE_VACIO','Error al leer respuesta servicio');
        json2:=response_requests_6000('1', 'OK', json3::varchar, json2);
        return json2;
    END;

    if get_json('CODIGO_RESPUESTA', v_respuesta::json) = '1' then
        -- FGE - Proxima parte a implementar NC/ND con asiento automático
        json3:=put_json(json3,'FGE - 12822 - Analiza Respuesta');
    end if;
        
    json2:=logjson(json2, 'Respuesta 12822 v_respuesta: ' || v_respuesta);

    v_asiento:=v_respuesta::json;
    perform logfile('-- FGE - 12822 v_asiento: ' || v_asiento::varchar);

    return grilla_obtiene_compromiso_chile_compras(json2);
end;
$function$ language plpgsql;

    


