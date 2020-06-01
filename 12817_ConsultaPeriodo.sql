delete from isys_querys_tx where llave='12817';
insert into isys_querys_tx values ('12817',10,1,2,'Consulta de periodo sigfe',4013,300,101,0,0,20,20);
insert into isys_querys_tx values ('12817',20,1,1,'select procesa_resp_ms_12817(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

create or replace function dp_consulta_periodo(json)
    returns json
    as $function$
declare
    json1                  alias for $1;
    json2                  json;
  
    v_area_transaccional   varchar;
    v_json_periodo         varchar;

begin
    json2:=json1;
    v_area_transaccional:=get_json('area_transaccional', json2);

 
    if v_area_transaccional = '' then
        json2:=response_requests_6000('2', 'Sin Área Transaccional', '', json2);
        return json2;
    end if;

    v_json_periodo:=put_json('{}', 'areaTrx', v_area_transaccional);

    json2:=put_json(json2,'LLAMA_FLUJO','SI');
    json2:=put_json(json2,'__SECUENCIAOK__','12817');
    json2:=get_parametros_motor_json(json2,'CONSULTAPERIODO_CHC');
    json2:=put_json(json2,'HOST_MS','servicios.acepta.com');
    json2:=put_json(json2,'URI_MS','chilecompra/obtieneperiodo');
    json2:=put_json(json2,'DATA_JSON',encode_hex(v_json_periodo::varchar));
    json2:=put_json(json2,'LARGO_JSON',(length(encode_hex(v_json_periodo::varchar))/2)::varchar);

    json2:=response_requests_6000('1', 'XYZ', '', json2);
    return json2;
end;
$function$ language plpgsql;

create or replace function procesa_resp_ms_12817(json)
    returns json
    as $function$
declare
    json1            alias for $1;
    json2            json;
    json3            json;

    v_respuesta      varchar;
begin
    json2:=json1;

    v_respuesta:=get_json('RESPUESTA',json2);
    if(strpos(v_respuesta,'HTTP/1.1 200')=0) then
        json2:=put_json(json2, 'PERIODO_SIGFE', '');
        return json2;
    end if;

    BEGIN
        v_respuesta:=split_part(v_respuesta,chr(10)||chr(10),2);
    EXCEPTION WHEN OTHERS THEN
        json2:=put_json(json2, 'PERIODO_SIGFE', '');
        return json2;
    END;

    json2:=put_json(json2, 'PERIODO_SIGFE', v_respuesta);
    return json2;
end;
$function$ language plpgsql;




