delete from isys_querys_tx where llave='12818';
insert into isys_querys_tx values ('12818',10,1,2,'Envia data chile compras',4013,300,101,0,0,20,20);
insert into isys_querys_tx values ('12818',20,1,1,'select procesa_resp_ms_12818(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

create or replace function procesa_resp_ms_12818(json)
 returns json
 as $function$
declare
        json1                            alias for $1;
        json2                            json;
        json3                            json;
        json_respuesta                   json;
        json_requerimiento               varchar;
        index_json                       integer;
        v_codigo_dv                      integer;
        resp1                            varchar;

        v_folio                          varchar;
        v_tipo_dte                       varchar;
        v_rut_receptor                   varchar;
        v_rut_emisor                     varchar;
        v_id_documento_ajustado          varchar;
        v_id_agrupacion_ajustada         varchar;
        v_saldo                          varchar;

        v_reg_devengo                    record;

        err_msg         varchar:='';
        msj_defecto     varchar;
        pg_context      TEXT;
begin
        json2:=json1;
        json2:=logjson(json2,'[procesa_resp_ms_12818] json2 entrada=>'||json2::varchar);
        v_codigo_dv:=get_json('codigo_dv', json2)::integer;

        json3:='{}';

        resp1:=get_json('RESPUESTA',json2);
        if(strpos(resp1,'HTTP/1.1 200')=0) then
                json3:=put_json(json3,'MENSAJE_VACIO','Error conexion servicio');
                json2:=response_requests_6000('1', 'OK', json3::varchar, json2);
                return json2;
        end if;

        BEGIN
                resp1:=split_part(resp1,chr(10)||chr(10),2);
        EXCEPTION WHEN OTHERS THEN
                json3:=put_json(json3,'MENSAJE_VACIO','Error al leer respuesta servicio');
                json2:=response_requests_6000('1', 'OK', json3::varchar, json2);
                return json2;
        END;
        json2:=logjson(json2,'[procesa_resp_ms_12818] resp1='||resp1);
        resp1:=procesa_json_12818(resp1);


        if get_json('folio',resp1::json) <> '' then
                v_folio := get_json('folio', resp1::json);
                v_rut_receptor := replace(split_part(get_json('rut_receptor', resp1::json), '-', 1), '.', '');
                v_rut_emisor := get_json('rutCliente', json2);
                v_tipo_dte := get_json('tipo', resp1::json);
                v_id_documento_ajustado := get_json('id_documento_ajustado', resp1::json);
                v_id_agrupacion_ajustada := get_json('id_agrupacion_ajustada', resp1::json);
                v_saldo := get_json('saldo', resp1::json);

                select codigo_dv from dp_devengo where dte_codigo_txel = (select codigo_txel from dte_recibidos where rut_emisor = v_rut_receptor::integer and rut_receptor = v_rut_emisor::integer and folio = v_folio::bigint and tipo_dte = v_tipo_dte::integer)::varchar into v_reg_devengo;
                json2:=logjson(json2,'[Devengo Referencia] => ' || v_reg_devengo::varchar);

                if found then
                        update dp_devengo set id_documento_ajustado = v_id_documento_ajustado, id_agrupacion_ajustada = v_id_agrupacion_ajustada, saldo = v_saldo::bigint where codigo_dv = v_reg_devengo.codigo_dv;
                        json2:=put_json(json2, 'SALDO_DEVENGO', v_saldo);
                else
                        json3:=put_json(json3,'REFERENCIA_VACIA','No se encuentra el devengo de Referencia');
                        json2:=response_requests_6000('2', 'NK', json3::varchar, json2);
                        return json2;
                end if;

        else
                json3:=put_json(json3,'REFERENCIA_VACIA','No se encuentra el devengo de Referencia');
                json2:=response_requests_6000('2', 'NK', json3::varchar, json2);
                return json2;

        end if;
        json2:=response_requests_6000('1', 'OK', json3::varchar, json2);
        return json2;
end;
$function$ language plpgsql;


create or replace function procesa_json_12818(json_in text)
    returns text
as $function$
import json
json_out={}

tipo_dte = {'0102': 33, '0202': 34, '0402': 61, '0502': 56 }

lista=[]
json1=json_in
json1=json1.decode('ascii', 'ignore')
json1=json.loads(json_in, strict = 'false')
json_out = {}
id_documento_ajusto = None
if 'devengo' in json1:
    for documento in json1['devengo']['documentos']['documento']:
        json_out['folio'] = documento['numero']
        json_out['tipo'] = tipo_dte[documento['tipo']]
        json_out['id_documento_ajustado'] = documento['idDocumentoAjustado']
        for principal in documento['principales']['principal']:
            json_out['rut_receptor'] = principal['id']
            for transaccion in principal['transaccionesPrevias']['transaccion']:
                json_out['id_agrupacion_ajustada'] = transaccion['idAgrupacionAjustada']
                #json_out['imputaciones'] = []
                saldo = 0
                for agrupacion in transaccion['agrupacionesDeImputacionesACatalogos']['agrupacion']:
                    for imputacion in agrupacion['imputacionesAConceptosPresupuestarios']['imputacion']:
                #        json_out['imputaciones'].append({'codigo': imputacion['codigo'], 'saldo': imputacion['saldo']})
                         saldo += int(imputacion['saldo'])
                json_out['saldo'] = saldo
return json.dumps(json_out)
$function$ language plpythonu;

