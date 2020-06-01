delete from isys_querys_tx where llave='15300';
insert into isys_querys_tx values ('15300',10,22,1,'$$QUERY_RS$$',0,0,0,9,1,20,0);
insert into isys_querys_tx values ('15300',20,9,1,'select procesa_resp_rs_15300(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);


create or replace function privote_detalle_oc_proveedores(json)
 returns json
 as $function$
declare
        json1 alias for $1;
        json2 json;
        json3 json;

        codigo_txel1 varchar;
        rut_emisor_oc varchar;
        folio_oc varchar;
        query_rs varchar;
        query_local varchar;
begin
        json2:=json1;
        json2:=put_json(json2,'LLAMA_FLUJO','SI');
        json2:=put_json(json2,'__SECUENCIAOK__','15300');
        json2:=put_json(json2,'TABLA_OC','dte_recibidos');
        json2:=put_json(json2,'PARAMETROS_OC','codigo_txel as info_sin_formato__codigo_txel__off, folio::varchar||''__''||uri as link__folio__on, tipo_dte as info__tipo_dte__on, monto_total as info__monto__on, estado_sii as info__estado__on');


        codigo_txel1:=replace(get_json('CODIGO_TXEL',json2),'.','');
        rut_emisor_oc:=replace(split_part(get_json('RUT_EMISOR',json2),'-',1),'.','');
        folio_oc:=replace(get_json('FOLIO_OC',json2),'.','');

        query_rs:='select codigo_txel from '||get_json('TABLA_OC',json2)||' where dia_emision<>'||to_char(now(),'YYYYMMDD')||' and rut_receptor='||rut_emisor_oc||' and strpos(referencias::varchar,''"Tipo":"801","Folio":"'||folio_oc||'"'')>0';
        query_local:='select '||get_json('PARAMETROS_OC',json2)||' from '||get_json('TABLA_OC',json2)||' where dia_emision='||to_char(now(),'YYYYMMDD')||' and rut_receptor='||rut_emisor_oc||' and strpos(referencias::varchar,''"Tipo":"801","Folio":"'||folio_oc||'"'')>0';

        json2:=logjson(json2,'[get_detalle_oc_proveedores] Ejecuto QUERY_RS='||query_rs);

        json2:=put_json(json2,'QUERY_RS',query_rs);
        json2:=put_json(json2,'QUERY_LOCAL',query_local);
        json2:=response_requests_6000('1', 'OK', '', json2);
        return json2;
end;
$function$ language plpgsql;

create or replace function procesa_resp_rs_15300(json)
 returns json
 as $function$
declare
        json1 alias for $1;
        json2 json;
        json3 json;

        codigo_txel1 varchar;
        rut_emisor_oc varchar;
        folio_oc varchar;
        query_rs varchar;
        query_local varchar;
        resp_rs varchar;
        codigos_txel varchar:='';
        lista1 varchar;
        aux varchar;
        i integer:=0;
        query_final varchar;

        select1 varchar:='';
begin
        json2:=json1;
        json2:=logjson(json2,'json incio=>'||json2::varchar);

        json3:='{}';
        json3:=put_json(json3,'flag_paginacion','SI');
        json3:=put_json(json3,'flag_paginacion_manual','NO');
        json3:=put_json(json3,'flag_tipo_cuadro','GRILLA');
        json3:=put_json(json3,'registros_por_pagina','10');
        json3:=put_json(json3,'MENSAJE_VACIO','No hay registros');

        resp_rs:=get_json('RES_JSON_1',json2);
        if resp_rs<>'' then
                if get_json('STATUS',resp_rs::json)='OK' then
                        if get_json('TOTAL_REGISTROS',resp_rs::json)='1' then
                                codigos_txel:=get_json('codigo_txel',resp_rs::json);
                        else
                                lista1:=get_json('LISTA',resp_rs::json);
                                aux:=get_json_index(lista1::json,i);
                                while(aux<>'') loop
                                        if codigos_txel<>'' then
                                                codigos_txel:=codigos_txel||','||get_json('codigo_txel',aux::json);
                                        else
                                                codigos_txel:=get_json('codigo_txel',aux::json);
                                        end if;
                                        i:=i+1;
                                        aux:=get_json_index(lista1::json,i);
                                end loop;
                        end if;
                end if;
        end if;
        json2:=logjson(json2,'[procesa_resp_rs_15300] codigos_txel RS='||codigos_txel);
        query_final:=get_json('QUERY_LOCAL',json2);
        if codigos_txel<>'' then
                query_final:=query_final||' union all select '||get_json('PARAMETROS_OC',json2)||' from '||get_json('TABLA_OC',json2)||' where codigo_txel in ('||codigos_txel||')';
        end if;
        json2:=logjson(json2,'[procesa_resp_rs_15300] query final='||query_final);
        begin
                execute 'select array_to_json(array_agg(row_to_json(sql))) from ('||query_final||')sql' into select1;
        exception when others then
                json2:=logjson(json2,'[procesa_resp_rs_15300] error al ejecutar query= select array_to_json(array_agg(row_to_json(sql))) from ('||coalesce(query_final,'<<NULL>>')||')sql');
        end;

        json3:=put_json(json3,'datos_tabla', select1::varchar);
        json2:=response_requests_6000('1', 'OK', json3::varchar, json2);
        return json2;
end;
$function$ language plpgsql;

