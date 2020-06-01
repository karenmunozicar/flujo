delete from isys_querys_tx where llave='25101';

insert into isys_querys_tx values ('25101','10',9,1,'select arma_filtros_25101(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);
--Pivote
insert into isys_querys_tx values ('25101','15',9,1,'select pivote_25101(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('25101','20',44,1,'$$QUERY_DATA$$',0,0,0,9,1,15,15);

CREATE or replace FUNCTION pivote_25101(json) RETURNS json AS $$
declare

        json1                alias for $1;
        json2                   json;
        json5                   json;
        query1  varchar;
        query2  varchar;
        select_vars1    varchaR;
        v_total integer;
        sec1            integer;
        v_out_resultado varchar;
        crit_busq1      varchar;
        aux1    varchar;
begin
        json2:=json1;
        sec1:=get_json('CONTADOR_SECOK',json2)::integer+10;
        json2:=put_json(json2,'CONTADOR_SECOK',sec1::varchar);
        if (get_json('order_excel',json2)='SI') then
                json2:=put_json(json2,'__TIMEOUT_SERV_PXML__','10');
        else
                json2:=put_json(json2,'__TIMEOUT_SERV_PXML__','3');
        end if;
        --Remplazo QUERY_DATA con QUERY_DATA_HEX porque el motor borra las comillas simples
        json2:=put_json(json2,'QUERY_DATA',decode_hex(get_json('QUERY_DATA_HEX',json2)));
        --Si viene resultado
        json2:=logjson(json2,'RES_JSON_1 '||get_json('RES_JSON_1',json2));
        if (get_json('TOTAL_RES_JSON',json2)<>'1') then
                json2:=logjson(json2,get_json('TAG_MENSAJE',json2)||' Falla Consulta');
                --Si falla vamos a indicar la falla en el mensaje
                json2:=put_json(json2,'MENSAJE_RESPUESTA',get_json('MENSAJE_RESPUESTA',json2)||'<br>'||get_json('MENSAJE_ERROR',json2));
        else
                --v_out_resultado:=get_json('LISTA',get_json('RES_JSON_1',json2)::json);
                v_out_resultado:=put_json_list('[]',replace(get_json('RES_JSON_1',json2),', "STATUS": "OK", "TOTAL_REGISTROS": "1"',''));
                --Si viene data
                if v_out_resultado<>'' then
                        json2:=logjson(json2,get_json('TAG_MENSAJE',json2)||' Concateno Registros');
                        --Si esta vacio lo inicializo, sino agrego
			json2:=put_json(json2,'v_out_resultado',v_out_resultado::varchar);
                else
                        json2:=logjson(json2,get_json('TAG_MENSAJE',json2)||' Sin Data');
                end if;
        end if;
        json2:=put_json(json2,'TOTAL_RES_JSON','');
        json2:=put_json(json2,'RES_JSON_1','');
        --Saco la anterior

        if (get_json('CONTADOR_SECOK',json2)='30') then
                json2:=put_json(json2,'__SECUENCIAOK__','0');
                 --Si no es nulo el resultado, cuente.
                if (get_json('v_out_resultado',json2)='') then
                        v_total:=0;
                        json2:=put_json(json2,'CODIGO_RESPUESTA','2');
                        json2:=put_json(json2,'MENSAJE_RESPUESTA','No se encontro el Documento');
                        json2:=responde_pantalla_25100(json2);
                        return json2;
                else
                        v_total:=count_array_json(get_json('v_out_resultado',json2)::json);
                end if;
                --Criterios de busqueda para reportes
                crit_busq1:=' <b>Folio=</b>'||get_json('FOLIO',json2);

                if (length(get_json('texto_filtro_params',json2))>0) then
                        crit_busq1:=crit_busq1||'<b>Parametros </b>'||get_json('texto_filtro_params',json2);
                end if;
                json2:=logjson(json2,'Total Resultados '||v_total::varchar);
                json2:=put_json(json2,'v_total_registros',v_total::varchar);
                json2:=put_json(json2,'v_in_offset',get_json('v_in_offset',json2));
                json2:=put_json(json2,'criterio_busqueda_excel',crit_busq1);
                json2:=put_json(json2,'CODIGO_RESPUESTA','1');
                json2:=logjson(json2,'MENSAJE='||get_json('MENSAJE_RESPUESTA',json2));
                json2:=put_json(json2,'MENSAJE',get_json('MENSAJE_RESPUESTA',json2));
                json2:=put_json(json2,'MENSAJE_RESPUESTA','OK');
                json2:=responde_pantalla_25100(json2);
                return json2;
        end if;

        return json2;
end
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION arma_filtros_25101(json) RETURNS json AS $$
declare

        json1                alias for $1;
        json2                   json;

        rol1            varchar;
        rut_usu1        varchar;
        cod1            varchar;
        query1          varchar;
        select_vars1    varchaR;
BEGIN
        json2:=json1;
        rol1:=get_json('rol_usuario',json2);
        rut_usu1:=get_json('rutUsuario',json2);
        cod1:=get_json('CODDOCUMENTO',json2);

        json2:=get_campos_busqueda_dec_25100(json2);
        select_vars1:=get_json('__campos_busqueda__',json2);

        --query1:=select_vars1||' from dc4_Documento where coddocumento='''||cod1||'''';
        query1:=select_vars1||' from (select x.*,y.desc_tipo as nombre_documento from (select * from dc4_Documento where coddocumento = '''||cod1||''' and Institucion='''||get_json('institucion_dec',json2)||''') x left join dc4_TipoDocto y on x.CodTipo=y.CodTipo and x.Institucion=y.Institucion) z';
        json2:=put_json(json2,'__TOTAL_RESP_ESPERADAS__','1');

        if (get_json('order_excel',json2)='SI') then
                json2:=put_json(json2,'__TIMEOUT_SERV_PXML__','10');
        else
                json2:=put_json(json2,'__TIMEOUT_SERV_PXML__','3');
        end if;
        json2:=logjson(json2,'QUERY='||query1::varchar);
        json2:=put_json(json2,'QUERY_DATA',query1);
        json2:=put_json(json2,'__SECUENCIAOK__','20');
        json2:=put_json(json2,'CONTADOR_SECOK','20');
        return json2;
end
$$ LANGUAGE plpgsql;

