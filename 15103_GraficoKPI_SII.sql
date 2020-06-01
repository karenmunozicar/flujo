delete from isys_querys_tx where llave='15103';
CREATE or replace FUNCTION pivote_grafico_sii_15103(json) RETURNS json AS $$
DECLARE
        json2   json;
        json1   alias for $1;
        v_fecha_inicio      integer;
        v_fecha_fin         integer;
        fecha_in1       varchar;
        json3       json;
        json4       json;
        json5       json;
        texto_ref1      varchar;
BEGIN
        json2:=json1;
        json2:=put_json(json2,'LLAMA_FLUJO','SI');
        json2:=put_json(json2,'__SECUENCIAOK__','15103');
        return json2;
END;
$$ LANGUAGE plpgsql;

insert into isys_querys_tx values ('15103','10',9,1,'select arma_query_grafico_15103(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0); 
--Redshift
insert into isys_querys_tx values ('15103','20',23,1,'$$QUERY_RS$$',0,0,0,9,1,50,50);
--Local
insert into isys_querys_tx values ('15103','30',9,1,'$$QUERY_RS$$',0,0,0,9,1,50,50);
--Junto resultados del contado
insert into isys_querys_tx values ('15103','50',9,1,'select resultado_grafico_15103(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);


CREATE or replace FUNCTION arma_query_grafico_15103(json) RETURNS json AS $$
DECLARE
    json1               alias for $1;
    json2               json;
    json_out            json;
    json_series_x               json;
    json_series_y               json;
    rut1        varchar;
        i       integer;
        filtro1 varchar;
        campo record;
        fecha_ini1 varchar;
        fecha_fin1 varchar;
        fs1     varchar;
        fe1     varchar;
        empresas1 varchar;
        json_emp1 json;
        filtro_emp      varchar;
        aux1    varchar;
        filtro2 varchar;
        rut_ind1        varchar;
        json_total1     json;
        json_enviados1  json;
        json_respondidos1       json;
        series1 json;
        series2 json;
        json_data1      json;
        total1  bigint;
        total_respondidos1      bigint;
                total_norespondidos1    bigint;
        json_aux1       json;
        lista1  json;
        json_norespondidos1     json;
        dia1    integer;
        salida1 json;
        total_final1    bigint;
        --inter1                interval;
        json_point1     json;
        color1          varchar;
        dia2    varchar;
        json_par1       json;
        json3   json;
        json_rut1       json;
        v_parametro_rut_emisor  varchar;
        flag_agrega1    boolean;

begin
	json2:=json1;

        dia2:=replace(get_json('DIA',json2),'-','');
        --Obtengo el Dia
        if (is_number(dia2) is false) then
                dia1:=to_char(now(),'YYYYMMDD');
        else
                dia1:=dia2::integer;
        end if;
        dia2:=substring(dia1::varchar,7,2)||'/'||substring(dia1::varchar,5,2)||'/'||substring(dia1::varchar,1,4);
	json2:=put_json(json2,'DIA_DISPLAY',dia2::varchar);

        fecha_ini1=to_char(now(),'HH24MI');


        if (get_json('rol_usuario',json2)='Sistemas' and get_json('rutCliente',json2)='96919050') then
                v_parametro_rut_emisor:=' 1=1 ';
        else
                json_rut1:=obtiene_filtro_perfilamiento_rut_emisor_6000(get_json('rutCliente',json2)::integer,get_json('rutUsuario',json2)::integer,'rut_emisor','*');
                v_parametro_rut_emisor:=get_json('TAG_RUT_EMISOR',json_rut1);
        end if;

        ----perform logfile('v_parametro_rut_emisor='||v_parametro_rut_emisor);
        if (dia1=to_char(now(),'YYYYMMDD')::integer) then
		json2:=put_json(json2,'QUERY_RS','select count(*) as total,to_char(fecha_ingreso,''HH24'')||'':''||lpad(((to_char(fecha_ingreso,''MI'')::integer/15)*15)::varchar,2,''00'') as fecha,extract (''epoch'' from avg(fecha_sii-fecha_ingreso))::integer as promedio,estado_sii from dte_emitidos where dia='||dia1||' and '||v_parametro_rut_emisor||'  group by 2,4 order by 2');
		json2:=put_json(json2,'__SECUENCIAOK__','30');
        else
                --Vamos al redshift a consultar
		json2:=put_json(json2,'QUERY_RS','select count(*) as total,to_char(fecha_ingreso,''HH24'')||'':''||lpad(((to_char(fecha_ingreso,''MI'')::integer/15)*15)::varchar,2,''00'') as fecha,(avg(fecha_sii-fecha_ingreso)/1000)/1000 as promedio,estado_sii from dte_emitidos where dia='||dia1::varchar||' and '||v_parametro_rut_emisor||' group by 2,4 order by 2');
		json2:=put_json(json2,'__SECUENCIAOK__','20');
        end if;
	return json2;
end;
$$
LANGUAGE plpgsql;

CREATE or replace FUNCTION resultado_grafico_15103(json) RETURNS json AS $$
DECLARE
        json2   json;
        json1   alias for $1;
    json_out            json;
    json_series_x               json;
    json_series_y               json;
    rut1        varchar;
        i       integer;
        filtro1 varchar;
        campo record;
        fecha_ini1 varchar;
        fecha_fin1 varchar;
        fs1     varchar;
        fe1     varchar;
        empresas1 varchar;
        json_emp1 json;
        filtro_emp      varchar;
        aux1    varchar;
        filtro2 varchar;
        rut_ind1        varchar;
        json_total1     json;
        json_enviados1  json;
        json_respondidos1       json;
        series1 json;
        series2 json;
        json_data1      json;
        total1  bigint;
        total_respondidos1      bigint;
                total_norespondidos1    bigint;
        json_aux1       json;
        lista1  json;
        json_norespondidos1     json;
        dia1    integer;
        salida1 json;
        total_final1    bigint;
        --inter1                interval;
        json_point1     json;
        color1          varchar;
        dia2    varchar;
        json_par1       json;
        json3   json;
        json_rut1       json;
        v_parametro_rut_emisor  varchar;
        flag_agrega1    boolean;

BEGIN
        json2:=json1;
	perform logfile('RES_JSON_1='||get_json('RES_JSON_1',json2));
	if (get_json('RES_JSON_1',json2)='') then
		return response_requests_6000('2','Reintente por favor*','',json2);
	end if;
	if (get_json('STATUS',get_json('RES_JSON_1',json2)::json)<>'OK') then
		return response_requests_6000('2','Reintente por favor**','',json2);
	end if;

	--Si viene un registro
	if (get_json('TOTAL_REGISTROS',get_json('RES_JSON_1',json2)::json)='1') then
		json_data1:=put_json_list('[]',get_json('RES_JSON_1',json2));
	else
		json_data1:=get_json('LISTA',get_json('RES_JSON_1',json2)::json)::json;
	end if;
        salida1:='[]';
        json_out:='{}';
        json_out:=put_json(json_out,'TITULO','Envíos al SII');
        json_out:=put_json(json_out,'SUBTITULO','Comportamiento de Respuestas del '||get_json('DIA_DISPLAY',json2));
        series1:='[]';
        series2:='[]';
        json_total1:=put_json('{}','name','No Entregados');
        json_total1:=put_json(json_total1,'data','[]');
        json_norespondidos1:=put_json('{}','name','Sin Respuesta');
        json_norespondidos1:=put_json(json_norespondidos1,'data','[]');
        json_respondidos1:=put_json('{}','name','Respondidos');
        json_respondidos1:=put_json(json_respondidos1,'data','[]');
        ----raise notice 'paso2';

        i:=0;
        flag_agrega1:=false;
        for campo in select to_char(generate_series,'HH24:MI') as fecha from generate_series(date_trunc('day',now()),date_trunc('day',now())+interval '23 hours 45 minutes','15 minutes') loop
                series2:=put_json_list(series2,'XX'||campo.fecha::varchar);
                --Saco el primer valor
                raise notice 'Fecha %',campo.fecha;
                total1:=0;
                total_respondidos1:=0;
                total_norespondidos1:=0;
                raise notice 'json_data1=%',json_data1;
                aux1:=get_json_index(json_data1,i);
                while aux1<>'' loop
                        raise notice 'aux1=%',aux1;
                        json_aux1:=aux1::json;
                        --Si corresponde la fecha actual
                        if (campo.fecha=get_json('fecha',json_aux1)) then
                                raise notice 'estado_sii=%',get_json('estado_sii',json_aux1);
                                if (get_json('estado_sii',json_aux1) in ('ACEPTADO_CON_REPAROS_POR_EL_SII','ACEPTADO_POR_EL_SII','RECHAZADO_POR_EL_SII')) then
                                        total_respondidos1:=total_respondidos1+get_json('total',json_aux1)::integer;
                                        --raise notice 'total_respondidos1=%',total_respondidos1;
                                elsif (get_json('estado_sii',json_aux1) in ('ENVIADO_AL_SII')) then
                                        total_norespondidos1:=total_norespondidos1+get_json('total',json_aux1)::integer;
                                        --raise notice 'total_norespondidos1=%',total_norespondidos1;
                                else
                                        total1:=total1+get_json('total',json_aux1)::integer;
                                        --raise notice 'total1=%',total1;
                                end if;
                                flag_agrega1:=false;
                        else
                                flag_agrega1:=true;
                                --Agergo el valor
                                lista1:=get_json('data',json_total1)::json;
                                --perform logfile('GRAFICO2 lista1='||lista1::varchar);
                                lista1:=put_json_list_int(lista1,total1);
                                --perform logfile('GRAFICO1 lista1='||lista1::varchar);
                                json_total1:=put_json(json_total1,'data',lista1::varchar);
                                --perform logfile('GRAFICO1 json_total1='||json_total1::varchar);

                                raise notice 'agrega';
                                lista1:=get_json('data',json_respondidos1)::json;
                                lista1:=put_json_list_int(lista1,total_respondidos1);
                                json_respondidos1:=put_json(json_respondidos1,'data',lista1::varchar);

                                lista1:=get_json('data',json_norespondidos1)::json;
                                lista1:=put_json_list_int(lista1,total_norespondidos1);
                                json_norespondidos1:=put_json(json_norespondidos1,'data',lista1::varchar);
                                --Salgo de recorrer la data y voy por la siguiente hora
                                exit;
                        end if;
                        i:=i+1;
                        aux1:=get_json_index(json_data1,i);
                end loop;

                --Sino agrego, agrego el 0
                if (flag_agrega1 is false) then
                        lista1:=get_json('data',json_total1)::json;
                        lista1:=put_json_list_int(lista1,total1);
                        --perform logfile('GRAFICO lista1='||lista1::varchar);
                        json_total1:=put_json(json_total1,'data',lista1::varchar);
                        --perform logfile('GRAFICO json_total1='||json_total1::varchar);

                        lista1:=get_json('data',json_respondidos1)::json;
                        lista1:=put_json_list_int(lista1,total_respondidos1);
                        json_respondidos1:=put_json(json_respondidos1,'data',lista1::varchar);

                        lista1:=get_json('data',json_norespondidos1)::json;
                        lista1:=put_json_list_int(lista1,total_norespondidos1);
                        json_norespondidos1:=put_json(json_norespondidos1,'data',lista1::varchar);
                end if;

        end loop;

        --perform logfile('GRAFICO json_total1='||json_total1);
        series1:=put_json_list(series1,json_total1);
        --raise notice 'series1=%',series1;
        series1:=put_json_list(series1,json_respondidos1);
        --raise notice 'series1=%',series1;
        series1:=put_json_list(series1,json_norespondidos1);
        --raise notice 'series1=%',series1;

        json_out:=put_json(json_out,'EJE_X_ARRAY',replace(series2::varchar,'XX',''));
        json_out:=put_json(json_out,'EJE_Y','Cantidad de Documentos');
        json_out:=put_json(json_out,'EJE_X','Hora');
        --perform logfile('GRAFICO series1='||series1);
        json_out:=put_json(json_out,'SERIES',series1::varchar);
        --perform logfile('GRAFICO series11='||get_json('SERIES',json_out));
        --json_out:=put_json(json_out,'SERIES',json_series_y::varchar);
        json_out:=put_json(json_out,'PATRON','patron_stacked_column_zoom.js');
        json_out:=put_json(json_out,'CLASE','col-sm-6');
        json_out:=put_json(json_out,'LISTA_COLORES',' colors: [''#507fe3'',''#3fc941'',''#e1da2f''], ');


        salida1:=put_json_list(salida1,json_out::varchar);
        --salida1:=put_json('{}','__LISTA_GRAFICOS__',put_json_list('[]',json_out)::varchar);
        --raise notice 'salida1=%',salida1;

        json_out:='{}';
        json_out:=put_json(json_out,'TITULO','Nivel de Servicio Respuestas SII');
        json_out:=put_json(json_out,'SUBTITULO','Medido en Minutos del '||get_json('DIA_DISPLAY',json2));
        series1:='[]';
        series2:='[]';
        json_respondidos1:=put_json('{}','name','Tiempo Promedio de Respuesta SII');
        json_respondidos1:=put_json(json_respondidos1,'data','[]');
        --raise notice 'paso2';

        i:=0;
        flag_agrega1:=false;
        --for campo in select to_char(generate_series,'HH24:MI') as fecha from generate_series(date_trunc('day',now()),date_trunc('day',now()+interval '1 day'),'15 minutes') loop
        for campo in select to_char(generate_series,'HH24:MI') as fecha from generate_series(date_trunc('day',now()),date_trunc('day',now())+interval '23 hours 45 minutes','15 minutes') loop
                series2:=put_json_list(series2,'XX'||campo.fecha::varchar);
                --Saco el primer valor
                --raise notice 'Fecha %',campo.fecha;
                total1:=0;
                total_final1:=0;
                total_respondidos1:=0;
                aux1:=get_json_index(json_data1,i);
                while aux1<>'' loop
                        json_aux1:=aux1::json;
                        --Si corresponde la fecha actual
                        if (campo.fecha=get_json('fecha',json_aux1)) then
                                if (get_json('estado_sii',json_aux1) in ('ACEPTADO_CON_REPAROS_POR_EL_SII','ACEPTADO_POR_EL_SII','RECHAZADO_POR_EL_SII')) then
                                        --inter1:=get_json('promedio',json_aux1);
                                        total_final1:=total_final1+get_json('total',json_aux1)::integer;
                                        total_respondidos1:=total_respondidos1+get_json('promedio',json_aux1)::integer*get_json('total',json_aux1)::integer;
                                end if;
                                flag_agrega1:=false;
                        else

                                flag_agrega1:=true;
                                lista1:=get_json('data',json_respondidos1)::json;
                                if (total_respondidos1>0 and total_final1>0) then
                                        total_respondidos1:=total_respondidos1/total_final1;

                                        if (total_respondidos1/60>120) then
                                                --red
                                                color1:='#c93d2e';
                                        elsif (total_respondidos1/60>60) then
                                                --yellow
                                                color1:='#dfce25';
                                        else
                                                --verde
                                                color1:='#4cb035';
                                        end if;
                                        json_point1:=(select row_to_json(sql) from (select total_respondidos1/60 as y,color1 as color) sql);
                                else
                                        json_point1:=(select row_to_json(sql) from (select 0 as  y,'green' as color) sql);
                                end if;
                                lista1:=put_json_list(lista1,json_point1);
                                json_respondidos1:=put_json(json_respondidos1,'data',lista1::varchar);

                                --Salgo de recorrer la data y voy por la siguiente hora
                                exit;
                        end if;
                        i:=i+1;
                        aux1:=get_json_index(json_data1,i);
                end loop;
                --Sino agrego, agrego el 0
                if (flag_agrega1 is false) then
                        lista1:=get_json('data',json_respondidos1)::json;
                        if (total_respondidos1>0 and total_final1>0) then
                                total_respondidos1:=total_respondidos1/total_final1;

                                if (total_respondidos1/60>120) then
                                        --red
                                        color1:='#c93d2e';
                                elsif (total_respondidos1/60>60) then
                                        --yellow
                                        color1:='#dfce25';
                                else
                                        --verde
                                        color1:='#4cb035';
                                end if;
                                json_point1:=(select row_to_json(sql) from (select total_respondidos1/60 as y,color1 as color) sql);
                        else
                                json_point1:=(select row_to_json(sql) from (select 0 as  y,'green' as color) sql);
                        end if;
                        lista1:=put_json_list(lista1,json_point1);
                        json_respondidos1:=put_json(json_respondidos1,'data',lista1::varchar);
                end if;
        end loop;

        series1:=put_json_list(series1,json_respondidos1);

        json_out:=put_json(json_out,'EJE_X_ARRAY',replace(series2::varchar,'XX',''));
        json_out:=put_json(json_out,'EJE_Y','Minutos');
        json_out:=put_json(json_out,'EJE_X','Hora');
        json_out:=put_json(json_out,'SERIES',series1::varchar);
        --json_out:=put_json(json_out,'SERIES',json_series_y::varchar);
        --json_out:=put_json(json_out,'PATRON','patron_basic_line.js');
        json_out:=put_json(json_out,'PATRON','patron_basic_line_zoom.js');
        json_out:=put_json(json_out,'UNIDAD',' min ');
        json_out:=put_json(json_out,'COLOR_LINEA','#FFFFFF');
        json_out:=put_json(json_out,'CLASE','col-sm-6');
        salida1:=put_json_list(salida1,json_out::varchar);
        --salida1:=put_json('{}','__LISTA_GRAFICOS__',put_json_list('[]',json_out)::varchar);
        --return put_json('{}','__LISTA_GRAFICOS__',salida1::varchar);
        --return salida1;
	json_out:=arma_respuesta_lista_grafico(json2,salida1);
	json2:=response_requests_6000('1', 'NO_ALERT', json_out::varchar,json2);
        RETURN json2;
end;
$$
LANGUAGE plpgsql;
