delete from isys_querys_tx where llave='25100';

insert into isys_querys_tx values ('25100','10',9,1,'select arma_querys_25100(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

--Cuenta Redshift
insert into isys_querys_tx values ('25100','20',47,1,'$$QUERY_RS$$',0,0,0,9,1,30,30);
insert into isys_querys_tx values ('25100','30',9,1,'select revisa_resultado_count_rs_25100(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);
--Cuenta Local
insert into isys_querys_tx values ('25100','40',44,1,'$$QUERY_LOCAL$$',0,0,0,9,1,50,50);
insert into isys_querys_tx values ('25100','50',9,1,'select revisa_resultado_count_local_25100(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

--Armo Querys para sacar los codigos de la primera pagina
insert into isys_querys_tx values ('25100','60',9,1,'select arma_querys_coddocumento_25100(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

--CodDocumento Redshift
insert into isys_querys_tx values ('25100','70',47,1,'$$QUERY_RS$$',0,0,0,9,1,80,80);
insert into isys_querys_tx values ('25100','80',9,1,'select revisa_resultado_query_rs_25100(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);
--CodDocumento Local
insert into isys_querys_tx values ('25100','90',44,1,'$$QUERY_LOCAL$$',0,0,0,9,1,100,100);
insert into isys_querys_tx values ('25100','100',9,1,'select revisa_resultado_query_local_25100(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

--Arma Query Final
insert into isys_querys_tx values ('25100','110',9,1,'select arma_query_final_25100(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

--Base Normal
insert into isys_querys_tx values ('25100','120',44,1,'$$QUERY_DATA$$',0,0,0,9,1,130,130);
insert into isys_querys_tx values ('25100','130',9,1,'select arma_respuesta_25100(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

CREATE or replace FUNCTION arma_respuesta_25100(json) RETURNS json AS $$
DECLARE
        json1   alias for $1;
        json2   json;
        json3   json;
        v_out_resultado json;
begin
        json2:=json1;
        --perform logfile('25100 RES_JSON='||get_json('RES_JSON_1',json2));
        if (get_json('RES_JSON_1',json2)='') then
                json2:=put_json(json2,'MENSAJE_RESPUESTA','Reintente por favor (FT)');
                json2:=put_json(json2,'CODIGO_RESPUESTA','2');
                json2:=put_json(json2,'__SECUENCIAOK__','0');
                json2:=responde_pantalla_25100(json2);
                return json2;
        end if;
        BEGIN
                if get_json('LISTA',get_json('RES_JSON_1',json2)::json)<>'' then
                        v_out_resultado:=get_json('LISTA',get_json('RES_JSON_1',json2)::json);
                else
                        --v_out_resultado:=put_json_list('[]',get_json('RES_JSON_1',json2));
			v_out_resultado:=put_json_list('[]',replace(get_json('RES_JSON_1',json2),', "STATUS": "OK", "TOTAL_REGISTROS": "1"',''));
                        --v_out_resultado:=put_json_list('[]',get_json('RES_JSON_1',json2));
                end if;
        EXCEPTION WHEN OTHERS THEN
                json2:=put_json(json2,'MENSAJE_RESPUESTA','Reintente por favor (FT).');
                json2:=put_json(json2,'CODIGO_RESPUESTA','2');
                json2:=put_json(json2,'__SECUENCIAOK__','0');
                json2:=responde_pantalla_25100(json2);
                return json2;
        END;
        json2:=logjson(json2,'TOTAL_REGISTROS='||get_json('TOTAL_REGISTROS',get_json('RES_JSON_1',json2)::json)||' COUNT JSON '||count_array_json(v_out_resultado)::varchar);
        json2:=put_json(json2,'v_out_resultado',v_out_resultado);

        json2:=put_json(json2,'MENSAJE_RESPUESTA','OK');
        json2:=put_json(json2,'CODIGO_RESPUESTA','1');
        json2:=put_json(json2,'__SECUENCIAOK__','0');
        json2:=responde_pantalla_25100(json2);
        return json2;
end;
$$
LANGUAGE plpgsql;



CREATE or replace FUNCTION arma_query_final_25100(json) RETURNS json AS $$
DECLARE
        json1   alias for $1;
        json2   json;
        json3   json;
        cods_base1      varchar;
        cods_base2      varchar;
        cods1   varchar;
        select_vars1    varchar;
        query1  varchar;
begin
        json2:=json1;
        cods_base1:=coalesce(nullif(decode_hex(get_json('CODIGOS_LOCAL',json2)),''),'''-1''');
        cods_base2:=coalesce(nullif(decode_hex(get_json('CODIGOS_RS',json2)),''),'''-1''');
        cods1:=cods_base1||','||cods_base2;
        json2:=logjson(json2,'CODIGOS FINAL '||cods1);

        --FAY-DAO 2019-02-13 Si el flujo se llama con FLAG_SOLO_CODIGOS=SI se responden solo los codigo de rs
        if (get_json('FLAG_SOLO_CODIGOS',json2)='SI') then
                --Borramos el -1
                cods1:=replace(cods1,'''-1'',','');
                cods1:=replace(cods1,'''','"');
                json2:=put_json(json2,'v_out_resultado',('['||cods1||']')::json::varchar);
                json2:=put_json(json2,'MENSAJE_RESPUESTA','OK');
                json2:=put_json(json2,'CODIGO_RESPUESTA','1');
                json2:=put_json(json2,'__SECUENCIAOK__','0');
                json2:=responde_pantalla_25100(json2);
                return json2;
        end if;

        json2:=get_campos_busqueda_dec_25100(json2);
        select_vars1:=get_json('__campos_busqueda__',json2);
        query1:=select_vars1||' from (select x.*,y.desc_tipo as nombre_documento from (select * from dc4_Documento where coddocumento in ('||cods1||')) x left join dc4_TipoDocto y on x.CodTipo=y.CodTipo and x.Institucion=y.Institucion) z';
        json2:=put_json(json2,'__TOTAL_RESP_ESPERADAS__','1');

        json2:=logjson(json2,'QUERY='||query1::varchar);
        json2:=put_json(json2,'QUERY_DATA',query1);
        json2:=put_json(json2,'__SECUENCIAOK__','120');
        return json2;
end;
$$
LANGUAGE plpgsql;



CREATE or replace FUNCTION revisa_resultado_query_rs_25100(json) RETURNS json AS $$
DECLARE
        json2   json;
        json1   alias for $1;
        json3   json;
begin
        json2:=json1;
        if (get_json('RES_JSON_1',json2)='') then
                json2:=logjsonfunc(json2,'Falla obtener resultado de '||get_json('TABLA',json2)|| ' Base '||get_json('PARAMETRO_TABLA',json2));
                json2:=put_json(json2,'MENSAJE_RESPUESTA','Reintente por favor (FRS)');
                json2:=put_json(json2,'CODIGO_RESPUESTA','2');
                json2:=put_json(json2,'__SECUENCIAOK__','0');
                json2:=responde_pantalla_25100(json2);
                return json2;
        end if;
        BEGIN
                json3:=get_json('RES_JSON_1',json2)::json;
        EXCEPTION WHEN OTHERS THEN
                json2:=logjsonfunc(json2,'Falla obtener resultado de '||get_json('TABLA',json2)|| ' Base '||get_json('PARAMETRO_TABLA',json2));
                json2:=put_json(json2,'MENSAJE_RESPUESTA','Reintente por favor (FRS).');
                json2:=put_json(json2,'CODIGO_RESPUESTA','2');
                json2:=put_json(json2,'__SECUENCIAOK__','0');
                json2:=responde_pantalla_25100(json2);
                return json2;
        END;
        json2:=logjson(json2,'CODIGOS_RS='||''''||replace(get_json('codigos',json3),',',''',''')||'''');
        json2:=put_json(json2,'CODIGOS_RS',encode_hex(''''||replace(get_json('codigos',json3),',',''',''')||''''));



        if get_json('QUERY_LOCAL',json2)<>'' then
                json2:=logjson(json2,'QUERY_LOCAL='||decode_hex(get_json('QUERY_LOCAL',json2)));
                json2:=put_json(json2,'QUERY_LOCAL',decode_hex(get_json('QUERY_LOCAL',json2)));
                json2:=put_json(json2,'__SECUENCIAOK__','90');
        else
                json2:=put_json(json2,'__SECUENCIAOK__','110');
        end if;
        return json2;
end;
$$
LANGUAGE plpgsql;

CREATE or replace FUNCTION revisa_resultado_query_local_25100(json) RETURNS json AS $$
DECLARE
        json2   json;
        json1   alias for $1;
        json3   json;
begin
        json2:=json1;
        if (get_json('RES_JSON_1',json2)='') then
                json2:=logjsonfunc(json2,'Falla obtener resultado de '||get_json('TABLA',json2)|| ' Base '||get_json('PARAMETRO_TABLA',json2));
                json2:=put_json(json2,'MENSAJE_RESPUESTA','Reintente por favor (FL)');
                json2:=put_json(json2,'CODIGO_RESPUESTA','2');
                json2:=put_json(json2,'__SECUENCIAOK__','0');
                json2:=responde_pantalla_25100(json2);
                return json2;
        end if;
        BEGIN
                json3:=get_json('RES_JSON_1',json2)::json;
        EXCEPTION WHEN OTHERS THEN
                json2:=logjsonfunc(json2,'Falla obtener resultado de '||get_json('TABLA',json2)|| ' Base '||get_json('PARAMETRO_TABLA',json2));
                json2:=put_json(json2,'MENSAJE_RESPUESTA','Reintente por favor (FL).');
                json2:=put_json(json2,'CODIGO_RESPUESTA','2');
                json2:=put_json(json2,'__SECUENCIAOK__','0');
                json2:=responde_pantalla_25100(json2);
                return json2;
        END;
        --json2:=put_json(json2,'CODIGOS_LOCAL',get_json('codigos',coalesce(nullif(get_json_index(coalesce(nullif(get_json('LISTA',json3),''),'[]')::json,0),''),'{}')::json));
        json2:=put_json(json2,'CODIGOS_LOCAL',get_json('codigos',json3));
        json2:=logjson(json2,'CODIGOS_LOCAL='||decode_hex(get_json('CODIGOS_LOCAL',json2)));
        json2:=put_json(json2,'__SECUENCIAOK__','110');
        return json2;
end;
$$
LANGUAGE plpgsql;

CREATE or replace FUNCTION revisa_resultado_count_local_25100(json) RETURNS json AS $$
DECLARE
        json2   json;
        json1   alias for $1;
        json3   json;
begin
        json2:=json1;
        if (get_json('RES_JSON_1',json2)='') then
                json2:=logjsonfunc(json2,'Falla obtener resultado de '||get_json('TABLA',json2)|| ' Base '||get_json('PARAMETRO_TABLA',json2));
                json2:=put_json(json2,'MENSAJE_RESPUESTA','Reintente por favor (FCL)');
                json2:=put_json(json2,'CODIGO_RESPUESTA','2');
                json2:=put_json(json2,'__SECUENCIAOK__','0');
                json2:=responde_pantalla_25100(json2);
                return json2;
        end if;
        BEGIN
                json3:=get_json('RES_JSON_1',json2)::json;
        EXCEPTION WHEN OTHERS THEN
                json2:=logjsonfunc(json2,'Falla obtener resultado de '||get_json('TABLA',json2)|| ' Base '||get_json('PARAMETRO_TABLA',json2));
                json2:=put_json(json2,'MENSAJE_RESPUESTA','Reintente por favor (FCL).');
                json2:=put_json(json2,'CODIGO_RESPUESTA','2');
                json2:=put_json(json2,'__SECUENCIAOK__','0');
                json2:=responde_pantalla_25100(json2);
                return json2;
        END;
        --json2:=put_json(json2,'COUNT_LOCAL',get_json('count',get_json_index(get_json('LISTA',json3)::json,0)::json));
        json2:=put_json(json2,'COUNT_LOCAL',get_json('count',json3));
        if is_number(get_json('COUNT_LOCAL',json2)) is false then
                json2:=logjsonfunc(json2,'Falla obtener resultado de '||get_json('TABLA',json2)|| ' Base '||get_json('PARAMETRO_TABLA',json2));
                json2:=put_json(json2,'MENSAJE_RESPUESTA','Reintente por favor (FCRS)..');
                json2:=put_json(json2,'CODIGO_RESPUESTA','2');
                json2:=put_json(json2,'__SECUENCIAOK__','0');
                json2:=responde_pantalla_25100(json2);
                return json2;
        end if;
        json2:=logjson(json2,'COUNT_LOCAL='||get_json('COUNT_LOCAL',json2));
        json2:=put_json(json2,'__SECUENCIAOK__','60');
        return json2;
end;
$$
LANGUAGE plpgsql;

CREATE or replace FUNCTION revisa_resultado_count_rs_25100(json) RETURNS json AS $$
DECLARE
        json2   json;
        json1   alias for $1;
        json3   json;
begin
        json2:=json1;
        if (get_json('RES_JSON_1',json2)='') then
                json2:=logjsonfunc(json2,'Falla obtener resultado de '||get_json('TABLA',json2)|| ' Base '||get_json('PARAMETRO_TABLA',json2));
                json2:=put_json(json2,'MENSAJE_RESPUESTA','Reintente por favor (FCRS)');
                json2:=put_json(json2,'CODIGO_RESPUESTA','2');
                json2:=put_json(json2,'__SECUENCIAOK__','0');
                json2:=responde_pantalla_25100(json2);
                return json2;
        end if;
        BEGIN
                json3:=get_json('RES_JSON_1',json2)::json;
        EXCEPTION WHEN OTHERS THEN
                json2:=logjsonfunc(json2,'Falla obtener resultado de '||get_json('TABLA',json2)|| ' Base '||get_json('PARAMETRO_TABLA',json2));
                json2:=put_json(json2,'MENSAJE_RESPUESTA','Reintente por favor (FCRS).');
                json2:=put_json(json2,'CODIGO_RESPUESTA','2');
                json2:=put_json(json2,'__SECUENCIAOK__','0');
                json2:=responde_pantalla_25100(json2);
                return json2;
        END;
        json2:=put_json(json2,'COUNT_RS',get_json('count',json3));
        if is_number(get_json('COUNT_RS',json2)) is false then
                json2:=logjsonfunc(json2,'Falla obtener resultado de '||get_json('TABLA',json2)|| ' Base '||get_json('PARAMETRO_TABLA',json2));
                json2:=put_json(json2,'MENSAJE_RESPUESTA','Reintente por favor (FCRS)..');
                json2:=put_json(json2,'CODIGO_RESPUESTA','2');
                json2:=put_json(json2,'__SECUENCIAOK__','0');
                json2:=responde_pantalla_25100(json2);
                return json2;
        end if;
        json2:=logjson(json2,'COUNT_RS='||get_json('COUNT_RS',json2));
        if get_json('QUERY_LOCAL',json2)<>'' then
                json2:=logjson(json2,'QUERY_LOCAL='||decode_hex(get_json('QUERY_LOCAL',json2)));
                json2:=put_json(json2,'QUERY_LOCAL',decode_hex(get_json('QUERY_LOCAL',json2)));
                json2:=put_json(json2,'__SECUENCIAOK__','40');
        else
                json2:=put_json(json2,'__SECUENCIAOK__','60');
        end if;
        return json2;
end;
$$
LANGUAGE plpgsql;

CREATE or replace FUNCTION arma_querys_25100(json) RETURNS json AS $$
DECLARE
        json2   json;
        json1   alias for $1;

        vin_fstart      varchar;
        vin_fend        varchar;
        v_in_fecha_inicio       varchar;
        v_in_fecha_fin          varchar;

        v_in_rut1       varchar;
        hoy1            varchar;
        filtro1 varchar;
        filtro2 varchar;
        query_rs1       varchar;
        query_local1    varchar;

        filtro_perf_local1      varchar;
        filtro_perf_local2      varchar;
        filtro_perf_rs1 varchar;
        from_query_local1       varchar;

        i       integer;
        aux     varchar;
        jaux    json;
        estado1 varchar;
        rol_dec varchar;
	filtro_doc_rs1	varchar;
	filtro_doc_local1	varchar;
	aux1	varchar;
	aux2	varchar;
BEGIN
        json2:=json1;
        vin_fstart:=get_json('FSTART',json2);
        vin_fend:=get_json('FEND',json2);
        --FECHAS--
        json2:=corrige_fechas(json2);
        v_in_fecha_inicio:=get_json('fstart',json2)::date::varchar;
        v_in_fecha_fin:=(get_json('fend',json2)::date + interval '1 day')::varchar;
        hoy1:=now()::date::varchar;
        estado1:=get_json('ESTADO',json2);

	filtro_doc_rs1:=' Institucion_doc='''||get_json('institucion_dec',json2)||''' and FecCreacion>='''||v_in_fecha_inicio||''' and FecCreacion<='''||v_in_fecha_fin||''' ';
	--filtro_doc_local1:=' Institucion='''||get_json('institucion_dec',json2)||''' and FecCreacion>='''||hoy1::varchar||''' ';
	filtro_doc_local1:=' Institucion_doc='''||get_json('institucion_dec',json2)||''' and FecCreacion>='''||hoy1::varchar||''' ';

        filtro_perf_local1:='';
        filtro_perf_rs1:='';

        if get_json('PAPELERA',json2)<>'' then
		filtro_doc_rs1:=filtro_doc_rs1||' and papelera='||get_json('PAPELERA',json2)||' ';
		filtro_doc_local1:=filtro_doc_local1||' and papelera='||get_json('PAPELERA',json2)||' ';
        else
		filtro_doc_rs1:=filtro_doc_rs1||' and papelera=0 ';
                filtro_doc_local1:=filtro_doc_local1||' and papelera=0 ';
        end if;

        if estado1<>'' then
		filtro_perf_rs1:=' estadodoc in ('||estado1||') and ';
		filtro_perf_local1:=' x.estadofirma in ('||estado1||') and ';
        end if;
        if get_json('tipoDcto',json2)<>'' then
		filtro_perf_rs1:=filtro_perf_rs1||' codtipo ='''||get_json('tipoDcto',json2)||''' and ';
		filtro_perf_local1:=filtro_perf_local1||' codtipo ='''||get_json('tipoDcto',json2)||''' and ';
        end if;

        --Si viene el ROL desde pantalla...
        rol_dec:=get_json('ROL',json2);
        if get_json('super_user',json2)<>'SI'  and strpos(upper(get_json('roles_institucion',json2)),'"REPORTES"')=0 and get_json('institucion_dec',json2)<>'HITES' then
                jaux:=get_json('roles_institucion',json2);
                json2:=logjson(json2,'roles_institucion ='||jaux::varchar);
                i:=0;
                aux:=get_json_index(jaux::json,i);
                filtro_perf_local2:='';
                while (aux<>'') loop
                        json2:=logjson(json2,'RolDec='||rol_dec||' Aux='||aux);
                        --Si viene un rol especifico...
                        if upper(rol_dec)=upper(aux) or rol_dec='*'  or rol_dec='__PERSONAL__'  then
                                if filtro_perf_local2='' then
                                        filtro_perf_local2:='( Rol='''||aux||''' ';
                                else
                                        filtro_perf_local2:=filtro_perf_local2||' or Rol='''||aux||''' ';
                                end if;
                        end if;
                        i:=i+1;
                        aux:=get_json_index(jaux::json,i);
                end loop;
                if filtro_perf_local2<>'' then
			filtro_perf_local1:=filtro_perf_local1||' '||filtro_perf_local2||') ';
			filtro_perf_rs1:=filtro_perf_rs1||' '||filtro_perf_local2||') ';
                else
                        filtro_perf_local1:=filtro_perf_local1||' and POSITION('''||get_json('rutUsuario',json2)||''' in Rol)>0 ';
                        filtro_perf_rs1:=filtro_perf_rs1||' strpos(rut,'''||get_json('rutUsuario',json2)||''')>0 ';
                end if;
		if rol_dec='__PERSONAL__' then
                        filtro_perf_local1:=filtro_perf_local1||' and strpos(tags,''<FIRMANTE_PERSONAL>SI</FIRMANTE_PERSONAL>'')>0 ';
                        filtro_perf_rs1:=filtro_perf_rs1||' and strpos(tags,''<FIRMANTE_PERSONAL>SI</FIRMANTE_PERSONAL>'')>0 ';
                end if;
	else
                filtro_perf_local1:=filtro_perf_local1||' 1=1 ';
                filtro_perf_rs1:=filtro_perf_rs1||' 1=1 ';
        end if;

        v_in_rut1:=replace(split_part(get_json('RUT',json2),'-',1),'.','');

        --Si incluye hoy vamos al Mysql a contar tambien, si no solo al RS
        if v_in_fecha_fin::date<hoy1::date then
		if v_in_rut1='' then
			filtro1:=filtro_doc_rs1||' and '||filtro_perf_rs1;
		else
                        --FAY-DAO 2018-12-10 Solo para HITES por mala construccion
			aux1:=lpad(split_part(v_in_rut1,'-',1)||'-'||modulo11(v_in_rut1),12,'0');
			aux2:=split_part(v_in_rut1,'-',1)||'-'||modulo11(v_in_rut1);
                        if (get_json('institucion_dec',json2)='HITES') then
                                --DAO 20201029 filtro1:=filtro_doc_rs1||' and '||filtro_perf_rs1||' and '||' FecCreacion>='''||v_in_fecha_inicio||''' and FecCreacion<'''||v_in_fecha_fin||''' and (strpos(rut,'''||v_in_rut1||''')>0 or strpos(Descripcion,'''||v_in_rut1||''')>0 or strpos(tags,'''||v_in_rut1||''')>0) ';
				--FAY-DAO 2020-11-23 si es modulo k buscamos con mayuscula y minuscula
				if upper(modulo11(v_in_rut1))='K' then
	                               filtro1:=filtro_doc_rs1||' and '||filtro_perf_rs1||' and '||' FecCreacion>='''||v_in_fecha_inicio||''' and FecCreacion<'''||v_in_fecha_fin||''' and (rut='''||aux1||''' or strpos(Descripcion,'''||aux2||''')>0 or strpos(tags,'''||aux2||''')>0 or rut='''||lower(aux1)||''' or strpos(Descripcion,'''||lower(aux2)||''')>0 or strpos(tags,'''||lower(aux2)||''')>0    ) ';
				else
					--DAO 20201029 cambiamos el strpos del rut
	                               filtro1:=filtro_doc_rs1||' and '||filtro_perf_rs1||' and '||' FecCreacion>='''||v_in_fecha_inicio||''' and FecCreacion<'''||v_in_fecha_fin||''' and (rut='''||aux1||''' or strpos(Descripcion,'''||aux2||''')>0 or strpos(tags,'''||aux2||''')>0) ';
				end if;
                        else
                                --DAO 20201029 filtro1:=filtro_doc_rs1||' and '||filtro_perf_rs1||' and '||' FecCreacion>='''||v_in_fecha_inicio||''' and FecCreacion<'''||v_in_fecha_fin||''' and (strpos(rut,'''||v_in_rut1||''')>0 or strpos(tags,'''||v_in_rut1||''')>0) ';
				--DAO 20201029 cambiamos el strpos del rut
                                filtro1:=filtro_doc_rs1||' and '||filtro_perf_rs1||' and '||' FecCreacion>='''||v_in_fecha_inicio||''' and FecCreacion<'''||v_in_fecha_fin||''' and (rut='''||aux1||''' or strpos(tags,'''||v_in_rut1||''')>0) ';
                        end if;
                end if;
                query_rs1:='select count(*) as count from (select distinct coddocumento from firmantes_dec_inst_doc where '||filtro1||') x ';
                --query_rs1:='select count(*) as count from firmantes_dec where '||filtro1;
                json2:=logjson(json2,'QUERY_RS='||query_rs1);
                json2:=put_json(json2,'QUERY_RS',query_rs1);
                json2:=put_json(json2,'FILTRO_RS',filtro1);
                json2:=put_json(json2,'__SECUENCIAOK__','20');
        elsif v_in_fecha_inicio::date>=hoy1::date then
                if v_in_rut1='' then
                        --from_query_local1:=' from (select distinct x.coddocumento from (select * from dc4_Documento where '||filtro_doc_local1||') x left join dc4_Firmantes y on x.coddocumento=y.coddocumento where '||filtro_perf_local1||') w';
                        from_query_local1:=' from (select distinct x.coddocumento from firmantes_dec_dia x where '||filtro_doc_local1||' and '||filtro_perf_local1||') w';
			
                        query_local1:='select count(*) as count '||from_query_local1;
                else
                        --FAY-DAO 2018-12-10 Solo para HITES por mala construccion
			aux1:=lpad(split_part(v_in_rut1,'-',1)||'-'||modulo11(v_in_rut1),12,'0');
			aux2:=split_part(v_in_rut1,'-',1)||'-'||modulo11(v_in_rut1);
                        if (get_json('institucion_dec',json2)='HITES') then
				if modulo11(v_in_rut1)='K' then
                                	from_query_local1:=' from (select distinct x.coddocumento from firmantes_dec_dia x where '||filtro_doc_local1||' and '||filtro_perf_local1||' and (rut='''||aux1||''' or strpos(Descripcion,'''||aux2||''')>0 or strpos(tags,'''||aux2||''')>0 or rut='''||lower(aux1)||''' or strpos(Descripcion,'''||lower(aux2)||''')>0 or strpos(tags,'''||lower(aux2)||''')>0) w';
				else
                                	from_query_local1:=' from (select distinct x.coddocumento from firmantes_dec_dia x where '||filtro_doc_local1||' and '||filtro_perf_local1||' and (rut='''||aux1||''' or strpos(Descripcion,'''||aux2||''')>0 or strpos(tags,'''||aux2||''')>0) ) w';
				end if;
                                --filtro1:=filtro_doc_rs1||' and '||filtro_perf_rs1||' and '||' FecCreacion>='''||v_in_fecha_inicio||''' and FecCreacion<'''||v_in_fecha_fin||''' and (strpos(rut,'''||v_in_rut1||''')>0 or strpos(Descripcion,'''||v_in_rut1||''')>0 or strpos(tags,'''||v_in_rut1||''')>0) ';
                        else
                                --from_query_local1:=' from (select distinct x.coddocumento from (select coddocumento,Descripcion from dc4_Documento where '||filtro_doc_local1||') x join dc4_Firmantes y on x.coddocumento=y.coddocumento and '||filtro_perf_local1||' join dc4_Firmantes z on x.coddocumento=z.coddocumento and POSITION('''||v_in_rut1||''' in z.Rut)>0 ) w';
                                from_query_local1:=' from (select distinct x.coddocumento from firmantes_dec_dia x where '||filtro_doc_local1||' and '||filtro_perf_local1||' and ( rut='''||aux1||''' or strpos(tags,'''||v_in_rut1||''')>0)  ) w';
                                --from_query_local1:=' from (select distinct x.coddocumento,Descripcion from firmantes_dec_dia x where '||filtro_doc_local1||' and '||filtro_perf_local1||' and POSITION('''||v_in_rut1||''' in Rut)>0 ) w';
                        end if;
                        query_local1:='select count(*) as count '||from_query_local1;
                        json2:=put_json(json2,'FROM_QUERY_LOCAL1',encode_hex(from_query_local1));
                end if;
                json2:=put_json(json2,'FROM_QUERY_LOCAL1',encode_hex(from_query_local1));
                json2:=put_json(json2,'QUERY_LOCAL',query_local1);
                json2:=logjson(json2,'QUERY_LOCAL='||query_local1);
                --json2:=put_json(json2,'FILTRO_LOCAL_DOCS',filtro1);
                --json2:=put_json(json2,'FILTRO_LOCAL_FRM',filtro2);
                json2:=put_json(json2,'__SECUENCIAOK__','40');
        else
                if v_in_rut1='' then
			filtro1:=filtro_doc_rs1||' and '||filtro_perf_rs1;
		else
                        --FAY-DAO 2018-12-10 Solo para HITES por mala construccion
			aux1:=lpad(split_part(v_in_rut1,'-',1)||'-'||modulo11(v_in_rut1),12,'0');
			aux2:=split_part(v_in_rut1,'-',1)||'-'||modulo11(v_in_rut1);
                        if (get_json('institucion_dec',json2)='HITES') then
				if modulo11(v_in_rut1)='K' then
	                                filtro1:=filtro_doc_rs1||' and '||filtro_perf_rs1||' and '||' FecCreacion>='''||v_in_fecha_inicio||''' and FecCreacion<'''||v_in_fecha_fin||''' and (rut='''||aux1||''' or strpos(Descripcion,'''||aux2||''')>0 or strpos(tags,'''||aux2||''')>0 or rut='''||lower(aux1)||''' or strpos(Descripcion,'''||lower(aux2)||''')>0 or strpos(tags,'''||lower(aux2)||''')>0) ';
				else
	                                filtro1:=filtro_doc_rs1||' and '||filtro_perf_rs1||' and '||' FecCreacion>='''||v_in_fecha_inicio||''' and FecCreacion<'''||v_in_fecha_fin||''' and (rut='''||aux1||''' or strpos(Descripcion,'''||aux2||''')>0 or strpos(tags,'''||aux2||''')>0) ';
				end if;
                        else
                                filtro1:=filtro_doc_rs1||' and '||filtro_perf_rs1||' and '||' FecCreacion>='''||v_in_fecha_inicio||''' and FecCreacion<'''||v_in_fecha_fin||''' and ( rut='''||aux1||''' or strpos(tags,'''||v_in_rut1||''')>0) ';
                        end if;
                end if;
                --query_rs1:='select count(*) as count from firmantes_dec where '||filtro1;
                query_rs1:='select count(*) as count from (select distinct coddocumento from firmantes_dec_inst_doc where '||filtro1||') x ';
                json2:=put_json(json2,'QUERY_RS',query_rs1);
                json2:=logjson(json2,'QUERY_RS='||query_rs1);
                json2:=put_json(json2,'FILTRO_RS',filtro1);

                if v_in_rut1='' then
                        --from_query_local1:=' from (select distinct x.coddocumento from (select * from dc4_Documento where '||filtro_doc_local1||') x left join dc4_Firmantes y on x.coddocumento=y.coddocumento where '||filtro_perf_local1||') w';
                        from_query_local1:=' from (select distinct x.coddocumento from firmantes_dec_dia x where '||filtro_doc_local1||' and '||filtro_perf_local1||') w';
                        query_local1:='select count(*) as count '||from_query_local1;
                else
                        --FAY-DAO 2018-12-10 Solo para HITES por mala construccion
                        if (get_json('institucion_dec',json2)='HITES') then
                                --Debemos hacer doble join con los firmantes, primero los que puedo ver y luego en  donde este el rut
                                from_query_local1:=' from (select distinct x.coddocumento from firmantes_dec_dia x where '||filtro_doc_local1||' and '||filtro_perf_local1||' and (POSITION('''||v_in_rut1||''' in Rut)>0 or POSITION('''||v_in_rut1||''' in Descripcion)>0) ) w';
                        else
                                --from_query_local1:=' from (select distinct x.coddocumento from (select coddocumento,Descripcion from dc4_Documento where '||filtro_doc_local1||') x join dc4_Firmantes y on x.coddocumento=y.coddocumento and '||filtro_perf_local1||' join dc4_Firmantes z on x.coddocumento=z.coddocumento and POSITION('''||v_in_rut1||''' in z.Rut)>0 ) w';
                                from_query_local1:=' from (select distinct x.coddocumento from firmantes_dec_dia x where '||filtro_doc_local1||' and '||filtro_perf_local1||' and POSITION('''||v_in_rut1||''' in Rut)>0 ) w';
                        end if;
                        query_local1:='select count(*) as count '||from_query_local1;
                        json2:=put_json(json2,'FROM_QUERY_LOCAL1',encode_hex(from_query_local1));
                end if;
                json2:=logjson(json2,'QUERY_LOCAL='||query_local1);
                json2:=put_json(json2,'QUERY_LOCAL',encode_hex(query_local1));
                json2:=put_json(json2,'FROM_QUERY_LOCAL1',encode_hex(from_query_local1));
                json2:=put_json(json2,'__SECUENCIAOK__','20');
        end if;

        return json2;
end;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION arma_querys_coddocumento_25100(json)
returns json as
$$
declare
        json1           alias for $1;
        json2           json;
        limit1  integer;
        of1     integer;
        paginas_base1   integer;
        sobra_base1     integer;
        total_base1     integer;
        total_base2     integer;
        query2  varchar;
        v_in_cant_reg   varchar;
        v_in_offset1    integer;
begin
        json2:=json1;
        --Limpiamos para no tener confusiones
        json2:=put_json(json2,'QUERY_LOCAL','');
        json2:=put_json(json2,'QUERY_RS','');

        total_base1:=coalesce(nullif(get_json('COUNT_LOCAL',json2),''),'0')::integer;
        total_base2:=coalesce(nullif(get_json('COUNT_RS',json2),''),'0')::integer;
        json2:=put_json(json2,'v_total_registros',(total_base1+total_base2)::varchar);
        if total_base1+total_base2=0 then
                json2:=put_json(json2,'MENSAJE_RESPUESTA','No se encontraron registros.');
                json2:=put_json(json2,'CODIGO_RESPUESTA','2');
                json2:=put_json(json2,'__SECUENCIAOK__','0');
                json2:=responde_pantalla_25100(json2);
                return json2;
        end if;

        v_in_cant_reg:=get_json('v_in_cant_reg',json2)::integer;
        limit1:=get_json('v_in_cant_reg',json2)::integer;
        v_in_offset1:=get_json('v_in_offset1',json2)::integer;
        of1:=v_in_offset1;
        --get_json('v_in_offset',json2)::integer;

        paginas_base1:=total_base1/limit1;
        sobra_base1:=total_base1%limit1;
        json2:=logjson(json2,'v_in_offset1='||v_in_offset1::varchar||' paginas_base1='||paginas_base1::varchar||' v_in_cant_reg='||v_in_cant_reg::varchar||' sobra_base1='||sobra_base1::varchar||' limit1='||limit1::varchar);
        --offset 0-100-200
        --limit 100
        --Dependiendo de cuantos mostrar, hay que ir o no a buscar codigos al redshift
        --Alcanza con lo que tenemos en la base1
        if (of1+limit1<=paginas_base1*limit1) or total_base2=0 then
                --Solo cuento el total
                query2:='select hex(string_agg(concat('''''''',r.coddocumento,''''''''),'','')) as codigos from (select * '||decode_hex(get_json('FROM_QUERY_LOCAL1',json2))||' order by coddocumento offset '||v_in_offset1::varchar||' limit '||v_in_cant_reg::varchar||') r';
                json2:=put_json(json2,'QUERY_LOCAL',query2);
                json2:=logjson(json2,'QUERY_LOCAL='||query2);
                json2:=put_json(json2,'__SECUENCIAOK__','90');
        else
                --Se sacan los id de la base1 siempre y cuando sea la pagina intermedia
                if (sobra_base1>0 and of1=(paginas_base1)*limit1) then
                        query2:='select hex(string_agg(concat('''''''',r.coddocumento,''''''''),'','')) as codigos from (select * '||decode_hex(get_json('FROM_QUERY_LOCAL1',json2))||' order by coddocumento offset '||v_in_offset1::varchar||' limit '||sobra_base1::varchar||') r';
                        json2:=put_json(json2,'QUERY_LOCAL',encode_hex(query2));
                        limit1:=limit1-sobra_base1;
                        of1:=0;
                        query2:='select listagg(''''||coddocumento||'''','','') as codigos from (select distinct coddocumento from firmantes_dec_inst_doc where '||get_json('FILTRO_RS',json2)||' order by coddocumento offset '||of1||' limit '||limit1||')x';
                        json2:=logjson(json2,'QUERY_RS '||query2);
                        json2:=put_json(json2,'QUERY_RS',query2);
                        json2:=put_json(json2,'__SECUENCIAOK__','70');
                else
                        --Se saca los id solo de la base2
                        limit1:=v_in_cant_reg::integer;
                        of1:=v_in_offset1::integer-paginas_base1::integer*v_in_cant_reg::integer-sobra_base1;
                        query2:='select listagg(''''||coddocumento||'''','','') as codigos from (select distinct coddocumento from firmantes_dec_inst_doc where '||get_json('FILTRO_RS',json2)||' order by coddocumento offset '||of1||' limit '||limit1||')x';
                        json2:=put_json(json2,'QUERY_RS',query2);
                        json2:=logjson(json2,'QUERY_RS '||query2);
                        json2:=put_json(json2,'__SECUENCIAOK__','70');
                end if;
        end if;

        return json2;
end;
$$
LANGUAGE plpgsql;


