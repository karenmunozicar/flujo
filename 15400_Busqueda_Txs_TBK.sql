delete from isys_querys_tx where llave='15400';
delete from isys_querys_tx where llave in ('15401','15402','15403');
delete from isys_querys_tx where llave in ('15405','15406','15407');

insert into isys_querys_tx values ('15400',10,1,9,'<EJECUTA0>15401</EJECUTA0><EJECUTA1>15402</EJECUTA1><EJECUTA2>15403</EJECUTA2><TIMEOUT>5</TIMEOUT>',0,0,0,1,1,20,20);
--Motor13
insert into isys_querys_tx values ('15401',10,1913,1,'$$QUERY_COUNT_M13$$',0,0,0,9,1,15,15);
insert into isys_querys_tx values ('15401',15,1,14,'{"f":"INSERTA_JSON","p1":{"COUNT_MOTOR13":$$RES_JSON_1$$}}',0,0,0,0,0,0,0);
--Motor14
insert into isys_querys_tx values ('15402',10,1914,1,'$$QUERY_COUNT_M14$$',0,0,0,9,1,15,15);
insert into isys_querys_tx values ('15402',15,1,14,'{"f":"INSERTA_JSON","p1":{"COUNT_MOTOR14":$$RES_JSON_1$$}}',0,0,0,0,0,0,0);
--Redshift
insert into isys_querys_tx values ('15403',10,22,1,'$$QUERY_COUNT_RS$$',0,0,0,9,1,15,15);
insert into isys_querys_tx values ('15403',15,1,14,'{"f":"INSERTA_JSON","p1":{"COUNT_RS":$$RES_JSON_1$$}}',0,0,0,0,0,0,0);

insert into isys_querys_tx values ('15400','20',19,1,'select armo_query_rs_15400(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('15400',30,1,9,'<EJECUTA0>15405</EJECUTA0><EJECUTA1>15406</EJECUTA1><EJECUTA2>15407</EJECUTA2><TIMEOUT>15</TIMEOUT>',0,0,0,1,1,40,40);
--Motor13
insert into isys_querys_tx values ('15405',10,1913,1,'$$QUERY_MOTOR13$$',0,0,0,9,1,15,15);
insert into isys_querys_tx values ('15405',15,1,14,'{"f":"INSERTA_JSON","p1":{"RES_MOTOR13":$$RES_JSON_1$$}}',0,0,0,0,0,0,0);
--Motor14
insert into isys_querys_tx values ('15406',10,1914,1,'$$QUERY_MOTOR14$$',0,0,0,9,1,15,15);
insert into isys_querys_tx values ('15406',15,1,14,'{"f":"INSERTA_JSON","p1":{"RES_MOTOR14":$$RES_JSON_1$$}}',0,0,0,0,0,0,0);
--Redshift
insert into isys_querys_tx values ('15407',10,22,1,'$$QUERY_RS$$',0,0,0,9,1,15,15);
insert into isys_querys_tx values ('15407',15,1,14,'{"f":"INSERTA_JSON","p1":{"RES_RS":$$RES_JSON_1$$}}',0,0,0,0,0,0,0);

insert into isys_querys_tx values ('15400','40',19,1,'select armo_resultado_15400(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

CREATE OR REPLACE FUNCTION public.app_dinamica_txs_tbk(json)
 RETURNS json
AS $$
DECLARE
	json1			alias for $1;
	json2			json;
	filtro1		varchar;
	f1	date;
	f2	date;
	aux1	varchar;
	jaux	json;
	json3	json;
	query1	varchar;
	offset1	integer;
	offset2	integer;
	cant_regs1	integer;
	total1	integer;
	query_base1	varchar;
BEGIN
        json2:=json1;
	return app_dinamica_txs_tbk_v1(json2);
	if get_json('rutUsuario',json2)<>'17597643' then
		return app_dinamica_txs_tbk_v1(json2);
	end if;
	BEGIN
		f1:=get_json('dia',json2)::date;
		f2:=get_json('dia_hasta',json2)::date+interval '1 day';
	EXCEPTION WHEN OTHERS THEN
       		return response_requests_6000('2', 'Falla Formato Fecha', '',json2);
	END;
	filtro1:=' where fecha_ingreso>='''||f1::varchar||''' and fecha_ingreso<'''||f2::varchar||''' ';
	if get_json('cliente',json2)<>'' then
		filtro1:=filtro1||' and rut_emisor='''||trim(replace(get_json('cliente',json2),'.',''))||''' ';
	end if;
	aux1:=get_json('filtro',json2);
	if aux1<>'' then
		filtro1:=filtro1||' and (id_tbk='''||encode(aux1::bytea,'base64')||''' or folio='''||aux1||''') ';
	end if;
	if get_json('estado',json2)<>'' then
		filtro1:=filtro1||' and estado='''||trim(replace(get_json('estado',json2),'.',''))||''' ';
	end if;
	cant_regs1:=100;
	if get_json('offset',json2)='' or is_number(get_json('offset',json2)) is false then
		offset1:=1;
	else
		offset1:=get_json('offset',json2)::integer;
	end if;
	offset2:=(offset1::integer-1)*cant_regs1;

        json3:='{}';
        json3:=put_json(json3,'titulo','Maestro de Clientes Acepta');
	json3:=put_json(json3,'flag_paginacion','NO');
        json3:=put_json(json3,'flag_paginacion_manual','SI');
	json3:=put_json(json3,'mostrar_mensaje_paginacion','SI');
	json3:=put_json(json3,'mostrar_paginador','false'); -- no muestra el paginador del datatables
	json3:=put_json(json3,'mostrar_info','false'); -- no muestra el mensaje de paginación
	json3:=put_json(json3,'mostrar_filtros','false'); -- no muestra el buscar del plugin
        json3:=put_json(json3,'cantregs',cant_regs1::varchar);
	json3:=put_json(json3,'offset',offset1::varchar);

        json3:=put_json(json3,'flag_tipo_cuadro','GRILLA');
        json3:=put_json(json3,'cantregs',cant_regs1::varchar);
        json3:=put_json(json3,'MENSAJE_DISPLAY','');

	json2:=put_json(json2,'FILTRO',filtro1);
	json2:=put_json(json2,'offset2',offset2::varchar);
	json2:=put_json(json2,'offset1',offset1::varchar);
	json2:=put_json(json2,'cant_regs1',cant_regs1::varchar);
	json2:=put_json(json2,'QUERY_COUNT_M13','select count(*),max(id) from transacciones_tbk '||filtro1||' and fecha_ingreso>='''||to_char(now(),'YYYY-MM-DD')||''' ');	
	json2:=put_json(json2,'QUERY_COUNT_M14','select count(*),max(id) from transacciones_tbk '||filtro1||' and fecha_ingreso>='''||to_char(now(),'YYYY-MM-DD')||''' ');	
	json2:=put_json(json2,'QUERY_COUNT_RS','select count(*),max(id) from transacciones_tbk '||filtro1);	
	json2:=put_json(json2,'JSON3',json3::varchar);
	json2:=put_json(json2,'LLAMA_FLUJO','SI');
	json2:=put_json(json2,'__SECUENCIAOK__','15400');
        RETURN json2;

END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION public.armo_query_rs_15400(json)
 RETURNS json
AS $$
DECLARE
        json1                   alias for $1;
        json2                   json;
	j13			json;
	j14			json;
	jrs			json;
	json3			json;
	count_rs1		bigint;
	count13			bigint;
	count14			bigint;
	cant_regs1	integer;
	offset2		bigint;

	paginas_base1	integer;
	sobra_base1	integer;
	paginas_base2	integer;
	sobra_base2	integer;

	off13		bigint;
	limit13		integer;
	off14		bigint;
	limit14		integer;
	off_rs		bigint;
	limit_rs	integer;

	query1		varchar;
	query_base1	varchar;
	jaux		json;
	total1		bigint;
	filtro1		varchar;
	--max13		bigint;
	--max14		bigint;
BEGIN
        json2:=json1;
	json2:=put_json(json2,'__SECUENCIAOK__','0');
	json3:=get_json('JSON3',json2)::json;
	cant_regs1:=get_json('cant_regs1',json2);
	offset2:=get_json('offset2',json2);
	BEGIN
		j13:=get_json('COUNT_MOTOR13',json2);
		j14:=get_json('COUNT_MOTOR14',json2);
		jrs:=get_json('COUNT_RS',json2);
		if get_json('STATUS',j13)<>'OK' or get_json('STATUS',j14)<>'OK' or get_json('STATUS',jrs)<>'OK' then
			json3:=put_json(json3,'datos_tabla','[]');
			json2:=response_requests_6000('1', 'Falla Busqueda.', json3::varchar,json2);
			return json2;
		end if;
	EXCEPTION WHEN OTHERS THEN
		json3:=put_json(json3,'datos_tabla','[]');
		json2:=response_requests_6000('1', 'Falla Busqueda.-', json3::varchar,json2);
		return json2;
	END;
	json2:=logjson(json2,'j13='||j13::varchar||' j14='||j14::varchar||' jrs='||jrs::varchar);
	count13:=get_json('count',j13);
	count14:=get_json('count',j14);
	--max13:=get_json('max',j13);
	--max14:=get_json('max',j14);
	count_rs1:=get_json('count',jrs);
	total1:=count13+count14+count_rs1;
	
	json3:=put_json(json3,'total_regs',total1::varchar);
	json3:=put_json(json3,'cantidad_paginas',(total1/cant_regs1+case when total1%cant_regs1>0 then 1 else 0 end)::varchar);
	json3:=put_json(json3,'cant_paginas',(total1/cant_regs1+case when total1%cant_regs1>0 then 1 else 0 end)::varchar);
	json2:=put_json(json2,'JSON3',json3::varchar);
	
	paginas_base1:=count13/cant_regs1;
        sobra_base1:=count13%cant_regs1;
	paginas_base2:=count14/cant_regs1;
        sobra_base2:=count14%cant_regs1;
	off13:=-1;
	limit13:=-1;
	off14:=-1;
	limit14:=-1;
	off_rs:=-1;
	limit_rs:=-1;
	--Si me alcanza con la primera base...
	if (offset2+cant_regs1<=paginas_base1*cant_regs1) then
		off13:=offset2;
		limit13:=cant_regs1;
	--Si estoy en el cambio de base...
	elsif sobra_base1>0 and offset2=(paginas_base1)*cant_regs1 then
		off13:=offset2;
		limit13:=cant_regs1;
		off14:=0;
		if count14>=cant_regs1-sobra_base1 then
			limit14:=cant_regs1-sobra_base1;
		else
			limit14:=count14;
			off_rs:=0;
			limit_rs:=cant_regs1-sobra_base1-count14;
		end if;
	--Si ya mostre tanto la primera como la segunda base...
	elsif offset2>=count13+count14 then
		off_rs:=offset2-paginas_base1*cant_regs1-sobra_base1-paginas_base2*cant_regs1-sobra_base2;
		limit_rs:=cant_regs1;
	--Solo segunda base...
	elsif offset2>count13 and offset2<count13+count14 then
		off14:=offset2-paginas_base1*cant_regs1-sobra_base1;
		if off14+cant_regs1<=count14 then
			limit14:=cant_regs1;
		else
			if count14-off14>0 then
				limit14:=count14-off14;
				off_rs:=0;
				limit_rs:=cant_regs1-limit14;
			else
				off_rs:=0;
				limit_rs:=cant_regs1;
			end if;
		end if;
	end if;
				
	json2:=logjson(json2,'off13='||off13::varchar||' limit13='||limit13||' off14='||off14::varchar||' limit14='||limit14::varchar||' off_rs='||off_rs::varchar||' limit_rs='||limit_rs::varchar);
	filtro1:=get_json('FILTRO',json2);
	query_base1:='select id as info__id__on,fecha_ingreso as info__fecha__on,rut_emisor as info__rut_emisor__on, folio||''__''||coalesce(uri,'''') as info__folio__on,estado as info_sin_formato__estado__on,round(EXTRACT(EPOCH FROM (fecha_termino-fecha_ingreso))::numeric,2) as info__tiempo_tx_ted__on,fecha_publicacion as info__fecha_publicacion__on,decode(id_tbk,''base64'') as info_sin_formato__id_tbk__on,mensaje as info__mensaje_err__on,case when uri is null or uri='''' then mensaje_publicacion else '''' end as info__mensaje_publicacion__on from transacciones_tbk '||filtro1||' ';
	if off13=-1 then
		json2:=put_json(json2,'FLAG_QUERY_MOTOR13','NO');
		json2:=put_json(json2,'QUERY_MOTOR13','select 1=0');
	else
		json2:=put_json(json2,'FLAG_QUERY_MOTOR13','SI');
		json2:=put_json(json2,'QUERY_MOTOR13',query_base1||' and fecha_ingreso>='''||to_char(now(),'YYYY-MM-DD')||''' order by fecha_ingreso desc offset '||off13::varchar||' limit '||limit13::varchar);
	end if;
	if off14=-1 then
		json2:=put_json(json2,'FLAG_QUERY_MOTOR14','NO');
		json2:=put_json(json2,'QUERY_MOTOR14','select 1=0');
	else
		json2:=put_json(json2,'FLAG_QUERY_MOTOR14','SI');
		json2:=put_json(json2,'QUERY_MOTOR14',query_base1||' and fecha_ingreso>='''||to_char(now(),'YYYY-MM-DD')||''' order by fecha_ingreso desc offset '||off14::varchar||' limit '||limit14::varchar);
	end if;
	if off_rs=-1 then
		json2:=put_json(json2,'FLAG_QUERY_RS','NO');
		json2:=put_json(json2,'QUERY_RS','select 1=0');
	else
		json2:=put_json(json2,'FLAG_QUERY_RS','SI');
		json2:=put_json(json2,'QUERY_RS',query_base1||' order by fecha_ingreso desc offset '||off_rs::varchar||' limit '||limit_rs::varchar);
	end if;
	json2:=put_json(json2,'__SECUENCIAOK__','30');
	RETURN json2;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION public.armo_resultado_15400(json)
 RETURNS json
AS $$
declare
        json1                alias for $1;
        json2                   json;
	json3		json;
	off_local	bigint;
	limit_local	integer;
	v_out_resultado	varchar;
	query1		varchar;
	jaux		json;
	j13		json;
	j14		json;
	jrs		json;
BEGIN
	json2:=json1;
	json3:=get_json('JSON3',json2)::json;
	
	BEGIN
		j13:='[]';
		j14:='[]';
		jrs:='[]';
		if get_json('FLAG_QUERY_MOTOR13',json2)='SI' then
			j13:=get_json('RES_MOTOR13',json2);
			if get_json('LISTA',j13)<>'' then
				j13:=get_json('LISTA',j13);
			else
				j13:=('['||replace(get_json('RES_MOTOR13',json2),', "STATUS": "OK", "TOTAL_REGISTROS": "1"','')||']')::json;
			end if;
		end if;
		if get_json('FLAG_QUERY_MOTOR14',json2)='SI' then
			j14:=get_json('RES_MOTOR14',json2);
			if get_json('LISTA',j14)<>'' then
				j14:=get_json('LISTA',j14);
			else
				j14:=('['||replace(get_json('RES_MOTOR14',json2),', "STATUS": "OK", "TOTAL_REGISTROS": "1"','')||']')::json;
			end if;
		end if;
		if get_json('FLAG_QUERY_RS',json2)='SI' then
			jrs:=get_json('RES_RS',json2);
			if get_json('LISTA',jrs)<>'' then
				jrs:=get_json('LISTA',jrs);
			else
				jrs:=('['||replace(get_json('RES_RS',json2),', "STATUS": "OK", "TOTAL_REGISTROS": "1"','')||']')::json;
			end if;
		end if;
        EXCEPTION WHEN OTHERS THEN
                json3:=put_json(json3,'datos_tabla','[]');
                json2:=response_requests_6000('1', 'Falla Busqueda.-.-', json3::varchar,json2);
                return json2;
        END;
	jaux:=json_merge_lists(j13::varchar,j14::varchar);
	jaux:=json_merge_lists(jaux::varchar,jrs::varchar);
	json3:=put_json(json3,'datos_tabla',jaux::varchar);
	json2:=response_requests_6000('1', 'OK',json3::varchar,json2);
	return json2;
end
$$ LANGUAGE plpgsql;

