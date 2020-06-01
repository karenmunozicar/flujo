delete from isys_querys_tx where llave='15100';
delete from isys_querys_tx where llave='15101';
delete from isys_querys_tx where llave='15102';

insert into isys_querys_tx values ('15100',10,9,1,'select pivote_dashboard_regional_15100(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('15100',20,9,1,'select dashboard_pais_empresas_15100(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('15100',30,1,9,'<EJECUTA0>15101</EJECUTA0><EJECUTA1>15102</EJECUTA1><TIMEOUT>15</TIMEOUT>',0,0,0,1,1,40,40);
insert into isys_querys_tx values ('15100',40,9,1,'select respuesta_dashboard_regional_15100(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('15100',50,1,9,'<EJECUTA0>15101</EJECUTA0><EJECUTA1>15102</EJECUTA1><TIMEOUT>15</TIMEOUT>',0,0,0,1,1,50,50);
insert into isys_querys_tx values ('15100',60,9,1,'select respuesta_dashboard_linea_negocio_15100(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('15100',70,9,1,'select armo_graficos_respuesta_15100(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('15101',10,9,1,'select select_emitidos_peru_15101(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('15102',10,9,1,'select select_emitidos_chile_15101(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

CREATE or replace FUNCTION pivote_dashboard_regional_15100(json) RETURNS json AS $$
DECLARE
        json2   json;
        json1   alias for $1;
BEGIN
        json2:=json1;
        json2 := put_json(json2,'__SECUENCIAOK__','0');

	--Si estoy en algun PAIS...
	if() then
		json2 := put_json(json2,'__SECUENCIAOK__','20');
		return json2;
	--Si estoy en el regional...
	elsif() then
		json2:=put_json(json2,'REGIONAL','SI');
		json2 := put_json(json2,'__SECUENCIAOK__','30');
		return json2;
	--Si estoy en una linea de negocio...
	elsif() then
		json2:=put_json(json2,'LINEA_NEGOCIO','SI');
		json2 := put_json(json2,'__SECUENCIAOK__','50');
		return json2;
	end if;

        return response_requests_6000('2', 'Fallan parámetros', '', json2);
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION dashboard_pais_empresas_15100(json) RETURNS json AS $$
DECLARE
        json2   json;
        json1   alias for $1;
BEGIN
        json2:=json1;
	json2 := put_json(json2,'__SECUENCIAOK__','70');
        return json2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION select_emitidos_peru_15101(json) RETURNS json AS $$
DECLARE
        json2   json;
        json1   alias for $1;
	json4	json;
	query1	varchar;
BEGIN
        json2:=json1;

	if(get_json('REGIONAL',json2)='SI') then
		query1:='';
	else
		query1:='';
	end if;
	json_par1:=get_parametros_motor_json('{}','BASE_REDSHIFT_EMITIDOS_PERU');
	json4:='{}';
	json3:=query_db_json(get_json('__IP_CONEXION_CLIENTE__',json_par1),get_json_int('__IP_PORT_CLIENTE__',json_par1)::integer,query1);
        if (get_json('STATUS',json3)<>'OK') then
                json4:=logjson(json4,'Falla Busqueda');
                json4:=put_json(json4,'EMITIDOS_PERU','[]');
                return json4;
        end if;
	
	--json3 resultado query
	json4:=put_json('{}','EMITIDOS_PERU',json3::varchar);
        return json4;	
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION select_emitidos_chile_15101(json) RETURNS json AS $$
DECLARE
        json2   json;
        json1   alias for $1;
        json4   json;
BEGIN
        json2:=json1;
	
	if(get_json('REGIONAL',json2)='SI') then
		query1:='';
	else
		query1:='';
	end if;
	json_par1:=get_parametros_motor_json('{}','BASE_REDSHIFT_EMITIDOS');
	json4:='{}';
	json3:=query_db_json(get_json('__IP_CONEXION_CLIENTE__',json_par1),get_json_int('__IP_PORT_CLIENTE__',json_par1)::integer,query1);
        if (get_json('STATUS',json3)<>'OK') then
                json4:=logjson(json4,'Falla Busqueda');
                json4:=put_json(json4,'EMITIDOS_PERU','[]');
                return json4;
        end if;

        --json3 resultado query
        json4:=put_json('{}','EMITIDOS_CHILE',json3::varchar);
        return json4;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION respuesta_dashboard_regional_15100(json) RETURNS json AS $$
DECLARE
        json2   json;
        json1   alias for $1;
        json_peru   json;
	json_chile	json;
BEGIN
        json2:=json1;
	begin
                json_peru:=get_json('EMITIDOS_PERU',json2);
        exception when others then
                json_peru:='[]';
        end;
	begin
                json_chile:=get_json('EMITIDOS_CHILE',json2);
        exception when others then
                json_chile:='[]';
        end;

	json2 := put_json(json2,'__SECUENCIAOK__','70');
        return json2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION respuesta_dashboard_linea_negocio_15100(json) RETURNS json AS $$
DECLARE
        json2   json;
        json1   alias for $1;
        json_peru   json;
        json_chile      json;
BEGIN
        json2:=json1;
        begin
                json_peru:=get_json('EMITIDOS_PERU',json2);
        exception when others then
                json_peru:='[]';
        end;
        begin
                json_chile:=get_json('EMITIDOS_CHILE',json2);
        exception when others then
                json_chile:='[]';
        end;

	json2 := put_json(json2,'__SECUENCIAOK__','70');
        return json2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION armo_graficos_respuesta_15100(json) RETURNS json AS $$
DECLARE
        json2   json;
        json1   alias for $1;
	json_out	json;
	lista1		json;
	j	integer;
	aux1	varchar;
	json_aux1	json;
	patron_grafico	varchar;
BEGIN
        json2:=json1;
        json2 := put_json(json2,'__SECUENCIAOK__','0');
	json_out:='[]';

	if (get_json('__LISTA_GRAFICOS__',out_query1)<>'') then
		lista1:=get_json('__LISTA_GRAFICOS__',out_query1);
		j:=0;
		aux1:=get_json_index(lista1,j);
		while aux1<>'' loop
			json_aux1:=aux1::json;
			patron_grafico:=pg_read_file('./patrones_higcharts/'||get_json('PATRON',json_aux1));
			if (patron_grafico='' or patron_grafico is null) then
				json2:=response_requests_6000('2', 'No existe patron grafico','', json2);
				return json2;
			end if;
			campo.id:=campo.id||j::varchar;
			json_aux:=put_json(json_aux,'container_id',get_json('ID',json_aux1));
			json_aux1:=put_json(json_aux1,'ID_DIV',get_json('ID',json_aux1));
			patron_grafico:=remplaza_tags_json_c(json_aux1,patron_grafico);
			patron_grafico:=limpia_tags(patron_grafico);
			json_aux:=put_json(json_aux,'PATRON',patron_grafico);	
			json_aux:=put_json(json_aux,'CLASE',get_json('CLASE',json_aux1));
			json_out:=put_json_list(json_out,json_aux);
			j:=j+1;
			aux1:=get_json_index(lista1,j);
		end loop;
	else
		return response_requests_6000('2','Falla generación graficos','',json2);
	end if;

        return response_requests_6000('1', '', json_out::varchar, json2);
END;
$$ LANGUAGE plpgsql;


