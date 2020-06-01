/*delete from isys_querys_tx where llave='14720';
delete from isys_querys_tx where llave='14711';
delete from isys_querys_tx where llave='14712';
*/
delete from isys_querys_tx where llave='14720';
delete from isys_querys_tx where llave='14721';
--Consultamos en la base de traza si el DTE ya esta publicado
insert into isys_querys_tx values ('14720',5,1,1,'select pivote_14720(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('14720',10,1,9,'$$MULTI_PROCESO$$',0,0,0,1,1,20,20);

--Pivote para armar respuesta
insert into isys_querys_tx values ('14720',20,1,1,'select valida_respuesta_14720(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('14721',10,1,1,'$$QUERY$$',0,0,0,1,1,-1,0);

CREATE or replace FUNCTION busqueda_folio_multihilo(json) RETURNS json AS $$
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
        json2:=put_json(json2,'__SECUENCIAOK__','14720');
        return json2;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION pivote_14720(json) RETURNS json AS $$
declare
        json2   json;
        json1   alias for $1;

	categoria1	varchar;
	query1		varchar;
	multi_proceso1	varchar;
	i		integer;
	campo		record;
begin
	json2:=json1;	
	categoria1:=get_json('CATEGORIA',json2);
	query1:=get_json('QUERY',json2);

	if(categoria1='EMITIDOS') then
		i:=0;
		multi_proceso1:='';
		for campo in select * from config_tabla_emitidos loop
			multi_proceso1:=multi_proceso1||'<EJECUTA'||i::varchar||'></EJECUTA'||i::varchar||'>';	
			i:=i+1;	
		end loop;	
		multi_proceso1:=multi_proceso1||'<TIMEOUT>15</TIMEOUT>';
	elsif(categoria1='EMITIDOS_IMPORTADOS') then
		i:=0;
		multi_proceso1:='';
		for campo in select * from config_tabla_emitidos_importados loop
			multi_proceso1:=multi_proceso1||'<EJECUTA'||i::varchar||'></EJECUTA'||i::varchar||'>';	
			i:=i+1;	
		end loop;	
		multi_proceso1:=multi_proceso1||'<TIMEOUT>15</TIMEOUT>';
	elsif(categoria1='RECIBIDOS') then
		i:=0;
		multi_proceso1:='';
		for campo in select * from config_tabla_recibidos loop
			multi_proceso1:=multi_proceso1||'<EJECUTA'||i::varchar||'></EJECUTA'||i::varchar||'>';	
			i:=i+1;	
		end loop;	
		multi_proceso1:=multi_proceso1||'<TIMEOUT>15</TIMEOUT>';
	elsif(categoria1='RECIBIDOS_IMPORTADOS') then
		i:=0;
                multi_proceso1:='';
                for campo in select * from config_tabla_recibidos_importados loop
                        multi_proceso1:=multi_proceso1||'<EJECUTA'||i::varchar||'></EJECUTA'||i::varchar||'>';
                        i:=i+1;
                end loop;
                multi_proceso1:=multi_proceso1||'<TIMEOUT>15</TIMEOUT>';
	elsif(categoria1='BOLETAS_IMPORTADOS') then
		i:=0;
                multi_proceso1:='';
                for campo in select * from config_tabla_boletas_importados loop
                        multi_proceso1:=multi_proceso1||'<EJECUTA'||i::varchar||'></EJECUTA'||i::varchar||'>';
                        i:=i+1;
                end loop;
                multi_proceso1:=multi_proceso1||'<TIMEOUT>15</TIMEOUT>';
	elsif(categoria1='BOLETAS') then
		i:=0;
                multi_proceso1:='';
                for campo in select * from config_tabla_boletas loop
                        multi_proceso1:=multi_proceso1||'<EJECUTA'||i::varchar||'></EJECUTA'||i::varchar||'>';
                        i:=i+1;
                end loop;
                multi_proceso1:=multi_proceso1||'<TIMEOUT>15</TIMEOUT>';
	end if;
	json2 := put_json(json2,'__SECUENCIAOK__','10');
	--json2 := put_json(json2,'MULTI_PROCESO',multi_proceso1);
	json2 := put_json(json2,'MULTI_PROCESO','<EJECUTA0>14721</EJECUTA0><TIMEOUT>15</TIMEOUT>');
	json2 := put_json(json2,'QUERY','SELECT array_to_json(array_agg(row_to_json(sql))) FROM (select 1)sql');
	return json2;
end;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION valida_respuesta_14720(json) RETURNS json AS $$
DECLARE
        json2   json;
        json1   alias for $1;
BEGIN
        json2:=json1;
        return response_requests_6000('1', '', json2::varchar, json2);
END;
$$ LANGUAGE plpgsql;
