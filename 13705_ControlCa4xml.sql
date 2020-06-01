--Publica documento
delete from isys_querys_tx where llave='13705';

--Llamamos a Escribir Directo
insert into isys_querys_tx values ('13705',10,1,1,'select graba_control_ca4xml_13705(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('13705',20,19,1,'select sp_procesa_respuesta_cola_motor88_json(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,0,0);



CREATE or replace FUNCTION graba_control_ca4xml_13705(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2    varchar;
    data1       varchar;
    uri1	varchar;
    json2	json;
	j3	json;
	jdir	json;
	categoria1	varchar;
	total1	bigint;
	aux2	varchar;
	i	integer;
	aux	varchar;
	t1	timestamp;
BEGIN
    xml2:=xml1;

    --El INPUT es un json en hexadecimal
    --Verificamos si es un json
    xml2:=put_campo(xml2,'__SECUENCIAOK__','20');

    BEGIN
	json2:=decode_hex(get_campo('INPUT',xml2))::varchar::json;
    	jdir=get_json('Directorio',json2);
    exception when others then
	--Si falla no es un json, se borra
	xml2:=put_campo(xml2,'INPUT','');
	xml2:=logapp(xml2,'Falla INPUT no es un un Json ');
	xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
	return xml2;
    end;

    t1:=get_campo('FECHA_INGRESO_COLA',xml2)::timestamp;
	
    i:=0;

    aux2:=get_json('TOTAL',json2);
    if is_number(aux2) then
	    total1:=aux2::bigint;
    else
            total1:=null::bigint;
    end if;
 
   --Insertamos el Control por extension
   insert into grafico_control_ca4xml (dia,fecha,rut_emisor,directorio,categoria,total,info,fecha_ingreso) values (to_char(t1,'YYYYMMDD')::integer,t1,get_json('rut_cliente',jdir),get_json('path',jdir),get_json('categoria',jdir),total1,json2::varchar,now());
    
    xml2:=put_campo(xml2,'INPUT','');
    xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
    return xml2;
END;
$$ LANGUAGE plpgsql;
