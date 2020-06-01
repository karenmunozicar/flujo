--Publica documento
delete from isys_querys_tx where llave='13702';

--Llamamos a Escribir Directo
insert into isys_querys_tx values ('13702',10,1,1,'select publica_documento_dec_13702(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('13702',20,19,1,'select sp_procesa_respuesta_cola_motor88_json(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,0,0);



CREATE or replace FUNCTION publica_documento_dec_13702(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2    varchar;
    data1       varchar;
    uri1	varchar;
    json2	json;
	j3	json;
BEGIN
    xml2:=xml1;

    --El INPUT es un json en hexadecimal
    --Verificamos si es un json
    xml2:=put_campo(xml2,'__SECUENCIAOK__','20');

    BEGIN
	json2:=decode(get_campo('INPUT',xml2),'hex')::varchar::json;
    exception when others then
	--Si falla no es un json, se borra
	xml2:=put_campo(xml2,'INPUT','');
	xml2:=logapp(xml2,'Falla INPUT no es un un Json ');
	xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
	return xml2;
    end;
	
    
    uri1:=get_json('URI_IN',json2);
    --Si no hay uri  se ignora
    if (uri1='') then
	xml2:=put_campo(xml2,'INPUT','');
	xml2:=logapp(xml2,'Falla no viene uri');
	xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
	return xml2;
    end if;
    j3:=json2;
    j3:=put_json(j3,'INPUT_CUSTODIUM','');
    insert into dec_1711 values (default,now(),uri1,j3);

    --Cambia la data de base64 a hex
    data1:=base642hex(get_json('INPUT_CUSTODIUM',json2));

    xml2:=put_campo(xml2,'INPUT_CUSTODIUM',data1);
    xml2:=put_campo(xml2,'RUT_OWNER','');
    xml2:=put_campo(xml2,'URI_IN',uri1);
    
    xml2:=graba_documento_s3(xml2);
    
    xml2:=put_campo(xml2,'INPUT','');
    xml2:=put_campo(xml2,'INPUT_CUSTODIUM','');
    xml2:=logapp(xml2,'Procesa URI='||uri1);
    xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
    return xml2;
END;
$$ LANGUAGE plpgsql;
