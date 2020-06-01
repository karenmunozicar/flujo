--Publica documento
delete from isys_querys_tx where llave='13703';

--Llamamos a Escribir Directo
insert into isys_querys_tx values ('13703',10,19,1,'select publica_documento_s3_13703(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('13703',20,19,1,'select sp_procesa_respuesta_cola_motor88_json(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,0,0);



CREATE or replace FUNCTION publica_documento_s3_13703(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2    varchar;
    data1       varchar;
    uri1	varchar;
    json2	json;
	j3	json;
    pos_inicial1	integer;
    pos_final1	integer;
    largo1	integer;
 
BEGIN
    xml2:=xml1;

    --El INPUT es un json en hexadecimal
    --Verificamos si es un json
    xml2:=put_campo(xml2,'__SECUENCIAOK__','20');

    uri1:=get_campo('URI_IN',xml2);
    --Si no hay uri  se ignora
    if (uri1='') then
	xml2:=put_campo(xml2,'INPUT','');
	xml2:=logapp(xml2,'Falla no viene uri');
	xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
	return xml2;
    end if;

    --Se limpia el Input
    data1:=get_campo('INPUT',xml2);
    largo1:=length(data1);
    pos_inicial1:=strpos(data1,'3c3f786d6c2076657273696f6e3d');
    --Buscamos al reves donde esta el primer signo > que en hex es 3e
    --Como se pone un reverse se busca e3
    --xml2:=logapp(xml2,'MAGIA:'||strpos(reverse(data1),'e3')::varchar);
    pos_final1:=largo1-pos_inicial1+4-strpos(reverse(data1),'e3');
    data1:=substring(data1,pos_inicial1,pos_final1);
    xml2 := put_campo(xml2,'INPUT_CUSTODIUM',data1);
    xml2 := put_campo(xml2,'LEN_INPUT_CUSTODIUM',length(data1)::varchar);

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
