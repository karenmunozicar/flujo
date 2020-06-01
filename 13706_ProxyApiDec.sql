delete from isys_querys_tx where llave='13706';
--Llamamos a Escribir Directo
insert into isys_querys_tx values ('13706',10,19,1,'select graba_cola_13706(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('13706',20,1,2,'Llamada a Escribir en Almacen',9017,104,200,0,0,30,30);
insert into isys_querys_tx values ('13706',30,19,1,'select analiza_respuesta_escritura_13706(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
--insert into isys_querys_tx values ('13706',20,19,1,'select sp_procesa_respuesta_cola_motor88_json(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,0,0);

CREATE or replace FUNCTION graba_cola_13706(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2    varchar;
	file1	varchar;
BEGIN
    	xml2:=xml1;
	
	file1:='test';
	xml2:=put_campo(xml2,'INPUT_CUSTODIUM',encode('test_dao','hex'));
        xml2:=put_campo(xml2,'LEN_INPUT_CUSTODIUM',length(encode('test_dao','hex'))::varchar);
	xml2:=put_campo(xml2,'ALMACEN','/opt/acepta/motor/dec/'||file1||'.tmp');
        xml2:=put_campo(xml2,'INPUT_CUSTODIUM','02'||encode('<TX=4>9015<INPUT='::bytea,'hex')||encode(((get_campo('LEN_INPUT_CUSTODIUM',xml2)::integer/2)::varchar||'>')::bytea,'hex')||get_campo('INPUT_CUSTODIUM',xml2)||encode(('<ALMACEN='||length(get_campo('ALMACEN',xml2))::varchar||'>'||get_campo('ALMACEN',xml2))::bytea,'hex')||'03');
	--xml2:=put_campo(xml2,'ALMACEN','/opt/acepta/motor/dec/'||file1||'.tmp');
    	--xml2:=put_campo(xml2,'SCRIPT_EDTE','/bin/mv /opt/acepta/motor/dec/'||file1||'.tmp /opt/acepta/motor/dec/'||file1||'.ok');
    	xml2:=put_campo(xml2,'__SECUENCIAOK__','20');

    	return xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION analiza_respuesta_escritura_13706(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2    varchar;
        file1   varchar;
BEGIN
        xml2:=xml1;
	xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK'||chr(10)||'Content-type: text/html'||chr(10)||'Content-length: '||length('OK')||chr(10)||chr(10)||'OK');
        return xml2;
END;
$$ LANGUAGE plpgsql;
