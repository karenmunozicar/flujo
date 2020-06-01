--Flujo que hace un GET al S3, y devuelve el documento
delete from isys_querys_tx where llave='12717';
insert into isys_querys_tx values ('12717',10,1,1,'select proc_prepara_get_s3_12717(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('12717',20,1,2,'Llamada al Storage Writer',4011,104,107,0,0,100,100);

insert into isys_querys_tx values ('12717',100,1,1,'select proc_respuesta_get_s3_12717(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);


CREATE or replace FUNCTION proc_prepara_get_s3_12717(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2    varchar;
    url1	varchar;
    header1	varchar;
    host1	varchar;
    input1	varchar;
    data2	varchar;
BEGIN
    xml2:=xml1;
    --Ya se parseo el DTE
    --SCRIPT_URL /motor/almacen.fcgi/clg/1308/6e/6c08a5ce0b5484503fa2212efd678e813ff671
    url1:=split_part(split_part(get_campo('REQUEST_URI',xml2),'almacen.fcgi',2),'?k=',1)||'.gz';
    xml2:=logapp(xml2,'GET S3 '||url1);
    --xml2:=logapp(xml2,'GET S3 '||replace(xml2,'###',';'));
    host1:='almacen_acepta.s3.amazonaws.com';
	
    --Nunca vamos a buscar al S3 si la URI tiene el mes actual
    if (strpos(url1,'/'||to_char(now()-interval '5 month','YYMM')||'/')>0) then
	
	data2:='';
	xml2 := put_campo(xml2,'RESPUESTA_HEX',encode(('Status: 404 NK'||chr(10)|| 'Content-type: text/html; charset=iso-8859-1'||chr(10)|| 'Content-length: '||(length(data2)/2)::varchar||chr(10)||chr(10))::bytea,'hex')::varchar||data2);
	xml2:=logapp(xml2,'No vamos al S3 por el mes muy actual');
	return xml2;
    end if;
     --Debo Agregar el header a INPUT para que el resto funcione OK
    header1:='GET '||url1||' HTTP/1.1'||chr(10)||'Host: '||host1||chr(10)||'Content-Type: text/html'||chr(10)||'Content-Length: 0'||chr(10)||chr(10);
    xml2:=put_campo(xml2,'INPUT_CUSTODIUM',encode(header1::bytea,'hex')::varchar);
    xml2:=put_campo(xml2,'__IP_CONEXION_CLIENTE__',host1);
    xml2:=put_campo(xml2,'__SECUENCIAOK__','20');
    xml2:=logapp(xml2,'12717: Pide Xml S3 '||host1||' '||url1);
    --xml2:=logapp(xml2,header1);
    return xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION proc_respuesta_get_s3_12717(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2    varchar;
    data1       varchar;
    data2	varchar;
    file1       varchar;
    sts         integer;
    pos1	integer;
BEGIN
    xml2:=xml1;
    data1:=get_campo('XML_ALMACEN',xml1);
    xml2 := put_campo(xml2,'__SECUENCIAOK__','0');

    if (strpos(data1,encode('200 OK','hex'))>0) then
        xml2 := logapp(xml2,'12717: Respuesta S3 200 OK, Se Obtiene XML de URI='||get_campo('SCRIPT_URL',xml2));
        xml2:=put_campo(xml2,'FALLA_CUSTODIUM','NO');
	xml2 := put_campo(xml2,'RESPUESTA_HEX',data1);
	xml2:=respuesta_no_chunked(xml2);
	
	data1:=get_campo('RESPUESTA_HEX',xml2);
	pos1:=strpos(data1,'0a0a');
	data1:=substring(data1,pos1+4,length(data1));
	data2:=gunzip_string(data1);
		
	xml2 := put_campo(xml2,'RESPUESTA_HEX',encode(('Status: 200 OK'||chr(10)|| 'Content-type: text/html; charset=iso-8859-1'||chr(10)|| 'Content-length: '||(length(data2)/2)::varchar||chr(10)||chr(10))::bytea,'hex')::varchar||data2);
        --xml2 := logapp(xml2,'12717: '||get_campo('RESPUESTA_HEX',xml2));
    else
        xml2 := logapp(xml2,'12717: Falla Servicio S3, no se obtiene XML de URI='||get_campo('SCRIPT_URL',xml2));
        xml2:=put_campo(xml2,'FALLA_CUSTODIUM','SI');
	data2:='';
	xml2 := put_campo(xml2,'RESPUESTA_HEX',encode(('Status: 404 OK'||chr(10)|| 'Content-type: text/html; charset=iso-8859-1'||chr(10)|| 'Content-length: '||(length(data2)/2)::varchar||chr(10)||chr(10))::bytea,'hex')::varchar||data2);
        --xml2 := logapp(xml2,'12717: '||data1);
    end if;
    xml2:=put_campo(xml2,'INPUT_CUSTODIUM','');
    RETURN xml2;
END;
$$ LANGUAGE plpgsql;


