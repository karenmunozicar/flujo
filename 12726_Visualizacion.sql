--Flujo que hace un GET al S3, y devuelve el documento
delete from isys_querys_tx where llave='12726';
insert into isys_querys_tx values ('12726',10,1,1,'select proc_valida_origen_12726(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

--Obtiene la llamada a ca4webv3
insert into isys_querys_tx values ('12726',20,1,2,'Llamada Obtener el ca4webv3',4011,104,107,0,0,100,100);

insert into isys_querys_tx values ('12726',100,1,1,'select proc_respuesta_12726(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);


CREATE or replace FUNCTION proc_valida_origen_12726(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2    varchar;
    url1	varchar;
    header1	varchar;
    host1	varchar;
    input1	varchar;
    data2	varchar;
    rqu1	varchar;
BEGIN
    xml2:=xml1;
    xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
    xml2 := get_parametros_get(xml2);
    --xml2:=logapp(xml2,'12726:'||replace(xml2,'###',';'));
    rqu1:=get_campo('REQUEST_URI',xml2);
    url1:=split_part(rqu1,'/',5)||'/ca4webv3/index.jsp?url='||split_part(rqu1,'/',7);
    xml2:=logapp(xml2,'12726: QUERY_STRING:'||get_campo('QUERY_STRING',xml2));
	
    data2:='';
    host1:='entelpcs1401.acepta.com';
    xml2:=logapp(xml2,'12726:'||get_campo('url',xml2));
    url1:=replace(get_campo('url',xml2),'v01','ca4webv3');
    if (strpos(url1,'yui')>0) then
    	url1:=replace(get_campo('url',xml2),'v01','ca4webv3');
    elsif (strpos(url1,'HtmlView')>0) then
    	url1:=replace(get_campo('url',xml2),'v01','ca4webv3');
    elsif (strpos(url1,'AppToolBar')>0) then
    	url1:=replace(get_campo('url',xml2),'v01','ca4webv3');
    elsif (strpos(url1,'InfoView')>0) then
    	url1:=replace(get_campo('url',xml2),'v01','ca4webv3');
    else
    	--url1:='/ca4webv3/index.jsp?url=http://'||get_campo('host',xml2)||'/v01/'||split_part(rqu1,'/',5);
    	url1:='/ca4webv3/index.jsp?url=empty';
    	--url1:='/ca4webv3/index.jsp';
    end if;
    xml2:=logapp(xml2,'12726:'||rqu1);
    header1:='GET '||url1||' HTTP/1.1'||chr(10)||'Host: '||host1||chr(10)||'Content-Type: text/html'||chr(10)||'Content-Length: 0'||chr(10)||chr(10);
    xml2:=put_campo(xml2,'__IP_CONEXION_CLIENTE__',host1);
    xml2:=put_campo(xml2,'__SECUENCIAOK__','20');
    xml2:=logapp(xml2,'12726: '||header1);
    xml2:=put_campo(xml2,'INPUT_CUSTODIUM',encode(header1::bytea,'hex')::varchar);
    xml2:=logapp(xml2,'12726: Vamos a Buscar el ca4webv3');
    return xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION proc_respuesta_12726(varchar) RETURNS varchar AS $$
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
    xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
    data1:=get_campo('XML_ALMACEN',xml1);
    --xml2 := logapp(xml2,'12726: XML_ALMACEN='||decode(data1,'hex')::varchar); 
    xml2 := put_campo(xml2,'RESPUESTA_HEX',data1);
    --Le saco el chunked si viene
    xml2 := respuesta_no_chunked(xml2);
    --Envio como proxy todos los header recibidos
    --xml2 := proxy_respuesta_web(xml2);
    if (strpos(data1,encode('200 OK','hex'))>0) then
        xml2 := logapp(xml2,'12726: Respuesta 200 OK ca4webv3');
	/*xml2 := put_campo(xml2,'RESPUESTA_HEX',data1);
	xml2:=respuesta_no_chunked(xml2);
	data1:=get_campo('RESPUESTA_HEX',xml2);
         
	pos1:=strpos(data1,'0a0a');
	data2:=substring(data1,pos1+4,length(data1));
	--data2:=gunzip_string(data1);
		
	xml2 := put_campo(xml2,'RESPUESTA_HEX',encode(('Status: 200 OK'||chr(10)|| 'Vary: Accept-Encoding'||chr(10)||'Content-type: text/html; charset=UTF-8'||chr(10)|| 'Set-Cookie: JSESSIONID=6EE04E947B8C226E74B3A0E40F23314F; Path=/ca4webv3'||chr(10)||'Content-length: '||(length(data2)/2)::varchar||chr(10)||chr(10))::bytea,'hex')::varchar||data2);
        --xml2 := logapp(xml2,'12726: RESPUESTA_HEX='||get_campo('RESPUESTA_HEX',xml2));
	*/
    else
        xml2 := logapp(xml2,'12726: Falla Obtener ca4webv3');
    end if;
    xml2 := logapp(xml2,'12726: RESPUESTA_HEX='||decode(get_campo('RESPUESTA_HEX',xml2),'hex')::varchar);
    xml2:=put_campo(xml2,'INPUT_CUSTODIUM','');
    RETURN xml2;
END;
$$ LANGUAGE plpgsql;


