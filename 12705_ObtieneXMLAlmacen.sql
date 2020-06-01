--A partir de una URI (URI_IN)
--Obtiene el XML desde al almacen haciendo un GET y luego deja la respuesta en hex en el tag XML_ALMACEN
delete from isys_querys_tx where llave='12705';
insert into isys_querys_tx values ('12705',10,1,1,'select proc_prepara_get_almacen_12705(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('12705',20,1,2,'Llamada al Storage Writer',4011,104,107,0,0,100,100);

insert into isys_querys_tx values ('12705',100,1,1,'select proc_respuesta_get_almacen_12705(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);


CREATE or replace FUNCTION proc_prepara_get_almacen_12705(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2    varchar;
    url1	varchar;
    header1	varchar;
    host1	varchar;
    periodo1	varchar;
BEGIN
    xml2:=xml1;
    --Ya se parseo el DTE
    --xml2:=logapp(xml2,'TAG_URL_12705='||get_campo('TAG_URL_12705',xml2));
    if (length(get_campo('TAG_URL_12705',xml2))>0) then
        url1:=get_campo('TAG_URL_12705',xml2);
        host1:=get_campo('TAG_HOST_12705',xml2);
     	--Debo Agregar el header a INPUT para que el resto funcione OK
	header1:='GET '||url1||' HTTP/1.1'||chr(10)||'Host: '||host1||chr(10)||'Content-Type: text/html'||chr(10)||'Content-Length: 0'||chr(10)||chr(10);
	xml2:=put_campo(xml2,'INPUT_CUSTODIUM',encode(header1::bytea,'hex')::varchar);
        xml2:=put_campo(xml2,'__SECUENCIAOK__','20');
        xml2:=logapp(xml2,'GET a '||host1||' '||url1);
	xml2:=put_campo(xml2,'__IP_CONEXION_CLIENTE__',host1);
        return xml2;
    else
        url1:='/depot'||split_part(replace(get_campo('URI_IN',xml2),'v01','depot'),'/depot',2);
        host1:=split_part(split_part(get_campo('URI_IN',xml2),'//',2),'/',1);
     	--Debo Agregar el header a INPUT para que el resto funcione OK
	--header1:='GET '||url1||' HTTP/1.1'||chr(10)||'Host: '||host1||chr(10)||'Content-Type: text/html'||chr(10)||'Content-Length: 0'||chr(10)||chr(10);
	header1:='GET '||url1||' HTTP/1.1'||chr(10)||'Host: '||host1||chr(10)||'User-Agent: curl/7.38.0'||chr(10)||'Accept: */*'||chr(10)||chr(10);
	xml2:=put_campo(xml2,'INPUT_CUSTODIUM',encode(header1::bytea,'hex')::varchar);
	xml2:=logapp(xml2,'GET a '||header1);
    end if;

    xml2:=put_campo(xml2,'__IP_PORT_CLIENTE__','80');
    --RME 2015-03-30 Para documentos antiguos se fuerza la 172.16.10.182
    periodo1:=right(split_part(get_campo('URI_IN',xml2),'.',1),4);
    if (is_number(periodo1) is false) then
	xml2:=put_campo(xml2,'__IP_CONEXION_CLIENTE__',host1);
    elsif periodo1::integer>=1701 then
	--xml2:=put_campo(xml2,'__IP_CONEXION_CLIENTE__','172.16.14.69');
	-- Cambio proxy_almacen
	xml2:=put_campo(xml2,'__IP_CONEXION_CLIENTE__','172.16.14.89');
    elsif periodo1::integer>1407 then
	xml2:=put_campo(xml2,'__IP_CONEXION_CLIENTE__','172.16.10.131');
    else	
	xml2:=put_campo(xml2,'__IP_CONEXION_CLIENTE__',host1);
    end if;
    xml2:=put_campo(xml2,'__SECUENCIAOK__','20');
    xml2:=logapp(xml2,'Pide XML Original '||host1||' '||get_campo('URI_IN',xml2)||' '||get_campo('__IP_CONEXION_CLIENTE__',xml2));
    xml2:=put_campo(xml2,'FALLA_CUSTODIUM','SI');
    return xml2;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION proc_respuesta_get_almacen_12705(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2    varchar;
    data1       varchar;
    file1       varchar;
    sts         integer;
	uri1	varchar;
	pos1	integer;
BEGIN
    xml2:=xml1;
    data1:=get_campo('XML_ALMACEN',xml1);
    xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
    if length(get_campo('URI_IN_ANTERIOR',xml2)) > 0 then
	xml2:=put_campo(xml2,'URI_IN',get_campo('URI_IN_ANTERIOR',xml2));
    end if;
    --if (strpos(data1,encode('200 OK','hex'))>0) then
    --RME 20160530 Se agrega validacion del termino del documento, para evitar que llegue cortado.
    if (strpos(data1,encode('200 OK','hex'))>0 and strpos(data1,encode('</Content>','hex'))>0) then
        xml2 := logapp(xml2,'Respuesta Custodium 200 OK, Se Obtiene XML de URI='||get_campo('URI_IN',xml2)||' Largo='||(length(data1)/2)::varchar);
        xml2:=put_campo(xml2,'FALLA_CUSTODIUM','NO');
	xml2 := put_campo(xml2,'RESPUESTA_HEX',data1);
	xml2:=respuesta_no_chunked(xml2);
	xml2 := put_campo(xml2,'XML_ALMACEN',get_campo('RESPUESTA_HEX',xml2));
	--xml2 := put_campo(xml2,'XML_CUSTODIUM',get_campo('RESPUESTA_HEX',xml2));
	data1:=get_campo('RESPUESTA_HEX',xml2);
	xml2 := put_campo(xml2,'RESPUESTA_HEX','');

	--Saco solo la data
	
	pos1:=strpos(data1,'0a0a');
	if (pos1>0) then
		xml2:= put_campo(xml2,'XML_ALMACEN',substring(data1,pos1+4));
	else
		--RME se saca el encabezado HTTP de la respuesta
		xml2:= put_campo(xml2,'XML_ALMACEN',encode('<?xml version="1.0"','hex')||split_part(data1,encode('<?xml version="1.0"','hex'),2));
	end if;
	xml2 := logapp(xml2,'Largo RESPUESTA_HEX='||length(get_campo('XML_ALMACEN',xml2))::varchar);
	--xml2:= put_campo(xml2,'XML_ALMACEN',encode('<?xml version="1.0"','hex')||split_part(data1,encode('<?xml version="1.0"','hex'),2));
    elsif (strpos(data1,encode('302 Found','hex'))>0) and get_campo('URI_IN_ANTERIOR',xml2) ='' then
	xml2:=put_campo(xml2,'URI_IN_ANTERIOR',get_campo('URI_IN',xml2));
	uri1:=split_part(split_part(data1,encode('Location: ','hex'),2),'0a',1);
	xml2:=logapp(xml2,'LOCATION:'|| decode(uri1,'hex')::varchar);
	xml2:=put_campo(xml2,'URI_IN',decode(uri1,'hex')::varchar);
	xml2 := put_campo(xml2,'__SECUENCIAOK__','10');
    else
	--Verificamos si existe por GET python
       	xml2 := logapp(xml2,'No se obtiene XML de URI='||get_campo('URI_IN',xml2));
        xml2:=put_campo(xml2,'FALLA_CUSTODIUM','SI');
	/*
       	xml2 := logapp(xml2,data1);
	data1:=get_input_almacen('{"uri":"'||get_campo('URI_IN',xml2)||'"}');
	if (length(data1)=0) then
        	xml2 := logapp(xml2,'Falla Servicio Custodium, no se obtiene XML de URI='||get_campo('URI_IN',xml2));
	        xml2:=put_campo(xml2,'FALLA_CUSTODIUM','SI');
	else
		xml2 := logapp(xml2,'Se lee URI desde GET python');
		xml2:= put_campo(xml2,'XML_ALMACEN',data1);
		xml2:=put_campo(xml2,'FALLA_CUSTODIUM','NO');	
	end if;
	*/	
    end if;
    xml2:=put_campo(xml2,'INPUT_CUSTODIUM','');
    RETURN xml2;
END;
$$ LANGUAGE plpgsql;


