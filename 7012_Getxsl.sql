--Flujo que hace un GET al S3, y devuelve el documento
delete from isys_querys_tx where llave='7012';
insert into isys_querys_tx values ('7012',10,1,1,'select proc_valida_origen_7012(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

--Obtiene la llamada a ca4webv3
insert into isys_querys_tx values ('7012',20,1,2,'Llamada Obtener el ca4webv3',4011,104,107,0,0,100,100);

insert into isys_querys_tx values ('7012',100,1,1,'select proc_respuesta_7012(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);


CREATE or replace FUNCTION proc_valida_origen_7012(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2    varchar;
    url1	varchar;
    header1	varchar;
    host1	varchar;
    input1	varchar;
    data2	varchar;
    rqu1	varchar;
    dia1	integer;
    dia2	integer;
    rut1	integer;
    tipo1	integer;
BEGIN
    xml2:=xml1;
    xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
	perform logfile('INDEXER F_7012 1.0 '||xml2);
    dia2:=to_char(now(),'YYYYMMDD')::integer;
    rut1:=get_campo('RUT_EMISOR',xml2)::integer;
    tipo1:=get_campo('TIPO_DTE',xml2)::integer;
    --Obtengo la uri del ultimo DTE emitido del cliente
    dia1:=(select max(dia) from indexer_estadisticas where id in (select id from indexer_hash where rut_emisor=rut1 and tipo_dte=tipo1) and dia<dia2 and estado='EMI' and tipo_dia='A');
    if (dia1 is null) then
	xml2:=logapp(xml2,'Sin Datos en indexer_estadisticas');
	xml2:=put_campo(xml2,'__FLAG_XSL__','NO');
	return xml2;
    end if;

   --Busco en dte_emitidos
    url1:=(select uri from dte_emitidos where rut_emisor=rut1 and tipo_dte=tipo1 and dia=dia1 limit 1);
    if (dia1 is null) then
	xml2:=logapp(xml2,'Sin Datos en dte_emitidos');
	xml2:=put_campo(xml2,'__FLAG_XSL__','NO');
	return xml2;
    end if;

    rqu1:='/ca4webv3/InfoView?url='||codifica_url(url1);
    host1:=split_part(split_part(url1,'http://',2),'/',1);
    header1:='GET '||rqu1||' HTTP/1.1'||chr(10)||'Host: '||host1||chr(10)||'Content-Type: text/html'||chr(10)||'Content-Length: 0'||chr(10)||chr(10);
    xml2:=put_campo(xml2,'__IP_CONEXION_CLIENTE__',host1);
    xml2:=put_campo(xml2,'__SECUENCIAOK__','20');
    xml2:=logapp(xml2,'7012: '||header1);
    xml2:=put_campo(xml2,'INPUT_CUSTODIUM',encode(header1::bytea,'hex')::varchar);
    xml2:=logapp(xml2,'7012: Vamos a Buscar el ca4webv3');
    return xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION proc_respuesta_7012(varchar) RETURNS varchar AS $$
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
    xml2:=put_campo(xml2,'XSL_DEFECTO','');
    data1:=get_campo('XML_ALMACEN',xml1);
    --xml2 := logapp(xml2,'7012: XML_ALMACEN='||decode(data1,'hex')::varchar); 
    xml2 := put_campo(xml2,'RESPUESTA_HEX',data1);
    --Le saco el chunked si viene
    xml2 := respuesta_no_chunked(xml2);
    --Envio como proxy todos los header recibidos
    --xml2 := proxy_respuesta_web(xml2);
    xml2:=put_campo(xml2,'INPUT_CUSTODIUM','');
    if (strpos(data1,encode('200 OK','hex'))>0) then
        xml2 := logapp(xml2,'7012: Respuesta 200 OK ca4webv3');
	--Obtengo el xsl por defecto
	data1:=decode(get_campo('RESPUESTA_HEX',xml2),'hex')::varchar;
	pos1:=strpos(data1,'<Default>');
	if (pos1>0) then
		data1:=split_part(split_part(substring(data1,pos1),'<StyleUri>',2),'</StyleUri>',1);
		xml2:=put_campo(xml2,'XSL_DEFECTO',data1);
		xml2:=logapp(xml2,'XSL DEFECTO='||data1);
		return xml2;
	end if;
    else
        xml2 := logapp(xml2,'7012: Falla Obtener ca4webv3');
    end if;
    RETURN xml2;
END;
$$ LANGUAGE plpgsql;


