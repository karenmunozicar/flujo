--A partir de una URI (URI_IN)
--Obtiene el XML desde al almacen haciendo un GET y luego deja la respuesta en hex en el tag PDF_ALMACEN
delete from isys_querys_tx where llave='12714';
insert into isys_querys_tx values ('12714',10,1,1,'select proc_prepara_get_almacen_12714(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('12714',20,1,2,'Llamada al Storage Writer',4011,104,108,0,0,100,100);

insert into isys_querys_tx values ('12714',100,1,1,'select proc_respuesta_get_almacen_12714(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);


CREATE or replace FUNCTION proc_prepara_get_almacen_12714(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2    varchar;
    url1	varchar;
    header1	varchar;
    host1	varchar;
BEGIN
    xml2:=xml1;
    --Ya se parseo el DTE
    url1:='/ca4webv3/PdfViewMedia?url='||get_campo('URI_IN',xml2);
    host1:=split_part(split_part(get_campo('URI_IN',xml2),'//',2),'/',1);
    --host1:='almacen.acepta.com';
     --Debo Agregar el header a INPUT para que el resto funcione OK
    header1:='GET '||url1||' HTTP/1.1'||chr(10)||'Host: '||host1||chr(10)||'Content-Type: text/html'||chr(10)||'Content-Length: 0'||chr(10)||chr(10);
    xml2:=put_campo(xml2,'INPUT_CUSTODIUM',encode(header1::bytea,'hex')::varchar);
    xml2:=put_campo(xml2,'__IP_CONEXION_CLIENTE__',host1);
    xml2:=put_campo(xml2,'__SECUENCIAOK__','20');
    xml2:=logapp(xml2,'Pide PDF Original '||host1||' '||get_campo('URI_IN',xml2));
    return xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION proc_respuesta_get_almacen_12714(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2    varchar;
    data1       varchar;
    file1       varchar;
    sts         integer;
BEGIN
    xml2:=xml1;
    data1:=get_campo('PDF_ALMACEN',xml1);
    xml2 := put_campo(xml2,'__SECUENCIAOK__','0');

    --FAY-ILB-JSE-PCS 2015-11-26
    --Si no contesta el PDF completo, fallamos
    if (strpos(data1,encode('200 OK','hex'))>0 and  length(split_part(data1,'25504446',2))>30) then
        xml2 := logapp(xml2,'Respuesta Custodium 200 OK, Se Obtiene PDF de URI='||get_campo('URI_IN',xml2));
        xml2:=put_campo(xml2,'FALLA_PDF_CUSTODIUM','NO');
	xml2 := put_campo(xml2,'RESPUESTA_HEX',data1);
	xml2:=respuesta_no_chunked(xml2);
	xml2 := put_campo(xml2,'PDF_ALMACEN',get_campo('RESPUESTA_HEX',xml2));
	data1:=get_campo('RESPUESTA_HEX',xml2);
	xml2 := put_campo(xml2,'RESPUESTA_HEX','');
	--RME se saca el encabezado HTTP de la respuesta
	--xml2:=logapp(xml2,data1);
        sts:=strpos(data1,'0a0a');

	xml2:= put_campo(xml2,'PDF_ALMACEN',substring(data1,sts,length(data1)));
    else
        xml2 := logapp(xml2,'Falla Servicio Custodium, no se obtiene PDF de URI='||get_campo('URI_IN',xml2));
        xml2:=put_campo(xml2,'FALLA_PDF_CUSTODIUM','SI');
        xml2 := logapp(xml2,data1);
    end if;
    xml2:=put_campo(xml2,'INPUT_CUSTODIUM','');
    RETURN xml2;
END;
$$ LANGUAGE plpgsql;


