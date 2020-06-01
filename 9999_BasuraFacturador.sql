delete from isys_querys_tx where llave='9999';

-- Prepara llamada al AML
insert into isys_querys_tx values ('9999',20,1,1,'select proc_procesa_basura_9999(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

CREATE or replace FUNCTION proc_procesa_basura_9999(varchar) RETURNS varchar AS $$
DECLARE
    xml1	alias for $1;
	xml2	varchar;
    data1	varchar;
    file1	varchar;
	respuesta1	varchar;
    sts		integer;
BEGIN
    xml2:=xml1;
    data1:=get_campo('INPUT',xml2);
    xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
    --xml2 := put_campo(xml2,'RESPUESTA','URL(True): Esta respuesta es porque el facturador ensobro mal');

   --Grabo la basura para uso posterior
   insert into dte_basura (fecha,data) values (now(),data1);

    --xml2 := put_campo(xml2,'STATUS_HTTP','200 OK');
    --xml2 := responde_http_8011(xml2);
    respuesta1:='URL(True): Documento no identificado';
    xml2 := put_campo(xml2,'RESPUESTA','Status: 200 OK'||chr(10)||
                 'Content-type: text/html'||chr(10)||
                 'Content-Location: '||get_campo('URI',xml1)||chr(10)||
                 'Content-length: '||length(respuesta1)||chr(10)||chr(10)||
		 respuesta1);
    xml2 := logapp(xml2,'Graba en dte_basura');
    RETURN xml2;
END;
$$ LANGUAGE plpgsql;
