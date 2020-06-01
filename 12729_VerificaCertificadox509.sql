--Publica documento
delete from isys_querys_tx where llave='12729';

--Llamamos a Escribir Direco en cuadratura_indexer
insert into isys_querys_tx values ('12729',10,1,10,'$$SCRIPT_CERTIFICADO_X509$$',0,0,0,1,1,20,20);
insert into isys_querys_tx values ('12729',20,1,1,'select proc_verifica_respuesta_certificado_x509_12729(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,0,0);

CREATE or replace FUNCTION proc_verifica_respuesta_certificado_x509_12729(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2        varchar;
	respuesta1	varchar;
	cert1	varchar;
	stCert	certificados%ROWTYPE;
BEGIN
        --Cambio la respuesta de cuadratura por la respuesta original
        xml2:=xml1;
	--xml2:=logapp(xml2,'12729:Script '||get_campo('SCRIPT_CERTIFICADO_X509',xml2));
	respuesta1:=get_campo('RESPUESTA_SYSTEM',xml2);
	--xml2:=logapp(xml2,'12729:Respuesta Validacion Certificado='||replace(respuesta1,chr(10),';'));
	--verifico si no existe en la tabla
	cert1:=get_campo('MD5_CERTIFICADO_X509',xml2);
	select * into stCert from certificados where id_cert=cert1;
	if not found then
		--Inserto el certificado
		insert into certificados (fecha_insercion,id_cert,fecha_expiracion,subject,issuer,rut_emisor) values (now(),cert1,split_part(split_part(respuesta1,'notAfter=',2),chr(10),1),split_part(split_part(respuesta1,'subject=',2),chr(10),1),split_part(split_part(respuesta1,'issuer=',2),chr(10),1),get_campo('RUT_EMISOR',xml2));
	else
		xml2:=logapp(xml2,'12729: El certificado ya esta registrado en certificados');
	end if;
        return xml2;
END;
$$ LANGUAGE plpgsql;

