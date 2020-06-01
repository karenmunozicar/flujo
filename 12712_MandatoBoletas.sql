delete from isys_querys_tx where llave='12712';

--Publica DTE
--insert into isys_querys_tx values ('12712',10,1,8,'Llamada Publica DTE',12704,0,0,0,0,20,20);
--insert into isys_querys_tx values ('12712',20,1,1,'select proc_procesa_mandato_boletas_12712(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
--Respaldo DTE
--insert into isys_querys_tx values ('12712',30,1,8,'Llamada Publica DTE',12713,0,0,0,0,65,65);
--Obtengo el PDF del Almacen
insert into isys_querys_tx values ('12712',65,1,8,'Obtiene PDF Almacen',12714,0,0,0,0,70,70);


/*
insert into isys_querys_tx values ('12712',40,1,1,'select proc_prepara_cuadratura_12712(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

--Cuadratura
insert into isys_querys_tx values ('12712',50,1,2,'Cuadratura',4011,100,101,0,0,60,60);

--Verifica Mensaje a Cuadratura
insert into isys_querys_tx values ('12712',60,1,1,'select proc_verifica_mensaje_cuadratura_12712(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
*/

--Verifica Recepcion de PDF y arma mensaje de mail para publicar
insert into isys_querys_tx values ('12712',70,1,1,'select proc_arma_mensaje_mail_12712(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

--Envio mail de mandato
insert into isys_querys_tx values ('12712',80,1,2,'Envia Mandato',4013,100,101,0,0,90,90);
--Verifico Mandato Enviado OK
insert into isys_querys_tx values ('12712',90,1,1,'select proc_verifica_envio_mail_mandato_12712(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

/*
CREATE or replace FUNCTION proc_procesa_mandato_boletas_12712(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2		varchar;
	
BEGIN
    xml2:=xml1; 
    --Verifico si me fue bien al publicar
    if (get_campo('FALLA_CUSTODIUM',xml2)='SI') then
	--Si falla la publicacion, no soltamos el DTE
	xml2:=logapp(xml2,'Mandato: No Respondo a la Cola');	
        xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
	return xml2;
    end if;
   --Vamos al publicar en el respaldo
   xml2 := put_campo(xml2,'__SECUENCIAOK__','30');

   return xml2;

END;
$$ LANGUAGE plpgsql;
*/

CREATE or replace FUNCTION proc_prepara_cuadratura_12712(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2    varchar;
    datos1	varchar;
    aux1	varchar;
    sts		integer;
    data1	varchar;
    largo1       integer;
    pos_final1 integer;
    pos_inicial1 integer;
BEGIN
    xml2:=xml1;

    /*
    --La data entrante la cambio de nombre de TAG
    data1:=get_campo('INPUT',xml2);
    --Nuevo Procedimiento
    largo1:=get_campo('CONTENT_LENGTH',xml2)::integer*2;

    --Busco donde empieza <?xml version
    pos_inicial1:=strpos(data1,'3c3f786d6c2076657273696f6e3d');
    --Buscamos al reves donde esta el primer signo > que en hex es 3e
    --Como se pone un reverse se busca e3
    --xml2:=logapp(xml2,'MAGIA:'||strpos(reverse(data1),'e3')::varchar);
    pos_final1:=largo1-pos_inicial1+4-strpos(reverse(data1),'e3');
    data1:=substring(data1,pos_inicial1,pos_final1);
    xml2:=put_campo(xml2,'INPUT_ORI',data1);
	*/
--    xml2:=put_campo(xml2,'INPUT_ORI',encode('<?xml version=','hex')::varchar||split_part(split_part(data1,encode('<?xml version=','hex')::varchar,2),encode('-----------------------------','hex')::varchar,1));
    --xml2:=logapp(xml2,decode(get_campo('INPUT_ORI',xml2),'hex')::varchar);



    --realizar dos envios a indexer, evento EMI y evento ISI
    xml2:=put_campo(xml2,'__IP_CONEXION_CLIENTE__','cuadraturav2.custodium.com');
    datos1:='_DOREMPRUT='||get_campo('RUT_RECEPTOR',xml2)||'&_RUTREC='||get_campo('RUT_EMISOR',xml2)||'&_TIPDOC='||get_campo('TIPO_DTE',xml2)||'&_DORFOL='||get_campo('FOLIO',xml2)||'&_CODEVE=EMI&_DORMNTTOT='||get_campo('MONTO_TOTAL',xml2)||'&_FCHEVE='||codifica_url(to_char(get_campo('FECHA_EMISION',xml2)::timestamp,'DD/MM/YYYY+HH24:MI:SS'));
    --Se pone en duro cuadratura.
    xml2:=put_campo(xml2,'INPUT','POST /cuadratura-indexer/ HTTP/1.1'||chr(10)||'Host: cuadraturav2.custodium.com'||chr(10)||'Content-Type: application/x-www-form-urlencoded'||chr(10)||'User-Agent: Apache-HttpClient/4.2.1.(java.1.5)'||chr(10)||'Content-Length: '||length(datos1)||chr(10)||chr(10)||datos1); 
    xml2:=logapp(xml2,'Mandato: Mensaje Cuadratura='||get_campo('INPUT',xml2));

    --Indica a cual secuencia vamos
    xml2 := put_campo(xml2,'__SECUENCIAOK__','50');
    RETURN xml2;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION proc_verifica_mensaje_cuadratura_12712(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2    varchar;
    resp1	varchar;
BEGIN
    xml2:=xml1;
    --Leemos la respuesta de cuadratura
    resp1:=get_campo('RESPUESTA',xml2);
    --Si fallo cuadratura
    if (strpos(resp1,'200 OK')=0) then
	xml2:=logapp(xml2,'Mandato: Falla Respuesta de Cuadratura');	
	xml2:=logapp(xml2,'Mandato: '||resp1);
        xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
	return xml2;
    end if;

    --Borramos la respuesta
    xml2 := put_campo(xml2,'RESPUESTA','');

    --Vamos a recuperar el PDF del almacen
    xml2 := put_campo(xml2,'__SECUENCIAOK__','65');
    return xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION proc_arma_mensaje_mail_12712(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2    varchar;
    resp1       varchar;
    datos1	varchar;
    len1	varchar;
    sts		integer;
    file1	varchar;
    nombre1	varchar;
    data1	varchar;
    largo1	integer;
    pos_inicial1	integer;
    pos_final1		integer;
BEGIN
    xml2:=xml1;
    --Leemos la respuesta de cuadratura
    --Si no hay PDF, fallamos 
    if (get_campo('FALLA_PDF_CUSTODIUM',xml2)='SI') then 
	xml2:=logapp(xml2,'Mandato: Falla Obtencion de PDF desde el Almacen');	
        xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
	return xml2;
    end if;
    
    --La data entrante la cambio de nombre de TAG
    data1:=get_campo('INPUT',xml2);
    --Nuevo Procedimiento
    largo1:=get_campo('CONTENT_LENGTH',xml2)::integer*2;

    --Busco donde empieza <?xml version
    pos_inicial1:=strpos(data1,'3c3f786d6c2076657273696f6e3d');
    --Buscamos al reves donde esta el primer signo > que en hex es 3e
    --Como se pone un reverse se busca e3
    --xml2:=logapp(xml2,'MAGIA:'||strpos(reverse(data1),'e3')::varchar);
    pos_final1:=largo1-pos_inicial1+4-strpos(reverse(data1),'e3');
    data1:=substring(data1,pos_inicial1,pos_final1);
    --xml2:=put_campo(xml2,'INPUT_ORI',data1);

    --Grabo eventos de Cuadratura
    xml2:=put_campo(xml2,'FECHA_EVENTO_EMI',get_campo('FECHA_EMISION',xml2));
    xml2:=graba_evento_cuadratura('EMI',xml2);
    --Esta fecha se llena en graba_bitacora dinamicamente FECHA_EVENTO_(Nombre Evento)
    xml2:=graba_evento_cuadratura('PUB',xml2);

    --xml2:=logapp(xml2,'Vamos bien'); 
    --xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
    --return xml2;
    --Guardemos el PDF en una carpeta
    nombre1:=nextval('correlativo_pdf')::varchar||'.pdf';
    file1:='/opt/acepta/mail/file/'||nombre1;
    sts:=write_file_hex(file1,get_campo('PDF_ALMACEN',xml2));
    if (sts<>1) then
	xml2:=logapp(xml2,'Mandato: Falla escribir pdf en disco carpeta /opt/acepta/mail/file/');
        xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
	return xml2;
    end if;

    --xml2:=put_campo(xml2,'MAIL_TO','fernando.arancibia@acepta.com');
    xml2:=put_campo(xml2,'PDF_ALMACEN',nombre1);

    --Armo mensaje de envio de mail
    --xml2:=put_campo(xml2,'__IP_CONEXION_CLIENTE__','motor91');
    xml2:=put_campo(xml2,'__IP_CONEXION_CLIENTE__','motor132');
    xml2:=put_campo(xml2,'__IP_PORT_CLIENTE__','8080');

    --Aquien envio mandatos
    xml2:=logapp(xml2,'Mail:'||get_campo('MAIL_TO',xml2));
    
    --datos1:='mailfrom='||get_campo('MAIL_FROM',xml2)||'&mailto='||replace(get_campo('MAIL_TO',xml2),';',',')||'&subject='||get_campo('MAIL_SUBJECT',xml2)||'&xml='||get_campo('INPUT_ORI',xml2)||'&xsl='||encode(pg_read_binary_file('xsl/'||get_campo('DOMINIO',xml2)||'/pdf/mail.xsl')::bytea,'hex')||'&adjunto='||get_campo('PDF_ALMACEN',xml2)||'&mid='||get_campo('MAIL_MESSAGE_ID',xml2)||'&nombre_adjunto='||get_campo('RUT_EMISOR',xml2)||'_'||get_campo('TIPO_DTE',xml2)||'_'||get_campo('FOLIO',xml2)||'.pdf';
    datos1:='mailfrom='||get_campo('MAIL_FROM',xml2)||'&mailto='||replace(get_campo('MAIL_TO',xml2),';',',')||'&subject='||get_campo('MAIL_SUBJECT',xml2)||'&xml='||data1||'&xsl='||encode(pg_read_binary_file('xsl/'||get_campo('DOMINIO',xml2)||'/pdf/mail.xsl')::bytea,'hex')||'&adjunto='||get_campo('PDF_ALMACEN',xml2)||'&mid='||get_campo('MAIL_MESSAGE_ID',xml2)||'&nombre_adjunto='||get_campo('RUT_EMISOR',xml2)||'_'||get_campo('TIPO_DTE',xml2)||'_'||get_campo('FOLIO',xml2)||'.pdf';
/*************************/
    --datos1:='mailfrom='||get_campo('MAIL_FROM',xml2)||'&mailto=farancibia@isys.cl&subject='||get_campo('MAIL_SUBJECT',xml2)||'&xml='||get_campo('INPUT_ORI',xml2)||'&xsl='||encode(pg_read_binary_file('xsl/'||get_campo('DOMINIO',xml2)||'/xsl/mail.xsl')::bytea,'hex')||'&adjunto='||get_campo('PDF_ALMACEN',xml2)||'&mid='||get_campo('MAIL_MESSAGE_ID',xml2)||'&nombre_adjunto='||get_campo('FOLIO',xml2)||'.pdf';
/*************************/

    xml2:=put_campo(xml2,'INPUT','POST /Mail/SendMail HTTP/1.1'||chr(10)||'Host: '||get_campo('__IP_CONEXION_CLIENTE__',xml2)||chr(10)||'Content-Type: application/x-www-form-urlencoded'||chr(10)||'User-Agent: Apache-HttpClient/4.2.1.(java.1.5)'||chr(10)||'Content-Length: '||length(datos1)||chr(10)||chr(10)||datos1); 
--    xml2:=logapp(xml2,'Mensaje SendMail='||get_campo('INPUT',xml2));

    len1:=length(get_campo('INPUT',xml2));
    xml2:=logapp(xml2,'Mandato: Largo Envio ='||len1::varchar);
	
    --perform logfile('MANDATO='||get_campo('INPUT',xml2));
  
    --Mandamos el mail del mandato
    --Indica a cual secuencia vamos
    xml2 := put_campo(xml2,'__SECUENCIAOK__','80');
    RETURN xml2;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION proc_verifica_envio_mail_mandato_12712(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2    varchar;
    resp1	varchar;
BEGIN
    xml2:=xml1;
    xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
    --Leemos la respuesta de cuadratura
    resp1:=get_campo('RESPUESTA',xml2);
    --Si fallo cuadratura
    if (strpos(resp1,'200 OK')=0) then
	xml2:=logapp(xml2,'Mandato: Falla Envio de Mandato');	
	xml2:=logapp(xml2,'Mandato: '||resp1);
	return xml2;
    end if;

   
    --Solo si es una boleta, Actualizo tabla de boletas
    if (get_campo('TIPO_DTE',xml2) in ('39','41')) then
	    xml2 := graba_bitacora(xml2,'GRABADO_BOLETA_OK');
	    xml2 := put_campo(xml2,'ESTADO','BOLETA_GRABADA_OK');
	    xml2 := put_campo(xml2,'ESTADO_SII','BOLETA_GRABADA_OK');
	    xml2 := update_dte(xml2);
    end if;
  
    --Grabo evento de Envio por mandato OK
    xml2:=put_campo(xml2,'COMENTARIO_TRAZA','Recibe: '||get_campo('MAIL_TO',xml2));
    xml2:=graba_bitacora(xml2,'EMA');
    --Respondo OK
    xml2:=logapp(xml2,'Mandato: Se envia correctamente el mail');
    resp1:='URL(True): '||get_campo('URI_IN',xml2);
    xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK'||chr(10)||
                 'Content-type: text/html'||chr(10)||
                 'Content-Location: '||get_campo('URI_IN',xml2)||chr(10)||
                 'Content-length: '||length(resp1)||chr(10)||chr(10)||resp1);

    RETURN xml2;
END;
$$ LANGUAGE plpgsql;

