delete from isys_querys_tx where llave='12700';

-- Prepara llamada al AML
insert into isys_querys_tx values ('12700',10,1,1,'select proc_traductor_fcgi_127000(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

--Ejecute Flujos
insert into isys_querys_tx values ('12700',20,1,8,'Flujo 8010 Factura Emitida',8010,0,0,1,1,100,100);
insert into isys_querys_tx values ('12700',30,1,8,'Flujo 8012 Factura Recibida',8012,0,0,1,1,100,100);
insert into isys_querys_tx values ('12700',40,1,8,'Flujo 8013 Cuadratura',8013,0,0,1,1,100,100);
insert into isys_querys_tx values ('12700',200,1,8,'Flujo 8011 Nueva Cuadratura',8011,0,0,1,1,100,100);

--Directo al Apache para los mensajes a cuadratura
insert into isys_querys_tx values ('12700',300,1,2,'Llamada a Cuadratura',4005,100,101,0,0,310,310);
insert into isys_querys_tx values ('12700',310,1,1,'select proc_respuesta_cuadratura_127000(''$$__XMLCOMPLETO__$$'') as __XML__',0,0,0,1,1,-1,0);

--Llamada directo a Traza Port 80
insert into isys_querys_tx values ('12700',400,1,2,'Llamada a Traza',4007,100,101,0,0,410,410);
insert into isys_querys_tx values ('12700',410,1,1,'select proc_respuesta_traza_127000(''$$__XMLCOMPLETO__$$'') as __XML__',0,0,0,1,1,-1,0);

--Ejecuta Flujo de Grabado en Almacen
insert into isys_querys_tx values ('12700',450,1,3,'Llamada a Almacen',8015,0,0,0,0,460,460);
insert into isys_querys_tx values ('12700',460,1,1,'select proc_respuesta_almacen_127000(''$$__XMLCOMPLETO__$$'') as __XML__',0,0,0,1,1,-1,0);

--Flujo Control Basura
insert into isys_querys_tx values ('12700',500,1,8,'Flujo 9999 Basura',9999,0,0,1,1,100,100);
--Parsea respuesta para FastCGI
insert into isys_querys_tx values ('12700',100,1,1,'select proc_respuesta_fcgi_127000(''$$__XMLCOMPLETO__$$'') as __XML__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('12700',110,1,1,'select proc_post_respuesta_127000(''$$__XMLCOMPLETO__$$'') as __XML__',0,0,0,1,1,-1,0);

CREATE or replace FUNCTION proc_traductor_fcgi_127000(varchar) RETURNS varchar AS $$
DECLARE
    xml1	alias for $1;
    xml2	varchar;
    data1	varchar;
    file1	varchar;
    sts		integer;
    header1	varchar;
    url1	varchar;
    host1	varchar;
BEGIN
    xml2:=xml1;
    data1:=decode(get_campo('INPUT',xml2),'hex');
	
    url1:=get_campo('SCRIPT_URL',xml2);
    host1:=get_campo('HTTP_HOST',xml2);
    xml2 := put_campo(xml2,'HTTP_CONTENT_TYPE',get_campo('CONTENT_TYPE',xml2));
    xml2 := put_campo(xml2,'HTTP_CONTENT_LENGTH',get_campo('CONTENT_LENGTH',xml2))i;
	
    --Si viene <Emisor> es del 8010
    if (strpos(data1,'<Emisor>')>0) then
	--El servicio esta en esa URL
	--url1:='/ca4/ca4dte';
        --host1:='pruebascge-pub.acepta.com';
	--xml2 := put_campo(xml2,'SCRIPT_NAME','/ca4/ca4dte');
	--xml2 := put_campo(xml2,'SERVER_NAME','pruebascge-pub.acepta.com');
	--xml2 := put_campo(xml2,'SCRIPT_URL','/ca4/ca4dte');
	--xml2 := put_campo(xml2,'SCRIPT_URI','http://pruebascge-pub.acepta.com/ca4/ca4dte');
	--xml2 := put_campo(xml2,'REQUEST_URI','/ca4/ca4dte');
    	xml2 := put_campo(xml2,'__SECUENCIAOK__','20');
    	xml2 := put_campo(xml2,'TIPO_TX','FACTURA_EMITIDA');
	xml2 := logapp(xml2,'Recibe Factura Emitida (12700)');
	
	--Prepara la respuesta inmediatamente
	--xml2 := respuesta_fast (xml2);
	return xml2;
    --Si vienen POST para la nueva cuadratura... (Aldo)
    elsif (strpos(data1,'tipo_tx=cuadraturaBusqBas&')>0) then
	--El servicio esta en esa URL
        url1:='';
        --host1:='localhost:8082/webiecv-sii-connector/';
        xml2 := put_campo(xml2,'TIPO_TX','NEW_CUADRATURA');
        xml2 := put_campo(xml2,'__SECUENCIAOK__','200');
	xml2 := logapp(xml2,'Recibe cuadraturaBusqBas (12700)');
    --Si viene en el Content-Type = vnd.com.acepta.webiecv.EstadoEnvioDTE viene del EDTE una factura_recibida
    elsif (strpos(get_campo('CONTENT_TYPE',xml2),'vnd.com.acepta.webiecv.EstadoEnvioDTE')>0) then
	--El servicio esta en esa URL
	--url1:='/';
        --host1:='192.168.3.93:8082/webiecv-sii-connector/';
    	xml2 := put_campo(xml2,'TIPO_TX','FACTURA_RECIBIDA');
    	xml2 := put_campo(xml2,'__SECUENCIAOK__','30');
	xml2 := logapp(xml2,'Recibe Estado DTE (12700)');
	return xml2;
    --Si viene un estado de Libros,voy a actualizar webiecv
    elsif (strpos(get_campo('CONTENT_TYPE',xml2),'vnd.com.acepta.webiecv.EstadoEnvioLibro')>0) then
	xml2 := put_campo(xml2,'TIPO_TX','ESTADO_LIBRO');
	xml2 := put_campo(xml2,'__SECUENCIAOK__','30');
	xml2 := logapp(xml2,'Recibe Estado Libro (12700)');
	return xml2;
    --Cuadratura Avisos
    elsif (strpos(get_campo('CONTENT_TYPE',xml2),'x-www-form-urlencoded')>0) then
	--El servicio esta en esa URL
	--url1:='/cuadratura-indexer/';
	--host1:='pruebascge-cuadindexer.acepta.com';
	--xml2 := put_campo(xml2,'SCRIPT_NAME','/cuadratura-indexer/');
	--xml2 := put_campo(xml2,'SERVER_NAME','pruebascge-cuadindexer.acepta.com');
	--xml2 := put_campo(xml2,'SCRIPT_URL','/cuadratura-indexer/');
	--xml2 := put_campo(xml2,'SCRIPT_URI','http://pruebascge-cuadindexer.acepta.com/cuadratura-indexer/');
	--xml2 := put_campo(xml2,'REQUEST_URI','/cuadratura-indexer/');
    	xml2 := put_campo(xml2,'TIPO_TX','ESTADO_EDTE');
    	xml2 := put_campo(xml2,'__SECUENCIAOK__','40');
	xml2 := logapp(xml2,'Recibe Evento para Cuadratura (12700)');
    -- Cuadratura Bitacora lista de receptores
    elsif (strpos(get_campo('CONTENT_TYPE',xml2),'text/csv')>0) then
	--Passthroug a Cuadratura
        --url1:='/cuadratura-indexer/';
        --host1:='pruebascge-cuadindexer.acepta.com';
	--xml2 := put_campo(xml2,'SCRIPT_NAME','/cuadratura-indexer/');
	--xml2 := put_campo(xml2,'SERVER_NAME','pruebascge-cuadindexer.acepta.com');
	--xml2 := put_campo(xml2,'SCRIPT_URL','/cuadratura-indexer/');
	--xml2 := put_campo(xml2,'SCRIPT_URI','http://pruebascge-cuadindexer.acepta.com/cuadratura-indexer/');
	--xml2 := put_campo(xml2,'REQUEST_URI','/cuadratura-indexer/');
    	xml2 := put_campo(xml2,'__SECUENCIAOK__','300');
	xml2 := logapp(xml2,'Recibe Bitacora lista de receptores (12700) ');

    --Si vienen los eventos de traza
    elsif (strpos(data1,'<trace source=')>0) then
	--El servicio esta en esa URL
	url1:='/tproxy/put';
	host1:='pruebascge-traza.acepta.com';
    	xml2 := put_campo(xml2,'__SECUENCIAOK__','400');
	xml2 := logapp(xml2,'TRAZA='||data1);
   --Consultas GET
   elsif (get_campo('REQUEST_METHOD',xml2)='GET') then
    	data1:=get_campo('QUERY_STRING',xml2);
   	--Consultas Portal Boletas
   	if (strpos(data1,'tipo_tx=ConsultaBoletaUsuario')>0) then
		xml2:=procesa_consulta_boletas_rut_12700(xml2);
		return xml2;
    	elsif (strpos(data1,'tipo_tx=ConsultaBoletas')>0) then
		xml2:=logapp(xml2,'ConsultaBoletas');
		xml2:=procesa_consulta_boletas_12700(xml2);
		return xml2;
    	elsif (strpos(data1,'tipo_tx=editaCliente')>0) then
		xml2:=procesa_edita_cliente_12700(xml2);
		return xml2;
    	else
		--Si no es nada de lo de arriba, es basura
	    	xml2 := put_campo(xml2,'__SECUENCIAOK__','500');
		xml2 := logapp(xml2,'Recibe Tx No Identificada (12700)');
    	end if;
    end if;
    --Debo Agregar el header a INPUT para que el resto funcione OK
    header1:='POST '||url1||' HTTP/1.1'||chr(10)||'Host: '||host1||chr(10)||'Content-Type: '||get_campo('CONTENT_TYPE',xml2)||chr(10)||'Content-Length: '||get_campo('CONTENT_LENGTH',xml2)||chr(10)||chr(10);
    xml2:=put_campo(xml2,'INPUT',header1||data1);

    RETURN xml2;
END;
$$ LANGUAGE plpgsql;

/*
CREATE or replace FUNCTION proc_respuesta_traza_127000(varchar) RETURNS varchar AS $$
DECLARE
    xml1	alias for $1;
    xml2	varchar;
	data1	varchar;
	status1	varchar;
	respuesta1	varchar;
BEGIN
	--Cambio la respuesta de cuadratura por la respuesta original
	xml2:=xml1;
	respuesta1:='';
        data1:=get_campo('RESPUESTA',xml1);
    	if (strpos(data1,'200 OK')>0) then
		xml2 := logapp(xml2,'Evento Traza Enviado OK');
		status1:='Status: 200 OK'||chr(10)||
		 'Content-type: text/html; charset=iso-8859-1'||chr(10)||
		 'Content-length: '||length(respuesta1)||chr(10)||
		 'Vary: Accept-Encoding'||chr(10);
	else
		status1:='Status: 400 Rechazado'||chr(10)||'Content-type: text/html; charset=iso-8859-1'||chr(10)||'Content-length: '||length(respuesta1)||chr(10);
		xml2 := logapp(xml2,'Falla Envio a Traza');
	end if;
	xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
    	xml2:=put_campo(xml2,'RESPUESTA',status1||chr(10)||respuesta1);
	return xml2;
END;
$$ LANGUAGE plpgsql;
*/
CREATE or replace FUNCTION proc_respuesta_almacen_127000(varchar) RETURNS varchar AS $$
DECLARE
    xml1	alias for $1;
    xml2	varchar;
	data1	varchar;
BEGIN
	--Cambio la respuesta de cuadratura por la respuesta original
	xml2:=xml1;
	if get_campo('_STS_FILE_',xml2)<>'OK' then
		xml2 := logapp(xml2,'Falla Almacen '||get_campo('_STS_FILE_',xml2));
	else
		xml2 := logapp(xml2,'Almacen OK Escritos '||get_campo('_STS_FILE_BYTES_WRITTEN_',xml2));
	end if;
	 xml2 := put_campo(xml2,'_STS_FILE_','');
	xml2 := put_campo(xml2,'RESPUESTA',get_campo('RESPUESTA_ORIGINAL',xml2));
	xml2 := put_campo(xml2,'RESPUESTA_ORIGINAL','');
	xml2 := put_campo(xml2,'INPUT','');
	xml2 := put_campo(xml2,'__SECUENCIAOK__','110');
	return xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION proc_respuesta_traza_127000(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2        varchar;
        data1   varchar;
BEGIN
        --Cambio la respuesta de cuadratura por la respuesta original
        xml2:=xml1;
        data1:=get_campo('RESPUESTA',xml1);
        if (strpos(data1,'200 OK')>0) then
                xml2 := logapp(xml2,'Traza Mensaje Enviado OK');
        else
                xml2 := logapp(xml2,'Falla Envio de Mensaje a Traza');
        end if;
        xml2 := put_campo(xml2,'RESPUESTA',get_campo('RESPUESTA_ORIGINAL',xml2));
        xml2 := put_campo(xml2,'RESPUESTA_ORIGINAL','');
        xml2 := put_campo(xml2,'INPUT','');
        xml2 := put_campo(xml2,'ENVIA_MENSAJE_TRAZA','');
        xml2 := put_campo(xml2,'__SECUENCIAOK__','110');
        return xml2;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION proc_respuesta_cuadratura_127000(varchar) RETURNS varchar AS $$
DECLARE
    xml1	alias for $1;
    xml2	varchar;
	data1	varchar;
BEGIN
	--Cambio la respuesta de cuadratura por la respuesta original
	xml2:=xml1;
        data1:=get_campo('RESPUESTA',xml1);
    	if (strpos(data1,'200 OK')>0) then
		xml2 := logapp(xml2,'Cuadratura Mensaje Enviado OK');
	else
		xml2 := logapp(xml2,'Falla Envio de Mensaje a Cuadratura');
	end if;
	xml2 := put_campo(xml2,'RESPUESTA',get_campo('RESPUESTA_ORIGINAL',xml2));
	xml2 := put_campo(xml2,'RESPUESTA_ORIGINAL','');
	xml2 := put_campo(xml2,'INPUT','');
        xml2 := put_campo(xml2,'ENVIA_MENSAJE_CUADRATURA','');
	xml2 := put_campo(xml2,'__SECUENCIAOK__','110');
	return xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION proc_post_respuesta_127000(varchar) RETURNS varchar AS $$
DECLARE
    xml1	alias for $1;
    xml2	varchar;
    data1	varchar;
    file1	varchar;
    status1	varchar;
    sts		integer;
    respuesta1	varchar;
    output1	varchar;
BEGIN
    xml2:=xml1;
	/*
    --Antes de Contestar, tambien verifico si hay que publica el Documento
    if (get_campo('PUBLICA_XML_CA4DOC',xml1)='SI') then
	--Guarda el archivo en el Almacen
	--Le saco al input el ensobrado del custodium
	xml2 := put_campo(xml2,'__SECUENCIAOK__','450');
	xml2 := put_campo(xml2,'TX','8015');
	--Corto desde el <?xml hasta los -----
        data1:=get_campo('INPUT',xml1);
	xml2 := put_campo(xml2,'INPUT','3c3f786d6c2076657273696f6e3d'||split_part(split_part(data1,'3c3f786d6c2076657273696f6e3d',2),'2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d',1));
	xml2 := put_campo(xml2,'RESPUESTA_ORIGINAL',get_campo('RESPUESTA',xml1));
	xml2 := put_campo(xml2,'PUBLICA_XML_CA4DOC','');
	xml2 := put_campo(xml2,'ENVIA_MENSAJE_CUADRATURA',get_campo('ENVIA_MENSAJE_CUADRATURA',xml1));
	xml2 := put_campo(xml2,'MENSAJE_CUADRATURA',get_campo('MENSAJE_CUADRATURA',xml1));
	xml2 := put_campo(xml2,'ENVIA_MENSAJE_TRAZA',get_campo('ENVIA_MENSAJE_TRAZA',xml1));
	xml2 := put_campo(xml2,'MENSAJE_TRAZA',get_campo('MENSAJE_TRAZA',xml1));
	xml2 := put_campo(xml2,'URI',get_campo('URI',xml1));
	xml2 := put_campo(xml2,'TIPO_TX',get_campo('TIPO_TX',xml1));
    	xml2 := put_campo(xml2,'_ID_REPROCESO_',get_campo('_ID_REPROCESO_',xml1));
	xml2 := put_campo(xml2,'_ESTADO_REPROCESO_',get_campo('_ESTADO_REPROCESO_',xml1));
	xml2 := logapp(xml2,'Voy a Grabar en Almacen');
	return xml2;
    end if;
	*/

	/*
    --Antes de Contestar, verifico si tengo un evento para informar a cuadratura
    if (get_campo('ENVIA_MENSAJE_CUADRATURA',xml1)='SI') then
	--Seteo el mensaje para cuadratura
	xml2 := '';
	xml2 := put_campo(xml2,'__SECUENCIAOK__','300');
	xml2 := put_campo(xml2,'INPUT',get_campo('MENSAJE_CUADRATURA',xml1));
	xml2 := put_campo(xml2,'MENSAJE_CUADRATURA','');
	xml2 := put_campo(xml2,'RESPUESTA_ORIGINAL',get_campo('RESPUESTA',xml1));
	xml2 := put_campo(xml2,'URI',get_campo('URI',xml1));
	xml2 := put_campo(xml2,'TIPO_TX',get_campo('TIPO_TX',xml1));
    	xml2 := put_campo(xml2,'_ID_REPROCESO_',get_campo('_ID_REPROCESO_',xml1));
	xml2 := put_campo(xml2,'_ESTADO_REPROCESO_',get_campo('_ESTADO_REPROCESO_',xml1));
	xml2 := put_campo(xml2,'ENVIA_MENSAJE_TRAZA',get_campo('ENVIA_MENSAJE_TRAZA',xml1));
	xml2 := put_campo(xml2,'MENSAJE_TRAZA',get_campo('MENSAJE_TRAZA',xml1));
	return xml2;
    end if;
	*/
	/*^
    --Tengo que enviar un mensaje a Traza..
    if (get_campo('ENVIA_MENSAJE_TRAZA',xml1)='SI') then
	xml2 := '';
	xml2 := put_campo(xml2,'__SECUENCIAOK__','400');
	xml2 := put_campo(xml2,'INPUT',get_campo('MENSAJE_TRAZA',xml1));
	xml2 := put_campo(xml2,'MENSAJE_TRAZA','');
	xml2 := put_campo(xml2,'RESPUESTA_ORIGINAL',get_campo('RESPUESTA',xml1));
	xml2 := put_campo(xml2,'URI',get_campo('URI',xml1));
	xml2 := put_campo(xml2,'TIPO_TX',get_campo('TIPO_TX',xml1));
    	xml2 := put_campo(xml2,'_ID_REPROCESO_',get_campo('_ID_REPROCESO_',xml1));
	xml2 := put_campo(xml2,'_ESTADO_REPROCESO_',get_campo('_ESTADO_REPROCESO_',xml1));
	return xml2;
    end if;
	*/

    xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
    xml2 := logapp(xml2,'Fin Procesamiento Posterior');
    RETURN xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION proc_respuesta_fcgi_127000(varchar) RETURNS varchar AS $$
DECLARE
    xml1	alias for $1;
    xml2	varchar;
    data1	varchar;
    file1	varchar;
    status1	varchar;
    sts		integer;
    respuesta1	varchar;
    output1	varchar;
    id1		bigint;
BEGIN
    xml2:=xml1;
    data1:=get_campo('RESPUESTA',xml1);
    respuesta1:=split_part(data1,chr(10)||chr(10),2);
    xml2 := put_campo(xml2,'__SECUENCIAOK__','110');

    if (strpos(data1,'200 OK')>0) then
	status1:='Status: 200 OK'||chr(10)||
		 'Content-type: text/html'||chr(10)||
		 'Content-Location: '||get_campo('URI',xml1)||chr(10)||
		 'Content-length: '||length(respuesta1)||chr(10);
	xml2 := logapp(xml2,'Respuesta Servicio 200 OK URI'||get_campo('URI',xml1));
    else
	--Si es un estado EDTE no conteste, el EDTE acepta cualqueir respuesta como valida
	if get_campo('TIPO_TX',xml1)='ESTADO_EDTE' then
    		xml2:=put_campo(xml2,'RESPUESTA','');
		xml2 := logapp(xml2,'No responde a EDTE');
		return xml2;
	else
		status1:='Status: 400 Rechazado'||chr(10)||'Content-type: text/html'||chr(10)||'Content-length: '||length(respuesta1)||chr(10);
		xml2 := logapp(xml2,'Respuesta Servicio 400 Rechazado (127000) URI'||get_campo('URI',xml1));
	end if;
    end if;

    --id1:=get_campo('ID_COLA_RECEPCION',xml2)::bigint;
    --delete from cola_recepcion where correlativo=id1;
    xml2:=put_campo(xml2,'RESPUESTA',status1||chr(10)||respuesta1);
    xml2 := put_campo(xml2,'__SOCKET_RESPONSE__','RESPUESTA'); 
	
    --xml2 := logapp(xml2,'Inicio Procesamiento Posterior');
    xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
    RETURN xml2;
END;
$$ LANGUAGE plpgsql;
