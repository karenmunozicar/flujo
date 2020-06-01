delete from isys_querys_tx where llave='12706';

-- Prepara llamada al AML
insert into isys_querys_tx values ('12706',10,1,1,'select proc_libros_fcgi_127006(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('12706',50,1,2,'Llamada directo al AML',14002,102,101,0,0,100,100);
insert into isys_querys_tx values ('12706',60,1,2,'Llamada directo al AML',14005,102,101,0,0,100,100);

--Publique
insert into isys_querys_tx values ('12706',100,1,8,'Llamada Publica DTE',12704,0,0,0,0,110,110);

--Ejecute Flujos
insert into isys_querys_tx values ('12706',110,1,1,'select proc_respuesta_libros_fcgi_127006(''$$__XMLCOMPLETO__$$'') as __XML__',0,0,0,1,1,-1,0);


CREATE or replace FUNCTION proc_libros_fcgi_127006(varchar) RETURNS varchar AS $$
DECLARE
    xml1	alias for $1;
    xml2	varchar;
    data1	varchar;
    file1	varchar;
    sts		integer;
    header1	varchar;
	mail1	varchar;
    url1	varchar;
    host1	varchar;
    status1	varchar;
	rut2	varchar;
	stSecuencia	secuencia_aml%ROWTYPE;
	resp1	varchar;
BEGIN
    xml2:=xml1;
    data1:=decode(get_campo('INPUT',xml2),'hex');
	
    url1:=get_campo('SCRIPT_URL',xml2);
    host1:=get_campo('HTTP_HOST',xml2);
    xml2 := put_campo(xml2,'HTTP_CONTENT_TYPE',get_campo('CONTENT_TYPE',xml2));
    xml2 := put_campo(xml2,'HTTP_CONTENT_LENGTH',get_campo('CONTENT_LENGTH',xml2))i;
    xml2 := put_campo(xml2,'__SECUENCIAOK__','0'); 
    --Si es un GET, verificamos si es del nagios
    if (get_campo('REQUEST_METHOD',xml2)='GET') then
	if (strpos(get_campo('HTTP_USER_AGENT',xml2),'nagios-plugins')>0) then
		xml2 := logapp(xml2,'Responde Nagios');
		status1:='Status: 200 OK'||chr(10)||
		 'Content-type: text/html'||chr(10)||
		 'Content-length: 0'||chr(10)||chr(10);
    		xml2 := put_campo(xml2,'RESPUESTA',status1);
    		xml2 := put_campo(xml2,'__SOCKET_RESPONSE__','RESPUESTA'); 
		xml2 := logapp(xml2,'Respuesta Nagios OK');
		return xml2;
	else
		xml2 := logapp(xml2,'Responde GET');

		resp1:='<html><b>'||get_campo('SERVER',xml2)||get_campo('SCRIPT_NAME',xml2)||'</b></html>';
		status1:='Status: 200 OK'||chr(10)||
		 'Content-type: text/html'||chr(10)||
		 'Content-length: '||length(resp1)||chr(10)||chr(10)||resp1;
    		xml2 := put_campo(xml2,'RESPUESTA',status1);
    		xml2 := put_campo(xml2,'__SOCKET_RESPONSE__','RESPUESTA'); 
		xml2 := logapp(xml2,'Respuesta GET OK');
		xml2 := logapp(xml2,status1);
		return xml2;
	end if;
	xml2 := logapp(xml2,'GET');
	xml2 := logapp(xml2,replace(xml2,'###','***'));
	return xml2;
    end if;


    --Parseamos alguos datos del Libro para grabar en Traza
    xml2 := put_campo(xml2,'URI_IN',split_part(split_part(data1,'filename="',2),'"',1));
    xml2 := put_campo(xml2,'RUT_ENVIA',get_xml('RutEnvia',data1)); 
    xml2 := put_campo(xml2,'FECHA_EVENTO',to_char(now()::timestamp,'MM/DD/YYYY+00:00'));
    xml2 := put_campo(xml2,'FECHA_EMISION',split_part(split_part(data1,'<Attribute Type="FECHAEMISION">',2),'</Attribute>',1));
    xml2 := put_campo(xml2,'RUT_EMISOR',split_part(get_xml('RutEmisorLibro',data1),'-',1));
    xml2 := put_campo(xml2,'FOLIO',replace(get_xml('PeriodoTributario',data1),'-',''));
    xml2 := put_campo(xml2,'TIPO_DTE',get_xml('TipoOperacion',data1));
    xml2 := put_campo(xml2,'DIGEST',get_xml('DigestValue',data1));
    xml2 := logapp(xml2,'Recibe Libro (12706) Len='||length(data1)::varchar||' URI='||get_campo('URI_IN',xml2)||' FECHA_EMISION='||get_campo('FECHA_EMISION',xml2)||' RUT_EMISOR='||get_campo('RUT_EMISOR',xml2)||' FOLIO='||get_campo('FOLIO',xml2));

   if (length(get_campo('RUT_EMISOR',xml2))=0 or length(get_campo('FOLIO',xml2))=0 or length(get_campo('FECHA_EMISION',xml2))=0) then
    	xml2 := logapp(xml2,data1);
	xml2 := logapp(xml2,get_campo('INPUT',xml2));
   end if;

    --Si viene un libro con RutEnvia con Gonzalo AU, lo rechazamos
    if (get_campo('RUT_ENVIA',xml2)='8675623-4') then
    	xml2 := logapp(xml2,'Rut Envia 8675623-4 Bloqueado');
	xml2 := put_campo(xml2,'COMENTARIO_TRAZA','Debe Cambiar Rut Enviador, Comuniquese con Acepta.');
	xml2 := graba_bitacora(xml2,get_campo('TIPO_DTE',xml2));
        --Vamos a publicar el DTE y Asumimos respuesta OK del AML
	xml2 := put_campo(xml2,'RESPUESTA','200 OK');
        xml2 := put_campo(xml2,'__SECUENCIAOK__','100');

	rut2:=get_campo('RUT_EMISOR',xml2);
	if (is_number(rut2)) then
		mail1:=get_mail_opventa(rut2);
	end if;
	--Insertamos el mensaje para enviar por mail
	insert into aviso_mail values (now(),get_campo('RUT_EMISOR',xml2),mail1,'Recepcion DTE Rut='||get_campo('RUT_EMISOR',xml2)||' Libro '||get_campo('TIPO_DTE',xml2)||' Periodo='||get_campo('FOLIO',xml2)||' (Error en Rut Enviador)','Estimado Cliente:
Informamos que su libro ha sido retenido, debido a que el RutEnviador, no es correcto.
Por favor comuniquese con nuestra mesa de ayuda al +562 24968100 (Opcion 2)
Atentamente
El equipo de Acepta.'||chr(10)||chr(10)||replace(get_campo('URI_IN',xml2),'v01','traza')||chr(10),nextval('aviso_mail_codigo_seq'::regclass),'ruben.munoz@acepta.com,fernando.arancibia@acepta.com,ingrid.leyton@acepta.com,soporteacepta@acepta.com','LIBRO_RUT_ENVIADOR');
	return xml2;
    end if;
	
    --Grabo Evento de PUB para que aparezca en la traza
    xml2 := graba_bitacora(xml2,'PUB');
    --xml2 := logapp(xml2,data1);
    --Determino a que AML tengo que ir
    host1=get_campo('SERVER_NAME',xml2);
    url1:=get_campo('SCRIPT_NAME',xml2);
    xml2 := logapp(xml2,'Server '||host1||' Url '||url1);
    select * into stSecuencia from secuencia_aml where server_name=host1 and script_name=url1;
    if found then
                xml2 := put_campo(xml2,'__SECUENCIAOK__',stSecuencia.secuencia);
    else
                xml2 := logapp(xml2,'FALLA SERVER_NAME='||host1||' SCRIPT_NAME='||url1||' no definido');
                --Vamos a publicar el DTE
                xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
    end if;
    xml2:=arma_scgi(xml2);
    xml2 := logapp(xml2,'arma_scgi='||get_campo('HEADER_SCGI_HEX',xml2));
    return xml2;
	
	
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION proc_respuesta_libros_fcgi_127006(varchar) RETURNS varchar AS $$
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
    xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
    --Si Falla Custodium
    if get_campo('FALLA_CUSTODIUM',xml2)='SI' then
    		--xml2:=logapp(xml2,'INPUT_CUSTODIUM='||get_campo('INPUT_CUSTODIUM',xml2));
		--Si el Libro no puede ser publicado, error
		respuesta1:='Falla Escritura en Almacen';
		status1:='Status: 400 Rechazado'||chr(10)||'Content-type: text/html'||chr(10)||'Content-length: '||length(respuesta1)||chr(10);
		xml2 := logapp(xml2,'Respuesta Servicio 400 Falla Almacen URI'||get_campo('URI_IN',xml2));
    		xml2:=put_campo(xml2,'RESPUESTA',status1||chr(10)||respuesta1);
		xml2 := put_campo(xml2,'__SOCKET_RESPONSE__','RESPUESTA'); 
		return xml2;
    end if;
    xml2:=logapp(xml2,'RESPUESTA='||data1);
    respuesta1:=split_part(data1,chr(10)||chr(10),2);

    if (strpos(data1,'200 OK')>0) then
	status1:='Status: 200 OK'||chr(10)||
		 'Content-type: text/html'||chr(10)||
		 'Content-Location: '||get_campo('URI_IN',xml2)||chr(10)||
		 'Content-length: '||length(respuesta1)||chr(10);
	xml2 := logapp(xml2,'Respuesta Servicio 200 OK URI'||get_campo('URI_IN',xml2));
	--Almacena los libros
	insert into dte_varios (codigo_txel,fecha_ingreso,rut_emisor,uri,digest,fecha_emision) values (nextval('sec_codigo_txel'),now(),get_campo('RUT_EMISOR',xml2),get_campo('URI_IN',xml2),get_campo('DIGEST',xml2),get_campo('FECHA_EMISION',xml2));
    else
	status1:='Status: 400 Rechazado'||chr(10)||'Content-type: text/html'||chr(10)||'Content-length: '||length(respuesta1)||chr(10);
	xml2 := logapp(xml2,'Respuesta Servicio 400 Rechazado URI'||get_campo('URI_IN',xml2));
    end if;

    xml2:=put_campo(xml2,'RESPUESTA',status1||chr(10)||respuesta1);
    xml2 := put_campo(xml2,'__SOCKET_RESPONSE__','RESPUESTA'); 
	
    RETURN xml2;
END;
$$ LANGUAGE plpgsql;
