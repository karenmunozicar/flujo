delete from isys_querys_tx where llave='12721';

--Primero que hacemos el publicar DTE
insert into isys_querys_tx values ('12721',10,1,8,'Publica DTE',12704,0,0,0,0,20,20);

-- Prepara llamada al AML
insert into isys_querys_tx values ('12721',20,1,1,'select proc_procesa_input_dte_12721(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('12721',30,1,2,'Llamada directo al Modulo de Peru',4013,103,106,0,0,40,40);

--Respuesta del AML
insert into isys_querys_tx values ('12721',40,1,1,'select proc_procesa_respuesta_dte_12721(''$$__XMLCOMPLETO__$$'') as __xml__ ',0,0,0,1,1,-1,0);

--Llamada a Grabar en Respaldo NINA
insert into isys_querys_tx values ('12721',100,1,8,'Llamada Publica Respaldo',12713,0,0,0,0,0,0);

CREATE or replace FUNCTION proc_procesa_input_dte_12721(varchar) RETURNS varchar AS $$
DECLARE
    xml1	alias for $1;
	xml2	varchar;
    data1	varchar;
    file1	varchar;
    sts		integer;
    host1	varchar;
    url1	varchar;
    respuesta1	varchar;
    resp1	varchar;
    status1	varchar;
    input1	varchar;
    salida1	varchar; 
    stSecuencia secuencia_aml%ROWTYPE;    
BEGIN
    xml2:=xml1;

    --xml2:=logapp(xml2,'__ID_DTE__='||get_campo('__ID_DTE__',xml2));
    --xml2:=logapp(xml2,'__COLA_MOTOR__='||get_campo('__COLA_MOTOR__',xml2));

    --Si es un nagios, ignoro el procesamiento
    if (get_campo('REQUEST_METHOD',xml2)='GET') then
	if ((strpos(get_campo('HTTP_USER_AGENT',xml2),'check_http')>0) or (length(get_campo('QUERY_STRING',xml2))=0)) then
                xml2 := logapp(xml2,'Nagios Check o GET sin datos, se ignora');
                --Se Responde OK
                xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK'||chr(10)||
                'Content-type: text/html; charset=iso-8859-1'||chr(10)||
                'Content-length: 0'||chr(10)||
                'Vary: Accept-Encoding'||chr(10)||chr(10));
                xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
		xml2 := sp_procesa_respuesta_cola_motor(xml2);
		xml2 := put_campo_ctx(xml2,'__ETAPA1__','OK');
                return xml2;
	end if;
    end if;

    --verifico si el documento ya fue publicado en el almacen, em caso contrario no sigo procesando
    if (get_campo('__PUBLICADO_OK__',xml2)<>'SI') then
    	xml2:=logapp(xml2,'Falla la Publicacion en Almacen '||get_campo('URI_IN',xml2));
	--Si es Borrador, lo dejo pasar., se maneja en las reglas
	if (strpos(get_campo('URI_IN',xml2),'http://pruebas')=0) then
		xml2 := put_campo(xml2,'STATUS_HTTP','400 NK');	
		xml2 := responde_aml(xml2);
		xml2 := sp_procesa_respuesta_cola_motor(xml2);
		return xml2;
	end if;
    end if;

    --Voy a servicio de Peru
    xml2 := put_campo(xml2,'__SECUENCIAOK__','30');
    xml2 := put_campo(xml2,'__IP_CONEXION_CLIENTE__','wsp01.acepta.com');
    --Cerficacion
    xml2 := put_campo(xml2,'__IP_PORT_CLIENTE__','80');
    --Produccion
    --xml2 := put_campo(xml2,'__IP_PORT_CLIENTE__','8091');
    --Input en hexa
    input1:=get_campo('INPUT',xml2);
    if length(input1)>0 then
	    salida1:='POST /asb/services/almacenarDocFep HTTP/1.1'||chr(10)||
		     'Host: wsp01.acepta.com'||chr(10)||
		     'Content-type: '||get_campo('CONTENT_TYPE',xml2)||chr(10)||
		     'Content-Location: '||get_campo('URI_IN',xml2)||chr(10)||
                     'Vary: Accept-Encoding'||chr(10)||
        	     'Content-Length: '||(length(input1)/2)::varchar||chr(10)||chr(10);
	    xml2 := logapp(xml2,'Envia '||salida1);
	    xml2 := put_campo(xml2,'INPUT',encode(salida1::bytea,'hex')::varchar||input1);
	    xml2 := logapp(xml2,'Envia POST a Modulo Peru');
    else
	xml2:=logapp(xml2,'Input Vacio, falla mensaje');
	xml2 := put_campo(xml2,'STATUS_HTTP','200 OK');	
	xml2 := responde_aml(xml2);
	xml2 := sp_procesa_respuesta_cola_motor(xml2);
    end if;

   return xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION proc_procesa_respuesta_dte_12721(varchar) RETURNS varchar AS $$
DECLARE
    xml1	alias for $1;
    xml2	varchar;
    data1	varchar;
    resp1	varchar;
    sts		integer;
    texto_resp1	varchar;
    respuesta1	varchar;
    status1	varchar;
BEGIN
    xml2:=xml1;
    data1:=get_campo('INPUT',xml2);
    xml2 := put_campo(xml2,'__SECUENCIAOK__','100');
    --Si hay respuesta del AML

    --Limpio el INPUT para el LOG
    resp1:= decode(get_campo('RESPUESTA_HEX',xml2),'hex');
    --Si viene este texto entonces AML responde OK
    --texto_resp1 := 'URL(True): '||get_campo('URI_IN',xml2);

    --Verifico si me fue bien con el AML
    --Debe contestar un OK y debe venir la URI que se envio a la entrada
    --if strpos(resp1,'200 OK')>0 then
    xml2 := logapp(xml2,resp1);
    if (strpos(resp1,'200 OK')>0) then
	xml2 := logapp(xml2,'URI='||get_campo('URI_IN',xml2)||' Envio OK');
    
    else
	xml2 := logapp(xml2,'URI='||get_campo('URI_IN',xml2)||' FALLA');
    end if; 

    --xml2 := put_campo(xml2,'INPUT','');
    respuesta1:=split_part(resp1,chr(10)||chr(10),2);
    if (strpos(resp1,'200 OK')>0) then
        status1:='Status: 200 OK'||chr(10)||
                 'Content-type: text/html'||chr(10)||
                 'Content-Location: '||get_campo('URI',xml1)||chr(10)||
                 'Content-length: '||length(respuesta1)||chr(10);
        xml2 := logapp(xml2,'Respuesta Servicio 200 OK URI'||get_campo('URI',xml1));
    else
        status1:='Status: 400 Rechazado'||chr(10)||'Content-type: text/html'||chr(10)||'Content-length: '||length(respuesta1)||chr(10);
        xml2 := logapp(xml2,'Respuesta Servicio 400 Rechazado (12721) URI'||get_campo('URI',xml1));
    end if;
    xml2:=put_campo(xml2,'RESPUESTA',status1||chr(10)||respuesta1);
                
    xml2 := sp_procesa_respuesta_cola_motor(xml2);

    --Respondo lo que viene
    RETURN xml2;
END;
$$ LANGUAGE plpgsql;
