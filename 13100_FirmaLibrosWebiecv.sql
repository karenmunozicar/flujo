--Publica documento
delete from isys_querys_tx where llave='13100';


-- Validamos si se puede enviar el libro
insert into isys_querys_tx values ('13100',1,13,1,'select iecv.valido_envio_libro(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

-- Validamos credenciales 
insert into isys_querys_tx values ('13100',5,1,8,'Servicio de Validacion de Firma',13220,109,106,0,0,12,12);


--Esta funcion verifica si ocupo la API de Long Timeout o la API normal
--insert into isys_querys_tx values ('13100',5,1,1,'select verifica_api_webiecv_13100(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

--Usa la API Normal de Webiecv
--insert into isys_querys_tx values ('13100',10,13,1,'select iecv.encabezado_acepta(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);
--Usa la API Long Timeout
insert into isys_querys_tx values ('13100',12,20,1,'select iecv.encabezado_acepta(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

--insert into isys_querys_tx values ('13100',20,1,1,'select iecv_valida_genera_libro_13100(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('13100',60,1,2,'Servicio de Firma 192.168.3.17',4013,109,106,0,0,70,70);

insert into isys_querys_tx values ('13100',70,1,1,'select valida_resp_firma_libro_13100(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

--Vamos a Publicar
insert into isys_querys_tx values ('13100',210,1,8,'Publica DTE',12704,0,0,0,0,220,220);
insert into isys_querys_tx values ('13100',220,1,1,'select verifica_publicacion_13100(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

--Enviamos al EDTE
insert into isys_querys_tx values ('13100',230,1,8,'Se envia libro IECV al EDTE',12788,0,0,0,0,240,240);
insert into isys_querys_tx values ('13100',240,1,1,'select verifica_grabacion_edte_13100(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0); 

insert into isys_querys_tx values ('13100',300,13,1,'select actualiza_estado_libro_13100(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

--Para finalizar
insert into isys_querys_tx values ('13100',500,13,1,'select iecv.verifica_fin_webiecv_13100(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);
--insert into isys_querys_tx values ('13100',301,13,1,'select verifica_actualiza_estado_libro(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,0,0);

/*CREATE or replace FUNCTION verifica_fin_webiecv_13100(json) RETURNS json AS $$
declare
        json1   alias for $1;
        json2   json;
	stProc	procesos_webiecv%ROWTYPE;
	id1	bigint;
	json_resp1	json;
	json3	json;
	json4	json;
BEGIN
        json2:=json1;
	json2:=put_json(json2,'__SECUENCIAOK__','0');
	
        IF (get_json('CODIGO_RESPUESTA', json2) in ('263', '264')) THEN
            json2:=response_requests_6000('1', get_json('MENSAJE_RESPUESTA', json2),'https://almacen.acepta.com/ca4webv3/?url='||get_json('URI_IN',json2),json2);
            RETURN json2;
        END IF;

	--Si viene Flag    
	if (get_json('__FLAG_LONG_TIMEOUT__',json2)='SI') then
		id1:=get_json('__ID__WEBIECV__',json2)::bigint;

		--Si me fue bien
		json_resp1:=split_part(get_json('RESPUESTA',json2),chr(10)||chr(10),2)::json;

		if (get_json('CODIGO_RESPUESTA',json_resp1)<>'1') then
			--Borro de la cola
			delete from procesos_webiecv where id=id1;
			if not found then
				raise notice 'Falla Borrado de procesos_webiecv';
				json2:=logjson(json2,'Falla Borrado de procesos_webiecv');
			end if;
			
			--Marco el mensaje de proceso del libro
			json3 := get_parametros_motor_json('{}'::json,'WEBIECV');
    			json4:= query_db_json(get_json('__IP_CONEXION_CLIENTE__',json3),get_json('__IP_PORT_CLIENTE__',json3)::integer,'update iecv.libros set mensaje_proceso='||quote_literal(to_char(now(),'YYYY/MM/DD HH24:MI')||' '||get_json('MENSAJE_RESPUESTA',json_resp1))||' where codigo='||get_json('CODIGO_LIBRO',json2));
		elsif (get_json('CODIGO_RESPUESTA',json_resp1)='1') then
			--Borro de la cola
			delete from procesos_webiecv where id=id1;
			if not found then
				raise notice 'Falla Borrado de procesos_webiecv';
				json2:=logjson(json2,'Falla Borrado de procesos_webiecv');
			end if;
			json2:=logjson(json2,'Libro Firmado y Publicado Ok');
			--Marco el mensaje de proceso del libro
			json3 := get_parametros_motor_json('{}'::json,'WEBIECV');
    			json4:= query_db_json(get_json('__IP_CONEXION_CLIENTE__',json3),get_json('__IP_PORT_CLIENTE__',json3)::integer,'update iecv.libros set mensaje_proceso='''||to_char(now(),'YYYY/MM/DD HH24:MI')||' Libro Firmado y Publicado Ok'' where codigo='||get_json('CODIGO_LIBRO',json2));
		end if;
		--Si me fue bine
	end if;
        return json2;
END;
$$ LANGUAGE plpgsql;
*/
/*

CREATE or replace FUNCTION verifica_api_webiecv_13100(json) RETURNS json AS $$
declare
        json1   alias for $1;
        json2   json;
	stProc	procesos_webiecv%ROWTYPE;
	id1	bigint;
BEGIN
        json2:=json1;
	
	
	--Si viene Flag    
	if (get_json('__FLAG_LONG_TIMEOUT__',json2)='SI') then
		id1:=get_json('__ID__WEBIECV__',json2)::bigint;
		--Leo el json que viene en el ID, ya que se ejecuta desde un proceso batch
		select * into stProc from procesos_webiecv where id=id1;
		if found then
			json2:=stProc.json_in;
			json2:=put_json(json2,'__SECUENCIAOK__','12');
			json2:=put_json(json2,'__TIMEOUT_SERV_PXML__','1260');
			json2:=logjson(json2,'Leo Variables de procesos_webiecv');

			--Lo marco en ejecucion
			update procesos_webiecv set estado='EXEC' where id=id1;
			
		else
			json2:=put_json(json2,'__SECUENCIAOK__','0');
			raise notice 'Error en tabla procesos_webiecv no existe ID=%',id1;
		end if;
	else
		json2:=put_json(json2,'__SECUENCIAOK__','10');
	end if;

	--Saco el parametro de donde esta el firmador
	json2:=get_parametros_motor_json(json2,'FIRMADOR');
        return json2;
END;
$$ LANGUAGE plpgsql;
*/

CREATE or replace FUNCTION verifica_actualiza_estado_libro(json) RETURNS json AS $$
declare
        json1   alias for $1;
        json2   json;
BEGIN
        json2:=json1;
        json2:=put_json(json2,'__SECUENCIAOK__','500');
	
	if strpos(get_json(json2,'RESPUESTA_IECV'),'200 OK')>0 then
	        json2:=put_json(json2,'URI_LIBRO_IECV','https://almacen.acepta.com/ca4webv3/?url='||get_json('URI_IN',json2));
         	json2:=put_json(json2,'TRAZA_URI_IECV','https://motor-prod.acepta.com/bitacora/?url='||get_json('URI_IN',json2));
	        json2:=response_requests_6000('1', 'Firmado OK','https://almacen.acepta.com/ca4webv3/?url='||get_json('URI_IN',json2),json2);
        	return json2;
	else
		json2:=response_requests_6000('2', 'Falla Envio al SII, Reintente por favor.','',json2);
	end if;
        return json2;
	
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION verifica_publicacion_13100(json) RETURNS json AS $$
declare
        json1   alias for $1;
        json2   json;
BEGIN
        json2:=json1;
        json2:=put_json(json2,'__SECUENCIAOK__','500');
        --Si publico OK
        json2:=logjson(json2,'Respuesta Publicacion ' || get_json('__PUBLICADO_OK__',json2));
        if (get_json('__PUBLICADO_OK__',json2)<>'SI') then
                json2:=logjson(json2,'Falla la Publicacion en Almacen '||get_json('URI_IN',json2));
                json2:=response_requests_6000('2', 'Falla Publicacion de Libro, Reintente por favor.','',json2);
                return json2;
        end if;

        json2:=put_json(json2,'ESTADO_LIBRO','PUB');    --Estado Firmado y Publicado
        json2:=put_json(json2,'__SECUENCIAOK__','230');
        json2:=put_json(json2,'URI_LIBRO_IECV','https://almacen.acepta.com/ca4webv3/?url='||get_json('URI_IN',json2));
        json2:=put_json(json2,'TRAZA_URI_IECV','https://motor-prod.acepta.com/bitacora/?url='||get_json('URI_IN',json2));
        return json2;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION verifica_grabacion_edte_13100(json) RETURNS json AS $$
declare
	json1	alias for $1;
	json2	json;
BEGIN
	json2:=json1;
        json2:=put_json(json2,'__SECUENCIAOK__','500');
	--Si publico OK
        /*json2:=logjson(json2,'Respuesta Publicacion ' || get_json('__PUBLICADO_OK__',json2));
        if (get_json('__PUBLICADO_OK__',json2)<>'SI') then
        	json2:=logjson(json2,'Falla la Publicacion en Almacen '||get_json('URI_IN',json2));
		json2:=response_requests_6000('2', 'Falla Publicacion de Libro, Reintente por favor.','',json2);
		return json2;
	end if;*/
	--Si grabo bien en el EDTE
        json2:=logjson(json2,'Respuesta EDTE ' || get_json('__EDTE_IECV_OK__',json2));
	if (get_json('__EDTE_IECV_OK__',json2)<>'OK') then
		json2:=logjson(json2,'Falla Grabacion al EDTE'||get_json('URI_IN',json2));
		json2:=response_requests_6000('2', 'Falla Envio al SII, Reintente por favor.','',json2);
                return json2;
	end if;
	
	json2:=put_json(json2,'ESTADO_LIBRO','PUB');	--Estado Firmado y Publicado
        json2:=put_json(json2,'__SECUENCIAOK__','300');
	json2:=put_json(json2,'URI_LIBRO_IECV','https://almacen.acepta.com/ca4webv3/?url='||get_json('URI_IN',json2));
	json2:=put_json(json2,'TRAZA_URI_IECV','https://motor-prod.acepta.com/bitacora/?url='||get_json('URI_IN',json2));
	json2:=response_requests_6000('1', 'Firmado OK','https://almacen.acepta.com/ca4webv3/?url='||get_json('URI_IN',json2),json2);
        return json2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION valida_resp_firma_libro_13100 (json) RETURNS json AS $$
DECLARE
    json1                alias for $1;
    json2                json;
    json3		json;
    pos1                integer;
    pos12               integer;
    resp1               varchar;
    geturl              varchar;
	dteF	varchar;
	json_resp1	varchar;
	data_firma1	varchar;
	clave_firmante	varchar;
	id_doc		varchar;
	data1		varchar;
	iecv1	varchar;
	buffer1	varchar;
	sts1	varchar;
    stMaestro maestro_clientes%rowtype; -- GAC 20170616
    rut_emisor1 integer;
BEGIN
        json2:=json1;
	json3:='{}';
	json2:=respuesta_no_chunked_json(json2);
	resp1:=decode(get_json('RESPUESTA_HEX',json2),'hex');
	json_resp1:=split_part(resp1,'\012\012',2);
        json2:=put_json(json2,'INPUT_FIRMADOR','');
        json2:=put_json(json2,'__SECUENCIAOK__','500');
	perform logfile('XXXX Paso1 Webiecv');

	perform logfile(resp1);
	if (strpos(resp1,'HTTP/1.1 500 ')>0) then
		perform logfile('XXXX Paso2 Webiecv');
                   json2:=logjson(json2,'Firma Libro: RESPUESTA_SYSTEM='||replace(resp1,chr(10),'-'));
		   resp1:=json_get('ERROR',json_resp1);
		   --Si no hay respuesta
		   if (length(resp1)=0) then
			resp1:='Falla en Firmar Libro';
		   end if;
                   --resp1:='Error: '||split_part(split_part(resp1,'<faultstring>com.acepta.custodiafirma.servicios.ServicioFirmaXMLException:',2),'</faultstring>',1);
		   --Cuando no tiene certificado Cargado, informamos URIS para carga de certificados y manuales para realizar el proceso.
		   if strpos(resp1,'No existen certificados para el rut') >0 then
			   json2:=logjson(json2,'Rut sin Certificado Rut='||get_json('RUT_FIRMA',json2));
			   json3:=put_json(json3,'TXT_URI_CARGA','Carga de Certificados');
			   json3:=put_json(json3,'URI_CARGA','https://apps.acepta.com/firma-web/instalarCertificado.html');
			   json3:=put_json(json3,'TXT_MANUAL1','Instrucciones Para crear archivo');
			   json3:=put_json(json3,'URI_MANUAL1','https://www.acepta.com/wp-content/uploads/2014/06/CA-E-01-Respalda-desde-Internet-Explorer.pdf');
		   end if;
	
--		   json2:=put_json(json2,'URI_MANUAL2','');
	
	 	   json2:=logjson(json2,'Firma Libro: No logra firmar Libro 1');		
                   json2:=response_requests_6000('223', resp1, json3::varchar,json2);
                   --json2:=response_requests_6000('223', resp1, '',json2);
                   insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json('RUT_FIRMA',json2),'FALLA',resp1,'IECV',get_json('URI_IN',json2));
		 return json2;
        --Si no es un HTTP/1.1 200, fallamos
        elsif (strpos(resp1,'HTTP/1.1 200')=0) then
		perform logfile('XXXX Paso3 Webiecv');
                json2:=logjson(json2,'Firma Libro: No logra firmar libro 1');
                --json2:=logjson(json2,'Firma Libro: RESPUESTA_SYSTEM='||replace(resp1,chr(10),'-'));
		json2:=response_requests_6000('2', 'Falla Firma Libro','',json2);
		insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json('RUT_ISSUER',json2),'FALLA','Servicio de Firma no responde','IECV',get_json('URI_IN',json2));
                return json2;
        end if;
	dteF:=json_get('documentoFirmado',json_resp1);
	if (length(dteF)=0) then
		perform logfile('XXXX Paso4 Webiecv');
                json2:=logjson(json2,'Firma Libro: No logra firmar libro 1');
                --json2:=put_json(json2,'__SECUENCIAOK__','0');
		json2:=response_requests_6000('2', 'Falla Firma Libro.','',json2);
		insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json('RUT_FIRMA',json2),'FALLA','No contesta Libro Firmado','IECV',get_json('URI_IN',json2));
                return json2;
	end if;
	--Verificamos que el libro sea el libro
/*	if (verifica_doc_firmado(dteF,get_json('ID_FIRMA_WEBIECV',json2))) then
                json2:=logjson(json2,'WEBIECV: Error en HSM, no devuelve doc original');
                json2:=response_requests_6000_upper('2', 'Falla firma Webiecv', '',json2);
                json2:=bitacora10k(json2,'FIRMA','Falla firma WEBIECV. ');
                return json2;
	end if;
*/
	--Si me fue bien
		perform logfile('XXXX OK');
        json2:=logjson(json2,'WEBIECV: Firma Exitosa Libro 1');
	buffer1:=base642hex(dteF);
	--436f6e74656e74 es Content
        iecv1:=get_xml_hex('436f6e74656e74',buffer1);
        sts1:=xsd_validador_iecv(iecv1);
	sts1:=replace(sts1,chr(39),' ');
	
	if (sts1<>'OK') then
                json2:=logjson(json2,'IECV: Falla Esquema '||sts1);
                --json2:=bitacora10k(json2,'FIRMA','Falla Esquema en IECV');
                --json2:=response_requests_6000('2', 'Falla Esquema IECV :'||sts1,'', json2);
                --return json2;
        end if;
	data1:=buffer1;
        -- BEGIN GAC 20170616 Las URIS se arman una vez este terminada la Firma
        rut_emisor1:=get_json('rutCliente',json2)::integer;
        select * into stMaestro from maestro_clientes where rut_emisor = rut_emisor1;
        json2:=put_json(json2,'URI_IN','http://'||stMaestro.dominio||to_char(now(),'YYMM')||'.acepta.com/v01/'||lpad('_'||replace(getipserver('eth0'),'.','')||'_'||to_char(now(),'DDHH24MISSMI')||'_'||nextval('correlativo_uri')::varchar||'_',40,'0')||'?k='||md5(stMaestro.dominio));
        -- END GAC 20170616 Las URIS se arman una vez este terminada la Firma


	data1:=replace(data1,encode('__REMPLAZA_URI__','hex')::varchar,encode(get_json_upper('URI_IN',json2)::bytea,'hex')::varchar);
	json2:=put_json(json2,'INPUT',data1);
	json2:=put_json(json2,'CONTENT_LENGTH',length(data1)::varchar);
	--json2:=put_json(json2,'__IP_CONEXION_CLIENTE__',quote_literal('192.168.3.32'));
	
	insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json('RUT_FIRMA',json2),'OK','','IECV',get_json('URI_IN',json2));
	--Vamos a Publicar
	json2:=put_json(json2,'__SECUENCIAOK__','210');
        RETURN json2;
END;
$$ LANGUAGE plpgsql;


