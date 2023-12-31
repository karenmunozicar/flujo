--Publica documento
delete from isys_querys_tx where llave='42797';
--Obtiene el DTE Original con la entrada URI_IN
insert into isys_querys_tx values ('42797',5,1,8,'GET XML desde Almacen',12705,0,0,1,1,10,10);

insert into isys_querys_tx values ('42797',10,1,1,'select proc_procesa_get_xml_cesion_42797(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

--insert into isys_querys_tx values ('42797',60,1,10,'$$SCRIPT$$',0,0,0,1,1,70,70);
insert into isys_querys_tx values ('42797',60,1,2,'Servicio de Firma 192.168.3.17',4013,109,106,0,0,70,70);

insert into isys_querys_tx values ('42797',70,1,1,'select cesion1_resp_42797(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

--insert into isys_querys_tx values ('42797',90,1,10,'$$SCRIPT$$',0,0,0,1,1,100,100);
insert into isys_querys_tx values ('42797',90,1,2,'Servicio de Firma 192.168.3.17',4013,109,106,0,0,100,100);

insert into isys_querys_tx values ('42797',100,1,1,'select cesion2_resp_42797(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

--insert into isys_querys_tx values ('42797',180,1,10,'$$SCRIPT$$',0,0,0,1,1,200,200);
insert into isys_querys_tx values ('42797',180,1,2,'Servicio de Firma 192.168.3.17',4013,109,106,0,0,200,200);

insert into isys_querys_tx values ('42797',200,1,1,'select cesion3_resp_42797(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

--Vamos a Publicar
insert into isys_querys_tx values ('42797',210,1,8,'Publica DTE',12704,0,0,0,0,220,220);
insert into isys_querys_tx values ('42797',220,1,1,'select verifica_pub_42797(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0); 

--Enviamos al EDTE
insert into isys_querys_tx values ('42797',230,1,8,'Llamada CESION EDTE',12786,0,0,0,0,240,240);
insert into isys_querys_tx values ('42797',240,1,1,'select verifica_cesion_edte_42797(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0); 


CREATE or replace FUNCTION verifica_cesion_edte_42797(json) RETURNS json AS $$
declare
	xml1	alias for $1;
	json2	json;
BEGIN
	json2:=xml1;
    json2:=put_json(json2,'__SECUENCIAOK__','0');
     --verifico si el documento ya fue publicado en el almacen, em caso contrario no sigo procesando
    if (get_json_upper('__EDTE_AEC_OK__',json2)<>'SI') then
        json2:=logjson(json2,'Falla la Publicacion de la Cesion '||get_json_upper('URI_IN',json2));
	json2:=response_requests_6000('2', 'Falla Envio de Cesion','', json2);
	return json2;
    end if;

    json2:=put_json(json2,'__SECUENCIAOK__','0');
    json2:=response_requests_6000('1', 'Cesion Exitosa',get_json_upper('URI_IN',json2), json2);
   return json2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION verifica_pub_42797(json) RETURNS json AS $$
declare
	json1	alias for $1;
	json2	json;
BEGIN
	json2:=json1;
    	json2:=put_json(json2,'__SECUENCIAOK__','0');
     --verifico si el documento ya fue publicado en el almacen, em caso contrario no sigo procesando
    	if (get_json_upper('__PUBLICADO_OK__',json2)<>'SI') then
        	json2:=logjson(json2,'Falla la Publicacion en Almacen '||get_json_upper('URI_IN',json2));
		json2:=response_requests_6000('2', 'Falla Lectura DTE del Almacen','', json2);
		return json2;
    	end if;

    	json2:=put_json(json2,'__SECUENCIAOK__','230');
   	return json2;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION proc_procesa_get_xml_cesion_42797(json) RETURNS json AS $$
DECLARE
    	json1    alias for $1;
        json2    json;
	rut1	varchar;
	data1	varchar;
	salida varchar;
	aux1	varchar;
BEGIN
    json2:=json1;
    json2:=put_json(json2,'__SECUENCIAOK__','0');
    json2:=logjson(json2,'CESION: COMIENZA FLUJO 42797');

    --Verifico si viene correctamete el DTE
    if (get_json_upper('FALLA_CUSTODIUM',json2)='SI') then
	json2:=logjson(json2,'CESION: DTE no leido desde almacen URI='||get_json_upper('URI_IN',json2));
	json2:=response_requests_6000('2', 'Falla Lectura DTE del Almacen','', json2);
	return json2;
    end if;
	
    --Genero la uri de la cesion
    aux1:=split_part(split_part(get_json_upper('URI_IN',json2),'//',2),'.',1);
    json2:=put_json(json2,'URI_IN','http://'||substring(aux1,1,length(aux1)-4)||to_char(now(),'YYMM')||'.acepta.com/v01/'||lpad('_'||replace(getipserver('eth0'),'.','')||'_'||to_char(now(),'DDHH24MISSMI')||'_'||nextval('correlativo_uri')::varchar||'_',40,'0')||'?k='||md5(coalesce(substring(aux1,1,length(aux1)-4),'X')));
    json2 := put_json(json2,'DOMINIO',substring(aux1,1,length(aux1)-4));
	
   --Rescato XML y otros datos
   data1 := decode(get_json_upper('XML_ALMACEN',json2), 'hex');
   json2 := put_json(json2,'RUT_EMISOR',split_part(get_xml('RUTEmisor',data1),'-',1));
   json2 := put_json(json2,'NOMBRE_FACTURADO',get_xml('RznSocRecep',data1));
   json2 := put_json(json2,'RUT_FACTURA',get_xml('RUTRecep',data1));
   json2 := put_json(json2,'FECHAEMISION',get_xml('FchEmis',data1));
   --Si viene la fecha de Vencimiento usamos esa, en caso contrario usamos la ingresada desde el portal
   aux1:=get_xml('FchVenc',data1);
   if (length(aux1)>0) then
	   json2 := put_json(json2,'FECHAVENCIMIENTO',aux1);
   else
	   json2 := put_json(json2,'FECHAVENCIMIENTO',get_json_upper('fecha_vencimiento',json2));
   end if;
   json2 := put_json(json2,'MONTO',get_xml('MntTotal',data1));
   json2:=cesion1_6000(json2);

   return json2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION cesion2_resp_42797 (json) RETURNS json AS $$
DECLARE
    json1                alias for $1;
    json2               json;
    xml_dte             varchar;
    xml_resp1           varchar;
    request1            varchar;
    pos1                integer;
    pos12               integer;
    dteF                varchar;
    resp1               varchar;
    rut_empresa         varchar;
    rut_firmante        varchar;
    clave_firmante      varchar;
    id_doc              varchar;
	uri1	varchar;
	salida varchar;
	json_resp1	varchar;
	data_firma1	varchar;
BEGIN
        json2:=json1;
	json2:=respuesta_no_chunked_json(json2);
	resp1:=decode(get_json_upper('RESPUESTA_HEX',json2),'hex');
	json_resp1:=split_part(resp1,'\012\012',2);
        json2:=put_json(json2,'RESPUESTA_HEX','');
        json2:=put_json(json2,'INPUT_FIRMADOR','');
        json2:=put_json(json2,'__SECUENCIAOK__','0');

	if (strpos(resp1,'HTTP/1.1 500 ')>0) then
		   resp1:=json_get('ERROR',json_resp1);
	 	   json2:=logjson(json2,'CESION: No logra firmar cesion 2');		
                   json2:=response_requests_6000('2', resp1, '',json2);
                   --insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json_upper('rutCedente',json2),'FALLA2_1',resp1,'AEC',get_json_upper('URI_IN',json2));
		   json2:=bitacora10k(json2,'FIRMA','Falla firma 2 de Cesion');
		 return json2;
        --Si no es un HTTP/1.1 200, fallamos
        elsif (strpos(resp1,'HTTP/1.1 200')=0) then
                json2:=logjson(json2,'CESION: Falla Firma Cesion 2');
                json2:=logjson(json2,'CESION: RESPUESTA_SYSTEM='||replace(resp1,chr(10),'-'));
		json2:=response_requests_6000('2', 'Falla Firma DTE Cedido','', json2);
                --insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json_upper('rutCedente',json2),'FALLA2_2','Falla Firma DTE Cedido','AEC',get_json_upper('URI_IN',json2));
		json2:=bitacora10k(json2,'FIRMA','Falla firma DTE Cedido');
                return json2;
        end if;
	dteF:=json_get('documentoFirmado',json_resp1);
	if (length(dteF)=0) then
                json2:=logjson(json2,'CESION: No logra firmar cesion 2');
                json2:=logjson(json2,'CESION: RESPUESTA_SYSTEM='||replace(resp1,chr(10),'-'));
		json2:=response_requests_6000('2', 'Falla Firma DTE Cedido.','', json2);
                --insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json_upper('rutCedente',json2),'FALLA2_3','Falla Firma DTE Cedido.','AEC',get_json_upper('URI_IN',json2));
		json2:=bitacora10k(json2,'FIRMA','Falla firma DTE Cedido.');
                return json2;
	end if;

        json2:=logjson(json2,'CESION: Firma Exitosa Cesion 2');
        --insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json_upper('rutCedente',json2),'OK2','','AEC',get_json_upper('URI_IN',json2));
        rut_firmante:=get_json_upper('rutCedente',json2);
	rut_empresa:=get_json_upper('RUT_EMISOR',json2)||'-'||modulo11(get_json_upper('RUT_EMISOR',json2));
	clave_firmante:=get_json_upper('pass',json2);
        id_doc:='T'||get_json_upper('tipoDte',json2)||'F'||replace(get_json_upper('folio',json2),'.','');

	 data_firma1:=replace('{"documento":"'||dteF||'","nodoId":"'||id_doc||'_AEC'||'","rutEmpresa":"'||rut_firmante||'","entidad":"SII","rutFirmante":"'||rut_firmante||'","codigoAcceso":"'||replace(decode(clave_firmante,'hex')::varchar,chr(92),chr(92)||chr(92))||'"}',chr(10),'');
	--json2:=get_parametros_motor_json(json2,'FIRMADOR');
	json2:=get_parametros_motor_json(json2,get_parametro_firmador(rut_firmante));
        json2:=put_json(json2,'INPUT_FIRMADOR','POST '||get_json_upper('PARAMETRO_RUTA',json2)||' HTTP/1.1'||chr(10)||'User-Agent: curl/7.26.0'||chr(10)||'Host: '||get_json_upper('__IP_CONEXION_CLIENTE__',json2)||chr(10)||'Accept: '||chr(42)||'/'||chr(42)||chr(10)||'Content-Type: application/json'||chr(10)||'Content-Length: '||length(data_firma1)::varchar||chr(10)||chr(10)||data_firma1);

        json2:=put_json(json2,'__SECUENCIAOK__','180');
 	return json2;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION cesion1_resp_42797 (json) RETURNS json AS $$
DECLARE
    json1                alias for $1;
    json2               json;
    pos1                integer;
    pos12               integer;
    resp1               varchar;
    geturl              varchar;
	dteF	varchar;
	json_resp1	varchar;
	rut_firmante	varchar;
	rut_empresa	varchar;
	data_firma1	varchar;
	clave_firmante	varchar;
	id_doc		varchar;
BEGIN
        json2:=json1;
	json2:=respuesta_no_chunked_json(json2);
	resp1:=decode(get_json_upper('RESPUESTA_HEX',json2),'hex');
	json_resp1:=split_part(resp1,'\012\012',2);
        json2:=put_json(json2,'RESPUESTA_HEX','');
        json2:=put_json(json2,'INPUT_FIRMADOR','');
        json2:=put_json(json2,'__SECUENCIAOK__','0');

	if (strpos(resp1,'HTTP/1.1 500 ')>0) then
                   json2:=logjson(json2,'CESION: RESPUESTA_SYSTEM='||replace(resp1,chr(10),'-'));
		   resp1:=json_get('ERROR',json_resp1);
		   --Si no hay respuesta
		   if (length(resp1)=0) then
			resp1:='Falla en Firmar Cesion';
		   end if;
	 	   json2:=logjson(json2,'CESION: No logra firmar cesion 1');		
                   json2:=response_requests_6000('2', resp1, '',json2);
                   --insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json_upper('rutCedente',json2),'FALLA1_1',resp1,'AEC',get_json_upper('URI_IN',json2));
		   json2:=bitacora10k(json2,'FIRMA','No logra primera firma para la Cesion');
		 return json2;
        --Si no es un HTTP/1.1 200, fallamos
        elsif (strpos(resp1,'HTTP/1.1 200')=0) then
                json2:=logjson(json2,'CESION: No logra firmar cesion 1');
                json2:=logjson(json2,'CESION: RESPUESTA_SYSTEM='||replace(resp1,chr(10),'-'));
		json2:=response_requests_6000('2', 'Falla Firma Cesion','', json2);
		--insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json_upper('rutCedente',json2),'FALLA1_2','Servicio de Firma no responde','AEC',get_json_upper('URI_IN',json2));
		json2:=bitacora10k(json2,'FIRMA','Servicio de Firma no responde para la Cesion');
                return json2;
        end if;
	--dteF:=get_xml('documentoFirmado',resp1);
	dteF:=json_get('documentoFirmado',json_resp1);
	if (length(dteF)=0) then
                json2:=logjson(json2,'CESION: No logra firmar cesion 1');
                json2:=logjson(json2,'CESION: RESPUESTA_SYSTEM='||replace(resp1,chr(10),'-'));
                json2:=put_json(json2,'__SECUENCIAOK__','0');
		json2:=response_requests_6000('2', 'Falla Firma Cesion.','', json2);
		--insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json_upper('rutCedente',json2),'FALLA1_3','Falla Firma Cesion.','AEC',get_json_upper('URI_IN',json2));
		json2:=bitacora10k(json2,'FIRMA','Falla Firma Cesion.');
                return json2;
	end if;
        json2:=logjson(json2,'CESION: Firma Exitosa Cesion 1');
	--insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json_upper('rutCedente',json2),'OK1','','AEC',get_json_upper('URI_IN',json2));
	rut_firmante:=get_json_upper('rutCedente',json2);
	rut_empresa:=get_json_upper('RUT_EMISOR',json2)||'-'||modulo11(get_json_upper('RUT_EMISOR',json2));
	clave_firmante:=get_json_upper('pass',json2);
	id_doc:='T'||get_json_upper('tipoDte',json2)||'F'||replace(get_json_upper('folio',json2),'.','');
       
        data_firma1:=replace('{"documento":"'||dteF||'","nodoId":"'||id_doc||'_Cedido'||'","rutEmpresa":"'||rut_firmante||'","entidad":"SII","rutFirmante":"'||rut_firmante||'","codigoAcceso":"'||replace(decode(clave_firmante,'hex')::varchar,chr(92),chr(92)||chr(92))||'"}',chr(10),'');
	--json2:=get_parametros_motor_json(json2,'FIRMADOR');
	json2:=get_parametros_motor_json(json2,get_parametro_firmador(rut_firmante));
        json2:=put_json(json2,'INPUT_FIRMADOR','POST '||get_json_upper('PARAMETRO_RUTA',json2)||' HTTP/1.1'||chr(10)||'User-Agent: curl/7.26.0'||chr(10)||'Host: '||get_json_upper('__IP_CONEXION_CLIENTE__',json2)||chr(10)||'Accept: '||chr(42)||'/'||chr(42)||chr(10)||'Content-Type: application/json'||chr(10)||'Content-Length: '||length(data_firma1)::varchar||chr(10)||chr(10)||data_firma1);
	json2:=put_json(json2,'__SECUENCIAOK__','90');
        RETURN json2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION cesion3_resp_42797(json)
RETURNS json AS $$
DECLARE
    json1                alias for $1;
    json2                json;
    xml_dte             varchar;
    xml_resp1           varchar;
    request1            varchar;
    pos1                integer;
    pos12               integer;
    dteF                varchar;
    resp1               varchar;
    geturl              varchar;
	json_resp1	varchar;
BEGIN
       json2:=json1;
	json2:=respuesta_no_chunked_json(json2);
	resp1:=decode(get_json_upper('RESPUESTA_HEX',json2),'hex');
        json_resp1:=split_part(resp1,'\012\012',2);
        json2:=put_json(json2,'RESPUESTA_HEX','');
        json2:=put_json(json2,'INPUT_FIRMADOR','');
       json2:=put_json(json2,'__SECUENCIAOK__','0');
	if (strpos(resp1,'HTTP/1.1 500 ')>0) then
                   --resp1:='Error: '||split_part(split_part(resp1,'<faultstring>com.acepta.custodiafirma.servicios.ServicioFirmaXMLException:',2),'</faultstring>',1);
		   resp1:=json_get('ERROR',json_resp1);
	 	   json2:=logjson(json2,'CESION: No logra firmar cesion 3');		
                   json2:=response_requests_6000('2', resp1, '',json2);
                   --insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json_upper('rutCedente',json2),'FALLA3_1',resp1,'AEC',get_json_upper('URI_IN',json2));
		   json2:=bitacora10k(json2,'FIRMA','Falla Firma 3 Cesion');
		 return json2;
       --Si no es un HTTP/1.1 200, fallamos
        elsif (strpos(resp1,'HTTP/1.1 200')=0) then
                json2:=logjson(json2,'CESION: Falla Firma Cesion 3');
                json2:=logjson(json2,'CESION: RESPUESTA_SYSTEM='||replace(resp1,chr(10),'-'));
                json2:=put_json(json2,'__SECUENCIAOK__','0');
		json2:=response_requests_6000('2', 'Falla Firma AEC','', json2);
		--insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json_upper('rutCedente',json2),'FALLA3_2','Falla Firma AEC','AEC',get_json_upper('URI_IN',json2));
		json2:=bitacora10k(json2,'FIRMA','Falla Firma AEC Cesion');
                return json2;
       end if;
	dteF:=json_get('documentoFirmado',json_resp1);
	if (length(dteF)=0) then
                json2:=logjson(json2,'CESION: No logra firmar cesion 3');
                json2:=logjson(json2,'CESION: RESPUESTA_SYSTEM='||replace(resp1,chr(10),'-'));
                json2:=put_json(json2,'__SECUENCIAOK__','0');
                json2:=response_requests_6000('2', 'Falla Firma AEC.','', json2);
		--insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json_upper('rutCedente',json2),'FALLA3_3','Falla Firma AEC.','AEC',get_json_upper('URI_IN',json2));
		json2:=bitacora10k(json2,'FIRMA','Falla Firma AEC Cesion.');
                return json2;
        end if;
        json2:=logjson(json2,'CESION: Firma Existosa Cesion 3');
	--insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json_upper('rutCedente',json2),'OK3','','AEC',get_json_upper('URI_IN',json2));
	json2:=bitacora10k(json2,'FIRMA','Cesion Firmada OK');

	json2:=put_json(json2,'INPUT',base642hex(dteF));
	json2:=put_json(json2,'CONTENT_LENGTH',(length(base642hex(dteF))/2)::varchar);

        json2:=put_json(json2,'__SECUENCIAOK__','210');
	RETURN json2;
END;
$$ LANGUAGE plpgsql;

