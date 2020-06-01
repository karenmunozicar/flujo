--Publica documento
delete from isys_querys_tx where llave='12795';
--Obtiene el DTE Original con la entrada URI_IN
insert into isys_querys_tx values ('12795',5,1,8,'GET XML desde Almacen',12705,0,0,1,1,10,10);

insert into isys_querys_tx values ('12795',10,1,1,'select proc_procesa_get_xml_cesion_12795(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('12795',60,1,2,'Servicio de Firma 192.168.3.17',4013,109,106,0,0,70,70);

insert into isys_querys_tx values ('12795',70,1,1,'select cesion1_resp_12795(''$$__XMLCOMPLETO__$$'',decode(''$$JSON_IN$$'',''hex'')::varchar) as __xml__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('12795',90,1,2,'Servicio de Firma 192.168.3.17',4013,109,106,0,0,100,100);

insert into isys_querys_tx values ('12795',100,1,1,'select cesion2_resp_12795(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('12795',180,1,2,'Servicio de Firma 192.168.3.17',4013,109,106,0,0,200,200);

insert into isys_querys_tx values ('12795',200,1,1,'select cesion3_resp_12795(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

CREATE or replace FUNCTION proc_procesa_get_xml_cesion_12795(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
        xml2    varchar;
	rut1	varchar;
	data1	varchar;
	salida varchar;
	json2	varchar;
	aux1	varchar;
BEGIN
    xml2:=xml1;
    xml2:=put_campo(xml2,'__SECUENCIAOK__','0');
    xml2:=logapp(xml2,'CESION: COMIENZA FLUJO 12795');
	json2:=decode(get_campo('JSON_IN',xml2),'hex')::varchar;
    --Verifico si viene correctamete el DTE
    if (get_campo('FALLA_CUSTODIUM',xml2)='SI') then
	xml2:=logapp(xml2,'CESION: DTE no leido desde almacen URI='||get_campo('URI_IN',xml2));
	xml2:=response_requests_5000('2', 'Falla Lectura DTE del Almacen','', xml2,json2);
	return xml2;
    end if;
	
    --Genero la uri de la cesion
    aux1:=split_part(split_part(get_campo('URI_IN',xml2),'//',2),'.',1);
    xml2:=put_campo(xml2,'URI_IN','http://'||substring(aux1,1,length(aux1)-4)||to_char(now(),'YYMM')||'.acepta.com/v01/'||lpad('_'||replace(getipserver('eth0'),'.','')||'_'||to_char(now(),'DDHH24MISSMI')||'_'||nextval('correlativo_uri')::varchar||'_',40,'0')||'?k='||md5(coalesce(substring(aux1,1,length(aux1)-4),'X')));
   xml2 := put_campo(xml2,'DOMINIO',substring(aux1,1,length(aux1)-4));
	
	
   --Rescato XML y otros datos
   data1 := decode(get_campo('XML_ALMACEN',xml2), 'hex');
   xml2 := put_campo(xml2,'RUT_EMISOR',split_part(get_xml('RUTEmisor',data1),'-',1));
   xml2 := put_campo(xml2,'NOMBRE_FACTURADO',get_xml('RznSocRecep',data1));
   xml2 := put_campo(xml2,'RUT_FACTURA',get_xml('RUTRecep',data1));
   xml2 := put_campo(xml2,'FECHAEMISION',get_xml('FchEmis',data1));
   --Si viene la fecha de Vencimiento usamos esa, en caso contrario usamos la ingresada desde el portal
   aux1:=get_xml('FchVenc',data1);
   if (length(aux1)>0) then
	   xml2 := put_campo(xml2,'FECHAVENCIMIENTO',aux1);
   else
	   xml2 := put_campo(xml2,'FECHAVENCIMIENTO',json_get('fecha_vencimiento',json2));
   end if;
   xml2 := put_campo(xml2,'MONTO',get_xml('MntTotal',data1));
   xml2:=cesion1_5000(xml2,json2);

   return xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION cesion2_resp_12795 (varchar) RETURNS varchar AS $$
DECLARE
    xml1                alias for $1;
    xml2                varchar;
    json2                varchar;
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
        xml2:=xml1;
        json2:=decode(get_campo('JSON_IN',xml2),'hex');
	xml2:=respuesta_no_chunked(xml2);
	resp1:=decode(get_campo('RESPUESTA_HEX',xml2),'hex');
        --resp1:=get_campo('RESPUESTA_SYSTEM',xml2);
	json_resp1:=split_part(resp1,'\012\012',2);
        xml2:=put_campo(xml2,'RESPUESTA_HEX','');
        xml2:=put_campo(xml2,'INPUT_FIRMADOR','');
        xml2:=put_campo(xml2,'__SECUENCIAOK__','0');

	if (strpos(resp1,'HTTP/1.1 500 ')>0) then
		   resp1:=json_get('ERROR',json_resp1);
                   --resp1:='Error: '||split_part(split_part(resp1,'<faultstring>com.acepta.custodiafirma.servicios.ServicioFirmaXMLException:',2),'</faultstring>',1);
	 	   xml2:=logapp(xml2,'CESION: No logra firmar cesion 2');		
                   xml2:=response_requests_5000('2', resp1, '',xml2,json2);
                   insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),json_get('rutCedente',json2),'FALLA2_1',resp1,'AEC',get_campo('URI_IN',xml2));
		 return xml2;
        --Si no es un HTTP/1.1 200, fallamos
        elsif (strpos(resp1,'HTTP/1.1 200')=0) then
                xml2:=logapp(xml2,'CESION: Falla Firma Cesion 2');
                xml2:=logapp(xml2,'CESION: RESPUESTA_SYSTEM='||replace(resp1,chr(10),'-'));
		xml2:=response_requests_5000('2', 'Falla Firma DTE Cedido','', xml2,json2);
                insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),json_get('rutCedente',json2),'FALLA2_2','Falla Firma DTE Cedido','AEC',get_campo('URI_IN',xml2));
                return xml2;
        end if;
	--dteF:=get_xml('documentoFirmado',resp1);
	dteF:=json_get('documentoFirmado',json_resp1);
	if (length(dteF)=0) then
                xml2:=logapp(xml2,'CESION: No logra firmar cesion 2');
                xml2:=logapp(xml2,'CESION: RESPUESTA_SYSTEM='||replace(resp1,chr(10),'-'));
		xml2:=response_requests_5000('2', 'Falla Firma DTE Cedido.','', xml2,json2);
                insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),json_get('rutCedente',json2),'FALLA2_3','Falla Firma DTE Cedido.','AEC',get_campo('URI_IN',xml2));
                return xml2;
	end if;

        xml2:=logapp(xml2,'CESION: Firma Exitosa Cesion 2');
        insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),json_get('rutCedente',json2),'OK2','','AEC',get_campo('URI_IN',xml2));
        rut_firmante:=json_get('rutCedente',json2);
	rut_empresa:=get_campo('RUT_EMISOR',xml2)||'-'||modulo11(get_campo('RUT_EMISOR',xml2));
	clave_firmante:=json_get('pass',json2);
        id_doc:='T'||json_get('tipoDte',json2)||'F'||replace(json_get('folio',json2),'.','');

	 data_firma1:=replace('{"documento":"'||dteF||'","nodoId":"'||id_doc||'_AEC'||'","rutEmpresa":"'||rut_firmante||'","entidad":"SII","rutFirmante":"'||rut_firmante||'","codigoAcceso":"'||decode(clave_firmante,'hex')::varchar||'"}',chr(10),'');
	xml2:=get_parametros_motor(xml2,'FIRMADOR');
        xml2:=put_campo(xml2,'INPUT_FIRMADOR','POST '||get_campo('PARAMETRO_RUTA',xml2)||' HTTP/1.1'||chr(10)||'User-Agent: curl/7.26.0'||chr(10)||'Host: '||get_campo('__IP_CONEXION_CLIENTE__',xml2)||chr(10)||'Accept: */*'||chr(10)||'Content-Type: application/json'||chr(10)||'Content-Length: '||length(data_firma1)::varchar||chr(10)||chr(10)||data_firma1);

        xml2:=put_campo(xml2,'__SECUENCIAOK__','180');
 	return xml2;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION cesion1_resp_12795 (varchar,varchar) RETURNS varchar AS $$
DECLARE
    xml1                alias for $1;
    xml2                varchar;
    json1                alias for $2;
    json2                varchar;
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
        xml2:=xml1;
        json2:=json1;
	xml2:=respuesta_no_chunked(xml2);
	resp1:=decode(get_campo('RESPUESTA_HEX',xml2),'hex');
	json_resp1:=split_part(resp1,'\012\012',2);
        xml2:=put_campo(xml2,'RESPUESTA_HEX','');
        xml2:=put_campo(xml2,'INPUT_FIRMADOR','');
        --resp1:=get_campo('RESPUESTA_SYSTEM',xml2);
        xml2:=put_campo(xml2,'__SECUENCIAOK__','0');

	if (strpos(resp1,'HTTP/1.1 500 ')>0) then
                   xml2:=logapp(xml2,'CESION: RESPUESTA_SYSTEM='||replace(resp1,chr(10),'-'));
		   resp1:=json_get('ERROR',json_resp1);
		   --Si no hay respuesta
		   if (length(resp1)=0) then
			resp1:='Falla en Firmar Cesion';
		   end if;
                   --resp1:='Error: '||split_part(split_part(resp1,'<faultstring>com.acepta.custodiafirma.servicios.ServicioFirmaXMLException:',2),'</faultstring>',1);
	 	   xml2:=logapp(xml2,'CESION: No logra firmar cesion 1');		
                   xml2:=response_requests_5000('2', resp1, '',xml2,json2);
                   insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),json_get('rutCedente',json2),'FALLA1_1',resp1,'AEC',get_campo('URI_IN',xml2));
		 return xml2;
        --Si no es un HTTP/1.1 200, fallamos
        elsif (strpos(resp1,'HTTP/1.1 200')=0) then
                xml2:=logapp(xml2,'CESION: No logra firmar cesion 1');
                xml2:=logapp(xml2,'CESION: RESPUESTA_SYSTEM='||replace(resp1,chr(10),'-'));
		xml2:=response_requests_5000('2', 'Falla Firma Cesion','', xml2,json2);
		insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),json_get('rutCedente',json2),'FALLA1_2','Servicio de Firma no responde','AEC',get_campo('URI_IN',xml2));
                return xml2;
        end if;
	--dteF:=get_xml('documentoFirmado',resp1);
	dteF:=json_get('documentoFirmado',json_resp1);
	if (length(dteF)=0) then
                xml2:=logapp(xml2,'CESION: No logra firmar cesion 1');
                xml2:=logapp(xml2,'CESION: RESPUESTA_SYSTEM='||replace(resp1,chr(10),'-'));
                xml2:=put_campo(xml2,'__SECUENCIAOK__','0');
		xml2:=response_requests_5000('2', 'Falla Firma Cesion.','', xml2,json2);
		insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),json_get('rutCedente',json2),'FALLA1_3','Falla Firma Cesion.','AEC',get_campo('URI_IN',xml2));
                return xml2;
	end if;
        xml2:=put_campo(xml2,'JSON_IN',encode(json2::bytea,'hex'));
        xml2:=logapp(xml2,'CESION: Firma Exitosa Cesion 1');
	insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),json_get('rutCedente',json2),'OK1','','AEC',get_campo('URI_IN',xml2));
	--xml2:=logapp(xml2,'JSONCESION2'||json2);
	rut_firmante:=json_get('rutCedente',json2);
	rut_empresa:=get_campo('RUT_EMISOR',xml2)||'-'||modulo11(get_campo('RUT_EMISOR',xml2));
	clave_firmante:=json_get('pass',json2);
	id_doc:='T'||json_get('tipoDte',json2)||'F'||replace(json_get('folio',json2),'.','');
       
        data_firma1:=replace('{"documento":"'||dteF||'","nodoId":"'||id_doc||'_Cedido'||'","rutEmpresa":"'||rut_firmante||'","entidad":"SII","rutFirmante":"'||rut_firmante||'","codigoAcceso":"'||decode(clave_firmante,'hex')::varchar||'"}',chr(10),'');
	xml2:=get_parametros_motor(xml2,'FIRMADOR');
        xml2:=put_campo(xml2,'INPUT_FIRMADOR','POST '||get_campo('PARAMETRO_RUTA',xml2)||' HTTP/1.1'||chr(10)||'User-Agent: curl/7.26.0'||chr(10)||'Host: '||get_campo('__IP_CONEXION_CLIENTE__',xml2)||chr(10)||'Accept: */*'||chr(10)||'Content-Type: application/json'||chr(10)||'Content-Length: '||length(data_firma1)::varchar||chr(10)||chr(10)||data_firma1);
	xml2:=put_campo(xml2,'__SECUENCIAOK__','90');
        RETURN xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION cesion3_resp_12795(varchar)
RETURNS varchar AS $$
DECLARE
    xml1                alias for $1;
    xml2                varchar;
    json2                varchar;
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
       xml2:=xml1;
       json2:=decode(get_campo('JSON_IN',xml2),'hex');
	xml2:=respuesta_no_chunked(xml2);
	resp1:=decode(get_campo('RESPUESTA_HEX',xml2),'hex');
        json_resp1:=split_part(resp1,'\012\012',2);
        xml2:=put_campo(xml2,'RESPUESTA_HEX','');
        xml2:=put_campo(xml2,'INPUT_FIRMADOR','');
        --resp1:=get_campo('RESPUESTA_SYSTEM',xml2);
       xml2:=put_campo(xml2,'__SECUENCIAOK__','0');
	if (strpos(resp1,'HTTP/1.1 500 ')>0) then
                   --resp1:='Error: '||split_part(split_part(resp1,'<faultstring>com.acepta.custodiafirma.servicios.ServicioFirmaXMLException:',2),'</faultstring>',1);
		   resp1:=json_get('ERROR',json_resp1);
	 	   xml2:=logapp(xml2,'CESION: No logra firmar cesion 3');		
                   xml2:=response_requests_5000('2', resp1, '',xml2,json2);
                   insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),json_get('rutCedente',json2),'FALLA3_1',resp1,'AEC',get_campo('URI_IN',xml2));
		 return xml2;
       --Si no es un HTTP/1.1 200, fallamos
        elsif (strpos(resp1,'HTTP/1.1 200')=0) then
                xml2:=logapp(xml2,'CESION: Falla Firma Cesion 3');
                xml2:=logapp(xml2,'CESION: RESPUESTA_SYSTEM='||replace(resp1,chr(10),'-'));
                xml2:=put_campo(xml2,'__SECUENCIAOK__','0');
		xml2:=response_requests_5000('2', 'Falla Firma AEC','', xml2,json2);
		insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),json_get('rutCedente',json2),'FALLA3_2','Falla Firma AEC','AEC',get_campo('URI_IN',xml2));
                return xml2;
       end if;
	--dteF:=get_xml('documentoFirmado',resp1);
	dteF:=json_get('documentoFirmado',json_resp1);
	if (length(dteF)=0) then
                xml2:=logapp(xml2,'CESION: No logra firmar cesion 3');
                xml2:=logapp(xml2,'CESION: RESPUESTA_SYSTEM='||replace(resp1,chr(10),'-'));
                xml2:=put_campo(xml2,'__SECUENCIAOK__','0');
                xml2:=response_requests_5000('2', 'Falla Firma AEC.','', xml2,json2);
		insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),json_get('rutCedente',json2),'FALLA3_3','Falla Firma AEC.','AEC',get_campo('URI_IN',xml2));
                return xml2;
        end if;
        xml2:=logapp(xml2,'CESION: Firma Existosa Cesion 3');
	insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),json_get('rutCedente',json2),'OK3','','AEC',get_campo('URI_IN',xml2));

        xml2:=put_campo(xml2,'__SECUENCIAOK__','0');
        xml2:=response_requests_5000('1', 'OK','', xml2,json2);
	RETURN xml2;
END;
$$ LANGUAGE plpgsql;

