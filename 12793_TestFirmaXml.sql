delete from isys_querys_tx where llave='12793';

insert into isys_querys_tx values ('12793',5,1,1,'select pivote_12793(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

--SUBIR FIRMA--
insert into isys_querys_tx values ('12793',10,1,1,'select subir_firma_12793(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('12793',15,1,10,'$$SCRIPT$$',0,0,0,1,1,20,20);

insert into isys_querys_tx values ('12793',20,1,1,'select subir_firma_resp_12793(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

--Probamos a firmar un NAR vacio
insert into isys_querys_tx values ('12793',30,1,1,'select xml_firma_12793(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('12793',40,1,2,'Servicio de Firma 192.168.3.17',4013,109,106,0,0,50,50);

--Validamos Firma
insert into isys_querys_tx values ('12793',50,1,1,'select valida_firma_12793(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

CREATE or replace FUNCTION pivote_12793(json) RETURNS json as $$
DECLARE
    json1               alias for $1;
    json2               json;
    flujo_secuencia1       varchar;
BEGIN
        json2:=json1;
	json2:=put_json(json2,'__SECUENCIAOK__','10');
	
	flujo_secuencia1:=get_json('LLAMA_FLUJO_SECUENCIA',json2);

	if(length(flujo_secuencia1)>0)then
		json2:=logjson(json2,'CAMBIO SECUENCIA A ' || flujo_secuencia1);
		json2:=put_json(json2,'__SECUENCIAOK__',flujo_secuencia1);
	end if;

        RETURN json2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION subir_firma_12793(json) RETURNS json as $$
DECLARE
    json1               alias for $1;
    json2               json;
    rut_usuario1    varchar;
    sts         integer;
    file1       varchar;
BEGIN
        json2:=json1;
	json2:=put_json(json2,'__SECUENCIAOK__','0');

        rut_usuario1:=get_json_upper('rutUsuario',json2);

	if(get_json('__FILE__',json2)<>'') then
		file1:=get_json('__FILE__',json2);
	else
		file1:='/opt/acepta/motor/firmas/'||rut_usuario1||'_firma.'||get_json('extension',json2);
		--DAO 20180316 Se borra archivo si es que existe
		perform	remove_file_python(file1);
		sts:=write_file_hex(file1,get_json('firma',json2)::varchar);
		if (sts<>1) then
			json2:=response_requests_6000('2', 'Falla escribir firma', '', json2);
			return json2;
		end if;
	end if;
	
	json2:=put_json(json2,'__SECUENCIAOK__','15');
        json2:=put_json(json2,'LLAMA_SCRIPT','SI');
        --json2:=put_json(json2,'SCRIPT','/opt/acepta/motor/scripts/funciones/generico10k/script_subir_firma_hsm1.sh '|| rut_usuario1 || '-' || modulo11(rut_usuario1) || ' "' || get_json('pass',json2) || '" ' || rut_usuario1||'_firma.'||get_json('extension',json2));
        json2:=put_json(json2,'SCRIPT','/opt/acepta/motor/Procesos/script_subir_firma_hsm_offline.sh '|| rut_usuario1 || '-' || modulo11(rut_usuario1) || ' "' || get_json('pass',json2) || '" ' || rut_usuario1||'_firma.'||get_json('extension',json2));
	json2:=put_json(json2,'rut_firmador_dv',rut_usuario1 || '-' || modulo11(rut_usuario1));
	json2:=logjson(json2,'__SECUENCIAOK__ = ' || get_json('__SECUENCIAOK__',json2));
	json2:=logjson(json2,'LLAMA_SCRIPT = ' || get_json('LLAMA_SCRIPT',json2));
	json2:=logjson(json2,'SCRIPT = ' || get_json('SCRIPT',json2));

        RETURN json2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION subir_firma_resp_12793(json) RETURNS json AS $$
DECLARE
    json1                alias for $1;
    json2                json;
    resp1               varchar;
    uri1        varchar;
    pos1        integer;
    pos12       integer;
BEGIN
        json2:=json1;
	json2:=put_json(json2,'__SECUENCIAOK__','0');

        resp1:=get_json('RESPUESTA_SYSTEM',json2);
        json2:=logjson(json2,'Respuesta='||resp1);

        if (strpos(resp1,'OK')>0) then
		--Debo insertar el rut  en la tabla parametro_firmador
		insert into parametro_firmador (rut,parametro) values (get_json('rut_firmador_dv',json2),'FIRMADOR_OFFLINE');
		json2:=put_json(json2,'__SECUENCIAOK__','30');
		json2:=bitacora10k(json2,'FIRMA','Usuario carga certificado en HSM correctamente.') ;
	else
           	json2:=response_requests_6000('2', resp1,'', json2);
		json2:=bitacora10k(json2,'FIRMA','Falla cargar certificado en HSM ('||resp1||')');
        end if;

	json2:=logjson(json2,'subir_firma_resp_12793 = ' || resp1);
	return json2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION xml_firma_12793(json) RETURNS json AS $$
DECLARE
    json1                alias for $1;
    json2                json;
    resp1               varchar;
    uri1        varchar;
    pos1        integer;
    pos12       integer;
    EncabezadoCusDoc	varchar;
    PieCusDoc	varchar;
    data_firma1	varchar;
    xml_resp1	varchar;
    id1                 varchar;
	campo	record;
	aux	varchar;
BEGIN
        json2:=json1;
	json2:=put_json(json2,'__SECUENCIAOK__','0');

	SELECT md5(get_json('rutUsuario',json2))||to_char(now(),'MSMIHH24SS') INTO id1;

	EncabezadoCusDoc:='<?xml version="1.0" encoding="ISO-8859-1"?>';
	EncabezadoCusDoc:=EncabezadoCusDoc || '<?xml-stylesheet type="text/xsl" href="http://www.custodium.com/intercambio/notificacion.xsl"?> ';
	EncabezadoCusDoc:=EncabezadoCusDoc || '<Document Domain="" Type="Intercambio">';
	EncabezadoCusDoc:=EncabezadoCusDoc || '<Issuer><PI Type="Rut"></PI></Issuer>'      ;
	EncabezadoCusDoc:=EncabezadoCusDoc || '<Signers><Signer><PI Type="Rut"></PI></Signer></Signers>';
	EncabezadoCusDoc:=EncabezadoCusDoc || '<Recipients><Recipient><PI Type="Rut"></PI></Recipient></Recipients>';
	EncabezadoCusDoc:=EncabezadoCusDoc || '<Attributes>';
	EncabezadoCusDoc:=EncabezadoCusDoc || '<Attribute Type="TIPODTE"></Attribute>';
	EncabezadoCusDoc:=EncabezadoCusDoc || '<Attribute Type="FOLIO"></Attribute>';
	EncabezadoCusDoc:=EncabezadoCusDoc || '<Attribute Type="FECHAEMISION"></Attribute>';
	EncabezadoCusDoc:=EncabezadoCusDoc || '<Attribute Type="RUTEMISOR"></Attribute>';
	EncabezadoCusDoc:=EncabezadoCusDoc || '<Attribute Type="RUTRECEPTOR"></Attribute>';
	EncabezadoCusDoc:=EncabezadoCusDoc || '<Attribute Type="MONTOTOTAL"></Attribute>';
	EncabezadoCusDoc:=EncabezadoCusDoc || '<Attribute Type="ESTADODTE"></Attribute>';
	EncabezadoCusDoc:=EncabezadoCusDoc || '<Attribute Type="ESTADODTEGLOSA"></Attribute>';
	EncabezadoCusDoc:=EncabezadoCusDoc || '</Attributes>';
	EncabezadoCusDoc:=EncabezadoCusDoc || '<Content>';
	
	xml_resp1:= '<RespuestaDTE xmlns="http://www.sii.cl/SiiDte" xmlns:sii="http://www.sii.cl/SiiDte" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.sii.cl/SiiDte RespuestaEnvioDTE_v10.xsd"  version="1.0">';
	xml_resp1:= xml_resp1 || '<Resultado ID="' || id1 || '">';
	xml_resp1:= xml_resp1 || '<Caratula version="1.0">';
	xml_resp1:= xml_resp1 || '<RutResponde></RutResponde>';
	xml_resp1:= xml_resp1 || '<RutRecibe></RutRecibe>';
	xml_resp1:= xml_resp1 || '<IdRespuesta></IdRespuesta>';
	xml_resp1:= xml_resp1 || '<NroDetalles></NroDetalles>';
	xml_resp1:= xml_resp1 || '<TmstFirmaResp></TmstFirmaResp>';
	xml_resp1:= xml_resp1 || '</Caratula>';
	xml_resp1:= xml_resp1 || '<ResultadoDTE>';
	xml_resp1:= xml_resp1 || '<TipoDTE></TipoDTE>';
	xml_resp1:= xml_resp1 || '<Folio></Folio>';
	xml_resp1:= xml_resp1 || '<FchEmis></FchEmis>';
	xml_resp1:= xml_resp1 || '<RUTEmisor></RUTEmisor>';
	xml_resp1:= xml_resp1 || '<RUTRecep></RUTRecep>';
	xml_resp1:= xml_resp1 || '<MntTotal></MntTotal>';
	xml_resp1:= xml_resp1 || '<CodEnvio></CodEnvio>';
	xml_resp1:= xml_resp1 || '<EstadoDTE></EstadoDTE>';
	xml_resp1:= xml_resp1 || '<EstadoDTEGlosa></EstadoDTEGlosa>';
	xml_resp1:= xml_resp1 || '</ResultadoDTE>';
	xml_resp1:= xml_resp1 || '</Resultado>';
	xml_resp1:= xml_resp1 || '</RespuestaDTE>';

	PieCusDoc:='</Content><Log><Process id="motor" version="1.0"><item name="custodium-uri">__REMPLAZA_URI__</item></Process><Process build="" id="MOTOR" version=""><item name="">item</item></Process></Log></Document>';

	xml_resp1:=EncabezadoCusDoc || xml_resp1 || PieCusDoc;

	json2:=logjson(json2,'rut_firma = ' || get_json_upper('rut_firma',json2));
	--json2:=logjson(json2,'pass = ' || get_json_upper('pass',json2));

	if(get_json_upper('pass',json2) = 'pruebasfirmaestres') then
		json2:=logjson(json2,'STRESSSSSSSSSSSSSSSSSSSS = ' || get_json_upper('rut_firma',json2));
		json2:=get_parametros_motor_json(json2,'TEST_FIRMA');
		json2:=put_json(json2,'RUT_FIRMA',get_json('__IP_CONEXION_CLIENTE__',json2));
                json2:=put_json(json2,'PASS',get_json('__VALOR_PARAM__',json2));
--		json2:=put_json(json2,'RUT_FIRMA','17597643');
--		json2:=put_json(json2,'PASS','dani2116');
	end if;	
	if(get_json_upper('pass',json2) = 'pruebasdefirma2') then
		json2:=logjson(json2,'STRESSSSSSSSSSSSSSSSSSSS = ' || get_json_upper('rut_firma',json2));
		json2:=put_json(json2,'RUT_FIRMA','16412824');
		json2:=put_json(json2,'PASS','sa011298');
	end if;	
	
	--Armamos para disparar directo a el firmador por socket
	aux:=get_json_upper('rut_firma',json2)||'-'||modulo11(get_json_upper('rut_firma',json2));
        data_firma1:=replace('{"documento":"'||encode(xml_resp1::bytea,'base64')::varchar||'","nodoId":"'||id1||'","rutEmpresa":"'||get_json_upper('rut_firma',json2)||'-'||modulo11(get_json_upper('rut_firma',json2))||'","entidad":"SII","rutFirmante":"'||aux||'","codigoAcceso":"' ||replace(get_json_upper('pass',json2),chr(92),chr(92)||chr(92))||'"}',chr(10),'');

	--json2:=get_parametros_motor_json(json2,'FIRMADOR');
	json2:=get_parametros_motor_json(json2,get_parametro_firmador(aux));
	json2:=put_json(json2,'INPUT_FIRMADOR','POST '||get_json('PARAMETRO_RUTA',json2)||' HTTP/1.1'||chr(10)||'User-Agent: curl/7.26.0'||chr(10)||'Host: '||get_json('__IP_CONEXION_CLIENTE__',json2)||chr(10)||'Accept: '||chr(42)||'/'||chr(42)||chr(10)||'Content-Type: application/json'||chr(10)||'Content-Length: '||octet_length(data_firma1)::varchar||chr(10)||chr(10)||data_firma1);
        --json2:=put_json(json2,'INPUT_FIRMADOR','POST /firma-10K-web/rest/firmar/xml HTTP/1.1'||chr(10)||'User-Agent: curl/7.26.0'||chr(10)||'Host: 192.168.3.17'||chr(10)||'Accept: */*'||chr(10)||'Content-Type: application/json'||chr(10)||'Content-Length: '||length(data_firma1)::varchar||chr(10)||chr(10)||data_firma1);

--	json2:=put_json(json2,'__IP_CONEXION_CLIENTE__','localhost');
--        json2:=put_json(json2,'__IP_PORT_CLIENTE__','1457');

        json2:=put_json(json2,'__SECUENCIAOK__','40');

	json2:=logjson(json2,'__SECUENCIAOK__ = ' || get_json('__SECUENCIAOK__',json2));
	json2:=logjson(json2,'INPUT_FIRMADOR = ' || get_json('INPUT_FIRMADOR',json2));
        
	return json2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION valida_firma_12793(json) RETURNS json AS $$
DECLARE
    	json1        alias for $1;
        json2   json;
        rut1    varchar;
        data1   varchar;
	resp1	varchar;
	pos1	integer;
	pos12	integer;
	uri1	varchar;
	request1            varchar;
	json_resp1	varchar;
	data_firma1     varchar;
	rut_usuario1	varchar;
	rut_cliente1	varchar;
	pass1		varchar;
	campo		record;
BEGIN
	json2:=json1;
	json2:=put_json(json2,'__SECUENCIAOK__','0');

	--Limpiamos 
	json2:=put_json(json2,'SCRIPT','');

	json2:=respuesta_no_chunked_json(json2);
	
	resp1:=decode(get_json_upper('RESPUESTA_HEX',json2),'hex');
	
	json_resp1:=split_part(resp1,'\012\012',2);

	--Limpiamos 
	json2:=put_json(json2,'RESPUESTA_HEX','');
	json2:=put_json(json2,'INPUT_FIRMADOR','');
	
	if (strpos(resp1,'HTTP/1.1 200 ')>0) then
		data1:=json_get('documentoFirmado',json_resp1);
                if (length(data1)=0) then
			json2:=response_requests_6000_upper('2', 'Test Firma Falla', '',json2);
			json2:=bitacora10k(json2,'FIRMA','Falla Firma Test');
			--insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json_upper('rut_firma',json2),'FALLA','','TEST','');
			return json2;
		else
			--Firme Correctamente
			--insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json_upper('rut_firma',json2),'OK','','TEST','');
			json2:=bitacora10k(json2,'FIRMA','Prueba de Firma OK.');
			
			--PLANES_10K -- Revisamos si el usuario es representante de alguna empresa y actualizamos su clave...
			pass1:=get_json('pass',json2);
			rut_usuario1:=get_json('rutUsuario',json2)||'-'||modulo11(get_json('rutUsuario',json2));
			select * into campo from empresa_certificacion_datos where rut_representante=rut_usuario1;	
			if found then
				update empresa_certificacion_datos set pass_cert_representante=pass1 where rut_representante=rut_usuario1 and pass_cert_representante<>pass1;
			end if;
        	end if;
	elsif (strpos(resp1,'HTTP/1.1 500 ')>0) then
--		json2:=logjson(json2,'Falla Firma en HSM ----- '||resp1);
		resp1:=json_get('ERROR',json_resp1);
                json2:=response_requests_6000_upper('2','Falla Firma en HSM '||resp1, '',json2);
		--insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json_upper('rut_firma',json2),'FALLA1',resp1,'TEST','');
		json2:=bitacora10k(json2,'FIRMA','Falla Firma Test '||resp1);
		json2:=logjson(json2,'valida_firma_12793' || resp1);
		return json2;
        else
                   json2:=response_requests_6000_upper('2', 'Servicio de Firma no responde', '',json2);
		   --insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json_upper('rut_firma',json2),'FALLA2','Servicio de Firma no responde','TEST','');
		   json2:=bitacora10k(json2,'FIRMA','Servicio de Firma no responde');
		   json2:=logjson(json2,'valida_firma_12793' || 'Servicio de Firma no responde ' || resp1);
		   return json2;
        end if;
	
	rut_usuario1:=get_json('rutUsuario',json2);
	rut_usuario1:=rut_usuario1||'-'||modulo11(rut_usuario1);
        rut_cliente1:=get_json('rutCliente',json2);
	rut_cliente1:=rut_cliente1||'-'||modulo11(rut_cliente1);
	pass1:=get_json('pass',json2);
	--Guardamos la clave ahora q sabemos q esta OK
	select * into campo from empresa_certificacion_datos where rutempresa=rut_cliente1 and rut_representante=rut_usuario1;
	if found then
		update empresa_certificacion_datos set pass_cert_representante=pass1 where rutempresa=rut_cliente1 and rut_representante=rut_usuario1;
	end if;

        json2:=response_requests_6000_upper('1', 'Firma Subida y Testeada Correctamente', '',json2);
	return json2;
END;
$$ LANGUAGE plpgsql;


