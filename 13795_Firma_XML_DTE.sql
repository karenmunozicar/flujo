--Publica documento
delete from isys_querys_tx where llave='13795';

--insert into isys_querys_tx values ('13795',5,19,1,'select obtiene_dte_13795(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);
--FAY 20190602 se cambia la base de ejecucion, la base principal
insert into isys_querys_tx values ('13795',5,1,1,'select obtiene_dte_13795(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('13795',10,1,1,'select obtiene_firmante_13795(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('13795',20,1,2,'Servicio de Firma offline 172.16.14.87',4013,109,106,0,0,30,30);

insert into isys_querys_tx values ('13795',30,1,1,'select registra_dte_13795(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0); 


CREATE OR REPLACE FUNCTION obtiene_firmante_13795(json) RETURNS json AS $$
DECLARE
        json1           	alias for $1;
        json2           	json;
	v_data_input		varchar;
	v_referencias		varchar;
	v_rut_emisor		varchar;
	v_rut_emisor_int	integer;
	v_rut_firma		varchar;
	v_clave			varchar;
	xml_dte1		varchar;
	id1			varchar;
	data_firma1		varchar;
BEGIN

        json2 := json1;
        json2 := put_json(json2, '__SECUENCIAOK__', '0');
	-- 201808006 AVC se lee parametro RUT_USUARIO agregado en reglas.parseo
        v_rut_firma := get_json('RUT_USUARIO_ACCION', json2);
        v_rut_emisor := get_json('RUT_EMISOR', json2);
	if is_numeric(v_rut_emisor) then
		v_rut_emisor_int:=v_rut_emisor::integer;
	else
		json2 := logjson(json2, 'rut_emisor no numerico' || v_rut_emisor);
                json2 := bitacora10k(json2, 'FIRMAR', 'Error en Rut emisor');
                if(get_json('__PROCESOXML__', json2) = 'PROCESADOR_ESCRITORIO') then
                        json2 := response_requests_6000('2','Rut Emisor no numerico ', json2);
                end if;
                return json2;

	end if;


	json2 := logjson(json2,'F 13795 Rut firma-->'|| v_rut_firma); 		

	if v_rut_firma='' and v_rut_emisor_int='96697410' then
		if (select count(*) from rut_firma_clave where rut_emisor=v_rut_emisor_int)=1 then
			json2 := logjson(json2,'Buscamos en rut_firma_clave dado que v_rut_firma vacio '||get_json('URI_IN', json2));
			json2:=put_json(json2,'RUT_USUARIO_ACCION',(select rut_firmante from rut_firma_clave where rut_emisor=v_rut_emisor_int)::varchar);
        		v_rut_firma := get_json('RUT_USUARIO_ACCION', json2);
		end if;
	end if;
--        select rut_firmante, decode(clave, 'hex')  into v_rut_firma, v_clave from rut_firma_clave where rut_emisor=v_rut_emisor::integer ;
        select decode(clave, 'hex') into v_clave from rut_firma_clave where rut_emisor=v_rut_emisor_int and rut_firmante=v_rut_firma;
        if not found then
                json2 := logjson(json2, 'No se encuentra firmador para el emisor ' || v_rut_emisor);
                json2 := bitacora10k(json2, 'FIRMAR', 'No se encuentra firmador para el emisor');
                if(get_json('__PROCESOXML__', json2) = 'PROCESADOR_ESCRITORIO') then
                        json2 := response_requests_6000('2','No se encuentra firmador para el emisor', '', json2);
                end if;
                return json2;
        end if;
        v_clave := corrige_pass(v_clave);
	--RME Se sacan los parametros antes de cambiar de BD
	--json2:=get_parametros_motor_json(json2,'FIRMADOR');
        json2:=get_parametros_motor_json(json2,get_parametro_firmador(v_rut_firma||'-'||modulo11(v_rut_firma)));

	-- AVC
	v_data_input := get_json('INPUT', json2);
        v_referencias := get_json('ADD_REFERENCIAS', json2);
	json2:=logjson(json2,'ADD_REFERENCIAS='||v_referencias);

	xml_dte1 := actualiza_dte_py(v_data_input, v_referencias);
	id1:='T' || get_xml_hex1('TipoDTE', xml_dte1) || 'F' || get_xml_hex1('Folio', xml_dte1);

	data_firma1 := replace('{"documento":"' || hex2ascii2base64(xml_dte1) || '"'
                || ',"nodoId":"' || id1 || '"'
                || ',"rutEmpresa":"' || v_rut_emisor || '-' || modulo11(v_rut_emisor) || '"'
                || ',"entidad":"SII"'
                || ',"rutFirmante":"' || v_rut_firma || '-'||modulo11(v_rut_firma) || '"'
                || ',"codigoAcceso":"'  || replace(v_clave, chr(92), chr(92) || chr(92)) || '"}',
                chr(10),
                '');
	json2 := logjson(json2, 'data: ' || data_firma1);

	json2 := put_json(json2, 'INPUT_FIRMADOR', 'POST '
                        || get_json_upper('PARAMETRO_RUTA', json2)
                        || ' HTTP/1.1' ||chr(10) || 'User-Agent: curl/7.26.0' || chr(10)
                        || 'Host: ' || get_json_upper('__IP_CONEXION_CLIENTE__',json2) || chr(10)
                        || 'Accept: ' || chr(42) || '/' || chr(42) || chr(10)
                        || 'Content-Type: application/json; charset=ISO-8859-1' || chr(10)
                        || 'Content-Length: ' || length(data_firma1)::varchar || chr(10)
                        || chr(10)
                        || data_firma1::varchar
                );

        json2 := put_json(json2, 'XML_DTE', '');
        json2 := put_json(json2, 'REQUEST_URI', '');
        json2 := put_json(json2, 'QUERY_STRING', '');
        json2 := put_json(json2, '__SECUENCIAOK__', '20');

	return json2;

END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION obtiene_dte_13795(json) RETURNS json AS $$
DECLARE
	json1		alias for $1;
	json2		json;

	v_rut_emisor	varchar;
	v_uri		varchar;
	v_data		varchar;
	v_data_input	varchar;
	v_referencias	varchar;
	
	jaux	json;
BEGIN
		
	json2 := json1;
	json2 := put_json(json2, '__SECUENCIAOK__', '0');
	
	v_rut_emisor := get_json('RUT_EMISOR', json2);
        --json2 := logjson(json2, 'rut_emisor: ' || v_rut_emisor);

	v_uri := get_json('URI', json2);
        json2 := logjson(json2, 'uri: ' || v_uri);
	if v_uri='' then
		v_uri := get_json('URI_IN', json2);
		json2 := logjson(json2, 'uri_in: ' || v_uri);
	end if;

	--RME y AVC 20181022 cuando ya fue procesado, sale y continua con la publicacion
	if get_json('__FLAG_REFERENCIAS_PRE_EMISION__',json2) = 'SI' then
		json2 := put_json(json2, '__SECUENCIAOK__', '0');
		json2 := logjson(json2, 'Documento ya habia sido proecesado anteriormente');
		return json2;	
	end if;
	
	--FAY-DAO 20200409 si viene de pantalla sacamos el INPUT de las colas
	if get_json('__ID_DTE__',json2)='' then
		jaux=query_bd_colas('select data from colas_motor_generica where uri ='''||v_uri||''' and rut_emisor='''||v_rut_emisor||''' and get_campo(''TX'',data)=''8010'' limit 1');
		if get_json('STATUS',jaux)='SIN_DATA' then
		--select data into v_data from colas_motor_generica where uri = v_uri and rut_emisor = v_rut_emisor limit 1;
		--if not found then
			json2 := logjson(json2, 'No se encuentra mensaje en la cola');
			json2 := bitacora10k(json2, 'FIRMAR', 'No se encuentra mensaje en la cola');
			if(get_json('__PROCESOXML__', json2) = 'PROCESADOR_ESCRITORIO') then
				json2 := response_requests_6000('2','No se encuentra mensaje en la cola', '', json2);
			end if;
			return json2;
		else
			json2 := logjson(json2, 'Se encuentra DTE en la cola');
			--FAY 20190602 si lo encuentra
			v_data:=get_json('data',jaux);
			json2 := put_json(json2, 'INPUT', get_campo('INPUT', v_data));
			json2:=logjson(json2,'RUT_USUARIO_ACCION v_data='||get_campo('RUT_USUARIO_ACCION',v_data));
		end if;
	end if;	

/*	select rut_firmante, decode(clave, 'hex')  into v_rut_firma, v_clave from rut_firma_clave where rut_emisor=v_rut_emisor::integer;
	if not found then
		json2 := logjson(json2, 'No se encuentra firmador para el emisor ' || v_rut_emisor);
		json2 := bitacora10k(json2, 'FIRMAR', 'No se encuentra firmador para el emisor');
		if(get_json('__PROCESOXML__', json2) = 'PROCESA_SCGI_2') then
                        json2 := response_requests_6000('2','No se encuentra firmador para el emisor', '', json2);
                end if;
		return json2;
	end if;
	v_clave := corrige_pass(v_clave);
	json2 := put_json(json2, 'rut_firma', v_rut_firma);
*/	
	json2:=logjson(json2,'RUT_USUARIO_ACCION json2='||get_json('RUT_USUARIO_ACCION',json2));
	if get_json('RUT_USUARIO_ACCION',json2) ='' then
		json2 := logjson(json2, 'Pisamos el RUT_USUARIO_ACCION con el de la cola');
		json2 := put_json(json2, 'RUT_USUARIO_ACCION', get_campo('RUT_USUARIO_ACCION', v_data));
	end if;
	json2 := put_json(json2, '__SECUENCIAOK__', '10');
	return json2;

END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION registra_dte_13795(varchar) RETURNS json AS $$
DECLARE
	json1			alias for $1;
	json2			json;
	--xml2			varchar;
	v_nombre_cola		varchar;
	resp1			varchar;
	json_resp1		varchar;
	aux1			varchar;
	v_documento_firmado	varchar;
	--id1			integer;
	v_uri			varchar;
	v_rut_emisor		varchar;
	v_input			varchar;
	v_mensaje		record;
	v_rut_firma		varchar;
	i			integer;

	jaux	json;
	jmensaje	json;
	v_mensaje1	varchar;
BEGIN
	json2 := json1;
	json2 := respuesta_no_chunked_json(json2);
	json2 := put_json(json2,'__SECUENCIAOK__', '0');
	--json2 := logjson(json2, 'Retorna del servicio firmador');

	resp1 := decode(get_json_upper('RESPUESTA_HEX',json2), 'hex');
	--json2 := logjson(json2, 'respuesta hex: ' || resp1);
	json_resp1 := split_part(resp1,'\012\012', 2);
	--json2 := logjson(json2, 'json resp: ' || json_resp1);
	
	json2 := put_json(json2,'RESPUESTA_HEX','');
	json2 := put_json(json2,'INPUT_FIRMADOR','');
	json2 := put_json(json2,'XML_DTE','');


	if (strpos(resp1,'HTTP/1.1 200 ')>0) then
		aux1 := json_get('documentoFirmado', json_resp1);
		--json2 := logjson(json2, 'documento firmado: ' || aux1);

		if (length(aux1)>0) then
			-- 20180806 AVC
                        --v_rut_firma := get_json_upper('rut_firma',json2);
                        v_rut_firma := get_json_upper('RUT_USUARIO_ACCION',json2);
			--Obtengo el documento firmado
        		v_documento_firmado := base642hex(aux1);
			--json2 := logjson(json2, 'documentoFirmado 1: ' || v_documento_firmado);
			v_documento_firmado := regexp_replace(v_documento_firmado, '.*(3c445445.*3c2f4454453e).*', '\1');
			--json2 := logjson(json2, 'documentoFirmado 2: ' || v_documento_firmado);

			insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),
				get_json_upper('rut_firma',json2),
				'OK',
				'',
				'DTE',
				get_json_upper('URI_IN',json2)
			);
			
			--json2 := put_json(json2,'__FLAG_PUB_10K__','SI');
			
			json2 := put_json(json2,'RESPUESTA','');

			v_rut_emisor := get_json('RUT_EMISOR', json2);
		        v_uri := get_json('URI', json2);
			--RME 20181019 se prueba URI_IN
			if v_uri='' then
				v_uri := get_json('URI_IN', json2);
				json2 := logjson(json2, 'FALLA URI--URI_IN: ' || v_uri);
			end if;
			json2 := logjson(json2, 'URI: ' || v_uri||' EMISOR '|| v_rut_emisor);

			jmensaje=query_bd_colas('insert into respaldo_procesa_pre_emitidos select id,fecha,uri,reintentos,data,xml_flags,tx,rut_emisor,reproceso,categoria,nombre_cola,rut_receptor,tipo_dte,folio,fecha_act,fecha_ingreso from colas_motor_generica where uri ='''||v_uri||''' and rut_emisor='''||v_rut_emisor||''' and get_campo(''TX'',data)=''8010'' limit 1 returning *');

			/*
			insert into respaldo_procesa_pre_emitidos
			select * from colas_motor_generica where uri = v_uri and rut_emisor = v_rut_emisor limit 1
			returning * into v_mensaje;
			if not found then
				json2 := logjson(json2,'No se pudo grabar en respaldo_procesa_pre_emitidos URI='||v_uri);
			end if;
			*/
			json2 := logjson(json2, 'jmensaje='||jmensaje::varchar);
			if get_json('STATUS',jmensaje)='OK' then

				v_mensaje1=get_json('data',jmensaje);

				-- reemplazar fragmento del input que tiene la firma
				v_input := get_campo('INPUT', v_mensaje1);
				json2 := logjson(json2, 'paso1: ' || v_input);

				-- <DatoAdjunto nombre="ReferenciaAdjuntada">SI</DatoAdjunto>
				v_input := regexp_replace(v_input, '3c445445.*3c2f4454453e', v_documento_firmado); -- || '3C4461746F41646A756E746F206E6F6D6272653D225265666572656E63696141646A756E74616461223E53493C2F4461746F41646A756E746F3E');
				 -- 20180806 AVC
                        	v_input := regexp_replace(v_input, '3C5369676E6572733E.*3C2F5369676E6572733E', encode(('<Signers><Signer><PI Type="Rut">' || v_rut_firma || '</PI></Signer></Signers>')::bytea, 'hex'));
				json2 := logjson(json2, 'paso2: ' || v_input);
				json2 := put_json(json2, 'INPUT', v_input);

				--v_mensaje.data := put_campo(v_mensaje.data, 'INPUT', v_input);
				v_mensaje1:=put_campo(v_mensaje1,'INPUT', v_input);
				json2 := put_json(json2, 'CONTENT_LENGTH', (length(v_input)/2)::varchar);

				--v_mensaje.data := put_campo(v_mensaje.data, 'CONTENT_LENGTH', (length(v_input)/2)::varchar);
				v_mensaje1:=put_campo(v_mensaje1,'CONTENT_LENGTH', (length(v_input)/2)::varchar);
				--v_mensaje.data := put_campo(v_mensaje.data,'__FLAG_REFERENCIAS_PRE_EMISION__','SI');
				v_mensaje1:=put_campo(v_mensaje1,'__FLAG_REFERENCIAS_PRE_EMISION__','SI');
				--v_mensaje.data := put_campo(v_mensaje.data, '__FECHA_FUTURO_COLA__', '');
		
			
				jaux=query_bd_colas('update colas_motor_generica set data=''' ||v_mensaje1 ||'''' ||', fecha=now()'|| ' where uri=''' ||v_uri || ''' and rut_emisor=''' || v_rut_emisor || '''');
				json2 := logjson(json2, 'jaux='||jaux::varchar);
				/*
				execute 'update ' || v_mensaje.nombre_cola
				|| ' set data=''' || v_mensaje.data || ''''
				|| ', fecha=now()' 
				|| ' where uri=''' || v_mensaje.uri || ''' and rut_emisor=''' || v_mensaje.rut_emisor || '''';

				--RME y AVC 20181022 Se valida respuesta del update
				GET DIAGNOSTICS i = ROW_COUNT;
				if i=0 t	hen
					json2 := put_json(json2, 'REFERENCIA_OK','FALLA');
					return json2;
				end if;	
			
				*/


				--json2 := logjson(json2, 'nombre_cola: ' || v_mensaje.nombre_cola);
				json2 := logjson(json2, 'data: ' || v_mensaje1);
				json2 := logjson(json2, 'uri: ' || v_uri);
				json2 := logjson(json2, 'rut_emisor: ' || v_rut_emisor);

				json2 := bitacora10k(json2, 'FIRMAR', 'Firma OK');
                	        json2 := logjson(json2,'Firma OK');
				
				json2 := put_json(json2, 'RESPUESTA', 'Firma OK');
				json2 := put_json(json2, '__SECUENCIAOK__', '0'); 
				if(get_json('__PROCESOXML__', json2) = 'PROCESADOR_ESCRITORIO') then
	                	        json2 := response_requests_6000('1','Actualización Exitosa', '', json2);
					json2 := put_json(json2, '__SECUENCIAOK__', '0');
				else
					json2 := put_json(json2, '__SECUENCIAOK__', '0');
        		        end if;
				json2 := put_json(json2, 'REFERENCIA_OK','SI');
			end if;

			return json2;
		else
			json2 := logjson(json2,'Falla Firma');
			resp1 := json_get('ERROR',json_resp1);
			if (length(resp1)=0) then
				resp1 := 'Servicio Firma Electronica no responde.<br>Reintente más tarde.';
			end if;
			insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json_upper('rut_firma',json2),'FALLA',resp1,'DTE',get_json_upper('URI_IN',json2));
			json2 := bitacora10k(json2,'FIRMA','Firma Falla');
			if(get_json('__PROCESOXML__', json2) = 'PROCESADOR_ESCRITORIO') then
        	                json2 := response_requests_6000('2','Firma Falla', '', json2);
	                end if;
			json2 := put_json(json2, 'REFERENCIA_OK','FALLA');

		end if;
	elsif (strpos(resp1,'HTTP/1.1 500 ')>0) then
		json2 := logjson(json2,'FIRMADOR ' || resp1);
		json2 := logjson(json2,'Falla Firma error 500');
		resp1 := json_get('ERROR',json_resp1);
		if (length(resp1) = 0) then
			resp1 := 'Servicio de Validación de Firma Electronica no responde.<br>Reintente más tarde.';
		end if;
		insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json_upper('rut_firma',json2),'FALLA',resp1,'DTE',get_json_upper('URI_IN',json2));
		json2 := bitacora10k(json2,'FIRMA','Firma Falla');
		json2 := put_json(json2, 'REFERENCIA_OK','FALLA');

	else
		json2 := logjson(json2,'Falla Firma error XXX');
		insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json_upper('rut_firma',json2),'FALLA','Servicio de Firma no responde','DTE',get_json_upper('URI_IN',json2));
		json2 := bitacora10k(json2,'FIRMA','Firma Falla');
		json2 := put_json(json2, 'REFERENCIA_OK','FALLA');

	end if;
	if(get_json('__PROCESOXML__', json2) = 'PROCESADOR_ESCRITORIO') then
		json2 := response_requests_6000('2','Firma Falla', '', json2);
        end if;

	return json2;
END
$$ LANGUAGE plpgsql;
