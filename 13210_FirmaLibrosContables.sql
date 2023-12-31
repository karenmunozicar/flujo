DELETE from isys_querys_tx where llave='13210';

-- Validamos si se puede enviar el libro
insert into isys_querys_tx values ('13210',1,16,1,'select lce.valido_envio_libro(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('13210',2,1,8,'Servicio de Obtencion CoCertif',13250,109,106,0,0,1,1);

-- Validamos credenciales 
insert into isys_querys_tx values ('13210',5,1,8,'Servicio de Validacion de Firma',13220,109,106,0,0,10,10);

insert into isys_querys_tx values ('13210',10,16,1,'select lce.encabezado_acepta(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('13210',20,1,1,'select preparar_firma(''$$__JSONCOMPLETO__$$'') AS __json__',0,0,0,1,1,-1,0); 

--Vamos a HSM
insert into isys_querys_tx values ('13210',60,1,2,'Servicio de Firma 192.168.3.17',4013,109,106,0,0,70,70);

insert into isys_querys_tx values ('13210',70,1,1,'select valida_resp_firma_libro_13210(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('13210',80,1,1,'select eliminar_firma_lista(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

--Vamos a Publicar
--insert into isys_querys_tx values ('13210',210,1,8,'Publica LCE',12704,0,0,0,0,230,230);
insert into isys_querys_tx values ('13210',210,1,8,'Publica LCE',13230,0,0,0,0,230,230);

--Enviamos al EDTE
insert into isys_querys_tx values ('13210',230,1,8,'Se envia libro LCE al EDTE',13240,0,0,0,0,240,240);
--insert into isys_querys_tx values ('13210',230,1,8,'Se envia libro LCE al EDTE',13200,0,0,0,0,240,240);

insert into isys_querys_tx values ('13210',240,1,1,'select verifica_grabacion_edte_13210(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('13210',300,16,1,'select lce.actualiza_estado_libro_lce(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,0,0);

-- Salida forzada por Credenciales incorrectas
insert into isys_querys_tx values ('13210',500,1,1,'select salida_lce_6000(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);


-----------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION preparar_firma(i_parametros_json json)
RETURNS json AS
$BODY$
DECLARE
    o_json              json := i_parametros_json;
    v_nodo_id           varchar;
    v_xml_envio         text;
    v_xml_sii           text;
    v_xml_edte          text;
    v_rut_firmante      varchar;
    v_data_firma        varchar;
    v_rut_empresa       varchar;
    v_codigo_acceso     varchar;
    v_json_parte_firma  json;
    v_parte_firma       varchar;
    v_firma             varchar;
    v_envio_obligatorio text;
    v_envio_mensual     varchar;
    v_contador          integer := 0;
    v_partes_envio      json;
    v_cont_envio        integer;
    v_data_envio        text := '';
    v_json_index        json;
    v_valor_parte       text;
    v_encabezado_mensual text := '3c4c6365456e76696f4c6962726f7320786d6c6e733d22687474703a2f2f7777772e7369692e636c2f5369694c63652220786d6c6e733a7873693d22687474703a2f2f7777772e77332e6f72672f323030312f584d4c536368656d612d696e7374616e6365222076657273696f6e3d22312e3022207873693a736368656d614c6f636174696f6e3d22687474703a2f2f7777772e7369692e636c2f5369694c6365204c6365456e76696f4c6962726f735f7631302e787364223e';
BEGIN
    o_json := put_json(o_json,'__SECUENCIAOK__','0');

    --Si se acaba la recursion
    IF (get_json('FIN',o_json)='SI') THEN
        o_json := logjson(o_json,'Fin de Recursion SI');
        -- No faltan firmas
        IF (get_json('FIRMA_EXITOSA', o_json) = '1') THEN
            -- Si el Envio realizado es Mensual o Especial*/
            -- Se concatenan las respuestas de las firmas en el XML_SII
            IF (get_json('TIPO_LIBRO', o_json) = 'MENSUAL_ESPECIAL') THEN
                --Json que viene del lce.armar_libro_xml, en Hex     
                v_envio_mensual:= get_json('XML_SII', o_json);
                v_partes_envio:= get_json('PARTES_FIRMA', o_json);
                v_cont_envio := count_array_json(v_partes_envio);

                v_contador := 0;
                v_data_envio:='';
                --Obtengo Nodos Firmados + El Nodo del Envio Final
                --TODO: "ESTA CODIGO DEBE SER CAMBIADO"
                WHILE (v_cont_envio >= v_contador +1) LOOP
                    -- Obtenemos la Parte y el ID de la Firma
                    v_json_index := get_json_index(v_partes_envio, v_contador);
                    -- Obtenemos la Parte de obtencion del JSON
                    v_firma := get_json('FIRMA', v_json_index);
                    -- Obtenemos el valor a concatenar al XML Final
                    v_valor_parte := get_json(v_firma, o_json);
                    -- Concatena al XML Envio
                    IF (v_contador = 0) THEN
                        v_valor_parte := replace(v_valor_parte, v_encabezado_mensual, '');
                        v_valor_parte := replace(v_valor_parte, '3c2f4c6365456e76696f4c6962726f733e', '');
                        v_valor_parte := v_valor_parte || '3c4c43453e';
                    END IF;
                    v_data_envio := v_data_envio || v_valor_parte || '0a';
                    v_contador := v_contador + 1;
                    o_json := put_json(o_json, v_firma, '');
               END LOOP;
               v_data_envio    := v_encabezado_mensual || v_data_envio || '3c2f4c43453e3c2f4c6365456e76696f4c6962726f733e';
               v_envio_mensual := v_data_envio;
               --v_envio_mensual := replace(v_envio_mensual, '3c786d6c5f6c63655f656e76696f2f3e', v_data_envio);
               --v_envio_mensual := replace(v_envio_mensual, '3c786d6c5f6c63655f656e76696f3e3c2f786d6c5f6c63655f656e76696f3e', v_data_envio);
               v_envio_mensual := replace(v_envio_mensual, '3c3f786d6c2076657273696f6e3d22312e302220656e636f64696e673d2249534f2d383835392d31223f3e', '');
               v_envio_mensual:= '3c3f786d6c2076657273696f6e3d22312e302220656e636f64696e673d2249534f2d383835392d31223f3e' || v_envio_mensual;
               --v_xml_envio := hex_2_base64(v_envio_mensual);
               --perform logfilefirmador('GAC 3 XML ENVIO-> ' || v_xml_envio);
               o_json := put_json(o_json, 'XML_SII', v_envio_mensual);
            END IF;
            
            --perform logfilefirmador('GAC LCE FIRMA EXITOSA ');
            --Tomo el XML formato Acepta y lo envio al Publicador 
            v_xml_envio := base642hex(get_json('XML_ENVIO',o_json));
            --perform logfilefirmador('GAC LCE ENVIO: ' || v_xml_envio);
            --Toma el Json XML_SII que viene firmado con todos los Nodos_ID
            --6f5f786d6c5f736969
            --v_xml_edte:= replace(v_xml_envio, '6f5f786d6c5f736969', base642hex(get_json('XML_SII', o_json)));
            v_xml_sii := get_json('XML_SII', o_json);
            --perform logfilefirmador('GAC LCE SII: ' || v_xml_sii);
            v_xml_edte:= replace(v_xml_envio, '6f5f786d6c5f736969', v_xml_sii);
            --v_xml_edte:= replace(v_xml_edte, '<?xml version="1.0" encoding="ISO-8859-1"?>', '');
            v_xml_edte:= replace(v_xml_edte, '3c3f786d6c2076657273696f6e3d22312e302220656e636f64696e673d2249534f2d383835392d31223f3e', '');

            o_json:= put_json(o_json,'INPUT', v_xml_edte);
            perform logfilefirmador('GAC LCE PUBLICA INPUT: ' || v_xml_edte);
            --Vamos al Publicador
            o_json:= put_json(o_json,'XML_ENVIO','');
            o_json:= put_json(o_json,'__SECUENCIAOK__','210');
        ELSE
            o_json:= put_json(o_json,'__SECUENCIAOK__','0');
        END IF;
        RETURN o_json;
    END IF;

    IF (get_json('CONTADOR_PARTES', o_json) = '') THEN 
        v_contador:= 0;
    ELSE 
        v_contador:= get_json('CONTADOR_PARTES', o_json)::integer;
    END IF;

    --Obtengo los Json del JsonList PARTES_FIRMA, para enviar al firmador
    v_json_parte_firma:= get_json_index(get_json('PARTES_FIRMA', o_json)::json, v_contador);

    v_nodo_id:= get_json('FIRMA_ID', v_json_parte_firma);
    --Obtiene la data XML del Json FIRMA
    v_parte_firma:= get_json('FIRMA', v_json_parte_firma);

    --v_xml_envio = decode(get_json('XML_ENVIO', o_json)::text, 'hex');
    v_xml_envio = get_json(v_parte_firma, o_json)::text;
    o_json := logjson(o_json,'Nodo a Firmar ' || v_nodo_id);

    --Firma Sobre del EnvioObligatorio
    IF strpos(v_nodo_id,'envioOblig_') > 0 THEN
        v_rut_firmante:= get_json('RUT_FIRMANTE_SOBRE', o_json);
        v_codigo_acceso:= corrige_pass(decode(get_json('LAST_PASS',o_json),'hex')::text);
        -- v_envio_obligatorio := hex_2_ascii(base642hex(get_json('XML_SII', o_json)));
        v_envio_obligatorio := get_json('XML_SII', o_json); -- valor en HEX
        /*v_envio_obligatorio := replace(v_envio_obligatorio, '<xml_lce_mayor_res/>', hex_2_ascii(get_json('MAYOR_RESUMEN', o_json)) || chr(13) );
        v_envio_obligatorio := replace(v_envio_obligatorio, '<xml_lce_diario_res/>', hex_2_ascii(get_json('DIARIO_RESUMEN', o_json)) || chr(13) );
        v_envio_obligatorio := replace(v_envio_obligatorio, '<xml_lce_balance/>', hex_2_ascii(get_json('BALANCE', o_json)) || chr(13) );
        v_envio_obligatorio := replace(v_envio_obligatorio, '<xml_lce_diccionario/>', hex_2_ascii(get_json('DICCIONARIO', o_json)) || chr(13) );
        v_envio_obligatorio := replace(v_envio_obligatorio, '<?xml version="1.0" encoding="ISO-8859-1"?>', '');*/
        v_envio_obligatorio := replace(v_envio_obligatorio, '3c786d6c5f6c63655f6d61796f725f7265732f3e', get_json('MAYOR_RESUMEN', o_json) || '0a' );
        v_envio_obligatorio := replace(v_envio_obligatorio, '3c786d6c5f6c63655f64696172696f5f7265732f3e', get_json('DIARIO_RESUMEN', o_json) || '0a' );
        v_envio_obligatorio := replace(v_envio_obligatorio, '3c786d6c5f6c63655f62616c616e63652f3e', get_json('BALANCE', o_json) || '0a' );
        v_envio_obligatorio := replace(v_envio_obligatorio, '3c786d6c5f6c63655f64696363696f6e6172696f2f3e', get_json('DICCIONARIO', o_json) || '0a' );
        v_envio_obligatorio := replace(v_envio_obligatorio, '3c3f786d6c2076657273696f6e3d22312e302220656e636f64696e673d2249534f2d383835392d31223f3e', '');

        -- Eliminamos todas las referencias de <?xml version="1.0" encoding="ISO-8859-1"?>
        v_envio_obligatorio := replace(v_envio_obligatorio, '3c3f786d6c2076657273696f6e3d22312e302220656e636f64696e673d2249534f2d383835392d31223f3e', '');
        -- Al principio del XML agregamos "<?xml version="1.0" encoding="ISO-8859-1"?>"
        v_envio_obligatorio := '3c3f786d6c2076657273696f6e3d22312e302220656e636f64696e673d2249534f2d383835392d31223f3e' || v_envio_obligatorio;
        v_xml_envio := hex_2_base64(v_envio_obligatorio);
        --perform logfilefirmador('GAC LCE FIRMANDO : ' || v_xml_envio);
        o_json := logjson(o_json,'Obligatorio ' || v_xml_envio);
        --Firma Sobre del EnvioMensual
    /*ELSIF strpos(v_nodo_id,'ENVIO') > 0 THEN
        --TODO: Test Rut Firma Final
        v_rut_firmante := get_json('RUT_FIRMANTE_SOBRE', o_json);
        --o_json:= logjson(o_json, '(JCC) V_RUT_FIRMANTE Envio == '|| v_rut_firmante);
        v_codigo_acceso := corrige_pass(decode(get_json('LAST_PASS',o_json),'hex')::text);

        --Json que viene del lce.armar_libro_xml, en Hex     
        --v_envio_mensual := hex_2_ascii(base642hex(get_json('XML_SII', o_json)));
        v_envio_mensual:= base642hex(get_json('XML_SII', o_json));

        --v_envio_mensual := encode(v_envio_mensual::bytea, 'hex');
        --v_envio_mensual := ascii_2_hex(v_envio_mensual);
        v_partes_envio:= get_json('PARTES_FIRMA', o_json);
        v_cont_envio := count_array_json(v_partes_envio);

        v_contador := 0;
        v_data_envio:='';
        --Obtengo Nodos Firmados + El Nodo del Envio Final
        --TODO: "ESTA CODIGO DEBE SER CAMBIADO"
        WHILE (v_cont_envio > v_contador +1) LOOP
            -- Obtenemos la Parte y el ID de la Firma
            v_json_index := get_json_index(v_partes_envio, v_contador);
            -- Obtenemos la Parte de obtencion del JSON
            v_firma := get_json('FIRMA', v_json_index);
            -- Obtenemos el valor a concatenar al XML Final
            v_valor_parte := get_json(v_firma, o_json);
            -- Concatena al XML Envio
            v_data_envio := v_data_envio || v_valor_parte || '0a';
            --v_data_envio := v_data_envio || base642hex(v_valor_parte) || '0a';
            --v_data_envio:= v_data_envio || v_valor_parte;
            v_contador := v_contador + 1;
        END LOOP;

        --v_data_envio :=  base642hex(v_data_envio);

        --Reemplaza tag xml_lce_envio/ por la date de envio
        v_envio_mensual := replace(v_envio_mensual, '3c786d6c5f6c63655f656e76696f2f3e', v_data_envio);
        --v_envio_mensual := replace(v_envio_mensual, '<xml_lce_envio/>', hex_2_ascii(v_data_envio));

        --Reemplaza todos los <?xml version="1.0" encoding="ISO-8859-1"?> de la data final del envio
        v_envio_mensual := replace(v_envio_mensual, '3c3f786d6c2076657273696f6e3d22312e302220656e636f64696e673d2249534f2d383835392d31223f3e', '');
        --v_envio_mensual := replace(v_envio_mensual, '<?xml version="1.0" encoding="ISO-8859-1"?>', '');

        --Concatena el UTF8 solo al inicio    
        v_envio_mensual:= '3c3f786d6c2076657273696f6e3d22312e302220656e636f64696e673d2249534f2d383835392d31223f3e' || v_envio_mensual;
        --v_envio_mensual:= '3c3f786d6c2076657273696f6e3d22312e302220656e636f64696e673d2249534f2d383835392d31223f3e' || v_envio_mensual;
        --v_envio_mensual:='<?xml version="1.0" encoding="ISO-8859-1"?>' || v_envio_mensual;

        --Convierte el HEX a BASE64
        v_xml_envio := hex_2_base64(v_envio_mensual);
    */
    ELSE
        --Firma Por Nodo. 
        --v_rut_firmante := get_json('RUT_FIRMA', o_json);
        v_rut_firmante := get_json('rut_firma', o_json);
        --v_codigo_acceso := corrige_pass(decode(get_json('PASS',o_json),'hex')::text);
        v_codigo_acceso := get_json('PASS',o_json);
    END IF;

    v_rut_empresa  := get_json('RUT_EMPRESA', o_json);

    --REQUEST XML al Firmador.  
    v_data_firma := replace('{"documento":"'|| v_xml_envio
            || '","nodoId":"'|| v_nodo_id ||'","rutEmpresa":"'||v_rut_empresa||'-'
            || modulo11(v_rut_empresa)||'","entidad":"SII","rutFirmante":"'||v_rut_firmante||'-'||modulo11(v_rut_firmante)
            || '","codigoAcceso":"'||v_codigo_acceso||'"}',chr(10),'');
 
    --o_json := get_parametros_motor_json(o_json,'FIRMADOR');
    o_json := get_parametros_motor_json(o_json,get_parametro_firmador(v_rut_firmante||'-'||modulo11(v_rut_firmante)));


    --Envia Nodo al HSM.  
    o_json := put_json(o_json,'INPUT_FIRMADOR', 
            'POST ' || get_json('PARAMETRO_RUTA', o_json) || ' HTTP/1.1'||chr(10)||'User-Agent: curl/7.26.0'
            ||chr(10)||'Host: ' || get_json('__IP_CONEXION_CLIENTE__',o_json) ||chr(10)||'Accept: */*'||chr(10)||'Content-Type: application/json'
            ||chr(10)||'Content-Length: '||length(v_data_firma)::varchar||chr(10)||chr(10)||v_data_firma);

    IF (v_data_firma is null) THEN
        o_json := put_json(o_json,'__SECUENCIAOK__','80');
        RETURN o_json;
    END IF;
    o_json := put_json(o_json,'FIRMA_ACTUAL', v_nodo_id);
    o_json := put_json(o_json,'PARTE_ACTUAL', v_parte_firma);
    o_json := put_json(o_json,'__SECUENCIAOK__','60');
    --perform logfilefirmador('GAC LCE FIRMANDO: ' || v_nodo_id || ' - ' || v_parte_firma);
    RETURN o_json;
END
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION eliminar_firma_lista(i_parametros_json json)
RETURNS json AS
$BODY$
DECLARE
    o_json          json := i_parametros_json;
    v_firmas_ids    json;
    v_firma_actual  varchar;
    v_parte_firma   json;
    v_parte_actual  varchar;
    v_contador      integer;
    v_total_list    integer;
BEGIN
    IF (get_json('CONTADOR_PARTES', o_json) ='' ) THEN
        v_contador:=0;
    ELSE 
        v_contador:=get_json('CONTADOR_PARTES', o_json)::integer;
    END IF;

    v_parte_firma := get_json('PARTES_FIRMA', o_json);
    v_parte_actual := get_json('PARTE_ACTUAL', o_json);

    v_total_list := count_array_json(v_parte_firma);

    IF (v_total_list - 1 > v_contador) THEN
        o_json := put_json(o_json, 'PARTES_FIRMA', v_parte_firma::text);
        o_json := put_json(o_json, 'CONTADOR_PARTES',(v_contador + 1)::text);
        --Al recorrer por ultima vez la lista, debe enviar el ENVIO_DIARIO al EDTE
    ELSE
        o_json := put_json(o_json, 'FIN', 'SI');
    END IF;                 

    o_json := put_json(o_json, '__SECUENCIAOK__', '20');
    o_json := put_json(o_json, 'FIRMA_EXITOSA', '1');
    RETURN o_json;
END
$BODY$
LANGUAGE plpgsql;

DROP FUNCTION valida_resp_firma_libro_13210(json);
CREATE or replace FUNCTION valida_resp_firma_libro_13210 (json) RETURNS json AS $$
DECLARE
    json1           alias for $1;
    json2           json;
    json3           json;
    pos1            integer;
    pos12           integer;
    resp1           varchar;
    geturl          varchar;
    dteF            varchar;
    json_resp1      varchar;
    data_firma1     varchar;
    clave_firmante  varchar;
    id_doc          varchar;
    data1           varchar;
    v_parte_actual  varchar;
BEGIN
    json2:=json1;
    json3:='{}';
    json2:=respuesta_no_chunked_json(json2);
    resp1:=decode(get_json('RESPUESTA_HEX',json2),'hex');

    json_resp1:=split_part(resp1,'\012\012',2);
    json2:=put_json(json2,'INPUT_FIRMADOR','');
    json2:=put_json(json2,'__SECUENCIAOK__','0');

    IF (strpos(resp1,'HTTP/1.1 500 ')>0) THEN
        json2:=logjson(json2,'Firma Libro: RESPUESTA_SYSTEM='||replace(resp1,chr(10),'-'));
        resp1:=json_get('ERROR',json_resp1);
        --Si no hay respuesta
        IF (length(resp1)=0) then
            resp1:='Falla en Firmar Libro (1)';
        END IF;
        --resp1:='Error: '||split_part(split_part(resp1,'<faultstring>com.acepta.custodiafirma.servicios.ServicioFirmaXMLException:',2),'</faultstring>',1);
        --Cuando no tiene certificado Cargado, informamos URIS para carga de certificados y manuales para realizar el proceso.
        IF strpos(resp1,'No existen certificados para el rut') >0 THEN
            json2:=logjson(json2,'Rut sin Certificado Rut='||get_json('RUT_FIRMA',json2));
            json3:=put_json(json3,'TXT_URI_CARGA','Carga de Certificados');
            json3:=put_json(json3,'URI_CARGA','https://apps.acepta.com/firma-web/instalarCertificado.html');
            json3:=put_json(json3,'TXT_MANUAL1','Instrucciones Para crear archivo');
            json3:=put_json(json3,'URI_MANUAL1','https://www.acepta.com/wp-content/uploads/2014/06/CA-E-01-Respalda-desde-Internet-Explorer.pdf');
        END IF;

        -- json2:=put_json(json2,'URI_MANUAL2','');

        json2:=logjson(json2,'Firma Libro: No logra firmar Libro 1 (2)');        
        json2:=response_requests_6000('223', resp1, json3::varchar,json2);
        --json2:=response_requests_6000('223', resp1, '',json2);
        insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json('RUT_FIRMA',json2),'FALLA',resp1,'LCE',get_json('URI_IN',json2));
        RETURN json2;
    --Si nes un HTTP/1.1 200, fallamos
    ELSIF (strpos(resp1,'HTTP/1.1 200')=0) THEN
        json2:=logjson(json2,'Firma Libro: No logra firmar libro 2 RESP -> ' || resp1);
        --json2:=logjson(json2,'Firma Libro: RESPUESTA_SYSTEM='||replace(resp1,chr(10),'-'));
        json2:=response_requests_6000('2', 'Falla Firma Libro','',json2);
        insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json('RUT_ISSUER',json2),'FALLA','Servicio de Firma no responde','LCE',get_json('URI_IN',json2));
        RETURN json2;
    END IF;

    --Si http request OK. Respuesta del Firmador. 
    --json2:=logjson(json2,'JCC-documentoFirmado ==' || substring(json_resp1,1,500));
    dteF:=json_get('documentoFirmado',json_resp1);
    IF (length(dteF)=0) THEN
        json2:=logjson(json2,'Firma Libro: No logra firmar libro 3');
        json2:=put_json(json2,'__SECUENCIAOK__','0');
        json2:=response_requests_6000('2', 'Falla Firma Libro (3)','',json2);
        insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json('RUT_FIRMA',json2),'FALLA','No contesta Libro Firmado','LCE',get_json('URI_IN',json2));
        RETURN json2;
    END IF;

    --....FIRMA EXITOSA...
    json2:=logjson(json2,'LCE: Firma Exitosa Libro 1' || get_json('PARTE_ACTUAL', json2));
    data1:=base642hex(dteF);
    --json2 := logjson(json2, 'XML FirmadoHEX -> ' || data1);

    /*valida_schema:=xsd_validador_lce(data1);
    if(valida_schema<>'OK')then
    json2:=logjson(json2,'Firma Libro: No logra firmar libro 3');
    json2:=put_json(json2,'__SECUENCIAOK__','0');
    json2:=response_requests_6000('2', 'Falla Firma Libro.','',json2);
    insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json('RUT_FIRMA',json2),'FALLA','No contesta Libro Firmado','LCE',get_json('URI_IN',json2));
    return json2;
    end if;*/

    data1:=replace(data1,encode('__REMPLAZA_URI__','hex')::varchar,encode(get_json_upper('URI_IN',json2)::bytea,'hex')::varchar);

    --Reemplaza en el Json la parte actual, por la parte actual firmada. 
    --json2 := logjson(json2, 'JCC-RSP-PARTE_ACTUAL -> ' || get_json('PARTE_ACTUAL', json2));
    v_parte_actual := get_json('PARTE_ACTUAL', json2);
    json2:=put_json(json2,v_parte_actual,data1);

    json2:=put_json(json2,'CONTENT_LENGTH',length(data1)::varchar);
    insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json('RUT_FIRMA',json2),'OK','','LCE',get_json('URI_IN',json2));
    --Hay que terminar de leer la lista jsom
    json2:=put_json(json2,'__SECUENCIAOK__','80');
    RETURN json2;
END;
$$ LANGUAGE plpgsql;


DROP FUNCTION verifica_grabacion_edte_13210(json);
CREATE OR REPLACE FUNCTION verifica_grabacion_edte_13210(json)
RETURNS json
LANGUAGE plpgsql
AS $function$
DECLARE
    json1   alias for $1;
    json2   json;
BEGIN
    json2:=json1;
    json2:=put_json(json2,'__SECUENCIAOK__','0');

    --Si publico OK
    IF (get_json('__PUBLICADO_OK__',json2)<>'SI') THEN
        json2:=logjson(json2,'Falla la Publicacion en Almacen '||get_json('URI_IN',json2));
        json2:=response_requests_6000('2', 'Falla Publicacion de Libro, Reintente por favor.','',json2);
        RETURN json2;
    END IF;
    --Si grabo bien en el EDTE
    IF (get_json('__EDTE_LCE_OK__',json2)<>'OK') THEN
        json2:=logjson(json2,'Falla Grabacion al EDTE == '||get_json('URI_IN',json2));
        json2:=response_requests_6000('2', 'Falla Envio al SII, Reintente por favor.','',json2);
    END IF;

    json2:=put_json(json2,'ESTADO_LIBRO','PUB');    --Estado Firmado y Publicado
    json2:=put_json(json2,'__SECUENCIAOK__','300');
    json2:=put_json(json2,'URI_LIBRO_LCE','https://almacen.acepta.com/ca4webv3/?url='||get_json('URI_IN',json2));
    json2:=put_json(json2,'TRAZA_URI_LCE','https://motor-prod.acepta.com/bitacora/?url='||get_json('URI_IN',json2));
    json2:=response_requests_6000('1', 'Firmado OK','https://almacen.acepta.com/ca4webv3/?url='||get_json('URI_IN',json2),json2);
    RETURN json2;
END
$function$

