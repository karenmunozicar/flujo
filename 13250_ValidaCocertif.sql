DELETE from isys_querys_tx WHERE llave='13250';

-- Validamos si el CoCertif existe
INSERT INTO isys_querys_tx VALUES ('13250',1,16,1,'select lce.obtiene_cocertif(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

--Vamos a HSM a Firmar CAL
INSERT INTO isys_querys_tx VALUES ('13250',60,1,2,'Servicio de Firma 192.168.3.17',4013,109,106,0,0,70,70);

INSERT INTO isys_querys_tx VALUES ('13250',70,1,1,'select valida_resp_firma_libro_13250(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

-- Agregamos CoCertif a la tabla lce.dominio
INSERT INTO isys_querys_tx VALUES ('13250',80,16,1,'select lce.agregar_cocertif(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

CREATE or replace FUNCTION valida_resp_firma_libro_13250 (json) RETURNS json AS $$
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
        json2:=logjson(json2,'Firma CoCertif: RESPUESTA_SYSTEM='||replace(resp1,chr(10),'-'));
        resp1:=json_get('ERROR',json_resp1);
        --Si no hay respuesta
        IF (length(resp1)=0) then
            resp1:='Falla en Firmar CoCertif (1)';
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

        json2:=logjson(json2,'Firma CoCertif: No logra firmar CoCertif 1 (2)');
        json2:=response_requests_6000('223', resp1, json3::varchar,json2);
        --json2:=response_requests_6000('223', resp1, '',json2);
        insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json('RUT_FIRMA',json2),'FALLA',resp1,'LCE',get_json('URI_IN',json2));
        RETURN json2;
    --Si nes un HTTP/1.1 200, fallamos
    ELSIF (strpos(resp1,'HTTP/1.1 200')=0) THEN
        json2:=logjson(json2,'Firma CoCertif: No logra firmar CoCertif 2 RESP -> ' || resp1);
        --json2:=logjson(json2,'Firma Libro: RESPUESTA_SYSTEM='||replace(resp1,chr(10),'-'));
        json2:=response_requests_6000('2', 'Falla Firma CoCertif','',json2);
        insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json('RUT_ISSUER',json2),'FALLA','Servicio de Firma no responde','LCE',get_json('URI_IN',json2));
        RETURN json2;
    END IF;

    --Si http request OK. Respuesta del Firmador. 
    --json2:=logjson(json2,'JCC-documentoFirmado ==' || substring(json_resp1,1,500));
    dteF:=json_get('documentoFirmado',json_resp1);
    IF (length(dteF)=0) THEN
        json2:=logjson(json2,'Firma Libro: No logra firmar CoCertif 3');
        json2:=put_json(json2,'__SECUENCIAOK__','0');
        json2:=response_requests_6000('2', 'Falla Firma CoCertif (3)','',json2);
        insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json('RUT_FIRMA',json2),'FALLA','No contesta CoCertif Firmado','LCE',get_json('URI_IN',json2));
        RETURN json2;
    END IF;

    --....FIRMA EXITOSA...
    json2:=logjson(json2,'LCE: Firma Exitosa Libro 1');
    data1:=base642hex(dteF);

    data1:=replace(data1,encode('__REMPLAZA_URI__','hex')::varchar,encode(get_json_upper('URI_IN',json2)::bytea,'hex')::varchar);

    --Reemplaza en el Json la parte actual, por la parte actual firmada. 
    json2:=put_json(json2,'COCERTIF',data1);
    json2:=put_json(json2,'RESPUESTA_HEX','');
    json2:=put_json(json2,'CONTENT_LENGTH',length(data1)::varchar);
    insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json('RUT_FIRMA',json2),'OK','','LCE',get_json('URI_IN',json2));
    --Hay que terminar de leer la lista jsom
    json2:=put_json(json2,'__SECUENCIAOK__','80');
    RETURN json2;
END;
$$ LANGUAGE plpgsql;

