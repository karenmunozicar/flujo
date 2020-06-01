delete from isys_querys_tx where llave='12703';

-- Prepara llamada al AML
insert into isys_querys_tx values ('12703',10,1,1,'select proc_recibidos_fcgi_12703(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

--Borra en la Base del Motor
insert into isys_querys_tx values ('12703',1000,1,1,'select proc_verifica_fin_dte_BaseMotor_12703(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,0,0);
--Borra en la Base de Colas
insert into isys_querys_tx values ('12703',1010,19,1,'select proc_verifica_fin_dte_BaseColas_12703(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,0,0);

--Puerto 4003
--insert into isys_querys_tx values ('12703',50,1,2,'Llamada directo al AML Mordor',14003,102,101,0,0,100,100);
--AML REC CGE
--insert into isys_querys_tx values ('12703',55,1,2,'Llamada directo al AML CGE',14010,102,101,0,0,100,100);
--Parsea respuesta para FastCGI
--insert into isys_querys_tx values ('12703',100,1,1,'select proc_respuesta_recibidos_fcgi_12703(''$$__XMLCOMPLETO__$$'') as __XML_NUEVO__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('12703',500,1,8,'Flujo 9999 Basura',9999,0,0,1,1,100,100);


--Verifica el control de termino del DTE
CREATE or replace FUNCTION proc_verifica_fin_dte_BaseMotor_12703(varchar) RETURNS varchar AS $$
DECLARE
        xml1        alias for $1;
        xml2    varchar;
BEGIN
    xml2:=xml1;
    xml2 := sp_procesa_respuesta_cola_motor_original(xml2);
    return xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION proc_verifica_fin_dte_BaseColas_12703(varchar) RETURNS varchar AS $$
DECLARE
        xml1        alias for $1;
        xml2    varchar;
BEGIN
    xml2:=xml1;
    xml2 := sp_procesa_respuesta_cola_motor_original(xml2);
    return xml2;
END;
$$ LANGUAGE plpgsql;


--Genera un XML para ir a borrar, con datos basicos
CREATE or replace FUNCTION limpia_xml_12703(varchar) RETURNS varchar AS $$
DECLARE
        xml2        alias for $1;
	xml3	varchar;
BEGIN
	if (get_campo('FLAG_NO_LIMPIA',xml2)='SI') then
		return xml2;
	end if;
	xml3:='';
	xml3 := put_campo(xml3,'__ID_DTE__',get_campo('__ID_DTE__',xml2));
	xml3 := put_campo(xml3,'__COLA_MOTOR__',get_campo('__COLA_MOTOR__',xml2));
	xml3 := put_campo(xml3,'CODIGO_TXEL',get_campo('CODIGO_TXEL',xml2));
	xml3 := put_campo(xml3,'RESPUESTA',get_campo('RESPUESTA',xml2));
	xml3 := put_campo(xml3,'MENSAJE_XML_FLAGS',get_campo('MENSAJE_XML_FLAGS',xml2));
	xml3 := put_campo(xml3,'_ID_REPROCESO_',get_campo('_ID_REPROCESO_',xml2));
	xml3 := put_campo(xml3,'ID_DTE_ERROR_REPROCESO',get_campo('ID_DTE_ERROR_REPROCESO',xml2));
	xml3 := put_campo(xml3,'_ESTADO_REPROCESO_',get_campo('_ESTADO_REPROCESO_',xml2));
	xml3 := put_campo(xml3,'CODIGO_TXEL_REPROCESO',get_campo('CODIGO_TXEL_REPROCESO',xml2));
	xml3 := put_campo(xml3,'CANAL',get_campo('CANAL',xml2));
	xml3 := put_campo(xml3,'_LOG_',get_campo('_LOG_',xml2));
	if(get_campo('_CATEGORIA_BD_',xml2)='COLAS')then
		xml3 := put_campo(xml3,'__SECUENCIAOK__','1010');
	else
		xml3 := put_campo(xml3,'__SECUENCIAOK__','1000');
	end if;
        return xml3;
END;
$$ LANGUAGE plpgsql;




CREATE or replace FUNCTION proc_recibidos_fcgi_12703(varchar) RETURNS varchar AS $$
DECLARE
    xml1	alias for $1;
    xml2	varchar;
    data1	varchar;
    data_hex2	varchar;
    file1	varchar;
    sts		integer;
    header1	varchar;
    url1	varchar;
    host1	varchar;
    uri1	varchar;
    resp1	varchar;
    respuesta1	varchar;
    status1	varchar;
    codigo1	bigint;
    rut2        varchar;
    tipo_dte1   varchar;
	campo	record;
	inserto_MaEmpresas      varchar;
	j3          json;
BEGIN
    xml2:=xml1;
   
    --FAY-DAO 20180409 este flag se usa para controlar en el flujo 8031
    xml2:=put_campo(xml2,'FLAG_12703','FALLA');

    --Si no esta publicado no seguimos
    --verifico si el documento ya fue publicado en el almacen, em caso contrario no sigo procesando
    if (get_campo('__PUBLICADO_OK__',xml2)<>'SI') then
        xml2:=logapp(xml2,'Falla la Publicacion en Almacen '||get_campo('URI_IN',xml2));

        --20150224 FAY si algun DTE viene sin URI_IN no puede ser procesado, se guarda en cola_motor_sin_uri y se borr a de las colas de trabajo
        if (length(get_campo('URI_IN',xml2))=0) then
                xml2 := sp_graba_cola_sin_uri(xml2);
                xml2 := put_campo(xml2,'RESPUESTA','Status: 200 OK');
	        xml2 :=	limpia_xml_12703(xml2);	
                return xml2;
        end if;

        --Si es Borrador, lo dejo pasar., se maneja en las reglas
        if (strpos(get_campo('URI_IN',xml2),'http://pruebas')=0) then
                xml2 := put_campo(xml2,'RESPUESTA','Status: 400 NK');
	        xml2 :=	limpia_xml_12703(xml2);	
                return xml2;
        end if;
    end if;
    
    xml2:=logapp(xml2,'__ID_DTE__='||get_campo('__ID_DTE__',xml2));
    xml2:=logapp(xml2,'__COLA_MOTOR__='||get_campo('__COLA_MOTOR__',xml2));

    --Si no viene INPUT es un GET
    if length(get_campo('INPUT',xml2))=0 then
    	data1:=get_campo('QUERY_STRING',xml2);
    else
	data_hex2:=get_campo('INPUT',xml2);
    	data1:=decode(data_hex2,'hex');
    end if;
    --xml2 := logapp(xml2,'HEX='||data_hex2||'***');

    --xml2 := logapp(xml2,'DATA='||data1||'***');
	
    url1:=get_campo('SCRIPT_URL',xml2);
    host1:=get_campo('HTTP_HOST',xml2);
    xml2 := put_campo(xml2,'HTTP_CONTENT_TYPE',get_campo('CONTENT_TYPE',xml2));
    xml2 := put_campo(xml2,'HTTP_CONTENT_LENGTH',get_campo('CONTENT_LENGTH',xml2))i;
    --Verifico que venga la URI



    --uri1:=split_part(split_part(data1,'filename="',2),'"',1);
    --La URI de los recibidos viene en URI_IN
    uri1:=get_campo('URI_IN',xml2);
    xml2:=logapp(xml2,'URI='||uri1);
    if (length(uri1)>0) then
        --El servicio esta en esa URL
        --url1:='/ca4/ca4dte';
        --host1:='pruebascge-pub.acepta.com';
        --xml2 := put_campo(xml2,'SCRIPT_NAME','/ca4/ca4rec');
        --xml2 := put_campo(xml2,'SERVER_NAME','pruebascge-pub.acepta.com');
        --xml2 := put_campo(xml2,'SCRIPT_URL','/ca4/ca4rec');
        --xml2 := put_campo(xml2,'SCRIPT_URI','http://pruebascge-pub.acepta.com/ca4/ca4rec');
        --xml2 := put_campo(xml2,'REQUEST_URI','/ca4/ca4rec');
        xml2 := put_campo(xml2,'TIPO_TX','FACTURA_RECIBIDA');


	--Los documentos recibidos vienen con este TAG, sino es una respuesta de recepcion
	--Se asume que solo viene 1 documento
	if (strpos(data1,'<DTE ')>0) then
        	xml2 := logapp(xml2,'Recibe Documento (12703) '||uri1);
        	--Parseo los datos entrantes
		xml2 := parseo_doc_recibido(xml2);

            	xml2:=procesa_documentos_relacionados(xml2);

		xml2 := reglas.validacion(xml2);
    		--Si la regla debe llamar otro flujo..
		if get_campo('__EXIT__',xml2)='1' then
		         resp1:=get_campo('RESPUESTA',xml2);
		         respuesta1:=split_part(resp1,chr(10)||chr(10),2);
		         if (strpos(resp1,'200 OK')>0) then
		 		xml2 := logapp(xml2,'Respuesta Servicio 200 OK URI'||get_campo('URI',xml1));
                		xml2 := put_campo(xml2,'RESPUESTA','Status: 200 OK');
         		else
				--Si lo rechazamos solo una vez
	                	xml2 := logapp(xml2,'Respuesta Servicio 444 Rechazado (12703) URI'||get_campo('URI',xml1));
                		xml2 := put_campo(xml2,'RESPUESTA','Status: 444 NK');
        		end if;
	        	xml2 :=	limpia_xml_12703(xml2);	
        		RETURN xml2;
	        end if;
		

		--si tiene que enviar XML al ERP se hace en esta funcion
		--2015-11-26 FAY-RME-KMS-PCS Si alguna regla genera una condicion especifica que no requiera
		--grabar al erp, se marca este campo con NO
		if (get_campo('GRABACION_ERP',xml2)<>'NO') then
			--Verifico si tengo que enviar este DTE al ERP
			rut2:=get_campo('RUT_RECEPTOR',xml2);
			tipo_dte1:=get_campo('TIPO_DTE',xml2);
			select * into campo from traza.rut_dnd where rut=rut2 and recibidos='SI' and not (tipo_dte_recibidos @@ tipo_dte1::tsquery);
			if found then
				xml2 := put_campo(xml2,'ENVIO_ERP','SI');
			else
				xml2 := put_campo(xml2,'ENVIO_ERP','NO');
			end if;
			--RME 20180710 Se comenta graba_dte_envio_erp para que no envie DTE sin estado.
		 	--xml2 := put_campo(xml2,'EVENTO','EMI');	
			--xml2 := graba_dte_envio_erp(xml2); 
		end if;


		/*
		-- NBV-DAO Maestro Empresas 20180117
		if(length(get_campo('INPUT',xml2))>0) then
			if(get_campo('TIPO_DTE',xml2) not in ('39','41')) then
				BEGIN
					xml2:=logapp(xml2,'INSERTO EMISOR-RECEPTOR EN maestro_empresas');
					xml2:=logapp(xml2,'INSERTO EMISOR-RECEPTOR EN maestro_empresas CANAL=>'||get_campo('CANAL',xml2)::varchar||' RUT_EMISOR=>'||get_campo('RUT_EMISOR',xml2)::varchar||' RUT_RECEPTOR=>'||get_campo('RUT_RECEPTOR',xml2)::varchar);
					j3:='{}';
					j3:=put_json(j3,'CANAL',get_campo('CANAL',xml2));
					j3:=put_json(j3,'RUT_EMISOR',get_campo('RUT_EMISOR',xml2));
					j3:=put_json(j3,'RAZON_SOCIAL_EMISOR',get_campo('RAZON_SOCIAL_EMISOR',xml2));
					j3:=put_json(j3,'RUT_RECEPTOR',get_campo('RUT_RECEPTOR',xml2));
					j3:=put_json(j3,'RAZON_SOCIAL_RECEPTOR',get_campo('RAZON_SOCIAL_RECEPTOR',xml2));

					inserto_MaEmpresas:=insertar_maestro_empresas(j3);
				EXCEPTION WHEN OTHERS THEN
					xml2:=logapp(xml2,'FALLA INSERTO  EN maestro_empresas '||get_campo('CANAL',xml2)||' '||get_campo('RUT_EMISOR',xml2)||' '||get_campo('RAZON_SOCIAL_EMISOR',xml2)||' '||get_campo('RUT_RECEPTOR',xml2)||' '||get_campo('RAZON_SOCIAL_RECEPTOR',xml2));
				END;
                	end if;
        	else
                	xml2:=logapp(xml2,'NO INSERTO EMISOR-RECEPTOR EN maestro_empresas');
        	end if;
    		-- NBV 20180117
		*/

		--Grabo en dte_recibido el documento 
		xml2 := put_campo(xml2,'ESTADO_INICIAL_DTE','PUBLICADO');
        	xml2 := insert_dte(xml2);
		--El Motor graba ahora el estado EMITIDO para los recibidos, porque viene con monto

		--Solo si grabo en dte_recibidos
		if (get_campo('FLAG_DTE_RECIBIDO_GRABADO_OK',xml2)='SI') then
			--Guardo los eventos en traza
			xml2:= put_campo(xml2,'COMENTARIO_TRAZA','');
			xml2 := graba_bitacora(xml2,'INGRESADO');
			xml2:= put_campo(xml2,'FECHA_EVENTO',get_campo('FECHA_FIRMA',xml2));
			xml2 := graba_bitacora(xml2,'FRM');
		
			xml2:= put_campo(xml2,'FECHA_EVENTO',now()::varchar);
			xml2 := graba_bitacora(xml2,'EMI');

			-- Se revisa si el dte esta en tabla pendientes del reporte consolidado
			xml2 := revisa_pendientes(xml2); 	
			
			-- Grabo en la cola para que webiecv lo saque posteriormente
			xml2 := graba_cola_webiecv(xml2);
		end if;
		--xml2 := graba_colaprocesamiento_webiecv(xml2);
	else
        	xml2 := logapp(xml2,'Recibe Comprobante (12703)');
	end if;

	--Un documento reprocesado, que se envia al AML, se puede borrar de la cola de procesamiento
        if (get_campo('_REPROCESO_',xml2)='SI') then
                xml2 := put_campo(xml2,'_ESTADO_REPROCESO_','OK');
        end if;

        xml2 := put_campo(xml2,'RESPUESTA','Status: 200 OK');
	xml2 :=	limpia_xml_12703(xml2);	
    	xml2:=put_campo(xml2,'FLAG_12703','OK');
	return xml2;

    else
	--Si no es nada de lo de arriba, es basura
    	xml2 := put_campo(xml2,'__SECUENCIAOK__','500');
	xml2 := logapp(xml2,'Recibe Tx No Identificada (12703)');
    end if;
    --Debo Agregar el header a INPUT para que el resto funcione OK
    header1:='POST '||url1||' HTTP/1.1'||chr(10)||'Host: '||host1||chr(10)||'Content-Type: '||get_campo('CONTENT_TYPE',xml2)||chr(10)||'Content-Length: '||get_campo('CONTENT_LENGTH',xml2)||chr(10)||chr(10);
    xml2:=put_campo(xml2,'INPUT',header1||data1);
    RETURN xml2;
END;
$$ LANGUAGE plpgsql;
