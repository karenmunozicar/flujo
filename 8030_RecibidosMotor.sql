delete from isys_querys_tx where llave='8030';

insert into isys_querys_tx values ('8030',5,1,1,'select pivote_xml_8030(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('8030',6,1,1,'select pivote_json_8030(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('8030',10,1,1,'select ensobra_dte_8030(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,1000);

--Primero que hacemos el publicar DTE Recibido
insert into isys_querys_tx values ('8030',40,1,8,'Publica DTE',112704,0,0,0,0,50,50);

--Proceso el DTE REcibido
insert into isys_querys_tx values ('8030',50,1,1,'select verifica_publicacion_rec_8030(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('8030',54,1,2,'Servicio de SII 172.16.14.88',4013,100,101,0,0,55,55);
insert into isys_querys_tx values ('8030',55,1,1,'select verifica_resp_sii_8030(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('8030',60,1,1,'select genera_crt_8030(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,1000);

insert into isys_querys_tx values ('8030',70,1,2,'Servicio de Firma 192.168.3.17',4013,109,106,0,0,80,80);

insert into isys_querys_tx values ('8030',80,1,1,'select verifica_firma_crt_8030(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);
--Publica CRT
insert into isys_querys_tx values ('8030',90,1,8,'Publica DTE',112704,0,0,0,0,100,100);
insert into isys_querys_tx values ('8030',100,1,1,'select valida_pub_crt_8030(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
--Envia mail CRT
insert into isys_querys_tx values ('8030',110,19,1,'select send_mail_crt_8030(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);
--Graba evento
insert into isys_querys_tx values ('8030',120,1,1,'select graba_evento_crt_8030(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('8030',1000,19,1,'select sp_procesa_respuesta_cola_motor88_json(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,0,0);


--Graba MENSAJE_XML_FLAGS
CREATE or replace FUNCTION genera_mensaje_flags_8030(varchar,varchar,varchar) RETURNS varchar AS $$
declare
	sec1	alias for $1;
	input1	alias for $2;
	tipo1	alias for $3;
	aux1	varchar;
	input2	varchar;
begin
	aux1:='<SECUENCIA>'||sec1||'</SECUENCIA>';
	if (tipo1='XML') then
		input2:=put_campo(input1,'_LOG_','');
		input2:=put_campo(input2,'XML_FLAGS','');
	elsif (tipo1='JSON') then 
		input2:=put_json(input1::json,'_LOG_','')::varchar;
		input2:=put_json(input2::json,'XML_FLAGS','')::varchar;
	end if;
	aux1:=aux1||'<INPUT>'||encode_hex(input2::varchar)||'</INPUT>';
	aux1:=aux1||'<TIPO_DATA>'||tipo1||'</TIPO_DATA>';
	aux1:=aux1||'<fecha>'||now()::varchar||'</fecha>';
	return aux1;
end;
$$ LANGUAGE plpgsql;


--Funcion que recibe 2 parametros secuencia donde quiero que parta el flujo y la foto del xml
CREATE or replace FUNCTION pivote_xml_8030(varchar) RETURNS varchar AS $$
DECLARE
	xml1	alias for $1;
	xml2	varchar;
	flags1	varchar;
	input1	varchar;
	campo	record;
BEGIN

	xml2:=xml1;	

	flags1:=get_campo('XML_FLAGS',xml2);
	--Si viene MENSAJE_XML_FLAGS, verificamos si tenemos q saltar a alguna secuencia
	if(get_xml('SECUENCIA',flags1)<>'') then
		--Si es un XML con el q parte la secuencia...
		if(get_xml('TIPO_DATA',flags1)='XML')then
			input1:=decode_hex(get_xml('INPUT',flags1));
			input1:=put_campo(input1,'__SECUENCIAOK__',get_xml('SECUENCIA',flags1));
			input1:=put_campo(input1,'__IP_CONEXION_CLIENTE__',split_part(get_campo('IPPORT_SII',input1),':',1));
			input1:=put_campo(input1,'__IP_PORT_CLIENTE__',split_part(get_campo('IPPORT_SII',input1),':',2));
			input1:=logapp(input1,'Se reprocesa '||get_campo('__ID_DTE__',xml2)||' XML, partiendo en SECUENCIA='||get_xml('SECUENCIA',flags1));
			--Se limpia para evitar recursion
			input1:=put_campo(input1,'MENSAJE_XML_FLAGS','');
			--El motor remplaza ; por ACK, lo volvemos normal
			input1:=replace(input1,chr(6),';');
			return input1;
		--Si es un JSON con el q parte la secuencia...
		elsif(get_xml('TIPO_DATA',flags1)='JSON') then
			xml2:=logapp(xml2,'Saltamos al pivote JSON');
			xml2:=put_campo(xml2,'__SECUENCIAOK__','6');
        		return xml2;
		else
			xml2:=logapp(xml2,'Partimos el flujo Normal');
			xml2:=put_campo(xml2,'__SECUENCIAOK__','10');
			return xml2;
		end if;
	--Si no empezamos el flujo desde 0
	else
		xml2:=logapp(xml2,'Partimos el flujo Normal..');
		xml2:=put_campo(xml2,'__SECUENCIAOK__','10');
        	return xml2;
	end if;
        return xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION pivote_json_8030(json) RETURNS json  AS $$
DECLARE
	json1	alias for $1;
	json2	json;
	flags1	varchar;
	input1	json;
BEGIN
	json2:=json1;
	flags1:=get_json('XML_FLAGS',json2);
	--Si viene MENSAJE_XML_FLAGS, verificamos si tenemos q saltar a alguna secuencia
	if(get_xml('SECUENCIA',flags1)<>'') then
		--Si es un XML con el q parte la secuencia...
		if(get_xml('TIPO_DATA',flags1)='JSON')then
			input1:=decode_hex(get_xml('INPUT',flags1))::json;
			input1:=put_json(input1,'__SECUENCIAOK__',get_xml('SECUENCIA',flags1));
			input1:=put_json(input1,'__IP_CONEXION_CLIENTE__',split_part(get_json('IPPORT_SII',input1),':',1));
			input1:=put_json(input1,'__IP_PORT_CLIENTE__',split_part(get_json('IPPORT_SII',input1),':',2));
			input1:=logjson(input1,'Se reprocesa JSON '||get_json('__ID_DTE__',json2)||' partiendo en SECUENCIA='||get_xml('SECUENCIA',flags1));
			--Se limpia para evitar recursion
			input1:=put_json(input1,'MENSAJE_XML_FLAGS','');
			--El motor remplaza ; por ACK, lo volvemos normal
			input1:=replace(input1::varchar,chr(6),';')::json;
			return input1;
		end if;
	end if;
	json2:=logjson(json2,'Partimos el flujo Normal');
	json2:=put_json(json2,'__SECUENCIAOK__','10');
	return json2;
END;
$$ LANGUAGE plpgsql;



CREATE or replace FUNCTION verifica_resp_sii_8030(varchar) RETURNS varchar AS $$
DECLARE
        xml1        alias for $1;
        xml2    varchar;
        --json_par1       json;
        json_aux        json;
        json_in         json;
        json_script1    json;
        resp_est        varchar;
        resp_cod        varchar;
        glosa_es        varchar;
        glosa_er        varchar;
        output1         varchar;
        fecha1          varchar;
        cola1   varchar;
        v_nombre_tabla  VARCHAR;
        rut1    varchar;
        aux1    varchar;
	json_out	json;
	j4	json;
BEGIN
    xml2:=xml1;

        xml2 := put_campo(xml2,'__SECUENCIAOK__','1000');
	output1:=get_campo('RESPUESTA',xml2);
	xml2:=logapp(xml2,'SII json='||replace(output1,chr(10),''));
	if(strpos(output1,'HTTP/1.0 200')=0) then
		xml2:=logapp(xml2,'Falla Respuesta del SII '||output1);
                xml2:=put_campo(xml2,'RESPUESTA','Status: 400 NK');
		perform libera_ipport_sii(get_campo('IPPORT_SII',xml2),'FALLA');
	
		--Vuelvo a poner la direccion porque el motor la borra
		--Guardamos el mensaje_flags para que comienze en otra secuencia
		xml2:=put_campo(xml2,'MENSAJE_XML_FLAGS',genera_mensaje_flags_8030('54',xml2,'XML'));
                return xml2;
        end if;
	
	--Si no es un json, reintentamos
        begin
                json_out:=split_part(output1,chr(10)||chr(10),2)::json;
                j4:=get_first_key_json(get_first_key_json(json_out::varchar));
        exception when others then
                xml2:=logapp(xml2,'Respuesta SII no es un json' );
                xml2:=put_campo(xml2,'RESPUESTA','Status: 400 NK');
		--Guardamos el mensaje_flags para que comienze en otra secuencia
		--Vuelvo a poner la direccion porque el motor la borra
		perform libera_ipport_sii(get_campo('IPPORT_SII',xml2),'FALLA');
		xml2:=put_campo(xml2,'MENSAJE_XML_FLAGS',genera_mensaje_flags_8030('54',xml2,'XML'));
                return xml2;
        end;
        
	perform libera_ipport_sii(get_campo('IPPORT_SII',xml2),'OK');
        resp_est:=get_json('ESTADO',j4);
        resp_cod:=get_json('ERR_CODE',j4);
        glosa_es:=get_json('GLOSA_ESTADO',j4);
        glosa_er:=get_json('GLOSA_ERR',j4);
        if(resp_cod<>'') then
                --resp_est:='FALLA';
                if(resp_est='DOK') then
                        xml2:=logapp(xml2,'DTE Documento Recibido por el SII. Datos Coinciden con los Registrados. ');
                        xml2:=put_campo(xml2,'COMENTARIO_TRAZA','Glosa: '||glosa_es||' ('||resp_est||')');
                        xml2:=put_campo(xml2,'EVENTO','ASI');
                        xml2:=actualiza_estado_dte(xml2);
                        xml2:=graba_bitacora(xml2,'ASI');
                        xml2 := put_campo(xml2,'__SECUENCIAOK__','60');
                        return xml2;
		--Se asume que si un DTE esta con una nota de credito en el SII, esta recibido, por ende aprobado
                elsif(resp_est in ('MMC','ANC','MMD','TMC','AND','TMD')) then
                        xml2:=logapp(xml2,'DTE Documento Recibido por el SII pero con Errores.');
                        xml2:=put_campo(xml2,'COMENTARIO_TRAZA','Glosa: DTE Documento Recibido por el SII.'||chr(10)||glosa_er||' ('||resp_cod||')');
                        xml2:=put_campo(xml2,'EVENTO','ASI');
                        xml2:=actualiza_estado_dte(xml2);
                        xml2:=graba_bitacora(xml2,'ASI');
                        xml2 := put_campo(xml2,'RESPUESTA','Status: 200 OK');
                        return xml2;
                elsif(resp_est in ('DNK')) then
                        xml2:=logapp(xml2,'DTE Documento Recibido por el SII pero Datos NO Coinciden con los registrados. ');
                        xml2:=put_campo(xml2,'COMENTARIO_TRAZA','Glosa: '||glosa_es||' ('||resp_est||')');
                        xml2:=put_campo(xml2,'EVENTO','RSI');
                        xml2:=actualiza_estado_dte(xml2);
                        xml2:=graba_bitacora(xml2,'RSI');
                        xml2 := put_campo(xml2,'RESPUESTA','Status: 200 OK');
                        return xml2;
		--Se crea un nuevo estado
		elsif(resp_est in ('FAN')) then
                        xml2:=logapp(xml2,'DTE Documento Anulado por el SII');
                        xml2:=put_campo(xml2,'COMENTARIO_TRAZA','Glosa: '||glosa_es||' ('||resp_est||')');
                        xml2:=put_campo(xml2,'EVENTO','RFAN');
                        xml2:=actualiza_estado_dte(xml2);
                        xml2:=graba_bitacora(xml2,'FAN');
                        xml2 := put_campo(xml2,'RESPUESTA','Status: 200 OK');
                        return xml2;

                elsif(resp_est in ('FAU','NA')) then
			--Aun no llega al sii, le damos tiempo
			if(now()-get_campo('FECHA_INGRESO_COLA',xml2)::timestamp>interval '2 days') then
                       		xml2 := put_campo(xml2,'RESPUESTA','Status: 200 OK');
				xml2:=logapp(xml2,'DTE por 2 dias, se borra de las cola');
				xml2:=put_campo(xml2,'EVENTO','RSI');
				resp_est:='RSI';
       		        else
                       		xml2 := put_campo(xml2,'RESPUESTA','Status: 444 NK');
				xml2:=put_campo(xml2,'EVENTO',resp_est);
				--Vuelvo a poner la direccion porque el motor la borra
				--Guardamos el mensaje_flags para que comienze en otra secuencia
				xml2:=put_campo(xml2,'MENSAJE_XML_FLAGS',genera_mensaje_flags_8030('54',xml2,'XML'));
	                end if;
			xml2:=put_campo(xml2,'COMENTARIO_TRAZA','Glosa: '||glosa_er||' ('||resp_cod||')');
			xml2:=graba_bitacora(xml2,resp_est);
                else
                       xml2 := put_campo(xml2,'RESPUESTA','Status: 400 NK');
                       xml2 := logapp(xml2,'Falla Consulta SII');
			--Vuelvo a poner la direccion porque el motor la borra
			--Guardamos el mensaje_flags para que comienze en otra secuencia
			xml2:=put_campo(xml2,'MENSAJE_XML_FLAGS',genera_mensaje_flags_8030('54',xml2,'XML'));
                       return xml2;
                end if;
        else
                --Lo graba en la cola para procesamiento posterior
                xml2 := logapp(xml2,'Falla Consulta SII');
                xml2 := put_campo(xml2,'RESPUESTA','Status: 400 NK');
		--Vuelvo a poner la direccion porque el motor la borra

		--Guardamos el mensaje_flags para que comienze en otra secuencia
		xml2:=put_campo(xml2,'MENSAJE_XML_FLAGS',genera_mensaje_flags_8030('54',xml2,'XML'));
                return xml2;
        end if;
        return xml2;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION verifica_publicacion_rec_8030(varchar) RETURNS varchar AS $$
DECLARE
        xml1        alias for $1;
        xml2    varchar;
	--json_par1	json;
	json_aux	json;
	json_in		json;
	json_script1	json;
	resp_est	varchar;
	resp_cod	varchar;	
	glosa_es	varchar;
	glosa_er	varchar;
	output1		varchar;
	fecha1		varchar;
	cola1	varchar;
	v_nombre_tabla	VARCHAR;
	rut1	varchar;
	aux1	varchar;
	port            varchar;
BEGIN
    xml2:=xml1;
    xml2 := put_campo(xml2,'FLAG_NO_LIMPIA','SI');
    xml2 := put_campo(xml2,'FIRMA_DA','1');
    xml2 := proc_recibidos_fcgi_12703(xml2);
    xml2 := logapp(xml2,'RESPUESTA proc_recibidos_fcgi_12703 '||get_campo('RESPUESTA',xml2));
    if strpos(get_campo('RESPUESTA',xml2),'200 OK')=0 then
		--Si fallo, aumento reintentos
		xml2 := logapp(xml2,'Falla proc_recibidos_fcgi_12703');
		xml2 := put_campo(xml2,'__SECUENCIAOK__','1000');
		return xml2;
    end if;

    --xml2 := put_campo(xml2,'FECHA_EVENTO',get_campo('FECHA_FIRMA',xml2));
    --xml2 := graba_bitacora(xml2,'FRM');

    --Se guarda Evento RCP (Procesado por el receptor)
    rut1:=get_campo('RUT_EMISOR',xml2);
    aux1:=(select email from contribuyentes where rut_emisor=rut1::integer);
    --perform logfile('F_8030 '||aux1||' '||rut1::varchar);
    xml2:=put_campo(xml2,'MAIL_EMISOR',aux1);

    --Si estaba repetido y ya tiene estado del sii
    if (get_campo('FLAG_DTE_RECIBIDO_REPETIDO',xml2)='SI') then
	--Si tengo estado del sii, vamos por el CRT
	if (get_campo('ESTADO_SII_DTE_REPETIDO',xml2)<>'') then
		xml2 := logapp(xml2,'DTE ya recibido, se envia CRT Repetido');
		xml2 := put_campo(xml2,'__SECUENCIAOK__','60');
		return xml2;
	end if; 	

	--Si no fue grabado por motor...
	if (get_campo('NUEVO_RECIBIDO',xml2)<>'1' and get_campo('RUT_RECEPTOR',xml2)='81201000') then
		xml2 := logapp(xml2,'DTE ya recibido de cencodud, se envia CRT Repetido');
		xml2 := put_campo(xml2,'__SECUENCIAOK__','60');
		return xml2;
	end if;	

	--Si es del motor y tiene distintas uri, se envia repetido
	 if (get_campo('NUEVO_RECIBIDO',xml2)='1' and get_campo('URI_DTE_REPETIDO',xml2)<>get_campo('URI_IN',xml2)) then
		--repetido	
		xml2 := logapp(xml2,'DTE ya recibido de motor, se envia CRT Repetido');
		xml2 := put_campo(xml2,'__SECUENCIAOK__','60');
		return xml2;
	end if;
    end if;

    xml2:=put_campo(xml2,'COMENTARIO_TRAZA','Emite: '||coalesce(aux1,'-'));
    xml2:=graba_bitacora(xml2,'RCP');                               

    --Guardo en la traza el sobre de envio
    xml2:=put_campo(xml2,'COMENTARIO_TRAZA',get_campo('eml',xml2));
    xml2:=graba_bitacora(xml2,'SOBRE_ENVIO');
	
    xml2 := put_campo(xml2,'__SECUENCIAOK__','1000');

    --Antes de ir al sii verificamos que no haya estado en el reporte consolidado..
    if(get_campo('FLAG_RC_OK',xml2)='SI') then
	xml2:=logapp(xml2,'DTE encontrado en reporte consolidado, se da por validado el DTE en el SII');
	--Damos por validado el DTE en el SII
	xml2:=put_campo(xml2,'COMENTARIO_TRAZA','Glosa: DTE Recibido (DOK)*');
	xml2:=put_campo(xml2,'EVENTO','ASI');
	--xml2:=put_campo(xml2,'FECHA_EVENTO_ASI',get_campo('FECHA_RC_OK',xml2));
	xml2:=actualiza_estado_dte(xml2);
	xml2:=graba_bitacora(xml2,'ASI');
	xml2 := put_campo(xml2,'__SECUENCIAOK__','60');
	return xml2;
    end if;

    --Si llega una boleta, hasta llega la recepcion
    if (get_campo('TIPO_DTE',xml2) in ('39','41')) then
	xml2:=logapp(xml2,'Boletas no van a comprobarse al SII');
	xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
	return xml2;
    end if;

	begin	
		fecha1:=to_char(get_campo('FECHA_EMISION',xml2)::timestamp,'DD-MM-YYYY');
	exception when others then
		--Si la fecha viene mal, se poner por defecto
		xml2:=logapp(xml2,'Fecha de Emision Invalida '||get_campo('FECHA_EMISION',xml2));
		fecha1:='01-01-1900';
	end;
	

	--port:=nextval('correlativo_servicio_sii')::varchar;
	port:=get_ipport_sii();
       --Si no hay puertos libres ...
        if (port='') then
                --Si no hay puertos libres...
               xml2:=logapp(xml2,'No hay puertos libres');
               xml2:=put_campo(xml2,'RESPUESTA','Status: 400 NK');
               xml2 := put_campo(xml2,'__SECUENCIAOK__','1000');
               return xml2;
        end if;

	json_in:='{"RutCompania":"'||get_campo('RUT_EMISOR',xml2)||'","DvCompania":"'||modulo11(get_campo('RUT_EMISOR',xml2))||'","RutReceptor":"'||get_campo('RUT_RECEPTOR',xml2)||'","DvReceptor":"'||modulo11(get_campo('RUT_RECEPTOR',xml2))||'","TipoDte":"'||get_campo('TIPO_DTE',xml2)||'","FolioDte":"'||get_campo('FOLIO',xml2)||'","FechaEmisionDte":"'||fecha1||'","URI":"'||get_campo('URI_IN',xml2)||'","RUT_OWNER":"'||get_campo('RUT_RECEPTOR',xml2)||'"}';
	json_in:=put_json(json_in,'MontoDte',get_campo('MONTO_TOTAL',xml2));

	xml2:=logapp(xml2,'SII json='||json_in::varchar);
    	xml2 := put_campo(xml2,'__SECUENCIAOK__','54');

        --xml2:=get_parametros_motor(xml2,'SERVICIO_SII');
	--Guardo en variables los parametros de envio, por si se reusa

	xml2:=put_campo(xml2,'__IP_CONEXION_CLIENTE__',split_part(port,':',1));
	xml2:=put_campo(xml2,'__IP_PORT_CLIENTE__',split_part(port,':',2));
	xml2:=put_campo(xml2,'IPPORT_SII',port);
		
	xml2:=put_campo(xml2,'IP_PORT_CLIENTE',split_part(port,':',2));
	xml2:=put_campo(xml2,'IP_CONEXION_CLIENTE',split_part(port,':',1));

        --xml2:=put_campo(xml2,'INPUT','POST /estado_dte HTTP/1.1'||chr(10)||'User-Agent: curl/7.26.0'||chr(10)||'Host: '||get_campo('__IP_CONEXION_CLIENTE__',xml2)||':'||port||chr(10)||'Accept: '||chr(42)||'/'||chr(42)||chr(10)||'Content-Type: application/json; charset=ISO-8859-1'||chr(10)||'Content-Length: '||length(json_in::varchar)::varchar||chr(10)||chr(10)||json_in::varchar);
        xml2:=put_campo(xml2,'INPUT','POST /estado_dte HTTP/1.1'||chr(10)||'User-Agent: curl/7.26.0'||chr(10)||'Host: '||port||chr(10)||'Accept: '||chr(42)||'/'||chr(42)||chr(10)||'Content-Type: application/json; charset=ISO-8859-1'||chr(10)||'Content-Length: '||length(json_in::varchar)::varchar||chr(10)||chr(10)||json_in::varchar);
	xml2:=put_campo(xml2,'RESPUESTA','');
	return xml2;

END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION public.ensobra_dte_8030(json) RETURNS json AS $$
DECLARE
    json1               alias for $1;
    json2               json;
        json3   json;
        patron_dte1     varchar;
        xml_dte1        varchar;
        dte1            varchar;
        esquema         varchar;
        uri1            varchar;
        rut1            varchar;
        campo           record;
        campo1          record;

	rut_emisor1	varchar;
	rut_emisor2	varchar;
	tipo_dte1	varchar;
	folio1		varchar;
	tms1		varchar;
	tms2		timestamp;
	rut_receptor2	bigint;
	certificado_x509	varchar;
	aux			varchar;
	rut_firma1		varchar;
	monto1		varchar;
	fecha1		varchar;

	json_in	json;
BEGIN
        json2:=json1;
	json2:=put_json(json2,'__SECUENCIAOK__','1000');

	--Sacamos el xml del mail
	xml_dte1:=get_json('XML',json2);
	--Limpio XML
	json2:=put_json(json2,'XML','');
	tipo_dte1:=get_xml_hex1('TipoDTE',xml_dte1);
	--Para corregir Visualizacion
	--Las liquidaciones son distintas
	if (tipo_dte1='43') then
		--xml_dte1:=encode('<DTE xmlns="http://www.sii.cl/SiiDte" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" version="1.0"><Liquidacion ','hex')||split_part(xml_dte1,encode('<Liquidacion ','hex'),2);
		xml_dte1:=encode('<DTE version="1.0"><Liquidacion ','hex')||split_part(xml_dte1,encode('<Liquidacion ','hex'),2);
	elsif (tipo_dte1 in ('110','111','112')) then
		--xml_dte1:=encode('<DTE xmlns="http://www.sii.cl/SiiDte" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" version="1.0"><Exportaciones ','hex')||split_part(xml_dte1,encode('<Exportaciones ','hex'),2);
		xml_dte1:=encode('<DTE version="1.0"><Exportaciones ','hex')||split_part(xml_dte1,encode('<Exportaciones ','hex'),2);
	else
		--FAY
		--xml_dte1:=encode('<DTE xmlns="http://www.sii.cl/SiiDte" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" version="1.0"><Documento ','hex')||split_part(xml_dte1,encode('<Documento ','hex'),2);
		xml_dte1:=encode('<DTE version="1.0"><Documento ','hex')||split_part(xml_dte1,encode('<Documento ','hex'),2);
	end if;
	--2017-0602 Cambiamos el <Signature> por <Signature xmlns="http://www.w3.org/2000/09/xmldsig#">
	xml_dte1:=replace(xml_dte1,encode('<Signature>','hex'),encode('<Signature xmlns="http://www.w3.org/2000/09/xmldsig#">','hex'));

	--perform logfile('RUTRecep='||get_xml_hex1('RUTRecep',xml_dte1));

        rut1:=split_part(get_xml_hex1('RUTRecep',xml_dte1),'-',1);
	if (is_number(rut1) is false) then
		json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
		json2:=logjson(json2,'Rut Receptor no es numerico, xml invalido '||rut1::varchar||' URI='||get_json('URI_IN',json2));
		return json2;
	end if;
	--Para la publicacion
	json2:=put_json(json2,'RUT_RECEPTOR',rut1);

        select * into campo from maestro_clientes where rut_emisor=rut1::integer;
	if not found then
		if(now()-get_json('FECHA_INGRESO_COLA',json2)::timestamp>interval '5 days') then
			json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
		else
			json2:=put_json(json2,'RESPUESTA','Status: 444 NK');
			json2:=put_json(json2,'MENSAJE_XML_FLAGS','Rut Receptor no se encuentra en maestro_clientes');
		end if;
		json2:=logjson(json2,'Rut Receptor no se encuentra en maestro_clientes '||rut1::varchar);
		return json2;
	end if;

        --json2:=put_json(json2,'RAZON_SOC_RECEPTOR_HEX',utf82latin1hex(campo.razon_social));
        json2:=put_json(json2,'RAZON_SOC_RECEPTOR_HEX',encode(escape_xml_characters_simple(campo.razon_social)::bytea,'hex'));
        json2:=put_json(json2,'RAZON_SOC_EMISOR_HEX',get_xml_hex(encode('RznSoc'::bytea,'hex'),xml_dte1));
	
	rut_emisor2:=get_xml_hex1('RUTEmisor',xml_dte1);
	rut_emisor1:=split_part(rut_emisor2,'-',1);
	tipo_dte1:=get_xml_hex1('TipoDTE',xml_dte1);
	folio1:=get_xml_hex1('Folio',xml_dte1);
	tms1:=get_xml_hex1('TmstFirma',xml_dte1);
	monto1:=get_xml_hex1('MntTotal',xml_dte1);
	fecha1:=get_xml_hex1('FchEmis',xml_dte1);
	
	if(is_number(rut_emisor1) is false or is_number(tipo_dte1) is false or is_number(folio1) is false) then
		json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
		json2:=logjson(json2,'Datos Inválidos, no numéricos...');
		return json2;
	end if;
	--Se normaliza el folio para q no queden ceros a la izquierda
	folio1:=folio1::bigint::varchar;

	--Solo aceptamos estos tipos de dete
    	select * into campo1 from tipo_dte where codigo=tipo_dte1::integer;
	if not found then
		json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
		json2:=logjson(json2,'Tipo Dte Invalido '||tipo_dte1);
		return json2;
	end if;
	


	BEGIN
		tms2:=tms1::timestamp;
		rut_receptor2:=split_part(get_xml_hex1('RUTRecep',xml_dte1),'-',1)::bigint;
	EXCEPTION WHEN OTHERS THEN
		json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
		json2:=logjson(json2,'Fecha Firma Inválida o RUTRecep invalido,se borra DTE');
		return json2;
	END;
	json2:=put_json(json2,'TmstFirma',tms1);
        json3:='{}';

	if (campo.dominio is null or campo.dominio='') then
                campo.dominio:='webdte';
	end if;
	
	--Reviso si el DTE ya esta en la tabla de pendientes
	select * into campo1 from dte_pendientes_recibidos where rut_emisor=rut_emisor2 and tipo_dte=tipo_dte1 and folio=folio1 and rut_receptor=rut_receptor2 and monto_total=monto1 and split_part(fecha_emision,' ',1)=fecha1 and coalesce(uri,'')<>'';
	if found then
		--Si se encuentra
		json2:=logjson(json2,'DTE ya registrado en dte_pendientes_recibidos, se usa esta URI='||campo1.uri);	
		uri1:=campo1.uri;
	else
		json2:=logjson(json2,'GENERO URI RUT_EMISOR='||rut_emisor1::varchar||' TIPO_DTE='||tipo_dte1::varchar||' FOLIO='||folio1::varchar||' FECHA_EMISION='||get_xml_hex1('FchEmis',xml_dte1)||' MONTO_TOTAL='||get_xml_hex1('MntTotal',xml_dte1));
        	uri1:='http://'||campo.dominio||to_char(now(),'YYMM')||'.acepta.com/v01/'||genera_uri2(rut_emisor1,tipo_dte1,folio1,get_xml_hex1('FchEmis',xml_dte1),get_xml_hex1('MntTotal',xml_dte1),'R');
		json2:=logjson(json2,'DTE no registrado en dte_pendientes_recibidos URI='||uri1);
	end if;
	
	json3:=put_json(json3,'DominioEmisor',campo.dominio);
	json3:=put_json(json3,'Dominio',campo.dominio);
	json2:=put_json(json2,'DOMINIO_EMISOR',campo.dominio);

	json2:=logjson(json2,'Ensobro DTE Recibido '||uri1||' URI_PY='||get_json('URIP',json2));

	certificado_x509:=replace(get_xml_hex1('X509Certificate',xml_dte1),' ','');
	--json2:=logjson(json2,'X509Certificate='||certificado_x509);
	aux:=verifica_certificado(certificado_x509);
	rut_firma1:=split_part(split_part(aux,'serialNumber=',2),'-',1);
	--Si no podemos sacar el rut correcto, ponemos el rut emisor
	if (is_number(rut_firma1) is false) then
		rut_firma1:=rut_emisor1;
	end if;

        json3:=put_json(json3,'RUTEmisor',get_xml_hex1('RUTEmisor',xml_dte1));
        json2:=put_json(json2,'RUT_EMISOR',split_part(get_xml_hex1('RUTEmisor',xml_dte1),'-',1));
        json2:=put_json(json2,'MONTO_TOTAL',get_xml_hex1('MntTotal',xml_dte1));

        json3:=put_json(json3,'RutFirma',rut_firma1||'-'||modulo11(rut_firma1));
        json3:=put_json(json3,'Folio',folio1);
	json2:=put_json(json2,'FOLIO',folio1);
	json2:=put_json(json2,'TIPO_DTE',tipo_dte1);
        json3:=put_json(json3,'FchEmis',get_xml_hex1('FchEmis',xml_dte1));
        json3:=put_json(json3,'SUCURSAL',get_xml_hex1('CdgSIISucur',xml_dte1));
        json3:=put_json(json3,'CdgSIISucur',get_xml_hex1('CdgSIISucur',xml_dte1));
        json3:=put_json(json3,'URI_IN',uri1);

	json3:=put_json(json3,'fechaVencimiento','');
	json3:=put_json(json3,'NombreEmisor','');
        
/*
	json3:=put_json(json3,'RUTRecep',get_xml_hex1('RUTRecep',xml_dte1));
        json3:=put_json(json3,'RznSocRecep',get_xml_hex1('RznSocRecep',xml_dte1));
        json3:=put_json(json3,'DirRecep',get_xml_hex1('DirRecep',xml_dte1));
        json3:=put_json(json3,'NombreEmisor',get_xml_hex1('RznSoc',xml_dte1));
*/

	--Buscamos si existe xsl, en caso de que el emisor sea cliente de acepta...
	/*
	select * into campo from cache_xsl_emisor where rut_emisor=rut_emisor1::bigint and tipo_dte=tipo_dte1::integer order by fecha_ingreso desc limit 1;
	if found then
		json3:=put_json(json3,'XSL',campo.xsl);
		--Si el XSL tiene dominio, usamos este para el dte
		if (get_dominio_uri(campo.xsl)<>'') then
			json3:=put_json(json3,'Dominio',get_dominio_uri(campo.xsl));
		end if;
	else
		json3:=put_json(json3,'XSL','http://www.custodium.com/docs/otros/dte/dte.xsl');
	end if;
	*/
	json3:=put_json(json3,'XSL','http://www.custodium.com/docs/otros/dte/dte.xsl');

        patron_dte1:=pg_read_file('./patron_dte_10k/patron_dte_sin_custodium_rec.xml');
        if (patron_dte1='' or patron_dte1 is null) then
		json2:=put_json(json2,'RESPUESTA','Status: 400 NK');
		json2:=logjson(json2,'Falla Insercion no existe patron de DTE');
		return json2;
        end if;
        json3:=escape_xml_characters(json3::varchar)::json;
        dte1:=remplaza_tags_json_c(json3,patron_dte1);
--      dte1:=limpia_tags(dte1);
	
	dte1:=encode(dte1::bytea,'hex');
	dte1:=replace(dte1,'2424245255545265636570242424',get_xml_hex('RUTRecep',xml_dte1));	
	dte1:=replace(dte1,'242424527a6e536f635265636570242424',get_xml_hex('RznSocRecep',xml_dte1));	
	dte1:=replace(dte1,'2424244469725265636570242424',get_xml_hex('DirRecep',xml_dte1));	
	dte1:=replace(dte1,'242424527a6e536f63242424',get_xml_hex('RznSoc',xml_dte1));	

        --xml_dte1:=replace(encode(dte1::bytea,'hex'),encode('#|#|#|#DTE#|#|#|#','hex'),xml_dte1);
        xml_dte1:=replace(dte1,encode('#|#|#|#DTE#|#|#|#','hex'),xml_dte1);

	json2:=put_json(json2,'INPUT',xml_dte1);
	json2:=put_json(json2,'CONTENT_LENGTH',(length(xml_dte1)/2)::varchar);
	json2:=put_json(json2,'URI_IN',uri1);
	json2:=put_json(json2,'__SECUENCIAOK__','40');
	return json2;

END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION public.genera_crt_8030(json) RETURNS json AS $$
DECLARE
        json1                   alias for $1;
        json2                   json;
        json3                   json;
        patron_dte1             varchar;
        xml_dte1                varchar;
        dte1                    varchar;
        campo                   record;
        campo1                  record;

        razonReceptor           varchar;
        razonEmisor             varchar;

        RecepEnvGlosa           varchar;
        estadoRecepcionEnvio    varchar;
        detallesEnvio           json;

        i                       integer;
        j                       integer;
        aux                     varchar;
        aux2                    json;
        dte                     varchar;
        detalleDte              json;
	rut_emisor1		varchar;
	rut_receptor1		varchar;
	detalle1		varchar;
	tms1			varchar;
	rut_firma1		varchar;
	pass1			varchar;
	id1			varchar;
	data_firma1		varchar;
	folio1			varchar;
	tipo_dte1		varchar;
	uri1		varchar;
	html1		varchar;
	json_aux1	json;
BEGIN
        json2:=json1;

	json2:=logjson(json2,'Se genera CRT');
	rut_emisor1:=get_json('RUT_EMISOR',json2);
	rut_receptor1:=get_json('RUT_RECEPTOR',json2);
	json3:='{}';
        json3:=put_json(json3,'RUTEmisor',rut_emisor1||'-'||modulo11(rut_emisor1));
        --Pablo Izquierdo
	rut_firma1:='5544700-4';
	pass1:='fkrran70aawwpq';
        json3:=put_json(json3,'RutFirma',rut_firma1);
        --Caratula
        json3:=put_json(json3,'RutResponde',rut_receptor1||'-'||modulo11(rut_receptor1));
        json3:=put_json(json3,'RutRecibe',rut_emisor1||'-'||modulo11(rut_emisor1));

	--secuencia (numero de identificacion de respuesta generado por quien responde)
        json3:=put_json(json3,'IdRespuesta',get_json('CODIGO_TXEL',json2));
        json3:=put_json(json3,'NroDetalles','1');
	tms1:=to_char(now(),'YYYY-MM-DD')||'T'||to_char(now(),'HH24:MM:SS');
        -- Fecha de la firma
        json3:=put_json(json3,'TmstFirmaResp',tms1);
        --RecepcionEnvio
        json3:=put_json(json3,'NmbEnvio',decode_latin1(get_json('NmbEnvio',json2)));
        json3:=put_json(json3,'FchRecep',tms1);
        json3:=put_json(json3,'FchRecepDis',to_char(now(),'DD/MM/YYYY'));
        -- secuencia (Codigo unico de envio generado por el receptor)
        json3:=put_json(json3,'CodEnvio',get_json('CODIGO_TXEL',json2));
        json3:=put_json(json3,'EnvioDTEID',decode_latin1(split_part(get_json('NmbEnvio',json2),'.',1)));
        json3:=put_json(json3,'Digest',get_json('DigestSobre',json2));
        json3:=put_json(json3,'RutEmisor',rut_emisor1||'-'||modulo11(rut_emisor1));
        json3:=put_json(json3,'RutReceptor',rut_receptor1||'-'||modulo11(rut_receptor1));


        json3:=put_json(json3,'NroDTE','1');
	


	if (get_json('FLAG_DTE_RECIBIDO_REPETIDO',json2)='SI') then
		json2:=logjson(json2,'FLAG_DTE_RECIBIDO_REPETIDO URI_DTE_REPETIDO='||get_json('URI_DTE_REPETIDO',json2));
		--Si esta repetido, verificamos que realmente este el PRE en la traza
		--Del DTE encontrado en recibidos
		json_aux1:=lee_traza_evento(get_json('URI_DTE_REPETIDO',json2),'PRE');
		--Si ya se envio con dTe original, entonces es un repetido
		if (strpos(get_json('comentario1',json_aux1),'DTE Recibido (0)')>0) then
			json2:=logjson(json2,'PRE ya enviado');
			--Si ya envie 5 veces, no lo hago mas
			if (get_json('veces',json_aux1)::integer>5) then
				json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
				json2:=logjson(json2,'PRE enviado muchas veces, se ignora el DTE');
				json2:=put_json(json2,'__SECUENCIAOK__','1000');
				return json2;
			end if;
			estadoRecepcionEnvio:='4';
			RecepEnvGlosa:='DTE No Recibido - DTE Repetido';
			json3:=put_json(json3,'EstadoRecepEnvM','RECHAZADO');
			json3:=put_json(json3,'RecepEnvGlosaM','EnvioDTE Recibido');
			json3:=put_json(json3,'DetalleGlosa','DTE No Recibido - DTE Repetido');
			json2:=logjson(json2,'Se envia CRT Repetido');
		else
			--No es un repetido, si el DTe original no tiene PRE Recibido
			estadoRecepcionEnvio:='0';
			RecepEnvGlosa:='Envio Recibido Conforme';
			json3:=put_json(json3,'EstadoRecepEnvM','RECIBIDO');
			json3:=put_json(json3,'RecepEnvGlosaM','EnvioDTE Recibido');
			json3:=put_json(json3,'DetalleGlosa','DTE Recibido');
			--Desmarcamos el FLAG
			json2:=put_json(json2,'FLAG_DTE_RECIBIDO_REPETIDO','');
			json2:=logjson(json2,'PRE no enviado, se envia OK');
		end if;
        else
		estadoRecepcionEnvio:='0';
		RecepEnvGlosa:='Envio Recibido Conforme';
		json3:=put_json(json3,'EstadoRecepEnvM','RECIBIDO');
		json3:=put_json(json3,'RecepEnvGlosaM','EnvioDTE Recibido');
		json3:=put_json(json3,'DetalleGlosa','DTE Recibido');
		json2:=logjson(json2,'PRE no enviado, se envia OK.');
	end if;
        json3:=put_json(json3,'EstadoRecepEnv',estadoRecepcionEnvio);
        json3:=put_json(json3,'RecepEnvGlosa',RecepEnvGlosa);
	
	folio1:=get_json('FOLIO',json2);
	tipo_dte1:=get_json('TIPO_DTE',json2);
        json3:=put_json(json3,'TipoDte',tipo_dte1);
        json3:=put_json(json3,'Folio',folio1);
        json3:=put_json(json3,'DominioEmisor',get_json('DOMINIO_EMISOR',json2));
	json3:=put_json(json3,'FechaEmision',to_char(to_timestamp(get_json('FECHA_EMISION',json2),'YYYY-MM-DD'),'DD/MM/YYYY'));
	--FAY-DAO 2018-03-01 El monto viene con caracteres especiales (salto de libeas), para que no se caiga
	begin
		json3:=put_json(json3,'MontoTotal',edita_monto(get_json('MONTO_TOTAL',json2)));
	exception when others then
		json3:=put_json(json3,'MontoTotal',get_json('MONTO_TOTAL',json2));
	end;
        
	uri1:='http://'||get_json('DOMINIO_EMISOR',json2)||to_char(now(),'YYMM')||'.acepta.com/v01/'||genera_uri(rut_emisor1,tipo_dte1,'CRT_'||folio1,tms1,'R');
	json2:=logjson(json2,'URI_CRT='||uri1);
	json3:=put_json(json3,'custodium-uri',uri1);
	json2:=put_json(json2,'URI_CRT',uri1);

        patron_dte1:=pg_read_file('./patron_dte_10k/patron_crt.xml');
        if (patron_dte1='' or patron_dte1 is null) then
		json2:=put_json(json2,'RESPUESTA','Status: 400 NK');
		json2:=logjson(json2,'Falla Insercion no existe patron de DTE');
		return json2;
        end if;
        json3:=escape_xml_characters(json3::varchar)::json;
	detalle1:='<RecepcionDTE><TipoDTE>'||get_json('TIPO_DTE',json2)||'</TipoDTE><Folio>'||get_json('FOLIO',json2)||'</Folio><FchEmis>'||get_json('FECHA_EMISION',json2)||'</FchEmis><RUTEmisor>'||rut_emisor1||'-'||modulo11(rut_emisor1)||'</RUTEmisor><RUTRecep>'||rut_receptor1||'-'||modulo11(rut_receptor1)||'</RUTRecep><MntTotal>'||get_json('MONTO_TOTAL',json2)||'</MntTotal><EstadoRecepDTE>'||estadoRecepcionEnvio||'</EstadoRecepDTE><RecepDTEGlosa>'||RecepEnvGlosa||'</RecepDTEGlosa></RecepcionDTE>';
	json3:=put_json(json3,'Detalle_RecepcionDTE',detalle1);

	id1:='RespuestaDTE-'||rut_receptor1||'-'||modulo11(rut_receptor1)||'-'||rut_emisor1||'-'||modulo11(rut_emisor1)||'-'||to_char(now(),'YYYYMMDD')||'-'||get_json('IdRespuesta',json3);
	json3:=put_json(json3,'NODO_ID',id1);
	json2:=put_json(json2,'ID_MAIL',id1);
	--Patron CRT
        dte1:=remplaza_tags_json_c(json3,patron_dte1);
	dte1:=encode(dte1::bytea,'hex');
	--RznSocResponde
	dte1:=replace(dte1,'242424527a6e536f63526573706f6e6465242424',get_json('RAZON_SOC_RECEPTOR_HEX',json2));
	--RznSocRecibe
	dte1:=replace(dte1,'242424527a6e536f63526563696265242424',get_json('RAZON_SOC_EMISOR_HEX',json2));
	--json3:=put_json(json3,'RznSocResponde',get_json('RAZON_SOC_RECEPTOR',json2));
        --json3:=put_json(json3,'RznSocRecibe',get_json('RAZON_SOC_EMISOR',json2));
        --dte1:=limpia_tags(dte1);

	--Genero Html para el mail
        patron_dte1:=pg_read_file('./patron_dte_10k/patron_mail_acuse_recibo_inter.html');
        if (patron_dte1='' or patron_dte1 is null) then
		json2:=put_json(json2,'RESPUESTA','Status: 400 NK');
		json2:=logjson(json2,'Falla Insercion no existe patron de mail de acuse DTE');
		return json2;
        end if;
	html1:=remplaza_tags_json_c(json3,patron_dte1);
	html1:=encode(html1::bytea,'hex');
	html1:=replace(html1,'242424527a6e536f63526573706f6e6465242424',get_json('RAZON_SOC_RECEPTOR_HEX',json2));
	html1:=replace(html1,'242424527a6e536f63526563696265242424',get_json('RAZON_SOC_EMISOR_HEX',json2));
	--html1:=limpia_tags(html1);
	--json2:=put_json(json2,'html_mail_crt',encode(html1::bytea,'hex')::varchar);
	json2:=put_json(json2,'html_mail_crt',html1);
        
	--data_firma1:=replace('{"documento":"'||str2latin12base64(dte1)||'","nodoId":"'||id1||'","rutEmpresa":"'||rut_firma1||'","entidad":"SII","rutFirmante":"'||rut_firma1||'","codigoAcceso":"'||replace(pass1,chr(92),chr(92)||chr(92))||'"}',chr(10),'');
	data_firma1:=replace('{"documento":"'||hex2ascii2base64(dte1)||'","nodoId":"'||id1||'","rutEmpresa":"'||rut_firma1||'","entidad":"SII","rutFirmante":"'||rut_firma1||'","codigoAcceso":"'||replace(pass1,chr(92),chr(92)||chr(92))||'"}',chr(10),'');
	json2:=logjson(json2,'data_firma1='||data_firma1);
	
	json2:=get_parametros_motor_json(json2,'FIRMADOR_OFFLINE');
	json2:=put_json(json2,'IP_PORT_CLIENTE',get_json('__IP_PORT_CLIENTE__',json2));
	json2:=put_json(json2,'IP_CONEXION_CLIENTE',get_json('__IP_CONEXION_CLIENTE__',json2));
	--Para el reproceso
	json2:=put_json(json2,'IPPORT_SII',get_json('__IP_CONEXION_CLIENTE__',json2)||':'||get_json('__IP_PORT_CLIENTE__',json2));
        json2:=put_json(json2,'INPUT_FIRMADOR','POST '||get_json_upper('PARAMETRO_RUTA',json2)||' HTTP/1.1'||chr(10)||'User-Agent: curl/7.26.0'||chr(10)||'Host: '||get_json_upper('__IP_CONEXION_CLIENTE__',json2)||chr(10)||'Accept: '||chr(42)||'/'||chr(42)||chr(10)||'Content-Type: application/json; charset=ISO-8859-1'||chr(10)||'Content-Length: '||length(data_firma1)::varchar||chr(10)||chr(10)||data_firma1);

	--json2:=logjson(json2,'CRT:'||data_firma1);

	json2:=put_json(json2,'__SECUENCIAOK__','70');

	return json2;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION public.verifica_firma_crt_8030(json) RETURNS json AS $$
DECLARE
        json1                   alias for $1;
        json2                   json;
        json3                   json;
	resp1	varchar;
	json_resp1	varchar;	
	aux1	varchar;
	data1	varchar;
        campo           record;
BEGIN
	json2:=json1;
	json2 :=put_json(json2,'__SECUENCIAOK__','1000');
	json2:=respuesta_no_chunked_json(json2);
        resp1:=decode(get_json_upper('RESPUESTA_HEX',json2),'hex');
        json_resp1:=split_part(resp1,'\012\012',2);
        json2:=put_json(json2,'RESPUESTA_HEX','');

	json2:=logjson(json2,get_json('INPUT_FIRMADOR',json2));

       	if (strpos(resp1,'HTTP/1.1 200 ')>0) then
                aux1:=get_json('documentoFirmado',json_resp1::json);
                if (length(aux1)>0) then
        		json2:=put_json(json2,'INPUT_FIRMADOR','');
			json2:=logjson(json2,'CRT Firmado OK');	
		        data1:=base642hex(aux1);
                        json2:=put_json(json2,'INPUT',data1);
                        json2:=put_json(json2,'CONTENT_LENGTH',(length(data1)/2)::varchar);
                        --Se procesa por el 8010
                        json2:=put_json(json2,'SCRIPT_NAME','/ca4/ca4dte');
                        json2:=put_json(json2,'__SECUENCIAOK__','90');
                        json2:=put_json(json2,'RESPUESTA','');
			--Guardamos la URI del dte
			json2:=logjson(json2,'URI_IN='||get_json('URI_IN',json2));
			json2:=logjson(json2,'URI_CRT='||get_json('URI_CRT',json2));

			json2:=put_json(json2,'URI_REC',get_json('URI_IN',json2));
			json2:=put_json(json2,'URI_IN',get_json('URI_CRT',json2));

			json2:=logjson(json2,'URI_IN='||get_json('URI_IN',json2));
			json2:=logjson(json2,'URI_CRT='||get_json('URI_CRT',json2));
			--Limpiamos el publicado ok
			json2:=put_json(json2,'__PUBLICADO_OK__','');
			json2:=put_json(json2,'__FLAG_CLIENTE_COMUNIDAD__','');
			return json2;
		else
			json2:=logjson(json2,'Respuesta Firmador '||json_resp1::varchar);
			json2:=logjson(json2,'Servicio de Firma no responde documento Firmado');
			json2 := put_json(json2,'RESPUESTA','Status: 400 NK');
			--Vuelvo a cambiar el 0x06 por ; que el motor remplaza por seguridad
			json2 := put_json(json2,'INPUT_FIRMADOR',replace(get_json('INPUT_FIRMADOR',json2),chr(6),';'));
			json2:=put_json(json2,'MENSAJE_XML_FLAGS',genera_mensaje_flags_8030('70',json2::varchar,'JSON'));
			return json2;
		end if;
	else
		json2:=logjson(json2,'Respuesta Firmador '||json_resp1::varchar);
		json2:=logjson(json2,'Servicio de Firma Falla');
	 	json2:= put_json(json2,'RESPUESTA','Status: 400 NK');
		--Vuelvo a cambiar el 0x06 por ; que el motor remplaza por seguridad
		json2 := put_json(json2,'INPUT_FIRMADOR',replace(get_json('INPUT_FIRMADOR',json2),chr(6),';'));
		json2:=put_json(json2,'MENSAJE_XML_FLAGS',genera_mensaje_flags_8030('70',json2::varchar,'JSON'));
		return json2;
	end if;
        return json2;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION public.valida_pub_crt_8030(varchar) RETURNS varchar AS $$
DECLARE
        xml1                   alias for $1;
        xml2                   varchar;
BEGIN
	xml2:=xml1;
	if (get_campo('__PUBLICADO_OK__',xml2)<>'SI') then
		xml2:=logapp(xml2,'Falla la Publicacion en Almacen del CRT '||get_campo('URI_IN',xml2)||' '||get_campo('URI_CRT',xml2));
		xml2 := put_campo(xml2,'RESPUESTA','Status: 400 NK');
		xml2 := put_campo(xml2,'__SECUENCIAOK__','1000');
		return xml2;
	end if;
	--Vamos 
	xml2 := put_campo(xml2,'__SECUENCIAOK__','110');

	--Vamos a enviar 
        return xml2;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.graba_evento_crt_8030(varchar) RETURNS varchar AS $$
DECLARE
        xml1                   alias for $1;
        xml2                   varchar;
	rut1	varchar;
	aux1	varchar;
	lista1	json;
	i	integer;
	file1	varchar;
	j3	json;
	cod1	bigint;
BEGIN
        xml2:=xml1;
	xml2 := put_campo(xml2,'__SECUENCIAOK__','1000');
	--RME Agrega Canal y Rut Owner.
	xml2 := put_campo(xml2,'CANAL','RECIBIDOS');
	xml2 := put_campo(xml2,'RUT_OWNER',get_campo('RUT_RECEPTOR',xml2));
	xml2:=put_campo(xml2,'URI_IN',get_campo('URI_REC',xml2));
	xml2:=put_campo(xml2,'URL_GET',get_campo('URI_CRT',xml2));
	aux1:=get_campo('MAIL_EMISOR',xml2);
	if (get_campo('FLAG_DTE_RECIBIDO_REPETIDO',xml2)='SI') then
		rut1:=get_campo('RUT_EMISOR',xml2);
		aux1:=(select email from contribuyentes where rut_emisor=rut1::integer);
		xml2:=put_campo(xml2,'COMENTARIO_TRAZA','Recibe: '||coalesce(aux1,'-')||chr(10)||'Glosa: DTE Repetido (4)');
		xml2:=logapp(xml2,'URI_REC='||get_campo('URI_REC',xml2)||' URI_DTE_REPETIDO='||get_campo('URI_DTE_REPETIDO',xml2));
		--Si tienen uris distintas los relaciono
		if (get_campo('URI_REC',xml2)<>get_campo('URI_DTE_REPETIDO',xml2)) then
			--Relaciono el DTE repetido con el encontrado en dte_recibidos
			xml2:=logapp(xml2,'Se relaciona URI='||get_campo('URI_REC',xml2)||' URI_DTE_REPETIDO='||get_campo('URI_DTE_REPETIDO',xml2));
			aux1:=graba_documentos_relacionados(get_campo('URI_REC',xml2),get_campo('TIPO_DTE',xml2),get_campo('FOLIO',xml2),get_campo('URI_DTE_REPETIDO',xml2),get_campo('TIPO_DTE',xml2),get_campo('FOLIO',xml2));
		end if;
	else
		xml2:=put_campo(xml2,'COMENTARIO_TRAZA','Recibe: '||coalesce(aux1,'-')||chr(10)||'Glosa: DTE Recibido (0)');
	end if;
        xml2:=put_campo(xml2,'EVENTO','PRE');
        xml2:=graba_bitacora(xml2,'PRE');

	--Grabamos los archivos adjuntos en la traza si existen
	i:=0;
	xml2:=logapp(xml2,'Adjuntos '||get_campo('adjuntos',xml2));
	if(is_json_list(get_campo('adjuntos',xml2))) then
		lista1:=get_campo('adjuntos',xml2)::json;
		aux1:=get_json_index(lista1,i);
		--Marcamos el DTE que tiene Adj
		if aux1<>'' then
			cod1:=get_campo('CODIGO_TXEL',xml2);
			xml2:=logapp(xml2,'Marco dte_recibidos '||cod1::varchar);
			update dte_recibidos set data_dte=coalesce(data_dte,'')||'<ADJ>SI</ADJ>' where codigo_txel=cod1::bigint;
		end if;

		xml2:=logapp(xml2,'Entra a grabar ADJ');
		while (aux1<>'') loop
			xml2:=logapp(xml2,'Graba ADJ '||aux1);
			--Grabo Evento con Documento Adjunto
			j3:=aux1::json;	
			xml2:=put_campo(xml2,'COMENTARIO_TRAZA','PDF Adjunto en Sobre de Envío ('||get_json('nombre',j3)||')');
			xml2:=put_campo(xml2,'URL_GET',split_part(get_json('uri',j3),'.gz',1));
			xml2:=put_campo(xml2,'COMENTARIO2','PDF Adjunto');
			xml2:=graba_bitacora(xml2,'ADJ');	
			i:=i+1;
			aux1:=get_json_index(lista1,i);
		end loop;
	else
		xml2:=logapp(xml2,'Sin Adjuntos '||get_campo('adjuntos',xml2));
	end if;

        xml2 := put_campo(xml2,'RESPUESTA','Status: 200 OK');
        return xml2;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.send_mail_crt_8030(json) RETURNS json AS $$
DECLARE
        json1                   alias for $1;
        json2        json;
	json4	json;

	jsonsts1	json;
	input1	varchar;
	aux1	varchar;
	rut1	varchar;
	sts1	varchar;
BEGIN
	json2:=json1;
	json2:=put_json(json2,'__SECUENCIAOK__','1000');
	input1:=get_json('INPUT',json2);

	rut1:=get_json('RUT_RECEPTOR',json2);
	aux1:=(select email from contribuyentes where rut_emisor=rut1::integer);
	json2:=put_json(json2,'MAIL_RECEPTOR',aux1);
        json4:='{}';
        json4:=put_json(json4,'uri',get_json('URI_IN',json2));
	--perform logfile('INPUT_CUSTODIUM ANTES='||input1);
        json4:=put_json(json4,'INPUT_CUSTODIUM',encode('<RespuestaDTE','hex')||split_part(split_part(input1,encode('<RespuestaDTE','hex'),2),encode('</RespuestaDTE>','hex'),1)||encode('</RespuestaDTE>','hex'));
	json4:=put_json(json4,'flag_data_xml','SI'); --Para que no saque el contenido de la URI
	--perform logfile('INPUT_CUSTODIUM='||get_json('INPUT_CUSTODIUM',json4));
        json4:=put_json(json4,'subject_hex',encode(('Acuse de Recibo de DTEs -'||chr(10)||get_json('ID_MAIL',json2))::bytea,'hex'));
        json4:=put_json(json4,'from_hex',get_json('RAZON_SOC_RECEPTOR_HEX',json2)||encode((' <'||aux1||'>')::bytea,'hex')::varchar);
        json4:=put_json(json4,'to',trim(get_json('MAIL_EMISOR',json2)));
        --json4:=put_json(json4,'to','fernando.arancibia@acepta.com');
        json4:=put_json(json4,'tipo_envio','HTML');
	json4:=put_json(json4,'content_html',get_json('html_mail_crt',json2));
	json2:=put_json(json2,'html_mail_crt','');
        --Buscamos el xsl que le corresponde
        json4:=put_json(json4,'file_xsl','/opt/acepta/motor/xsl/CRT/comprobanterecepcion.xsl');
        --json4:=put_json(json4,'ip_envio','172.16.14.82');
	json4:=put_json(json4,'adjunta_xml','SI');
	json4:=put_json(json4,'nombre_xml','crt_'||get_json('FOLIO',json2)||'_'||get_json('TIPO_DTE',json2));
	--perform logfile('F_8030 select send_mail_python2('''||json4::varchar||''')');
	--raise notice 'xml=%',get_json('INPUT_CUSTODIUM',json4);

	json4:=put_json(json4,'CATEGORIA','CRT');
        json4:=put_json(json4,'RUT_OWNER',rut1::varchar);
       	json4:=put_json(json4,'ip_envio','http://interno.acepta.com:8080/sendmail');
        jsonsts1:=send_mail_python2_colas(json4::varchar);
	--jsonsts1:=send_mail_python2(json4::varchar);
        if (get_json('status',jsonsts1)='OK') then
		json2:=logjson(json2,'Envio CRT Exitoso retorno='||get_json('retorno_send_mail',jsonsts1)||' Confirma='||get_json('confirmacion',jsonsts1)||' msg-id='||get_json('msg-id',jsonsts1));

		json2:=put_json(json2,'INPUT','');
		json2:=put_json(json2,'__SECUENCIAOK__','120');
		if (get_json('TIPO_DTE',json2) in ('33','34','43')) then
			--si envio correctamente el CRT, grabo en las colas para buscar la fecha de recepcion real del sii
			sts1:=insert_cola_fecha_rec_sii_16103(get_json('CODIGO_TXEL',json2),get_json('RUT_EMISOR',json2),get_json('TIPO_DTE',json2),get_json('FOLIO',json2),get_json('RUT_RECEPTOR',json2),get_json('URI_REC',json2));
			json2:=logjson(json2,'Se graba Busqueda de fecha de recepcion '||sts1);
		end if;
	else
		json2:=logjson(json2,'Envio CRT Fallido ');
		json2:= put_json(json2,'RESPUESTA','Status: 400 NK');
	end if;

	return json2;
END;
$$ LANGUAGE plpgsql;

