delete from isys_querys_tx where llave='8010';

insert into isys_querys_tx values ('8010',5,1,14,'{"f":"INSERTA_JSON","p1":{"GRABA_PUB_8010":"SI","__SECUENCIAOK__":"10","__SOCKET_RESPONSE__":"RESPUESTA","__TIPO_SOCKET_RESPONSE__":"SCGI","RESPUESTA":"Status: 555 OK\nContent-Type: text/plain\n\n{\"STATUS\":\"Responde sin Espera\",\"__PROC_ACTIVOS__\":\"$$__PROC_ACTIVOS__$$\"}"}}',0,0,0,0,0,10,10);
--insert into isys_querys_tx values ('8010',5,19,1,'select control_flujo_80101(''$$__JSONCOMPLETO__["__PROC_ACTIVOS__","TX","REQUEST_URI","__ARGV__","__CATEGORIA_COLA__","__FLUJO_ACTUAL__"]$$''::json) as __json__',0,0,0,1,1,-1,10);

--Primero que hacemos el publicar DTE
insert into isys_querys_tx values ('8010',10,1,8,'Publica DTE',112704,0,0,0,0,20,20);

-- Prepara llamada al AML
--20201027 Se cambia la secuencia de error a las 1005 para que si falla no envie el mandato
--XX insert into isys_querys_tx values ('8010',20,45,1,'select proc_procesa_input_dte_8010(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,1005);
insert into isys_querys_tx values ('8010',20,8021,1,'select proc_procesa_input_dte_8010(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,1005);

--Reglas de Validacion Base ACM
insert into isys_querys_tx values ('8010',600,7,1,'select reglas.validacion_lista(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
--Reglas de BASE_1_LOCAL
insert into isys_querys_tx values ('8010',610,8021,1,'select reglas.validacion_lista(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

--20201027 Se cambia la secuencia de error a las 1005 para que si falla no envie el mandato
--XX insert into isys_querys_tx values ('8010',28,1,1,'select proc_procesa_input_dte_8010_parte2(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,1005);
insert into isys_querys_tx values ('8010',28,8021,1,'select proc_procesa_input_dte_8010_parte2(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,1005);

--Borra o Actualiza el contenido de la cola API MOTOR
--XX insert into isys_querys_tx values ('8010',1000,45,1,'select sp_procesa_respuesta_cola_motor_original(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,0,0);
insert into isys_querys_tx values ('8010',1000,8021,1,'select sp_procesa_respuesta_cola_motor_original(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,0,0);
--20201027 Secuencia para aumentar reintentos en caso de error
insert into isys_querys_tx values ('8010',1005,19,1,'select sp_procesa_respuesta_cola_motor(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,0,0);
--Ejecuta la respuesta para borrar y envia el mandato en el caso de las boletas
insert into isys_querys_tx values ('8010',1010,19,1,'select proc_verifica_fin_dte88_8010(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,0,0);

--Llamada al Flujo de Publicacion
insert into isys_querys_tx values ('8010',100,1,8,'Llamada Publica DTE',112704,0,0,0,0,1000,1000);

--Llamada al Flujo de Escritura Directa al EDTE para Mandatos
insert into isys_querys_tx values ('8010',110,1,8,'Llamada Publica Mandato  EDTE',12771,0,0,0,0,120,120);
--Llamada a Grabar en Respaldo NINA
--insert into isys_querys_tx values ('8010',115,1,8,'Llamada Publica Respaldo',12713,0,0,0,0,120,120);
--Llamada al Flujo de Escritura Directa al EDTE
insert into isys_querys_tx values ('8010',120,1,8,'Llamada Publica EDTE',12702,0,0,0,0,40,40);

--Para envio de mandatos de boletas
insert into isys_querys_tx values ('8010',1600,1,8,'Llamada Publica Mandato  EDTE',12771,0,0,0,0,40,40);
--Envia Boletas con mandato
insert into isys_querys_tx values ('8010',1610,1,8,'Llamada Publica Mandato  EDTE',12771,0,0,0,0,1010,1010);

--EnvioBoletaSII-
insert into isys_querys_tx values ('8010',1620,19,1,'select encola_envio_boleta_sii_8010(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,1010);
insert into isys_querys_tx values ('8010',1630,1,2,'Llamada MS EnvioBoletaSII',4013,300,101,0,0,1631,1631);
--XX insert into isys_querys_tx values ('8010',1631,1,1,'select valida_respuesta_envio_boleta_sii_8010(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,1632);
insert into isys_querys_tx values ('8010',1631,8021,1,'select valida_respuesta_envio_boleta_sii_8010(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,1632);
insert into isys_querys_tx values ('8010',1632,19,1,'select valida_respuesta_envio_boleta_sii_error_8010(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,1010);

--Llamada al AML por el Apache
--insert into isys_querys_tx values ('8010',30,1,2,'Llamada al AML',4001,100,101,0,0,40,40);
--Llamada al AML directo SGCI
insert into isys_querys_tx values ('8010',30,1,2,'Llamada directo al AML',14000,102,101,0,0,40,40);
insert into isys_querys_tx values ('8010',31,1,2,'Llamada directo al AML',14001,102,101,0,0,40,40);
insert into isys_querys_tx values ('8010',32,1,2,'Llamada directo al AML',14002,102,101,0,0,40,40);
insert into isys_querys_tx values ('8010',33,1,2,'Llamada directo al AML',14003,102,101,0,0,40,40);
insert into isys_querys_tx values ('8010',34,1,2,'Llamada directo al AML',14004,102,101,0,0,40,40);
insert into isys_querys_tx values ('8010',35,1,2,'Llamada directo al AML',14005,102,101,0,0,40,40);
insert into isys_querys_tx values ('8010',36,1,2,'Llamada directo al AML',14006,102,101,0,0,40,40);
insert into isys_querys_tx values ('8010',37,1,2,'Llamada directo al AML',14007,102,101,0,0,40,40);
insert into isys_querys_tx values ('8010',38,1,2,'Llamada directo al AML',14008,102,101,0,0,40,40);
--AML CGE
insert into isys_querys_tx values ('8010',39,1,2,'Llamada directo al AML CGE',14009,102,101,0,0,40,40);
--Respuesta del AML
--insert into isys_querys_tx values ('8010',40,45,1,'select proc_procesa_respuesta_dte_8010(''$$__XMLCOMPLETO__$$'') as __xml__ ',0,0,0,1,1,-1,0);
--DAO 20210225 insert into isys_querys_tx values ('8010',40,8021,1,'select proc_procesa_respuesta_dte_8010(''$$__XMLCOMPLETO__$$'') as __xml__ ',0,0,0,1,1,-1,0);
--Vamos a la base de las colas
insert into isys_querys_tx values ('8010',40,19,1,'select proc_procesa_respuesta_dte_8010_colas(''$$__XMLCOMPLETO__$$'') as __xml__ ',0,0,0,1,1,-1,0);

--Flujo de Mandato de Boletas y DTE
--Va directo al 1000 para verificar si le fue bien en el flujo y se borra de la cola
insert into isys_querys_tx values ('8010',150,1,8,'Flujo Mandato Boletas y DTEs',12712,0,0,0,0,1000,1000);

--Flujo de Mensajes Basura con y sin URI
insert into isys_querys_tx values ('8010',500,1,8,'Flujo 9999 Basura',9999,0,0,1,1,1010,1010);
insert into isys_querys_tx values ('8010',510,1,8,'Flujo 9999 Basura',9999,0,0,1,1,1010,1010);


--Lee tabla webdte.boletas de base 
--insert into isys_querys_tx values ('8010',200,3,1,'select * from webdte.boletas where rut=split_part(''$$RUT_EMISOR$$'',''-'',1) and periodo=(substring(''$$FECHA_EMISION$$'',1,4)||substring(''$$FECHA_EMISION$$'',6,2))::numeric order by periodo asc',0,0,0,1,1,250,250);

--Acumula Boletas webiecv
insert into isys_querys_tx values ('8010',200,3,1,'select proc_graba_webiecv_boletas_8010(''$$__XMLCOMPLETO__$$'') as respuesta_boleta',0,0,0,1,1,210,210);
--insert into isys_querys_tx values ('8010',210,1,1,'select proc_procesa_respuesta_boleta_8010(''$$__XMLCOMPLETO__$$'') as __xml__ ',0,0,0,1,1,-1,0);


--Flujo de validacion de certificados X509
insert into isys_querys_tx values ('8010',250,1,8,'Flujo Validacion Certificado x509',12729,0,0,1,1,0,0);

--Flujo de los CA4ARM
insert into isys_querys_tx values ('8010',1500,1,8,'Flujo ARM',12718,0,0,1,1,0,0);
--insert into isys_querys_tx values ('8010',1501,1,8,'Flujo ARM',112718,0,0,1,1,0,0);
--Flujo de los CA4RESP
--insert into isys_querys_tx values ('8010',1510,1,8,'Flujo RESP',12727,0,0,1,1,0,0);
insert into isys_querys_tx values ('8010',1510,1,8,'Flujo RESP',12779,0,0,1,1,0,0);
--insert into isys_querys_tx values ('8010',1511,1,8,'Flujo RESP',112779,0,0,1,1,0,0);
--Flujo de los CA4AEC
--insert into isys_querys_tx values ('8010',1520,1,8,'Flujo AEC',12728,0,0,1,1,0,0);
insert into isys_querys_tx values ('8010',1520,1,8,'Flujo AEC',12786,0,0,1,1,0,0);
--Flujo de los CA4REC Recibidos
insert into isys_querys_tx values ('8010',1530,1,8,'Flujo Recibidos',12703,0,0,1,1,0,0);

--Flujo de los CA4SAR Reclamos Recibidos
insert into isys_querys_tx values ('8010',1540,1,8,'Flujo Reclamos Recibidos',16200,0,0,1,1,1010,1010);

--Flujo de los CA4LIB, igual que el de WEBIECV
insert into isys_querys_tx values ('8010',700,1,8,'Flujo CA4LIB',12788,0,0,1,1,0,0);
--Flujo de los CA4RCF, igual que el de WEBIECV
insert into isys_querys_tx values ('8010',710,1,8,'Flujo CA4RCF',12802,0,0,1,1,0,0);

CREATE or replace FUNCTION encola_envio_boleta_sii_8010(varchar) RETURNS varchar AS $$
DECLARE
        xml1        alias for $1;
        xml2    varchar;
        xml7    varchar;
        id1     bigint;
BEGIN
        xml2:=xml1;

        xml7:=put_campo(xml2,'_LOG_','');
	xml7:=put_campo(xml7,'TX','6010');
	execute 'insert into cola_motor_4 (fecha,uri,reintentos,data,tx,rut_emisor,reproceso,categoria,nombre_cola) values (now(),'||quote_literal(get_campo('URI_IN',xml2))||',0,'||quote_literal(xml7)||','||'10'||',null,''NO'',''ENVIO_BOLETA_SII'',''cola_motor_4'') returning id' into id1;
	xml2:=logapp(xml2,'Inserto ENVIO_BOLETA_SII '||get_campo('URI_IN',xml2)||' id='||id1::varchar);

	--DAO 20210226 Si viene por el flujo 12794 no borramos porque continua
	xml2 := put_campo(xml2,'RESPUESTA','Status: 200 OK');
	if get_campo('__FLAG_PUB_10K__',xml2)<>'SI' then
		xml2 := sp_procesa_respuesta_cola_motor_original(xml2);
	end if;
        return xml2;
END;
$$ LANGUAGE plpgsql;


--Genera un XML para ir a borrar, con datos basicos
CREATE or replace FUNCTION pivote_borrado_8010(varchar) RETURNS varchar AS $$
DECLARE
        xml2        alias for $1;
        xml3    varchar;
BEGIN
	xml3:=xml2;
	xml3:=logapp(xml3,'BD_ORIGEN='||get_campo('_CATEGORIA_BD_',xml2));
        if(get_campo('_CATEGORIA_BD_',xml2)='COLAS')then
                xml3 := put_campo(xml3,'__SECUENCIAOK__','1010');
        else
		--Si no viene de las colas(Flujo Escritorio), no vamos a la secuencia de borrado
		if is_number(get_campo('__ID_DTE__',xml2)) is false then
			xml3:=logapp(xml3,'No viene desde las colas, Teminamos aqui');
                	xml3 := put_campo(xml3,'__SECUENCIAOK__','0');
		else	
                	xml3 := put_campo(xml3,'__SECUENCIAOK__','1000');
		end if;
        end if;
        return xml3;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION proc_verifica_fin_dte88_8010(varchar) RETURNS varchar AS $$
DECLARE
	xml1        alias for $1;
	xml2    varchar;
	sts1	varchar;
	xml3	varchar;
BEGIN
    xml2:=xml1;
    --Verificamos si la boleta tiene mandato, que se haya ejecutado correctamente
    if (get_campo('TIPO_DTE',xml2) in ('39','41') and get_campo('__DTE_CON_MANDATO__',xml2)='SI' and get_campo('__EDTE_MANDATO_OK__',xml2)='') then
		--Si esto paso es porque el flujo 12770 no se ejecuto, entonces envio del mandato 
		xml3:=put_campo('','URI_IN',get_campo('URI_IN',xml2));
		sts1:=sp_reprocesa_mandato2(xml3);
		if (strpos(sts1,'OK-')>0) then
			xml2:=logapp(xml2,'Se encola Mandato para DTE URI='||get_campo('URI_IN',xml2));
		else
			xml2:=logapp(xml2,'Falla encolar Mandato para DTE URI='||get_campo('URI_IN',xml2));
		end if;
    end if;

    xml2 := sp_procesa_respuesta_cola_motor_original(xml2);
    return xml2;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION proc_procesa_input_dte_8010(varchar) RETURNS varchar AS $$
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
    json1	json;
    data_anulacion varchar;
    stSecuencia secuencia_aml%ROWTYPE;    
    uri1 	varchar;
	inserto_MaEmpresas      varchar;
	j3	json;
BEGIN
    xml2:=xml1;
	if get_campo('GRABA_PUB_8010',xml2)='SI' and get_campo('__PUBLICADO_OK__',xml2)='SI' then
		if (get_campo('SCRIPT_NAME',xml2) in ('/ca4/ca4rec','/ca4/recmotor')) then
			xml2:=put_campo(xml2,'CANAL','RECIBIDOS');
			xml2:=put_campo(xml2,'RUT_OWNER',get_campo('RUT_RECEPTOR',xml2));
		else
			xml2:=put_campo(xml2,'CANAL','EMITIDOS');
			xml2:=put_campo(xml2,'RUT_OWNER',get_campo('RUT_EMISOR',xml2));
		end if;
		xml2:=put_campo(xml2,'COMENTARIO_TRAZA','');
		xml2:=put_campo(xml2,'COMENTARIO2','');
		--FAY-DAO 20210226 Grabamos hacia aws
		xml2:= put_campo(xml2,'FECHA_EVENTO',now()::varchar);
		xml2 := graba_bitacora_aws(xml2,'PUB');
	end if;

    --xml2:=logapp(xml2,'Paso1');
    --xml2:=logapp(xml2,'Llega DTE '||get_campo('SCRIPT_NAME',xml2));
    --xml2:=logapp(xml2,'__ID_DTE__='||get_campo('__ID_DTE__',xml2));
    --xml2:=logapp(xml2,'__COLA_MOTOR__='||get_campo('__COLA_MOTOR__',xml2));

	/*
	if strpos(get_campo('URI_IN',xml2),'http://entelpcs')>0 then 
			xml2:=logapp(xml2,'ENTELPCS Detenido '||get_campo('URI_IN',xml2));
			xml2 := put_campo(xml2,'RESPUESTA','Status: 444 NK');
			xml2 := pivote_borrado_8010(xml2);
			return xml2;
	end if;
	*/
    --Si es un nagios, ignoro el procesamiento
    if (get_campo('REQUEST_METHOD',xml2)='GET') then
	if ((strpos(get_campo('HTTP_USER_AGENT',xml2),'check_http')>0) or (length(get_campo('QUERY_STRING',xml2))=0)) then
                xml2 := logapp(xml2,'Nagios Check o GET sin datos, se ignora');
                --Se Responde OK
                xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
		xml2 := pivote_borrado_8010(xml2);
		--xml2 := put_campo_ctx(xml2,'__ETAPA1__','OK');
                return xml2;
	end if;
    end if;
	
    --2015-12-10 FAY,FBU Registro de Ip entrantes
    --xml2:=logapp(xml2,'REG_ORI INI');
    --Todo menos lo que se genera en el escritorio
    if (get_campo('__FLAG_PUB_10K__',xml2)<>'SI') then
	    perform graba_registro_origen(xml2);
    end if;
    --xml2:=logapp(xml2,'REG_ORI FIN');
	
    --FAY-DAO 2018-03-13 PAra soportar el CONTROLLER PRE-EMISION
    if (get_campo('__RETIENE_DTE__',xml2)='SI') then
	xml2:=logapp(xml2,'Retiene DTE MENSAJE_XML_FLAGS='||get_campo('MENSAJE_XML_FLAGS',xml2));
	xml2:=put_campo(xml2,'RESPUESTA','Status: 555 NK');
	xml2 := pivote_borrado_8010(xml2);
	return xml2;
    end if;

    --verifico si el documento ya fue publicado en el almacen, em caso contrario no sigo procesando
    if (get_campo('__PUBLICADO_OK__',xml2)<>'SI') then
    	xml2:=logapp(xml2,'Falla la Publicacion en Almacen '||get_campo('URI_IN',xml2));

	--20150224 FAY si algun DTE viene sin URI_IN no puede ser procesado, se guarda en cola_motor_sin_uri y se borra de las colas de trabajo
	if (length(get_campo('URI_IN',xml2))=0) then
		xml2 := sp_graba_cola_sin_uri(xml2);
		--xml2 := put_campo(xml2,'STATUS_HTTP','200 OK');
                xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
		xml2 := pivote_borrado_8010(xml2);
		return xml2;
	end if;	


	--Si es Borrador, lo dejo pasar., se maneja en las reglas
	if (strpos(get_campo('URI_IN',xml2),'http://pruebas')=0) then
		--xml2 := put_campo(xml2,'STATUS_HTTP','400 NK');	
		xml2 := put_campo(xml2,'RESPUESTA','Status: 400 NK');
		xml2 := pivote_borrado_8010(xml2);
		return xml2;
	end if;

    end if;

    --Si es un contrato de FEB solo se publica
    if (get_campo('SCRIPT_NAME',xml2)='/ca4/ca4feb' or get_campo('SCRIPT_NAME',xml2)='/ca4/ca4fed') then
	xml2:=graba_contrato_feb(xml2);
	xml2:=logapp(xml2,'CA4FEB: Contrato OK');
	xml2 := put_campo(xml2,'RESPUESTA','Status: 200 OK');
	xml2 := pivote_borrado_8010(xml2);
	return xml2;
    --Si es una consulta de cesion...
    elsif (get_campo('SCRIPT_NAME',xml2)='/ca4/ca4verifica_cesion') then
	xml2:=logapp(xml2,'CESION: Consulta estado de cesion');
	xml2 := put_campo(xml2,'RESPUESTA','Status: 400 NK');
	xml2 := pivote_borrado_8010(xml2);
	return xml2;
    --Si es un mandato de Entel mandamos el mandato
    --Si es un ARM voy al flujo de los ARM
    --elsif (get_campo('SCRIPT_NAME',xml2)='/ca4/ca4arm') then
    --2017-09-14 se cambia la mirada por el CONTENIDO y no por donde se publica
    elsif (get_campo('CONTENIDO',xml2)='ARM') then
	xml2:=logapp(xml2,'ARM: DTE es un ARM* '||get_campo('URI_IN',xml2));
	/*if (strpos(get_campo('URI_IN',xml2),'cencosud')>0) then
		xml2 := put_campo(xml2,'__SECUENCIAOK__','1501');
	else
		xml2 := put_campo(xml2,'__SECUENCIAOK__','1500');
	end if;
	*/
	xml2 := put_campo(xml2,'__SECUENCIAOK__','1500');
	return xml2;
    --elsif (get_campo('SCRIPT_NAME',xml2)='/ca4/ca4resp') then
    -- NBV 20170405 DAO
    elsif (get_campo('SCRIPT_NAME',xml2)='/ca4/ca4sar') then
        xml2:=logapp(xml2,'CA4REC: RECLAMO es un ca4sar');
        xml2 := put_campo(xml2,'__SECUENCIAOK__','1540');
        xml2 := put_campo(xml2,'DESC_ORIGEN','Integracion: ca4sar');
        return xml2;
--Revisamos el contenido, si no es una RESPUESTA, lo procesamos como DTE
    elsif (get_campo('CONTENIDO',xml2)='RESPUESTA') then
		xml2:=logapp(xml2,'CA4RESP: DTE es un ca4resp');
		xml2 := put_campo(xml2,'__SECUENCIAOK__','1510');
		return xml2;
    elsif (get_campo('SCRIPT_NAME',xml2)='/ca4/ca4aec') then
	xml2:=logapp(xml2,'CA4AEC: DTE es un ca4aec');
	xml2 := put_campo(xml2,'ORIGEN_AEC','/ca4/ca4aec');
	xml2 := put_campo(xml2,'__SECUENCIAOK__','1520');
	return xml2;
    elsif (get_campo('SCRIPT_NAME',xml2)='/ca4/ca4n') then
	xml2:=logapp(xml2,'CA4N: Es un ca4n');
	data1:=decode(get_campo('INPUT',xml2),'hex');
	if (strpos(data1,'<SendList ')>=0) then
		xml2:=logapp(xml2,'CA4N: Es un SendList ');
		uri1:=get_xml('Uri',data1);
		xml2 := put_campo(xml2,'__FLAG_REINTENTO_MANDATO__','SI');
		xml2 := put_campo(xml2,'__FLAG_REENVIO_MANDATO__','SI');
	 	xml2 := put_campo(xml2,'__DTE_CON_MANDATO__','SI');
		xml2 := put_campo(xml2,'DTE_MANDATO_PDF','');
		xml2 := put_campo(xml2,'DTE_MANDATO_PDF_CLAVE','');
		xml2 := put_campo(xml2,'DTE_MANDATO','ALL');
		--xml2 := put_campo(xml2,'URI_IN',uri1);
		xml2 := put_campo(xml2,'MANDATO_MAIL_EMISOR',split_part(split_part(data1,'From="',2),'"',1));
		xml2 := put_campo(xml2,'MANDATO_EMAIL',get_xml('Email',data1));
		--xml2 := put_campo(xml2,'MANDATO_EMAIL','fernando.arancibia@acepta.com');
		data1:=get_input_almacen('{"uri":"'||uri1||'"}');
		if (length(data1)=0) then
			xml2:=logapp(xml2,'CA4N: Falla leer DTE almacen '||uri1);
			xml2 := put_campo(xml2,'RESPUESTA','Status: 444 NK');
			xml2 := pivote_borrado_8010(xml2);
			return xml2;
		end if;
		xml2 := put_campo(xml2,'INPUT_CUSTODIUM',data1);
		data1:=decode(data1,'hex');
		xml2 := put_campo(xml2,'URI_IN',split_part(split_part(data1,'"custodium-uri">',2),'<',1));
		xml2 := put_campo(xml2,'TIPO_DTE',get_xml('TipoDTE',data1));
		xml2 := put_campo(xml2,'DOMINIO',get_dominio_uri(get_campo('URI_IN',xml2)));
		xml2 := put_campo(xml2,'__SECUENCIAOK__','1610');
		return xml2;
	end if;
	xml2 := put_campo(xml2,'RESPUESTA','Status: 444 NK');
	xml2 := pivote_borrado_8010(xml2);
	return xml2;
    elsif (get_campo('SCRIPT_NAME',xml2)='/ca4/ca4rec') then
	xml2:=logapp(xml2,'CA4REC: DTE es un ca4rec');
	xml2 := put_campo(xml2,'__SECUENCIAOK__','1530');
	return xml2;
    elsif (get_campo('SCRIPT_NAME',xml2)='/ca4/ca4ocrec') then
        xml2:=logapp(xml2,'CA4REC: DTE es un ca4ocrec');
        xml2 := put_campo(xml2,'__SECUENCIAOK__','1530');
        return xml2;
    elsif (get_campo('SCRIPT_NAME',xml2)='/ca4/ca4eventos') then
	xml2:=logapp(xml2,'CA4EVENTOS: DTE es un ca4eventos');
	xml2 := put_campo(xml2,'RESPUESTA','Status: 400 NK');
	xml2 := pivote_borrado_8010(xml2);
	return xml2;
--  elsif (get_campo('SCRIPT_NAME',xml2)='/ca4/ca4lib') then
    elsif (get_campo('SCRIPT_NAME',xml2) in ('/ca4/ca4lib','/ca4/ca4windtelib','/ca4/ca4enternetlib')) then
        xml2:=logapp(xml2,'CA4LIB: DTE es un ca4lib');
        xml2 := put_campo(xml2,'FLAG_ORIGEN', 'SI'); -- GAVILA - RME / FLAG ORIGEN PARA QUE LLEGUE A "LIBROS MENSUALES" 2016-10-26
	--RME 20151126
	xml2 := put_campo(xml2,'ORIGEN_LIBRO',get_campo('SCRIPT_NAME',xml2));
        xml2 := put_campo(xml2,'__SECUENCIAOK__','700');
        return xml2;
    elsif (get_campo('SCRIPT_NAME',xml2)='/ca4/ca4rcf') then
        xml2:=logapp(xml2,'CA4RCF: DTE es un ca4rcf');
        xml2 := put_campo(xml2,'__SECUENCIAOK__','710');
        return xml2;
    --Si es un DTE emitido-importado
    elsif (get_campo('SCRIPT_NAME',xml2)='/ca4/ca4importer') or (get_campo('SCRIPT_NAME',xml2)='/ca4/ca4importer_rec') then

        if get_campo('CANAL_IMPORTER',xml2) = 'EMITIDOS' then
                xml2:=proc_ca4_importer_8010(xml2);
        elsif get_campo('CANAL_IMPORTER',xml2) = 'RECIBIDOS' then
                 xml2:=proc_ca4_importer_recibidos_8010(xml2);
        end if;
        return xml2;
    --Si es un DTE recibido-importado
 --   elsif (get_campo('SCRIPT_NAME',xml2)='/ca4/ca4importer_rec') then
 --       xml2:=proc_ca4_importer_recibidos_8010(xml2);
 --       return xml2;
	--paso de Anula DTE, 
    elsif (get_campo('SCRIPT_NAME',xml2)='/ca4/ca4anuladte') then
        xml2:=logapp(xml2,'CA4ANULADTE: Anula DTE');
        xml2:= procesa_anula_dte(xml2);
        return xml2;

    --paso a prod anula folios 20151124
    elsif (get_campo('SCRIPT_NAME',xml2)='/ca4/ca4anula') then
	xml2:=logapp(xml2,'CA4ANULA: UPDATE Anula Folios SII');
        json1 := '{}';
        data_anulacion := decode(get_campo('INPUT',xml2),'hex');
        json1 := put_json(json1,'RUTEMISOR',get_xml('RUTEmisor',data_anulacion));
        json1 := put_json(json1,'FECHASOLICITUD',get_xml('FechaSolicitud',data_anulacion));
        json1 := put_json(json1,'TIPODTE',get_xml('TipoDTE',data_anulacion));
        json1 := put_json(json1,'FOLIOINICIAL',get_xml('FolioInicial',data_anulacion));
        json1 := put_json(json1,'FOLIOFINAL',get_xml('FolioFinal',data_anulacion));
        json1 := put_json(json1,'MOTIVOANULACION',get_xml('MotivoAnulacion',data_anulacion));
        json1 := put_json(json1,'URI',get_campo('URI_IN',xml2));
        json1 := put_json(json1,'PARAMETRO1',get_xml('Parametro1',data_anulacion));
        json1 := put_json(json1,'PARAMETRO2',get_xml('Parametro2',data_anulacion));
        json1 := put_json(json1,'PARAMETRO3',get_xml('Parametro3',data_anulacion));
        json1 := put_json(json1,'PARAMETRO4',get_xml('Parametro4',data_anulacion));

	xml2:=logapp(xml2,'select eliminar_rango_folios_ca4anula('''||json1::varchar||''');');
	json1 := eliminar_rango_folios_ca4anula(json1);
	xml2:=logapp(xml2,'LOG_GF '||get_json('_LOG_',json1));
	xml2:=logapp(xml2,'CA4ANULA: STATUS_ELIMINA_RANGO='||get_json('STATUS_ELIMINA_RANGO',json1)||' MSG_ELIMINA_RANGO='||get_json('MSG_ELIMINA_RANGO',json1));
	if get_json('STATUS_ELIMINA_RANGO',json1)='OK' then
		xml2 := put_campo(xml2,'RESPUESTA','Status: 200 OK');
	else
		xml2 := put_campo(xml2,'RESPUESTA','Status: 400 OK');
	end if;
	xml2 := pivote_borrado_8010(xml2);
        return xml2;

        json1 := guardar_solicitud_anula_folios(json1);
        --xml2 := put_campo(xml2,'RESPUESTA',get_json('__Respuesta__',json1));
	xml2 := put_campo(xml2,'RESPUESTA','Status: 200 OK');
	xml2 := pivote_borrado_8010(xml2);
        return xml2;
    end if;
  
    --xml2:=logapp(xml2,'Entro a  Parseo de Datos');
    xml2 := reglas.parseo_datos(xml2);

    --Si emite directo por pantalla y hay algun error..
    if (get_campo('__FLAG_PUB_10K__',xml2)='SI' and (get_campo('__BASURA_CON_URI__',xml2)='SI' or get_campo('__BASURA__',xml2)='SI')) then
	 if (length(get_campo('__MENSAJE_10K__',xml2))=0) then
         	xml2:=put_campo(xml2,'__MENSAJE_10K__','Error al Emitir Documento');
	 end if;
         xml2 := put_campo(xml2,'STATUS_HTTP','400 NK');
         xml2 := responde_aml(xml2);
         return xml2;
   end if;

    --Si no parseo bien y no es un DTE
    if (get_campo('__BASURA_CON_URI__',xml2)='SI') then
	 xml2 := put_campo(xml2,'__SECUENCIAOK__','510');
         return xml2;
    elsif (get_campo('__BASURA__',xml2)='SI') then
	 --Grabo Evento en la traza con error
	 xml2 := graba_bitacora_aws(xml2,'ERROR_DTE');
	 xml2 := put_campo(xml2,'__SECUENCIAOK__','500');
	 return xml2;
    end if;

    if (get_campo('BORRADOR',xml2)<>'SI') then
	    xml2:=procesa_documentos_relacionados(xml2);
    end if;

    --Procesador de Reglas
    xml2 := reglas.validacion(xml2);
    --Si la regla debe llamar otro flujo..
    if get_campo('__EXIT__',xml2)='1' then
   	 resp1:=get_campo('RESPUESTA',xml2); 
	 respuesta1:=split_part(resp1,chr(10)||chr(10),2);
	 if (strpos(resp1,'200 OK')>0) then
	        xml2 := logapp(xml2,'Respuesta Servicio 200 OK URI'||get_campo('URI',xml1));
		xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
	 elsif (strpos(resp1,'555 NK')>0) then
		xml2:=put_campo(xml2,'RESPUESTA','Status: 555 NK');
		--Si no esta encolado, lo mandamos a la secuencia para encolar el envio
		if is_number(get_campo('__ID_DTE__',xml2)) is false then
			xml2 := logapp(xml2,'Encolamos el Envio');
			xml2 := put_campo(xml2,'__SECUENCIAOK__','1632');	
			RETURN xml2;
		end if;
		xml2 := logapp(xml2,'Enviamos a Futuro ');
	 else
	        xml2 := logapp(xml2,'Respuesta Servicio 400 Rechazado (8010) URI'||get_campo('URI',xml1));
		xml2:=put_campo(xml2,'RESPUESTA','Status: 400 NK');
   	 end if;
    	--xml2:=put_campo(xml2,'RESPUESTA',status1||chr(10)||respuesta1);

	--Si voy a borrar no necesito INPUT ni CERTIFICADO_X509, para que ni viaje de vuelta en la respuesta
	xml2 := pivote_borrado_8010(xml2);
	xml2:=put_campo(xml2,'INPUT','');
	xml2:=put_campo(xml2,'CERTIFICADO_X509','');
	xml2:=put_campo(xml2,'XML3','');
	RETURN xml2;
    end if;

    if get_campo('__SECUENCIAOK__',xml2)='28' then
	--FAY 2021-02-25
	--Si la proxima secuencia es la 28, no es necesario ejecutarla, ya que estamos en la misma base, la ejecutamos inmediatamente, nos evitamos idas y vueltas del procesador
	xml2 := proc_procesa_input_dte_8010_parte2(xml2);
    end if;
    return xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION proc_procesa_input_dte_8010_parte2(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
        xml2    varchar;
    data1       varchar;
    file1       varchar;
    sts         integer;
    host1       varchar;
    url1        varchar;
    respuesta1  varchar;
    resp1       varchar;
    status1     varchar;
    json1       json;
        j3      json;
    data_anulacion varchar;
    stSecuencia secuencia_aml%ROWTYPE;
    uri1        varchar;
        -- NBV 20170921
        inserto_MaEmpresas      varchar;
        xml_dte1        varchar;
BEGIN
    xml2:=xml1;
    --Cuando vuelve de las reglas de cualquier base..
    if get_campo('__EXIT__',xml2)='1' then
         resp1:=get_campo('RESPUESTA',xml2);
         respuesta1:=split_part(resp1,chr(10)||chr(10),2);
         if (strpos(resp1,'200 OK')>0) then
                xml2 := logapp(xml2,'Respuesta Servicio 200 OK URI'||get_campo('URI',xml1));
                xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
         else
                xml2 := logapp(xml2,'Respuesta Servicio 400 Rechazado (8010) URI'||get_campo('URI',xml1));
                xml2:=put_campo(xml2,'RESPUESTA','Status: 400 NK');
         end if;
        --xml2:=put_campo(xml2,'RESPUESTA',status1||chr(10)||respuesta1);

        --Si voy a borrar no necesito INPUT ni CERTIFICADO_X509, para que ni viaje de vuelta en la respuesta
        xml2 := pivote_borrado_8010(xml2);
        xml2:=put_campo(xml2,'INPUT','');
        xml2:=put_campo(xml2,'CERTIFICADO_X509','');
        xml2:=put_campo(xml2,'XML3','');
        RETURN xml2;
    end if;


    --raise notice 'despues reglas';
    if (get_campo('BORRADOR',xml2)<>'SI') then
	xml2 := put_campo(xml2,'ESTADO_INICIAL_DTE','INGRESADO');
    	xml2 := insert_dte(xml2);
    end if;
        
    --Graba Bitacora
    xml2:= put_campo(xml2,'COMENTARIO_TRAZA',''); 
    --FAY-DAO 20210226 no tiene sentido tanto evento
    --xml2 := graba_bitacora(xml2,'INGRESADO');
    --Grabo Eventos de Timbre y Firma
    --xml2:= put_campo(xml2,'FECHA_EVENTO',get_campo('FECHA_TIMBRE',xml2));
    --xml2 := graba_bitacora(xml2,'TMB');
    xml2:= put_campo(xml2,'FECHA_EVENTO',get_campo('FECHA_FIRMA',xml2));
    --FAY-DAO 20210226 grabamos hacia aws 
    xml2 := graba_bitacora_aws(xml2,'FRM');

    --Este flag indica que no se procesara con el AML
    if get_campo('__FLUJO_EXIT__',xml2)='SI' then
        xml2:=logapp(xml2,'Ejecuto Sec='||get_campo('__SECUENCIAOK__',xml2));
	return xml2;
    end if;
  
    --Si es Borrador lo borro de la cola
    if (get_campo('BORRADOR',xml2)='SI') then
                --Se Responde OK
		xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
		xml2 := pivote_borrado_8010(xml2);
		return xml2;
    end if;


   --2015-03-26 FAY,RME.ILB Todos los DTE que no sean CGE se van directo al EDTE
   xml2:=put_campo(xml2,'RUT_CGE',get_campo('RUT_EMISOR',xml2));
   --xml2:=verifica_evento_cge(xml2);
   xml2:=put_campo(xml2,'EVENTO_CGE','NO');
	
   --Si no es una boleta o sea DTE
   if (get_campo('TIPO_DTE',xml2) not in ('39','41','801')) then
                xml2 := put_campo(xml2,'__SECUENCIAOK__','120');
       	        return xml2;
   else
	--Si es una Boleta
	if (get_campo('__DTE_CON_MANDATO__',xml2)='SI') then
		xml2:=get_parametros_motor(xml2,'MANDATO_NORMAL');
		--Para que borre despues de ejecutar el envio de mandatos
		xml2 := put_campo(xml2,'STATUS_HTTP','200 OK');
	        xml2 := responde_aml(xml2);
		xml2 := proc_procesa_respuesta_dte_8010(xml2);
	        xml2 := put_campo(xml2,'__SECUENCIAOK__','1610');
		return xml2;
	else
		--Para que borre despues de ejecutar el envio de mandatos
	        xml2 := put_campo(xml2,'STATUS_HTTP','200 OK');
	        xml2 := responde_aml(xml2);
	        xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
		return 	proc_procesa_respuesta_dte_8010(xml2);
	end if;
    end if; 

   return xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION proc_procesa_respuesta_dte_8010_colas(varchar) RETURNS varchar AS $$
DECLARE
    xml1	alias for $1;
    xml2	varchar;
    --data1	varchar;
    resp1	varchar;
    sts		integer;
    texto_resp1	varchar;
    respuesta1	varchar;
    status1	varchar;
BEGIN
    xml2:=xml1;
    --data1:=get_campo('INPUT',xml2);
    xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
    --Si hay respuesta del AML

    --Limpio el INPUT para el LOG
    resp1:= get_campo('RESPUESTA',xml2);
    --Si viene este texto entonces AML responde OK
    texto_resp1 := 'URL(True): '||get_campo('URI_IN',xml2);

    --Verifico si me fue bien con el AML
    --Debe contestar un OK y debe venir la URI que se envio a la entrada
    --if strpos(resp1,'200 OK')>0 then
    if (strpos(resp1,'200 OK')>0 and strpos(resp1,texto_resp1)>0) then
	--Un documento reprocesado, que se envia al AML, se puede borrar de la cola de procesamiento
	if (get_campo('_REPROCESO_',xml2)='SI') then
		xml2 := put_campo(xml2,'_ESTADO_REPROCESO_','OK');
		xml2 := logapp(xml2,'Reproceso Marcado OK');
	end if;

    	--xml2 := put_campo(xml2,'ESTADO','ENVIADO_EDTE');
        --xml2 := graba_bitacora(xml2,'ENVIADO_AML');
	xml2 := logapp(xml2,'FOLIO='||get_campo('FOLIO',xml2)||' RUT_EMISOR='||get_campo('RUT_EMISOR',xml2)||' TIPO_DTE='||get_campo('TIPO_DTE',xml2)||' EMITIDO');
    
	--Saco los datos que requiero de la respuesta
	xml2 := put_campo(xml2,'URI',get_tag_http(resp1,'URL(True): '));
    
	--SI se activo en alguna regla el TAG __FLUJO_POST_EXIT__ se ejecuta la secuencia de __SECUENCIA_POST_OK__
        if (get_campo('__FLUJO_POST_EXIT__',xml2)='SI') then
		xml2 := logapp(xml2,'Activa Flujo Post Secuencia '||get_campo('__SECUENCIA_POST_OK__',xml2));
                xml2 := put_campo(xml2,'__SECUENCIAOK__',get_campo('__SECUENCIA_POST_OK__',xml2));
        end if;
    else
	xml2 := logapp(xml2,'FOLIO='||get_campo('FOLIO',xml2)||' RUT_EMISOR='||get_campo('RUT_EMISOR',xml2)||' TIPO_DTE='||get_campo('TIPO_DTE',xml2)||' Falla Respuesta AML');
	xml2 := logapp(xml2,resp1);
    end if; 

    --TODO hacer un control cuando falle el update
    --xml2 := put_campo(xml2,'INPUT','');
    respuesta1:=split_part(resp1,chr(10)||chr(10),2);
    if (strpos(resp1,'200 OK')>0) then
    	xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
        xml2 := logapp(xml2,'Respuesta Servicio 200 OK URI'||get_campo('URI',xml1));
    else
    	xml2:=put_campo(xml2,'RESPUESTA','Status: 400 NK');
        xml2 := logapp(xml2,'Respuesta Servicio 400 Rechazado (8010) URI'||get_campo('URI',xml1));
    end if;
                
    --No borro y voy a borrar en la secuencia 1010
    xml2 := pivote_borrado_8010(xml2);
    --Respondo lo que viene
    RETURN xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION proc_procesa_respuesta_dte_8010(varchar) RETURNS varchar AS $$
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
    xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
    --Si hay respuesta del AML

    --Limpio el INPUT para el LOG
    resp1:= get_campo('RESPUESTA',xml2);
    --Si viene este texto entonces AML responde OK
    texto_resp1 := 'URL(True): '||get_campo('URI_IN',xml2);

    --Verifico si me fue bien con el AML
    --Debe contestar un OK y debe venir la URI que se envio a la entrada
    --if strpos(resp1,'200 OK')>0 then
    if (strpos(resp1,'200 OK')>0 and strpos(resp1,texto_resp1)>0) then
	--Un documento reprocesado, que se envia al AML, se puede borrar de la cola de procesamiento
	if (get_campo('_REPROCESO_',xml2)='SI') then
		xml2 := put_campo(xml2,'_ESTADO_REPROCESO_','OK');
		xml2 := logapp(xml2,'Reproceso Marcado OK');
	end if;

    	--xml2 := put_campo(xml2,'ESTADO','ENVIADO_EDTE');
        --xml2 := graba_bitacora(xml2,'ENVIADO_AML');
	xml2 := logapp(xml2,'FOLIO='||get_campo('FOLIO',xml2)||' RUT_EMISOR='||get_campo('RUT_EMISOR',xml2)||' TIPO_DTE='||get_campo('TIPO_DTE',xml2)||' EMITIDO');
    
	--Saco los datos que requiero de la respuesta
	xml2 := put_campo(xml2,'URI',get_tag_http(resp1,'URL(True): '));
    
	-- Guardo la boleta en Por ahora. Pendiente las Boletas Exentas
	if (get_campo('TIPO_DTE',xml2) in ('39','41')) then
	    --Actualizo esta de la boleta
            --xml2 := graba_bitacora(xml2,'GRABADO_BOLETA_OK');
	    xml2 := logapp(xml2,'GRABADO BOLETA OK FOLIO='||get_campo('FOLIO',xml2)||' RUT_EMISOR='||get_campo('RUT_EMISOR',xml2));
    	    xml2 := put_campo(xml2,'ESTADO','BOLETA_GRABADA_OK');
    	    xml2 := put_campo(xml2,'ESTADO_SII','BOLETA_GRABADA_OK');
	    xml2 := logapp(xml2,'##BOLETA39 FOLIO='||get_campo('FOLIO',xml2));
	    if (get_campo('BORRADOR',xml2)<>'SI') then
        	xml2 := update_dte(xml2);
	    end if;
	end if;

	--SI se activo en alguna regla el TAG __FLUJO_POST_EXIT__ se ejecuta la secuencia de __SECUENCIA_POST_OK__
        if (get_campo('__FLUJO_POST_EXIT__',xml2)='SI') then
		xml2 := logapp(xml2,'Activa Flujo Post Secuencia '||get_campo('__SECUENCIA_POST_OK__',xml2));
                xml2 := put_campo(xml2,'__SECUENCIAOK__',get_campo('__SECUENCIA_POST_OK__',xml2));
        end if;
    else
	xml2 := logapp(xml2,'FOLIO='||get_campo('FOLIO',xml2)||' RUT_EMISOR='||get_campo('RUT_EMISOR',xml2)||' TIPO_DTE='||get_campo('TIPO_DTE',xml2)||' Falla Respuesta AML');
    	--xml2 := put_campo(xml2,'ESTADO','ERROR_AML');
        --xml2 := graba_bitacora(xml2,'ERROR_AML');
	xml2 := logapp(xml2,resp1);
    end if; 

/*    if (get_campo('BORRADOR',xml2)<>'SI') then
        xml2 := update_dte(xml2);
    end if;
*/
    --TODO hacer un control cuando falle el update
    --xml2 := put_campo(xml2,'INPUT','');
    respuesta1:=split_part(resp1,chr(10)||chr(10),2);
    if (strpos(resp1,'200 OK')>0) then
    	xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
        xml2 := logapp(xml2,'Respuesta Servicio 200 OK URI'||get_campo('URI',xml1));
    else
    	xml2:=put_campo(xml2,'RESPUESTA','Status: 400 NK');
        xml2 := logapp(xml2,'Respuesta Servicio 400 Rechazado (8010) URI'||get_campo('URI',xml1));
    end if;
                
    --No borro y voy a borrar en la secuencia 1010
    xml2 := pivote_borrado_8010(xml2);
    --Respondo lo que viene
    RETURN xml2;
END;
$$ LANGUAGE plpgsql;



CREATE or replace FUNCTION proc_ca4_importer_8010(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2    varchar;
BEGIN
    xml2:=xml1;
    xml2:=logapp(xml2,'CA4IMPORTER: Dte para importar');
        xml2 := put_campo(xml2,'__FLAG_IMPORTER__','SI');
        --Parseo los datos
        xml2 := reglas.parseo_datos(xml2);
        

        --Si no parseo bien y no es un DTE
        if (get_campo('__BASURA_CON_URI__',xml2)='SI') then
            xml2 := put_campo(xml2,'__SECUENCIAOK__','510');
            return xml2;
        elsif (get_campo('__BASURA__',xml2)='SI') then
             --Grabo Evento en la traza con error
             xml2 := graba_bitacora_aws(xml2,'ERROR_DTE');
             xml2 := put_campo(xml2,'__SECUENCIAOK__','500');
             return xml2;
        end if;
	

        --Aplico algunas reglas
        xml2:=reglas.maestro_clientes(xml2);
        if get_campo('__EXIT__',xml2)='1' then
                xml2 := logapp(xml2,'Respuesta Servicio 400 Rechazado (No es cliente) URI'||get_campo('URI',xml1));
		xml2 := put_campo(xml2,'RESPUESTA','Status: 400 NK');
		xml2 := pivote_borrado_8010(xml2);
                return xml2;
        end if;
	if (get_campo('TIPO_DTE',xml2) in ('39','41')) then
        	xml2:=reglas.proxy_boletas(xml2);
	else
        	xml2:=reglas.proxy_dte(xml2);
	end if;
	

        if get_campo('__EXIT__',xml2)='1' then
        	xml2 := graba_bitacora_aws(xml2,'ERROR_IMP');
		xml2 := put_campo(xml2,'RESPUESTA','Status: 200 OK');
		xml2 := pivote_borrado_8010(xml2);
                return xml2;
        end if;
	--RME 20151202 se agrega Traza con comentario de Importado
        xml2:= put_campo(xml2,'COMENTARIO_TRAZA','Documento Importado');
        xml2:= put_campo(xml2,'FECHA_EVENTO',now()::varchar);
        xml2 := graba_bitacora_aws(xml2,'IMP');
        --Insertamos
        xml2 := put_campo(xml2,'ESTADO_INICIAL_DTE','IMPORTADO');
        xml2 := put_campo(xml2,'ESTADO_SII','IMPORTADO');
        xml2 := insert_DTE(xml2);

	xml2 := put_campo(xml2,'RESPUESTA','Status: 200 OK');
	xml2 := pivote_borrado_8010(xml2);
        return xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION proc_ca4_importer_recibidos_8010(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2    varchar;
BEGIN
    xml2:=xml1;
    xml2:=logapp(xml2,'CA4IMPORTERREC: Dte para importar');
        xml2 := put_campo(xml2,'__FLAG_IMPORTER__','SI');
         --Parseo los datos entrantes
        xml2 := parseo_doc_recibido(xml2);
        --Si no parseo bien y no es un DTE
        if (get_campo('__BASURA_CON_URI__',xml2)='SI') then
            xml2 := put_campo(xml2,'__SECUENCIAOK__','510');
            return xml2;
        elsif (get_campo('__BASURA__',xml2)='SI') then
             --Grabo Evento en la traza con error
             xml2 := graba_bitacora_aws(xml2,'ERROR_DTE');
             xml2 := put_campo(xml2,'__SECUENCIAOK__','500');
             return xml2;
        end if;
        --Aplico algunas reglas
        xml2:=reglas.proxy_dte_recibidos(xml2);
        if get_campo('__EXIT__',xml2)='1' then
                xml2 := logapp(xml2,'CA4IMPORTERREC: Saliendo por Duplicado');
               -- xml2 := put_campo(xml2,'STATUS_HTTP','200 OK');
        	xml2 := graba_bitacora_aws(xml2,'ERROR_IMP');
                xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
		xml2 := pivote_borrado_8010(xml2);
                return xml2;
        end if;
        --Insertamos
        xml2 := put_campo(xml2,'ESTADO_INICIAL_DTE','IMPORTADO');
        xml2 := put_campo(xml2,'ESTADO_SII','IMPORTADO');
        xml2 := insert_DTE(xml2);

        --RME 20151202 se agrega Traza con comentario de Importado
        xml2:= put_campo(xml2,'COMENTARIO_TRAZA','Documento Importado');
        xml2:= put_campo(xml2,'FECHA_EVENTO',now()::varchar);
        xml2 := graba_bitacora_aws(xml2,'IMP');
        xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
	xml2 := pivote_borrado_8010(xml2);
        return xml2;

END;
$$ LANGUAGE plpgsql;
