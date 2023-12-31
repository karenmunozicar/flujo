delete from isys_querys_tx where llave='12718';

--Primero que hacemos el publicar DTE
insert into isys_querys_tx values ('12718',10,1,8,'Publica DTE',112704,0,0,0,0,20,20);

-- Prepara llamada al AML
insert into isys_querys_tx values ('12718',20,45,1,'select proc_procesa_input_arm_12718(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0); 

--Inserta en las colas sii el reclamo si corresponde
insert into isys_querys_tx values ('12718',25,19,1,'select revisa_reclamo_arm_12718(''$$__XMLCOMPLETO__$$'') as __xml__ ',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('12718',31,1,8,'Escribe ARM EDTE',12780,0,0,0,0,40,40);

--Respuesta del AML
insert into isys_querys_tx values ('12718',40,45,1,'select proc_procesa_respuesta_arm_12718(''$$__XMLCOMPLETO__$$'') as __xml__ ',0,0,0,1,1,-1,0);

--Se ejecuta en la 88
insert into isys_querys_tx values ('12718',1000,19,1,'select sp_procesa_respuesta_cola_motor88(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,0,0);



CREATE or replace FUNCTION revisa_reclamo_arm_12718(varchar) RETURNS varchar AS $$
DECLARE
	xml1	alias for $1;
	xml2	varchar;
	campo3	record;
	xml3	varchar;
	uri1	varchar;	
	cola1	varchar;
	nombre_tabla1	varchar;
	id1	bigint;
		
BEGIN
   xml2:=xml1;

   uri1:=get_campo('URI_DTE',xml2);
   --Si ya esta insertado no lo procese
   select * into campo3 from cola_sii_generica where uri=uri1 and categoria='RECLAMO_ARM';
   if not found then
	xml3:='';
	xml3:=put_campo(xml3,'TX','16201');
	xml3:=put_campo(xml3,'RUT_EMISOR',get_campo('RUT_EMISOR_ARM',xml2));
	xml3:=put_campo(xml3,'TIPO_DTE',get_campo('TIPO_DTE_ARM',xml2));
	xml3:=put_campo(xml3,'FOLIO',get_campo('FOLIO_ARM',xml2));
	xml3:=put_campo(xml3,'EVENTO_RECLAMO','ERM');
	xml3:=put_campo(xml3,'URI_DTE',uri1);
	cola1:=nextval('id_cola_sii');
	nombre_tabla1:='cola_sii_'||cola1::varchar;
	xml3:=put_campo(xml3,'FECHA_INGRESO_COLA',now()::varchar);
	execute 'insert into ' || nombre_tabla1 || ' (fecha,uri,reintentos,data,tx,rut_emisor,reproceso,categoria,nombre_cola,rut_receptor,tipo_dte,folio) values ( now(),'||quote_literal(uri1)||',0,'||quote_literal(xml3)||',35,'||quote_literal(get_campo('RUT_RECEPTOR',xml2))||',''NO'','||quote_literal('RECLAMO_ARM')||','||quote_literal(nombre_tabla1)||','||quote_literal(get_campo('RUT_EMISOR_ARM',xml2))||','||quote_literal(get_campo('TIPO_DTE_ARM',xml2))||','||quote_literal(get_campo('FOLIO_ARM',xml2))||') returning id' into id1;
  	xml2:=logapp(xml2,'ARM: Grabo Reclamo en las Colas SII ID='||id1::varchar);
   else
	 xml2:=logapp(xml2,'ARM: Reclamo ya existe en las colas sii ');
   end if;

   if (get_campo('EVENTO_CGE',xml2)='SI') then
        xml2:=put_campo(xml2,'__IP_CONEXION_CLIENTE__','172.16.10.181');
   else
        xml2:=put_campo(xml2,'__IP_CONEXION_CLIENTE__',get_campo('__IP_CONEXION_CLIENTE__',get_parametros_motor('','EDTE')));
   end if;
   xml2 := put_campo(xml2,'__SECUENCIAOK__','31');
   return xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION proc_procesa_input_arm_12718(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
        xml2    varchar;
    --data1       varchar;
    file1       varchar;
    sts         integer;
    host1       varchar;
    url1        varchar;
    respuesta1  varchar;
    resp1       varchar;
    status1     varchar;
    input1      varchar;
    rut_emisor1 varchar;
    rut_receptor1 varchar;
    tipo_dte1   varchar;
    folio1      varchar;
    monto1      varchar;
    fecha_emi1  varchar;
    stRec       dte_recibidos%ROWTYPE;
    mail1       varchar;
    stMail      windte.wdte_op_clientes%ROWTYPE;
    md5_input1	varchar;  
    stRechazado	dte_arm_rechazados%ROWTYPE;

    stSecuencia secuencia_aml%ROWTYPE;
    falla1	integer;
	json_reclamo	json;
	json_in	json;
	json2	json;
	port varchar;
	input_ori1	varchar;
BEGIN
    xml2:=xml1;
    --Si es un nagios, ignoro el procesamiento
    if (get_campo('REQUEST_METHOD',xml2)='GET') then
        if ((strpos(get_campo('HTTP_USER_AGENT',xml2),'check_http')>0) or (length(get_campo('QUERY_STRING',xml2))=0)) then
                xml2 := logapp(xml2,'Nagios Check o GET sin datos, se ignora');
                --Se Responde OK
                xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK'||chr(10)||
                'Content-type: text/html; charset=iso-8859-1'||chr(10)||
                'Content-length: 0'||chr(10)||
                'Vary: Accept-Encoding'||chr(10)||chr(10));
                xml2 := put_campo(xml2,'__SECUENCIAOK__','1000');
		--xml2 := sp_procesa_respuesta_cola_motor(xml2);
                return xml2;
        end if;
    end if;


    --verifico si el documento ya fue publicado en el almacen, em caso contrario no sigo procesando
    if (get_campo('__PUBLICADO_OK__',xml2)<>'SI') then
        xml2:=logapp(xml2,'Falla la Publicacion en Almacen '||get_campo('URI_IN',xml2));

        --20150224 FAY si algun DTE viene sin URI_IN no puede ser procesado, se guarda en cola_motor_sin_uri y se borra de las colas de trabajo
        if (length(get_campo('URI_IN',xml2))=0) then
                --xml2 := sp_graba_cola_sin_uri(xml2);
		xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
		xml2 := put_campo(xml2,'__SECUENCIAOK__','1000');
                --xml2 := put_campo(xml2,'STATUS_HTTP','200 OK');
                --xml2 := responde_aml(xml2);
                return xml2;
        end if;


        --Si es Borrador, lo dejo pasar., se maneja en las reglas
        if (strpos(get_campo('URI_IN',xml2),'http://pruebas')=0) then
                xml2 := put_campo(xml2,'STATUS_HTTP','400 NK');
                xml2 := responde_aml(xml2);
                --xml2 := sp_procesa_respuesta_cola_motor(xml2);
		xml2 := put_campo(xml2,'__SECUENCIAOK__','1000');
                return xml2;
        end if;
    end if;

    --Parseo datos
   input_ori1:=get_campo('INPUT',xml2);
   input1:=decode(input_ori1,'hex');
   xml2 := put_campo(xml2,'TIPO_DTE',get_xml('TipoDoc',input1));
   tipo_dte1:=get_campo('TIPO_DTE',xml2);
   if (tipo_dte1='') then
   	xml2 := put_campo(xml2,'TIPO_DTE',get_xml('TipoDTE',input1));
	tipo_dte1:=get_campo('TIPO_DTE',xml2);
   end if;

   --FAY 2018-08-31
   --xml2 := put_campo(xml2,'INPUT','');

	
   xml2 := put_campo(xml2,'FOLIO',get_xml('Folio',input1));
   xml2 := put_campo(xml2,'RUT_EMISOR',split_part(get_xml('RUTEmisor',input1),'-',1));
   xml2 := put_campo(xml2,'RUT_RECEPTOR',split_part(get_xml('RUTRecep',input1),'-',1));
   xml2 := put_campo(xml2,'FECHA_EMISION',get_xml('FchEmis',input1));
   xml2 := put_campo(xml2,'MONTO_TOTAL',get_xml('MntTotal',input1));
   xml2 := put_campo(xml2,'RECINTO',get_xml('Recinto',input1));
   xml2 := put_campo(xml2,'URI_IN',split_part(split_part(input1,'filename="',2),'"',1));

   --xml2 := logapp(xml2,'DATA ARM='||input1);

   rut_emisor1:=get_campo('RUT_EMISOR',xml2);
   rut_receptor1:=get_campo('RUT_RECEPTOR',xml2);
   folio1:=get_campo('FOLIO',xml2);
   monto1:=get_campo('MONTO_TOTAL',xml2);
   fecha_emi1:=get_campo('FECHA_EMISION',xml2);

   xml2:=logapp(xml2,'ARM rut_emisor1='||rut_emisor1||' tipo_dte1='||tipo_dte1||' folio1='||folio1||' monto1='||monto1||' fecha_emi1='||fecha_emi1||' URI='||get_campo('URI_IN',xml2));


   --NO hay falla
   falla1:=0;
   --Si algun dato no viene bien, lo recibimos pero se guarda en los rechazos
   if (is_number(rut_emisor1) is false or is_number(tipo_dte1) is false or is_number(folio1) is false) then
	--Falla de Datos
	falla1:=1;
	xml2:=logapp(xml2,'Datos ARM Invalidos'); 
	--Corrijo la basura para grabar evento en traza
   	xml2 := put_campo(xml2,'FOLIO',substring(get_campo('FOLIO',xml2),1,20));
   	xml2 := put_campo(xml2,'TIPO_DTE',substring(get_campo('TIPO_DTE',xml2),1,20));
   else
   	   --Valido si existe en DTE Recibidos
	   select * into stRec from dte_recibidos where rut_emisor=rut_emisor1::integer and tipo_dte=tipo_dte1::integer and folio=folio1::bigint;
	   if not found then
		--Falla de no existe registro
		falla1:=2;
	   end if;

	   --Valido que el Rut Receptor del ARM sea el mismo del DTE recibido
	   if (stRec.rut_receptor<>get_campo('RUT_RECEPTOR',xml2)::integer) then
                xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
		--Que aparezca el vinculo del ARM mal generado
		xml2 := put_campo(xml2,'URL_GET',get_campo('URI_IN',xml2));
		--Que el evento se graba en el DTE recibido
		xml2 := put_campo(xml2,'URI_IN',stRec.uri);
		xml2 := put_campo(xml2,'COMENTARIO_TRAZA','ARM mal generado, Rut Receptor no corresponde');
		xml2 := graba_bitacora(xml2,'ERROR_DTE');
		xml2 := put_campo(xml2,'URI_IN',get_campo('URL_GET',xml2));
        	respuesta1:='URL(True): '||get_campo('URI_IN',xml2)||chr(10)||'ARM mal generado';
	        status1:='Status: 200 OK'||chr(10)||
        	         'Content-type: text/html'||chr(10)||
                	 'Content-Location: '||get_campo('URI_IN',xml2)||chr(10)||
	                 'Content-length: '||length(respuesta1)||chr(10);
        	xml2:=put_campo(xml2,'RESPUESTA',status1||chr(10)||respuesta1);
        	xml2 := logapp(xml2,'ARM: ARM mal generado , Rut receptor invalido URI='||get_campo('URI_IN',xml2));
		if (length(get_campo('__COLA_MOTOR__',xml2))>0) then
			--xml2:=sp_procesa_respuesta_cola_motor(xml2);
			--Si estoy reprocesando y esta en los rechazados los borro
			md5_input1:=md5(input_ori1);
			xml2:=put_campo(xml2,'MD5_INPUT',md5_input1);
			
			xml2:=logapp(xml2,'Se borra de dte_arm_rechazados');
			delete from dte_arm_rechazados where md5_input=md5_input1;
		end if;
		--xml2 := sp_procesa_respuesta_cola_motor(xml2);
		xml2 := put_campo(xml2,'__SECUENCIAOK__','1000');
		return xml2;
	   end if;

	   --Si el DTE recibido no esta aceptado, esperamos 
	   if (stRec.estado_sii not in ('ACEPTADO_POR_EL_SII')) then
                xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
		--Si lleva el ARM mas de X dias y no se aprueba el dte, se borra
		if (now()-get_campo('FECHA_INGRESO_COLA',xml2)::timestamp>interval '15 days') then
        		xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
	        	xml2 := logapp(xml2,'DTE no esta aprobado por el SII, despues de 15 dias se borra ARM URI='||stRec.uri);
	        else
        		xml2:=put_campo(xml2,'RESPUESTA','Status: 444 NK');
	        	xml2 := logapp(xml2,'DTE no esta aprobado por el SII, se espera URI='||stRec.uri);
		end if;
		xml2 := put_campo(xml2,'__SECUENCIAOK__','1000');
        	return xml2;
	   end if;
	   
	   
   	   --Antes de actualizar, si el DTE ya tiene marcarcado el estado ARM, no lo volvemos a marcar
	
	   if (stRec.uri_arm is not null) then
                xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
        	respuesta1:='URL(True): '||get_campo('URI_IN',xml2)||chr(10)||'Dte no se encuentra registrado';
	        status1:='Status: 200 OK'||chr(10)||
        	         'Content-type: text/html'||chr(10)||
                	 'Content-Location: '||get_campo('URI_IN',xml2)||chr(10)||
	                 'Content-length: '||length(respuesta1)||chr(10);
        	xml2:=put_campo(xml2,'RESPUESTA',status1||chr(10)||respuesta1);
        	xml2:= put_campo(xml2,'COMENTARIO_TRAZA','DTE ya tiene ARM Registrado');
	        xml2 := graba_bitacora(xml2,'ARM_REPETIDO');
        	xml2 := logapp(xml2,'DTE ya tiene ARM registrado URI='||stRec.uri);
		if (length(get_campo('__COLA_MOTOR__',xml2))>0) then
			--xml2:=sp_procesa_respuesta_cola_motor(xml2);
			--Si estoy reprocesando y esta en los rechazados los borro
			--md5_input1:=md5(get_campo('INPUT',xml2));
			md5_input1:=md5(input_ori1);
			xml2:=put_campo(xml2,'MD5_INPUT',md5_input1);
			xml2:=logapp(xml2,'Se borra de dte_arm_rechazados');
			delete from dte_arm_rechazados where md5_input=md5_input1;
		end if;
		--xml2 := sp_procesa_respuesta_cola_motor(xml2);
		xml2 := put_campo(xml2,'__SECUENCIAOK__','1000');
        	return xml2;
	   end if;
	
   end if;

   if (falla1>0) then
        --texto_resp1 := 'URL(True): '||get_campo('URI_IN',xml2);
        respuesta1:='URL(True): '||get_campo('URI_IN',xml2)||chr(10)||'Dte no se encuentra registrado';
        status1:='Status: 200 OK'||chr(10)||
                 'Content-type: text/html'||chr(10)||
                 'Content-Location: '||get_campo('URI_IN',xml2)||chr(10)||
                 'Content-length: '||length(respuesta1)||chr(10);
        xml2 := logapp(xml2,'No existe en Dte Recibidos URI'||get_campo('URI',xml2));
        xml2:=put_campo(xml2,'RESPUESTA',status1||chr(10)||respuesta1);
        xml2:= put_campo(xml2,'COMENTARIO_TRAZA','No existe en Dte Recibidos');
        xml2 := graba_bitacora(xml2,'FALLA_ARM');
        select * into stMail from windte.wdte_op_clientes where rut=rut_receptor1;
        if not found then
                mail1:='sistemas@acepta.com';
        else
                mail1:=stMail.opc;
        end if;
        --Envia Aviso
	/*
        insert into aviso_mail values (now(),get_campo('RUT_EMISOR',xml2),mail1,'Rechazo ARM Rut='||get_campo('RUT_EMISOR',xml2)||' Tipo_dte='||get_campo('TIPO_DTE',xml2)||' Folio='||get_campo('FOLIO',xml2),'Estimado Cliente:
Informamos que su ARM ha sido rechazado, debido a que no se encuentra registrado en los DTE recibidos.
Por favor comuniquese con nuestra mesa de ayuda al +562 24968100 (Opcion 2)
Atentamente
El equipo de Acepta.'||chr(10)||chr(10)||replace(get_campo('URI_IN',xml2),'v01','traza')||chr(10),nextval('aviso_mail_codigo_seq'::regclass),'ruben.munoz@acepta.com,fernando.arancibia@acepta.com,ingrid.leyton@acepta.com');
	*/
	--Si se esta reprocesando desde la cola
	if (length(get_campo('__COLA_MOTOR__',xml2))=0) then
		--Voy a publicar igual
                xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
		--Si es falla de no existe lo registro
		if (falla1=2) then
			--Si ya existe no inserto
			--md5_input1:=md5(get_campo('INPUT',xml2));
			md5_input1:=md5(input_ori1);
			xml2:=put_campo(xml2,'MD5_INPUT',md5_input1);
			select * into stRechazado from dte_arm_rechazados where md5_input=md5_input1;
			if found then
				xml2:=logapp(xml2,'Ya existe en dte_arm_rechazados');
			else
				xml2:=logapp(xml2,'Graba en dte_arm_rechazados');
				insert into dte_arm_rechazados (fecha,xml_in,md5_input,rut_emisor,tipo_dte,folio) values (now(),xml2,md5_input1,rut_emisor1::bigint,tipo_dte1::integer,folio1::bigint);
			end if;
		end if;
		xml2:=logapp(xml2,'Respuesta = '||get_campo('RESPUESTA',xml2));
	end if;
	--xml2 := sp_procesa_respuesta_cola_motor(xml2);
	xml2 := put_campo(xml2,'__SECUENCIAOK__','1000');
        return xml2;
   end if;


   --20150225 FAY Guardamos el codigo_Txl para marcar el arm una vez que conteste el AML
   xml2:=put_campo(xml2,'CODIGO_TXEL_ARM',stRec.codigo_txel::varchar);
    xml2:=put_campo(xml2,'RUT_EMISOR_ARM',stRec.rut_emisor::varchar);
    xml2:=put_campo(xml2,'TIPO_DTE_ARM',stRec.tipo_dte::varchar);
    xml2:=put_campo(xml2,'FOLIO_ARM',stRec.folio::varchar);
   xml2:=put_campo(xml2,'URI_DTE',stRec.uri);
   xml2:=logapp(xml2,'CODIGO_TXEL_ARM='||get_campo('CODIGO_TXEL_ARM',xml2));


    xml2:=arma_scgi(xml2);

   --Si es CGE
   xml2:=put_campo(xml2,'RUT_CGE',get_campo('RUT_RECEPTOR',xml2));
   xml2:=verifica_evento_cge(xml2);
/*
*/
   if(split_part(split_part(split_part(input1,'<NombreDA>ReclamarDTE</NombreDA>',2),'<ValorDA>',2),'</ValorDA>',1)='SI' and stRec.tipo_dte::varchar in ('33','34','43')) then
	xml2:=logapp(xml2,'ARM: Vamos a realizar reclamo');
	xml2:=put_campo(xml2,'FLAG_RECLAMO_ARM','SI');
	xml2:=put_campo(xml2,'__FLAG_INFORMA_SII__','SI');
        --Siempre voy a revisar el reclamo primero
	xml2 := put_campo(xml2,'__SECUENCIAOK__','25');
   else 
	xml2:=logapp(xml2,'ARM: Arm sin reclamo');
	xml2:=put_campo(xml2,'FLAG_RECLAMO_ARM','NO');
	xml2:=put_campo(xml2,'__FLAG_INFORMA_SII__','NO');
   	if (get_campo('EVENTO_CGE',xml2)='SI') then
		xml2:=put_campo(xml2,'__IP_CONEXION_CLIENTE__','172.16.10.181');
        else
		xml2:=put_campo(xml2,'__IP_CONEXION_CLIENTE__',get_campo('__IP_CONEXION_CLIENTE__',get_parametros_motor('','EDTE')));
        end if;
	xml2 := put_campo(xml2,'__SECUENCIAOK__','31');
   end if;
   return xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION proc_procesa_respuesta_arm_12718(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2        varchar;
    --data1       varchar;
    resp1       varchar;
    sts         integer;
    texto_resp1 varchar;
    respuesta1  varchar;
    status1     varchar;
    md5_input1  varchar;
	codigo1	bigint;
    --stRec       dte_recibidos%ROWTYPE;
	stContribuyente	contribuyentes%ROWTYPE;
	rut1	varchar;	
	mail1	varchar;
BEGIN
    xml2:=xml1;
    --data1:=get_campo('INPUT',xml2);
    xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
    --xml2 := put_campo(xml2,'__SECUENCIAOK__','100');
    --Si hay respuesta del AML
    xml2:=logapp(xml2,'ARM: __EDTE_ARM_OK__='||get_campo('__EDTE_ARM_OK__',xml2));
    if (get_campo('__EDTE_ARM_OK__',xml2)='SI') then
	xml2:=put_campo(xml2,'RESPUESTA','200 OK'||chr(10)||'URL(True): '||get_campo('URI_IN',xml2));
	
	--2015-04-30 FAY,RME Se graba inmediatamente el Evento ARM para el DTE recibido
	rut1:=get_campo('RUT_EMISOR',xml2);
	select * into stContribuyente from contribuyentes where rut_emisor=rut1::integer;
	if not found then
		xml2:=logapp(xml2,'ARM: Rut Emisor del DTE Recibido no registrado en contribuyentes');
		mail1:='Sin mail de intercambio';
	else
		mail1:=stContribuyente.email;
	end if;
	xml2:=put_campo(xml2,'COMENTARIO_TRAZA','Recibe: '||mail1||chr(10)||get_campo('RECINTO',xml2)||'.');
	--Se asignan las uris para grabar el evento en traza
	xml2:=put_campo(xml2,'URL_GET',get_campo('URI_IN',xml2));
	xml2:=put_campo(xml2,'URI_IN',get_campo('URI_DTE',xml2));
	xml2:=graba_bitacora2(xml2,'ARM');
	--Vuelo a dejar en uri_in la uri del ARM
	xml2:=put_campo(xml2,'URI_IN',get_campo('URL_GET',xml2));

    else
	xml2:=put_campo(xml2,'RESPUESTA','400 NK');
    end if;

    --Limpio el INPUT para el LOG
    resp1:= get_campo('RESPUESTA',xml2);  
    xml2:=logapp(xml2,'ARM: Texto Respuesta AML='||resp1);
    --Si viene este texto entonces AML responde OK
    texto_resp1 := 'URL(True): '||get_campo('URI_IN',xml2);

    --Verifico si me fue bien con el AML
    --Debe contestar un OK y debe venir la URI que se envio a la entrada
    --if strpos(resp1,'200 OK')>0 then
    if (strpos(resp1,'200 OK')>0 and strpos(resp1,texto_resp1)>0) then
        --Un documento reprocesado, que se envia al AML, se puede borrar de la cola de procesamiento
        if (get_campo('_REPROCESO_',xml2)='SI') then
                xml2 := put_campo(xml2,'_ESTADO_REPROCESO_','OK');
        end if;

        xml2 := put_campo(xml2,'ESTADO','ENVIADO_EDTE');
        --xml2 := graba_bitacora(xml2,'ENVIADO_AML');
        xml2 := logapp(xml2,'ARM: FOLIO='||get_campo('FOLIO',xml2)||' RUT_EMISOR='||get_campo('RUT_EMISOR',xml2)||' TIPO_DTE='||get_campo('TIPO_DTE',xml2)||' ARM URI_DTE='||get_campo('URI_DTE',xml2));

        --Saco los datos que requiero de la respuesta
        xml2 := put_campo(xml2,'URI',get_tag_http(resp1,'URL(True): '));
    
/*
	--Actualizo Estado en dte_recibidos
	codigo1:=get_campo('CODIGO_TXEL_ARM',xml2)::bigint;
	update dte_recibidos set estado_arm='SI',uri_arm=get_campo('URI_IN',xml2) where codigo_txel=codigo1;
	*/
    else

        xml2 := logapp(xml2,'ARM: FOLIO='||get_campo('FOLIO',xml2)||' RUT_EMISOR='||get_campo('RUT_EMISOR',xml2)||' TIPO_DTE='||get_campo('TIPO_DTE',xml2)||' Falla ARM URI_DTE='||get_campo('URI_DTE',xml2));
        --xml2 := put_campo(xml2,'ESTADO','ERROR_AML');
        xml2 := logapp(xml2,resp1);
	--Si falla el AML, lo guardamos en una tabla y lo sacamos de la cola porque tiene la caga
	--insert into cola_rechazo_arm (fecha,xml_in) values (now(),xml2);
	--xml2:=logapp(xml2,'Forzamos respuesta OK');
	--resp1:='200 OK';
    end if;

    --TODO hacer un control cuando falle el update
    --xml2 := put_campo(xml2,'INPUT','');
    --respuesta1:=split_part(resp1,chr(10)||chr(10),2);
    if (strpos(resp1,'200 OK')>0) then
        status1:='Status: 200 OK'||chr(10)||
                 'Content-type: text/html'||chr(10)||
                 'Content-Location: '||get_campo('URI_IN',xml2)||chr(10)||
                 'Content-length: '||length(texto_resp1)||chr(10);
        xml2 := logapp(xml2,'ARM: Respuesta Servicio 200 OK ARM URI'||get_campo('URI_IN',xml1));
	xml2 := logapp(xml2,'ARM: Respuesta'||status1||chr(10)||texto_resp1);
        xml2:=put_campo(xml2,'RESPUESTA',status1||chr(10)||texto_resp1);
	--Si estoy reprocesando y esta en los rechazados los borro
	--md5_input1:=md5(get_campo('INPUT',xml2));
	md5_input1:=get_campo('MD5_INPUT',xml2);
	xml2:=logapp(xml2,'Se borra de dte_arm_rechazados');
	delete from dte_arm_rechazados where md5_input=md5_input1;
    else
        status1:='Status: 400 Rechazado'||chr(10)||'Content-type: text/html'||chr(10)||'Content-length: '||length(respuesta1)||chr(10);
        xml2 := logapp(xml2,'Respuesta Servicio 400 Rechazado (8010) URI'||get_campo('URI_IN',xml2));
    end if;

    xml2 := put_campo(xml2,'__SOCKET_RESPONSE__','RESPUESTA');
    --xml2 := sp_procesa_respuesta_cola_motor(xml2);
    xml2 := put_campo(xml2,'__SECUENCIAOK__','1000');
    --Respondo lo que viene
    RETURN xml2;
END;
$$ LANGUAGE plpgsql;


