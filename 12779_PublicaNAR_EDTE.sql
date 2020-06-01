--Publica documento
delete from isys_querys_tx where llave='12779';

insert into isys_querys_tx values ('12779',5,19,1,'select revisa_reclamo_sii_12779(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('12779',10,45,1,'select busco_dte_nar_12779 (''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('12779',15,19,1,'select envia_nar_12779 (''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('12779',20,45,1,'select actualizo_dte_12779 (''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('12779',30,19,1,'select revisa_respuesta_nar_12779(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);


--Revisa si hay reclamo, lo graba en las colas y saca datos del nar
CREATE or replace FUNCTION revisa_reclamo_sii_12779(json) RETURNS json AS $$
DECLARE
        json1   alias for $1;
        json2   json;
	data1	varchar;
	sts2	varchar;
	flag_reclamo	varchar;
	input1	varchar;
	tipo_dte1	varchar;
	campo3		record;
	uri1		varchar;
	xml3		varchar;
	rut_emisor1	varchar;
	folio1		varchar;
	cola1		varchar;
	nombre_tabla1	varchar;
	id1	bigint;
BEGIN
	json2:=json1;
        --NAR a ser enviado
        data1:=get_json('INPUT',json2);
	rut_emisor1:=split_part(get_xml_hex1('RUTEmisor',data1),'-',1);
	json2:=put_json(json2,'RUT_EMISOR',rut_emisor1);
	folio1:=get_xml_hex1('Folio',data1);
	json2:=put_json(json2,'FOLIO',folio1);
	tipo_dte1:=get_xml_hex1('TipoDTE',data1);
	json2:=put_json(json2,'TIPO_DTE',tipo_dte1);
	json2:=put_json(json2,'RUT_RECEPTOR',split_part(get_xml_hex1('RUTRecep',data1),'-',1));
	json2:=put_json(json2,'FECHA_EMISION',get_xml_hex1('FchEmis',data1));
	json2:=put_json(json2,'ESTADO_NAR',get_xml_hex1('EstadoDTE',data1));
	json2:=put_json(json2,'GLOSA_NAR',get_xml_hex1('EstadoDTEGlosa',data1));
	json2:=put_json(json2,'MONTO_NAR',get_xml_hex1('MntTotal',data1));
	
	sts2:=get_xml_hex1('EstadoDTE',data1);
	input1:=decode(data1,'hex');
        flag_reclamo:=split_part(split_part(split_part(input1,'<NombreDA>ReclamarDTE</NombreDA>',2),'<ValorDA>',2),'</ValorDA>',1);
	--DAO-2017-11-20 Si no es un DTE reclamable en el SII, solo hace el NAR
        if(flag_reclamo='SI' and tipo_dte1::varchar  in ('33','34','43','46')) then
        	json2:=put_json(json2,'FLAG_RECLAMO_NAR','SI');
        	json2:=put_json(json2,'__FLAG_INFORMA_SII__','SI');
		uri1:=get_json('URI_IN',json2);
	
	 	--Si ya esta insertado no lo procese
	        select * into campo3 from cola_sii_generica where uri=uri1 and categoria='RECLAMO_NAR';
	        if not found then
			xml3:='';
			xml3:=put_campo(xml3,'TX','16201');
			xml3:=put_campo(xml3,'RUT_EMISOR',rut_emisor1);
			xml3:=put_campo(xml3,'TIPO_DTE',tipo_dte1);
			xml3:=put_campo(xml3,'FOLIO',folio1);
                	if (sts2 in ('0','1')) then
				xml3:=put_campo(xml3,'EVENTO_RECLAMO','ACD');
			else
				xml3:=put_campo(xml3,'EVENTO_RECLAMO','RCD');
			end if;
			/*
			ACD - 0: Acepta Contenido del Documento
			RCD - 1: Reclamo al Contenido del Documento
			ERM - 2: Otorga Recibo de Mercaderías o Servicios
			RFP - 3: Reclamo por Falta Parcial de Mercaderías
			RFT - 4: Reclamo por Falta Total de Mercaderías
			*/
			--DAO 20181614 En el caso de que venga en en el XML el Dato Adjunto CodigoEventoReclamoSII - Tomamos ese. Para que un NAR pueda gatillar en el SII ERM,RFT,RFP que hoy no puede
        		flag_reclamo:=split_part(split_part(split_part(input1,'<NombreDA>CodigoEventoReclamoSII</NombreDA>',2),'<ValorDA>',2),'</ValorDA>',1);
			json2:=logjson(json2,'CodigoEventoReclamoSII='||flag_reclamo||' Valores Esperados (0,1,2,3,4)');
			if flag_reclamo in ('0','1','2','3','4') then
				if flag_reclamo='0' then
					xml3:=put_campo(xml3,'EVENTO_RECLAMO','ACD');
				elsif flag_reclamo='1' then
					xml3:=put_campo(xml3,'EVENTO_RECLAMO','RCD');
				elsif flag_reclamo='2' then
					xml3:=put_campo(xml3,'EVENTO_RECLAMO','ERM');
				elsif flag_reclamo='3' then
					xml3:=put_campo(xml3,'EVENTO_RECLAMO','RFP');
				elsif flag_reclamo='4' then
					xml3:=put_campo(xml3,'EVENTO_RECLAMO','RFT');
				end if;
				json2:=logjson(json2,'CodigoEventoReclamoSII Evento SII '||get_campo('EVENTO_RECLAMO',xml3));
			end if;
			
			cola1:=nextval('id_cola_sii');
			nombre_tabla1:='cola_sii_'||cola1::varchar;
			xml3:=put_campo(xml3,'FECHA_INGRESO_COLA',now()::varchar);
			execute 'insert into ' || nombre_tabla1 || ' (fecha,uri,reintentos,data,tx,rut_emisor,reproceso,categoria,nombre_cola,rut_receptor,tipo_dte,folio) values ( now(),'||quote_literal(uri1)||',0,'||quote_literal(xml3)||',5,'||quote_literal(get_json('RUT_RECEPTOR',json2))||',''NO'','||quote_literal('RECLAMO_NAR')||','||quote_literal(nombre_tabla1)||','||quote_literal(rut_emisor1)||','||quote_literal(tipo_dte1)||','||quote_literal(folio1)||') returning id' into id1;
			json2:=logjson(json2,'NAR: Grabo Reclamo en las Colas SII ID='||id1::varchar);
		else
			json2:=logjson(json2,'NAR: Reclamo ya existe en las colas sii ');
   		end if;
	else
		json2:=logjson(json2,'NAR: NO va al SII');
        	json2:=put_json(json2,'FLAG_RECLAMO_NAR','NO');
        	json2:=put_json(json2,'__FLAG_INFORMA_SII__','NO');
        end if;
	json2:=put_json(json2,'__SECUENCIAOK__','10');
	return json2;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION revisa_respuesta_nar_12779(varchar) RETURNS varchar AS $$
DECLARE
        xml1   alias for $1;
        xml2  	varchar; 
BEGIN
	xml2:=xml1;
	xml2:=put_campo(xml2,'__SECUENCIAOK__','0');
	if (get_campo('__EDTE_NAR_OK__',xml2)='SI') then
		xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
	else
		xml2:=put_campo(xml2,'RESPUESTA','Status: 444 FALLA NAR');
	end if;
	xml2 := put_campo(xml2,'__SOCKET_RESPONSE__','RESPUESTA');
	xml2 := sp_procesa_respuesta_cola_motor88(xml2);
        return xml2;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION  busco_dte_nar_12779(json) RETURNS json AS $$
DECLARE
        json1   alias for $1;
        json2   json;
        total1  integer;
        uri1    varchar;
        hash1   varchar;
        ver_dcto_attrib varchar;
        html1   varchar;
        --data1   varchar;
        largo1  integer;
        pos_inicial1    integer;
        pos_final1      integer;
        mail1   varchar;
        campo   record;
        campo1   record;
        json4   json;
        sts1    varchar;
        pos     integer;
        subject1        varchar;
        json_out1       json;
        comentario1     varchar;
        xml3    varchaR;
        cola1   varchar;
        nombre_tabla1   varchar;
        tx1     varchar;
        id1     varchar;
        aux1    varchar;
        comentario_traza1       varchar;
        json3   json;
        jsonsts1        json;
        i       integer;
        xml4    varchar;
        url_get1        varchar;
        data_lma        varchar;
        json_par1       json;
        rut1    varchar;
        rut_receptor1   varchar;
        folio1  varchar;
        data2   varchar;
        tipo_dte1       varchar;
        uri_dte1        varchar;
        fecha_emi1      varchar;
        sts2            varchar;
        estado_nar1     varchar;
        codigo_txel1    varchar;
	json_reclamo	json;
	flag_reclamo	varchar;
	input1	varchar;
	port	varchar;
	json_in	json;
BEGIN
        json2:=json1;

	if (get_json('__FLAG_PUB_10K__',json2)='SI') then
        	json2:=put_json(json2,'__SECUENCIAOK__','0');
	else
        	json2:=put_json(json2,'__SECUENCIAOK__','30');
	end if;
	json2:=logjson(json2,'URI_NAR='||get_json('URI_IN',json2));
        --NAR a ser enviado
        rut1:=get_json('RUT_EMISOR',json2);
        folio1:=get_json('FOLIO',json2);
        tipo_dte1:=get_json('TIPO_DTE',json2);
        rut_receptor1:=get_json('RUT_RECEPTOR',json2);
        fecha_emi1:=get_json('FECHA_EMISION',json2);
        if (length(folio1)=0) then
                json2:=logjson(json2,'Si no hay Folio , no se acepta el NAR');
		json2:=put_json(json2,'__EDTE_NAR_OK__','SI');
                return json2;
        end if;
	--Si el RUT_EMISOR no es numerico borre el DTE
   	if (is_number(rut1) is false) then
                json2:=logjson(json2,'Se borra DTE, rut_emisor no numerico (NAR)');
		json2:=put_json(json2,'__EDTE_NAR_OK__','SI');
                return json2;
   	end if;
	--Si no viene tipo_dte no se puede generar el NAR
	if (is_number(tipo_dte1) is false) then
                json2:=logjson(json2,'Se borra DTE, tipo_dte no numerico (NAR)');
		json2:=put_json(json2,'__EDTE_NAR_OK__','SI');
                return json2;
   	end if;

	json2:=logjson(json2,'URI_DTE='||get_json('URI_DTE',json2)||' CODIGO_TXEL_NAR='||get_json('CODIGO_TXEL_NAR',json2));
        --Valido si trae URI_DTE, si viene es porque viene des escritoriom, sino validamos el DTE para contestar
        if (get_json('URI_DTE',json2)='' or is_number(get_json('CODIGO_TXEL_NAR',json2)) is false) then
                --Buscamos el dte recibido
                SELECT * into campo1 FROM dte_recibidos WHERE rut_emisor=rut1::integer and tipo_dte=tipo_dte1::integer and folio=folio1::integer;
                if not found then
                        json2:=logjson(json2,'Si no hay DTE recibido, no se acepta el NAR rut_emisor='||rut1||' tipo_dte='||tipo_dte1||' folio='||folio1);
			json2:=put_json(json2,'__EDTE_NAR_OK__','SI');
                        return json2;
		else
			json2:=logjson(json2,'Encuentra Dte recibido para recibir el NAR');
                end if;
                uri_dte1:=campo1.uri;
		json2:=put_json(json2,'URI_DTE',uri_dte1);
                codigo_txel1:=campo1.codigo_txel;
		json2:=put_json(json2,'CODIGO_TXEL_NAR',codigo_txel1);
		json2:=logjson(json2,'CODIGO_TXEL_NAR='||codigo_txel1::varchar);
	else
		json2:=logjson(json2,'No entra a buscar DTE');
        end if;

	-- NBV 20170405 DAO
        if(codigo_txel1 is null) then
		json2:=logjson(json2,'codigo_txel1 nulo');
                codigo_txel1:=get_json('CODIGO_TXEL_NAR',json2);
        end if;

	--DEBUG
	select data_dte into aux1 from dte_recibidos where codigo_txel=codigo_txel1::bigint;
	if found then
		json2:=logjson(json2,'DATA_DTE='||coalesce(aux1,'NULO'));
	end if;

	--Actualizo el origen en dte_recibidos si no lo tiene
	update dte_recibidos set data_dte=put_data_dte(data_dte,'ORINAR',get_json('__ORIGEN__',json2)||'-'||get_json('__FLAG_INFORMA_SII__',json2)) where codigo_txel=codigo_txel1::bigint and (strpos(coalesce(data_dte,''),'<ORINAR>')=0 or strpos(coalesce(data_dte,''),'<ORINAR><')>0);
	if found then
		json2:=logjson(json2,'Actualiza origen de NAR '||codigo_txel1::varchar||' '||get_json('URI_IN',json2));
	else
		json2:=logjson(json2,'No Actualiza origen de NAR '||codigo_txel1::varchar||' '||get_json('URI_IN',json2));
	end if;

        sts2:=get_json('ESTADO_NAR',json2);
        -- NBV 20170321

        uri1:=get_json('URI_IN',json2);
        if (length(uri1)=0) then
		xml3:='';
        	xml3:=put_campo(xml3,'__FLUJO_ACTUAL__',get_json('__FLUJO_ACTUAL__',json2));
        	xml3:=put_campo(xml3,'RUT_EMISOR',get_json('RUT_EMISOR',json2));
	        xml3:=put_campo(xml3,'RUT_OWNER',get_json('RUT_RECEPTOR',json2));
        	xml3:=put_campo(xml3,'FECHA_EVENTO',now()::varchar);
	        xml3:=put_campo(xml3,'RUT_RECEPTOR',get_json('RUT_RECEPTOR',json2));
        	xml3:=put_campo(xml3,'FOLIO',get_json('FOLIO',json2));
	        xml3:=put_campo(xml3,'TIPO_DTE',get_json('TIPO_DTE',json2));
	        xml3:=put_campo(xml3,'CANAL','RECIBIDOS');
        	xml3:=put_campo(xml3,'URI_IN',get_json('URI_DTE',json2));
        	xml3:=put_campo(xml3,'EVENTO','ERROR_NAR');
		xml3:=put_campo(xml3,'COMENTARIO_TRAZA','NAR mal generado uri vacia');
		xml3:=graba_bitacora(xml3,'ERROR_NAR');
		json2:=logjson(json2,get_campo('_LOG_',xml3));	
                json2:=logjson(json2,'Uri NAR vacia');
                return json2;
        end if;

        select * into campo from contribuyentes where rut_emisor=rut1::integer;
        if not found then
		xml3:='';
        	xml3:=put_campo(xml3,'__FLUJO_ACTUAL__',get_json('__FLUJO_ACTUAL__',json2));
        	xml3:=put_campo(xml3,'RUT_EMISOR',get_json('RUT_EMISOR',json2));
	        xml3:=put_campo(xml3,'RUT_OWNER',get_json('RUT_RECEPTOR',json2));
        	xml3:=put_campo(xml3,'FECHA_EVENTO',now()::varchar);
	        xml3:=put_campo(xml3,'RUT_RECEPTOR',get_json('RUT_RECEPTOR',json2));
        	xml3:=put_campo(xml3,'FOLIO',get_json('FOLIO',json2));
	        xml3:=put_campo(xml3,'TIPO_DTE',get_json('TIPO_DTE',json2));
	        xml3:=put_campo(xml3,'CANAL','RECIBIDOS');
        	xml3:=put_campo(xml3,'URI_IN',get_json('URI_DTE',json2));
        	xml3:=put_campo(xml3,'EVENTO','ERROR_NAR');
		xml3:=put_campo(xml3,'COMENTARIO_TRAZA','Receptor no registrado en tabla de contribuyentes');
		xml3:=graba_bitacora(xml3,'ERROR_NAR');
		json2:=logjson(json2,get_campo('_LOG_',xml3));	
                json2:=logjson(json2,'Receptor no registrado en tabla de contribuyentes');
                return json2;
        end if;
	json2:=put_json(json2,'CORREO_EMISOR',campo.email);
	json2:=put_json(json2,'NOMBRE_EMISOR',campo.nombre);
        select * into campo1 from contribuyentes where rut_emisor=rut_receptor1::integer;
        if not found then
		xml3:='';
        	xml3:=put_campo(xml3,'__FLUJO_ACTUAL__',get_json('__FLUJO_ACTUAL__',json2));
        	xml3:=put_campo(xml3,'RUT_EMISOR',get_json('RUT_EMISOR',json2));
	        xml3:=put_campo(xml3,'RUT_OWNER',get_json('RUT_RECEPTOR',json2));
        	xml3:=put_campo(xml3,'FECHA_EVENTO',now()::varchar);
	        xml3:=put_campo(xml3,'RUT_RECEPTOR',get_json('RUT_RECEPTOR',json2));
        	xml3:=put_campo(xml3,'FOLIO',get_json('FOLIO',json2));
	        xml3:=put_campo(xml3,'TIPO_DTE',get_json('TIPO_DTE',json2));
	        xml3:=put_campo(xml3,'CANAL','RECIBIDOS');
        	xml3:=put_campo(xml3,'URI_IN',get_json('URI_DTE',json2));
        	xml3:=put_campo(xml3,'EVENTO','ERROR_NAR');
		xml3:=put_campo(xml3,'COMENTARIO_TRAZA','Sin mail de intercambio en tabla de contribuyentes');
		xml3:=graba_bitacora(xml3,'ERROR_NAR');
		json2:=logjson(json2,get_campo('_LOG_',xml3));	
                json2:=logjson(json2,'Sin mail de intercambio RUT_RECEPTOR');
                return json2;
        end if;
	json2:=put_json(json2,'CORREO_RECEPTOR',campo1.email);
	json2:=put_json(json2,'NOMBRE_RECEPTOR',campo1.nombre);

        json2:=logjson(json2,'Mail NAR='||campo.email);
        --Validamos el correo
        if (valida_email(campo.email) is false) then
		xml3:='';
        	xml3:=put_campo(xml3,'__FLUJO_ACTUAL__',get_json('__FLUJO_ACTUAL__',json2));
        	xml3:=put_campo(xml3,'RUT_EMISOR',get_json('RUT_EMISOR',json2));
	        xml3:=put_campo(xml3,'RUT_OWNER',get_json('RUT_RECEPTOR',json2));
        	xml3:=put_campo(xml3,'FECHA_EVENTO',now()::varchar);
	        xml3:=put_campo(xml3,'RUT_RECEPTOR',get_json('RUT_RECEPTOR',json2));
        	xml3:=put_campo(xml3,'FOLIO',get_json('FOLIO',json2));
	        xml3:=put_campo(xml3,'TIPO_DTE',get_json('TIPO_DTE',json2));
	        xml3:=put_campo(xml3,'CANAL','RECIBIDOS');
        	xml3:=put_campo(xml3,'URI_IN',get_json('URI_DTE',json2));
        	xml3:=put_campo(xml3,'EVENTO','ERROR_NAR');
		xml3:=put_campo(xml3,'COMENTARIO_TRAZA','Mail Invalido en contribuyentes '||campo.email);
		xml3:=graba_bitacora(xml3,'ERROR_NAR');
		json2:=logjson(json2,get_campo('_LOG_',xml3));	
                json2:=logjson(json2,'Mail Invalido '||campo.email);
                return json2;
        end if;

        json2:=put_json(json2,'__SECUENCIAOK__','15');
        return json2;
END;
$$ LANGUAGE plpgsql;




CREATE or replace FUNCTION  envia_nar_12779(json) RETURNS json AS $$
DECLARE
        json1   alias for $1;
        json2   json;
        total1  integer;
        uri1    varchar;
        hash1   varchar;
        ver_dcto_attrib varchar;
        html1   varchar;
        --data1   varchar;
        largo1  integer;
        pos_inicial1    integer;
        pos_final1      integer;
        mail1   varchar;
        campo   record;
        campo1   record;
        json4   json;
        sts1    varchar;
        pos     integer;
        subject1        varchar;
        json_out1       json;
        comentario1     varchar;
        xml3    varchaR;
        cola1   varchar;
        nombre_tabla1   varchar;
        tx1     varchar;
        id1     varchar;
        aux1    varchar;
        comentario_traza1       varchar;
        json3   json;
        jsonsts1        json;
        i       integer;
        xml4    varchar;
        url_get1        varchar;
        data_lma        varchar;
        json_par1       json;
        rut1    varchar;
        rut_receptor1   varchar;
        folio1  varchar;
        data2   varchar;
        tipo_dte1       varchar;
        uri_dte1        varchar;
        fecha_emi1      varchar;
        sts2            varchar;
        estado_nar1     varchar;
        codigo_txel1    varchar;
	mail_emisor1	varchar;
	mail_receptor1	varchar;
	nombre_emisor1	varchar;
	nombre_receptor1	varchar;
	tmp1	varchar;
	tmp2	varchar;
BEGIN
        json2:=json1;
	if (get_json('__FLAG_PUB_10K__',json2)='SI') then
        	json2:=put_json(json2,'__SECUENCIAOK__','0');
	else
        	json2:=put_json(json2,'__SECUENCIAOK__','30');
	end if;
        --NAR a ser enviado
        --data1:=get_json('INPUT',json2);
        rut1:=get_json('RUT_EMISOR',json2);
        folio1:=get_json('FOLIO',json2);
        tipo_dte1:=get_json('TIPO_DTE',json2);
        rut_receptor1:=get_json('RUT_RECEPTOR',json2);
        fecha_emi1:=get_json('FECHA_EMISION',json2);
	uri_dte1:=get_json('URI_DTE',json2);
	codigo_txel1:=get_json('CODIGO_TXEL_NAR',json2);
	uri1:=get_json('URI_IN',json2);	
	mail_emisor1:=get_json('CORREO_EMISOR',json2);
	mail_receptor1:=get_json('CORREO_RECEPTOR',json2);
	nombre_emisor1:=get_json('NOMBRE_EMISOR',json2);
	nombre_receptor1:=get_json('NOMBRE_RECEPTOR',json2);

        json2:=logjson(json2,'Casilla NAR --> '||mail_emisor1||' URI='||uri1||' URI_DTE='||uri_dte1);
        sts2:=get_json('ESTADO_NAR',json2);
        json4:='{}';
        if (sts2 in ('0','1')) then
                hash1 := encripta_hash_evento_VDC('uri='||uri_dte1||'&owner='||rut_receptor1||'&rutEmisor='||rut1||'&tipoDTE='||tipo_dte1||'&folio='||folio1||'&mail='||trim(mail_emisor1)||'&type=LNRE'||'&rutRecep='||rut_receptor1||'&fchEmis='||fecha_emi1||'&relatedUrl=&comment=Mail Leído por '||trim(mail_emisor1)||'&');
                if (sts2='0') then
                        json4:=put_json(json4,'ESTADO_DTE','ACEPTADO OK');
                else
                        json4:=put_json(json4,'ESTADO_DTE','ACEPTADA CON DISCREPANCIA');
                end if;
        else
                hash1 := encripta_hash_evento_VDC('uri='||uri_dte1||'&owner='||rut_receptor1||'&rutEmisor='||rut1||'&tipoDTE='||tipo_dte1||'&folio='||folio1||'&mail='||trim(mail_emisor1)||'&type=LRRE'||'&rutRecep='||rut_receptor1||'&fchEmis='||fecha_emi1||'&relatedUrl=&comment=Mail Leído por '||trim(mail_emisor1)||'&');
                json4:=put_json(json4,'ESTADO_DTE','RECHAZADO');
        end if;

        --Enviamos el NAR
        json4:=put_json(json4,'GLOSA_DTE',get_json('GLOSA_NAR',json2));
        json4:=put_json(json4,'RAZON_EMISOR',nombre_emisor1);
        json4:=put_json(json4,'RAZON_RECEPTOR',nombre_receptor1);
        --Case
        json_par1:=get_parametros_motor_json('{}','SERVIDOR_CORREO');

        json4:=put_json(json4,'uri',uri1);
        json4:=put_json(json4,'FECHA_EMISION',fecha_emi1);
        json4:=put_json(json4,'TIPO_DTE',tipo_dte1);
        json4:=put_json(json4,'FOLIO',folio1);
        json4:=put_json(json4,'flag_data_xml','NO');
        json4:=put_json(json4,'RUT_EMISOR_DV',rut1||'-'||modulo11(rut1));
        json4:=put_json(json4,'RUT_RECEPTOR_DV',rut_receptor1||'-'||modulo11(rut_receptor1));
        json4:=put_json(json4,'IMG_LECTURA','<img style="display: none;" src="'||get_json('__VALOR_PARAM__',json_par1)||'?hash='||hash1||'&"/>');
        --json4:=put_json(json4,'IMG_LECTURA','<img style="display: none;" src="http://servicios.acepta.com/traza?hash='||hash1||'&"/>');
        json4:=put_json(json4,'TITULO','Producción Intercambio Revisión Comercial de DTE');
        json4:=put_json(json4,'MONTO_TOTAL','$ '||get_json('MONTO_NAR',json2));

        html1:=pg_read_file('./patron_dte_10k/patron_nar.html');
        html1:=remplaza_tags_json_c(json4,html1);
        json4:=put_json(json4,'content_html',encode(html1::bytea,'hex'));
        --Solo se envia el RespuestaDTE
	tmp1:=encode('<RespuestaDTE','hex');
	tmp2:=encode('</RespuestaDTE>','hex');
        data2:=encode(('<?xml version="1.0" encoding="ISO-8859-1"?>'||chr(10))::bytea,'hex')||tmp1||split_part(split_part(get_json('INPUT',json2),tmp1,2),tmp2,1)||tmp2;
        json4:=put_json(json4,'INPUT_CUSTODIUM',data2);
        --json4:=put_json(json4,'adjunta_xml','SI');
        json4:=put_json(json4,'adjunta_attach','SI');
        json4:=put_json(json4,'nombre_xml','Notificacion_Aprobacion_o_Rechazo_'||folio1::varchar);
        json4:=put_json(json4,'RUT_RECEPTOR',rut_receptor1);
        json4:=put_json(json4,'subject_hex',encode('Revisión Comercial de DTEs -','hex'));
        json4:=put_json(json4,'from',mail_receptor1);
        json4:=put_json(json4,'to',mail_emisor1);
        --json4:=put_json(json4,'bcc','fernando.arancibia@acepta.com');
        json4:=put_json(json4,'tipo_envio','HTML');

        --json4:=put_json(json4,'return_path','confirmacion_envio@custodium.com');
        --json4:=put_json(json4,'ip_envio','172.16.10.185');
        json4:=put_json(json4,'return_path',get_json('PARAMETRO_RUTA',json_par1));
        json4:=put_json(json4,'ip_envio',get_json('__IP_CONEXION_CLIENTE__',json_par1));
        comentario_traza1:='Recibe: '||mail_emisor1||chr(10)||get_json('GLOSA_NAR',json2);

	if get_json('DESC_ORIGEN',json2)<>'' then
		json2:=logjson(json2,'NAR: Concateno DESC_ORIGEN en comentario_traza1');
		json2:=logjson(json2,'DESC_ORIGEN='||get_json('DESC_ORIGEN',json2)||' '||get_json('URI_DTE',json2));
		comentario_traza1:=comentario_traza1||chr(10)||get_json('DESC_ORIGEN',json2);
	end if;
        --json4:=put_json(json4,'url_traza','http://servicios.acepta.com/traza');
        json4:=put_json(json4,'url_traza',get_json('__VALOR_PARAM__',json_par1));

        json4:=put_json(json4,'uri_dte',uri_dte1);
        json4:=put_json(json4,'CANAL','RECIBIDOS');

        if (sts2 in ('0','1')) then
                json4:=put_json(json4,'evento_ema','<trace source="ENVIA_NAR" version="1.1"><node name="NRE" stamp="'||to_char(now(),'YYYY-MM-DD')||'T'||to_char(now(),'HH24:MI:SS')||'" owner="' ||get_json('RUT_RECEPTOR_DV',json4)||'"><keys><key name="rutEmisor" value="'||get_json('RUT_EMISOR_DV',json4)||'"/><key name="tipoDTE" value="'||tipo_dte1||'"/><key name="folio" value="'||folio1||'"/><key name="fchEmis" value="'||fecha_emi1||'"/></keys><attrs><attr key="code">'||tipo_dte1||'</attr><attr key="url">'||uri_dte1||'</attr><attr key="relatedUrl">'||uri1||'</attr><attr key="orig">'||get_json('RUT_EMISOR_DV',json4)||'</attr><attr key="dest">'||get_json('RUT_RECEPTOR_DV',json4)||'</attr><attr key="tag">'||folio1||'</attr><attr key="data"></attr><attr key="comment">'||decode_utf8(comentario_traza1)||'</attr></attrs></node></trace>');
                json4:=put_json(json4,'evento_confirmacion','ENRE');
		--Se envian los 2 eventos para guardar en la conf de la traza
                json4:=put_json(json4,'eok','ONRE');
                json4:=put_json(json4,'enk','FNRE');
                json4:=put_json(json4,'evento_confirmacion','');
                estado_nar1:='NAR_APROBADO';
        else
                json4:=put_json(json4,'evento_ema','<trace source="ENVIA_NAR" version="1.1"><node name="RRE" stamp="'||to_char(now(),'YYYY-MM-DD')||'T'||to_char(now(),'HH24:MI:SS')||'" owner="' ||get_json('RUT_RECEPTOR_DV',json4)||'"><keys><key name="rutEmisor" value="'||get_json('RUT_EMISOR_DV',json4)||'"/><key name="tipoDTE" value="'||tipo_dte1||'"/><key name="folio" value="'||folio1||'"/><key name="fchEmis" value="'||fecha_emi1||'"/></keys><attrs><attr key="code">'||tipo_dte1||'</attr><attr key="url">'||uri_dte1||'</attr><attr key="relatedUrl">'||uri1||'</attr><attr key="orig">'||get_json('RUT_EMISOR_DV',json4)||'</attr><attr key="dest">'||get_json('RUT_RECEPTOR_DV',json4)||'</attr><attr key="tag">'||folio1||'</attr><attr key="data"></attr><attr key="comment">'||decode_utf8(comentario_traza1)||'</attr></attrs></node></trace>');
                json4:=put_json(json4,'eok','ORRE');
                json4:=put_json(json4,'enk','FRRE');
                json4:=put_json(json4,'evento_confirmacion','');
                --json4:=put_json(json4,'evento_confirmacion','ERRE');
                estado_nar1:='NAR_RECHAZADO';
        end if;
	id1:='ACP'||encripta_hash_evento_VDC(rut1||'##'||tipo_dte1||'##'||folio1||'##'||fecha_emi1||'##'||uri_dte1||'####RECIBIDOS##'||rut_receptor1||'##'||get_json('eok',json4)||'##'||get_json('enk',json4));
       	--Generamos un id para confirmar la lectura de correo
        --id1:=md5(now()::varchar)||nextval('id_confirmacion_mail');
        json4:=put_json(json4,'msg_id','<'||id1||'@motor2.acepta.com>');

        jsonsts1:=send_mail_python2(json4::varchar);
        if (get_json('status',jsonsts1)='OK') then
                json2:=put_json(json2,'__EDTE_NAR_OK__','SI');
                --perform logfile('send_mail_python1 html2='||get_json('html2',jsonsts1));
                --perform logfile('send_mail_python1 html='||get_json('html',jsonsts1));
                --Si envie correctamente, inserto en confirmacion_mail para generar el evento de envio
                json4:=put_json(json4,'INPUT_CUSTODIUM','');
                json4:=put_json(json4,'evento_ema','');
                json4:=put_json(json4,'content_html','');
                json4:=put_json(json4,'IMG_LECTURA','');
                --insert into confirmacion_mail (id,json_data) values (id1,json4);
        	json2:=put_json(json2,'__SECUENCIAOK__','20');
		json2:=put_json(json2,'ESTADO_NAR',estado_nar1);
		json2:=put_json(json2,'MENSAJE_NAR',comentario_traza1);
		json2:=put_json(json2,'URI_NAR',uri1);
		
                --update dte_recibidos set uri_nar=uri1,mensaje_nar=comentario_traza1,estado_nar=estado_nar1,fecha_nar=now() where codigo_txel=codigo_txel1::bigint;
        else
                json2:=logjson(json2,'Falla Mail '||jsonsts1::varchar);
                json2:=put_json(json2,'__EDTE_OK__','NO');
                json2:=put_json(json2,'__EDTE_NAR_OK__','NO');
        end if;
    return json2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION  actualizo_dte_12779(json) RETURNS json AS $$
DECLARE
        json1   alias for $1;
        json2   json;
        id1     varchar;
        codigo_txel1    varchar;
BEGIN
        json2:=json1;
	if (get_json('__FLAG_PUB_10K__',json2)='SI') then
        	json2:=put_json(json2,'__SECUENCIAOK__','0');
	else
        	json2:=put_json(json2,'__SECUENCIAOK__','30');
	end if;
        json2:=put_json(json2,'__EDTE_OK__','SI');
        json2:=put_json(json2,'__EDTE_NAR_OK__','SI');
    return json2;
END;
$$ LANGUAGE plpgsql;

