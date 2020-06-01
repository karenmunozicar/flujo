--Publica documento
delete from isys_querys_tx where llave='12773';

--Arma lista de mails y genera contador en 0
insert into isys_querys_tx values ('12773',10,19,1,'select solicita_id_ecm_12773(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

--Armo correo para el mail[contador]
insert into isys_querys_tx values ('12773',20,9,1,'select verifica_envio_12773(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('12773',1000,1,1,'select sp_procesa_respuesta_cola_motor_original(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,0,0);

CREATE or replace FUNCTION define_secuencia_12773(varchar) RETURNS json AS $$
DECLARE
        json1                   alias for $1;
        json2                   json;
	json3		json;
BEGIN
        json2:=json1::json;
	json3:=split_part(get_json('RESPUESTA',json2),chr(10)||chr(10),2)::json;
        if (get_json('__COLA_MOTOR__',json2)<>'') then
                if (get_json('CODIGO_RESPUESTA',json3)='1') then
                        json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
                else
                        json2:=put_json(json2,'RESPUESTA','Status: 400 NK');
                end if;
                json2:=put_json(json2,'__SECUENCIAOK__','1000');
        else
                json2:=put_json(json2,'__SECUENCIAOK__','0');
        end if;
        return json2;
END;
$$ LANGUAGE plpgsql;



--Funcion para pintar cuando aprete el Formulario Web
CREATE or replace FUNCTION datos_formulario_Web_12773(varchar,varchar) RETURNS json
AS $$
DECLARE
        hash1   alias for $1;
	j3	json;
	jout	json;
	id1	bigint;
	campo	record;
	frm1	alias for $2;
	anomes1	varchar;
	campo_cd record;
BEGIN
	jout:='{}';	
	j3:=desencripta_hash_evento_vdc2(hash1)::json;
	id1:=get_json('id_fin',j3)::bigint;
	
	--Leo la tabla financiamiento_solicitudes
	select * into campo from financiamiento_solicitudes where id=id1;
	if not found then 
		jout:=put_json(jout,'__URL_REDIRECT__','https://escritorio.acepta.com/mensaje.php?mensaje='||encode('Solicitud no Existe','hex'));
		return put_json_list('[]',jout::varchar);
	end if;
	if ((campo.estado<>'SOLICITADO' and frm1<>'PAGO') or (frm1='PAGO' and campo.estado<>'CEDIDA')) then
	--if (campo.estado not in ('SOLICITADO','CEDIDA')) then
		jout:=put_json(jout,'__URL_REDIRECT__','https://escritorio.acepta.com/mensaje.php?mensaje='||encode('Solicitud ya no esta disponible','hex'));
		return put_json_list('[]',jout::varchar);
	end if;
	
	anomes1:=(to_char(campo.fecha_ingreso,'YYYY')||'-'||to_char(campo.fecha_ingreso,'MM')::integer::varchar);
	--anomes1:=(to_char(now(),'YYYY')||'-'||to_char(now(),'MM')::integer::varchar);
	select * into campo_cd from cd_lista_carpeta_legal where categoria='CARPETA_TRIBUTARIA' and rut=campo.rut_emisor::integer and nro_cliente=campo.rut_usuario_cliente::varchar and subcategoria=anomes1;
	if found then
		jout:=put_json(jout,'URI_CARPETA',campo_cd.url);
	else
		jout:=put_json(jout,'URI_CARPETA','HIDDEN');
	end if;
	
	--jout:=put_json(jout,'RAZON_SOCIAL',(select nombre from contribuyentes where rut_emisor=campo.rut_emisor limit 1));
	jout:=put_json(jout,'EMISOR','<b>Emisor:</b> '||(select nombre from contribuyentes where rut_emisor=campo.rut_emisor limit 1)||' ('||campo.rut_emisor::varchar||'-'||modulo11(campo.rut_emisor::varchar)||')');
	jout:=put_json(jout,'TIPO','<b>Tipo:</b> '||campo.tipo_dte::varchar);
	jout:=put_json(jout,'FOLIO','<b>Folio:</b> '||campo.folio::varchar);
	jout:=put_json(jout,'FECHA_PAGO_CLIENTE','<b>Fecha Pago Cliente:</b> '||campo.fecha_pago_cliente::varchar);
	jout:=put_json(jout,'CONTACTO','<b>Contacto:</b> '||(select nombre||'   <b>Mail:</b> '||coalesce(mail,'')||'   <b>Fono:</b> '||coalesce(fono,'') from user_10k where rut_usuario =campo.rut_usuario_cliente));
	jout:=put_json(jout,'URI',campo.uri);
	jout:=put_json(jout,'URI_TRAZA',replace(campo.uri,'/v01/','/traza/'));
	return put_json_list('[]',jout::varchar);

	--juri:=put_json('{}','id_fin',id1::varchar);
        --juri:=put_json(juri,'codigo_txel',get_json('codigo_txel',jemi)::varchar);
        --hash1:=encripta_hash_evento_VDC(juri::varchar);
END
$$
LANGUAGE plpgsql;


CREATE or replace FUNCTION pivote_financiamiento_12773(json) RETURNS json
AS $$
DECLARE
	json1	alias for $1;
	json2	json;
	json3   json;
	j2	json;
        patron1 varchar;
        patron2 varchar;
        sts2    varchar;
        query1  varchar;
        fecha_desde1    timestamp;
        jsonsts1        json;
        subject1        varchar;
        rut1    varchar;
        tipo1   varchar;
        folio1  varchar;
        campo   financiamiento_solicitudes%ROWTYPE;
        campo1  dte_emitidos%ROWTYPE;
	campox	record;
        id1     bigint;
        data_lma        varchar;
        id_ecm1 varchar;
        juri    json;
        uri_short1      varchar;
        hash1           varchar;
        j3      json;
	jemi	json;
	fin1	record;
	rut_aux1	bigint;
	date1	date;
	campo_cd record;
	rut_usu1	varchar;
	anomes1	varchar;
	uri_cd1	varchar;
	cola1   varchar;
        nombre_tabla1   varchar;
	xml2	varchar;
	flag_prueba1	varchar;
	campo_usu1	record;
BEGIN
	json2:=json1;
	json2:=put_json(json2,'LLAMA_FLUJO','SI');
        rut1:=split_part(replace(get_json('EMISOR',json2),'.',''),'-',1);
        tipo1:=get_json('TIPO',json2);
        folio1:=replace(get_json('FOLIO',json2),'.','');
	rut_usu1:=get_json('rutUsuario',json2);
	anomes1:=(to_char(now(),'YYYY')||'-'||to_char(now(),'MM')::integer::varchar);

	json2:=logjson(json2,'Entra a pivote_financiamiento_12773');
	if (get_json('check_tyc',json2)<>'on') then
		return response_requests_6000('2','Debe aceptar los Términos y Condiciones.','',json2);
	end if;

	/*
	select * into fin1 from financiamiento_financiadores where codigo=get_json('financiador',json2);
	if not found then
		return response_requests_6000('2','Financiador no registrado.','',json2);
	end if;
	*/
	BEGIN
		date1:=get_json('fecha_pago',json2)::date;
	EXCEPTION WHEN OTHERS THEN
		return response_requests_6000('2','Por favor ingrese correctamente la fecha de pago.','',json2);
	END;

	--json2:=put_json(json2,'para',fin1.correo);

	json2:=logjson(json2,'Entra a pivote_financiamiento_12773 1');
        --Verificamos si no existe una cotizacion enviada
        select *  into campo from financiamiento_solicitudes where rut_emisor=rut1::integer and tipo_dte=tipo1::integer and folio=folio1::bigint;
        if found then
                if campo.estado='SOLICITADO' then
                        return response_requests_6000('2','Ya existe una solicitud pendiente de financiamiento','',json2);
                end if;
        end if;
	select * into campo_usu1 from user_10k where rut_usuario=rut_usu1;
	if not found then
		return response_requests_6000('2','Falla Obtención Datos de Contacto','',json2);	
	end if;
	json2:=put_json(json2,'mail_usuario',campo_usu1.mail);
	json2:=put_json(json2,'fono_usuario',campo_usu1.fono);
	json2:=put_json(json2,'movil_usuario',campo_usu1.movil);
	--Verificamos si tiene la carpeta tributaria del mes
--	if get_json('rol_usuario',json2)<>'Sistemas' then
		select * into campo_cd from cd_lista_carpeta_legal where categoria='CARPETA_TRIBUTARIA' and rut=rut1::integer and nro_cliente=rut_usu1 and subcategoria=anomes1;
		if not found then
			uri_cd1:=coalesce((select remplaza_tags_6000(href,json2) from menu_info_10k where id2='casilla_legal'),'');
			--return response_requests_6000('2','La Empresa aún no tiene su carpeta legal del mes actual en nuestros registros '||anomes1,'{"URL_RESPUESTA_BLANK":"'||uri_cd1||'"}',json2);
			json2:=put_json(json2,'MSG_CARPETA',chr(10)||chr(10)||'Estimado Cliente,'||chr(10)||'Su empresa no tiene la carpeta tributaria eléctronica del mes actual en nuestros registros.'||chr(10)||'Esta información facilita la evaluación por parte de nuestros financiadores asociados.'||chr(10)||'Recomendamos que la actualice en su Casilla Legal en el menú de Financiamiento.');
			--return response_requests_6000('2','Estimado Cliente,'||chr(10)||'La empresa no tiene su carpeta tributaria eléctronica del mes actual en nuestros registros.'||chr(10)||'Esta información es necesaria para que el financista pueda realizar la evaluación correspondiente.'||chr(10)||'Le pedimos que la actualice en su Casilla Legal para continuar con la cotización. ','{"URL_RESPUESTA_BLANK":"'||uri_cd1||'"}',json2);
		end if;	
--	end if;

        --Verificamos que exista el DTE
        select row_to_json(dte_emitidos) into jemi from dte_emitidos where rut_emisor=rut1::integer and tipo_dte=tipo1::integer and folio=folio1::bigint;
        if not found then
                return response_requests_6000('2','No existe el DTE','',json2);
        end if;
	json2:=logjson(json2,'Entra a pivote_financiamiento_12773 2');
	json2:=put_json(json2,'pagador',get_json('rut_receptor',jemi));
	rut_aux1:=get_json('rut_receptor',jemi)::bigint;
	json2:=put_json(json2,'razon_pagador',(select nombre from contribuyentes where rut_emisor=rut_aux1));

	json2:=put_json(json2,'jemi',jemi::varchar);
        --Armo ID de envio para marcar en la traza los eventos de envio de mail
        j3=put_json('{}','E',get_json('rut_emisor',jemi));
        j3=put_json(j3,'T',get_json('tipo_dte',jemi));
        j3=put_json(j3,'F',get_json('folio',jemi));
        j3=put_json(j3,'FE',get_json('fecha_emision',jemi));
        j3=put_json(j3,'C','EMITIDOS');
        j3=put_json(j3,'U',get_json('uri',jemi));
        j3=put_json(j3,'R',get_json('rut_receptor',jemi));
        j3=put_json(j3,'EO','FMS');
        j3=put_json(j3,'EN','FMF');
	
	json2:=put_json(json2,'J3',j3::varchar);

	json2:=logjson(json2,'Entra a pivote_financiamiento_12773 3');

	--if get_json('rutUsuario',json2)<>'7621836' then
	if 1=0 then
	        insert into financiamiento_solicitudes (id,fecha_ingreso,codigo_txel,rut_emisor,tipo_dte,folio,uri,rut_usuario_cliente,estado,financiador,monto_total,fecha_vencimiento_dte,fecha_pago_cliente,rut_receptor,fecha_sii,rut_financiador) values (default,now(),get_json('codigo_txel',jemi)::bigint,rut1::integer,tipo1::integer,folio1::bigint,get_json('uri',jemi),get_json('rutUsuario',json2),'ENVIANDO',get_json('financiador',json2),get_json('monto_total',jemi)::bigint,get_json('fecha_vencimiento',jemi),get_json('fecha_pago',json2)::date,get_json('rut_receptor',jemi)::bigint,get_json('fecha_sii',jemi)::timestamp,fin1.rut) returning id into id1;
		perform limpia_menu_sesion_usuario_6000(get_json('rutUsuario',json2));
		json2:=put_json(json2,'ID_FIN',id1::varchar);
		json2:=put_json(json2,'__SECUENCIAOK__','12773');
		json2:=logjson(json2,'Entra a pivote_financiamiento_12773 4');
		return json2;
	else
		xml2:='';
		xml2:=put_campo(xml2,'TX','12773');
		j2:=json2;
		j2:=put_json(j2,'_LOG_','');	
		--xml2:=json_to_xml(json2::varchar,'');
		--xml2:=put_campo(xml2,'TX','12773');
		--xml2:=put_campo(xml2,'_LOG_','');
	
		if (get_json('rol_usuario',json2)='Sistemas') then
			flag_prueba1:='SI';
		else
			flag_prueba1:='NO';
		end if;
		for campox in select * from financiamiento_financiadores where flag_prueba=flag_prueba1 and codigo is not null loop
			/*if campox.codigo='TANNER' and get_json('monto_total',jemi)::bigint<100000 then
				json2:=logjson(json2,'Financiador '||campox.codigo||' no recibe Facturas con monto menor a $100.000');
				continue;*/
			if campox.codigo='INCOFIN' and get_json('monto_total',jemi)::bigint<1000000 then
				json2:=logjson(json2,'Financiador '||campox.codigo||' no recibe Facturas con monto menor a $1.000.000');
				continue;
			end if;
	        	insert into financiamiento_solicitudes (id,fecha_ingreso,codigo_txel,rut_emisor,tipo_dte,folio,uri,rut_usuario_cliente,estado,financiador,monto_total,fecha_vencimiento_dte,fecha_pago_cliente,rut_receptor,fecha_sii,rut_financiador) values (default,now(),get_json('codigo_txel',jemi)::bigint,rut1::integer,tipo1::integer,folio1::bigint,get_json('uri',jemi),get_json('rutUsuario',json2),'ENVIANDO',campox.codigo,get_json('monto_total',jemi)::bigint,get_json('fecha_vencimiento',jemi),nullif(get_json('fecha_pago',json2),'')::date,get_json('rut_receptor',jemi)::bigint,nullif(get_json('fecha_sii',jemi),'')::timestamp,campox.rut) returning id into id1;
			--xml2:=put_campo(xml2,'financiador',campox.codigo);
			--xml2:=put_campo(xml2,'ID_FIN',id1::varchar);
			--xml2:=put_campo(xml2,'para',campox.correo);
			j2:=put_json(j2,'financiador',campox.codigo);
			j2:=put_json(j2,'ID_FIN',id1::varchar);
			j2:=put_json(j2,'para',campox.correo);	
			xml2:=put_campo(xml2,'INPUT_JSON',encode_hex(j2::varchar));
			--Obtiene el valor de la cola que corresponde grabar
			cola1:=nextval('id_cola_procesamiento');
			--Obtengo el dia
			nombre_tabla1:='cola_motor_'||cola1::varchar;
			execute 'insert into ' || nombre_tabla1 || ' (fecha,reintentos,data,tx,categoria) values ( now(),0,'||quote_literal(xml2)||',10,''FINANCIAMIENTO'') returning id ' into id1;
			--raise notice 'paso4';
			--xml2 := logapp(xml2,'Graba Solicitud Financiamiento '|| id1::varchar);
			json2:=logjson(json2,'Graba Solicitud Financiamiento '|| id1::varchar);
        	end loop;
		return response_requests_6000('1','Solicitud Enviada OK.'||get_json('MSG_CARPETA',json2),'',json2);
	end if;
END;
$$
LANGUAGE plpgsql;



CREATE or replace FUNCTION solicita_id_ecm_12773(json) RETURNS json
AS $$
DECLARE
        json1   alias for $1;
        json2   json;
        json3   json;
        patron1 varchar;
        patron2 varchar;
        sts2    varchar;
        query1  varchar;
        fecha_desde1    timestamp;
        jsonsts1        json;
        subject1        varchar;
        rut1    varchar;
        tipo1   varchar;
        folio1  varchar;
	jemi	json;
        id1     bigint;
        data_lma        varchar;
        id_ecm1 varchar;
        juri    json;
        uri_short1      varchar;
        hash1           varchar;
        j3      json;
BEGIN
        json2:=json1;
	--Siempre se sacan los datos de la cola
	json2:=decode_hex(get_json('INPUT_JSON',json2))::json;
	/*
	if get_json('INPUT_JSON',json2)<>'' then
		json2:=decode_hex(get_json('INPUT_JSON',json2))::json;
	end if;
	*/

	j3:=get_json('J3',json2);
        id_ecm1:=get_id_ecm()::varchar;
	jemi:=get_json('jemi',json2)::json;
	id1:=get_json('ID_FIN',json2);
	json2:=put_json(json2,'__SECUENCIAOK__','0');

        --Armo evento de lectura del correo
        data_lma := encripta_hash_evento_VDC2('uri='||get_json('uri',jemi)||'&owner='||get_json('rut_emisor',jemi)||'&rutEmisor='||get_json('rut_emisor',jemi)::varchar||'&tipoDTE='||get_json('tipo_dte',jemi)::varchar||'&folio='||get_json('folio',jemi)::varchar||'&mail='||trim(get_json('para',json2))||'&type=FMA'||'&rutRecep='||get_json('rut_receptor',jemi)::Varchar||'&fchEmis='||get_json('fecha_emision',jemi)::varchar||'&relatedUrl=&comment=Mail Leído por '||trim(get_json('para',json2))||'&url_redirect=https://traza.acepta.com/imgs/blank.png&id_ecm='||id_ecm1||'&');
        --Creamos el json para generar la URI del evento
        juri:='{}';
        juri:=put_json(juri,'id',id_ecm1::varchar||'_FMA');
        juri:=put_json(juri,'cliente','financiamiento');
        --Esta es la URI que se necesita hacer redirect
        juri:=put_json(juri,'url','https://traza.acepta.com/imgs/blank.png');
        --Esta es la URL donde se posteara el evento
        juri:=put_json(juri,'url_get','http://servicios.acepta.com/traza?hash=');
        --Esta es la data
        juri:=put_json(juri,'data_get',data_lma);
        --Se indica al servicio que la data viene encriptada
        juri:=put_json(juri,'flag_data_encriptada','SI');
        juri:=sp_crea_url_short(juri);
        json2 := logjson(json2,get_json('_LOG_',juri));
        --Se obtiene la URL donde se gatillara el evento
        uri_short1:=get_json('url_short',juri);
        json2:=logjson(json2,'URI_SHORT LMA='||uri_short1::varchar);
        if uri_short1='' then
                json2 := logjson(json2,'Falla generacion uri corta');
		return define_secuencia_12773(response_requests_6000('2','Error interno financiamiento Cod(74)','',json2));
        end if;


        --Armamos la URL de abrir el formulario
        juri:=put_json('{}','id_fin',id1::varchar);
        juri:=put_json(juri,'codigo_txel',get_json('codigo_txel',jemi)::varchar);
        hash1:=encripta_hash_evento_VDC2(juri::varchar);

        --Inserto la solicitud
        json3:='{}';
        json3:=put_json(json3,'LINK','https://escritorio.acepta.com/appDinamicaOffline/index.php?app_dinamica=financiamiento_ofertar_dte&hash='||hash1||'&');
        json3:=put_json(json3,'evento_lma',uri_short1);
        json3:=put_json(json3,'ID_ECM',id_ecm1);
        json3:=put_json(json3,'tipo_envio','HTML');
        subject1:='Financiar Factura  Emisor '||get_json('razon_social',json2)||' Pagador '||get_json('razon_pagador',json2);
        json3:=put_json(json3,'subject',subject1);
        json3:=put_json(json3,'from','Financiamiento Acepta<noreply@acepta.com>');
        patron1:=pg_read_file('./patron_correos/mail_propuesta_financiamiento.html');
        json3:=put_json(json3,'CABECERA','Solicitud de Financiamiento de DTE');
        json3:=put_json(json3,'ESTIMADO',get_json('financiador',json2));
        json3:=put_json(json3,'MENSAJE1','Este mail tiene por finalidad, evaluar la opcion de financiamiento de esta factura:');
        json3:=put_json(json3,'MENSAJE3','Rut Emisor '||get_json('EMISOR',json2)||'<br>'||'Tipo Dte '||get_json('TIPO',json2)||'<br>Folio '||get_json('FOLIO',json2)||'<br>Pagador '||get_json('rut_receptor',jemi)||'<br>Monto '||get_json('monto_total',jemi)||'<br> Contacto: '||get_json('nombre_usuario',json2)||' Mail: '||get_json('mail_usuario',json2)||' Fono: '||get_json('fono_usuario',json2)||' Movil: '||get_json('movil_usuario',json2));
        json3:=put_json(json3,'CATEGORIA','FINANCIAMIENTO');
        json3:=put_json(json3,'RUT_OWNER',get_json('rutCliente',json2));
        json3:=put_json(json3,'msg_id','<JCP'||encripta_hash_evento_VDC2(j3::varchar)||'@motor2.acepta.com>');

        json3:=put_json(json3,'to',get_json('para',json2));
        patron2=remplaza_tags_json_c(json3,patron1);
        json3:=put_json(json3,'content_html',encode(patron2::bytea,'hex'));
        jsonsts1:=send_mail_python2_colas(json3::varchar);

	json2:=put_json(json2,'jsonsts',jsonsts1::varchar);
	json2:=put_json(json2,'__SECUENCIAOK__','20');
	return json2;
END;
$$
LANGUAGE plpgsql;


CREATE or replace FUNCTION verifica_envio_12773(json) RETURNS json
AS $$
DECLARE
        json1   alias for $1;
        json2   json;
        json3   json;
        patron1 varchar;
        patron2 varchar;
        sts2    varchar;
        query1  varchar;
        fecha_desde1    timestamp;
        jsonsts1        json;
        subject1        varchar;
        rut1    varchar;
        tipo1   varchar;
        folio1  varchar;
        id1     bigint;
        id_ecm1 varchar;
        juri    json;
        uri_short1      varchar;
        hash1           varchar;
        j3      json;
	codigo1	bigint;
BEGIN
        json2:=json1;
	jsonsts1:=get_json('jsonsts',json2);
	id1:=get_json('ID_FIN',json2);
	json2:=put_json(json2,'__SECUENCIAOK__','0');
	codigo1:=get_json('codigo_txel',get_json('jemi',json2)::json)::bigint;

        if (get_json('status',jsonsts1)='OK') then
                --Se inserta en financiamiento_solicitudes
                update financiamiento_solicitudes set estado='SOLICITADO',id_mail=get_json('ID_ECM',json2),fecha_actualizacion=now() where id=id1;
                --Se actualiza la base
                update dte_emitidos set data_dte=coalesce(data_dte,'')||'<FINTECH>'||id1::varchar||'</FINTECH>' where codigo_txel=codigo1::bigint;
		return define_secuencia_12773(response_requests_6000('1','Solicitud Enviada OK.'||get_json('MSG_CARPETA',json2),'{"NO_REFRESH":"SI"}',json2));
        else
		return define_secuencia_12773(response_requests_6000('2','Falla Envio de Solicitud','{"NO_REFRESH":"SI"}',json2));
        end if;
END;
$$
LANGUAGE plpgsql;

