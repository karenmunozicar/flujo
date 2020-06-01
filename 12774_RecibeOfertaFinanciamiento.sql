--Publica documento
delete from isys_querys_tx where llave='12774';

--Arma lista de mails y genera contador en 0
insert into isys_querys_tx values ('12774',10,19,1,'select solicita_id_ecm_12774(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

--Armo correo para el mail[contador]
insert into isys_querys_tx values ('12774',20,9,1,'select verifica_envio_12774(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);


CREATE or replace FUNCTION pivote_financiamiento_hash1_12774(json) RETURNS json
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
        jsonsts1        varchar;
        subject1        varchar;
        rut1    varchar;
        tipo1   varchar;
        folio1  varchar;
        id1     bigint;
        data_lma        varchar;
        id_ecm1 varchar;
        juri    json;
        uri_short1      varchar;
        hash1           varchar;
        j3      json;
        jemi    json;
        fin1    varchar;
        usu1    varchar;
        campo record;
        campo3  record;
	uri_default1	varchar;
	mensaje1	varchar;
	mensaje12	varchar;
	campo_dte	record;
	fecha1	timestamp;
	id_sol1	varchar;
	rut_fin1	varchar;
	campo_contacto  record;
	tot_contacto1	integer;
        json4   json;
        tipo_oferta1    varchar;
BEGIN
	json2:=json1;
        rut1:=split_part(replace(get_json('EMISOR',json2),'.',''),'-',1);
        tipo1:=get_json('TIPO',json2);
        folio1:=replace(get_json('FOLIO',json2),'.','');
	j3:=desencripta_hash_evento_vdc2(get_json('hash1',json2))::json;
	id1:=get_json('id_fin',j3);

        select * into campo from financiamiento_solicitudes where id=id1;
        if not found then
                return response_requests_6000('2','La solicitud de financiamiento, no existe','',json2);
        end if;
	
        if campo.estado not in ('SOLICITADO','PROCESO_OFERTA','OFERTA','PRE-OFERTA','PRE-OFERTA-ACEPTADA') then
                return response_requests_6000('2','La solicitud ya esta (Aprobada o Rechazada)','',json2);
        end if;
/*	--El Financiados la esta reactivando...
	if campo.estado='OCULTA_FIN' then
		rut_fin1:=get_json('rutCliente',json2)||'-'||modulo11(get_json('rutCliente',json2));
		delete from financiamiento_solicitudes where id=id1 and rut_financiador=rut_fin1; 
		if not found then
			return response_requests_6000('2','Falla. Reintente','',json2);
		end if;
	end if;
*/

        --Debe tener al menos un documento en la grilla
	select * into campo3 from financiamiento_documentos where id_grupo=campo.id_grupo limit 1;
        if not found then
                return response_requests_6000('2','Es necesario adjuntar al menos 1 documento.','',json2);
        end if;
        /*select * into campo3 from financiamiento_documentos where id=id1 limit 1;
        if not found then
                return response_requests_6000('2','Es necesario adjuntar al menos 1 documento.','',json2);
        end if;*/
	if is_number(replace(get_json('giro',json2),'.','')) is false then
                return response_requests_6000('2','Giro debe ser numerico.','',json2);
	end if;
	if is_number(replace(get_json('excedente',json2),'.','')) is false then
                return response_requests_6000('2','Excedente debe ser numerico.','',json2);
	end if;
	if get_json('fecha_caducidad',json2)='' then
                return response_requests_6000('2','Debe ingresar Fecha de Caducidad','',json2);
	end if;
	BEGIN
		fecha1:=get_json('fecha_caducidad',json2)::timestamp;
	EXCEPTION WHEN OTHERS THEN
		return response_requests_6000('2','Debe ingresar Fecha de Caducidad Válida','',json2);
	END;
	if fecha1<now() then
		return response_requests_6000('2','Debe ingresar Fecha de Caducidad Válida','',json2);
	end if;

	--Revisamos que justo el cliente no la haya ocultado
	select * into campo_dte from dte_emitidos where codigo_txel=campo.codigo_txel::bigint;
	if not found then
                return response_requests_6000('2','La solicitud de financiamiento, no existe.','',json2);
	end if;
	if strpos(campo_dte.data_dte,'<FINTECHOCULTAR>SI<')>0 then
                return response_requests_6000('2','El cliente oculto esta Factura, no se puede ofertar','',json2);
	end if;

	--Marcamos la factura en dte_emitidos
	if get_json('switch_oferta',json2)='on' then
		id_sol1:='-S'||id1::varchar||'-'||split_part(campo.rut_financiador,'-',1)||'-';
		--update dte_emitidos set data_dte=financiamiento_put_data_dte(data_dte,'FINTECH',id_sol1::varchar) where codigo_txel=campo.codigo_txel::bigint and strpos(coalesce(data_dte,''),'<FINTECH>'||id_sol1::varchar||'</FINTECH>')=0;
		update dte_emitidos set data_dte=put_data_dte(data_dte,'FINTECH',coalesce(get_xml('FINTECH',data_dte),'')||id_sol1) where codigo_txel=campo.codigo_txel::bigint;
        	update financiamiento_solicitudes set estado='OFERTA',fecha_actualizacion=now(),giro=replace(get_json('giro',json2),'.','')::bigint,excedente=replace(get_json('excedente',json2),'.','')::bigint,comentario_oferta=get_json('comentario',json2),fecha_caducidad=fecha1 where id=id1;
		mensaje1:='Oferta';
		mensaje12:='ofertado';
		json2:=bitacora10k(json2,'OFERTA','Oferta Factura Folio '||campo_dte.folio::varchar||' Giro '||get_json('giro',json2)||' Excedente '||get_json('excedente',json2));
	else
		id_sol1:=id1::varchar||'-'||split_part(campo.rut_financiador,'-',1);
		--update dte_emitidos set data_dte=financiamiento_put_data_dte(data_dte,'PREFINTECH',id_sol1::varchar) where codigo_txel=campo.codigo_txel::bigint and strpos(coalesce(data_dte,''),'<PREFINTECH>'||id_sol1::varchar||'</PREFINTECH>')=0;
		update dte_emitidos set data_dte=put_data_dte(data_dte,'PREFINTECH',coalesce(get_xml('PREFINTECH',data_dte),'')||id_sol1) where codigo_txel=campo.codigo_txel::bigint;
        	update financiamiento_solicitudes set estado='PRE-OFERTA',fecha_actualizacion=now(),giro=replace(get_json('giro',json2),'.','')::bigint,excedente=replace(get_json('excedente',json2),'.','')::bigint,comentario_oferta=get_json('comentario',json2),fecha_caducidad=fecha1 where id=id1;
		mensaje1:='Pre-Oferta';
		mensaje12:='pre-ofertado';
		json2:=bitacora10k(json2,'PRE-OFERTA','Oferta Factura Folio '||campo_dte.folio::varchar||' Giro '||get_json('giro',json2)||' Excedente '||get_json('excedente',json2));
	end if;

	--Limipia la sesion, para que se actualice al total del menu
	update sesion_web_10k set json_menu=null where rut_usuario in (select distinct rut_usuario from menu_10k where empresa=campo.rut_emisor::varchar and aplicacion in ('FINANCIAMIENTO'));

	--Se remplaza en el json2 el rut del emisor de la factura y no del financiador
        uri_default1:= coalesce((select url from apps_10k where appname='FINANCIAMIENTO'),'');

        --Mostramos un mensaje en la pantalla del usuario
	--insert into mensajes_pantalla values (campo.rut_emisor::varchar,'*','Estimado Cliente:<br>El financiador'||campo.financiador||', ha '||mensaje12||' por la factura '||campo.folio::varchar||'<br>Si desea revisar esta '||mensaje1||' , ingrese a la aplicacion "Financiamiento"<br>Atte.<br>Acepta.<br>',now(),now(),now()+interval '2 day','DTE','yesno','borrar_mensaje_pantalla',default,null,'FINANCIAMIENTO',uri_default1);
	uri_default1:='https://'||get_json('host_canal',json2)||'/ext.php?r=https://'||get_json('host_canal',json2)||'/appDinamicaClasses/index.php%3Fapp_dinamica=buscarNEW_emitidos%26session_id=#%#session_id#%#%26rutUsuario=#%#rutUsuario#%#%26rutCliente='||campo.rut_emisor::varchar||'%26aplicacion=FINANCIAMIENTO%26valores_campos='||encode(('{"folio_desde":"'||campo_dte.folio::varchar||'","FSTART":"'||to_char(campo_dte.fecha_ingreso,'YYYY-MM-DD')||'"}')::bytea,'hex')||'%26';	
	--insert into aviso_maestro_clientes(rut,fecha,mensaje,uri,estado,id,aplicacion,categoria) values(campo.rut_emisor::varchar,now(),'El financiador'||campo.financiador||', ha '||mensaje12||' por la factura '||campo.folio::varchar,uri1,default,'DTE','FINANCIAMIENTO');
	--insert into aviso_maestro_clientes(rut,fecha,mensaje,uri,estado,id,aplicacion,categoria) values(campo.rut_emisor::varchar,now(),'El financiador'||campo.financiador||', ha '||mensaje12||' por la factura '||campo.folio::varchar,uri_default1,default,'FINANCIAMIENTO','FINANCIAMIENTO');
	--perform limpia_menu_sesion_empresa_6000(campo.rut_emisor::varchar);

        --Guardamos el id del mensaje para borrarlo, cuando lo vea el usuario

        --return response_requests_6000('1','Solicitud Enviada OK','{"SHOW_ALERT":"NO","URL_RESPUESTA":"https://escritorio.acepta.com/mensaje.php?mensaje='||encode('Solicitud Enviada OK','hex')||'"}',json2);
	/*MVG envio mail*/
        if  get_json('switch_oferta',json2)='on' then
                tipo_oferta1:='Oferta';
        else
                tipo_oferta1:='Pre-Oferta';
        end if;
	/*
	tot_contacto1:=0;
        for campo_contacto in select rut_contacto,nombre_contacto,mail_contacto,fono_contacto from financiamiento_contacto where empresa=rut1
        loop
		json2:=logjson(json2,'FIN-ENVIO-CORREOS-CONTACTO-'||tot_contacto1::varchar);
		tot_contacto1:=tot_contacto1+1;
                json3:='{}';
                json3:=put_json(json3,'LINK','https://escritorio.acepta.com');
                --json3:=put_json(json3,'evento_lma',uri_short1);
                --json3:=put_json(json3,'ID_ECM',id_ecm1);
                json3:=put_json(json3,'tipo_envio','HTML');
                subject1:= upper(get_json('razon_social',json2))||' ha realizado una '||tipo_oferta1||' de financiamiento de DTE';
                json3:=put_json(json3,'subject',subject1);
                json3:=put_json(json3,'from','Financiamiento Acepta<noreply@acepta.com>');
                patron1:=pg_read_file('./patron_correos/mail_propuesta_financiamiento.html');
                json3:=put_json(json3,'CABECERA',upper(get_json('razon_social',json2))||' ha realizado una '||tipo_oferta1||' de financiamiento de DTE');
                json3:=put_json(json3,'ESTIMADO',campo_contacto.nombre_contacto);
                json3:=put_json(json3,'MENSAJE1','Este mail tiene por finalidad, informar la '||tipo_oferta1||' de esta factura:');
                json3:=put_json(json3,'MENSAJE3','Tipo Dte '||campo_dte.tipo_dte||'<br>Folio '||campo_dte.folio||'<br>'||'<br><br>Rut Financiador '||campo.rut_financiador||'<br>Razon Social '||get_json('razon_social',json2)||'<br>Giro '||get_json('giro',json2)||'<br>Excedente '||get_json('excedente',json2)||'<br>Monto Total '||campo.monto_total||'<br>');
                json3:=put_json(json3,'CATEGORIA','FINANCIAMIENTO');
                json3:=put_json(json3,'RUT_OWNER',get_json('rutCliente',json2));
                --json3:=put_json(json3,'msg_id','<JCP'||encripta_hash_evento_VDC2(j3::varchar)||'@motor2.acepta.com>');
                json3:=put_json(json3,'uri',campo_dte.uri);
                json3:=put_json(json3,'to',campo_contacto.mail_contacto);
                patron2=remplaza_tags_json_c(json3,patron1);
                json3:=put_json(json3,'content_html',encode(patron2::bytea,'hex'));
                --jsonsts1:=send_mail_python2_colas(json3::varchar);
                jsonsts1:=graba_mail_cola(json3);
                --json2:=put_json(json2,'jsonsts',jsonsts1::varchar);
        end loop;
	--Recorremos a los Admin
	if tot_contacto1=0 then
		json2:=logjson(json2,'FIN-ENVIO-CORREOS-ADMIN');
		for campo_contacto in select rut_usuario as rut_contacto,initcap(nombre) as nombre_contacto,mail as mail_contacto,fono as fono_contacto from user_10k where rut_usuario in (select rut_usuario from menu_10k where empresa=rut1::varchar and aplicacion='DTE' and perfil like '%Admin%' order by fecha_ultimo_acceso desc limit 5) loop
			json3:='{}';
			json3:=put_json(json3,'LINK','https://escritorio.acepta.com');
			--json3:=put_json(json3,'evento_lma',uri_short1);
			--json3:=put_json(json3,'ID_ECM',id_ecm1);
			json3:=put_json(json3,'tipo_envio','HTML');
			subject1:= upper(get_json('razon_social',json2))||' ha realizado una '||tipo_oferta1||' de financiamiento de DTE';
			json3:=put_json(json3,'subject',subject1);
			json3:=put_json(json3,'from','Financiamiento Acepta<noreply@acepta.com>');
			patron1:=pg_read_file('./patron_correos/mail_propuesta_financiamiento.html');
			json3:=put_json(json3,'CABECERA',upper(get_json('razon_social',json2))||' ha realizado una '||tipo_oferta1||' de financiamiento de DTE');
			json3:=put_json(json3,'ESTIMADO',campo_contacto.nombre_contacto);
			json3:=put_json(json3,'MENSAJE1','Este mail tiene por finalidad, informar la '||tipo_oferta1||' de esta factura:');
			json3:=put_json(json3,'MENSAJE3','Tipo Dte '||campo_dte.tipo_dte||'<br>Folio '||campo_dte.folio||'<br>'||'<br><br>Rut Financiador '||campo.rut_financiador||'<br>Razon Social '||get_json('razon_social',json2)||'<br>Giro '||get_json('giro',json2)||'<br>Excedente '||get_json('excedente',json2)||'<br>Monto Total '||campo.monto_total||'<br>');
			json3:=put_json(json3,'CATEGORIA','FINANCIAMIENTO');
			json3:=put_json(json3,'RUT_OWNER',get_json('rutCliente',json2));
			--json3:=put_json(json3,'msg_id','<JCP'||encripta_hash_evento_VDC2(j3::varchar)||'@motor2.acepta.com>');
			json3:=put_json(json3,'uri',campo_dte.uri);
			json3:=put_json(json3,'to',campo_contacto.mail_contacto);
			patron2=remplaza_tags_json_c(json3,patron1);
			json3:=put_json(json3,'content_html',encode(patron2::bytea,'hex'));
			--jsonsts1:=send_mail_python2_colas(json3::varchar);
			jsonsts1:=graba_mail_cola(json3);
		end loop;
	end if;
	*/
        /*MVG fin envio mail*/
	return response_requests_6000('1',mensaje1||' Enviada','',json2);
END;

$$
LANGUAGE plpgsql;




CREATE or replace FUNCTION pivote_financiamiento_12774(json) RETURNS json
AS $$
DECLARE
	json1	alias for $1;
	json2	json;
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
        data_lma        varchar;
        id_ecm1 varchar;
        juri    json;
        uri_short1      varchar;
        hash1           varchar;
        j3      json;
	jemi	json;
	fin1	varchar;
	usu1	varchar;	
	campo record;
	campo3	record;
BEGIN
	json2:=json1;
	--Si viene hash1 es pivote de la funcion v2
	if (get_json('hash1',json2)<>'') then
		return pivote_financiamiento_hash1_12774(json2);
	end if;
	json2:=put_json(json2,'LLAMA_FLUJO','SI');
	json2:=put_json(json2,'__SECUENCIAOK__','0');
        rut1:=split_part(replace(get_json('EMISOR',json2),'.',''),'-',1);
        tipo1:=get_json('TIPO',json2);
        folio1:=replace(get_json('FOLIO',json2),'.','');
	j3:=desencripta_hash_evento_vdc2(get_json('hash',json2))::json;
	id1:=get_json('id_fin',j3);



	select * into campo from financiamiento_solicitudes where  id=id1;
	if not found then
                return response_requests_6000('2','La solicitud de financiamiento, no existe','',json2);
	end if;
	json2:=put_json(json2,'para',(select mail from user_10k where rut_usuario=campo.rut_usuario_cliente limit 1));
	
	if campo.estado not in ('SOLICITADO','PROCESO_OFERTA') then	
                return response_requests_6000('2','La solicitud ya esta (Aprobada o Rechazada)','',json2);
	end if;

	--Debe tener al menos un documento en la grilla
	select * into campo3 from financiamiento_documentos where id=id1 limit 1;
	if not found then
		return response_requests_6000('2','Es necesario adjuntar al menos 1 documento.','',json2);
	end if;

	--Verificamos si esta aceptada la solicitud de financiamiento
	if (get_json('ACEPTAR',json2)='RECHAZAR') then
		--Marcamos la solicitud como rechazada
		update financiamiento_solicitudes set estado='CON_REPAROS',fecha_actualizacion=now() where id=id1;
	elsif (get_json('ACEPTAR',json2)='ACEPTAR') then
		--if is_numeric(get_json('tasa',json2)) is false then
		--	return response_requests_6000('2','Tasa Inválida','',json2);
		--end if;
		--update financiamiento_solicitudes set estado='DISPONIBLE',fecha_actualizacion=now(),tasa=get_json('tasa',json2)::float where id=id1;
		update financiamiento_solicitudes set estado='DISPONIBLE',fecha_actualizacion=now() where id=id1;
	else
                return response_requests_6000('2','Debe escoger una acción','',json2);
	end if;
	json2:=put_json(json2,'FINANCIADOR',campo.financiador);
	json2:=put_json(json2,'CLIENTE',campo.rut_emisor::varchar);
	json2:=put_json(json2,'RUT_USUARIO_CLIENTE',campo.rut_usuario_cliente);
	json2:=put_json(json2,'FOLIO',campo.folio::varchar);
	json2:=put_json(json2,'ID_FIN',id1::varchar);
	json2:=put_json(json2,'__SECUENCIAOK__','12774');
	return json2;
END;
$$
LANGUAGE plpgsql;



CREATE or replace FUNCTION solicita_id_ecm_12774(json) RETURNS json
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

	--j3:=get_json('J3',json2);
        id_ecm1:=get_id_ecm()::varchar;
	--jemi:=get_json('jemi',json2)::json;
	id1:=get_json('ID_FIN',json2);
	json2:=put_json(json2,'__SECUENCIAOK__','0');

        --Inserto la solicitud
        json3:='{}';
        json3:=put_json(json3,'LINK','https://escritorio.acepta.com/');
        --json3:=put_json(json3,'evento_lma',uri_short1);
        json3:=put_json(json3,'ID_ECM',id_ecm1);
        json3:=put_json(json3,'tipo_envio','HTML');
        subject1:='Respuesta '||get_json('FINANCIADOR',json2)||' Factura Folio='||get_json('FOLIO',json2);
        json3:=put_json(json3,'subject',subject1);
        json3:=put_json(json3,'from','Financiamiento Acepta<noreply@acepta.com>');
        patron1:=pg_read_file('./patron_correos/mail_respuesta_financiamiento.html');
        json3:=put_json(json3,'CABECERA','Respuesta Financiamiento de DTE');
        json3:=put_json(json3,'ESTIMADO','Cliente');
        json3:=put_json(json3,'MENSAJE1','El Financiador '||get_json('FINANCIADOR',json2)||' ha respondido a su solicitud de financiamiento para esta factura.<br>Por favor revisa la respuesta en nuestro portal <a href="https://escritorio.acepta.com/">Escritorio Acepta DTE</a>');
        json3:=put_json(json3,'MENSAJE3','');
        json3:=put_json(json3,'CATEGORIA','FINANCIAMIENTO');
        json3:=put_json(json3,'RUT_OWNER',get_json('rutCliente',json2));
        --json3:=put_json(json3,'msg_id','<JCP'||encripta_hash_evento_VDC2(j3::varchar)||'@motor2.acepta.com>');

        json3:=put_json(json3,'to',get_json('para',json2));
        patron2=remplaza_tags_json_c(json3,patron1);
        json3:=put_json(json3,'content_html',encode(patron2::bytea,'hex'));
        jsonsts1:=send_mail_python2_colas(json3::varchar);
        if (get_json('status',jsonsts1)='OK') then
		json2:=logjson(json2,'Solicitud Enviada OK');
        else
		json2:=logjson(json2,'Falla envio de Solicitud ');
        end if;

	json2:=put_json(json2,'__SECUENCIAOK__','20');
	return json2;
END;
$$
LANGUAGE plpgsql;


CREATE or replace FUNCTION verifica_envio_12774(json) RETURNS json
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
	id_mensaje1	bigint;
	uri_default1	varchar;
BEGIN
        json2:=json1;
	--jsonsts1:=get_json('jsonsts',json2);
	id1:=get_json('ID_FIN',json2);
	json2:=put_json(json2,'__SECUENCIAOK__','0');
	--codigo1:=get_json('codigo_txel',get_json('jemi',json2)::json)::bigint;
	--Limpiamos el menu del usuario para que se refresque el badge
	perform limpia_menu_sesion_usuario_6000(get_json('RUT_USUARIO_CLIENTE',json2));

	uri_default1:= coalesce((select href||'%26awsfec=DISPONIBLE%26' from menu_info_10k where id2='grilla_menu_financiamiento'),'');

	--Mostramos un mensaje en la pantalla del usuario
	insert into mensajes_pantalla values (get_json('CLIENTE',json2),get_json('RUT_USUARIO_CLIENTE',json2),'Estimado Cliente:<br>El financiador '||get_json('FINANCIADOR',json2)||', ha respondido a su solicitud.<br>Por favor revise en el menú de "Financiamiento" opción "Cotizaciones Disponibles".<br>Atte.<br>Acepta.<br>',now(),now(),now()+interval '1 month','DTE','yesno','borrar_mensaje_pantalla',default,null,'FINANCIAMIENTO',uri_default1) returning id into id_mensaje1;
	
	--Guardamos el id del mensaje para borrarlo, cuando lo vea el usuario	

        return response_requests_6000('1','Solicitud Enviada OK','{"SHOW_ALERT":"NO","URL_RESPUESTA":"https://escritorio.acepta.com/mensaje.php?mensaje='||encode('Solicitud Enviada OK','hex')||'"}',json2);
END;
$$
LANGUAGE plpgsql;

