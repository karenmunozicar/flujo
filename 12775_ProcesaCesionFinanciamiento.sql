--Publica documento
delete from isys_querys_tx where llave='12775';

insert into isys_querys_tx values ('12775',5,1,8,'Llamada Consulta Reclamo',16100,0,0,0,0,10,10);

insert into isys_querys_tx values ('12775',10,9,8,'Llamada CESION JSON',12797,0,0,0,0,20,20);

insert into isys_querys_tx values ('12775',20,9,1,'select procesa_resp_cesion_12775(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('12775',30,19,1,'select solicita_id_ecm_12775(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);


CREATE or replace FUNCTION solicita_id_ecm_12775(json) RETURNS json
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
        jemi    json;
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
        --id_ecm1:=get_id_ecm()::varchar;
        --jemi:=get_json('jemi',json2)::json;
        id1:=get_json('ID_FIN',json2);
        json2:=put_json(json2,'__SECUENCIAOK__','0');

        --Inserto la solicitud
        json3:='{}';
        json3:=put_json(json3,'LINK','https://escritorio.acepta.com/');
        --json3:=put_json(json3,'evento_lma',uri_short1);
        --json3:=put_json(json3,'ID_ECM',id_ecm1);
        json3:=put_json(json3,'tipo_envio','HTML');
        subject1:='Cesion para '||get_json('FINANCIADOR_CODIGO',json2)||' de Factura Emisor '||get_json('razon_social',json2)||' Pagador '||get_json('razon_pagador',json2)||' (En espera de respuesta del SII)' ;
        json3:=put_json(json3,'subject',subject1);
        json3:=put_json(json3,'from','Financiamiento Acepta<noreply@acepta.com>');
        patron1:=pg_read_file('./patron_correos/mail_respuesta_financiamiento.html');
        json3:=put_json(json3,'CABECERA','Cesion  de DTE');
        json3:=put_json(json3,'ESTIMADO',get_json('FINANCIADOR_CODIGO',json2));
	
	juri:=put_json('{}','id_fin',get_json('ID_FIN',json2));
        juri:=put_json(juri,'codigo_txel',get_json('CODIGO_TXEL_FIN',json2));
        hash1:=encripta_hash_evento_VDC2(juri::varchar);

        json3:=put_json(json3,'MENSAJE1','El cliente '||get_json('razon_social',json2)||' ha cedido el documento para financiar la operacion.<br>Por favor seguir el <a href="https://escritorio.acepta.com/appDinamicaOffline/index.php?app_dinamica=financiamiento_ofertar_dte&hash='||hash1||'&frm=PAGO&">link</a> e indicar fecha y monto de pago.<br><br>El documento cedido es : <a href="'||get_json('URI_IN',json2)||'">Documento</a><br>');
        json3:=put_json(json3,'MENSAJE3','');
        json3:=put_json(json3,'CATEGORIA','FINANCIAMIENTO');
        json3:=put_json(json3,'RUT_OWNER',get_json('rutCliente',json2));
        --json3:=put_json(json3,'msg_id','<JCP'||encripta_hash_evento_VDC2(j3::varchar)||'@motor2.acepta.com>');

        json3:=put_json(json3,'to',get_json('CORREO_FIN',json2));
        patron2=remplaza_tags_json_c(json3,patron1);
        json3:=put_json(json3,'content_html',encode(patron2::bytea,'hex'));
        jsonsts1:=send_mail_python2_colas(json3::varchar);
        if (get_json('status',jsonsts1)='OK') then
                json2:=logjson(json2,'Solicitud Enviada OK');
		json3:=put_json(json3,'to','tomas.silva@acepta.com');
		perform graba_mail_cola(json3);
		json3:=put_json(json3,'to','alejandro.bustamante@acepta.com');
		perform graba_mail_cola(json3);
        else
                json2:=logjson(json2,'Falla envio de Solicitud ');
        end if;

        json2:=put_json(json2,'__SECUENCIAOK__','0');
        return json2;
END;
$$
LANGUAGE plpgsql;


CREATE or replace FUNCTION pivote_financiamiento_12775(json) RETURNS json
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
	campo1 record;
	campop record;
	id_fin1	varchar;
	datos_json      json;
        estado_cliente1 varchar;
        mensaje_cliente1 varchar;
        rut_financiador_sin_dv  integer;
        codigo_financiador      varchar;
	cta_cte1 varchar;
BEGIN
	json2:=json1;

	cta_cte1:=get_json('cta_cte',json2);
        id_fin1:=replace(get_json('ID',json2),'.','');
        if(is_number(id_fin1) is false) then
		id_fin1:=replace(get_json('id_tabla_detalle',json2),'.','');
        	if(is_number(id_fin1) is false) then
                	return response_requests_6000('2', 'ID Inválido','',json2);
		end if;
        end if;

        if(is_number(get_json('rutUsuario',json2)) is false) then
                return response_requests_6000('2', 'Usuario Inválido','',json2);
        end if;

        select * into campo from financiamiento_solicitudes where id=id_fin1::bigint;
        if not found then
                return response_requests_6000('2', 'Solicitud no encontrada..','',json2);
        end if;
	json2:=put_json(json2,'ID_FIN',id_fin1::varchar);
	json2:=put_json(json2,'CODIGO_TXEL_FIN',campo.codigo_txel::varchar);

	json2:=put_json(json2,'pagador',campo.rut_receptor::varchar);
        json2:=put_json(json2,'razon_pagador',(select nombre from contribuyentes where rut_emisor=campo.rut_receptor));


	--Si la esta rechazando solo actualizamos el estado
	if get_json('tipo_tx',json2)='financiamiento_rechazar_oferta' then
		json2:=bitacora10k(json2,'RECHAZA_OFERTA','Rechaza Oferta de '||campo.financiador||' para la Factura Folio '||campo.folio::varchar);
		update financiamiento_solicitudes set estado='CON_REPAROS',estado_cliente='RECHAZADA',fecha_actualizacion=now() where id=id_fin1::bigint;
		perform limpia_menu_sesion_usuario_6000(campo.rut_usuario_cliente);
		--MVG 20190207 Envia data a Financiador
                datos_json:=put_json('{}','ID',id_fin1::varchar);
                datos_json:=put_json(datos_json,'rutCliente',get_json('rutCliente',json2));
                datos_json:=put_json(datos_json,'tipo_tx','OFERTA');
                datos_json:=put_json(datos_json,'estado_cliente','RECHAZADA');
                datos_json:=put_json(datos_json,'mensaje_cliente',get_json('razon_rechazo',json2));
                datos_json:=financiamiento_envia_data_financiador(datos_json);
		if get_json('CODIGO_RESPUESTA',split_part(get_json('RESPUESTA',datos_json),chr(10)||chr(10),2)::json)<>'1' then
                        json2:=logjson(json2,'Error al llamar financiamiento_envia_data_financiador MENSAJE='||get_json('MENSAJE_RESPUESTA',datos_json)||' DATA_JSON='||datos_json);
                end if;
                return response_requests_6000('1', 'Solicitud Rechazada','',json2);
	end if;

        json2:=put_json(json2,'FOLIO',campo.folio::varchar);
        json2:=put_json(json2,'folio',campo.folio::varchar);
        json2:=put_json(json2,'TIPO',campo.tipo_dte::varchar);
        json2:=put_json(json2,'TIPO_DTE',campo.tipo_dte::varchar);
        json2:=put_json(json2,'tipoDte',campo.tipo_dte::varchar);
        json2:=put_json(json2,'URI_IN',campo.uri);
        json2:=put_json(json2,'rutCliente',campo.rut_emisor::varchar);
        json2:=put_json(json2,'RUT_EMISOR',campo.rut_emisor::varchar);
        json2:=put_json(json2,'CODIGO_TXEL',campo.codigo_txel::varchar);
	
	if get_json('hash',json2)<>get_json('hash1',json2) then
		return response_requests_6000('1', 'Claves no coinciden','',json2);
	end if;
        json2:=put_json(json2,'pass',encode(get_json('hash',json2)::bytea,'hex')::varchar);
        json2:=put_json(json2,'montoCesion',campo.monto_total::varchar);


	json2:=bitacora10k(json2,'ACEPTA_OFERTA','Acepta Oferta de '||campo.financiador||' para la Factura Folio '||campo.folio::varchar);
	--Si el Financiador es la Bolsa ...
	--Si el Pagador no es de la lista de pagadores de la bolsa o la factura no tiene merito ejecutivo aun.... Se cede a la filial.
	--DAO 20171222
	--if campo.financiador='BOLSA' then
	if strpos(campo.financiador,'BOLSA-')>0 then	
		json2:=logjson(json2,'Financiador BOLSA');
		if (now()-campo.fecha_sii< interval '8 days') then
			json2:=logjson(json2,'Factura no tiene merito...Se le cede a la filial 76184721-K');
			--Leemos la tabla para los datos del financiador
			select * into campo1 from financiamiento_financiadores where rut='76184721-K';
			if not found then
				return response_requests_6000('2', 'Financiador no registrado','',json2);
			end if;
		else
			--Verifico pagador
			select * into campop from bolsa_pagadores where rut_emisor=campo.rut_receptor and estado='1';
			if not found then
				json2:=logjson(json2,'Pagador no es de la bolsa...Se le cede a la filial 76184721-K');
				select * into campo1 from financiamiento_financiadores where rut='76184721-K';
				if not found then
					return response_requests_6000('2', 'Financiador no registrado','',json2);
				end if;
			else
				json2:=logjson(json2,'Pagador de la bolsa... y tiene merito');
				--select * into campo1 from financiamiento_financiadores where codigo=campo.financiador;
				select * into campo1 from financiamiento_financiadores where rut='99575550-5';
				if not found then
					return response_requests_6000('2', 'Financiador no registrado','',json2);
				end if;
			end if;
		end if;
	else
		json2:=logjson(json2,'Financiador '||campo.financiador);
		--Leemos la tabla para los datos del financiador
		select * into campo1 from financiamiento_financiadores where codigo=campo.financiador;
		if not found then
			return response_requests_6000('2', 'Financiador no registrado','',json2);
		end if;
	end if;
	
	json2:=put_json(json2,'CORREO_FIN',campo1.correo);

        json2:=put_json(json2,'FINANCIADOR_CODIGO',campo1.codigo);
        json2:=put_json(json2,'rutCesionario',campo1.rut);
        json2:=put_json(json2,'razCesionario',campo1.razon_social);
        json2:=put_json(json2,'mailCesionario',campo1.correo);
        json2:=put_json(json2,'dirCesionario',campo1.direccion);
        --FALTA
        json2:=put_json(json2,'otrasCond','');
        json2:=put_json(json2,'mailDeudor',get_json('mail_usuario',json2));
	--Si la campo.fecha_vencimiento_dte es nula o vacia, tomamos la que envia en usuario
	if (campo.fecha_vencimiento_dte is not null and campo.fecha_vencimiento_dte<>'') then
		json2:=put_json(json2,'fecha_vencimiento',campo.fecha_vencimiento_dte);
	else
		json2:=put_json(json2,'fecha_vencimiento',campo.fecha_pago_cliente::varchar);
	end if;
	if length(get_json('fecha_vencimiento',json2))=0 then
                json2:=put_json(json2,'fecha_vencimiento',get_json('fecha_vencimiento1',json2));
        end if;
	--MVG 20180604 Envia data a cliente
        estado_cliente1:='ACEPTADA';
        mensaje_cliente1:=get_json('razon_rechazo',json2);
        rut_financiador_sin_dv:=replace(split_part(campo.rut_financiador,'-',1),'.','')::integer;
        codigo_financiador:=genera_uri_python(coalesce(campo1.codigo,campo1.rut_sin_dv::varchar));
	/*
        datos_json:=put_json('{}','token',codigo_financiador);
        datos_json:=put_json(datos_json,'cod_transaccion',campo.codigo_txel::varchar||'_'||rut_financiador_sin_dv::varchar);
        datos_json:=put_json(datos_json,'tipo_tx','OFERTA');
        datos_json:=put_json(datos_json,'estado_cliente',estado_cliente1);
        datos_json:=put_json(datos_json,'mensaje_cliente',mensaje_cliente1);
        perform logfile('MVG [pivote_financiamiento_12775] inserto en financiamiento_data_pendientes_x_enviar data_json='||datos_json::varchar);
	if cta_cte1<>'' then
		update financiamiento_solicitudes set id_cta_cte=cta_cte1::bigint, fecha_actualizacion=now() where id=id_fin1::bigint;
		datos_json:=put_json(datos_json,'cuenta_corriente',(select array_to_json(array_agg(row_to_json(sql))) from (select rut_cuenta,banco_cuenta,tipo_cuenta,numero_cuenta,correo_cuenta from financiamiento_cuenta_corriente where id=nullif(cta_cte1,'')::bigint)sql)::varchar);
	end if;
	if campo1.ip is not null and campo1.ip<>'' then
	        insert into financiamiento_data_pendientes_x_enviar (id,fecha,rut_financiador_sin_dv,data_enviar_json,estado,reintentos,fec_ult_reintento,prioridad,flag_pendiente,codigo_txel,uri) values (default,default,rut_financiador_sin_dv,datos_json::varchar,'PENDIENTE',0,default,default,default,campo.codigo_txel,campo.uri);
        	if not found then
                	perform logfile('[finan_comunicacion_notifica_publica] MVG Error al insertar en financiamiento_data_pendientes_x_enviar RUT_FINANCIADOR='||rut_financiador_sin_dv::varchar||' DATA_JSON='||datos_json::varchar);
	        end if;
	end if;
        -- fin
	*/
	--MVG 20190207 Envia data a Financiador
	datos_json:=put_json('{}','ID',id_fin1::varchar);
    	datos_json:=put_json(datos_json,'rutCliente',get_json('rutCliente',json2));
    	datos_json:=put_json(datos_json,'tipo_tx','OFERTA');
    	datos_json:=put_json(datos_json,'estado_cliente','ACEPTADA');
    	datos_json:=put_json(datos_json,'mensaje_cliente',get_json('razon_rechazo',json2));
    	datos_json:=financiamiento_envia_data_financiador(datos_json);
	if get_json('CODIGO_RESPUESTA',split_part(get_json('RESPUESTA',datos_json),chr(10)||chr(10),2)::json)<>'1' then
    	        json2:=logjson(json2,'Error al llamar financiamiento_envia_data_financiador MENSAJE='||get_json('MENSAJE_RESPUESTA',datos_json)||' DATA_JSON='||datos_json);
    	end if;
	
        select * into campo from dte_cesiones where rut_emisor=campo.rut_emisor::integer and tipo_dte=campo.tipo_dte::integer and folio=campo.folio::bigint;
        if found then
                if (campo.estado_sii is not null) then
                        if(campo.estado_sii='ENVIADO_AL_SII' or campo.estado_sii is null) then
                                return response_requests_6000('2', 'Estamos esperando la respuesta del SII de la cesión anterior.','', json2);
                        elsif(strpos(campo.estado_sii,'ACEPTADO')>0) then
                                return response_requests_6000('2', 'Este documento ya fue cedido y fue cedido exitosamente.','', json2);
                        end if;
                end if;
        end if;

        json2:=put_json(json2,'LLAMA_FLUJO','SI');
        json2:=put_json(json2,'__SECUENCIAOK__','12775');

        return json2;

END;
$$
LANGUAGE plpgsql;


CREATE or replace FUNCTION procesa_resp_cesion_12775(json) RETURNS json AS $$
declare
        json1   alias for $1;
        json2   json;
        json3   json;
        campop  json;

        json_resp       json;
        id1     bigint;
	rut_usuario1	varchar;
	rut_emisor1	integer;
	tipo_dte1	integer;
	folio1		bigint;
	fin1	varchar;
	rut_fin1        varchar;
        rut_cesion1     varchar;
	codigo_txel1    varchar;
BEGIN
        json2:=json1;
	json2:=logjson(json2,'ID_FIN='||get_json('ID_FIN',json2));
        json_resp:=split_part(get_json('RESPUESTA',json2),chr(10)||chr(10),2)::json;
        if(get_json('CODIGO_RESPUESTA',json_resp)='1') then
                id1:=replace(get_json('ID_FIN',json2),'.','');
		json2:=logjson(json2,'Marco como CEDIDA ID_FIN='||id1||' '||get_json('ID',json2));
		--Actualizamos el estado de la solicitud
		update financiamiento_solicitudes set estado='CEDIDA',fecha_cesion=now(),fecha_actualizacion=now(),id_cesion=get_json('CODIGO_CESION',json2)::bigint,uri_cesion=get_json('URI_IN',json2) where id=id1::bigint returning rut_usuario_cliente,rut_emisor,tipo_dte,folio,financiador,rut_financiador into rut_usuario1,rut_emisor1,tipo_dte1,folio1,fin1,rut_fin1;
		
		update financiamiento_solicitudes set estado='CEDIDA',fecha_cesion=now(),fecha_actualizacion=now(),id_cesion=get_json('CODIGO_CESION',json2)::bigint,uri_cesion=get_json('URI_IN',json2) where rut_emisor=rut_emisor1 and tipo_dte=tipo_dte1 and folio=folio1 and financiador=fin1;

		update financiamiento_solicitudes set estado='CON_REPAROS',estado_cliente='RECHAZADA',fecha_actualizacion=now() where rut_emisor=rut_emisor1 and tipo_dte=tipo_dte1 and folio=folio1 and financiador<>fin1;
		codigo_txel1:=get_json('CODIGO_TXEL',json2);
                if codigo_txel1='' then
                        json2:=logjson(json2,'NO VIENE CODIGO_TXEL PARA ACTUALIZAR DATA_DTE');
                else
                        update dte_emitidos set data_dte=put_data_dte(data_dte,'CESION',rut_fin1) where codigo_txel=codigo_txel1::bigint;
                end if;
		--Para que se actualize el badge
		perform limpia_menu_sesion_usuario_6000(rut_usuario1);

		--Debemos mandar un mail al financiador para que marque el pago de esta operacion
		json2:=put_json(json2,'__SECUENCIAOK__','30');	
        end if;

        return json2;
END;
$$ LANGUAGE plpgsql;

