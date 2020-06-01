--Publica documento
delete from isys_querys_tx where llave='12778';

insert into isys_querys_tx values ('12778',30,19,1,'select solicita_id_ecm_12778(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('12778',100,1,1,'select sp_procesa_respuesta_cola_motor_original_json(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,0,0);

CREATE or replace FUNCTION solicita_id_ecm_12778(json) RETURNS json
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
        subject1:='Rechazo Oferta para '||get_json('FINANCIADOR_CODIGO',json2)||' de Factura Emisor:'||get_json('razon_social',json2)||'  y Pagador:'||get_json('razon_pagador',json2);
        json3:=put_json(json3,'subject',subject1);
        json3:=put_json(json3,'from','Financiamiento Acepta<noreply@acepta.com>');
        patron1:=pg_read_file('./patron_correos/mail_respuesta_financiamiento.html');
        json3:=put_json(json3,'CABECERA','Rechazo de Oferta');
        json3:=put_json(json3,'ESTIMADO',get_json('FINANCIADOR_CODIGO',json2));
	
	juri:=put_json('{}','id_fin',get_json('ID_FIN',json2));
        juri:=put_json(juri,'codigo_txel',get_json('CODIGO_TXEL_FIN',json2));
        hash1:=encripta_hash_evento_VDC2(juri::varchar);

        json3:=put_json(json3,'MENSAJE1','El cliente '||get_json('razon_social',json2)||' ha rechazado su oferta de financiar la operacion.<br>Motivo de Rechazo:'||get_json('razon_rechazo',json2)||'<br>El documento es : <a href="'||get_json('URI_IN',json2)||'">Documento</a><br>');
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
		--Si viene de pantalla se contesta, sino se envia a borrar
		if get_json('__COLA_MOTOR__',json2)<>'' then
        		json2:=put_json(json2,'__SECUENCIAOK__','100');
			json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
		        return json2;
		else
        		json2:=put_json(json2,'__SECUENCIAOK__','0');
		        return response_requests_6000('1', 'Solicitud Rechazada','',json2);
		end if;
        else
                json2:=logjson(json2,'Falla envio de Solicitud ');
		--Si viene de pantalla se contesta, sino se envia a borrar
		if get_json('__COLA_MOTOR__',json2)<>'' then
        		json2:=put_json(json2,'__SECUENCIAOK__','100');
			json2:=put_json(json2,'RESPUESTA','Status: 400 NK');
		        return json2;
		else
        		json2:=put_json(json2,'__SECUENCIAOK__','0');
		        return response_requests_6000('1', 'Falla Rechazo de Solicitud','',json2);
		end if;
        end if;
END;
$$
LANGUAGE plpgsql;

CREATE or replace FUNCTION pivote_rechazo_financiamiento_12778(json) RETURNS json
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
	accion1	varchar;
	datos_json      json;
        estado_cliente1 varchar;
        mensaje_cliente1 varchar;
        rut_financiador_sin_dv  integer;
        tipo_tx1        varchar;
        codigo_financiador      varchar;
BEGIN
	json2:=json1;
        json2:=json1;

        id_fin1:=replace(get_json('ID',json2),'.','');
        if(is_number(id_fin1) is false) then
		id_fin1:=replace(get_json('id_tabla_detalle',json2),'.','');
                if(is_number(id_fin1) is false) then
                        return response_requests_6000('2', 'ID Inválido','',json2);
                end if;
        end if;

/*
        if(is_number(get_json('rutUsuario',json2)) is false) then
                return response_requests_6000('2', 'Usuario Inválido','',json2);
        end if;
*/

        select * into campo from financiamiento_solicitudes where id=id_fin1::bigint;
        if not found then
                return response_requests_6000('2', 'Solicitud no encontrada '||id_fin1::varchar,'',json2);
        end if;
	json2:=put_json(json2,'ID_FIN',id_fin1::varchar);
	json2:=put_json(json2,'CODIGO_TXEL_FIN',campo.codigo_txel::varchar);

	json2:=put_json(json2,'pagador',campo.rut_receptor::varchar);
        json2:=put_json(json2,'razon_pagador',(select nombre from contribuyentes where rut_emisor=campo.rut_receptor));

	accion1:='RFIN';
	if get_json('tipo_tx',json2)='financiamiento_aceptar_pre_oferta' then
		accion1:='ACPREFIN';
	end if;
	--Marcamos el dte_emitidos como rechazado por el cliente
	--update dte_emitidos set data_dte=financiamiento_put_data_dte(data_dte,accion1,id_fin1::varchar)  where codigo_txel=campo.codigo_txel;
	update dte_emitidos set data_dte=put_data_dte(data_dte,accion1,coalesce(get_xml(accion1,data_dte),'')||'-'||split_part(campo.rut_financiador,'-',1)||'-')  where codigo_txel=campo.codigo_txel;

	--Si viene Comentario lo registramos ...
	if get_json('razon_rechazo',json2)<>'' then
		insert into financiamiento_documentos values(id_fin1::bigint,default,'FINTECH_CLIENTE',null,null,now(),null,null,get_json('razon_rechazo',json2));
	end if;

	--Leemos la tabla para los datos del financiador
	select * into campo1 from financiamiento_financiadores where codigo=campo.financiador;
	if not found then
		return response_requests_6000('2', 'Financiador no registrado','',json2);
	end if;


	--MVG 20180604 Envia data a cliente
        if campo.estado='PRE-OFERTA' then
                tipo_tx1:='PREOFERTA';
        else
                tipo_tx1:='OFERTA';
        end if;
        if accion1='RFIN' then
                estado_cliente1:='RECHAZADA';
        else
                estado_cliente1:='ACEPTADA';
        end if;
        mensaje_cliente1:=get_json('razon_rechazo',json2);
        rut_financiador_sin_dv:=replace(split_part(campo.rut_financiador,'-',1),'.','')::integer;
        codigo_financiador:=genera_uri_python(coalesce(campo.financiador,split_part(campo.rut_financiador,'-',1)));
	/*
        datos_json:=put_json('{}','token',codigo_financiador);
        datos_json:=put_json(datos_json,'cod_transaccion',campo.codigo_txel::varchar||'_'||rut_financiador_sin_dv::varchar);
        datos_json:=put_json(datos_json,'tipo_tx',tipo_tx1);
        datos_json:=put_json(datos_json,'estado_cliente',estado_cliente1);
        datos_json:=put_json(datos_json,'mensaje_cliente',mensaje_cliente1);
        perform logfile('MVG [pivote_financiamiento_12775] inserto en financiamiento_data_pendientes_x_enviar data_json='||datos_json::varchar);
        insert into financiamiento_data_pendientes_x_enviar (id,fecha,rut_financiador_sin_dv,data_enviar_json,estado,reintentos,fec_ult_reintento,prioridad,flag_pendiente,codigo_txel,uri) values (default,default,rut_financiador_sin_dv,datos_json::varchar,'PENDIENTE',0,default,default,default,campo.codigo_txel,campo.uri);
        if not found then
                perform logfile('[finan_comunicacion_notifica_publica] MVG Error al insertar en financiamiento_data_pendientes_x_enviar RUT_FINANCIADOR='||rut_financiador_sin_dv::varchar||' DATA_JSON='||datos_json::varchar);
        end if;
        -- fin
	*/
	--MVG 20190207 Envia data a Financiador
	datos_json:=put_json('{}','ID',id_fin1::varchar);
        datos_json:=put_json(datos_json,'rutCliente',get_json('rutCliente',json2));
        datos_json:=put_json(datos_json,'tipo_tx',tipo_tx1);
        datos_json:=put_json(datos_json,'estado_cliente',estado_cliente1);
        datos_json:=put_json(datos_json,'mensaje_cliente',get_json('razon_rechazo',json2));
        datos_json:=financiamiento_envia_data_financiador(datos_json);
        if get_json('CODIGO_RESPUESTA',split_part(get_json('RESPUESTA',datos_json),chr(10)||chr(10),2)::json)<>'1' then
                json2:=logjson(json2,'Error al llamar financiamiento_envia_data_financiador MENSAJE='||get_json('MENSAJE_RESPUESTA',datos_json)||' DATA_JSON='||datos_json);
        end if;
	
	if accion1='RFIN' then
		json2:=bitacora10k(json2,'RECHAZA_OFERTA','Rechaza Oferta de '||campo.financiador||' para la Factura Folio '||campo.folio::varchar);
		update financiamiento_solicitudes set estado='CON_REPAROS',estado_cliente='RECHAZADA',fecha_actualizacion=now(),mensaje_cliente=get_json('razon_rechazo',json2) where id=id_fin1::bigint;
	else
		json2:=bitacora10k(json2,'PRE-ACEPTA_OFERTA','Rechaza Oferta de '||campo.financiador||' para la Factura Folio '||campo.folio::varchar);
		update financiamiento_solicitudes set estado='PRE-OFERTA-ACEPTADA',estado_cliente='ACEPTADA',fecha_actualizacion=now(),mensaje_cliente=get_json('razon_rechazo',json2) where id=id_fin1::bigint;
		return response_requests_6000('1', 'Pre-Oferta Aceptada','',json2);
	end if;

	perform limpia_menu_sesion_usuario_6000(campo.rut_usuario_cliente);

        json2:=put_json(json2,'FOLIO',campo.folio::varchar);
        json2:=put_json(json2,'folio',campo.folio::varchar);
        json2:=put_json(json2,'TIPO',campo.tipo_dte::varchar);
        json2:=put_json(json2,'tipoDte',campo.tipo_dte::varchar);
        json2:=put_json(json2,'URI_IN',campo.uri);
        json2:=put_json(json2,'rutCliente',campo.rut_emisor::varchar);
	
	json2:=logjson(json2,'Financiador '||campo.financiador);
	
	json2:=put_json(json2,'CORREO_FIN',campo1.correo);
        json2:=put_json(json2,'FINANCIADOR_CODIGO',campo1.codigo);
        json2:=put_json(json2,'rutCesionario',campo1.rut);
        json2:=put_json(json2,'razCesionario',campo1.razon_social);
        json2:=put_json(json2,'mailCesionario',campo1.correo);
        json2:=put_json(json2,'dirCesionario',campo1.direccion);
        --FALTA
        json2:=put_json(json2,'LLAMA_FLUJO','SI');
        json2:=put_json(json2,'__SECUENCIAOK__','12778');
        return json2;

END;
$$
LANGUAGE plpgsql;
