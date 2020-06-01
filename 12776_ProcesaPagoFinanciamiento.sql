--Publica documento
delete from isys_querys_tx where llave='12776';

--Arma lista de mails y genera contador en 0
insert into isys_querys_tx values ('12776',10,19,1,'select solicita_id_ecm_12776(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

--Armo correo para el mail[contador]
insert into isys_querys_tx values ('12776',20,9,1,'select verifica_envio_12776(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);


CREATE or replace FUNCTION pivote_financiamiento_12776(json) RETURNS json
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
	folio1	bigint;
	rut_cliente1	bigint;
	codigo1	bigint;	
BEGIN
	json2:=json1;
	json2:=put_json(json2,'LLAMA_FLUJO','SI');
	json2:=put_json(json2,'__SECUENCIAOK__','0');
        id1:=get_json('id_fin',desencripta_hash_evento_vdc2(get_json('hash',json2))::json)::bigint;

        update financiamiento_solicitudes set estado='PAGADO',monto_pago=get_json('monto_pago',json2)::bigint,fecha_pago=get_json('fecha_pago',json2)::date,fecha_actualizacion=now() where id=id1 returning rut_usuario_cliente,codigo_txel,financiador,folio,rut_emisor into rut1,codigo1,fin1,folio1,rut_cliente1;
        perform limpia_menu_sesion_usuario_6000(rut1);
        update dte_emitidos set data_dte=coalesce(data_dte,'')||'<FINTECHPAGO>'||get_json('fecha_pago',json2)||'</FINTECHPAGO>' where codigo_txel=codigo1::bigint;

	json2:=put_json(json2,'para',(select mail from user_10k where rut_usuario=rut1 limit 1));
	json2:=put_json(json2,'FINANCIADOR',fin1);
	json2:=put_json(json2,'RUT_CLIENTE',rut_cliente1::varchar);
	json2:=put_json(json2,'FOLIO',folio1::varchar);
	json2:=put_json(json2,'__SECUENCIAOK__','12776');
	return json2;
END;
$$
LANGUAGE plpgsql;



CREATE or replace FUNCTION solicita_id_ecm_12776(json) RETURNS json
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

        --Inserto la solicitud
        json3:='{}';
        json3:=put_json(json3,'LINK','https://escritorio.acepta.com/');
        --json3:=put_json(json3,'evento_lma',uri_short1);
        json3:=put_json(json3,'ID_ECM',id_ecm1);
        json3:=put_json(json3,'tipo_envio','HTML');
        subject1:='Pago '||get_json('FINANCIADOR',json2)||' por Factura Folio='||get_json('FOLIO',json2);
        json3:=put_json(json3,'subject',subject1);
        json3:=put_json(json3,'from','Financiamiento Acepta<noreply@acepta.com>');
        patron1:=pg_read_file('./patron_correos/mail_respuesta_financiamiento.html');
        json3:=put_json(json3,'CABECERA','Pago Factura');
        json3:=put_json(json3,'ESTIMADO','Cliente');
        json3:=put_json(json3,'MENSAJE1','El Financiador '||get_json('FINANCIADOR',json2)||' ha informado el pago a su factura.<br>Por favor revise la documentacion en nuestro portal <a href="https://escritorio.acepta.com/">Escritorio Acepta DTE</a>');
        json3:=put_json(json3,'MENSAJE3','');
        json3:=put_json(json3,'CATEGORIA','FINANCIAMIENTO');
        json3:=put_json(json3,'RUT_OWNER',get_json('RUT_CLIENTE',json2));

        json3:=put_json(json3,'to',get_json('para',json2));
        patron2=remplaza_tags_json_c(json3,patron1);
        json3:=put_json(json3,'content_html',encode(patron2::bytea,'hex'));
        jsonsts1:=send_mail_python2_colas(json3::varchar);
        if (get_json('status',jsonsts1)='OK') then
		json2:=logjson(json2,'Mail Info Pago Enviada OK');
		json2:=put_json(json2,'__SECUENCIAOK__','20');
        else
		json2:=logjson(json2,'Falla envio mail de info pago ');
		json2:=put_json(json2,'__SECUENCIAOK__','0');
        end if;

	return json2;
END;
$$
LANGUAGE plpgsql;


CREATE or replace FUNCTION verifica_envio_12776(json) RETURNS json
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
	json2:=put_json(json2,'__SECUENCIAOK__','0');
	--codigo1:=get_json('codigo_txel',get_json('jemi',json2)::json)::bigint;
	--Limpiamos el menu del usuario para que se refresque el badge
	perform limpia_menu_sesion_usuario_6000(get_json('RUT_USUARIO_CLIENTE',json2));

	uri_default1:= coalesce((select href||'%26awsfec=PAGADO%26' from menu_info_10k where id2='grilla_menu_financiamiento'),'');

	--Mostramos un mensaje en la pantalla del usuario
	insert into mensajes_pantalla values (get_json('CLIENTE',json2),get_json('RUT_USUARIO_CLIENTE',json2),'Estimado Cliente:<br>El financiador '||get_json('FINANCIADOR',json2)||', ha informado el depósito de la factura '||get_json('FOLIO',json2)||'.<br>Por favor revise en el menú de "Financiamiento" opción "Operaciones Finalizadas".<br>Atte.<br>Acepta.<br>',now(),now(),now()+interval '1 month','DTE','yesno','borrar_mensaje_pantalla',default,null,'FINANCIAMIENTO',uri_default1) returning id into id_mensaje1;
	
	--Guardamos el id del mensaje para borrarlo, cuando lo vea el usuario	
        return response_requests_6000('1','Pago Ingresado','{"SHOW_ALERT":"NO","URL_RESPUESTA":"https://escritorio.acepta.com/mensaje.php?mensaje='||encode('Pago Ingresado','hex')||'"}',json2);
END;
$$
LANGUAGE plpgsql;

