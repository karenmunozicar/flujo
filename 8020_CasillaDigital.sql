delete from isys_querys_tx where llave='8020';


--Procesa solo los datos que necesita
insert into isys_querys_tx values ('8020',5,19,1,'select proc_casilla_digital_8020_parte1(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,20);
--Casilla
insert into isys_querys_tx values ('8020',10,42,1,'select proc_casilla_digital_8020_amz(''$$JSON_DATA_CD$$'') as __json__',0,0,0,1,1,-1,20);
--Para Carpeta Legal
insert into isys_querys_tx values ('8020',12,45,1,'select proc_casilla_digital_8020(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,20);
--Llamo al 5075
insert into isys_querys_tx values ('8020',15,1,8,'Llamada 5075 para marcar confirmacion',5075,0,0,0,0,20,20);

insert into isys_querys_tx values ('8020',20,19,1,'select proc_respuesta_5075_8020_json(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,0,0);


CREATE or replace FUNCTION proc_casilla_digital_8020_parte1(json) RETURNS json AS $$
DECLARE
        json1   alias for $1;
        json2   json;
        jres    json;
	texto1	varchar;
	j1	json;
	j3	json;
BEGIN
        json2:=json1;
        json2:=put_json(json2,'__SECUENCIAOK__','0');

	--Viene los datos dentro de esta estructura..
	texto1:=replace(decode(get_xml('Mail',decode(get_json('INPUT',json2),'hex')::varchar),'hex')::varchar,'\\"','\"');
	j1:=texto1::json;

	json2:=logjson(json2,'from='||get_json('from',j1));
	if strpos(get_json('from',j1),'@botsii.cl')>0 then
		json2:=logjson(json2,'Carpeta Legal');
		json2:=put_json(json2,'__SECUENCIAOK__','12');
		return json2;
	end if;
	j3:='{}';
	j3:=put_json(j3,'message-id',get_json('message-id',j1));
	j3:=put_json(j3,'to',get_json('to',j1));
	j3:=put_json(j3,'from',get_json('from',j1));
	j3:=put_json(j3,'subject',get_json('subject',j1));
	j3:=put_json(j3,'files',get_json('files',j1));
	j3:=put_json(j3,'size_mail',get_json('size_mail',j1));
	j3:=put_json(j3,'sub_categoria',get_json('sub_categoria',j1));
	j3:=put_json(j3,'url_html',get_json('url_html',j1));
	j3:=put_json(j3,'url_text',get_json('url_text',j1));
	j3:=put_json(j3,'monto',get_json('monto',j1));
	j3:=put_json(j3,'fecha_vencimiento',get_json('fecha_vencimiento',j1));
	j3:=put_json(j3,'pais',get_json('pais',j1));
	j3:=put_json(j3,'url_boleta',get_json('url_boleta',j1));
	j3:=put_json(j3,'dia_emision',get_json('dia_emision',j1));
	j3:=put_json(j3,'nro_cliente',get_json('nro_cliente',j1));
	j3:=put_json(j3,'mes_emision',get_json('mes_emision',j1));
	j3:=put_json(j3,'fecha_ingreso_cola',get_json('fecha_ingreso_cola',j1));
	j3:=put_json(j3,'NOW',now()::varchar);
	j3:=put_json(j3,'__FLUJO_ACTUAL__',get_json('__FLUJO_ACTUAL__',json2));

	--Si viene de directv, parseamos los datos necesarios desde el XML del Almacen
        if (strpos(decode(get_json('texto_html',j1),'hex')::varchar,'directv')>0) then
		json2:=logjson(json2,'Ejecutamos funcion de directv');
		j3:=finput_directv(put_json(j3,'texto_html',get_json('texto_html',j1)));
		json2:=logjson(json2,get_json('_LOG_',j3));
		if get_json('STATUS_XML',j3)='FALLA' then
			json2:=put_json(json2,'RESPUESTA','Status: 400 NK');
                	json2:=sp_procesa_respuesta_cola_motor_original_json(json2);
			return json2;
		end if;
        end if;

	j3:=put_json(j3,'__FLUJO_ACTUAL__','F_8020:SEC_10:BD_42:QUERY');
	json2:=put_json(json2,'JSON_DATA_CD',j3::varchar);
        json2:=put_json(json2,'__SECUENCIAOK__','10');
	json2:=put_json(json2,'INPUT','');
	return json2;
END;
$$ LANGUAGE plpgsql;




CREATE or replace FUNCTION proc_respuesta_5075_8020_json(json) RETURNS json AS $$
DECLARE
        json1   alias for $1;
        json2   json;
	jres	json;
BEGIN
        json2:=json1;
	json2:=put_json(json2,'__SECUENCIAOK__','0');
	BEGIN
		jres:=split_part(get_json('RESPUESTA',json2),chr(10)||chr(10),2)::json;
	EXCEPTION WHEN OTHERS THEN
		json2:=logjson(json2,'Falla Respuesta 5075 RESPUESTA='||get_json('RESPUESTA',json2));
		json2:=put_json(json2,'RESPUESTA','Status: 400 NK');
		json2:=sp_procesa_respuesta_cola_motor_original_json(json2);
		return json2;
	END;
	json2:=logjson(json2,'Respuesta 5075'||jres::varchar);
	if get_json('CODIGO_RESPUESTA',jres)='1' then
		json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
	else
		json2:=put_json(json2,'RESPUESTA','Status: 400 NK');
	end if;
	json2:=sp_procesa_respuesta_cola_motor_original_json(json2);
	return json2;
END;
$$ LANGUAGE plpgsql;



CREATE or replace FUNCTION proc_casilla_digital_8020(json) RETURNS json AS $$
DECLARE
	json1	alias for $1;
	json2	json;
	sts1	varchar;
	respuesta1 varchar;
BEGIN
	json2:=json1;
	--perform logfile('select cd_graba_attach('''||json2::varchar||''');');
	json2:=cd_graba_attach(json2);
	--cd_graba_attach=OK BOLETA_ENTEL_PCS_SA--BOLETA-
	if (strpos(get_json('CD_STATUS',json2),'OK ')>0 or strpos(get_json('CD_STATUS',json2),'ID_YA_PROCESADO')>0) then
		--json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
		respuesta1:=put_json('{}','CODIGO_RESPUESTA','1');
		json2:=put_json(json2,'RESPUESTA','HTTP/1.1 200 OK'||chr(10)||'Content-Length: '||length(respuesta1)::varchar||chr(10)||chr(10)||respuesta1);
		--Solo para los correos de Acepta
		if (strpos(get_json('msg-id',json2),'<ACP')>0 or strpos(get_json('msg-id',json2),'<JCP')>0) then
			json2:=logjson(json2,'CD se envia confirmacion a send_mail(5075)');
			--Armo el json para la confirmacion
			json2:=put_json(json2,'delivery-status','{"Status":"2.0.0","Diagnostic-Code":"smtp; 250 2.0.0:  Message accepted", "Final-Recipient":"rfc822; '||get_json('to',json2)||'", "X-Postfix-Sender":"rfc822; confirmacion_envio@custodium.com", "Action":"delivered", "Original-Recipient":"rfc822;'||get_json('to',json2)||'", "Arrival-Date":"'||to_char(now() AT TIME ZONE 'GMT','Dy, DD Mon YYYY HH24:MI:SS GMT')||'", "Remote-MTA":"dns; casilladigital.cl", "Reporting-MTA":"dns; smtp.casilladigital.cl"}');
			json2:=put_json(json2,'rfc822-headers','{"Message-ID":"'||get_json('msg-id',json2)||'","From":"'||get_json('from',json2)||'","To":"'||get_json('to',json2)||'","Subject":"'||get_json('subject',json2)||'"}');
			json2:=put_json(json2,'INPUT','');
			json2:=put_json(json2,'__SECUENCIAOK__','15');
		else
			json2:=put_json(json2,'__SECUENCIAOK__','20');
		end if;
	else
		--json2:=put_json(json2,'RESPUESTA','Status: 400 NK');
		respuesta1:=put_json('{}','CODIGO_RESPUESTA','2');
		json2:=put_json(json2,'RESPUESTA','HTTP/1.1 400 NK'||chr(10)||'Content-Length: '||length(respuesta1)::varchar||chr(10)||chr(10)||respuesta1);
		json2:=put_json(json2,'__SECUENCIAOK__','20');
	end if;
	json2:=logjson(json2,'RESP cd_graba_attach='||get_json('CD_STATUS',json2));

	return json2;
END;
$$ LANGUAGE plpgsql;

