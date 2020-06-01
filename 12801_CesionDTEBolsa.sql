--Publica documento
delete from isys_querys_tx where llave='12801';
--Pivote
--pivote_bolsa_cesion_12801

--Flujo Cesion
insert into isys_querys_tx values ('12801',10,1,8,'Llamada CESION JSON',12797,0,0,0,0,20,20);

insert into isys_querys_tx values ('12801',20,1,1,'select procesa_resp_bolsa_cesion_12801(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

CREATE or replace FUNCTION procesa_resp_bolsa_cesion_12801(json) RETURNS json AS $$
declare
	json1	alias for $1;
	json2	json;
	json3	json;
	campop  json;

	json_resp	json;
	id1	bigint;
BEGIN
	json2:=xml1;
	json_resp:=split_part(get_json('RESPUESTA',json2),chr(10)||chr(10),2)::json;
	if(get_json('CODIGO_RESPUESTA',json_resp)='1') then
		id1:=replace(get_json('ID_OFER',json2),'.','');

		--Avanzamos wf
		campop:=get_json('PARAMETRO_BASE_MOTOR',json2)::parametros_motor;
		json3:='{}';
		json3:=put_json(json3,'rutCliente',get_json('rutCliente',json2));
		json3:=put_json(json3,'rutUsuario',get_json('rutUsuario',json2));
		json3:=put_json(json3,'aplicacion','BOLSA_PRODUCTOS');
		json3:=put_json(json3,'perfil','ClienteBolsa');
		json3:=put_json(json3,'wf_cod_respuesta','1');
		json3:=put_json(json3,'wf_id_solicitud',get_json('ID_SOLICITUD',json2));
		json3:=put_json(json3,'wf_id_pendiente',get_json('ID_PENDIENTE',json2));

		json3:=query_db_json(campop.host,campop.port::integer,'select wf_avanza_solicitud_tienda('''||json3::varchar||''')');

		if(get_json('STATUS',json3)='OK') then
			if(get_json('WF_CODIGO_RESPUESTA',get_json('wf_avanza_solicitud_tienda',json3)::json)='1') then
				id_pen1:=get_json('id_pendiente',get_json('pendiente',get_json('wf_pendientes',get_json('wf_avanza_solicitud_tienda',json3)::json)::json)::json);
				if(is_number(id_pen1)) then
					update bolsa_ofertas set id_pendiente=id_pen1::bigint,estado='CEDIDO' where id_dte=id_dte1;
					return response_requests_6000('1', 'Documento Cediso', '',json2);
				else
					return response_requests_6000('2', 'Falla Registro', '',json2);
				end if;
			else
				return response_requests_6000('2', 'Falla Registro..', '',json2);
			end if;
		else
			return response_requests_6000('2', '.Falla Registro.', '',json2);
		end if;
	else
		return response_requests_6000('2', 'Falla Cesion','',json2);
	end if;

   	return json2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION pivote_bolsa_cesion_12801(json) RETURNS json AS $$
declare
        json1    alias for $1;
        json2   json;
	id_dte1	varchar;
	id1	varchar;
	campo	record;
	campo1	record;
BEGIN
        json2:=json1;

	id_dte1:=replace(get_json('ID_DTE',json2),'.','');
	if(is_number(id_dte1) is false)
		return response_requests_6000('2', 'ID Inválido','',json2);
	end if;
	id1:=replace(get_json('ID_OFER',json2),'.','');
	if(is_number(id1) is false)
		return response_requests_6000('2', 'ID Inválido','',json2);
	end if;
	
	if(is_number(get_json('rutUsuario',json2)) is false)
		return response_requests_6000('2', 'Usuario Inválido','',json2);
	end if;
	
	select * into campo1 from bolsa_ofertas where id=id1;
	if not found then
		return response_requests_6000('2', 'Oferta no encontrada','',json2);
	end if;
	json2:=put_json(json2,'ID_SOLICITUD',campo.id_solicitud::varchar);
	json2:=put_json(json2,'ID_PENDIENTE',campo.id_pendiente::varchar);
	
	select * into campo1 from bolsa_dte where id=id_dte1::bigint;	
	if not found then
		return response_requests_6000('2', 'Documento no encontrado','',json2);
	end if;

	json2:=put_json(json2,'FOLIO',campo.folio::varchar);
	json2:=put_json(json2,'TIPO',campo.tipo_dte::varchar);
	json2:=put_json(json2,'URI_IN',campo.uri);
	json2:=put_json(json2,'rutCliente',campo.rut_emisor::varchar);
	json2:=put_json(json2,'pass',encode(desencripta_hash_evento_VDC(campo.hash_cliente)::bytea,'hex')::varchar);
	json2:=put_json(json2,'montoCesion',campo.monto_total);
	json2:=put_json(json2,'rutCesionario','99575550-5');
	json2:=put_json(json2,'razCesionario','BOLSA DE PRODUCTOS DE CHILE BOLSA DE PRODUCTOS AGROPECUARIOS S A');
	json2:=put_json(json2,'mailCesionario','facturas@bolsadeproductos.cl');
	json2:=put_json(json2,'dirCesionario','huérfanos 770 piso 14');
	--FALTA
	json2:=put_json(json2,'otrasCond','');
	json2:=put_json(json2,'mailDeudor','');


        select * into campo from dte_cesiones where rut_emisor=campo.rut_emisor and tipo_dte=campo.tipo_dte and folio=campo.folio;
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
        json2:=put_json(json2,'__SECUENCIAOK__','83');

   	return json2;
END;
$$ LANGUAGE plpgsql;

