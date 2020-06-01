delete from isys_querys_tx where llave='14800';

insert into isys_querys_tx values ('14800',10,9,1,'select agrega_accion_automatica_14800(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,15);

insert into isys_querys_tx values ('14800',20,1,8,'Llamada SUBIR FIRMA',12793,0,0,0,0,30,30);

insert into isys_querys_tx values ('14800',30,9,1,'select resp_agrega_accion_automatica_14800(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,15);

CREATE OR REPLACE FUNCTION agrega_accion_automatica_14800(json)
  RETURNS json AS
$$
DECLARE
    json1               alias for $1;
    json2               json;
    campo               record;
    id1		varchar;
	empresa1	varchar;
BEGIN
        json2:=json1;
	json2:=put_json(json2,'__SECUENCIAOK__','0');
	--Si la accion es desactivar...
	if (get_json('tipo_tx',json2)='tx_desactivar_accion') then
		id1:=replace(get_json('ID',json2),'.','');
        	empresa1:=get_json('rutCliente',json2);
		json2:=bitacora10k(json2,'CONTROLLER','Se desactiva la regla registra clave para accion Automatica Controller');
		--update controller_cabecera_regla_10k set accion_automatica=null,usuario_accion=null,fecha_accion=null,accion_evento=null where id=id1::integer and rut_empresa=empresa1::integer;	
		update controller_cabecera_regla_10k set accion_automatica=null,usuario_accion=null,fecha_accion=null,accion_evento=null where id=id1::integer and rut_empresa=empresa1::integer;	
		if not found then
			return response_requests_6000('2','Falla desactivar Accion','',json2);
		else
			return response_requests_6000('1','Accion Desactivada','',json2);
		end if;
	end if;	
	--Para que el flujo de verificacion de firma solo haga test
	json2:=put_json(json2,'LLAMA_FLUJO_SECUENCIA','30');		
	json2:=put_json(json2,'__SECUENCIAOK__','20');
	json2:=put_json(json2,'rut_firma',get_json('rutUsuario',json2));
	json2:=put_json(json2,'PASS',get_json('hash',json2));
        return json2;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION resp_agrega_accion_automatica_14800(json)
  RETURNS json AS
$$
DECLARE
    json1               alias for $1;
    json2               json;
    campo               record;
    pass1	varchar;
	json3	json;
	empresa1	varchar;
	rut_usuario1	varchar;
	id1		varchar;
	j3	json;
BEGIN
        json2:=json1;

	--Sacamos la respuesa del test
	json3:=split_part(get_json('RESPUESTA',json2),chr(10)||chr(10),2)::json;
	if (get_json('CODIGO_RESPUESTA',json3)='2') then
		return json2;
	end if;

	--Ya se que el pass esta correcto
        --pass1:=genera_pass(get_json('hash',json2));
	--Ya se que el pass esta correcto
        if(get_json('tipo_tx',json2)='tx_guardar_password_nc_automatica') then
                pass1:=genera_pass_limpio(get_json('hash',json2));
        else
		pass1:=genera_pass(get_json('pass',json2));
        end if;
        empresa1:=get_json('rutCliente',json2);
        rut_usuario1:=get_json('rutUsuario',json2);
        --Verificamos que no exista la clave
        select * into campo from rut_firma_clave where rut_emisor=empresa1::integer and rut_firmante=rut_usuario1;
        if not found then
                --Lo inserta
                insert into rut_firma_clave (fecha_creacion,rut_emisor,rut_firmante,clave) values (now(),empresa1::integer,rut_usuario1,pass1);
		json2:=bitacora10k(json2,'CONTROLLER','Se registra clave para accion Automatica Controller');
        else
		update rut_firma_clave set clave=pass1 where rut_emisor=empresa1::integer and rut_firmante=rut_usuario1;
		json2:=bitacora10k(json2,'CONTROLLER','Se acutaliza clave para accion Automatica Controller');
        end if;

	id1:=replace(get_json('ID',json2),'.','');
        if (get_json('tipo_tx',json2)='tx_agregar_rechazo') then
		--Actualizo la tabla de caberca del controller para que genere los rechazos
		update controller_cabecera_regla_10k set usuario_accion=rut_usuario1,fecha_accion=now(),accion_evento='rechazo_comercial' where id=id1::integer and rut_empresa=empresa1::integer;
		if not found then
			return response_requests_6000('2','Error al registrar la accion','',json2);
		end if;
        elsif (get_json('tipo_tx',json2)='tx_agregar_aprobacion') then
		update controller_cabecera_regla_10k set usuario_accion=rut_usuario1,fecha_accion=now(),accion_evento='aprobacion_comercial' where id=id1::integer and rut_empresa=empresa1::integer;
		if not found then
			return response_requests_6000('2','Error al registrar la accion','',json2);
		end if;
	elsif (get_json('tipo_tx',json2)='tx_agregar_aceptar') then
		if (get_json('checkACD',json2)='') then
			return response_requests_6000('2','Debe aceptar las condiciones','',json2);
		end if;
		j3:=put_json('{}','accion_arm',get_json('accion_arm',json2));
		j3:=put_json(j3,'tipoAceptacion',get_json('tipoAceptacion',json2));
		j3:=put_json(j3,'rbtn_sii',get_json('rbtn_sii',json2));
		j3:=put_json(j3,'rbtn_emi',get_json('rbtn_emi',json2));
		j3:=put_json(j3,'checkACD',get_json('checkACD',json2));
		j3:=put_json(j3,'glosaEstado',get_json('glosaEstado',json2));
		update controller_cabecera_regla_10k set usuario_accion=rut_usuario1,fecha_accion=now(),accion_evento='aceptar',accion_automatica=j3 where id=id1::integer and rut_empresa=empresa1::integer;
		if not found then
			return response_requests_6000('2','Error al registrar la accion','',json2);
		end if;
	elsif (get_json('tipo_tx',json2)='tx_agregar_reclamar') then
		if (get_json('checkACD',json2)='') then
			return response_requests_6000('2','Debe aceptar las condiciones','',json2);
		end if;
		j3:=put_json('{}','accion_arm',get_json('accion_arm',json2));
		j3:=put_json(j3,'tipoAceptacion',get_json('tipoAceptacion',json2));
		j3:=put_json(j3,'rbtn_sii',get_json('rbtn_sii',json2));
		j3:=put_json(j3,'rbtn_emi',get_json('rbtn_emi',json2));
		j3:=put_json(j3,'checkACD',get_json('checkACD',json2));
		j3:=put_json(j3,'glosaEstado',get_json('glosaEstado',json2));
		update controller_cabecera_regla_10k set usuario_accion=rut_usuario1,fecha_accion=now(),accion_evento='reclamar',accion_automatica=j3 where id=id1::integer and rut_empresa=empresa1::integer;
		if not found then
			return response_requests_6000('2','Error al registrar la accion','',json2);
		end if;
	elsif(get_json('tipo_tx',json2)='tx_guardar_password_nc_automatica') then
                update controller_cabecera_regla_10k set usuario_accion=rut_usuario1,fecha_accion=now(),accion_evento='nc_automatica' where id=id1::integer and rut_empresa=empresa1::integer;
                if not found then
                        return response_requests_6000('2','Error al registrar la accion','',json2);
                end if;
        else
                return response_requests_6000('2','Tx no definida','',json2);
        end if;

        return response_requests_6000('1','Accion Habilitada Correctamente','',json2);
END
$$ LANGUAGE plpgsql;


