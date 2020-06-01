delete from isys_querys_tx where llave='14766';
--Obtiene el DTE Original con la entrada URI_IN
insert into isys_querys_tx values ('14766',10,1,1,'select genera_sesion_10k_14766(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('14766',20,1,2,'Servicio de Alianza para Sesion',4013,100,101,0,0,30,30);

insert into isys_querys_tx values ('14766',30,1,1,'select verifica_sesion_10k_14766(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('14766',40,1,1,'select servicio_validacion_14766(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);


CREATE or replace FUNCTION servicio_validacion_14766(json) RETURNS json as $$
DECLARE
    json1               alias for $1;
    json2               json;
	campo 	record;
	rut1	varchar;
	sesion1	varchar;
BEGIN
        json2:=json1;
	rut1:=replace(split_part(get_json('rutUsuario',json2),'-',1),'.','');
	sesion1:=get_json('session_id',json2);

	select * into campo from sesion_web_10k where sesion=sesion1 and rut_usuario=rut1;
	if not found then
		return response_requests_6000('2','Sesión Inválida','',json2);
	else
		return response_requests_6000('1','OK','',json2);
	end if; 
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION genera_sesion_10k_14766(json) RETURNS json as $$
DECLARE
    json1               alias for $1;
    json2               json;
        json3   json;
        json4   json;
        respuesta1      varchar;
	data1	json;
	campo	record;
	campo1	record;
	rut1	varchar;
	empresa1	varchar;
	json_par1	json;
	dominio1	varchar;
BEGIN
        json2:=json1;

	dominio1:=split_part(split_part(get_json('host_canal',json2),'.',1),'_',1);
	json2:=get_parametros_motor_json(json2,upper(dominio1));
	perform logfile('DAO parametros motor alianzas ' || json2::varchar);

/*	--Solo para IP de la ALIANZA
	if (get_json('REMOTE_ADDR',json2)<>get_json('__IP_CONEXION_CLIENTE__',json_par1)) then
		json2:=logjson(json2,'IP no habilitada para consultar para alianza='||dominio1);
		json4:='{"Status":"Servicio no Habilitado"}';
        	respuesta1:='Status: 404' ||chr(10)||'Content-type: json'||chr(10)||'Content-Length: '||octet_length(json4::varchar)::varchar||chr(10)||chr(10)||json4;
        	json2:=put_json(json2,'RESPUESTA',respuesta1);
        	return json2;
	end if;
*/
	rut1:=get_json('rut',json2);
	empresa1:=get_json('empresa',json2);

	if(get_json('tipo_tx',json2)='servicio_valida_sesion') then
		json2:=put_json(json2,'__SECUENCIAOK__','40');
		return json2;
	end if;


	--Verirficamos si el usuario y la empresa tiene relacion con acepta
	select * into campo from menu_10k where rut_usuario=rut1 and empresa=empresa1;
	if not found then
		json3:='{}';
		json3:=put_json(json3,'url_default','https://'||get_json('host_canal',json2)||'/tienda');
		perform logfile('DAO Flujo 14766-2');
		json2:=logjson(json2,'Rut no habilitado para entrar esa empresa. Alianza='||dominio1);
		json2:=response_requests_6000('2','Rut sin permisos para esta empresa',json3::varchar,json2);
        	return json2;
	end if;
	--Si es solo un logIn
        if(get_json('__FLAG_AUTENTIFICACION__',json2)='SI') then
                json2:=logjson(json2,'Se llama al login sesion externa - __FLAG_AUTENTIFICACION__');
                return log_in_6000(json2);
        end if;

	--perform logfile('DAO Flujo 14766-1');

	--Buscamos el dominio que quiere ingresar para ver si esta definido en el maestro de clientes alianzas
	select * into campo from maestro_clientes_alianzas where dominio=dominio1;
	if not found then
		json3:='{}';
		json3:=put_json(json3,'url_default','https://'||get_json('host_canal',json2)||'/tienda');
		json2:=logjson(json2,'ALIANZA: Dominio no registrado en maestro_cliente_alianzas');
		json2:=response_requests_6000('2','Sesion Alianza invalida',json3::varchar,json2);
        	return json2;
	end if;

	--Verificamos que la empresa pertenezca al canal
	select * into campo1 from maestro_clientes where rut_emisor=empresa1::integer and rut_factura=campo.rut_emisor;
	if not found then
		json3:='{}';
		json3:=put_json(json3,'url_default','https://'||get_json('host_canal',json2)||'/tienda');
		json2:=logjson(json2,'ALIANZA: El rut empresa no esta asociado '||campo.rut_emisor||' en el maestro_clientes');
		json2:=response_requests_6000('2','Sesion Alianza invalida',json3::varchar,json2);
        	return json2;
	end if;
	
	data1:='{"rut":"'||rut1||'","tipo_tx":"sesion_externa","rutCliente":"'||empresa1||'","session_externa":"'||get_json('sesion_externa',json2)||'"}';

	perform logfile('DAO Flujo 14766-1' || data1);
	--Armo la consulta para verificar la sesion de manager
	json2:=put_json(json2,'INPUT','POST '||get_json_upper('PARAMETRO_RUTA',json2)||' HTTP/1.1'||chr(10)||'User-Agent: curl/7.26.0'||chr(10)||'Host: '||get_json_upper('__IP_CONEXION_CLIENTE__',json2)||chr(10)||'Accept: '||chr(42)||'/'||chr(42)||chr(10)||'Content-Type: json'||chr(10)||'Content-Length: '||octet_length(data1::varchar)::varchar||chr(10)||chr(10)||data1);

	json2:=logjson(json2,'Mensaje a alianza='||dominio1||' ['||get_json('INPUT',json2)||']');
	json2:=put_json(json2,'__SECUENCIAOK__','20');
        return json2;

END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION verifica_sesion_10k_14766(json) RETURNS json as $$
DECLARE
    json1               alias for $1;
    json2               json;
        json3   json;
        json4   json;
        respuesta1      varchar;
        data1   json;
	resp1	varchar;
	codigo1	varchar;
BEGIN
        json2:=json1;

	perform logfile('DAO verifica_sesion_10k_14766 json2='||json2::varchar);
	--Debo verirfica si la sesion de manager es real
	resp1:=get_json('RESPUESTA',json2);
	
	json2:=logjson(json2,'Respuesta de Alianza='||resp1);
	if (length(resp1)=0) then
		json2:=logjson(json2,'ALIANZA: Servicio Alianza no responde para verificar sesion');
		json2:=response_requests_6000('2','','Servicio Alianza no responde para verificar sesion',json2);
        	return json2;
	end if;

	resp1:=split_part(resp1,chr(10)||chr(10),2);

	--Si contesta no
	codigo1:=get_json('CODIGO_RESPUESTA',resp1::json);
	
	
	if (codigo1<>'1') then
		json3:='{}';
		json3:=put_json(json3,'url_default','https://'||get_json('host_canal',json2)||'/tienda');
		json2:=logjson(json2,'ALIANZA: Sesion Alianza invalida');
		json2:=response_requests_6000('2','Sesion Alianza invalida',json3::varchar,json2);
        	return json2;
	end if;
	
	

        json2:=put_json(json2,'flag_sesion_externa','SI');
        json2:=logjson(json2,'Se llama al login sesion externa');
	return log_in_6000(json2);
/*        json3:=log_in_6000(json2);
        --Obtengo la respuesta
        json3:=split_part(get_json('RESPUESTA',json3),chr(10)||chr(10),2)::json;
        json4:='{}';
        json4:=put_json(json4,'CODIGO_RESPUESTA',get_json('CODIGO_RESPUESTA',json3));
        json4:=put_json(json4,'MENSAJE_RESPUESTA',get_json('MENSAJE_RESPUESTA',json3));
        if (get_json('CODIGO_RESPUESTA',json3)='1') then
                --json4:=put_json(json4,'SESION_ACEPTA',get_json('session_id',json3));
        end if;
        respuesta1:='Status: 200' ||chr(10)||'Content-type: json'||chr(10)||'Content-Length: '||octet_length(json4::varchar)::varchar||chr(10)||chr(10)||json4;
        json2:=put_json(json2,'RESPUESTA',respuesta1);
        return json2;
*/
END;
$$ LANGUAGE plpgsql;


