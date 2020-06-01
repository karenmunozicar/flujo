delete from isys_querys_tx where llave='14765';
--Obtiene el DTE Original con la entrada URI_IN
insert into isys_querys_tx values ('14765',10,1,1,'select genera_sesion_10k_14765(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('14765',20,1,2,'Servicio de Manager para Sesion',4013,100,101,0,0,30,30);

insert into isys_querys_tx values ('14765',30,1,1,'select verifica_sesion_10k_14765(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);


CREATE or replace FUNCTION genera_sesion_10k_14765(json) RETURNS json as $$
DECLARE
    json1               alias for $1;
    json2               json;
        json3   json;
        json4   json;
        respuesta1      varchar;
	data1	json;
	campo	record;
	rut1	varchar;
	empresa1	varchar;
BEGIN
        json2:=json1;

	--Solo para IP de Manager
	if (get_json('REMOTE_ADDR',json2)<>'198.41.35.81') then
		json2:=logjson(json2,'MANAGER: IP no habilitada para consultar');
		json4:='{"Status":"Servicio no Habilitado"}';
        	respuesta1:='Status: 404' ||chr(10)||'Content-type: json'||chr(10)||'Content-Length: '||octet_length(json4::varchar)::varchar||chr(10)||chr(10)||json4;
        	json2:=put_json(json2,'RESPUESTA',respuesta1);
        	return json2;
	end if;

	rut1:=get_json('rut',json2);
	empresa1:=get_json('rutCliente',json2);
	--Verirficamos si el usuario y la empresa tiene relacion con acepta
	select * into campo from menu_10k where rut_usuario=rut1 and empresa=empresa1;
	if not found then
		json2:=logjson(json2,'MANAGER: Rut no habilitado para entrar esa empresa');
		json4:='{"Status":"Servicio no Habilitado.."}';
        	respuesta1:='Status: 404' ||chr(10)||'Content-type: json'||chr(10)||'Content-Length: '||octet_length(json4::varchar)::varchar||chr(10)||chr(10)||json4;
        	json2:=put_json(json2,'RESPUESTA',respuesta1);
        	return json2;
	end if;
	
	data1:='{"rut":"'||rut1||'","tipo_tx":"sesion_externa","rutCliente":"'||empresa1||'","session_id":"'||get_json('sesion_manager',json2)||'"}';

	--Armo la consulta para verificar la sesion de manager
	json2:=get_parametros_motor_json(json2,'MANAGER');
	json2:=put_json(json2,'INPUT','POST '||get_json_upper('PARAMETRO_RUTA',json2)||' HTTP/1.1'||chr(10)||'User-Agent: curl/7.26.0'||chr(10)||'Host: '||get_json_upper('__IP_CONEXION_CLIENTE__',json2)||chr(10)||'Accept: '||chr(42)||'/'||chr(42)||chr(10)||'Content-Type: application/json'||chr(10)||'Content-Length: '||octet_length(data1::varchar)::varchar||chr(10)||chr(10)||data1);

	json2:=logjson(json2,'MEnsaje a Manager='||get_json('INPUT',json2));
	json2:=put_json(json2,'__SECUENCIAOK__','20');
        return json2;

END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION verifica_sesion_10k_14765(json) RETURNS json as $$
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
	
	json2:=logjson(json2,'Respuesta de Manager='||resp1);
	if (length(resp1)=0) then
		json2:=logjson(json2,'MANAGER: Servicio Manager no responde para verificar sesion');
		json4:='{"Status":"Servicio Manager no responde para verificar sesion"}';
        	respuesta1:='Status: 404' ||chr(10)||'Content-type: json'||chr(10)||'Content-Length: '||octet_length(json4::varchar)::varchar||chr(10)||chr(10)||json4;
        	json2:=put_json(json2,'RESPUESTA',respuesta1);
        	return json2;
	end if;

	resp1:=split_part(resp1,chr(10)||chr(10),2);

	--Si contesta no
	codigo1:=get_json('CODIGO_RESPUESTA',resp1::json);
	if (codigo1<>'1') then
		json2:=logjson(json2,'MANAGER: Sesion Manager invalida');
		json4:='{"Status":"Sesion Manager invalida"}';
        	respuesta1:='Status: 404' ||chr(10)||'Content-type: json'||chr(10)||'Content-Length: '||octet_length(json4::varchar)::varchar||chr(10)||chr(10)||json4;
        	json2:=put_json(json2,'RESPUESTA',respuesta1);
        	return json2;
	end if;

        json2:=put_json(json2,'flag_sesion_externa','SI');
        json2:=logjson(json2,'Se llama al login sesion externa');
        json3:=log_in_6000(json2);
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

END;
$$ LANGUAGE plpgsql;

