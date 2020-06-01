delete from isys_querys_tx where llave='7020';
insert into isys_querys_tx values ('7020',10,9,1,'select proc_procesa_bitacora_7020(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);
CREATE or replace FUNCTION proc_procesa_bitacora_7020(json) RETURNS json AS $$
DECLARE
	json1			alias for $1;
	json2			json;
	json3			json;
	json_aux		json;
	json_solicitud		json;
	respuesta		varchar;
BEGIN
	json2:=json1;
	json3:='{}';
	json_solicitud:='{}';
	json2:=put_json(json2,'__FLUJO_ENTRADA__','7020');
	if (get_json('REQUEST_METHOD',json2)='GET') then
		json2:=logjson(json2,'Entro GET');
		json2:=put_json(json2,'QUERY_STRING',decodifica_url(get_json('QUERY_STRING',json2)));
		json2:=get_parametros_get_json(json2);
		json_solicitud:=put_json(json_solicitud,'id_solicitud',get_json('solicitud',json2));
		-- Se obtiene las trazas
		json_aux:=icar_get_traza_bitacora(json_solicitud);
		json3:=put_json(json3,'BITACORA',json_aux);
		respuesta:='Status: 200' ||chr(10)||'Content-type: json'||chr(10)||'Content-Length: '||octet_length(json3::varchar)::varchar||chr(10)||chr(10)||json3;
		json2:=put_json(json2,'RESPUESTA',respuesta);
	else
		json3:=put_json(json3,'CODIGO_RESPUESTA','2');
		json3:=put_json(json3,'MENSAJE_RESPUESTA','Error, metodo no permitido');
		json3:=put_json(json3,'RESPUESTA','');
		respuesta:='Status: 200' ||chr(10)||'Content-type: json'||chr(10)||'Content-Length: '||octet_length(json3::varchar)::varchar||chr(10)||chr(10)||json3;
		json2:=put_json(json2,'RESPUESTA',respuesta);
		json2:=logjson(json2,'JSON RESPUESTA='||json3);
		RETURN json2;
	end if;
	RETURN json2;
END;
$$ LANGUAGE plpgsql;
