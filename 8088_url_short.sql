delete from isys_querys_tx where llave='8088';

--Pivote
insert into isys_querys_tx values ('8088',5,19,1,'select pivote_url_short_8088(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

--Servicio inicial.
insert into isys_querys_tx values ('8088',10,1901,1,'select ejecuta_url_short_8088(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('8088',20,1914,1,'select ejecuta_url_short_8088(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);


CREATE or replace FUNCTION pivote_url_short_8088(json) RETURNS json AS $$
DECLARE
    json1                alias for $1;
    json2                json;
    u1	varchar;
    s1	varchar;
BEGIN
    json2:=json1;
   --Obtengo el u
    if (get_json('REQUEST_METHOD',json2)='GET') then
        json2:=put_json(json2,'QUERY_STRING',decodifica_url(get_json('QUERY_STRING',json2)));
        json2:=get_parametros_get_json(json2);
    end if;
    u1:=get_json('u',json2);
    --Identificamos el servidor donde se ejecuto 
    s1:=right(split_part(u1,'_',1),2);
    json2:=logjson(json2,'Metodo='||get_json('REST_METODO',json2)||' Server='||s1||' u='||u1); 
    if (s1='01') then
    	json2:=put_json(json2,'__SECUENCIAOK__','10');
    else
    	json2:=put_json(json2,'__SECUENCIAOK__','20');
    end if;
    return json2;
END;
$$ LANGUAGE plpgsql;



CREATE or replace FUNCTION ejecuta_url_short_8088(json) RETURNS json AS $$
DECLARE
    json1                alias for $1;
    json2                json;
    v_cliente		 varchar;
    v_servicio  	 varchar;
    v_autorizacion_env	 varchar;
    v_autorizacion_ok	 varchar;	
    v_servicio_in	 varchar;
    v_servicio_out	 varchar;
    v_respuesta		 varchar;
    v_pass		 varchar;
    v_secuencia_flujo	integer;
BEGIN
    --insert into jsprueba2 select json1::varchar;
    json2:=json1;
    --insert into jsprueba2 select json2::varchar;
    v_servicio:= get_json('REST_METODO',json2);
    json2:=put_json(json2,'__SECUENCIAOK__','0');
    if (get_json('REQUEST_METHOD',json2)='GET') then
        --json2:=logjson(json2,'Entro GET');
        json2:=put_json(json2,'QUERY_STRING',decodifica_url(get_json('QUERY_STRING',json2)));
        json2:=get_parametros_get_json(json2);
    end if;

    --json2:=logjson(json2,'v_servicio='||v_servicio);
    if (v_servicio='GetIdECM') then
	return sp_get_id_ecm(json2);
    elsif (v_servicio='GetUrlShort') then
	return sp_get_url_short(json2);
    elsif (v_servicio='CreaShortUri') then
	return sp_crea_url_short(json2);
    elsif (v_servicio='DelShortUri') then
	return sp_del_url_short(json2);
    end if;

    json2 := logjson(json2,'Servicio Desconocido '|| v_servicio_in) ;
    return json2;
END;
$$ LANGUAGE plpgsql;

