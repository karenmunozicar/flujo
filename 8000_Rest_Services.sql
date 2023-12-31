delete from isys_querys_tx where llave='8000';
--Servicio inicial.
insert into isys_querys_tx values ('8000',10,1,1,'select sp_inicia_rest_8000(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);
--Valida Siguiente secuencia
--insert into isys_querys_tx values ('8000',50,1,1,'select sp_valida_siguiente_secuencia_8000(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);
--Multi hilo
insert into isys_querys_tx values ('8000',20,1,9,'<EJECUTA0>8001</EJECUTA0><EJECUTA1>8002</EJECUTA1><TIMEOUT>15</TIMEOUT>',0,0,0,1,1,80,80);

--Ejecuta en base 88
insert into isys_querys_tx values ('8000',30,19,1,'select $$FUNCION_INPUT$$(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

--Sevicio respuesta
insert into isys_querys_tx values ('8000',80,1,1,'select sp_respuesta_rest_8000(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);
--FLUJO Y SECUENCIA PARA BUSQUEDA DOCTOS BBVA.
delete from isys_querys_tx where llave='8001';
delete from isys_querys_tx where llave='8002';


--insert into isys_querys_tx values ('8000',21,9,1,'select sp_consultadoc_bbva_out(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('8001',10,9,1,'select sp_busca_importados_servicio_8001(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('8002',10,9,1,'select sp_busca_emitidos_servicio_8002(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);


--Secuencia para Login
--insert into isys_querys_tx values ('8000',40,1,10,'$$SCRIPT$$',0,0,0,1,1,50,0);
--Secuencias para flujo AddTag RemoveTag

CREATE or replace FUNCTION sp_inicia_rest_8000(json) RETURNS json AS $$
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
    --json2:=logjson(json2,'MDA -- json entrada, '||json2::varchar);
    v_cliente := get_json('REST_CLIENTE',json2);
    v_servicio:= get_json('REST_METODO',json2);
    json2:=put_json(json2,'__SECUENCIAOK__','0');
    v_autorizacion_env := ltrim(replace(get_json('HTTP_AUTHORIZATION',json2),'Basic',''));

    if (get_json('REQUEST_METHOD',json2)='GET') then
        json2:=logjson(json2,'Entro GET');
        json2:=put_json(json2,'QUERY_STRING',decodifica_url(get_json('QUERY_STRING',json2)));
        json2:=get_parametros_get_json(json2);
	v_servicio:=get_json('tipo_tx',json2);
    else
	--MDA 20180627 para BBVA que no usa tipo_tx
	if(v_servicio = '') then
   		v_servicio:=get_json('tipo_tx',json2);
	end if;
    end if;

	json2:=logjson(json2,'v_cliente='||v_cliente||' '||'v_servicio='||v_servicio);


    select sp_rest_in,sp_rest_out,autorizacion,password,secuencia_flujo into v_servicio_in,v_servicio_out,v_autorizacion_ok, v_pass,v_secuencia_flujo from servicio_x_cliente where cliente=v_cliente and servicio=v_servicio;
    if not found then
    	v_respuesta := '<?xml version="1.0" encoding="ISO-8859-1"?> ' || chr(10) ||'<Servicio>Servicio no existe *</Servicio>';
	json2:=logjson(json2,'No existe servicio habilitado para cliente.') ;
	json2:=put_json(json2,'__SECUENCIAOK__','0');
	json2:=put_json(json2,'RESPUESTA','Status: 404 Not Found'||chr(10)||'Content-type: application/xml'||chr(10)||'Content-length: '||length(v_respuesta)||chr(10)||chr(10)||v_respuesta);
	return json2;
    else 
	--Si tiene secuencia_flujo no nula, vamoas a ejecutarla
	if (v_secuencia_flujo is not null) then
		json2 := logjson(json2,'Se ejecuta secuencia '||v_secuencia_flujo::varchar||' Funcion='||v_servicio_in);
		json2:=put_json(json2,'FUNCION_INPUT',v_servicio_in);
		json2:=put_json(json2,'__SECUENCIAOK__',v_secuencia_flujo::varchar);
		return json2;
	end if;
	if(v_autorizacion_env <> v_autorizacion_ok) then
		v_respuesta := '<?xml version="1.0" encoding="ISO-8859-1"?> ' || chr(10) ||'<Servicio>Fallo en autorizacion</Servicio>';
	        json2:=logjson(json2,'Error en el envio de Usuario o Password.') ;
       	 	json2:=put_json(json2,'__SECUENCIAOK__','0');
        	json2:=put_json(json2,'RESPUESTA','Status: 401 Unauthorized'||chr(10)||'Content-type: application/xml'||chr(10)||'Content-length: '||length(v_respuesta)||chr(10)||chr(10)||v_respuesta);
        return json2;

	end if;
    end if;

    json2 := put_json(json2,'SP_REST_IN',v_servicio_in);
    json2 := put_json(json2,'SP_REST_OUT',v_servicio_out);
    json2 := put_json(json2,'SERV_PASS',v_pass);
   
    json2 := logjson(json2,'REST_IN : ' || v_servicio_in ) ;
    json2 := logjson(json2,'REST_OUT : ' || v_servicio_out ) ;
    json2 := logjson(json2,'PASS : ' || v_pass ) ;

    json2 := logjson(json2,'Ejecuta servicio : ' || v_servicio_in) ;
    execute 'select ' || v_servicio_in || '(' || quote_literal(json2) || ')' into json2;
    json2 := logjson(json2,'Siguiente secuencia : ' || get_json('__SECUENCIAOK__',json2)) ;
    return json2;

END;
$$ LANGUAGE plpgsql;

/*

CREATE or replace FUNCTION sp_valida_siguiente_secuencia_8000(json) RETURNS json AS $$
DECLARE
    json1                  alias for $1;
    json2                  json;
    v_rut_empresa	   varchar;
    v_numero_paso          varchar;
    v_total_pasos          varchar;
    v_paso_siguiente	   varchar;
    t_secuencias_x_serv	   secuencias_x_servicio_8000%ROWTYPE;

    v_funcion		   varchar;
    v_script		   varchar;
    v_data_script	   varchar;

BEGIN

    json2 := json1;
  

    v_rut_empresa :='13035795';  
    v_paso_siguiente := get_json('SIGUIENTE_PASO',json2);
    v_total_pasos := get_json('TOTAL_PASOS',json2);
    json2:=put_json(json2,'SCRIPT',' ');
    if (is_numeric(v_paso_siguiente) is false) then
   	v_paso_siguiente='0';
    	select count(1) from secuencias_x_servicio_8000 into v_total_pasos where rut_empresa = v_rut_empresa::integer;
	json2 := put_json(json2,'TOTAL_PASOS',v_total_pasos);
    end if;

    v_paso_siguiente := v_paso_siguiente::integer+1;
    json2 := put_json(json2,'SIGUIENTE_PASO',v_paso_siguiente);
    if (v_total_pasos::integer>=1  and v_paso_siguiente::integer<=v_total_pasos::integer) then
	select rut_empresa,nombre_servicio,nombre_script,funcion_in,paso into t_secuencias_x_serv from secuencias_x_servicio_8000 where rut_empresa = v_rut_empresa::integer and paso = v_paso_siguiente::integer;	
	if found then
		v_funcion := t_secuencias_x_serv.funcion_in;
		v_script  := t_secuencias_x_serv.nombre_script;
		if (length(v_funcion)>0) then
			execute 'select ' || v_funcion || '(' || quote_literal(json2) || ')' into json2;
		end if;
		if (length(v_script)>0) then
			if (length(get_json('DATA_SCRIPT',json2))>0) then
				v_data_script := get_json('DATA_SCRIPT',json2);
			else 
				v_data_script := '';
			end if;
			json2:=put_json(json2,'SCRIPT',v_script || ' ' || v_data_script);
--			insert into jsprueba2 select get_json('SCRIPT',json2);
			json2:=put_json(json2,'__SECUENCIAOK__','40');
		end if;
	else 
		 json2 := logjson(json2,'No existe registro secuencia_x_servicio: ' || v_rut_empresa) ;
		json2:=put_json(json2,'__SECUENCIAOK__','0');
	end if;

    else 
	json2:=put_json(json2,'__SECUENCIAOK__','80');
    end if;

	
    return json2;

END;
$$ LANGUAGE plpgsql;

*/


CREATE or replace FUNCTION sp_respuesta_rest_8000(json) RETURNS json AS $$
DECLARE
    json1                alias for $1;
    json2                json;
    v_servicio_out       varchar;
    v_respuesta          varchar;
    v_content_type      varchar;
BEGIN
   json2 := json1;
   v_servicio_out := get_json('SP_REST_OUT',json2);

    if(length(v_servicio_out)>0) then
        json2 := logjson(json2,'Ejecuta servicio Respuesta : ' || v_servicio_out) ;
        execute 'select ' || v_servicio_out ||  '(' || chr(39) || json2 || chr(39) || '::json)' into json2;
        v_respuesta := get_json('RESPUESTA_REST',json2);
    else
        if get_json('tipo_tx',json2)='estado_dte' then
                v_respuesta:=get_json('RESPUESTA_REST',json2);
        else
                v_respuesta :='';
        end if;
    end if;
    v_content_type := get_json('CONTENT_TYPE_RESP',json2);
    if (length(v_content_type)=0 or v_content_type is null) then
        v_content_type := 'text/html';
    end if;
    json2:=put_json(json2,'RESPUESTA','Status: 200 OK'||chr(10)||'Content-type: '||v_content_type||chr(10)||'Content-length: '||length(v_respuesta)||chr(10)||chr(10)||v_respuesta);
    json2:=put_json(json2,'__SECUENCIAOK__','0');

    return json2;

END;
$$ LANGUAGE plpgsql;

--------------
CREATE or replace FUNCTION sp_busca_importados_servicio_8001(json) RETURNS json AS $$
DECLARE
    json1                       alias for $1;
    json2                       json;
    json_input                  json;
    v_resp                      json;


    json_resp_completo_imp      json;
    json_resp_imp               json;

BEGIN
    json2:=json1;
    json_input:= replace(get_json('_-JSON_CONSULTA-_',json2),'\u0005','''')::json;




--------IMPORTADOS
        json_input:=put_json(json_input,'ESTADO','IMPORTADO');
        json_resp_completo_imp:=app_dinamica_buscar_emitidos2(json_input);
        json_resp_completo_imp:=split_part(get_json('RESPUESTA',json_resp_completo_imp),chr(10)||chr(10),2);
	--raise notice 'json_resp_completo_imp=%',json_resp_completo_imp;
        if(json_resp_completo_imp::varchar like '%Busqueda No Exitosa%') then
                json2:=put_json(json2,'RESPUESTA_REST','No existen registros Facturas');
                json_resp_imp:='[{}]';
                --insert into jsprueba select json_resp_completo_imp;
        else
                json_resp_imp:= get_json('datos_tabla',get_json('RESPUESTA',json_resp_completo_imp)::json);
                --insert into jsprueba select json_resp_imp;
                if (json_resp_imp::varchar='[ ]') then
                        json_resp_imp:='[{}]';
                end if;
        end if;
--------------------

    --v_resp:='{"param1":"valor1","param2":"valor2"}';

    json2:=put_json(json2,'BUSCA_IMPORTADOS',json_resp_imp);

    return json2;

END;
$$ LANGUAGE plpgsql;



CREATE or replace FUNCTION sp_busca_emitidos_servicio_8002(json) RETURNS json AS $$
DECLARE
    json1                alias for $1;
    json2                json;
    v_resp               json;
    json_resp_completo   json;
    json_resp            json;
    json_input           json;
BEGIN
    json2:=json1;
    json_input:=replace(get_json('_-JSON_CONSULTA-_',json2),'\u0005','''')::json;


------Normales
	--insert into jsprueba select json_input;
        json_resp_completo:=app_dinamica_buscar_emitidos2(json_input);
        json_resp_completo:=split_part(get_json('RESPUESTA',json_resp_completo),chr(10)||chr(10),2);
        if(json_resp_completo::varchar like '%Busqueda No Exitosa%') then
                json2:=put_json(json2,'RESPUESTA_REST','No exísten registros Facturas');
                json_resp:='[{}]';
        else
                json_resp:= get_json('datos_tabla',get_json('RESPUESTA',json_resp_completo)::json);
                if (json_resp::varchar='[ ]') then
                        json_resp:='[{}]';
                end if;
        end if;

------


    --v_resp:='{"param4":"valor4","param5":"valor5"}'::json;
    json2:=put_json(json2,'BUSCA_EMITIDOS',json_resp);

    return json2;

END;
$$ LANGUAGE plpgsql;


