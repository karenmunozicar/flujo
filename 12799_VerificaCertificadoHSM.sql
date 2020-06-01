--Publica documento
delete from isys_querys_tx where llave='12799';
--Obtiene el DTE Original con la entrada URI_IN


insert into isys_querys_tx values ('12799',1,9,1,'select verifica_secuencia_12799(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('12799',10,14,1,'select array_agg(sql) as certificados from (SELECT id as id_certificado,estado,fecha_ingreso,fecha_valido_hasta from certificado_persona where persona_id in (SELECT id from persona where rut=''$$RUT_HSM$$'') order by fecha_valido_hasta) sql',0,0,0,1,1,20,20);
insert into isys_querys_tx values ('12799',15,40,1,'select array_agg(sql) as certificados from (SELECT id as id_certificado,estado,fecha_ingreso,fecha_valido_hasta from certificado_persona where persona_id in (SELECT id from persona where rut=''$$RUT_HSM$$'') order by fecha_valido_hasta) sql',0,0,0,1,1,20,20);

insert into isys_querys_tx values ('12799',20,9,1,'select get_perfil_6000(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,0,0);


CREATE or replace FUNCTION verifica_secuencia_12799(json) RETURNS json
AS $$
DECLARE
        json1               alias for $1;
        json2                   json;
BEGIN
        json2:=json1;
        json2:=put_json(json2,'LLAMA_FLUJO','SI');
        json2:=put_json(json2,'__SECUENCIAOK__','150');
        json2:=put_json(json2,'RUT_HSM',get_json('rut_firma',json2)||'-'||modulo11(get_json('rut_firma',json2)));
	if get_parametro_firmador(get_json('RUT_HSM',json2))='FIRMADOR' then
		json2:=put_json(json2,'__SECUENCIAOK__','10');
	else
		json2:=put_json(json2,'__SECUENCIAOK__','15');
	end if;
        RETURN json2;
END;
$$
LANGUAGE plpgsql;

