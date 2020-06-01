delete from isys_querys_tx where llave='7000';

insert into isys_querys_tx values ('7000',5,9,1,'select log_generico10k_6000(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,10);

insert into isys_querys_tx values ('7000',10,9,1,'select generico10k_7000(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);
--insert into isys_querys_tx values ('7000',99,9,1,'select generico10k_resp_7000(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('7000',12774,1,8,'LLAMADA AL FLUJO 12774',12774,0,0,1,1,0,0);
insert into isys_querys_tx values ('7000',12776,1,8,'LLAMADA AL FLUJO 12776',12776,0,0,1,1,0,0);

CREATE or replace FUNCTION generico10k_7000(json) RETURNS json AS $$
DECLARE
    json1                alias for $1;
    json2                json;
    v_perfil		varchar;
    v_aplicacion	varchar;
    v_tx		varchar;
    v_funcionalidad	varchar;
    st_define_secuencia define_secuencia_generico10k%ROWTYPE;
	stSec	record;
	app1	varchar;
	campo1	record;
BEGIN
    json2:=json1;
    json2:=put_json(json2,'__SECUENCIAOK__','0');
    json2:=put_json(json2,'__FLUJO_ENTRADA__','7000');

    v_perfil :='FREE';
    v_tx     :=get_json('tipo_tx',json2);
	app1:=get_json('app_dinamica',json2);
	select * into campo1 from menu_info_10k where id2=app1 and flag_offline='SI';
	if not found then
		return response_requests_6000('2','Servicio '||v_tx||' No Habilitado','',json2);
	end if;
	

    select * into stSec from define_secuencia_generico10k where tipo_tx=v_tx;
    if not found then
        json2:=put_json(json2,'CODIGO_RESPUESTA','2');
        json2:=put_json(json2,'MENSAJE_RESPUESTA','Servicio '||tipo_tx1||'No Habilitado');
        return response_requests_6000('2','Servicio '||tipo_tx1||' No Habilitado','',json2);
    end if;

    json2 := put_json(json2,'FX_INPUT',stSec.funcion_input);
    json2 := put_json(json2,'FX_INPUT',stSec.funcion_input);

    execute 'select '|| stSec.funcion_input || '('|| quote_literal(json2) || ')' into json2;  

    --Si es un flujo
    if (get_json('LLAMA_FLUJO',json2)='SI') then
        json2:=logjson(json2,'Ejecuta Flujo Secuencia='||get_json('__SECUENCIAOK__',json2));
        return json2;
    end if;

    json2:=put_json(json2,'__SECUENCIAOK__','0');

    return json2;


END;
$$ LANGUAGE plpgsql;

/*
CREATE or replace FUNCTION generico10k_resp_7000(json) RETURNS json AS $$
DECLARE
    json1                alias for $1;
    json2                json;
    st_define_secuencia define_secuencia_generico10k%ROWTYPE;
BEGIN


    return json2;


END;
$$ LANGUAGE plpgsql;
*/
