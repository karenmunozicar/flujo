delete from isys_querys_tx where llave='2200';
-- debo agregar una funcion en el motor que saque los datos de la TX
insert into isys_querys_tx values ('2200',5,45,1,'select lee_parametros_motor_2200(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('2200',10,15,1,'select pivote_gestorfolios_6000(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,20,0);
insert into isys_querys_tx values ('2200',20,45,1,'select salida_gestorfolios_6000(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,0,0);

CREATE or replace FUNCTION lee_parametros_motor_2200(json) RETURNS json AS $$
DECLARE
    json1                alias for $1;
    json2        json;
    resp1    varchar;
    cod_resp1 varchar;
    msg_resp1 varchar;
    campo record;
begin
    json2:=json1;
    json2:=put_json(json2,'__SECUENCIAOK__','10'); 
    json2:=logjson(json2,'JSON INPUT='||chr(10)||json2);
    
    --Leo la base del parametro_motor BASE_MOTOR para uso en el gestor de folios
    select * into campo from parametros_motor where parametro='BASE_MOTOR';
    if not found then
                json2:=logjson(json2,'No definido BASE_MOTOR en parametros_motor');
                json2:=put_json(json2,'__SECUENCIAOK__','0');
                return json2;
    end if;
    json2:=put_json(json2,'PARAMETRO_BASE_MOTOR',campo::varchar);
    return json2;
END;
$$ LANGUAGE plpgsql;

