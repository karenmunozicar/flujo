delete from isys_querys_tx where llave='6003';
insert into isys_querys_tx values ('6003',5,19,1,'select control_flujo_80101(''$$__JSONCOMPLETO__["__PROC_ACTIVOS__","TX","REQUEST_URI","__ARGV__","__CATEGORIA_COLA__","__FLUJO_ACTUAL__"]$$''::json) as __json__',0,0,0,1,1,-1,10);

insert into isys_querys_tx values ('6003',10,9,1,'select interno_6003(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('6003',15,9,1,'select secuencia_timeout_6000(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,0,0);
--Llamada a Script Generico
insert into isys_querys_tx values ('6003',20,1,10,'$$SCRIPT$$',0,0,0,1,1,25,25);

insert into isys_querys_tx values ('6003',25,9,1,'select interno_resp_6003(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

--Llamada a un MicroServicio POST
insert into isys_querys_tx values ('6003',28,1,2,'Microservicioe 127.0.0.1',4013,300,101,0,0,35,35);

--Llama Servicio Generico
insert into isys_querys_tx values ('6003',30,1,2,'Servicio HTTP Generico',4013,100,101,0,0,25,25);

--Procesa la respuesta de los scripts
insert into isys_querys_tx values ('6003',35,9,1,'select generico10k_resp_6000(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,25);
--Flujo Firma XML Subido en el Subir
insert into isys_querys_tx values ('6003',40,1,8,'Llamada EMITIR DTE',13794,0,0,0,0,0,0);

insert into isys_querys_tx values ('6003',55,1,8,'Llamada SUBIR FIRMA',12793,0,0,0,0,0,0);
--Flujo Emitir Proceso Certificacion
--insert into isys_querys_tx values ('6003',56,1,8,'Llamada EMITIR DTE',6002,0,0,0,0,0,0);
--Emitir docs recibidos Certificacion
--insert into isys_querys_tx values ('6003',60,1,8,'Llamada EMITIR DTE',6004,0,0,0,0,0,0);
insert into isys_querys_tx values ('6003',81,1,8,'Llamada CESION JSON',12797,0,0,0,0,0,0);

--Llama al reenvio por intercambio
insert into isys_querys_tx values ('6003',110,1,8,'LLAMADA AL FLUJO 12764 Reenvio Intercambio',12764,0,0,1,1,0,0);
-- Flujo 6000 --ICAR - Nuevos
insert into isys_querys_tx values ('6003',180,1,1,'select icar_spie_pivote_6000(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('6003',181,1,1,'select icar_enviar_decision_6000(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,30,0);

--FLujo Libros
--insert into isys_querys_tx values ('6002',300,1,8,'Llamada WEBIECV',12724,0,0,0,0,400,400);
--insert into isys_querys_tx values ('6002',300,1,8,'Llamada WEBIECV',12734,0,0,0,0,400,400);
--Bolsa de Productos
insert into isys_querys_tx values ('6003',400,24,1,'select $$FUNCION_INPUT$$(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);
--Base de Colas
insert into isys_querys_tx values ('6003',410,19,1,'select $$FUNCION_INPUT$$(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);


--insert into isys_querys_tx values ('6002',500,1,8,'Llamada Flujo Pide CAF',6005,0,0,0,0,0,0);
insert into isys_querys_tx values ('6003',510,15,1,'select $$FUNCION_INPUT$$(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);
--Secuencia Borrado
insert into isys_querys_tx values ('6003',1000,19,1,'select sp_procesa_respuesta_cola_motor_original_json(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('6003',14720,1,8,'Llama Flujo 14720',14720,0,0,1,1,0,0);

--Financiamiento
insert into isys_querys_tx values ('6003',12772,1,8,'LLAMADA AL FLUJO 12772',12772,0,0,1,1,25,25);
insert into isys_querys_tx values ('6003',12773,1,8,'LLAMADA AL FLUJO 12773',12773,0,0,1,1,25,25);
insert into isys_querys_tx values ('6003',12774,1,8,'LLAMADA AL FLUJO 12774',12774,0,0,1,1,25,25);
insert into isys_querys_tx values ('6003',12775,1,8,'LLAMADA AL FLUJO 12775',12775,0,0,1,1,25,25);
insert into isys_querys_tx values ('6003',12776,1,8,'LLAMADA AL FLUJO 12776',12776,0,0,1,1,25,25);
insert into isys_querys_tx values ('6003',12778,1,8,'LLAMADA AL FLUJO 12778',12778,0,0,1,1,25,25);
--Dipres
insert into isys_querys_tx values ('6003',12815,1,8,'LLAMADA AL FLUJO 12815',12815,0,0,1,1,35,35);
--Llama flujo 12814 DIPRES
insert into isys_querys_tx values ('6003',12814,1,8,'Llama Flujo 12814',12814,0,0,1,1,0,0);
insert into isys_querys_tx values ('6003',12818,1,8,'LLAMADA AL FLUJO 12818',12818,0,0,1,1,35,35);
insert into isys_querys_tx values ('6003',12819,1,8,'LLAMADA AL FLUJO 12819',12819,0,0,1,1,35,35);
insert into isys_querys_tx values ('6003',12821,1,8,'LLAMADA AL FLUJO 12821',12821,0,0,1,1,35,35);

--Llama flujo 25100 Busqueda DEC
insert into isys_querys_tx values ('6003',25100,1,8,'Llama Flujo 25100',25100,0,0,1,1,0,0);
--Llama flujo 25101 Busqueda DEC Codigo DOC
insert into isys_querys_tx values ('6003',25101,1,8,'Llama Flujo 25101',25101,0,0,1,1,0,0);
insert into isys_querys_tx values ('6003',25102,1,8,'Llama Flujo 25102',25102,0,0,1,1,0,0);
insert into isys_querys_tx values ('6003',25106,1,8,'Llama Flujo 25106',25106,0,0,1,1,0,0);




CREATE or replace FUNCTION interno_6003(json) RETURNS json AS $$
DECLARE
    json1                alias for $1;
    json2                json;
    data1               varchar;
    file1               varchar;
    sts                 integer;
    header1             varchar;
    url1                varchar;
    host1               varchar;
    rut_emisor1         varchar;
    query1              varchar;
    resp_json1           varchar;
    tipo_tx1            varchar;
    exists_select1      varchar;
    estado_select1      varchar;
    tipo_resp1          varchar;
    estado1             varchar;
    respuesta1          varchar;
    stSec               define_secuencia_generico10k%ROWTYPE;
    file_wsdl1  varchar;
        sesion1 varchar;
        stSesion        sesion_web_10k%ROWTYPE;
        rut_empresa1    varchar;
        stMenu          menu_10k%ROWTYPE;
        stMaster        maestro_clientes%ROWTYPE;
        --stPerfilApp   perfil_10k_aplicacion%ROWTYPE;
        modo_qa1        varchar;
        rol1            varchar;
        funcionalidad1  varchar;
        aux1            varchar;
        xml2            varchar;
	campo		record;
BEGIN
    json2:=json1;
    json2:=put_json(json2,'__SECUENCIAOK__','0');
    json2:=put_json(json2,'__FLUJO_ENTRADA__','6003');

    json2:=logjson(json2,'JSON INPUT 6003='||json2::varchar);


    --Este flujo procesa las entradas de interno, solo en GET
    if (get_json('REQUEST_METHOD',json2)='GET') then
        json2:=logjson(json2,'Entro GET');
        json2:=put_json(json2,'QUERY_STRING',decodifica_url(get_json('QUERY_STRING',json2)));
        json2:=get_parametros_get_json(json2);
        json2:=logjson(json2,'Entro GET'||json2::varchar);
    end if;

   --Este flujo procesa las entradas de interno, en GET y POST
    if (get_json('REQUEST_METHOD',json2)='POST') then
        json2:=logjson(json2,'Entro POST');
--      json2:=put_json(json2,'QUERY_STRING',decodifica_url(get_json('QUERY_STRING',json2)));
--        json2:=get_parametros_getpost_json(json2);
        json2:=put_json(json2,'QUERY_STRING',decode(get_json('INPUT',json2),'hex')::varchar);
        json2:=get_parametros_get_json(json2);
    end if;

    --Si es un flujo
    if (get_json('LLAMA_FLUJO_AL_ENTRAR',json2)='SI') then
        json2:=logjson(json2,'SECUENCIA'||get_json('SECUENCIA_FLUJO',json2));
        json2:=put_json(json2,'__SECUENCIAOK__',get_json('SECUENCIA_FLUJO',json2));
        return json2;
    end if;

    tipo_tx1:=get_json('tipo_tx',json2);
    --json2:=logjson(json2,'JSON INPUT='||chr(10)||log_json_ident_c(json2));
    --json2:=logjson(json2,'JSON INPUT='||chr(10)||json2);
    select * into stSec from define_secuencia_generico10k where tipo_tx=tipo_tx1;
    if not found then
        json2:=put_json(json2,'CODIGO_RESPUESTA','2');
        json2:=put_json(json2,'MENSAJE_RESPUESTA','Servicio '||tipo_tx1||'No Habilitado');
        return response_requests_6000('2','Servicio '||tipo_tx1||' No Habilitado','',json2);
    end if;

    json2:=put_json(json2,'FUNCION_INPUT',stSec.funcion_input);
    json2:=put_json(json2,'FUNCION_RESPUESTA',stSec.funcion_output);

    if (stSec.flag_json='SI' and length(sesion1)>0) then
        json2:=logjson(json2,'GRABA json_respuesta en session, tipo_tx=' || tipo_tx1);
        respuesta1:='Status: 200' ||chr(10)||'Content-type: json'||chr(10)||'Content-Length: '||octet_length(json1::text)::varchar||chr(10)||chr(10)||json1::varchar;
        --Grabo input original
    end if;

    --Leo la base del parametro_motor BASE_MOTOR para uso en el gestor de folios
    select * into campo from parametros_motor where parametro='BASE_MOTOR';
    if not found then
                json2:=logjson(json2,'No definido BASE_MOTOR en parametros_motor');
                json2:=put_json(json2,'__SECUENCIAOK__','0');
                return json2;
    end if;
    json2:=put_json(json2,'PARAMETRO_BASE_MOTOR',campo::varchar);


    --Si no es una base local, vamos a ejecutar a la base que corresponda
    if (stSec.base_datos<>'BASE_1_LOCAL') then
        --Saco la base que corresponda
        if (stSec.base_datos='BASE_3_WEBIECV') then

                json2:=logjson(json2,'Ejecuta Funcion '||stSec.funcion_input||' tipo_tx='||tipo_tx1||' en Base BASE_3_WEBIECV');
                json2:=put_json(json2,'__SECUENCIAOK__','95');
                return json2;
        end if;
        --Base de Gestor de Folio
        if (stSec.base_datos='BASE_15_GESTORFOLIOS') then
                json2:=logjson(json2,'Ejecuta Funcion '||stSec.funcion_input||' tipo_tx='||tipo_tx1||' en Base BASE_15_GESTORFOLIOS');
                json2:=put_json(json2,'__SECUENCIAOK__','500');
                return json2;
        end if;
	if (stSec.base_datos='BASE_15_GESTORFOLIOS_FUNC') then
                json2:=logjson(json2,'Ejecuta Funcion '||stSec.funcion_input||' tipo_tx='||tipo_tx1||' en Base BASE_15_GESTORFOLIOS');
                json2:=put_json(json2,'__SECUENCIAOK__','510');
                return json2;
        end if;
	--Base de Bolsa
        if (stSec.base_datos='BASE_FINANCIAMIENTO') then
                json2:=logjson(json2,'Ejecuta Funcion '||stSec.funcion_input||' tipo_tx='||tipo_tx1||' en BASE BASE_FINANCIAMIENTO');
                json2:=put_json(json2,'__SECUENCIAOK__','400');
                return json2;
        end if;
	--Base de Colas
	if (stSec.base_datos='BASE_COLAS') then
                json2:=logjson(json2,'Ejecuta Funcion '||stSec.funcion_input||' tipo_tx='||tipo_tx1||' en BASE BASE_COLAS');
                json2:=put_json(json2,'__SECUENCIAOK__','410');
                return json2;
	end if;
		
    end if;

    --EJECUTA FUNCION INPUT
    json2:=logjson(json2,'Ejecuta='||stSec.funcion_input);
    if length(stSec.funcion_input)>0 then
        EXECUTE 'SELECT ' || stSec.funcion_input || '(' || chr(39) || json2 || chr(39) || '::json)' into json2;
    end if;

    --Si necesita llamar un scrip lo ejecuta
    if (get_json('LLAMA_SCRIPT',json2)='SI') then
        json2:=logjson(json2,'Ejecuta Shell='||get_json('SCRIPT',json2));
        json2:=put_json(json2,'__SECUENCIAOK__','20');
    end if;
    --Si necesita llamar un microservicio
    if (get_json('LLAMA_MS',json2)='SI') then
        json2:=logjson(json2,'Ejecuta MicroServicio='||get_json('URI_MS',json2));
        json2:=put_json(json2,'__SECUENCIAOK__','28');
    end if;

    --Si es un flujo
    if (get_json('LLAMA_FLUJO',json2)='SI') then
        json2:=logjson(json2,'Ejecuta Flujo Secuencia='||get_json('__SECUENCIAOK__',json2));
        return json2;
    end if;

     --Si algo va mal con este query, el procesador enviara a la secuencia timeout
    json2:=put_json(json2,'__SECUENCIA_TIMEOUT__','15');
    return json2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION interno_resp_6003(varchar) RETURNS varchar AS $$
DECLARE
    json1                alias for $1;
    json2        json;
    funcion1    varchar;
begin
    --EJECUTA FUNCION output
    json2:=json1;
    json2:=put_json(json2,'__SECUENCIAOK__','0');
    funcion1:=get_json('FUNCION_RESPUESTA',json2);
    --insert into jsprueba select json2;
    json2:=logjson(json2,'Ejecuta Respuesta='||funcion1);
    if length(funcion1)>0 then
        EXECUTE 'SELECT ' || funcion1 || '(' || chr(39) || json2 || chr(39) || '::json)' into json2;
    end if;
    return json2;
END;
$$ LANGUAGE plpgsql;

