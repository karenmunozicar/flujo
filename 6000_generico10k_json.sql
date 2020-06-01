delete from isys_querys_tx where llave='6000';

--Para hacer log de todo
--SOLO LOG
insert into isys_querys_tx values ('6000',5,9,16,'LOG_JSON',0,0,0,1,1,10,10);
--insert into isys_querys_tx values ('6000',5,9,1,'select log_generico10k_6000(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,10);

insert into isys_querys_tx values ('6000',10,9,1,'select generico10k_6000(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,15);
--Para Test
insert into isys_querys_tx values ('6000',12,9,1,'select generico10k_6000_test(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,15);
insert into isys_querys_tx values ('6000',13,1,3,'Llamada a 6002 de Test',6002,0,0,0,0,0,0);

insert into isys_querys_tx values ('6000',15,9,1,'select secuencia_timeout_6000(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,0,0);


--Parser XML Emitir
insert into isys_querys_tx values ('6000',17,1,8,'LLAMADA AL FLUJO GET XML',12705,0,0,1,1,30,0);

--Llamada a Script Generico
insert into isys_querys_tx values ('6000',20,1,10,'$$SCRIPT$$',0,0,0,1,1,30,30);

--Llamada a un MicroServicio POST
insert into isys_querys_tx values ('6000',22,1,2,'Microservicioe 127.0.0.1',4013,300,101,0,0,30,30);

--Llama Servicio Generico
insert into isys_querys_tx values ('6000',25,1,2,'Servicio HTTP Generico',4013,100,101,0,0,30,30);

--Procesa la respuesta de los scripts
insert into isys_querys_tx values ('6000',30,9,1,'select generico10k_resp_6000(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);
--BASE DEC
insert into isys_querys_tx values ('6000',44,44,1,'$$QUERY_DEC$$',0,0,0,9,1,30,30);
insert into isys_querys_tx values ('6000',48,48,1,'$$QUERY_DEC$$',0,0,0,9,1,30,30);
insert into isys_querys_tx values ('6000',49,44,1,'select $$FUNCION_INPUT$$(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('6000',52,65029,1,'select $$FUNCION_INPUT$$(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('6000',59,1,8,'Llamada EMITIR DTE',13794,0,0,0,0,0,0);

-- llama a Firma XML DTE
insert into isys_querys_tx values ('6000',54,1,8,'Llamada a Firma XML DTE',13795,0,0,0,0,0,0);


--Llama flujo subir firma
insert into isys_querys_tx values ('6000',55,1,8,'Llamada SUBIR FIRMA',12793,0,0,0,0,0,0);
--Test Firma con funcion respuesta
insert into isys_querys_tx values ('6000',57,1,8,'Llamada SUBIR FIRMA',12793,0,0,0,0,30,30);

--Llama flujo subir firma
insert into isys_querys_tx values ('6000',56,1,8,'Llamada EMITIR DTE',12794,0,0,0,0,0,0);

--Flujo de Agregar Acciones automaticas Controller
insert into isys_querys_tx values ('6000',58,1,8,'Llamada Agregar Acciones Automaticas',14800,0,0,0,0,0,0);

--Graba Archivo en el EDTE NAR
--insert into isys_querys_tx values ('6000',50,1,8,'Llamada NAR EDTE',12779,0,0,0,0,0,0);
--Llama flujo ARM
--insert into isys_querys_tx values ('6000',60,1,8,'Llamada ARM',12762,0,0,0,0,0,0);
--Llama flujo ARM JSON
insert into isys_querys_tx values ('6000',60,1,8,'Llamada ARM JSON',12767,0,0,0,0,0,0);
insert into isys_querys_tx values ('6000',65,1,8,'Llamada NAR JSON',12796,0,0,0,0,0,0);
--Llama flujo 16210 Reclamo Multiple
insert into isys_querys_tx values ('6000',16210,1,8,'Llamada Reclamo Multiple',16210,0,0,0,0,0,0);

--Llama flujo NAR
insert into isys_querys_tx values ('6000',70,1,8,'Llamada NAR',12765,0,0,0,0,0,0);
--Llamada CESION
insert into isys_querys_tx values ('6000',80,1,8,'Llamada CESION',12785,0,0,0,0,0,0);
insert into isys_querys_tx values ('6000',81,1,8,'Llamada CESION JSON',12797,0,0,0,0,0,0);
insert into isys_querys_tx values ('6000',83,1,8,'Llamada CESION BOLSA',12801,0,0,0,0,0,0);
insert into isys_querys_tx values ('6000',84,1,8,'REVISA RECLAMO-CESION',16108,0,0,0,0,0,0);
--insert into isys_querys_tx values ('6000',82,1,8,'Llamada CESION JSON2',42797,0,0,0,0,0,0);
--Llamada Get Datos CESION
--insert into isys_querys_tx values ('6000',90,1,8,'Llamada CESION',12787,0,0,0,0,0,0);
insert into isys_querys_tx values ('6000',90,1,8,'Llamada Get Datos CESION JSON',12798,0,0,0,0,0,0);
--Firma de Libros WebIecv
insert into isys_querys_tx values ('6000',201,1,8,'Llamada FIRMA LIBROS IECV',13100,0,0,0,0,96,96);
insert into isys_querys_tx values ('6000',202,1,8,'Llamada FIRMA LIBROS IECV RECTIFICA',13130,0,0,0,0,96,96);
--FLujo Get_Perfil
insert into isys_querys_tx values ('6000',150,1,8,'Get Perfil',12799,0,0,0,0,0,0);
--FLujo Match WebPay
insert into isys_querys_tx values ('6000',160,1,8,'FLujo Match WebPay',12810,0,0,0,0,0,0);
insert into isys_querys_tx values ('6000',161,1,8,'FLujo Match WebPay',12811,0,0,0,0,0,0);

--Llamada al reenvio de mandato
insert into isys_querys_tx values ('6000',100,1,8,'LLAMADA AL FLUJO 12763 Reenvio Mandato',12763,0,0,1,1,0,0);
insert into isys_querys_tx values ('6000',101,1,8,'LLAMADA AL FLUJO 12770 Reenvio Mandato',12770,0,0,1,1,30,30);
insert into isys_querys_tx values ('6000',102,1,8,'LLAMADA AL FLUJO 12771 Reenvio Mandato',12771,0,0,1,1,30,30);
--Llama al reenvio por intercambio
insert into isys_querys_tx values ('6000',110,1,8,'LLAMADA AL FLUJO 12764 Reenvio Intercambio',12764,0,0,1,1,0,0);

--Para Gestor de Folios en base 15
insert into isys_querys_tx values ('6000',125,15,1,'select pivote_gestorfolios_6000(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,126,0);
insert into isys_querys_tx values ('6000',126,1,1,'select salida_gestorfolios_6000(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,30,0);

--Base de Replica
insert into isys_querys_tx values ('6000',127,11,1,'select pivote_base_replica_6000(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);
--Base Primaria
insert into isys_querys_tx values ('6000',128,9,1,'select pivote_base_primaria_6000(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

--LLama a sesion externa Manager
insert into isys_querys_tx values ('6000',250,1,8,'Sesion Manager',14765,0,0,0,0,0,0);
--Llamada a sesion externa Generica
insert into isys_querys_tx values ('6000',251,1,8,'Sesion Alianza',14766,0,0,0,0,0,0);


--Para WebIecv se ejecuta en base 13
insert into isys_querys_tx values ('6000',95,13,1,'select pivote_webiecv_6000(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,96,96);
insert into isys_querys_tx values ('6000',96,1,1,'select salida_webiecv_6000(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('6000',98,1,8,'LLAMA AL FLUJO 13110 - Importacion de Libro',13110,0,0,1,1,0,0);
--RME, GAC 20160324 Secuencia para exportar asincrono los archivos
insert into isys_querys_tx values ('6000',99,1,8,'LLAMA AL FLUJO 13120 - Exportacionn de Libro',13120,0,0,1,1,0,0);
--Wewbiecv
insert into isys_querys_tx values ('6000',97,13,1,'select $$FUNCION_INPUT$$(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);
--Llamada al Gestor de Folios
--insert into isys_querys_tx values ('6000',500,15,1,'select pivote_gestor_6000(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,510,510);
--insert into isys_querys_tx values ('6000',510,1,1,'select salida_gestor_6000(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

-- Flujo 6000 --ICAR - Nuevos
insert into isys_querys_tx values ('6000',180,1,1,'select icar_spie_pivote_6000(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('6000',181,1,1,'select icar_enviar_decision_6000(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,30,0);

--Libros Contables en base 16
insert into isys_querys_tx values ('6000',300,16,1,'select pivote_lce_6000(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,305,305);
insert into isys_querys_tx values ('6000',305,1,1,'select salida_lce_6000(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

--RME 20170628 Se agregan secuencias para Nuevo ACM
insert into isys_querys_tx values ('6000',301,7,1,'select pivote_acm_6000(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('6000',302,1,10,'$$SCRIPT$$',0,0,0,1,1,303,303);
insert into isys_querys_tx values ('6000',303,1,1,'select salida_acm_6000(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,0,0);








insert into isys_querys_tx values ('6000',306,1,8,'Llamada Firma Libros LCE',13210,0,0,0,0,0,0);
insert into isys_querys_tx values ('6000',307,1,10,'$$SCRIPT_NAME$$',0,0,0,1,1,300,300);

insert into isys_querys_tx values ('6000',400,27,1,'select pivote_importer_6000(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,401,401);
insert into isys_querys_tx values ('6000',401,1,1,'select salida_importer_6000(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

--Bolsa de Productos
insert into isys_querys_tx values ('6000',500,24,1,'select $$FUNCION_INPUT$$(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);


--Llama flujo 12813 mercado publico
insert into isys_querys_tx values ('6000',12813,1,8,'Llama Flujo 12813',12813,0,0,1,1,0,0);
--Llama flujo 12814 DIPRES
insert into isys_querys_tx values ('6000',12814,1,8,'Llama Flujo 12814',12814,0,0,1,1,30,30);
--Llama flujo 12814 DIPRES
insert into isys_querys_tx values ('6000',12816,1,8,'Llama Flujo 12816',12816,0,0,1,1,0,0);
--Llama flujo 12817 DIPRES
insert into isys_querys_tx values ('6000',12817,1,8,'Llama Flujo 12817',12817,0,0,1,1,0,0);
--Llama flujo 12819 DIPRES
insert into isys_querys_tx values ('6000',12819,1,8,'Llama Flujo 12819',12819,0,0,1,1,0,0);
--Llama flujo 12821 DIPRES
insert into isys_querys_tx values ('6000',12821,1,8,'Llama Flujo 12821',12821,0,0,1,1,0,0);
insert into isys_querys_tx values ('6000',12820,1,8,'Llama Flujo 12820',12820,0,0,1,1,0,0);




--Send Mail
insert into isys_querys_tx values ('6000',501,30,1,'select $$FUNCION_INPUT$$(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

-- WEBPAY
insert into isys_querys_tx values ('6000',551,34,1,'select pivote_webpay_6000(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,553,553);
insert into isys_querys_tx values ('6000',553,1,1,'select salida_webpay_6000(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,0,0);

-- OC 20190509
insert into isys_querys_tx values ('6000',15300,1,8,'Llama Flujo 15300',15300,0,0,1,1,0,0);

--Financiamiento
insert into isys_querys_tx values ('6000',12772,1,8,'LLAMADA AL FLUJO 12772',12772,0,0,1,1,30,30);
insert into isys_querys_tx values ('6000',12773,1,8,'LLAMADA AL FLUJO 12773',12773,0,0,1,1,30,30);
insert into isys_querys_tx values ('6000',12774,1,8,'LLAMADA AL FLUJO 12774',12774,0,0,1,1,30,30);
insert into isys_querys_tx values ('6000',12775,1,8,'LLAMADA AL FLUJO 12775',12775,0,0,1,1,30,30);
insert into isys_querys_tx values ('6000',12776,1,8,'LLAMADA AL FLUJO 12776',12776,0,0,1,1,30,30);
insert into isys_querys_tx values ('6000',12778,1,8,'LLAMADA AL FLUJO 12778',12778,0,0,1,1,30,30);

--Llama flujo 13710 Cuadro 1 Emitidos
insert into isys_querys_tx values ('6000',13710,1,8,'Llama Flujo 13710',13710,0,0,1,1,0,0);
--Llama flujo 15100 Busqueda
insert into isys_querys_tx values ('6000',15100,1,8,'Llama Flujo 15100',15100,0,0,1,1,0,0);
--Llama flujo 15101 Busqueda Folio
insert into isys_querys_tx values ('6000',15101,1,8,'Llama Flujo 15101',15101,0,0,1,1,0,0);
--Llama flujo 15102 Busqueda Recibidos
insert into isys_querys_tx values ('6000',15102,1,8,'Llama Flujo 15102',15102,0,0,1,1,0,0);
--Grafico KPI SII
insert into isys_querys_tx values ('6000',15103,1,8,'Llama Flujo 15103',15103,0,0,1,1,0,0);
--Flujo busqueda RDS
insert into isys_querys_tx values ('6000',15200,1,8,'Llama Flujo 15200',15200,0,0,1,1,0,0);
--Consulta Estado Reclamo
insert into isys_querys_tx values ('6000',16106,1,8,'Llama Flujo 16106',16106,0,0,1,1,0,0);
--Llama flujo 14710 Cuadro 1 Emitidos
insert into isys_querys_tx values ('6000',14710,1,8,'Llama Flujo 14710',14710,0,0,1,1,0,0);
--Para el dashboardv2
insert into isys_querys_tx values ('6000',14711,1,8,'Llama flujo 14710',14710,0,0,0,0,30,30);
insert into isys_querys_tx values ('6000',14715,1,8,'Llama Flujo 14715',14715,0,0,1,1,0,0);
insert into isys_querys_tx values ('6000',14730,1,8,'Llama Flujo 14730',14730,0,0,1,1,0,0);
insert into isys_querys_tx values ('6000',14810,1,8,'Llama Flujo 14810',14810,0,0,1,1,0,0);
insert into isys_querys_tx values ('6000',14720,1,8,'Llama Flujo 14720',14720,0,0,1,1,0,0);


--Flujo Reclamo
insert into isys_querys_tx values ('6000',16100,1,8,'Llama flujo 16100',16100,0,0,1,1,30,30);
insert into isys_querys_tx values ('6000',16201,1,8,'Llama flujo 16201',16201,0,0,1,1,0,0);

--Llama flujo 25100 Busqueda DEC
insert into isys_querys_tx values ('6000',25100,1,8,'Llama Flujo 25100',25100,0,0,1,1,0,0);
--Llama flujo 25101 Busqueda DEC Codigo DOC
insert into isys_querys_tx values ('6000',25101,1,8,'Llama Flujo 25101',25101,0,0,1,1,0,0);
--Llama flujo 25106 Reporteria DEC
insert into isys_querys_tx values ('6000',25106,1,8,'Llama Flujo 25106',25106,0,0,1,1,0,0);

CREATE or replace FUNCTION generico10k_6000_test(json) RETURNS json AS $$
DECLARE
    json1                alias for $1;
    json2                json;
BEGIN
    json2:=json1;
    json2:=put_json(json2,'parametro_test','12345_test');
    json2:=put_json(json2,'__SECUENCIAOK__','13'); 
    return json2;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION log_generico10k_6000(json) RETURNS json AS $$
DECLARE
    json1                alias for $1;
    json2                json;
BEGIN
    json2:=json1;
    json2:=put_json(json2,'__SECUENCIAOK__','10'); 
    json2:=logjson(json2,'JSON INPUT='||chr(10)||json2);
    return json2;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION generico10k_6000(json) RETURNS json AS $$
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
    respuesta1		varchar;
    stSec               define_secuencia_generico10k%ROWTYPE;
    file_wsdl1  varchar;
	sesion1	varchar;
	stSesion	sesion_web_10k%ROWTYPE;
	rut_empresa1	varchar;
	stMenu		menu_10k%ROWTYPE;
	--stMaster	maestro_clientes%ROWTYPE;
	--stPerfilApp	perfil_10k_aplicacion%ROWTYPE;
	modo_qa1	varchar;	
	rol1		varchar;
	funcionalidad1	varchar;
	aux1		varchar;
	request_uri1    varchar;
        aux2            varchar;
        campo1  record;
	campo2	record;
	json_menu1      json;
	app1	varchar;
	tab1	varchar;
	json3	json;
	flag_actualiza_sesion1 boolean;
	grupo_app1	varchar;
	campo	record;
	menu_recursivo1	varchar;
	menu_lateral1	varchar;
	flag_maestro_clientes	boolean;
BEGIN
    json2:=json1;
    json2:=put_json(json2,'__SECUENCIAOK__','0');
    json2:=put_json(json2,'__FLUJO_ENTRADA__','6000');

    --json2:=decode(get_json('INPUT',json2),'hex');
    -- CD 20170619 permite peticion get para aplicacion movil
    if (get_json('REQUEST_METHOD',json2)='GET') then
        json2:=logjson(json2,'Entro GET');
        json2:=put_json(json2,'QUERY_STRING',decodifica_url(get_json('QUERY_STRING',json2)));
        json2:=get_parametros_get_json(json2);
    end if;

    tipo_tx1:=get_json('tipo_tx',json2);
    --json2:=logjson(json2,'JSON INPUT='||chr(10)||log_json_ident_c(json2));
    --json2:=logjson(json2,'JSON INPUT2='||chr(10)||json2);

    --Siempre se limpia esta variable para que el login
    json2:=put_json(json2,'flag_sesion_externa','NO');
   
   --json2:=logjson(json2,'TX='||tipo_tx1||' Procesos Activos='||get_json('__PROC_ACTIVOS__',json2));

    select * into stSec from define_secuencia_generico10k where tipo_tx=tipo_tx1;
    if not found then
	json2:=put_json(json2,'CODIGO_RESPUESTA','2');
	json2:=put_json(json2,'MENSAJE_RESPUESTA','Servicio '||tipo_tx1||'No Habilitado');
	return response_requests_6000('2','Servicio '||tipo_tx1||' No Habilitado','',json2);
    end if;
    sesion1:=get_json('session_id',json2);


   --Toda tx debe validar la sesion excepto el login
   if (tipo_tx1 not in ('logIn','logInMobile','sesion_externa','logIn_qa','changePassSerieCI','addNewPerson','guardar_transbank_facturacion','update_estado_certificado','consultaEmpresaLanding','validacion_hash_correo','icar_verifica_usuario','get_listado_productos_notarisa','get_empresas_enrolamiento')) then
	--Obtengo la sesion
	if length(sesion1)=0 then
		json2:=logjson(json2,'Sesion Invalida');
		return response_requests_6000('666','Sesion Invalida','',json2);
	end if;
	--Busco la sesion en la tabla de sesiones
	select * into stSesion from sesion_web_10k where sesion=sesion1;
	if not found then
		--Se verifica la sesion externa (manager)
		select * into stSesion from sesion_web_10k where sesion_manager=sesion1;
		if not found then
			json2:=logjson(json2,'Sesion '||sesion1||' No encontrada');
			return response_requests_6000('666','Su sesión ha expirado, la aplicacion se cerrara','',json2);
			return response_requests_6000('666','Se ha abierto otra sesion con su usuario, la aplicacion se cerrara','',json2);
		end if;
		--Se abre con sesion manager
		json2:=put_json(json2,'flag_sesion_manager','SI');
	end if;
	--2018-06-04 se graba la fecha del ultimo acceso (mas de 5 minutos) para cerrar las sesiones inactivas
	update sesion_web_10k set fecha_ultimo_acceso=now() where sesion=sesion1 and now()-coalesce(fecha_ultimo_acceso,'2000-01-01'::timestamp)>interval '5 minutes';

        --json2:=put_json(json2,'json_data_sesion',stSesion.json_data::varchar);
	json2:=put_json(json2,'sesion_manager',stSesion.sesion_manager);
	rol1:=stSesion.rol;
	
	if (get_json('rutCliente',json2)='' or is_number(replace(split_part(get_json('rutCliente',json2),'-',1),'.','')) is false) then
		json2:=put_json(json2,'rutCliente',stSesion.id_empresa);
	end if;

	--2015-12-07 DAO  Se modifica la forma de verificar los tipo_tx de manera de hacer 1 insert en vez de 3 cada vez que se crea una nueva appDinamica
	
	if (stSec.categoria not in ('FRAMEWORK','WEBSERVICE')) then
		app1:=get_json('APP',json2);
    		json2:=logjson(json2,'Ejecuta app='||app1||' tipo_tx='||tipo_tx1||' Sesion='||sesion1);

		if(strpos(app1,'###')>0) then
			app1:=split_part(app1,'###',1);
			tab1:=split_part(app1,'###',2);
		end if;
		if(app1='') then
			json2:=logjson(json2,'ERROR_6000 Aplicacion no definida tipo_tx='||tipo_tx1||' APP='||app1||' no viene en app_dinamica');
			return response_requests_6000('666','Transaccion no habilitada para su perfil','',json2);
		end if;

                --Validamos la transaccion que pertenezca a la aplicacion, sobre el catalogo de aplicaciones
                select * into campo2 from menu_info_10k where id2=app1;
                if not found then
                        json2:=logjson(json2,'ERROR_6000 aplicacion no definida en menu_info_10k id2='||app1);
			return response_requests_6000('666','Transaccion no habilitada para su perfil','',json2);
                else
                        --Si no esta la tx la agregamos a la app
                        if ((campo2.txs@@get_json('tipo_tx',json2)::tsquery) is false or campo2.txs is null) then
                                --chequeamos si esta dentro de las txs de las funcionalidades
                                json2:=logjson(json2,'ERROR_6000 Tx '||get_json('tipo_tx',json2)||' no incluida en '||app1);
				return response_requests_6000('666','Transaccion no habilitada para su perfil','',json2);
                        end if;
                end if;
				

		if (get_json('aplicacion',json2)='') then
			json2:=put_json(json2,'aplicacion',stSesion.aplicacion);
		end if;
		grupo_app1:=get_json('aplicacion',json2);

		json2:=logjson(json2,'FAY-'||get_json('rutCliente',json2)||'-'||stSesion.id_empresa||'-'||get_json('aplicacion',json2)||'-'||stSesion.aplicacion||'-'||sesion1);
		--Verificamos si cambio el rut cliente (caso de 2 pestañas de busqueda)
		if (get_json('rutCliente',json2)<>stSesion.id_empresa or get_json('aplicacion',json2)<>stSesion.aplicacion) then
			--Reviso el cambio
			json3:='{}';	
			json3:=put_json(json3,'rutUsuario',stSesion.rut_usuario);
			--la que estaba en la sesion o la nueva 
			json3:=put_json(json3,'aplicacion',grupo_app1);
			json3:=put_json(json3,'rutCliente',get_json('rutCliente',json2));
			json3:=put_json(json3,'rutClienteAnterior',stSesion.id_empresa::varchar);
			json3:=put_json(json3,'aplicacionAnterior',stSesion.aplicacion);
	 
			json2:=logjson(json2,'DAONOT6000 Validamos Cambio de App o Rut Cliente para sesion '||sesion1||' json3='||json3::varchar);
			json3:=valida_super_user_6000(json3);
			if (get_json('__exit__',json2)='SI') then
				json2:=logjson(json2,'ERROR_6000 Falla Cambio de  App o Rut Cliente para sesion '||sesion1||' json3='||json3::varchar);
				return response_requests_6000('666','Transaccion no habilitada para su perfil','',json2);
			end if;
			--Actualizamos la sesion del usuario
			UPDATE sesion_web_10k SET dominio_canal=null,id_empresa=get_json('rutCliente',json3),aplicacion=get_json('aplicacion',json3),json_menu=null,rol=get_json('perfil',json3),flag_super_user=get_json('flag_super_user',json3),flag_input_empresa=get_json('flag_input_empresa',json3) WHERE sesion=stSesion.sesion;
			--Seteo los datos hacia adelante
			json2:=put_json(json2,'aplicacion',get_json('aplicacion',json3));
			json2:=put_json(json2,'rutCliente',get_json('rutCliente',json3));
			json2:=put_json(json2,'rol_usuario',get_json('perfil',json3));
			json2:=put_json(json2,'flag_super_user',get_json('flag_super_user',json3));
			rol1:=get_json('perfil',json3);
			stSesion.id_empresa:=get_json('rutCliente',json3);
			stSesion.aplicacion:=get_json('aplicacion',json3);
			stSesion.rol:=rol1;
			--Cambia el menu recursivo original
			menu_recursivo1=get_json('menu_recursivo',json3);
			menu_lateral1=get_json('menu_lateral',json3);
			json2:=logjson(json2,'DAONOT6000 Termina en sesion '||sesion1||' json3='||json3::varchar);
		else
	               --Busca el menu usado recientemente para este usuario (98%)
	                select * into campo1 from menu_perfil_10k where perfil=rol1 and aplicacion=stSesion.aplicacion;
        	        if not found then
				json2:=logjson(json2,'No se encuentra perfil:'||coalesce(rol1,'NULO')||', aplicacion='||coalesce(stSesion.aplicacion,'NULO'));
                	        json2:=logjson(json2,'ERROR_6000 Menu no encontrado para la sesion '||sesion1||' '||stSesion.aplicacion||' '||rol1);
                        	return response_requests_6000('666','Transaccion no habilitada para su perfil','',json2);
	                end if;
			 menu_recursivo1:=campo1.menu_recursivo;
			 menu_lateral1:=campo1.menu_lateral;
		end if;

		--Leemos el maestro de clientes para verificar la funcionalidad permitida
		json2:=put_json(json2,'rol_usuario',stSesion.rol);
		json2:=lee_maestro_clientes_multiapp(stSesion.id_empresa::integer,stSesion.aplicacion,json2);
                if (get_json('__exit__',json2)='SI') then
                        return json2;
                end if;
		flag_maestro_clientes:=true;
		if (get_json('empresa_bloqueada',json2)='SI') then
			menu_recursivo1:=get_json('menu_recursivo',json2);
			menu_lateral1:=get_json('menu_lateral',json2);
			stSesion.rol:=get_json('rol_usuario',json2);	
		end if;

                --chequeamos si tiene permitida la aplicacion
                json_menu1:=aplicaciones_menu(menu_recursivo1,app1,json2);
		if get_json('rutUsuario',json2)='18379016' then
			perform logfile('select aplicaciones_menu('''||menu_recursivo1||''','''||app1||''','''||json2::varchar||''');');
		end if;
                if (get_json('STATUS',json_menu1)<>'OK') then
			--Busco la app en el menu lateral
                        --json_menu1:=aplicaciones_menu(menu_lateral1,app1);
			--DAO-20180423 Se tienen que considerar las funcionalidades del menu_lateral
			json_menu1:=aplicaciones_menu(menu_lateral1,app1,json2);
                        if (get_json('STATUS',json_menu1)<>'OK') then
		                json2:=logjson(json2,'ERROR_6000 Aplicacion no permitida para la sesion '||coalesce(sesion1,'')||' APP='||coalesce(app1,'')||' tipo_tx='||tipo_tx1::varchar||' MENU_PADRE='||menu_recursivo1||' LATERAL='||menu_lateral1);
				return response_requests_6000('666','Transaccion no habilitada para su perfil','',json2);
			end if;
		end if; 
		--2017-12-26 DAO-FAY se agregan los campos flag_ como funcionalidades del escritorio
		funcionalidad1:=get_json('FUNCIONALIDAD',json_menu1)||' '||get_json('FUNCIONALIDADES_MAESTRO',json2);
		json2:=put_json(json2,'FUNCIONALIDAD',funcionalidad1);
		json2:=logjson(json2,'Funcionalidad '||coalesce(funcionalidad1,''));
	end if;

   	
	--Para todas las tx que no sean escritorio, el rut cliente es el que tengo en la sesion
	--No el que viene en el json
   	if (tipo_tx1 not in ('escritorio','escritorio_qa')) then
		json2:=put_json(json2,'rutCliente',stSesion.id_empresa);
		json2:=put_json(json2,'aplicacion',stSesion.aplicacion);
		rut_empresa1:=stSesion.id_empresa;
		--Para otras tx el rol siempre es el que tiene la sesion
		json2:=put_json(json2,'rol_usuario',stSesion.rol);
	
		--Si no he leido el maestro de clientes
		if (flag_maestro_clientes is false) then
			--Se lee el maestro de clientes segun la aplicaion entrante
		        json2:=lee_maestro_clientes_multiapp(rut_empresa1::integer,stSesion.aplicacion,json2);
        		if (get_json('__exit__',json2)='SI') then
                		return json2;
		        end if;
		end if;

		--Vemos si existe funcion de validacion para este grupo de aplicacion...XXXX	
		select * into campo from apps_10k where appname=stSesion.aplicacion;
		if (campo.funcion_validacion is not null and campo.funcion_validacion<>'') then
        		EXECUTE 'SELECT ' || campo.funcion_validacion || '(' || chr(39) || json2 || chr(39) || '::json)' into json2;
        		if (get_json('__exit__',json2)='SI') then
                		return json2;
		        end if;
		end if; 
	else
		--Solo para el escritorio
		json2:=put_json(json2,'json_menu',stSesion.json_menu);
		json2:=put_json(json2,'flag_input_empresa',stSesion.flag_input_empresa);
		rut_empresa1:=replace(split_part(get_json('rutCliente',json2),'-',1),'.','');
	        if(length(rut_empresa1)=0)then
         	       rut_empresa1:=stSesion.id_empresa;
                       json2:=put_json(json2,'rutCliente',stSesion.id_empresa);
	        else
        	       json2:=put_json(json2,'rutCliente',rut_empresa1);
	        end if;
	end if;
    


	/*
	--Validamos el rut de la empresa
	rut_empresa1:=replace(split_part(get_json('rutCliente',json2),'-',1),'.','');
	if(length(rut_empresa1)=0)then
		rut_empresa1:=stSesion.id_empresa;
		json2:=put_json(json2,'rutCliente',stSesion.id_empresa);
	else
		json2:=put_json(json2,'rutCliente',rut_empresa1);
	end if;
	*/

	--Validamos que el rut de la empresa este activo en el maestro de clientes
	/*
	select * into stMaster from maestro_clientes where rut_emisor=rut_empresa1::integer;
	if not found then
		json2:=logjson(json2,'No existe empresa en maestro de clientes '||stSesion.rut_usuario||' y Empresa '||rut_empresa1);
		return response_requests_6000('666','Empresa no registrada','',json2);
	end if;
	*/
	/*
	--Se lee el maestro de clientes segun la aplicaion entrante
        json2:=lee_maestro_clientes_multiapp(rut_empresa1::integer,stSesion.aplicacion,json2);
        if (get_json('__exit__',json2)='SI') then
                return json2;
        end if;
	*/

	
	--Solo empresas activas
	/*
	if (get_json('estado_maestro',json2) in ('BLOQUEADO','BLOQUEADO_QBIS') and stSesion.flag_super_user='NO') then
		json2:=logjson(json2,'Empresa Bloqueada en maestro_clientes- '||stSesion.rut_usuario||' y Empresa '||rut_empresa1);
		return response_requests_6000('666','Empresa Bloqueada, llame a Acepta','',json2);
	end if;
	*/

	--Si viene la IPCliente desde el php, debe ser igual a la de apertura de la sesion
	aux1:=get_json('IPCliente',json2);
	if (length(aux1)>0) then
		if (aux1<>stSesion.ip_cliente) then
			--return response_requests_6000('666','Sesion Invalida','',json2);
			json2:=logjson(json2,'Cliente cambia IP de Origen');
		end if;
	else
		--Si no viene la seteo en el json
		json2:=put_json(json2,'IPCliente',stSesion.ip_cliente);
	end if;
   
		
	json2:=put_json(json2,'dominio_canal',stSesion.dominio_canal);
	aux1:=get_json('SERVER_NAME_ORI',json2);
	if (aux1='') then
		json2:=put_json(json2,'host_canal',stSesion.host_canal);
		--json2:=put_json(json2,'host_canal','escritorio.acepta.com');
	else
		json2:=put_json(json2,'host_canal',aux1);
	end if;
	json2:=put_json(json2,'mail_usuario',stSesion.mail_usuario);
	--json2:=put_json(json2,'dominio_empresa',stMaster.dominio);
	--json2:=put_json(json2,'razon_social',replace(stMaster.razon_social,chr(39),' '));
	--json2:=put_json(json2,'tipo_plan_mc',stMaster.plan);
	--json2:=put_json(json2,'flag_reporte_impuesto',stMaster.flag_reporte_impuesto);
	--json2:=put_json(json2,'maestro_total_referencias',stMaster.total_referencias::varchar);	
	--json2:=put_json(json2,'maestro_flag_excel',stMaster.flag_excel);	
	--Guardo el rutCliente anterior, por si se esta cambiando
	json2:=put_json(json2,'rutClienteAnterior',stSesion.id_empresa);
	json2:=put_json(json2,'aplicacionAnterior',stSesion.aplicacion);
	json2:=put_json(json2,'ip_cliente_actual',stSesion.ip_cliente);

	json2:=put_json(json2,'json_sesion',split_part(stSesion.json_respuesta,chr(10)||chr(10),2));
	json2:=put_json(json2,'uri_base',stSesion.uri_base);
	json2:=put_json(json2,'aplicacion_actual',stSesion.aplicacion);
	json2:=put_json(json2,'flag_super_user',stSesion.flag_super_user);
	json2:=put_json(json2,'rutUsuario',stSesion.rut_usuario);
	json2:=put_json(json2,'rut_firma',stSesion.rut_usuario);
	json2:=put_json(json2,'nombre_usuario',stSesion.nombre);
	--json2:=put_json(json2,'parametros_config_usuarios',stMaster.flag_parametros_avanzados_usuario);
	--json2:=put_json(json2,'parametro_muestra_excepciones',stMaster.flag_muestra_excepciones);
	--json2:=put_json(json2,'parametro_pdf_masivo',stMaster.flag_pdf_masivo);
--	json2:=put_json(json2,'ESCRITORIO',stSesion.json_menu);
	json2:=logjson(json2,'rutUsuario Sesion='||get_json('rutUsuario',json2));
	json2:=logjson(json2,'rut_firma Sesion='||get_json('rut_firma',json2));
   else
	--Si es una tx login u otra que no requiere sesion
	aux1:=get_json('SERVER_NAME_ORI',json2);
	if (aux1='') then
		json2:=put_json(json2,'host_canal','escritorio.acepta.com');
	else
		json2:=put_json(json2,'host_canal',aux1);
	end if;
   end if;

/*


    select * into stSec from define_secuencia_generico10k where tipo_tx=tipo_tx1;
    if not found then
	json2:=put_json(json2,'CODIGO_RESPUESTA','2');
	json2:=put_json(json2,'MENSAJE_RESPUESTA','Servicio '||tipo_tx1||'No Habilitado');
	return response_requests_6000('2','Servicio '||tipo_tx1||' No Habilitado','',json2);
    end if;
*/
    
    json2:=put_json(json2,'FUNCION_INPUT',stSec.funcion_input);
    json2:=put_json(json2,'FUNCION_RESPUESTA',stSec.funcion_output);

    if (stSec.flag_json='SI' and length(sesion1)>0) then
        json2:=logjson(json2,'GRABA json_respuesta en session, tipo_tx=' || tipo_tx1);
	respuesta1:='Status: 200' ||chr(10)||'Content-type: json'||chr(10)||'Content-Length: '||octet_length(json1::text)::varchar||chr(10)||chr(10)||json1::varchar;
	--Grabo input original
        UPDATE sesion_web_10k set json_respuesta=respuesta1,tipo_res='NORMAL',last_tx=tipo_tx1  WHERE sesion=sesion1;
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
		if (stSec.tipo_data='NEW') then
                	json2:=logjson(json2,'Ejecuta New Funcion '||stSec.funcion_input||' tipo_tx='||tipo_tx1||' en Base BASE_3_WEBIECV');
	                json2:=put_json(json2,'__SECUENCIAOK__','97');
		else
                	json2:=logjson(json2,'Ejecuta Funcion '||stSec.funcion_input||' tipo_tx='||tipo_tx1||' en Base BASE_3_WEBIECV');
	                json2:=put_json(json2,'__SECUENCIAOK__','95');
		end if;
                return json2;
        end if;

	--Base de Gestor de Folio
	if (stSec.base_datos='BASE_GESTOR_FOLIOS') then
		json2:=logjson(json2,'Ejecuta Funcion '||stSec.funcion_input||' tipo_tx='||tipo_tx1||' en Base BASE_15_GESTORFOLIOS');
		json2:=put_json(json2,'__SECUENCIAOK__','125');
		return json2;
	end if;
	--Base de DEC
	if (stSec.base_datos='BASE_DEC') then
		json2:=logjson(json2,'Ejecuta Funcion '||stSec.funcion_input||' tipo_tx='||tipo_tx1||' en Base BASE_DEC');
		json2:=put_json(json2,'__SECUENCIAOK__','49');
		return json2;
	end if;
	--API_BASE_IDD_CHILE
	if (stSec.base_datos='API_BASE_IDD_CHILE') then
		json2:=logjson(json2,'Ejecuta Funcion '||stSec.funcion_input||' tipo_tx='||tipo_tx1||' en Base BASE_DEC');
		json2:=put_json(json2,'__SECUENCIAOK__','52');
		return json2;
	end if;
	--Base de Bolsa
	if (stSec.base_datos='BASE_FINANCIAMIENTO') then
		json2:=logjson(json2,'Ejecuta Funcion '||stSec.funcion_input||' tipo_tx='||tipo_tx1||' en BASE BASE_FINANCIAMIENTO');
		json2:=put_json(json2,'__SECUENCIAOK__','500');
		return json2;
	end if;
	--Base Send Mail
	if (stSec.base_datos='BASE_SEND_MAIL') then
		json2:=logjson(json2,'Ejecuta Funcion '||stSec.funcion_input||' tipo_tx='||tipo_tx1||' en BASE BASE_FINANCIAMIENTO');
		json2:=put_json(json2,'__SECUENCIAOK__','501');
		return json2;
	end if;


        --Si va a la base replica
	if (stSec.base_datos='BASE_REPLICA_11') then
		json2:=logjson(json2,'Base BASE_REPLICA_11');
		json2:=put_json(json2,'__SECUENCIAOK__','127');
		return json2;
        end if;
	--RME GAC 20160812 Base de LCE
	--Libros Contables Electronicos en Host 
        if (stSec.base_datos='BASE_16_LCE') then
                json2:=logjson(json2,'Ejecuta Funcion= '||stSec.funcion_input||' tipo_tx='||tipo_tx1||' en Base BASE_16_LCE');
                json2:=put_json(json2,'__SECUENCIAOK__','300');
                return json2;
        end if;
        -- GAC 20170213 Base Importer 27
        if (stSec.base_datos='BASE_27_IMPORTER') then
                json2:=logjson(json2,'Ejecuta Funcion= '||stSec.funcion_input||' tipo_tx='||tipo_tx1||' en Base BASE_16_LCE');
                json2:=put_json(json2,'__SECUENCIAOK__','400');
                return json2;
        end if;
        --JSE 20170719 BD_ACM.
       if (stSec.base_datos='BD_ACM') then
                json2:=logjson(json2,'Ejecuta Funcion= '||stSec.funcion_input||' tipo_tx='||tipo_tx1||' en Base BD_ACM');
                json2:=put_json(json2,'__SECUENCIAOK__','301');
                return json2;
        end if;
	-- NBV 20180309 BASE_WEBPAY
        if (stSec.base_datos='BASE_WEBPAY') then
                json2:=logjson(json2,'Ejecuta Funcion= '||stSec.funcion_input||' tipo_tx='||tipo_tx1||' en Base BASE_WEBPAY');
                json2:=put_json(json2,'__SECUENCIAOK__','551');
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
        json2:=put_json(json2,'__SECUENCIAOK__','22');
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


CREATE or replace FUNCTION pivote_base_primaria_6000(json) RETURNS json AS $$
DECLARE
    json1                alias for $1;
    json2        json;
    f1    varchar;
    segundos1	numeric;
begin
    json2:=json1;
    json2:=logjson(json2,'pivote_base_primaria_6000');
    f1:=get_json('FUNCION_INPUT',json2);
    --EJECUTA FUNCION INPUT
    json2:=logjson(json2,'Ejecuta='||f1);
    if length(f1)>0 then
        EXECUTE 'SELECT ' || f1 || '(' || chr(39) || json2 || chr(39) || '::json)' into json2;
    end if;

    --Si necesita llamar un scrip lo ejecuta
    if (get_json('LLAMA_SCRIPT',json2)='SI') then
        json2:=logjson(json2,'Ejecuta Shell='||get_json('SCRIPT',json2));
        json2:=put_json(json2,'__SECUENCIAOK__','20');
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

CREATE or replace FUNCTION pivote_base_replica_6000(json) RETURNS json AS $$
DECLARE
    json1                alias for $1;
    json2        json;
    f1    varchar;
    segundos1	numeric;
begin
    json2:=json1;
    --Verifico que la base de replica este sincronizada
    segundos1:=(SELECT CASE WHEN pg_last_xlog_receive_location() = pg_last_xlog_replay_location() THEN 0 ELSE EXTRACT (EPOCH FROM now() - pg_last_xact_replay_timestamp()) END AS log_delay);
    if (segundos1>60) then
	--Procesamos en la base primaria
	json2:=logjson(json2,'pivote_base_replica_6000 Volvemos a primaria '||segundos1::varchar);
	json2:=put_json(json2,'__SECUENCIAOK__','128');
        return json2;
    end if;
    json2:=logjson(json2,'pivote_base_replica_6000');
    f1:=get_json('FUNCION_INPUT',json2);
    --EJECUTA FUNCION INPUT
    json2:=logjson(json2,'Ejecuta='||f1);
    if length(f1)>0 then
        EXECUTE 'SELECT ' || f1 || '(' || chr(39) || json2 || chr(39) || '::json)' into json2;
    end if;
/*
    --Si necesita llamar un scrip lo ejecuta
    if (get_json('LLAMA_SCRIPT',json2)='SI') then
        json2:=logjson(json2,'Ejecuta Shell='||get_json('SCRIPT',json2));
        json2:=put_json(json2,'__SECUENCIAOK__','20');
    end if;

    --Si es un flujo
    if (get_json('LLAMA_FLUJO',json2)='SI') then
        json2:=logjson(json2,'Ejecuta Flujo Secuencia='||get_json('__SECUENCIAOK__',json2));
        return json2;
    end if;
*/
    --Si algo va mal con este query, el procesador enviara a la secuencia timeout
   json2:=put_json(json2,'__SECUENCIA_TIMEOUT__','15');

   return json2;
END;
$$ LANGUAGE plpgsql;



CREATE or replace FUNCTION generico10k_resp_6000(varchar) RETURNS varchar AS $$
DECLARE
    json1                alias for $1;
    json2        json;
    funcion1    varchar;
begin
    --EJECUTA FUNCION output
    json2:=json1;
    json2:=put_json(json2,'__SECUENCIAOK__','0');
    funcion1:=get_json('FUNCION_RESPUESTA',json2);
    json2:=logjson(json2,'Ejecuta Respuesta='||funcion1);
    if length(funcion1)>0 then
        EXECUTE 'SELECT ' || funcion1 || '(' || chr(39) || json2 || chr(39) || '::json)' into json2;
    end if;
    return json2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION secuencia_timeout_6000(varchar) RETURNS json AS $$
DECLARE
    json1                alias for $1;
    json2        json;
    funcion1    varchar;
begin
    --EJECUTA FUNCION output
    BEGIN
    json2:=json1::json;
    EXCEPTION WHEN OTHERS THEN
    	insert into log_timeout_motor_6000 (fecha,tipo_tx,data_mala) values (now(),'NO JSON',json1);
	return response_requests_6000('2','Reintente Por favor','',json2);
    END;
    json2:=put_json(json2,'__SECUENCIAOK__','0');
    --Registramos el timeout
    insert into log_timeout_motor_6000 values (now(),get_json('tipo_tx',json2),get_json('rutUsuario',json2),get_json('rutCliente',json2),json1::json);
    return response_requests_6000('2','Reintente Por favor','',json2);
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION salida_webiecv_6000(json) RETURNS json AS $$
DECLARE
    json1                alias for $1;
    json2        json;
    resp1    varchar;
    cod_resp1 varchar;
    msg_resp1 varchar;
begin
    --EJECUTA FUNCION output
/*json2:='{}'::json;
json2:=logjson(json2,'PARAMETRO salida 6000-->'|| json1);

return json2;
*/
    json2:=json1;
    json2:=put_json(json2,'__SECUENCIAOK__','0');
    resp1:=get_json('RESPUESTA_WEBIECV',json2);
    --resp1 := json2::varchar;
    json2:=logjson(json2,'WEBIECV:Respuesta='||resp1);
    cod_resp1:=get_json('CODIGO_RESPUESTA',json2);
    msg_resp1:=get_json('MENSAJE_RESPUESTA',json2);
    --Cuando hay un error en la BD
    if cod_resp1='' then
	cod_resp1:='700';
	msg_resp1:='Sin respuesta del servidor, reintente en unos minutos.';
    end if;


    -- BEGIN GAC 2016-09-21 Graba Bitacora
    IF (get_json('COMENTARIO_BITACORA', json2) != '') THEN
        perform bitacora10k(json2, 'WEBIECV', get_json('COMENTARIO_BITACORA', json2));
    END IF;
    -- END GAC 2016-09-21 Graba Bitacora

	
/*   IF (get_json('FUNCION_INPUT',json2) = 'iecv.iniciar_carga' AND get_json('CODIGO_RESPUESTA',json2) = '200')   THEN
	json2:=put_json(json2, '__SOCKET_RESPONSE__','RESPUESTA');
	json2:=put_json(json2, '__TIPO_SOCKET_RESPONSE__','SCGI');
        --json2:=put_json(json2, '__FLUJO_POST_EXIT__', 'SI');
        json2:=put_json(json2, '__SECUENCIAOK__', '95');
        --json2:=put_json(json2, '__SECUENCIA_POST_OK__', '95');
        json2:=put_json(json2,'FUNCION_INPUT','iecv.iniciar_carga_csv');
        --json2:=logjson(json2,'Cambio de Funcion Asincronica!!! ------------->'|| json1);
    END IF;
*/
    --RME, GAC 20160324 para exportar libros asincronicamente
    IF (get_json('tipo_tx',json2) = 'iecv_exportacion_libro' AND get_json('CODIGO_RESPUESTA',json2) = '239') THEN
        json2:=put_json(json2, '__SOCKET_RESPONSE__','RESPUESTA');
        json2:=put_json(json2, '__TIPO_SOCKET_RESPONSE__','SCGI');
        -- llama al flujo 13120 (Exportacion de Libros)
        json2:=put_json(json2, '__SECUENCIAOK__', '99');
    END IF;

    --json2:=decode(get_json('INPUT',json2),'hex');
    --return response_requests_6000(get_json('CODIGO_RESPUESTA',json2),get_json('MENSAJE_RESPUESTA',json2),resp1,json2);
    return response_requests_6000(cod_resp1,msg_resp1,resp1,json2);


END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION salida_gestor_6000(json) RETURNS json AS $$
DECLARE
    json1                alias for $1;
    json2        json;
    resp1    varchar;
    cod_resp1 varchar;
    msg_resp1 varchar;
begin
    --EJECUTA FUNCION output
/*json2:='{}'::json;
json2:=logjson(json2,'PARAMETRO salida 6000-->'|| json1);

return json2;
*/
    json2:=json1;
    json2:=put_json(json2,'__SECUENCIAOK__','0');
    resp1:=get_json('RESPUESTA_GESTOR',json2);
    --resp1 := json2::varchar;
    json2:=logjson(json2,'GESTOR:Respuesta='||resp1);
    cod_resp1:=get_json('CODIGO_RESPUESTA',json2);
    msg_resp1:=get_json('MENSAJE_RESPUESTA',json2);
    --Cuando hay un error en la BD
    if cod_resp1='' then
        cod_resp1:='700';
        msg_resp1:='Sin respuesta del servidor, reintente en unos minutos.';
    end if;

    --json2:=decode(get_json('INPUT',json2),'hex');
    --return response_requests_6000(get_json('CODIGO_RESPUESTA',json2),get_json('MENSAJE_RESPUESTA',json2),resp1,json2);
    return response_requests_6000(cod_resp1,msg_resp1,resp1,json2);


END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION salida_lce_6000(json) RETURNS json AS $$
DECLARE
    json1                alias for $1;
    json2        json;
    resp1    varchar;
    cod_resp1 varchar;
    msg_resp1 varchar;
begin
    json2:=json1;
    json2:=put_json(json2,'__SECUENCIAOK__','0');
    resp1:=get_json('RESPUESTA_LCE',json2);
    json2:=logjson(json2,'LCE:Respuesta='||resp1);
    cod_resp1:=get_json('CODIGO_RESPUESTA',json2);
    msg_resp1:=get_json('MENSAJE_RESPUESTA',json2);
    --Cuando hay un error en la BD
    if cod_resp1='' then
        cod_resp1:='700';
        msg_resp1:='Sin respuesta del servidor, reintente en unos minutos.';
    end if;

    IF (get_json('tipo_tx',json2) = 'lce_enviar_libro' AND get_json('CODIGO_RESPUESTA',json2) = '200') THEN
        json2:=put_json(json2, '__SOCKET_RESPONSE__','RESPUESTA');
        json2:=put_json(json2, '__TIPO_SOCKET_RESPONSE__','SCGI');
        -- llama al flujo 13210 (Firma de Libros)
        json2:=put_json(json2, '__SECUENCIAOK__', '306');
    ELSIF (get_json('tipo_tx',json2) = 'lce_exportar_csv' AND get_json('CODIGO_RESPUESTA',json2) = '239') THEN
        json2:=put_json(json2, '__SOCKET_RESPONSE__','RESPUESTA');
        json2:=put_json(json2, '__TIPO_SOCKET_RESPONSE__','SCGI');
        -- llama al flujo 13120 (Exportacion de Libros)
        json2:=put_json(json2, '__SECUENCIAOK__', '99');
    END IF;

    return response_requests_6000(cod_resp1,msg_resp1,resp1,json2);


END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION salida_importer_6000(json) RETURNS json AS $$
DECLARE
    i_parametros        alias for $1;
    o_json              json;
    v_respuesta         varchar;
    v_codigo_respuesta  varchar;
    v_mensaje_respuesta varchar;
begin
    o_json := i_parametros;
    o_json := put_json(o_json,'__SECUENCIAOK__','0');
    v_respuesta := get_json('RESPUESTA_IMPORTER',o_json);
    --v_respuesta := o_json::varchar;
    o_json := logjson(o_json,'IMPORTER:Respuesta='||v_respuesta);
    v_codigo_respuesta := get_json('CODIGO_RESPUESTA',o_json);
    v_mensaje_respuesta := get_json('MENSAJE_RESPUESTA',o_json);
    --Cuando hay un error en la BD
    IF v_codigo_respuesta = '' THEN
        v_codigo_respuesta := '700';
        v_mensaje_respuesta := 'Sin respuesta del servidor, reintente en unos minutos.';
    end if;

    IF (get_json('COMENTARIO_BITACORA', o_json) != '') THEN
        perform bitacora10k(o_json, 'IMPORTER', get_json('COMENTARIO_BITACORA', o_json));
    END IF;

    RETURN response_requests_6000(v_codigo_respuesta, v_mensaje_respuesta, v_respuesta, o_json);
END;
$$ LANGUAGE plpgsql;

