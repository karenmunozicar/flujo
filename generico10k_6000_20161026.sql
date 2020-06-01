CREATE OR REPLACE FUNCTION public.generico10k_6000(json)
 RETURNS json
 LANGUAGE plpgsql
AS $function$
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
	app_dinamica1	varchar;
	request_uri1    varchar;
        aux2            varchar;
        campo1  record;
BEGIN
    json2:=json1;
    json2:=put_json(json2,'__SECUENCIAOK__','0');
    json2:=put_json(json2,'__FLUJO_ENTRADA__','6000');

    --json2:=decode(get_json('INPUT',json2),'hex');

    tipo_tx1:=get_json('tipo_tx',json2);
    --json2:=logjson(json2,'JSON INPUT='||chr(10)||log_json_ident_c(json2));
    --json2:=logjson(json2,'JSON INPUT='||chr(10)||json2);

    --Siempre se limpia esta variable para que el login
    json2:=put_json(json2,'flag_sesion_externa','NO');
   
   json2:=logjson(json2,'TX='||tipo_tx1||' Procesos Activos='||get_json('__PROC_ACTIVOS__',json2));
   json2:=logjson(json2,'JSON INPUT='||chr(10)||json2);

    select * into stSec from define_secuencia_generico10k where tipo_tx=tipo_tx1;
    if not found then
	json2:=put_json(json2,'CODIGO_RESPUESTA','2');
	json2:=put_json(json2,'MENSAJE_RESPUESTA','Servicio '||tipo_tx1||'No Habilitado');
	return response_requests_6000('2','Servicio '||tipo_tx1||' No Habilitado','',json2);
    end if;

   --Si viene de 172.16.10.184, entoces la tx sera considerada como _qa
   if (get_json('REMOTE_ADDR',json2)='172.16.10.184') then
	json2:=logjson(json2,'MODO QA ON');
	json2:=put_json(json2,'MODO_QA','ON');
	--json2:=put_json(json2,'tipo_tx',tipo_tx1||'_qa');
	--json2:=put_json(json2,'TIPO_TX',tipo_tx1||'_qa');
	--tipo_tx1:=tipo_tx1||'_qa';
	modo_qa1:='ON';
   end if;
    

   --Toda tx debe validar la sesion excepto el login
   if (tipo_tx1 not in ('logIn','sesion_externa','logIn_qa','changePassSerieCI','addNewPerson','guardar_transbank_facturacion','update_estado_certificado','consultaEmpresaLanding','validacion_hash_correo')) then
	--Obtengo la sesion
	sesion1:=get_json('session_id',json2);
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

	if (modo_qa1='ON') then
		rol1:=split_part(stSesion.rol,'_',1);
	else
		rol1:=stSesion.rol;
	end if;

	--2015-12-07 DAO  Se modifica la forma de verificar los tipo_tx de manera de hacer 1 insert en vez de 3 cada vez que se crea una nueva appDinamica

	app_dinamica1:=get_json('app_dinamica',json2);
        if(app_dinamica1='') then
                select replace(funcionalidad::varchar,chr(39),'-') into funcionalidad1 from perfil_10k_app where tipo_tx=tipo_tx1 and perfil=rol1;
        else
                select replace(funcionalidad::varchar,chr(39),'-') into funcionalidad1 from perfil_10k_app where tipo_tx=app_dinamica1 and perfil=rol1;
        end if;

	request_uri1:=get_json('REQUEST_URI_ORI',json2);
        if (strpos(request_uri1,'appDinamicaClasses')>0 or strpos(request_uri1,'appDinamicaAcepta')>0) then
                aux2:=split_part(split_part(split_part(request_uri1,'app_dinamica=',2),'%26',1),'&',1);
                if(strpos(request_uri1,'appDinamicaClasses')>0) then
                        if (length(aux2)>0 and length(app_dinamica1)>0) then
                                select * into campo1 from app_tx where aplicacion=aux2 and tipo_tx=app_dinamica1;
                                if not found then
                                        insert into app_tx values(aux2,app_dinamica1);
                                end if;
                        end if;
                else
                        if (length(aux2)>0) then
                                select * into campo1 from app_tx where aplicacion=aux2 and tipo_tx=tipo_tx1;
                                if not found then
                                        insert into app_tx values(aux2,tipo_tx1);
                                end if;
                        end if;
                end if;
        else
                aux2:=split_part(split_part(split_part(request_uri1,'escritorio.acepta.com/',2),'/',1),'%3F',1);
                if (length(aux2)>0 and length(tipo_tx1)>0) then
                        select * into campo1 from app_tx where aplicacion=aux2 and tipo_tx=tipo_tx1;
                        if not found then
                                insert into app_tx values(aux2,tipo_tx1);
                        end if;
                else
                        aux2:=split_part(split_part(split_part(request_uri1,'/',2),'/',1),'%3F',1);
                        select * into campo1 from app_tx where aplicacion=aux2 and tipo_tx=tipo_tx1;
                        if not found then
                                insert into app_tx values(aux2,tipo_tx1);
                        end if;
                end if;
        end if;	

--	select replace(funcionalidad::varchar,chr(39),'-') into funcionalidad1 from perfil_10k_app where tipo_tx=tipo_tx1 and perfil=rol1;
	if not found then
		--Verificamos si el usuario tiene la tienda habilitada
		select replace(funcionalidad::varchar,chr(39),'-') into funcionalidad1 from perfil_10k_app where tipo_tx=tipo_tx1 and perfil=(select perfil from menu_10k where rut_usuario=stSesion.rut_usuario and aplicacion='MARKETPLACE' limit 1);
		if not found then
			json2:=logjson(json2,'El perfil '||rol1||' no puede ejecutar '||tipo_tx1||' en la tabla perfil_10k_app');
			return response_requests_6000('666','Transaccion no habilitada para su perfil','',json2);
		end if;
	end if;

	--Si tiene funcionalidades
	if funcionalidad1 is null then
		json2:=put_json(json2,'FUNCIONALIDAD','');
		json2:=logjson(json2,'FUNCIONALIDADES= Sin Funcionalidades Tx='||tipo_tx1||' Rol='||rol1);
	else
		json2:=put_json(json2,'FUNCIONALIDAD',funcionalidad1);
		json2:=logjson(json2,'FUNCIONALIDADES=' || funcionalidad1||'  Tx='||tipo_tx1||' Rol='||rol1);
	end if;
   	
	--Para todas las tx que no sean escritorio, el rut cliente es el que tengo en la sesion
	--No el que viene en el json
   	if (tipo_tx1 not in ('escritorio','escritorio_qa')) then
		json2:=put_json(json2,'rutCliente',stSesion.id_empresa);
		json2:=put_json(json2,'aplicacion',stSesion.aplicacion);
		rut_empresa1:=stSesion.id_empresa;
		--Para otras tx el rol siempre es el que tiene la sesion
		json2:=put_json(json2,'rol_usuario',stSesion.rol);
	
		--Se lee el maestro de clientes segun la aplicaion entrante
	        json2:=lee_maestro_clientes_multiapp(rut_empresa1::integer,stSesion.aplicacion,json2);
        	if (get_json('__exit__',json2)='SI') then
                	return json2;
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
	if (get_json('estado_maestro',json2) in ('BLOQUEADO','BLOQUEADO_QBIS') and stSesion.flag_super_user='NO') then
		json2:=logjson(json2,'Empresa Bloqueada en maestro_clientes- '||stSesion.rut_usuario||' y Empresa '||rut_empresa1);
		return response_requests_6000('666','Empresa Bloqueada, llame a Acepta','',json2);
	end if;

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
	json2:=put_json(json2,'host_canal',stSesion.host_canal);
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

    --Si es un flujo
    if (get_json('LLAMA_FLUJO',json2)='SI') then
        json2:=logjson(json2,'Ejecuta Flujo Secuencia='||get_json('__SECUENCIAOK__',json2));
        return json2;
    end if;

    --Si algo va mal con este query, el procesador enviara a la secuencia timeout
   json2:=put_json(json2,'__SECUENCIA_TIMEOUT__','15');

    return json2;

END;
$function$
