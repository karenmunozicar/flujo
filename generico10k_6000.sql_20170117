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

    tipo_tx1:=get_json('tipo_tx',json2);
    --json2:=logjson(json2,'JSON INPUT='||chr(10)||log_json_ident_c(json2));
    --json2:=logjson(json2,'JSON INPUT='||chr(10)||json2);

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
    json2:=logjson(json2,'Ejecuta tipo_tx='||tipo_tx1||' Sesion='||sesion1);

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
	json2:=put_json(json2,'sesion_manager',stSesion.sesion_manager);

	if (modo_qa1='ON') then
		rol1:=split_part(stSesion.rol,'_',1);
	else
		rol1:=stSesion.rol;
	end if;


	--2015-12-07 DAO  Se modifica la forma de verificar los tipo_tx de manera de hacer 1 insert en vez de 3 cada vez que se crea una nueva appDinamica
	app_dinamica1:=get_json('app_dinamica',json2);
	
	if (stSec.categoria not in ('FRAMEWORK','WEBSERVICE')) then

		app1:=get_json('APP',json2);
		if(strpos(app1,'###')>0) then
			app1:=split_part(app1,'###',1);
			tab1:=split_part(app1,'###',2);
		end if;
		if(app1='') then
			json2:=logjson(json2,'ERROR_6000 Aplicacion no definida tipo_tx='||tipo_tx1||' APP='||app1||' no viene en app_dinamica');
			return response_requests_6000('666','Transaccion no habilitada para su perfil','',json2);
		end if;

                --Validamos la transaccion que pertenezca a la aplicacion, sobre el catalogo de aplicaciones
                --select * into campo2 from menu_info_10k where id2=app_dinamica1 and txs@@get_json('tipo_tx',json2)::tsquery;
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
				

		if (get_json('rutCliente',json2)='') then
			json2:=put_json(json2,'rutCliente',stSesion.id_empresa);
		end if;
		if (get_json('aplicacion',json2)='') then
			json2:=put_json(json2,'aplicacion',stSesion.aplicacion);
		end if;
		grupo_app1:=get_json('aplicacion',json2);

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
                	        json2:=logjson(json2,'ERROR_6000 Menu no encontrado para la sesion '||sesion1);
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
                if (get_json('STATUS',json_menu1)<>'OK') then
			--Busco la app en el menu lateral
                        json_menu1:=aplicaciones_menu(menu_lateral1,app1);
                        if (get_json('STATUS',json_menu1)<>'OK') then
		                json2:=logjson(json2,'ERROR_6000 Aplicacion no permitida para la sesion '||coalesce(sesion1,'')||' APP='||coalesce(app1,'')||' tipo_tx='||tipo_tx1::varchar||' MENU_PADRE='||menu_recursivo1||' LATERAL='||menu_lateral1);
				return response_requests_6000('666','Transaccion no habilitada para su perfil','',json2);
			end if;
		end if; 
		funcionalidad1:=get_json('FUNCIONALIDAD',json_menu1);
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
