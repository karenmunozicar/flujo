delete from isys_querys_tx where llave='6000';

--Para hacer log de todo
--SOLO LOG
insert into isys_querys_tx values ('6000',5,9,16,'LOG_JSON',0,0,0,1,1,10,10);
--insert into isys_querys_tx values ('6000',5,9,1,'select log_generico10k_6000(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,10);

insert into isys_querys_tx values ('6000',10,9,1,'/*$$__JSONCOMPLETO__["tipo_tx","app_dinamica"]$$*/ select generico10k_6000(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,15);
--Para Test
insert into isys_querys_tx values ('6000',12,9,1,'select generico10k_6000_test(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,15);
insert into isys_querys_tx values ('6000',13,1,3,'Llamada a 6002 de Test',6002,0,0,0,0,0,0);

--Se registra y se contesta el timeout
insert into isys_querys_tx values ('6000',15,19,1,'select secuencia_timeout_6000(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,0,0);


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
-- Flujo 6000 --ICAR - Nuevos
insert into isys_querys_tx values ('6000',180,1,1,'select icar_spie_pivote_6000(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('6000',181,1,1,'select icar_enviar_decision_6000(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,30,0);
insert into isys_querys_tx values ('6000',307,1,10,'$$SCRIPT_NAME$$',0,0,0,1,1,300,300);
--Send Mail
insert into isys_querys_tx values ('6000',501,30,1,'select $$FUNCION_INPUT$$(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);
--Para el dashboardv2
insert into isys_querys_tx values ('6000',14711,1,8,'Llama flujo 14710',14710,0,0,0,0,30,30);
insert into isys_querys_tx values ('6000',14715,1,8,'Llama Flujo 14715',14715,0,0,1,1,0,0);
insert into isys_querys_tx values ('6000',14730,1,8,'Llama Flujo 14730',14730,0,0,1,1,0,0);
insert into isys_querys_tx values ('6000',14810,1,8,'Llama Flujo 14810',14810,0,0,1,1,0,0);
insert into isys_querys_tx values ('6000',14720,1,8,'Llama Flujo 14720',14720,0,0,1,1,0,0);

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
	json1                	alias for $1;
	json2                	json;
	data1               	varchar;
	file1               	varchar;
	sts                 	integer;
	header1             	varchar;
	url1                	varchar;
	host1               	varchar;
	rut_emisor1         	varchar;
	query1              	varchar;
	resp_json1           	varchar;
	tipo_tx1            	varchar;
	exists_select1      	varchar;
	estado_select1      	varchar;
	tipo_resp1          	varchar;
	estado1             	varchar;
	respuesta1		varchar;
	stSec               	define_secuencia_generico10k%ROWTYPE;
	file_wsdl1		varchar;
	sesion1			varchar;
	stSesion		sesion_web_10k%ROWTYPE;
	rut_empresa1		varchar;
	stMenu			menu_10k%ROWTYPE;
	--stMaster	maestro_clientes%ROWTYPE;
	--stPerfilApp	perfil_10k_aplicacion%ROWTYPE;
	modo_qa1		varchar;	
	rol1			varchar;
	funcionalidad1		varchar;
	aux1			varchar;
	request_uri1    	varchar;
        aux2            	varchar;
        campo1  		record;
	campo2			record;
	json_menu1      	json;
	app1			varchar;
	tab1			varchar;
	json3			json;
	flag_actualiza_sesion1 	boolean;
	grupo_app1		varchar;
	campo			record;
	menu_recursivo1		varchar;
	menu_lateral1		varchar;
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
		json2:=logjson(json2,'Sesion '||sesion1||' No encontrada');
		return response_requests_6000('666','Su sesión ha expirado, la aplicacion se cerrara','',json2);
		--return response_requests_6000('666','Se ha abierto otra sesion con su usuario, la aplicacion se cerrara','',json2);
	end if;
	--json2=logjson(json2,'stSesion: '||stSesion::varchar);
	update sesion_web_10k set fecha_ultimo_acceso=now() where sesion=sesion1 and now()-coalesce(fecha_ultimo_acceso,'2000-01-01'::timestamp)>interval '5 minutes';
	rol1:=stSesion.rol;
	if (get_json('rutCliente',json2)='' or is_number(replace(split_part(get_json('rutCliente',json2),'-',1),'.','')) is false) then
		json2:=put_json(json2,'rutCliente',stSesion.id_empresa);
	end if;

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
	 
			--json2:=logjson(json2,'DAONOT6000 Validamos Cambio de App o Rut Cliente para sesion '||sesion1||' json3='||json3::varchar);
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
                if (get_json('STATUS',json_menu1)<>'OK') then
			--Busco la app en el menu lateral
			json_menu1:=aplicaciones_menu(menu_lateral1,app1,json2);
                        if (get_json('STATUS',json_menu1)<>'OK') then
		                json2:=logjson(json2,'ERROR_6000 Aplicacion no permitida para la sesion '||coalesce(sesion1,'')||' APP='||coalesce(app1,'')||' tipo_tx='||tipo_tx1::varchar||' MENU_PADRE='||menu_recursivo1||' LATERAL='||menu_lateral1);
				return response_requests_6000('666','Transaccion no habilitada para su perfil','',json2);
			end if;
		end if; 
		BEGIN
			funcionalidad1:=get_json('FUNCIONALIDAD',json_menu1)||' '||get_json('FUNCIONALIDADES_MAESTRO',json2)||' '||get_json('funcionalidades_persona',get_json_index(stSesion.json_menu,0)::json);
		EXCEPTION WHEN OTHERS THEN
			json2:=logjson(json2,'Falla cargar funcionalidades_persona '||coalesce(stSesion.json_menu::varchar,'NULO'));
			funcionalidad1:=get_json('FUNCIONALIDAD',json_menu1)||' '||get_json('FUNCIONALIDADES_MAESTRO',json2);
		END;
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
	json2:=logjson(json2,'rutUsuario Sesion='||get_json('rutUsuario',json2));
	json2:=logjson(json2,'rut_firma Sesion='||get_json('rut_firma',json2));
   else
	--Si es una tx login u otra que no requiere sesion
	aux1:=get_json('SERVER_NAME_ORI',json2);
	if (aux1='') then
		json2:=put_json(json2,'host_canal','escritorio.icar.com');
	else
		json2:=put_json(json2,'host_canal',aux1);
	end if;
   end if;
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
	--Base Send Mail
	if (stSec.base_datos='BASE_SEND_MAIL') then
		json2:=logjson(json2,'Ejecuta Funcion '||stSec.funcion_input||' tipo_tx='||tipo_tx1||' en BASE BASE_FINANCIAMIENTO');
		json2:=put_json(json2,'__SECUENCIAOK__','501');
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
	json1		alias for $1;
	json2		json;
	funcion1	varchar;
begin
	--EJECUTA FUNCION output
	BEGIN
		json2:=json1::json;
	EXCEPTION WHEN OTHERS THEN
		insert into log_timeout_motor_6000 (fecha,tipo_tx,data_mala) values (now(),'NO JSON',json1);
		return response_requests_6000('2','Reintente Por favor.','',json2);
	END;
	json2=logjson(json2,'sale por timeout tipo_tx: '||get_json('tipo_tx',json2));
	json2:=put_json(json2,'__SECUENCIAOK__','0');
	--Registramos el timeout
	insert into log_timeout_motor_6000 values (now(),get_json('tipo_tx',json2),get_json('rutUsuario',json2),get_json('rutCliente',json2),json1::json);
	return response_requests_6000('2','Reintente Por favor..','',json2);
END;
$$ LANGUAGE plpgsql;
