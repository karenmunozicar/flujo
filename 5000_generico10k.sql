delete from isys_querys_tx where llave='5000';

insert into isys_querys_tx values ('5000',10,9,1,'select generico10k_5000(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

--Llama al nuevo flujo
insert into isys_querys_tx values ('5000',15,1,8,'FLujo 6000 json',6000,0,0,1,1,0,0);

--Llamada a Script Generico
insert into isys_querys_tx values ('5000',20,1,10,'$$SCRIPT$$',0,0,0,1,1,30,30);

--Procesa la respuesta de los scripts
insert into isys_querys_tx values ('5000',30,9,1,'select generico10k_resp_5000(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

--Graba Archivo en el EDTE NAR
--insert into isys_querys_tx values ('5000',50,1,8,'Llamada NAR EDTE',12779,0,0,0,0,0,0);
--Llama flujo ARM
insert into isys_querys_tx values ('5000',60,1,8,'Llamada ARM',12762,0,0,0,0,0,0);
--Llama flujo NAR
insert into isys_querys_tx values ('5000',70,1,8,'Llamada NAR',12765,0,0,0,0,0,0);
--Llamada CESION
insert into isys_querys_tx values ('5000',80,1,8,'Llamada CESION',12785,0,0,0,0,0,0);
--Llamada Get Datos CESION
insert into isys_querys_tx values ('5000',90,1,8,'Llamada CESION',12787,0,0,0,0,0,0);

--Llamada al reenvio de mandato
insert into isys_querys_tx values ('5000',100,1,8,'LLAMADA AL FLUJO 12763 Reenvio Mandato',12763,0,0,1,1,0,0);
--Llama al reenvio por intercambio
insert into isys_querys_tx values ('5000',110,1,8,'LLAMADA AL FLUJO 12764 Reenvio Intercambio',12764,0,0,1,1,0,0);

CREATE or replace FUNCTION generico10k_5000(varchar) RETURNS varchar AS $$
DECLARE
    xml1                alias for $1;
    xml2                varchar;
    data1               varchar;
    file1               varchar;
    sts                 integer;
    header1             varchar;
    url1                varchar;
    host1               varchar;
    rut_emisor1         varchar;
    query1              varchar;
    resp_xml1           varchar;
    tipo_tx1            varchar;
    exists_select1      varchar;
    estado_select1      varchar;
    tipo_resp1          varchar;
    estado1             varchar;
    input1              varchar;
    json1               varchar;
    respuesta1		varchar;
    stSec               define_secuencia_generico10k%ROWTYPE;
    file_wsdl1  varchar;
	sesion1	varchar;
	stSesion	sesion_web_10k%ROWTYPE;
	rut_empresa1	varchar;
	stMenu		menu_10k%ROWTYPE;
	rec1		record;
	json_aux1	json;	
	json3		json;
BEGIN
    xml2:=xml1;
    --xml2:=get_parametros(xml2);
    xml2:=put_campo(xml2,'__FLUJO_ENTRADA__','5000');
    xml2:=put_campo(xml2,'__SECUENCIAOK__','0');
    --xml2:=logapp(xml2,'XML21= '||xml2);  

    input1:=decode(get_campo('INPUT',xml2),'hex');
    --xml2:=logapp(xml2,'INPUT= '||input1);

    xml2:=logapp(xml2,'REMOTE_1ADDR='||get_campo('REMOTE_ADDR',xml2));

    --Valido que sea un json valido
    BEGIN
	json_aux1:=input1::json;
    EXCEPTION WHEN OTHERS THEN
	xml2:=logapp(xml2,'Data Entrante no es un json '||input1||' xml2='||xml2);
	input1:=json_put(input1,'CODIGO_RESPUESTA','666');
	input1:=json_put(input1,'MENSAJE_RESPUESTA','Usuario o clave invalida');
	json1:='Status: 200' ||chr(10)||'Content-type: json'||chr(10)||'Content-Length: '||octet_length(input1)::varchar||chr(10)||chr(10)||input1;
        xml2:=put_campo(xml2,'RESPUESTA_HEX',encode(json1::bytea,'hex')::varchar);
	insert into log_10k_json_invalido values (now(),get_campo('REMOTE_ADDR',xml2),xml2);
	return xml2;
    end;

    --xml2:=logapp(xml2,'JSON= '||input1);
    --xml2:=json_to_upper_xml(input1,xml2);
    --xml2:=logapp(xml2,'XML21= '||xml2);

    tipo_tx1:=json_get('tipo_tx',input1);
    --xml2:=logapp(xml2,'TX='||tipo_tx1||' Procesos Activos='||get_campo('__PROC_ACTIVOS__',xml2));
    --xml2:=logapp(xml2,'JSON INPUT='||chr(10)||log_json_ident_c(input1));

    select * into stSec from define_secuencia_generico10k where tipo_tx=tipo_tx1;
    if not found then
	json3:='{}'::json;
	json3:=put_json(json3,'CODIGO_RESPUESTA','2');
	json3:=put_json(json3,'MENSAJE_RESPUESTA','Servicio '||tipo_tx1||' No Habilitado');
--	input1:=json_put(input1,'CODIGO_RESPUESTA','2');
--	input1:=json_put(input1,'MENSAJE_RESPUESTA','Servicio '||tipo_tx1||'No Habilitado');
	json3:=response_requests_6000('2','Servicio '||tipo_tx1||' No Habilitado','',json3);
	--json1:='Status: 200' ||chr(10)||'Content-type: json'||chr(10)||'Content-Length: '||octet_length(json3::varchar)::varchar||chr(10)||chr(10)||json3::varchar;
        xml2:=put_campo(xml2,'RESPUESTA',get_json('RESPUESTA',json3));
        xml2:=logapp(xml2,'Servicio '||tipo_tx1||' No Habilitado');
        return xml2;
    end if;
    --Si la funcion esta preparada para json, entonces ejecuto solo json
    if (stSec.tipo_data='JSON') then
                xml2:=put_campo(xml2,'__SECUENCIAOK__','15');
                xml2:=logapp(xml2,'Funcion con soporte Json');
                return xml2;
    end if;





   --Toda tx debe validar la sesion excepto el login
   if (tipo_tx1 not in ('logIn','changePassSerieCI')) then
	--Obtengo la sesion
	sesion1:=json_get('session_id',input1);
	if length(sesion1)=0 then
		xml2:=logapp(xml2,'Sesion Invalida');
		return response_requests_5000('666','Sesion Invalida','',xml2,input1);
	end if;
	--Busco la sesion en la tabla de sesiones
	select * into stSesion from sesion_web_10k where sesion=sesion1;
	if not found then
		xml2:=logapp(xml2,'Sesion '||sesion1||' No encontrada');
		return response_requests_5000('666','Se ha abierto otra sesion con su usuario, la aplicacion se cerrara','',xml2,input1);
	end if;
	
	--Validamos el rut de la empresa
	rut_empresa1:=json_get('rutCliente',input1);
	
	--No validamos para los super usuarios de Acepta y Canales
	if (stSesion.flag_super_user='NO') then
		select * into stMenu from menu_10k where rut_usuario=stSesion.rut_usuario and empresa=rut_empresa1;
		if not found then
			xml2:=logapp(xml2,'No existe relacion entre '||stSesion.rut_usuario||' y Empresa '||rut_empresa1);
			return response_requests_5000('2','Usuario no tiene permisos','',xml2,input1);
		end if;
	end if;
	input1:=json_put(input1,'rutUsuario',stSesion.rut_usuario);
	input1:=json_put(input1,'rut_firma',stSesion.rut_usuario);
	--Guardo el rutCliente anterior, por si se esta cambiando
	input1:=json_put(input1,'rutClienteAnterior',stSesion.id_empresa);
--	input1:=json_put(input1,'ESCRITORIO',stSesion.json_menu);
   end if;


    xml2:=logapp(xml2,'JSON INPUT='||chr(10)||log_json_ident_c(input1));

    xml2:=put_campo(xml2,'FUNCION_RESPUESTA',stSec.funcion_output);
     xml2:=put_campo(xml2,'FLAG_JSON',stSec.flag_json);  

    --EJECUTA FUNCION INPUT
    xml2:=logapp(xml2,'Ejecuta='||stSec.funcion_input);
    if length(stSec.funcion_input)>0 then
        EXECUTE 'SELECT ' || stSec.funcion_input || '(' || chr(39) || xml2 || chr(39) || ', ' || chr(39) || input1 || chr(39) || ')' into xml2;
    end if;

    --Si necesita llamar un scrip lo ejecuta
    if (get_campo('LLAMA_SCRIPT',xml2)='SI') then
	xml2:=logapp(xml2,'Ejecuta Shell='||get_campo('SCRIPT',xml2));
        xml2:=put_campo(xml2,'__SECUENCIAOK__','20');
    end if;

    --Si es un flujo
    if (get_campo('LLAMA_FLUJO',xml2)='SI') then
        xml2:=logapp(xml2,'Ejecuta Flujo Secuencia='||get_campo('__SECUENCIAOK__',xml2));
        return xml2;
    end if;


    return xml2;

END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION generico10k_resp_5000(varchar) RETURNS varchar AS $$
DECLARE
    xml1                alias for $1;
    xml2        varchar;
    funcion1    varchar;
begin
    --EJECUTA FUNCION output
    xml2:=xml1;
    xml2:=put_campo(xml2,'__SECUENCIAOK__','0');
    funcion1:=get_campo('FUNCION_RESPUESTA',xml2);
    xml2:=logapp(xml2,'Ejecuta Respuesta='||funcion1);
    if length(funcion1)>0 then
        EXECUTE 'SELECT ' || funcion1 || '(' || chr(39) || xml2 || chr(39) || ',' || chr(39) || decode(get_campo('INPUT',xml2),'hex') || chr(39) ||')' into xml2;
    end if;
    return xml2;
END;
$$ LANGUAGE plpgsql;

