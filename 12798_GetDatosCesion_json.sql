--Publica documento
delete from isys_querys_tx where llave='12798';
--Obtiene el DTE Original con la entrada URI_IN
insert into isys_querys_tx values ('12798',5,1,8,'GET XML desde Almacen',12705,0,0,1,1,10,10);

insert into isys_querys_tx values ('12798',10,1,1,'select get_datos_cesion_12798(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

CREATE or replace FUNCTION get_datos_cesion_12798(json) RETURNS json AS $$
DECLARE
    json1        alias for $1;
        json2   json;
	data1	varchar;
	salida varchar;
	aux1	varchar;
	select1	varchar;
	rut1	varchar;
	fecha1	varchar;
	nombre_cedente1	varchar;
	monto1	varchar;
BEGIN
    json2:=json1;
    json2:=put_json(json2,'__SECUENCIAOK__','0');
    --json2:=decode(get_json('JSON_IN',json2),'hex')::varchar;
    --Verifico si viene correctamete el DTE
    if (get_json('FALLA_CUSTODIUM',json2)='SI') then
	json2:=logjson(json2,'GETDATOS_CESION: DTE no leido desde almacen URI='||get_json('URI_IN',json2));
	json2:=response_requests_6000('2', 'Falla Lectura DTE del Almacen','', json2);
	return json2;
    end if;
	
   --Rescato XML y otros datos
   data1 := decode(get_json('XML_ALMACEN',json2), 'hex');
   rut1:=get_json('rutUsuario',json2);
   --Contesto los datos 
   nombre_cedente1:=(select nombre from user_10k where rut_usuario=rut1);
   fecha1:=get_xml('FchVenc',data1);
   if (length(fecha1)=0) then
	fecha1:='NO';
   else
	fecha1:='SI';
   end if;
	monto1:=get_xml('MntTotal',data1);
   select array_to_json(array_agg(row_to_json(sql))) from (select nombre_cedente1 as nombre_cedente, fecha1 as fecha_vencimiento,monto1 as monto_total ) sql into select1;
   json2:=response_requests_6000('1', 'OK', select1, json2);

   return json2;
END;
$$ LANGUAGE plpgsql;

