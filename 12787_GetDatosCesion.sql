--Publica documento
delete from isys_querys_tx where llave='12787';
--Obtiene el DTE Original con la entrada URI_IN
insert into isys_querys_tx values ('12787',5,1,8,'GET XML desde Almacen',12705,0,0,1,1,10,10);

insert into isys_querys_tx values ('12787',10,1,1,'select get_datos_cesion_12787(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

CREATE or replace FUNCTION get_datos_cesion_12787(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
        xml2    varchar;
	data1	varchar;
	salida varchar;
	json2	varchar;
	aux1	varchar;
	select1	varchar;
	rut1	varchar;
	fecha1	varchar;
	nombre_cedente1	varchar;
	monto1	varchar;
BEGIN
    xml2:=xml1;
    xml2:=put_campo(xml2,'__SECUENCIAOK__','0');
    json2:=decode(get_campo('JSON_IN',xml2),'hex')::varchar;
    --Verifico si viene correctamete el DTE
    if (get_campo('FALLA_CUSTODIUM',xml2)='SI') then
	xml2:=logapp(xml2,'GETDATOS_CESION: DTE no leido desde almacen URI='||get_campo('URI_IN',xml2));
	xml2:=response_requests_5000('2', 'Falla Lectura DTE del Almacen','', xml2,json2);
	return xml2;
    end if;
	
   --Rescato XML y otros datos
   data1 := decode(get_campo('XML_ALMACEN',xml2), 'hex');
   rut1:=json_get('rutUsuario',json2);
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
   xml2:=response_requests_5000('1', 'OK', select1, xml2, json2);

   return xml2;
END;
$$ LANGUAGE plpgsql;

