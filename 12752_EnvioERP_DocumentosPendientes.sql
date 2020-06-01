--Publica documento
delete from isys_querys_tx where llave='12752';
insert into isys_querys_tx values ('12752',10,1,1,'select proc_procesa_envio_erp_12752(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
--WS Generico por Nombre, Se envia HEX y se responde ASCII
insert into isys_querys_tx values ('12752',50,1,2,'Generico',4013,103,101,0,0,100,100);
--SCRIPT
insert into isys_querys_tx values ('12752',90,1,10,'$$SCRIPT$$',0,0,0,1,1,100,100);
--Respuesta.
insert into isys_querys_tx values ('12752',100,1,1,'select proc_procesa_respuesta_erp_12752(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
-------------------------------


CREATE or replace FUNCTION proc_procesa_envio_erp_12752(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
        xml2    varchar;
	stDND	traza.rut_dnd%ROWTYPE;
	rut1	varchar;
	data1	varchar;
	--JsotoPDF
        v_marca_pdf     varchar;
        v_ejecuta_in    varchar;
        v_uri_modificada varchar;
BEGIN
    xml2:=xml1;
    xml2:=logapp(xml2,'COMIENZA FLUJO ENVIO NO RECIBIDOS PENDIENTES');
    --Verifico si viene correctamete el DTE
    rut1:=get_campo('RUT_RECEPTOR',xml2);
    if (is_number(rut1) is false) then
	xml2:=logapp(xml2,'RUT no numerico RUT='||rut1);
	return xml2;
    end if;

    --Leo procedimiento de entrada y salida desde rut_dnd
    select * into stDND from traza.rut_dnd where rut=rut1;
    if found then
	xml2:=put_campo(xml2,'FUNCION_OUT',stDND.sp_out_recibidos);
    else
   	xml2:=logapp(xml2,'Rut no definido en tabla traza.rut_dnd RUT='||rut1);
	return xml2;
    end if;
   --Si no tiene funcion no haga nada
   if (length(stDND.sp_in_recibidos)=0 or stDND.sp_in_recibidos is null) then
   	xml2:=logapp(xml2,'Funcion no definida sp_in_recibidos para RUT='||rut1);
	return xml2;
   end if;

/*	
      --Graba Evento en traza
      data1 := decode(get_campo('XML_ALMACEN',xml2), 'hex');
      xml2 := put_campo(xml2,'FECHA_EVENTO',to_char(now(),'YYYY/MM/DD HH24:MI:SS'));
      xml2 := put_campo(xml2,'RUT_EMISOR',split_part(get_xml('RUTEmisor',data1),'-',1));
      xml2 := put_campo(xml2,'RUT_RECEPTOR',split_part(get_xml('RUTRecep',data1),'-',1));
      xml2 := put_campo(xml2,'TIPO_DTE',get_xml('TipoDTE',data1));
      xml2 := put_campo(xml2,'FOLIO',get_xml('Folio',data1));
      xml2 := put_campo(xml2,'FECHA_EMISION',get_xml('FchEmis',data1));
      xml2 := put_campo(xml2,'MONTO_NETO',get_xml('MntNeto',data1));
      xml2 := graba_bitacora(xml2,'ERP_SEND_CLI');
   
      xml2 := logapp(xml2,'Graba Evento ERP_SEND_CLI para URI='||get_campo('URI_IN',xml2));

*/
    --Ejecuto la funcion inn
    xml2:=logapp(xml2,'Ejecuta '||stDND.sp_in_recibidos);
    execute 'select ' || stDND.sp_in_recibidos || '(' || quote_literal(xml2) || ')' into xml2;
--TIENE SCRIPT
   if (get_campo('__SECUENCIAOK__',xml2)='90') then
	xml2:= put_campo(xml2,'TIENE_SCRIPT','SI');
   end if;

   xml2 := logapp(xml2,'SIGUIENTE SECUENCIA:' || get_campo('__SECUENCIAOK__',xml2));
   return xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION proc_procesa_respuesta_erp_12752(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2    varchar;
	id1	varchar;
	funcion1	varchar;
	
	uri_resp		varchar;
	data_resp		varchar;
	v_respuesta		varchar;
	v_envia_script		varchar;
	v_envia_input		varchar;
	v_nombre_archivo_resp	varchar;

BEGIN
    xml2:=xml1;
    --Procesamos la respuesta la respuesta
    funcion1:=get_campo('FUNCION_OUT',xml2);
    execute 'select ' || funcion1 || '(' || quote_literal(xml2) || ')' into xml2;
    xml2:=logapp(xml2,'Funcion out:' || funcion1); 
    id1:=get_campo('ID',xml2);
    v_respuesta := get_campo('RESPUESTA_SYSTEM',xml2);
------------------------------------------------------	
	--Verificamos si fue exitoso el envio
    if (get_campo('ESTADO_ENVIO_ERP',xml2)='OK') then
		--insert into documentos_no_recibidos_pendientes_x_enviar () select  from --documentos_no_recibidos_x_enviar_erp where id=id1::bigint;
		--documentos_no_recibidos_x_enviar_erp

		if not found then
			 --xml2:=logapp(xml2,'No se pudo grabar Historico NO RECIBIDOS de envio ERP ='||get_campo('URI_IN',xml2));
		end if;
		--Borro el registro de la tabla 
		
		delete from documentos_no_recibidos_x_enviar_erp  where id=id1::bigint;
		
		--Grabo en traza el exito del envio
		--Campo COMENTARIO_TRAZA viene desde el sp_out_recibidos
   		--xml2 := put_campo(xml2,'FECHA_EVENTO',to_char(clock_timestamp(),'YYYY/MM/DD HH24:MI:SS'));
		--xml2:=graba_bitacora(xml2,'ERP_RECV_CLI');
		xml2:=logapp(xml2,'DTE recibido OK ERP Cliente ');
    else
		--Se actualiza el numero de reintentos y la fecha
		
		--update documentos_recibidos_x_enviar_erp set reintentos=coalesce(reintentos,0)+1, fec_ult_reintento=now(), data_resp=uri_resp where id=id1::bigint;
   		
		--xml2 := put_campo(xml2,'FECHA_EVENTO',to_char(clock_timestamp(),'YYYY/MM/DD HH24:MI:SS'));
   		--xml2 := put_campo(xml2,'COMENTARIO_TRAZA','Cliente no recibe DTE.');
		--xml2:=graba_bitacora(xml2,'ERP_FALLA_CLI');
		xml2:=logapp(xml2,'DTE no recibido por ERP Cliente NO RECIBIDOS');
    end if;    
    
    RETURN xml2;
END;
$$ LANGUAGE plpgsql;


