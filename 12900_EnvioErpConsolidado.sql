--Publica documento
delete from isys_querys_tx where llave='12900';

insert into isys_querys_tx values ('12900',10,1,1,'select proc_procesa_envio_erp_12900(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

--WS Generico por Nombre, Se envia HEX y se responde ASCII
insert into isys_querys_tx values ('12900',50,1,2,'Generico',4013,103,101,0,0,100,100);
--Para los que ncesitan respuesta en HEX
insert into isys_querys_tx values ('12900',55,1,2,'Generico',4013,103,106,0,0,100,100);
--WS COPEC
insert into isys_querys_tx values ('12900',12,1,2,'COPEC',231312,100,101,0,0,100,100);
--WS IANSA
insert into isys_querys_tx values ('12900',13,1,2,'IANSA',4012,100,101,0,0,100,100);
--WS CODELCO
insert into isys_querys_tx values ('12900',14,1,2,'IANSA',4014,100,101,0,0,100,100);
--Generico Llamada de SCRIPT
insert into isys_querys_tx values ('12900',90,1,10,'$$SCRIPT$$',0,0,0,1,1,100,100);
--Respuesta.
insert into isys_querys_tx values ('12900',100,1,1,'select proc_procesa_respuesta_erp_12900(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);


CREATE or replace FUNCTION proc_procesa_envio_erp_12900(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
        xml2    varchar;
	stDND	traza.rut_dnd%ROWTYPE;
	rut1	varchar;
	data1	varchar;

BEGIN
    xml2:=xml1;
    xml2:=logapp(xml2,'COMIENZA FLUJO ENVIO CONSOLIDADO');
    rut1:=get_campo('RUT',xml2);
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
	



   --Graba Evento en traza
   xml2 := put_campo(xml2,'FECHA_EVENTO',to_char(now(),'YYYY/MM/DD HH24:MI:SS'));
   xml2 := put_campo(xml2,'RUT_EMISOR',split_part(get_xml('RUTEmisor',data1),'-',1));
   xml2 := put_campo(xml2,'RUT_RECEPTOR',split_part(get_xml('RUTRecep',data1),'-',1));
   xml2 := put_campo(xml2,'TIPO_DTE',get_xml('TipoDTE',data1));
   xml2 := put_campo(xml2,'FOLIO',get_xml('Folio',data1));
   xml2 := put_campo(xml2,'FECHA_EMISION',get_xml('FchEmis',data1));
   xml2 := put_campo(xml2,'MONTO_NETO',get_xml('MntNeto',data1));
   --xml2 := graba_bitacora(xml2,'ERP_SEND_CLI');
  
--   xml2 := logapp(xml2,'Graba Evento ERP_SEND_CLI para URI='||get_campo('URI_IN',xml2));

   --Ejecuto la funcion inn
   xml2:=logapp(xml2,'Ejecuta '||stDND.sp_in_recibidos);
   execute 'select ' || stDND.sp_in_recibidos || '(' || quote_literal(xml2) || ')' into xml2;

   xml2 := logapp(xml2,'SIGUIENTE SECUENCIA:' || get_campo('__SECUENCIAOK__',xml2));
   return xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION proc_procesa_respuesta_erp_12900(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2    varchar;
	id1	varchar;
	funcion1	varchar;
BEGIN
    xml2:=xml1;
    --Procesamos la respuesta la respuesta
    funcion1:=get_campo('FUNCION_OUT',xml2);
    execute 'select ' || funcion1 || '(' || quote_literal(xml2) || ')' into xml2;
    xml2:=logapp(xml2,'Funcion out:' || funcion1); 
    id1:=get_campo('ID',xml2);
    --Verificamos si fue exitoso el envio
    if (get_campo('ESTADO_ENVIO_ERP',xml2)='OK') then
		--RME Se copia el registro en la tabla historica cuando se logra enviar con exito
		insert into documentos_recibidos_x_enviar_erp_historica (fecha,rut,canal,uri,id,reintentos,fec_ult_reintento,fec_envio_erp) select  fecha,rut,canal,uri,id,reintentos,fec_ult_reintento, now() from documentos_recibidos_x_enviar_erp  where id=id1::bigint;

		if not found then
			 xml2:=logapp(xml2,'No se pudo grabar Historico de envio ERPi URI='||get_campo('URI_IN',xml2));
		end if;
		--Borro el registro de la tabla 
		delete from documentos_recibidos_x_enviar_erp where id=id1::bigint;
		--Grabo en traza el exito del envio
		--Campo COMENTARIO_TRAZA viene desde el sp_out_recibidos
   		xml2 := put_campo(xml2,'FECHA_EVENTO',to_char(clock_timestamp(),'YYYY/MM/DD HH24:MI:SS'));
		xml2:=graba_bitacora(xml2,'ERP_RECV_CLI');
		xml2:=logapp(xml2,'DTE recibido OK ERP Cliente URI='||get_campo('URI_IN',xml2));
    else
		--Se actualiza el numero de reintentos y la fecha
		update documentos_recibidos_x_enviar_erp set reintentos=coalesce(reintentos,0)+1, fec_ult_reintento=now() where id=id1::bigint;
   		xml2 := put_campo(xml2,'FECHA_EVENTO',to_char(clock_timestamp(),'YYYY/MM/DD HH24:MI:SS'));
   		xml2 := put_campo(xml2,'COMENTARIO_TRAZA','Cliente no recibe DTE.');
		xml2:=graba_bitacora(xml2,'ERP_FALLA_CLI');
		xml2:=logapp(xml2,'DTE no recibido por ERP Cliente URI='||get_campo('URI_IN',xml2));
    end if;    
    
    RETURN xml2;
END;
$$ LANGUAGE plpgsql;


