delete from isys_querys_tx where llave='12800';

-- Prepara llamada al AML
insert into isys_querys_tx values ('12800',10,1,1,'select proc_reprocesa_dte_12800(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

--Ejecute Flujos
insert into isys_querys_tx values ('12800',20,1,8,'Flujo 8010 DTE',8010,0,0,1,1,100,100);
insert into isys_querys_tx values ('12800',30,1,8,'Flujo 12703 DTE',12703,0,0,1,1,100,100);

insert into isys_querys_tx values ('12800',100,1,1,'select proc_respuesta_reproceso_128000(''$$__XMLCOMPLETO__$$'') as __XML_NUEVO__',0,0,0,1,1,-1,0);

CREATE or replace FUNCTION proc_reprocesa_dte_12800(varchar) RETURNS varchar AS $$
DECLARE
    xml1	alias for $1;
    stFactura	dte_reproceso%ROWTYPE;
    xml2	varchar;
    data1	varchar;
    file1	varchar;
    sts		integer;
    header1	varchar;
    url1	varchar;
    host1	varchar;
    xml_in	varchar;
    id1	varchar;
BEGIN
    xml2:=xml1;

    id1:=get_campo('ID_REPROCESO',xml2);

    --Saco datos de la tabla  para reprocesar
    select * into stFactura from dte_reproceso where id=id1::bigint;
    if not found then
    	xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
	xml2 := logapp(xml2,'No hay facturas pendientes de Reprocesar');
	return xml2;
    end if;

    xml_in:='';
	
    --Para el control de las NC
    xml_in:=put_campo(xml_in,'FECHA_INGRESO_COLA',stFactura.fecha_ingreso::varchar);
    --Genero el xml entrante para la transaccion que enviare..    
    xml_in:=put_campo(xml_in,'CODIGO_TXEL_REPROCESO',get_campo('CODIGO_TXEL',stFactura.xml_in));
    xml_in:=put_campo(xml_in,'SERVER',get_campo('SERVER',stFactura.xml_in));
    xml_in:=put_campo(xml_in,'FCGI_ROLE',get_campo('FCGI_ROLE',stFactura.xml_in));
    xml_in:=put_campo(xml_in,'SCRIPT_URL',get_campo('SCRIPT_URL',stFactura.xml_in));
    xml_in:=put_campo(xml_in,'SCRIPT_URI',get_campo('SCRIPT_URI',stFactura.xml_in));
    xml_in:=put_campo(xml_in,'HTTP_HOST',get_campo('HTTP_HOST',stFactura.xml_in));
    xml_in:=put_campo(xml_in,'CONTENT_TYPE',get_campo('CONTENT_TYPE',stFactura.xml_in));
    xml_in:=put_campo(xml_in,'SERVER_NAME',get_campo('SERVER_NAME',stFactura.xml_in));
    xml_in:=put_campo(xml_in,'SERVER_ADDR',get_campo('SERVER_ADDR',stFactura.xml_in));
    xml_in:=put_campo(xml_in,'REQUEST_URI',get_campo('REQUEST_URI',stFactura.xml_in));
    xml_in:=put_campo(xml_in,'SCRIPT_NAME',get_campo('SCRIPT_NAME',stFactura.xml_in));
    xml_in:=put_campo(xml_in,'CONTENT_LENGTH',get_campo('CONTENT_LENGTH',stFactura.xml_in));
    xml_in:=put_campo(xml_in,'SERVER_ADMIN',get_campo('SERVER_ADMIN',stFactura.xml_in));
    xml_in:=put_campo(xml_in,'SERVER_SOFTWARE',get_campo('SERVER_SOFTWARE',stFactura.xml_in));
    xml_in:=put_campo(xml_in,'SERVER_PROTOCOL',get_campo('SERVER_PROTOCOL',stFactura.xml_in));
    xml_in:=put_campo(xml_in,'SERVER_PORT',get_campo('SERVER_PORT',stFactura.xml_in));
    xml_in:=put_campo(xml_in,'REMOTE_ADDR',get_campo('REMOTE_ADDR',stFactura.xml_in));
    xml_in:=put_campo(xml_in,'REMOTE_PORT',get_campo('REMOTE_PORT',stFactura.xml_in));
    xml_in:=put_campo(xml_in,'ID_DTE_ERROR_REPROCESO',get_campo('ID_DTE_ERROR',stFactura.xml_in));
    xml_in:=put_campo(xml_in,'URI_IN',get_campo('URI_IN',stFactura.xml_in));

	


    --FAY-RME 2015-10-20
    --Estos DTE no se encuentran en dte_emitidos y cuando se graben, se haran con la fecha actual
    --Por eso no seteamos la FECHA_PUBLICACION, porque se setea en parseo_datos.
    --La fecha de publicacion es la fecha cuando se guarda en dte_emitidos para tener consistencia con
    --las estadisticas.
/*
    --Para que se manbtenga la fecha de publicacion original
    if (get_campo('FECHA_PUBLICACION',stFactura.xml_in)='') then
    	xml_in:=put_campo(xml_in,'FECHA_PUBLICACION',to_char(now(),'YYYYMMDD')::varchar);
    else
    	xml_in:=put_campo(xml_in,'FECHA_PUBLICACION',get_campo('FECHA_PUBLICACION',stFactura.xml_in));
    end if;
*/
    xml_in:=put_campo(xml_in,'QUERY_STRING',get_campo('QUERY_STRING',stFactura.xml_in)); 
    	xml_in:=put_campo(xml_in,'TX','8010');
        xml_in:= put_campo(xml_in,'__SECUENCIAOK__','20');

/*
    if (get_campo('CANAL',stFactura.xml_in)='EMITIDOS') then
    	xml_in:=put_campo(xml_in,'TX','8010');
        xml_in:= put_campo(xml_in,'__SECUENCIAOK__','20');
    else
    	xml_in:=put_campo(xml_in,'TX','12703');
        xml_in:= put_campo(xml_in,'__SECUENCIAOK__','30');
    end if;
*/
    xml_in:=put_campo(xml_in,'_REPROCESO_','SI');
    xml_in:=put_campo(xml_in,'__ID_DTE__','-1');
    xml_in:=put_campo(xml_in,'__COLA_MOTOR__','cola_motor_1');
    xml_in:=put_campo(xml_in,'_REPROCESO_VECES_',coalesce(stFactura.veces,0)::varchar);
    xml_in:=put_campo(xml_in,'_ID_REPROCESO_',stFactura.id::varchar);
    xml_in:=put_campo(xml_in,'INPUT',get_campo('INPUT',stFactura.xml_in));
    xml_in:= logapp(xml_in,'Se envia Factura a Reproceso ID='||stFactura.id::varchar);
    return xml_in;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION proc_respuesta_reproceso_128000(varchar) RETURNS varchar AS $$
DECLARE
    xml1	alias for $1;
    xml2	varchar;
    data1	varchar;
    respuesta1	varchar;
	id1	bigint;
	codigo1	bigint;
	id_error1	bigint;
	aux1	varchar;
    stError dte_emitidos_errores%ROWTYPE;
BEGIN
    xml2:=xml1;
    id1:=get_campo('_ID_REPROCESO_',xml1)::bigint; 
    aux1:=get_campo('ID_DTE_ERROR_REPROCESO',xml1);
    if (is_number(aux1)) then 
    	id_error1:=aux1::bigint;
    else
    	id_error1:=0;
    end if;
		
    --Solo borro el registro si fue aprobado el reproceso
    if (get_campo('_ESTADO_REPROCESO_',xml2)='OK') then
	xml2:=logapp(xml2,'ID_DTE_ERROR='||id_error1::varchar);
	--Se borra del re-proceso
	--Grabamos la bitacora del documento
	xml2:=put_campo(xml2,'COMENTARIO_TRAZA','Documento Reprocesado');
	xml2:=graba_bitacora(xml2,'REPROCESO_OK');
	delete from dte_reproceso where id=id1::bigint;
	xml2=logapp(xml2,'Re-proceso OK');

	--Se deben bajar las estadisticas y borrar de la tabla de errores
    	codigo1:=get_campo('CODIGO_TXEL_REPROCESO',xml2)::bigint;
	--Las estadisticas solo para los EMITIDOS
   	if (get_campo('CANAL',xml2)='EMITIDOS') then
	   if (id_error1>0) then
		xml2=logapp(xml2,'Busqueda por id_error');
		select * into stError from dte_emitidos_errores where id=id_error1;
		if found then
			--if (stError.estado='DTE_EN_ESPERA') then
			--	xml2:=actualiza_indexer(xml2,'YEE','RESTA');
			--elsif (stError.estado='NOTA_CREDITO_ESPERA_REFERENCIA') then
			--	xml2:=actualiza_indexer(xml2,'YWR','RESTA');
			--end if;
			delete from dte_emitidos_errores where id=id_error1;
		else
			xml2:=logapp(xml2,'Caso Raro');
		end if;
	   else
		xml2=logapp(xml2,'Busqueda por codigo_txel');
		--Solo borrra el DTE que esta en estado en Espera..
		delete from dte_emitidos_errores where codigo_txel=codigo1 and estado='DTE_EN_ESPERA';
		--if found then
			--xml2:=actualiza_indexer(xml2,'YEE','RESTA');
		--end if;
	   end if;
	end if;
    else
    	--FAY 2017-03-20 si un DTE lleva 3 meses en la cola de reproceso, se envia al historico
        if (now()-get_campo('FECHA_INGRESO_COLA',xml2)::timestamp>interval '3 month') then
		--Se pasa al historico
	    	xml2:= logapp(xml2,'Se envia Factura a historico Reproceso ID='||id1::varchar);
		insert into dte_reproceso_historico  select * from dte_reproceso where id=id1::bigint;
		if found then
			--xml2:=put_campo(xml2,'COMENTARIO_TRAZA','Documento EnviaodReprocesado');
			xml2:=graba_bitacora(xml2,'PASO_HISTORICO_REPROCESO');
			delete from dte_reproceso where id=id1::bigint;
		end if;
	else
		--Se aumenta la cantidad de veces de falla
		update dte_reproceso set veces=veces+1,fecha_intento=now() where id=id1::bigint;
		xml2=logapp(xml2,'Falla Reproceso');
	end if;
    end if;
    xml2:= put_campo(xml2,'__SECUENCIAOK__','0');
    RETURN xml2;
END;
$$ LANGUAGE plpgsql;
