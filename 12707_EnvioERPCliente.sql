--Publica documento
delete from isys_querys_tx where llave='12707';
--Verifica si se trata de un Recibido o Pendiente de recibir
insert into isys_querys_tx values ('12707',3,1,1,'select valida_pendiente_12707(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

--Verifica si se trata de un Recibido o Pendiente de recibir
--insert into isys_querys_tx values ('12707',3,1,1,'select valida_uri_no_recibido_12707 (''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
--Obtiene el DTE Original con la entrada URI_IN
insert into isys_querys_tx values ('12707',5,1,8,'GET XML desde Almacen',12705,0,0,1,1,10,10);
--PDF ENVIO
insert into isys_querys_tx values ('12707',6,1,8,'Obtiene PDF Almacen',12714,0,0,0,0,10,10);

insert into isys_querys_tx values ('12707',10,1,1,'select proc_procesa_envio_erp_12707(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

--WS Generico por Nombre, Se envia HEX y se responde ASCII
insert into isys_querys_tx values ('12707',50,1,2,'Generico',4013,103,101,0,0,100,100);
--Para los que ncesitan respuesta en HEX
insert into isys_querys_tx values ('12707',55,1,2,'Generico',4013,103,106,0,0,100,100);
--WS COPEC
insert into isys_querys_tx values ('12707',12,1,2,'COPEC',231312,100,101,0,0,100,100);
--WS IANSA
insert into isys_querys_tx values ('12707',13,1,2,'IANSA',4012,100,101,0,0,100,100);
--WS CODELCO
insert into isys_querys_tx values ('12707',14,1,2,'IANSA',4014,100,101,0,0,100,100);
--Generico Llamada de SCRIPT
insert into isys_querys_tx values ('12707',90,1,10,'$$SCRIPT$$',0,0,0,1,1,100,100);
--Respuesta.
insert into isys_querys_tx values ('12707',100,1,1,'select proc_procesa_respuesta_erp_12707(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
-------------------------------
--JS Se cambia secuencias especiales para transelect para que pase por flujo normal.
--Obtengo el PDF del Almacen Transelec
--insert into isys_querys_tx values ('12707',60,1,8,'Obtiene PDF Almacen',12714,0,0,0,0,70,70);
--insert into isys_querys_tx values ('12707',70,1,1,'select sp_respuesta_pdf_transelec(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);


CREATE or replace FUNCTION valida_pendiente_12707(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2        varchar;
    uri1        varchar;
    flag1       varchar;
    stdoc_recibidos   dte_pendientes_recibidos%ROWTYPE;
    v_fec_recepcion_sii     varchar;
    v_fecha_ingreso         timestamp;
    tipo_dte1       integer;
BEGIN
    xml2:=xml1;

    uri1:=get_campo('URI_IN',xml2);
    flag1:=get_campo('FLAG_PENDIENTE',xml2);



    if(flag1='SI')then

        select * into stdoc_recibidos from dte_pendientes_recibidos where uri=uri1;
        if found then
		xml2 := logapp(xml2,'Encontrado en dte_pendientes_recibidos');
                xml2 := put_campo(xml2,'TIPO_DTE',stdoc_recibidos.tipo_dte::varchar);
                xml2 := put_campo(xml2,'FOLIO',stdoc_recibidos.folio::varchar);
                xml2 := put_campo(xml2,'FECHA_EMISION',stdoc_recibidos.fecha_emision);
                xml2 := put_campo(xml2,'FECHA_RECEPCION_SII',stdoc_recibidos.fecha_recepcion_sii);
                xml2 := put_campo(xml2,'RUT_EMISOR',split_part(stdoc_recibidos.rut_emisor,'-',1));
                xml2 := put_campo(xml2,'RUT_RECEPTOR',stdoc_recibidos.rut_receptor::varchar);
                xml2 := put_campo(xml2,'MONTO_TOTAL',stdoc_recibidos.monto_total::varchar);

                xml2 := put_campo(xml2,'__SECUENCIAOK__','10');
        else
                xml2 := logapp(xml2,'Dte_pendiente no encontrado se elimina registro del pendiente->'||uri1);
                delete from documentos_recibidos_x_enviar_erp where uri=uri1 and flag_pendiente='SI';
                if not found then
                         xml2 := logapp(xml2,'No se puede eliminar documentos_recibidos_x_enviar_erp uri-->'||uri1);
                end if;
                xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
        end if;
    else
	    select coalesce(fecha_recepcion_sii::varchar,'') as fecha_recepcion_sii , fecha_ingreso, tipo_dte into v_fec_recepcion_sii, v_fecha_ingreso, tipo_dte1 from dte_recibidos where uri=uri1;

    	if found then
	    if length(v_fec_recepcion_sii)=0 then
		--Si es tipo_dte sin fecha
		--RME 20171031 Se cambia validacion, para solo incluir DTE reclamables
		if tipo_dte1  not in  (33,34,43) then
	--	if tipo_dte1  in  (110,111,112,52,61) then
			v_fec_recepcion_sii:='1900-01-01';
		--Se verifica si ya pasaron 24 horas, ya no llegara la fecha	
		elsif ( now() - interval '24 hours' > v_fecha_ingreso::timestamp) then 
			v_fec_recepcion_sii:='1900-01-01';
		else
			--Espera hasta que aparezca la fecha o pasen 24 horas
			xml2:=logapp(xml2,'DTE Sin fecha en SII espera siguiente iteracion '||uri1);
			xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
		        return xml2;
		end if;
	    end if;
    	else
		xml2:=logapp(xml2,'URI no encontrada en tabla dte_recibidos '||uri1);
        	xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
	       	return xml2;
	end if;
    	xml2 := put_campo(xml2,'FECHA_RECEPCION_SII',v_fec_recepcion_sii);
        ---Si no tiene flag.
        xml2 := put_campo(xml2,'__SECUENCIAOK__','5');
    end if;

    return xml2;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION proc_procesa_envio_erp_12707(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
        xml2    varchar;
	stDND	traza.rut_dnd%ROWTYPE;
	rut1	varchar;
	data1	varchar;
	xml3	varchar;
        --JsotoPDF
        v_marca_pdf     varchar;
        v_ejecuta_in    varchar;
        v_uri_modificada varchar;
	v_rec_pendiente  varchar;
	v_fec_recepcion_sii	varchar;
	v_fecha_ingreso		timestamp;
	tipo_dte1	integer;
	uri1		varchar;

BEGIN
    xml2:=xml1;
    xml2:=logapp(xml2,'COMIENZA FLUJO ENVIO RECIBIDOS');
    v_rec_pendiente:=get_campo('FLAG_PENDIENTE',xml2);
    uri1:=get_campo('URI_IN',xml2);
	

/*
    --RME 20170830 Se valida si el dte, tiene fecha de recepcion en SII, sino sigue esperando
    if v_rec_pendiente<>'SI' then
	    select coalesce(fecha_recepcion_sii::varchar,'') as fecha_recepcion_sii , fecha_ingreso, tipo_dte into v_fec_recepcion_sii, v_fecha_ingreso, tipo_dte1 from dte_recibidos where uri=uri1;

xml2:=logapp(xml2,'fecha_ingreso '||v_fec_recepcion_sii );
	    if found then
		    if length(v_fec_recepcion_sii)=0 then
			--Si es tipo_dte sin fecha
			if tipo_dte1 not in  (110,111,112,52) then
				v_fec_recepcion_sii:='1900-01-01';
			--Se verifica si ya pasaron 24 horas, ya no llegara la fecha	
			elsif ( now() - interval '24 hours' > v_fecha_ingreso::timestamp) then 
				v_fec_recepcion_sii:='1900-01-01';
			else
				--Espera hasta que aparezca la fecha o pasen 24 horas
				xml2:=logapp(xml2,'DTE Sin fecha en SII espera siguiente iteracion '||uri1);
				xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
			        return xml2;
			end if;
		    end if;
	    else
		xml2:=logapp(xml2,'URI no encontrada en tabla dte_recibidos '||uri1);
	        xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
        	return xml2;
	    end if;
    end if;
    
    xml2 := put_campo(xml2,'FECHA_RECEPCION_SII',v_fec_recepcion_sii);
*/
    --Verifico si viene correctamete el DTE 
    if (get_campo('FALLA_CUSTODIUM',xml2) in ('SI','') and v_rec_pendiente='NO') then
	xml2:=logapp(xml2,'DTE no leido desde almacen URI='||get_campo('URI_IN',xml2));
	return xml2;
    end if;

    if (get_campo('CANAL',xml2)='E') then
	xml2:=put_campo(xml2,'CANAL','EMITIDOS');
    else
	xml2:=put_campo(xml2,'CANAL','RECIBIDOS');
    end if;
    rut1:=get_campo('RUT',xml2);
    if (is_number(rut1) is false) then
	xml2:=logapp(xml2,'RUT no numerico URI='||get_campo('URI_IN',xml2)||' RUT='||rut1);
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
	

-------------------Modificacion PDF--------------------------

   xml2 := put_campo(xml2,'LLEVA_PDF',stDND.pdf::varchar);

        if ((stDND.pdf)  and (v_rec_pendiente='NO')) then
                --Verificamos PDF.-
                if(strpos(xml2,'FALLA_PDF_CUSTODIUM')>0) then
                        if (get_campo('FALLA_PDF_CUSTODIUM',xml2)='SI') then
                                xml2:=logapp(xml2,'PDF no leido URI='||get_campo('URI_IN',xml2));
                                xml2 := put_campo(xml2,'URI_IN',get_campo('URI_ORIGINAL',xml2));
                                xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
                        else
                                --insert into jsprueba select '3';
                                xml2 := put_campo(xml2,'URI_IN',get_campo('URI_ORIGINAL',xml2));
                                v_ejecuta_in:='OK';
                        end if;
                else
                        --insert into jsprueba select 'secuencia 6';
                        --REDIRECCIONO FLUJO PDF.

                        --cambio en la secuencia se concatena xsl para pdf 
                        xml2 := put_campo(xml2,'URI_ORIGINAL',get_campo('URI_IN',xml2));
                        v_uri_modificada        := get_campo('URI_IN',xml2);
                        --v_uri_modificada        := v_uri_modificada || '&xsl=http://pruebaswindte1501.acepta.com/styles/DTE/DTE-xslfo.xsl';
                        -- se modifica la uri de la bolsa de gatos adjuntandole la hoja de estilo pdf 
                        --v_uri_modificada        := v_uri_modificada || '&xsl=http://www.custodium.com/docs/otros/dte/dte.xsl';
                        --v_uri_modificada        := v_uri_modificada || '&xsl=http://windte1509.acepta.com/styles/dte/dte_browser-xslfo.xsl';
                        --v_uri_modificada        := v_uri_modificada || '&xsl=http://carpetaweb.acepta.com/proyectos/jlabra/dte-xslfo.xsl';
                        v_uri_modificada        := v_uri_modificada || '&xsl=http://www.custodium.com/docs/otros/dte/dte.xsl';
                        xml2 := put_campo(xml2,'URI_IN',v_uri_modificada);
                        -- insert into mzmprueba select xml2;                           
                        xml2 := put_campo(xml2,'__SECUENCIAOK__','6');
                        return xml2;
                end if;
        else
                --insert into mzmprueba select '1';
                -- se revierte el cambio  a la uri por el valor original
                v_ejecuta_in:='OK';
        end if;

--------------------

  if (v_ejecuta_in='OK') then
      if (v_rec_pendiente='NO' or v_rec_pendiente='') then
           --Graba Evento en traza
	   data1 := decode(get_campo('XML_ALMACEN',xml2), 'hex');
	   xml2 := put_campo(xml2,'FECHA_EVENTO',to_char(now(),'YYYY/MM/DD HH24:MI:SS'));
	   xml2 := put_campo(xml2,'RUT_EMISOR',split_part(get_xml('RUTEmisor',data1),'-',1));
	   xml2 := put_campo(xml2,'RUT_RECEPTOR',split_part(get_xml('RUTRecep',data1),'-',1));
	   xml2 := put_campo(xml2,'TIPO_DTE',get_xml('TipoDTE',data1));
	   xml2 := put_campo(xml2,'FOLIO',get_xml('Folio',data1));
	   xml2 := put_campo(xml2,'FECHA_EMISION',get_xml('FchEmis',data1));
	   xml2 := put_campo(xml2,'MONTO_NETO',get_xml('MntNeto',data1));
       else
	   xml2 := put_campo(xml2,'FECHA_EVENTO',to_char(now(),'YYYY/MM/DD HH24:MI:SS'));
    	   xml2 := put_campo(xml2,'MONTO_NETO',get_campo('MONTO_TOTAL',xml2));           
       end if;
       if (get_campo('FLAG_PENDIENTE',xml2)='SI') then
		xml2 := graba_bitacora(xml2,'ERP_SEND_CLI_PENDIENTE');
       else	
	       xml2 := graba_bitacora(xml2,'ERP_SEND_CLI');
       end if;
       xml2 := logapp(xml2,'Graba Evento ERP_SEND_CLI para URI='||get_campo('URI_IN',xml2));
   --Ejecuto la funcion inn
       xml2:=logapp(xml2,'Ejecuta '||stDND.sp_in_recibidos);
       xml3:=xml2;
       execute 'select ' || stDND.sp_in_recibidos || '(' || quote_literal(xml3) || ')' into xml3;
       if (xml3 is null or xml3='') then
		xml2 := logapp(xml2,'Falla Ejecucion Funcion '||stDND.sp_in_recibidos);
	else
		--Se remplaza xml2 por xml3
		xml2:=xml3;
       end if;	
   else
	   xml2:=logapp(xml2,'No llena variable v_ejecuta_in');
   end if;

   xml2 := logapp(xml2,'SIGUIENTE SECUENCIA:' || get_campo('__SECUENCIAOK__',xml2));
   return xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION proc_procesa_respuesta_erp_12707(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2    varchar;
	id1	varchar;
	funcion1	varchar;
BEGIN
    xml2:=xml1;
    --Procesamos la respuesta la respuesta
    funcion1:=get_campo('FUNCION_OUT',xml2);
--    if (get_campo('RUT_RECEPTOR',xml2)='90844000') then
	--insert into jsprueba select xml2;
--	perform logfile('JSPRUEBA: '||xml2);
--	xml2:=logapp(xml2,'Funcion JSPRUEBA:' || xml2);
--	return xml2;
--    end if;
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
		if  (get_campo('FLAG_PENDIENTE',xml2)='SI') then
			xml2:=graba_bitacora(xml2,'ERP_RECV_CLI_PENDIENTE');
		else
			xml2:=graba_bitacora(xml2,'ERP_RECV_CLI');
		end if;
		xml2:=logapp(xml2,'DTE recibido OK ERP Cliente URI='||get_campo('URI_IN',xml2));
    else
		--Se actualiza el numero de reintentos y la fecha
		update documentos_recibidos_x_enviar_erp set reintentos=coalesce(reintentos,0)+1, fec_ult_reintento=now() where id=id1::bigint;
   		xml2 := put_campo(xml2,'FECHA_EVENTO',to_char(clock_timestamp(),'YYYY/MM/DD HH24:MI:SS'));
   		xml2 := put_campo(xml2,'COMENTARIO_TRAZA','Cliente no recibe DTE.');
		if (get_campo('FLAG_PENDIENTE',xml2)='SI') then		
			xml2:=graba_bitacora(xml2,'ERP_FALLA_CLI_PENDIENTE');
		else
			xml2:=graba_bitacora(xml2,'ERP_FALLA_CLI');
		end if;
		xml2:=logapp(xml2,'DTE no recibido por ERP Cliente URI='||get_campo('URI_IN',xml2));
    end if;    
    
    RETURN xml2;
END;
$$ LANGUAGE plpgsql;


