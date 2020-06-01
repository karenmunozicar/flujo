delete from isys_querys_tx where llave='8012';

-- Prepara llamada a Webiecv 
insert into isys_querys_tx values ('8012',20,1,1,'select proc_procesa_input_recibidos_edte_8012(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

--Voy a Grabar la tabla coladeprocesamiento webiecv
insert into isys_querys_tx values ('8012',30,2,1,'select proc_graba_colaprocesamientodte_8012(''$$INPUT$$'') as respuesta_webiecv',0,0,0,1,1,40,40);
--Actualiza Estado del libro en webiecv
insert into isys_querys_tx values ('8012',35,3,1,'select proc_actualiza_estado_libro_8012(''$$__XMLCOMPLETO__$$'') as respuesta_estado_libro',0,0,0,1,1,40,40);

--Llamada al AML
--insert into isys_querys_tx values ('8012',30,1,2,'Llamada a Webiecv',4001,100,101,0,0,40,40);
--Llamada al WebIecv
--insert into isys_querys_tx values ('8012',30,1,2,'Llamada a Webiecv',4003,100,101,0,0,40,40);

--Respuesta del AML
insert into isys_querys_tx values ('8012',40,1,1,'select proc_procesa_respuesta_edte_8012(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

CREATE or replace FUNCTION proc_procesa_input_recibidos_edte_8012(varchar) RETURNS varchar AS $$
DECLARE
    xml1	alias for $1;
    xml2	varchar;
    data1	varchar;
    sts		varchar;
BEGIN
    xml2:=xml1;
    data1:=decode(get_campo('INPUT',xml2),'hex');


    --Si es un estado de libro
    if (get_campo('TIPO_TX',xml2)='ESTADO_LIBRO') then 
	xml2 := put_campo(xml2,'RUT_EMISOR_LIBRO',get_xml('RutEmisorLibro',data1));
	xml2 := put_campo(xml2,'PERIODO_ENVIO',get_xml('PeriodoEnvio',data1));
	xml2 := put_campo(xml2,'TIPO_OPERACION',get_xml('TipoOperacion',data1));
	xml2 := put_campo(xml2,'TIPO_LIBRO',get_xml('TipoLibro',data1));
	xml2 := put_campo(xml2,'TIPO_ENVIO',get_xml('TipoEnvio',data1));
	xml2 := put_campo(xml2,'TRACK_ID',get_xml('TrackId',data1));
	xml2 := put_campo(xml2,'TMST_RECEPCION',get_xml('TmstRecepcion',data1));
	xml2 := put_campo(xml2,'NRO_SEGMENTO',get_xml('NroSegmento',data1));
	xml2 := put_campo(xml2,'ESTADO_ENVIO',get_xml('EstadoEnvio',data1));
	xml2 := put_campo(xml2,'ERROR_ENVIO_LIBRO',get_xml('ErrorEnvioLibro',data1));
	xml2 := put_campo(xml2,'COD_AUT_REC',get_xml('CodAutRec',data1));
	xml2 := put_campo(xml2,'STATUS',get_xml('Status',data1));
	xml2 := put_campo(xml2,'__SECUENCIAOK__','35');
	xml2 := logapp(xml2,'Libro Emisor='||get_campo('RUT_EMISOR_LIBRO',xml2)||' PERIODO_ENVIO='||get_campo('PERIODO_ENVIO',xml2)||' TRACK_ID='||get_campo('TRACK_ID',xml2));
        return xml2;
	
    end if;

    --Cambio el campo Host: localhost:9000 para que vaya al conector webiecv
    --data1:=regexp_replace(data1,'\nHost: [0-9\.]+|[:alnum:]\.]+\n',chr(10)||'Host: localhost:8082/webiecv-sii-connector/');
    --xml2:=put_campo(xml2,'INPUT',data1);

    --se Parsean datos basicos para continuar con procesamiento
    xml2 := reglas.parseo_datos(xml2);
    

    -- Valido si es un DTE recibido y salgo y lo paso a WebIecv para su proceso
    -- Porque ya fue procesado por flujo 8010 en la entrada, para la base de indice
   if strpos(data1,'<TipoOperacion>VENTA</TipoOperacion>')>0  or strpos(data1,'<TipoOperacion>COMPRA</TipoOperacion>')>0 then
	xml2 := put_campo(xml2,'__SECUENCIAOK__','30');
	xml2 := logapp(xml2,'Paso por aca');
        return xml2;
   end if;

    --Obtengo nombre para el archivo
    --sts:=write_file(get_campo('FILE',xml2),data1);
    --xml2 := put_campo(xml2,'STS',sts::varchar);

    --Procesador de Reglas
    xml2 := reglas.validacion(xml2);
    if get_campo('__EXIT__',xml2)='1' then
    	xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
	RETURN xml2;
    end if;

    /*xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
    return xml2;*/

    xml2 := put_campo(xml2,'ESTADO_INICIAL_DTE','INGRESADO');
    xml2 := insert_dte(xml2);
    --TODO hacer un control cuando falle el insert
    --xml2 := put_campo(xml2,'INPUT','');
    --Vamos por ahora siempre al AML
    xml2 := put_campo(xml2,'__SECUENCIAOK__','30');
    RETURN xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION proc_procesa_respuesta_edte_8012(varchar) RETURNS varchar AS $$
DECLARE
    xml1	alias for $1;
    xml2	varchar;
    data1	varchar;
    resp1	varchar;
BEGIN
    xml2:=xml1;
    --data1:=get_campo('INPUT',xml2);

    --Limpio el INPUT para el LOG
    xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
    xml2 := put_campo(xml2,'INPUT','CLEAN');
    resp1:= get_campo('RESPUESTA',xml2);

    if (get_campo('TIPO_TX',xml2)='ESTADO_LIBRO') then
    	   --Si me fue bien grabando el webiecv contesto para liberar el EDTE
	    if get_campo('RESPUESTA_ESTADO_LIBRO',xml2)='OK' then
        	xml2 := graba_bitacora(xml2,'ESTADO_LIBRO_INFORMADO_OK');
		xml2:=put_campo(xml2,'RESPUESTA','0');
		xml2:=put_campo(xml2,'STATUS_HTTP','200 OK');
		xml2:=responde_http_8011(xml2);
		xml2 := logapp(xml2,'Estado Libro Informado OK TRACK_ID='||get_campo('TRACK_ID',xml2));
		return xml2;
	    else
        	xml2 := graba_bitacora(xml2,'FALLA_ACT_ESTADO_LIBRO');
		xml2:=put_campo(xml2,'RESPUESTA','No Actualiza Estado Libro');
		xml2:=put_campo(xml2,'STATUS_HTTP','400 OK');
		xml2:=responde_http_8011(xml2);
		xml2 := logapp(xml2,'Falla Actualizacion de Estado Libro TRACK_ID='||get_campo('TRACK_ID',xml2));
		return xml2;
	    end if;
    else
    	   --Si me fue bien grabando el webiecv contesto para liberar el EDTE
	    if get_campo('RESPUESTA_WEBIECV',xml2)='OK' then
        	xml2 := graba_bitacora(xml2,'INFORMADO_LIBRO_WEBIECV');
		xml2:=put_campo(xml2,'RESPUESTA','0');
		xml2:=put_campo(xml2,'STATUS_HTTP','200 OK');
		xml2:=responde_http_8011(xml2);
		xml2 := logapp(xml2,'Grabacion OK en WEBIECV');
		return xml2;
	    else
        	xml2 := graba_bitacora(xml2,'FALLA_WEBIECV');
		xml2:=put_campo(xml2,'RESPUESTA','No Graba WEBIEVC');
		xml2:=put_campo(xml2,'STATUS_HTTP','400 OK');
		xml2:=responde_http_8011(xml2);
		xml2 := logapp(xml2,'Falla Grabacion en WEBIECV');
		return xml2;
	    end if;
   end if;

	/*
    --Verifico si me fue bien con el conector webiecv
    if strpos(resp1,'200 OK')>0 then
    	xml2 := put_campo(xml2,'ESTADO','ENVIADO_WEBIECV');
    else
    	xml2 := put_campo(xml2,'ESTADO','ERROR_CONECTOR_WEBIECV');
    end if; 

   -- Si es un DTE emitido y enviado desde edte, sale sin actualizar.
   if strpos(data1,'<TipoOperacion>VENTA</TipoOperacion>')>0 then
        xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
        return xml2;
   end if;


    --Saco los datos que requiero de la respuesta
    xml2 := put_campo(xml2,'URI',get_tag_http(resp1,'URL(True): '));

    
    xml2 := update_dte(xml2);
    --TODO hacer un control cuando falle el update
  
    --Respondo lo que viene
    xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
    RETURN xml2;
	*/
END;
$$ LANGUAGE plpgsql;
