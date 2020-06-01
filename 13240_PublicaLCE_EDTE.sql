--Publica documento
delete from isys_querys_tx where llave='13240';

--Llamamos a Escribir Directo
insert into isys_querys_tx values ('13240',40,1,1,'select proc_prepara_grabacion_edte_13240(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('13240',50,1,3,'Llamada a Escribir en EDTE',8016,0,0,0,0,60,60);
insert into isys_querys_tx values ('13240',60,1,1,'select proc_respuesta_edte_13240(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

DROP FUNCTION proc_prepara_grabacion_edte_13240(varchar);
CREATE or replace FUNCTION proc_prepara_grabacion_edte_13240(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2    varchar;
    data1       varchar;
    file1       varchar;
    sts         integer;
    host1       varchar;
    header1     varchar;
    largo1	integer;
    pos_final1 integer;	
    pos_inicial1 integer;
    dominio1 varchar;
    fecha1  varchar;
    directorio1 varchar;
    tabla_traza1    varchar;
    uri1    varchar;
    stTraza traza.traza%ROWTYPE;
	id1	varchar;
   	aux	varchar; 
	xml_libro varchar;
	--xml_libro_hexa varchar;
	
	rut_libro1 varchar;
	periodo1   varchar;
	tipo_libro1 varchar;
	tipo_operacion1 varchar;
	tipo_envio1	varchar;
        resultado_py	varchar;
BEGIN
    xml2:=xml1; 
    xml2:=put_campo(xml2,'__SECUENCIAOK__','0');
    xml2 := put_campo(xml2, 'INPUT_CUSTODIUM', get_campo('INPUT',xml2));
    xml2 := put_campo(xml2, 'LEN_INPUT_CUSTODIUM', length(get_campo('INPUT',xml2))::varchar);

    --Si es un get salgo altiro
    if (get_campo('REQUEST_METHOD',xml2)='GET') then
        if ((strpos(get_campo('HTTP_USER_AGENT',xml2),'check_http')>0) or (length(get_campo('QUERY_STRING',xml2))=0))
	then
                xml2 := logapp(xml2,'Nagios Check o GET sin datos, se ignora');
                --Se Responde OK
                xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK'||chr(10)||
                'Content-type: text/html; charset=iso-8859-1'||chr(10)||
                'Content-length: 0'||chr(10)||
                'Vary: Accept-Encoding'||chr(10)||chr(10));
                xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
                xml2 := sp_procesa_respuesta_cola_motor(xml2);
                xml2 := put_campo_ctx(xml2,'__ETAPA1__','OK');
                return xml2;
        end if;
    end if;

    uri1:=get_campo('URI_IN',xml2);

    --20150224 FAY Si no viene URI no se puede publicar
    if (length(uri1)=0) then
	xml2 := logapp(xml2,'No viene URI_IN, no se puede publicar');
        xml2 := put_campo(xml2,'__EDTE_LCE_OK__','NO');
    	--xml2:=logapp(xml2,'JCC-RUT_EMISOR_LCE RutEmisorLibro: '||rut_libro1||' '||periodo1||' '||tipo_libro1||' '||tipo_operacion1||' '||tipo_envio1);
	return xml2;	
    end if;

    --Verificamos si viene comprimido en bzip2
    aux:=get_xml_hex(encode('CipherValue','hex'),get_campo('INPUT',xml2));
    if (length(aux)>0) then
	data1:=encode(('<?xml version="1.0" encoding="ISO-8859-1"?>'||chr(10))::bytea,'hex')||bzip2_to_xml(aux);
	xml2:=put_campo(xml2,'INPUT_CUSTODIUM',data1);
	xml2 := put_campo(xml2,'LEN_INPUT_CUSTODIUM',length(data1)::varchar);
    end if;

    --- MDA, inserta en tabla libros_data para busqueda
    /*xml_libro_hexa:=get_campo('INPUT_CUSTODIUM',xml2);
    if (length(xml_libro_hexa)=0) then
	--si viene compreso lo obtengo de input_custodium, sino de input
   	xml_libro_hexa:=get_campo('INPUT',xml2);
    end if;
    */
    --Si ya tiene el evento ENVIADO_EDTE_LCE, no envio el mandato
    tabla_traza1:=get_tabla_traza(uri1);
    --begin
    execute 'select * from '||tabla_traza1||' where uri=$1 and evento=''ENVIADO_EDTE_LCE''' into stTraza using uri1;
    --Si no esta el evento..
    if stTraza.uri is not null then
        xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
        xml2 := put_campo(xml2,'__EDTE_LCE_OK__','OK');
	xml2 := logapp(xml2,'Uri '||uri1||' LCE ya enviado al EDTE');
	xml2 := put_campo(xml2,'RESPUESTA','Status: 200 OK');
	xml2 := sp_procesa_respuesta_cola_motor(xml2);
        return xml2;
    end if;

    --xml2:=put_context(xml2,'CONTEXTO_ALMACEN');
    xml2 := put_campo(xml2,'TX','8016'); 
    --xml2 := put_campo(xml2,'TX','8015'); 

    --Si ya vengo de publicar el Documento y tengo el INPUT_CUSTODIUM, no lo vuelvo a parsear
    
    if (get_campo('INPUT_CUSTODIUM',xml2)='') then
	    --Ya se parseo el DTE
	    host1:=split_part(split_part(uri1,'//',2),'/',1);
	    data1:=get_campo('INPUT',xml2);

	    --Nuevo Procedimiento
	    largo1:=get_campo('CONTENT_LENGTH',xml2)::integer*2;
	    --Busco donde empieza <?xml version
	    pos_inicial1:=strpos(data1,'3c3f786d6c2076657273696f6e3d');
	    --Buscamos al reves donde esta el primer signo > que en hex es 3e
	    --Como se pone un reverse se busca e3
	    --xml2:=logapp(xml2,'MAGIA:'||strpos(reverse(data1),'e3')::varchar);
	    pos_final1:=largo1-pos_inicial1+4-strpos(reverse(data1),'e3');
	    data1:=substring(data1,pos_inicial1,pos_final1);
	    xml2 := put_campo(xml2,'INPUT_CUSTODIUM',data1);
	    xml2 := put_campo(xml2,'LEN_INPUT_CUSTODIUM',length(data1)::varchar);
    end if;

    --xml2:=get_parametros_motor(xml2,'PUBLICADOR');
    xml2:=get_parametros_motor(xml2,'EDTE_LCE');

    file1:=replace(replace(replace(replace(uri1,':','%3A'),'/','%2F'),'?','%3F'),'=','%3D');

    xml2:=put_campo(xml2,'ALMACEN',get_campo('PARAMETRO_RUTA',xml2)||'/escribiendo_motor/'||file1);
    xml2:=logapp(xml2,'EDTE LCE: '||get_campo('ALMACEN',xml2));
    xml2:=put_campo(xml2,'SCRIPT_EDTE','/bin/mv '||get_campo('PARAMETRO_RUTA',xml2)||'/escribiendo_motor/'||file1||' '||get_campo('PARAMETRO_RUTA',xml2)|| '/pendiente/'||file1);
    xml2:=logapp(xml2,'Script EDTE: '|| get_campo('SCRIPT_EDTE', xml2));
    xml2:=put_campo(xml2,'__SECUENCIAOK__','50');
    xml2:=put_campo(xml2,'_STS_FILE_','');
    xml2:=put_campo(xml2,'__EDTE_LCE_OK__','NK');
    /*
    xml2:=put_campo(xml2,'ALMACEN',get_campo('PARAMETRO_RUTA',xml2)||dominio1||'/'||fecha1||'/'||directorio1||'/'||file1);
    xml2:=logapp(xml2,'Almacen '||get_campo('ALMACEN',xml2));
    xml2:=logapp(xml2,'Almacen: Envia Data Directo '||get_campo('__IP_CONEXION_CLIENTE__',xml2)||' '||uri1);
    xml2:=put_campo(xml2,'SCRIPT_EDTE','/bin/mv '||(get_campo('PARAMETRO_RUTA',xml2)||dominio1||'/'||fecha1||'/'||directorio1||'/'||file1)||' /opt/acepta/enviodte/work/sii/lce/pendiente/'||file1);
    xml2:=logapp(xml2,'Script EDTE: '|| get_campo('SCRIPT_EDTE', xml2));
    xml2:=put_campo(xml2,'__SECUENCIAOK__','50');
    xml2:=put_campo(xml2,'_STS_FILE_','');
    */
/*
    file1:=replace(replace(replace(replace(uri1,':','%3A'),'/','%2F'),'?','%3F'),'=','%3D');
    xml2:=put_campo(xml2,'ALMACEN','/opt/acepta/enviodte/work/sii/lce/escribiendo_motor/'||file1);
    xml2:=logapp(xml2,'EDTE LCE: '||get_campo('ALMACEN',xml2));


    xml2:=put_campo(xml2,'SCRIPT_EDTE','/bin/mv /opt/acepta/enviodte/work/sii/lce/escribiendo_motor/'||file1||' /opt/acepta/enviodte/work/sii/lce/pendiente/'||file1);
    --xml2:=put_campo(xml2,'SCRIPT_EDTE','echo 1');
    xml2:=logapp(xml2,'EDTE LCE Script:'||get_campo('SCRIPT_EDTE',xml2));
    
    --Voy siempre a la IP del EDTE
    if (get_campo('MODO_QA',xml2)='ON') then
    	xml2:=put_campo(xml2,'__IP_CONEXION_CLIENTE__','172.16.14.116');
    	xml2:=put_campo(xml2,'__IP_PORT_CLIENTE__','8015');
    else
    	xml2:=put_campo(xml2,'__IP_CONEXION_CLIENTE__', get_campo('__IP_CONEXION_CLIENTE__',xml2));
    	xml2:=put_campo(xml2,'__IP_PORT_CLIENTE__','8015');
    end if;

    xml2:=logapp(xml2,'EDTE LCE: Envia Data Directo '||get_campo('__IP_CONEXION_CLIENTE__',xml2)||' '||uri1);
    xml2:=put_campo(xml2,'__SECUENCIAOK__','50');
    xml2 := put_campo(xml2,'_STS_FILE_','');
    xml2 := put_campo(xml2,'__EDTE_LCE_OK__','NK');
   */
    return xml2;
END;
$$ LANGUAGE plpgsql;


DROP FUNCTION proc_respuesta_edte_13240(varchar);
CREATE or replace FUNCTION proc_respuesta_edte_13240(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2        varchar;
        data1   varchar;
	sts1	varchar;
	publicado1	varchar;
	xml3		varchar;
	cola1  		bigint;
	nombre_tabla1   varchar;
        uri1    	varchar;
        rut1    varchar;
        tx1     varchar;
	id1	varchar;
	codigo1	varchar;


BEGIN
        --Cambio la respuesta de cuadratura por la respuesta original
        xml2:=xml1;
	sts1:=get_campo('_STS_FILE_',xml2);
	if (sts1='FILE_YA_EXISTE') then
		xml2 := logapp(xml2,'EDTE LCE:File ya existe en EDTE');	
    		xml2 := put_campo(xml2,'__EDTE_LCE_OK__','OK');
		xml2 := graba_bitacora(xml2,'ENVIADO_EDTE_LCE');
	--	codigo1:=get_campo('CODIGO_TXEL_LCE',xml2);
	elsif (sts1='OK') then
                xml2 := logapp(xml2,'EDTE:OK Escritos '||get_campo('_STS_FILE_BYTES_WRITTEN_',xml2)||' ContentLength:'||get_campo('CONTENT_LENGTH',xml2)||' Largo Data:'||get_campo('LEN_INPUT_CUSTODIUM',xml2));
    		xml2 := put_campo(xml2,'__EDTE_LCE_OK__','OK');
		xml2 := graba_bitacora(xml2,'ENVIADO_EDTE_LCE');
	else
                xml2 := logapp(xml2,'EDTE LCE:Falla EDTE Directo '||get_campo('_STS_FILE_',xml2));
    		xml2 := put_campo(xml2,'__EDTE_LCE_OK__','NK');
		xml2 := graba_bitacora(xml2,'FALLA_ENVIADO_EDTE_LCE');
        end if;
        xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
        xml2 := put_campo(xml2,'_STS_FILE_','');

	--Si falla, ponemos el mensaje en la cola para procesamiento posterior
        /*
	if (get_campo('__EDTE_LCE_OK__',xml2)<>'OK') then
		--Si no viene de reintento tengo que entrar a la cola
		if (get_campo('__FLAG_REINTENTO_LCE__',xml2)<>'SI') then
    			xml2 := put_campo(xml2,'__EDTE_LCE_OK__','OK');
			xml3:='';
			xml3:=put_campo(xml3,'TX','13240');
			xml3:=put_campo(xml3,'URI_IN',get_campo('URI_IN',xml2));
			xml3:=put_campo(xml3,'INPUT_CUSTODIUM',get_campo('INPUT_CUSTODIUM',xml2));
			xml3:=put_campo(xml3,'LEN_INPUT_CUSTODIUM',get_campo('LEN_INPUT_CUSTODIUM',xml2));
			xml3:=put_campo(xml3,'RUT_EMISOR',get_campo('RUT_EMISOR',xml2));
			xml3:=put_campo(xml3,'__FLAG_REINTENTO_LCE__','SI');
			xml3:=put_campo(xml3,'__DTE_CON_LCE__','SI');
			
			cola1:=nextval('id_cola_procesamiento');
		        nombre_tabla1:='cola_motor_'||cola1::varchar;
		        uri1:=get_campo('URI_IN',xml2);
		        rut1:=get_campo('RUT_EMISOR',xml2);
			tx1:='45';
			xml2 := logapp(xml2,'EDTE LCE: Graba uri '||uri1||' en cola');

			execute 'insert into ' || nombre_tabla1 || ' (fecha,uri,reintentos,data,tx,rut_emisor,reproceso,categoria) values ( now(),'||quote_literal(uri1)||',0,'||quote_literal(xml3)||','||tx1||','||quote_literal(rut1)||',''NO'',''LCE_EDTE'');';	
			return xml2;
		else
			id1:=get_campo('__ID_DTE__',xml2);
			--Si viene de un reintento, aumento reintentos
			xml2:=logapp(xml2,'Aumenta Reintentos Envio de LCE Edte');
			execute 'update '||get_campo('__COLA_MOTOR__',xml2)||' set reintentos=reintentos+1 where id='||id1;
			return xml2;
		end if;
	--Envio Libros exitoso
	else
		--id1:=get_campo('__ID_DTE__',xml2);
		--Si viene de un reintento, aumento reintentos
		xml2:=logapp(xml2,'Se borra LCE edte de la cola');
		xml2 := put_campo(xml2,'RESPUESTA','Status: 200 OK');
		xml2 := sp_procesa_respuesta_cola_motor(xml2);
		--execute 'delete from '||get_campo('__COLA_MOTOR__',xml2)||' where id='||id1;
		return xml2;
               */
		/*
		--Si me fue bien y es un __FLAG_REINTENTO_LCE__, lo borro de la cola
		if (get_campo('__FLAG_REINTENTO_LCE_',xml2)='SI') then
			id1:=get_campo('__ID_DTE__',xml2);
			--Si viene de un reintento, aumento reintentos
			xml2:=logapp(xml2,'Se borra LCE edte de la cola');
			execute 'delete from '||get_campo('__COLA_MOTOR__',xml2)||' where id='||id1;
			return xml2;
		end if;
		*/
       /*
	end if;
        */
        return xml2;
END;
$$ LANGUAGE plpgsql;

