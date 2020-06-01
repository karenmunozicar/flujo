--Publica documento
delete from isys_querys_tx where llave='12788';

--Llamamos a Escribir Directo
insert into isys_querys_tx values ('12788',40,1,1,'select proc_prepara_grabacion_edte_12788(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('12788',50,1,3,'Llamada a Escribir en EDTE',8016,0,0,0,0,60,60);
insert into isys_querys_tx values ('12788',60,1,1,'select proc_respuesta_edte_12788(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

CREATE or replace FUNCTION proc_prepara_grabacion_edte_12788(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2    varchar;
    xml3    varchar;
    data1       varchar;
    file1       varchar;
    sts         integer;
    host1       varchar;
    header1     varchar;
   largo1	integer;
    pos_final1 integer;	
    pos_inicial1 integer;
    dominio1 varchar;
fecha1	varchar;
directorio1 varchar;
tabla_traza1	varchar;
uri1	varchar;
stTraza	traza.traza%ROWTYPE;
	id1	varchar;
   	aux	varchar; 
	xml_libro varchar;
	xml_libro_hexa varchar;
	rut_libro1 varchar;
	periodo1   varchar;
	tipo_libro1 varchar;
	tipo_operacion1 varchar;
	tipo_envio1	varchar;
        resultado_py	varchar;
    tipo_operacion_WEBIECV1 varchar;
    tipo_libro1_WEBIECV varchar;
	json1	json;
	digest1	varchar;
BEGIN
    xml2:=xml1; 
    xml2:=put_campo(xml2,'__SECUENCIAOK__','0');

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
        xml2 := put_campo(xml2,'__EDTE_IECV_OK__','NO');
	return xml2;	
    end if;

   --Verificamos si viene comprimido en bzip2
   aux:=get_xml_hex(encode('CipherValue','hex'),get_campo('INPUT',xml2));
   if (length(aux)>0) then
	begin
		data1:=encode(('<?xml version="1.0" encoding="ISO-8859-1"?>'||chr(10))::bytea,'hex')||bzip2_to_xml(aux);
	EXCEPTION WHEN OTHERS THEN
		xml2 := logapp(xml2,'Falla Funcion bzip2_to_xml');
		xml2 := put_campo(xml2,'__EDTE_IECV_OK__','NO');
         	xml2:=put_campo(xml2,'RESPUESTA','Status: 444 NK');
         	xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
		xml2 := sp_procesa_respuesta_cola_motor(xml2);
		return xml2;
	END;
	xml2:=put_campo(xml2,'INPUT_CUSTODIUM',data1);
	xml2 := put_campo(xml2,'LEN_INPUT_CUSTODIUM',length(data1)::varchar);
   end if;

    --- MDA, inserta en tabla libros_data para busqueda
    xml_libro_hexa:=get_campo('INPUT_CUSTODIUM',xml2);
    if (length(xml_libro_hexa)=0) then
	--si viene compreso lo obtengo de input_custodium, sino de input
   	xml_libro_hexa:=get_campo('INPUT',xml2);
    end if;
    xml_libro:= decode(xml_libro_hexa,'hex');
    rut_libro1:= get_xml('RutEmisorLibro',xml_libro);
    
    if (valida_rut(rut_libro1) is false) then
	--Graba en la traza
	 xml2 := put_campo(xml2,'COMENTARIO_TRAZA','Rut Emisor Libro no Numerico '||rut_libro1);
	 xml2 := graba_bitacora(xml2,'ERROR_DTE');
	 xml2 := logapp(xml2,'Rut Emisor Invalido '||rut_libro1);
	 xml2 := put_campo(xml2,'__EDTE_IECV_OK__','NO');
         xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK'||chr(10)||
                'Content-type: text/html; charset=iso-8859-1'||chr(10)||
                'Content-length: 0'||chr(10)||
                'Vary: Accept-Encoding'||chr(10)||chr(10));
         xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
	 xml2 := sp_procesa_respuesta_cola_motor(xml2);
	 return xml2;
   	 rut_libro1:='0-0';
    end if;
    --Si no trae un - esta malo

    periodo1:= get_xml('PeriodoTributario',xml_libro);
    --fecha_emision y dia se obtiene con now()
    tipo_libro1:= get_xml('TipoLibro',xml_libro);
    tipo_operacion1:= get_xml('TipoOperacion',xml_libro);
    tipo_envio1:= get_xml('TipoEnvio',xml_libro);
    --se obtienen los montos y cantidades de dtes para sumar 
    xml2:=logapp(xml2,'rut emisor iecv:'||rut_libro1||' '||periodo1||' '||tipo_libro1||' '||tipo_operacion1||' '||tipo_envio1);
    perform logfile('[ ' || rut_libro1 || '] rut emisor iecv:'||rut_libro1||' '||periodo1||' '||tipo_libro1||' '||tipo_operacion1||' '||tipo_envio1);

	digest1:=get_xml('DigestValue',xml_libro);
   
    --se obtiene la data de los dtes y de los totales 
     --resultado_py:= suma_tags_xml(xml_libro_hexa,'ResumenPeriodo');
     --xml2:=logapp(xml2,'resultado suma python '|| resultado_py );

    --insert into libros_data
    execute 'insert into libros_data (rut_emisor, periodo, fecha_emision, dia, tipo_libro, tipo_operacion, tipo_envio, total_dtes, monto_total, uri, fecha_ingreso,digest) 
	     values ('||split_part(rut_libro1,'-',1)::integer||','||quote_literal(periodo1)||',now(),to_char(now(),''YYYYMMDD'')::integer,'||quote_literal(tipo_libro1)||','||quote_literal(tipo_operacion1)||','||quote_literal(tipo_envio1)||',0,0,'||quote_literal(uri1)||',now(),'||quote_literal(digest1)||');';

    perform logfile('[ ' || rut_libro1 || '] FLUJO_12788 inserto libros_data:'||rut_libro1||' '||periodo1||' '||tipo_libro1||' '||tipo_operacion1||' '||tipo_envio1);
    perform logfile('[ ' || rut_libro1 || '] FLUJO_12788 flag_origen:' ||rut_libro1||' '||periodo1||' '||tipo_libro1||' '||tipo_operacion1||' '||tipo_envio1 || get_campo('FLAG_ORIGEN',xml2));
    --RME Se inserta en App Webiecv.
    --RME GAC 20160224 Se valida FLAG_ORIGEN para no volver a grabar libro cuando viene desde escritorio-webiecv
    --if (get_campo('FLAG_ORIGEN',xml2) <> 'WEBIECV 2') or  (get_campo('FLAG_ORIGEN',xml2) <> '' )then
    xml2:=put_campo(xml2, 'TIPO_DTE', tipo_operacion1);
    if get_campo('FLAG_ORIGEN',xml2) not in ('WEBIECV 2','') then
    
	    --Se transforma tipo_operacion
	    if tipo_operacion1 = 'VENTA' then
		tipo_operacion_WEBIECV1:='2';
	    elsif tipo_operacion1 = 'COMPRA' then
		tipo_operacion_WEBIECV1:='1';
	    end if;

	    --se transforma Tipo Libro
	    if tipo_libro1 = 'MENSUAL' then
		tipo_libro1_WEBIECV:='1';
	    elsif tipo_libro1 = 'ESPECIAL' then
		tipo_libro1_WEBIECV:='2';
	    elsif tipo_libro1 = 'RECTIFICA' then
		tipo_libro1_WEBIECV:='3';
	    else
		tipo_libro1_WEBIECV:='1';
	    end if;	

    
	    xml3 := get_parametros_motor('','WEBIECV');
	    xml2:=logapp(xml2,'Grabando en IECV2-->'||uri1);
	    --Se graba libro en WEBIECV, con estado inicial 7=PUBLICADO
	    perform logfile('rut emisor iecv:'||tipo_libro1_WEBIECV);
	    perform logfile('rut emisor iecv: insert into iecv.libros (rut_emisor,periodo,tipo_operacion_codigo,estado_libro_codigo,traza_uri,usuario_responsable,fecha_registro,tipo_libro_codigo,url_libro,origen,fecha_modificacion) values ('||split_part(rut_libro1,'-',1)||','||replace(periodo1,'-','')||','||tipo_operacion_WEBIECV1||',7,''https://motor-prod.acepta.com/bitacora/?url='||uri1||''',''MOTOR'',now(),'||tipo_libro1_WEBIECV||',''https://almacen.acepta.com/ca4webv3/?url='||uri1||''',''CA4XML'',now())'); 

	    json1:= query_db_json(get_campo('__IP_CONEXION_CLIENTE__',xml3),get_campo('__IP_PORT_CLIENTE__',xml3)::integer,'insert into iecv.libros (rut_emisor,periodo,tipo_operacion_codigo,estado_libro_codigo,traza_uri,usuario_responsable,fecha_registro,tipo_libro_codigo,url_libro,origen,fecha_modificacion) values ('||split_part(rut_libro1,'-',1)||','||replace(periodo1,'-','')||','||tipo_operacion_WEBIECV1||',7,''https://motor-prod.acepta.com/bitacora/?url='||uri1||''',''MOTOR'',now(),'||tipo_libro1_WEBIECV||',''https://almacen.acepta.com/ca4webv3/?url='||uri1||''',''CA4XML'',now())'); 
           perform logfile('[ ' || rut_libro1 || '] FLUJO_12788 JSON Salida: ' ||rut_libro1||' '||periodo1||' '||tipo_libro1||' '||tipo_operacion1||' '||tipo_envio1 || ' --> ' || json1);
    end if;

    --Si ya tiene el evento ENVIADO_EDTE_IECV, no envio el mandato
    tabla_traza1:=get_tabla_traza(uri1);
    --begin
               execute 'select * from '||tabla_traza1||' where uri=$1 and evento=''ENVIADO_EDTE_IECV''' into stTraza using uri1;
               --Si no esta el evento..
               if stTraza.uri is not null then
        		xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
        		xml2 := put_campo(xml2,'__EDTE_IECV_OK__','OK');
			xml2 := logapp(xml2,'Uri '||uri1||' IECV ya enviado al EDTE');
			xml2 := put_campo(xml2,'RESPUESTA','Status: 200 OK');
			xml2 := sp_procesa_respuesta_cola_motor(xml2);
			/*
			--Si ya existe el envio de mandato
                	if (get_campo('__FLAG_REINTENTO_IECV__',xml2)='SI') then
                        	id1:=get_campo('__ID_DTE__',xml2);
	                        --Si viene de un reintento, aumento reintentos
        	                xml2:=logapp(xml2,'Se borra libro envio_edte de la cola');
                	        execute 'delete from '||get_campo('__COLA_MOTOR__',xml2)||' where id='||id1;
	                end if;
			*/

		        return xml2;
               end if;
	/*
    exception WHEN OTHERS THEN
               select * into stTraza from traza.traza where uri=uri1 and evento='ENVIADO_EDTE_IECV';
               if not found then
                        --Si no esta el evento PUB vamos a publicar
        		xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
        		xml2 := put_campo(xml2,'__EDTE_IECV_OK__','OK');
			xml2 := logapp(xml2,'Uri '||uri1||' IECV ya enviado al EDTE*');
		        return xml2;
               end if;
    end;
	*/
    --xml2:=put_context(xml2,'CONTEXTO_ALMACEN');
    xml2 := put_campo(xml2,'TX','8016'); 

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

   /*
    dominio1:=split_part(split_part(uri1,'//',2),'.',1);
    --Los ultimo 4 del dominio1
    if length(dominio1)>4 then
    	fecha1:=substring(dominio1,length(dominio1)-3,4);
        --xml2:=logapp(xml2,'fecha1='||fecha1);
        dominio1:=lower(substring(dominio1,1,length(dominio1)-4));
        --xml2:=logapp(xml2,'dominio1='||dominio1);
        file1:=split_part(uri1,'/',5);
        --xml2:=logapp(xml2,'file1='||file1);
        directorio1:=substring(file1,1,2);
        --xml2:=logapp(xml2,'directorio1='||directorio1);
        file1:=split_part(substring(file1,3,length(file1)),'?',1);
        --xml2:=logapp(xml2,'file1='||file1);
    end if;
    */



    --http%3A%2F%2Fdcummins1503.acepta.com%2Fv01%2F8747A8B9163F1433677E5676D9E619701998F1AA%3Fk%3D8181b82294071788d29d6992b4caf785
    file1:=replace(replace(replace(replace(uri1,':','%3A'),'/','%2F'),'?','%3F'),'=','%3D');
    xml2:=put_campo(xml2,'ALMACEN','/opt/acepta/enviodte/work/sii/iecv/escribiendo_motor/'||file1);
    xml2:=logapp(xml2,'EDTE IECV: '||get_campo('ALMACEN',xml2));


    --RME 20170327 Se agrega diferencia para los libros de CGE

   --xml2:=put_campo(xml2,'RUT_CGE',get_campo('RUT_EMISOR',xml2));
   xml2:=put_campo(xml2,'RUT_CGE',split_part(rut_libro1,'-',1));
   xml2:=verifica_evento_cge(xml2);

   --Si no es CGE
   if (get_campo('EVENTO_CGE',xml2)<>'SI') then
        xml2:=get_parametros_motor(xml2,'IECV_ENVIO_EDTE');
   else
        xml2:=get_parametros_motor(xml2,'IECV_EDTE_CGE');
   end if;
    




    --xml2:=put_campo(xml2,'SCRIPT_EDTE','/bin/mv /opt/acepta/enviodte/work/sii/iecv/escribiendo_motor/'||file1||' /opt/acepta/enviodte/work/sii/iecv/pendiente/'||file1);
    xml2:=put_campo(xml2,'SCRIPT_EDTE','/bin/mv '||get_campo('PARAMETRO_RUTA',xml2)||'/escribiendo_motor/'||file1||' ' || get_campo('PARAMETRO_RUTA',xml2)||'/pendiente/'||file1);
    --xml2:=put_campo(xml2,'SCRIPT_EDTE','echo 1');

    xml2:=logapp(xml2,'EDTE IECV Script:'||get_campo('SCRIPT_EDTE',xml2));
    --Voy siempre a la IP del EDTE
--RME 20170327 Ya obtube parametros segun CGE o no. 
/*
    if (get_campo('MODO_QA',xml2)='ON') then
    	xml2:=put_campo(xml2,'__IP_CONEXION_CLIENTE__',get_campo('__IP_CONEXION_CLIENTE__',get_parametros_motor('','EDTE')));
    	xml2:=put_campo(xml2,'__IP_PORT_CLIENTE__','8015');
    else
    	xml2:=put_campo(xml2,'__IP_CONEXION_CLIENTE__',get_campo('__IP_CONEXION_CLIENTE__',get_parametros_motor('','EDTE')));
    	xml2:=put_campo(xml2,'__IP_PORT_CLIENTE__','8015');
    end if;
*/
    xml2:=logapp(xml2,'EDTE IECV: Envia Data Directo '||get_campo('__IP_CONEXION_CLIENTE__',xml2)||' '||uri1);
    xml2:=put_campo(xml2,'__SECUENCIAOK__','50');
    xml2 := put_campo(xml2,'_STS_FILE_','');
    xml2 := put_campo(xml2,'__EDTE_IECV_OK__','NK');
    return xml2;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION proc_respuesta_edte_12788(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2        varchar;
        data1   varchar;
	sts1	varchar;
	publicado1	varchar;
	xml3	varchar;
	cola1  bigint;
	nombre_tabla1   varchar;
	        uri1    varchar;
        rut1    varchar;
	        tx1     varchar;
	id1	varchar;
	codigo1	varchar;


BEGIN
        --Cambio la respuesta de cuadratura por la respuesta original
        xml2:=xml1;
	sts1:=get_campo('_STS_FILE_',xml2);
	if (sts1='FILE_YA_EXISTE') then
		xml2 := logapp(xml2,'EDTE IECV:File ya existe en EDTE');	
    		xml2 := put_campo(xml2,'__EDTE_IECV_OK__','OK');
		xml2 := graba_bitacora(xml2,'ENVIADO_EDTE_IECV');
	--	codigo1:=get_campo('CODIGO_TXEL_IECV',xml2);
	elsif (sts1='OK') then
                xml2 := logapp(xml2,'EDTE:OK Escritos '||get_campo('_STS_FILE_BYTES_WRITTEN_',xml2)||' ContentLength:'||get_campo('CONTENT_LENGTH',xml2)||' Largo Data:'||get_campo('LEN_INPUT_CUSTODIUM',xml2));
    		xml2 := put_campo(xml2,'__EDTE_IECV_OK__','OK');
		xml2 := graba_bitacora(xml2,'ENVIADO_EDTE_IECV');
	else
                xml2 := logapp(xml2,'EDTE IECV:Falla EDTE Directo '||get_campo('_STS_FILE_',xml2));
    		xml2 := put_campo(xml2,'__EDTE_IECV_OK__','NK');
		xml2 := graba_bitacora(xml2,'FALLA_ENVIADO_EDTE_IECV');
        end if;
        xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
        xml2 := put_campo(xml2,'_STS_FILE_','');

	--Si falla, ponemos el mensaje en la cola para procesamiento posterior
	if (get_campo('__EDTE_IECV_OK__',xml2)<>'OK') then
		--Si no viene de reintento tengo que entrar a la cola
		if (get_campo('__FLAG_REINTENTO_IECV__',xml2)<>'SI') then
    			xml2 := put_campo(xml2,'__EDTE_IECV_OK__','OK');
			xml3:='';
			xml3:=put_campo(xml3,'TX','12788');
			xml3:=put_campo(xml3,'URI_IN',get_campo('URI_IN',xml2));
			xml3:=put_campo(xml3,'INPUT_CUSTODIUM',get_campo('INPUT_CUSTODIUM',xml2));
			xml3:=put_campo(xml3,'LEN_INPUT_CUSTODIUM',get_campo('LEN_INPUT_CUSTODIUM',xml2));
			xml3:=put_campo(xml3,'RUT_EMISOR',get_campo('RUT_EMISOR',xml2));
			xml3:=put_campo(xml3,'__FLAG_REINTENTO_IECV__','SI');
			xml3:=put_campo(xml3,'__DTE_CON_IECV__','SI');
			
			cola1:=nextval('id_cola_procesamiento');
		        nombre_tabla1:='cola_motor_'||cola1::varchar;
		        uri1:=get_campo('URI_IN',xml2);
		        rut1:=get_campo('RUT_EMISOR',xml2);
			tx1:='45';
			xml2 := logapp(xml2,'EDTE IECV: Graba uri '||uri1||' en cola');


			execute 'insert into ' || nombre_tabla1 || ' (fecha,uri,reintentos,data,tx,rut_emisor,reproceso,categoria) values ( now(),'||quote_literal(uri1)||',0,'||quote_literal(xml3)||','||tx1||','||quote_literal(rut1)||',''NO'',''IECV_EDTE'');';	
			return xml2;
		else
			id1:=get_campo('__ID_DTE__',xml2);
			--Si viene de un reintento, aumento reintentos
			xml2:=logapp(xml2,'Aumenta Reintentos Envio de IECV Edte');
			execute 'update '||get_campo('__COLA_MOTOR__',xml2)||' set reintentos=reintentos+1 where id='||id1;
			return xml2;
		end if;
	--Envio Libros exitoso
	else
		--id1:=get_campo('__ID_DTE__',xml2);
		--Si viene de un reintento, aumento reintentos
		xml2:=logapp(xml2,'Se borra IECV edte de la cola');
		xml2 := put_campo(xml2,'RESPUESTA','Status: 200 OK');
		xml2 := sp_procesa_respuesta_cola_motor(xml2);
		--execute 'delete from '||get_campo('__COLA_MOTOR__',xml2)||' where id='||id1;
		return xml2;
		/*
		--Si me fue bien y es un __FLAG_REINTENTO_IECV__, lo borro de la cola
		if (get_campo('__FLAG_REINTENTO_IECV_',xml2)='SI') then
			id1:=get_campo('__ID_DTE__',xml2);
			--Si viene de un reintento, aumento reintentos
			xml2:=logapp(xml2,'Se borra IECV edte de la cola');
			execute 'delete from '||get_campo('__COLA_MOTOR__',xml2)||' where id='||id1;
			return xml2;
		end if;
		*/
	end if;
        return xml2;
END;
$$ LANGUAGE plpgsql;

