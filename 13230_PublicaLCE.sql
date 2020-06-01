--Publica documento
delete from isys_querys_tx where llave='13230';
--insert into isys_querys_tx values ('13230',10,1,1,'select proc_procesa_publica_dte_13230(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
--insert into isys_querys_tx values ('13230',20,1,2,'Llamada al Storage Writer',4010,104,105,0,0,30,30);
--insert into isys_querys_tx values ('13230',30,1,1,'select proc_procesa_publica_dte_respuesta_13230(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

--Llamamos a Escribir Directo
insert into isys_querys_tx values ('13230',40,1,1,'select proc_prepara_graba_directo_almacen_13230(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('13230',50,1,3,'Llamada a Escribir en Almacen',8015,0,0,0,0,60,60);
insert into isys_querys_tx values ('13230',60,1,1,'select proc_respuesta_almacen_13230(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

CREATE or replace FUNCTION proc_prepara_graba_directo_almacen_13230(varchar) RETURNS varchar AS $$
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
    fecha1	varchar;
    directorio1 varchar;
    tabla_traza1	varchar;
    uri1	varchar;
    stTraza	traza.traza%ROWTYPE;
BEGIN
    xml2:=xml1;

    --Si es un get salgo altiro
    if (get_campo('REQUEST_METHOD',xml2)='GET') then
        if ((strpos(get_campo('HTTP_USER_AGENT',xml2),'check_http')>0) or (length(get_campo('QUERY_STRING',xml2))=0)) then
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
        xml2 := put_campo(xml2,'__PUBLICADO_OK__','NO');
	return xml2;	
    end if;


    --FAY 2015-03-26 Solo para EMITIDOS
    if (strpos(get_campo('URI_IN',xml2),'?k=')>0) then
	    --Si ya tiene el evento PUB en traza, no se publica
	    tabla_traza1:=get_tabla_traza(uri1);
	    begin
               execute 'select * from '||tabla_traza1||' where uri=$1 and evento=''PUB''' into stTraza using uri1;
               --Si no esta el evento..
               if stTraza.uri is not null then
        		xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
                        xml2 := put_campo(xml2,'__PUBLICADO_OK__','SI');
			xml2 := logapp(xml2,'Uri '||uri1||' ya publicado');
		        return xml2;
               end if;
	    exception WHEN OTHERS THEN
               select * into stTraza from traza.traza where uri=uri1 and evento='PUB';
               if found then
                        --Si no esta el evento PUB vamos a publicar
        		xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
                        xml2 := put_campo(xml2,'__PUBLICADO_OK__','SI');
			xml2 := logapp(xml2,'Uri '||uri1||' ya publicado*');
		        return xml2;
               end if;
	    end;
    end if;
    --xml2:=put_context(xml2,'CONTEXTO_ALMACEN');

    --Solo windte por ahora
    --Ya se parseo el DTE
    host1:=split_part(split_part(uri1,'//',2),'/',1);
    data1:=get_campo('INPUT',xml2);
    --if (strpos(host1,'windte')=0) then
	--xml2:=put_campo(xml2,'__SECUENCIAOK__','0');
	--return xml2;
    --end if;
    xml2 := put_campo(xml2,'TX','8015'); 
    --xml2 := put_campo(xml2,'INPUT','3c3f786d6c2076657273696f6e3d'||split_part(split_part(get_campo('INPUT',xml2),'3c3f786d6c2076657273696f6e3d',2),'2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d',1));
 

    --Nuevo Procedimiento
    --largo1:=get_campo('CONTENT_LENGTH',xml2)::integer*2;
    --Busco donde empieza <?xml version
    --pos_inicial1:=strpos(data1,'3c3f786d6c2076657273696f6e3d');
    --Buscamos al reves donde esta el primer signo > que en hex es 3e
    --Como se pone un reverse se busca e3
    --xml2:=logapp(xml2,'MAGIA:'||strpos(reverse(data1),'e3')::varchar);
    --pos_final1:=largo1-pos_inicial1+4-strpos(reverse(data1),'e3');
    --data1:=substring(data1,pos_inicial1,pos_final1);
    xml2 := put_campo(xml2,'INPUT_CUSTODIUM',data1);
    xml2 := put_campo(xml2,'LEN_INPUT_CUSTODIUM',length(data1)::varchar);
    --xml2 := put_campo(xml2,'INPUT',data1);

    --TODO DTE PUBLICADO SE VA AL S3
    xml2:=graba_documento_s3(xml2);

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
    else
	--2015-03-17FAY para los DTE que vienen sin dominio se graban en cola_motor_sin_uri
       	xml2:=logapp(xml2,'Almacen: DTE sin dominio '||dominio1||' Se graba en cola_motor_sin_uri');
	xml2:=put_campo(xml2,'URI_IN','');
       	xml2 := put_campo(xml2,'__PUBLICADO_OK__','NO');
	return xml2;	
    end if;


    if (get_fecha_uri(uri1)::integer>=1701) then
        xml2:=get_parametros_motor(xml2,'PUBLICADOR_2017');
    else
        xml2:=get_parametros_motor(xml2,'PUBLICADOR');
    end if;

    --MDA 2014-09-11 Se cambia storage almacen por mas espacio
    --xml2:=put_campo(xml2,'ALMACEN','/opt/acepta/custodium/depot/'||dominio1||'/'||fecha1||'/'||directorio1||'/'||file1);
    xml2:=put_campo(xml2,'ALMACEN',get_campo('PARAMETRO_RUTA',xml2)||dominio1||'/'||fecha1||'/'||directorio1||'/'||file1);
    --xml2:=put_campo(xml2,'__IP_CONEXION_CLIENTE__','172.16.10.131');
    xml2:=logapp(xml2,'Almacen '||get_campo('ALMACEN',xml2));

    xml2:=logapp(xml2,'Almacen: Envia Data Directo '||get_campo('__IP_CONEXION_CLIENTE__',xml2)||' '||uri1);
    xml2:=put_campo(xml2,'__SECUENCIAOK__','50');
        
    xml2 := put_campo(xml2,'_STS_FILE_','');

    return xml2;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION proc_respuesta_almacen_13230(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2        varchar;
        data1   varchar;
	sts1	varchar;
	publicado1	varchar;
BEGIN
        --Cambio la respuesta de cuadratura por la respuesta original
        xml2:=xml1;
	sts1:=get_campo('_STS_FILE_',xml2);
	if (sts1='FILE_YA_EXISTE') then
		xml2 := logapp(xml2,'Almacen:File ya existe en Almacen');	
		xml2 := graba_bitacora(xml2,'PUB');
		publicado1:='SI';
		--Si va bien la publicacion, vamos a guardar el DTE al S3
		--xml2:=graba_documento_s3(xml2);
	elsif (sts1='OK') then
                xml2 := logapp(xml2,'Almacen:OK Escritos '||get_campo('_STS_FILE_BYTES_WRITTEN_',xml2)||' ContentLength:'||get_campo('CONTENT_LENGTH',xml2)||' Largo Data:'||get_campo('LEN_INPUT_CUSTODIUM',xml2));
		xml2:=put_campo(xml2,'COMENTARIO_TRAZA','');
		xml2:=put_campo(xml2,'COMENTARIO2','');
		xml2 := graba_bitacora(xml2,'PUB');
		publicado1:='SI';
		--Si va bien la publicacion, vamos a guardar el DTE al S3
		--xml2:=graba_documento_s3(xml2);
	else
                xml2 := logapp(xml2,'Almacen:Falla Almacen Directo '||get_campo('_STS_FILE_',xml2));
		publicado1:='NO';
        end if;
        xml2 := put_campo(xml2,'_STS_FILE_','');
        xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
	--xml2:=get_context(xml2,'CONTEXTO_ALMACEN');
        xml2 := put_campo(xml2,'__PUBLICADO_OK__',publicado1);
        return xml2;
END;
$$ LANGUAGE plpgsql;

