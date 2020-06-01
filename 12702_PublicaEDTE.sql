--Publica documento
delete from isys_querys_tx where llave='12702';
--insert into isys_querys_tx values ('12702',10,1,1,'select proc_procesa_publica_dte_12702(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
--insert into isys_querys_tx values ('12702',20,1,2,'Llamada al Storage Writer',4010,104,105,0,0,30,30);
--insert into isys_querys_tx values ('12702',30,1,1,'select proc_procesa_publica_dte_respuesta_12702(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

--Llamamos a Escribir Directo
insert into isys_querys_tx values ('12702',40,45,1,'select proc_prepara_grabacion_edte_12702(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('12702',50,1,3,'Llamada a Escribir en EDTE',8016,0,0,0,0,60,60);
--Llamada al flujo 8015 modo transaccional
--insert into isys_querys_tx values ('12702',55,1,2,'Llamada a Escribir en EDTE',9015,104,106,0,0,65,65);

insert into isys_querys_tx values ('12702',60,45,1,'select proc_respuesta_edte_12702(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('12702',65,45,1,'select proc_respuesta_edte_12702(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

CREATE or replace FUNCTION proc_prepara_grabacion_edte_12702(varchar) RETURNS varchar AS $$
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
   eventos1	varchar; 
	jtraza	json;
BEGIN
    xml2:=xml1;


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
        xml2 := put_campo(xml2,'__EDTE_OK__','NO');
	return xml2;	
    end if;


    --FAY-DAO 20180423
    jtraza=lee_traza_filtro(uri1,' evento in (''PSI'',''ASI'',''CSI'',''RSI'',''ESI'') ');
    if count_array_json(jtraza)>0 then
		xml2 := logapp(xml2,'eventos='||jtraza::varchar);
		 --Ya esta enviado, OK, salgo
                 xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
                 xml2 := put_campo(xml2,'__EDTE_OK__','SI');
                 xml2 := logapp(xml2,'Uri '||uri1||' ya esta enviado al SII');
                 xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
		--FAY-DAO-MDA 2018-11-27 Se debe sacar el documento de la dte_emitidos y enviarlo a la dte_Errores...
                 return xml2;
    end if;

	/*

    --Si ya tiene el evento PUB en traza, no se publica
    tabla_traza1:=get_tabla_traza(uri1);
    begin
	       
	        execute 'select string_agg(evento,'';'') from '||tabla_traza1||' where uri=$1 and evento in (''PSI'',''ASI'',''CSI'',''RSI'',''ESI'')' into eventos1 using uri1;
		eventos1:=coalesce(eventos1,'');
		xml2 := logapp(xml2,'eventos='||eventos1);
		--Si esta aprobado no enviamos al EDTE
		if strpos(eventos1,'ASI')>0 or strpos(eventos1,'CSI')>0 or  strpos(eventos1,'ESI')>0 or strpos(eventos1,'RSI')>0 or strpos(eventos1,'PSI')>0 then
			--Ya esta enviado, OK, salgo
			xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
			xml2 := put_campo(xml2,'__EDTE_OK__','SI');
			xml2 := logapp(xml2,'Uri '||uri1||' ya esta enviado al SII');
			xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
			return xml2;
		end if;
    exception WHEN OTHERS THEN
		xml2 := logapp(xml2,'Falla string_agg 12702');
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
    --xml2:=put_campo(xml2,'ALMACEN','/opt/acepta/enviodte/work/sii/dte/pendiente/.'||file1);

   xml2:=put_campo(xml2,'RUT_CGE',get_campo('RUT_EMISOR',xml2));
   xml2:=verifica_evento_cge(xml2);

   --Si no es CGE
   if (get_campo('EVENTO_CGE',xml2)<>'SI') then
	xml2:=get_parametros_motor(xml2,'ENVIO_EDTE');
   else
	xml2:=get_parametros_motor(xml2,'EDTE_CGE');	
   end if;

    --xml2:=put_campo(xml2,'ALMACEN','/opt/acepta/enviodte/work/sii/dte/escribiendo_motor/'||file1);
    xml2:=put_campo(xml2,'ALMACEN',get_campo('PARAMETRO_RUTA',xml2)||'/escribiendo_motor/'||file1);
    xml2:=logapp(xml2,'EDTE: '||get_campo('ALMACEN',xml2));

    xml2:=put_campo(xml2,'SCRIPT_EDTE','/bin/mv '||get_campo('PARAMETRO_RUTA',xml2)||'/escribiendo_motor/'||file1||' '||get_campo('PARAMETRO_RUTA',xml2)||'/pendiente/'||file1);

    --Voy siempre a la IP del EDTE
    --xml2:=put_campo(xml2,'__IP_CONEXION_CLIENTE__','192.168.3.32');
    xml2:=logapp(xml2,'EDTE: Envia Data Directo '||get_campo('__IP_CONEXION_CLIENTE__',xml2)||' '||uri1);
    xml2:=put_campo(xml2,'__SECUENCIAOK__','50');

    if get_campo('__COLA_MOTOR__',xml2)='cola_motor_x' then
	xml2:=put_campo(xml2,'__IP_PORT_CLIENTE__','8033');
   end if;

    --Limpia el Status antes de ir al almacen	
    xml2 := put_campo(xml2,'_STS_FILE_','');

  /*  Si la URI es
    if (get_campo('URI_IN',xml2)='http://rtc1512.acepta.com/v01/CC94FADB9FB7A17632F60227E5A74801BA444F96?k=00b81398a69e71181cd4ee13c80fa3df') then
    		--Armo paquete para ir directo al edte
		xml2:=put_campo(xml2,'INPUT_CUSTODIUM','02'||encode('<TX=4>9015<INPUT='::bytea,'hex')||encode(((get_campo('LEN_INPUT_CUSTODIUM',xml2)::integer/2)::varchar||'>')::bytea,'hex')||get_campo('INPUT_CUSTODIUM',xml2)||encode(('<ALMACEN='||length(get_campo('ALMACEN',xml2))::varchar||'>'||get_campo('ALMACEN',xml2))::bytea,'hex')||'03');
        	xml2:=put_campo(xml2,'__SECUENCIAOK__','55');
    end if;
*/

    return xml2;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION proc_respuesta_edte_12702(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2        varchar;
        data1   varchar;
	sts1	varchar;
	publicado1	varchar;
	xml3	varchar;
	nombre_tabla1	varchar;
	cola1	bigint;	
	id1	bigint;
BEGIN
        --Cambio la respuesta de cuadratura por la respuesta original
        xml2:=xml1;
       	xml2 := put_campo(xml2,'__EDTE_OK__','NO');
	xml2:=logapp(xml2,'EDTE: Respuesta System='||get_campo('RESPUESTA_SYSTEM',xml2));
	sts1:=get_campo('_STS_FILE_',xml2);
	if (sts1='FILE_YA_EXISTE') then
		xml2 := logapp(xml2,'EDTE:File ya existe en EDTE');	
        	xml2 := put_campo(xml2,'__EDTE_OK__','SI');
		xml2 := graba_bitacora(xml2,'ENVIO_DIRECTO_EDTE*');
	elsif (sts1='OK') then
                xml2 := logapp(xml2,'EDTE:OK Escritos '||get_campo('_STS_FILE_BYTES_WRITTEN_',xml2)||' ContentLength:'||get_campo('CONTENT_LENGTH',xml2)||' Largo Data:'||get_campo('LEN_INPUT_CUSTODIUM',xml2));
        	xml2 := put_campo(xml2,'__EDTE_OK__','SI');
		xml2 := graba_bitacora(xml2,'ENVIO_DIRECTO_EDTE');
	else
               	xml2 := logapp(xml2,'EDTE:Falla EDTE Directo '||get_campo('_STS_FILE_',xml2));
		--20171214 FAY_DAOSi es una Factura de escritorio10k siempre responde OK
		if (get_campo('__FLAG_PUB_10K__',xml2)='SI') then
			--Insertamos en las colas para reprocesar el envio al EDTE solo ejecutando este flujo
			xml3:=xml2;
			xml3:=put_campo(xml3,'TX','112702');
			cola1:=nextval('id_cola_procesamiento');
       			nombre_tabla1:='cola_motor_'||cola1::varchar;
		        execute 'insert into ' || nombre_tabla1 || ' (fecha,uri,reintentos,data,tx,rut_emisor,reproceso,categoria) values ( now(),'||quote_literal(get_campo('URI_IN',xml2))||',0,'||quote_literal(xml3)||',10,'||quote_literal(get_campo('RUT_EMISOR',xml2))||',''NO'',''REENVIO_EDTE_10K'') returning id ' into id1;
			xml2 := put_campo(xml2,'__EDTE_OK__','SI');	
			xml2 := logapp(xml2,'EDTE:Se graba en las colas REENVIO_EDTE_10K ID='||id1::varchar);
			xml2 := graba_bitacora(xml2,'FALLA_EDTE_GRABA_COLAS');
		else
        		xml2 := put_campo(xml2,'__EDTE_OK__','NO');
			xml2 := graba_bitacora(xml2,'FALLA_ENVIO_DIRECTO_EDTE');
		end if;
        end if;
	xml2 := put_campo(xml2,'_STS_FILE_','');
        xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
	--PAra que la respuesta se procese en proc_procesa_respuesta_dte del 8010
	if (get_campo('__EDTE_OK__',xml2)='SI') then
		xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK'||chr(10)||chr(10)||'URL(True): '||get_campo('URI_IN',xml2));
	else
		--Si falla el envio ponemos reintentos 10 y esperamos por si no fallo el envio al EDTE y que no se repita
		xml2:=put_campo(xml2,'RESPUESTA','Status: 444 NK');
		xml2:=put_campo(xml2,'MENSAJE_XML_FLAGS','Falla envio al EDTE');
	end if;
        return xml2;
END;
$$ LANGUAGE plpgsql;

