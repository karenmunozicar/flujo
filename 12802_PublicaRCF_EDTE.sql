--Publica documento
delete from isys_querys_tx where llave='12802';

--Llamamos a Escribir Directo
insert into isys_querys_tx values ('12802',40,8021,1,'select proc_prepara_grabacion_edte_12802(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('12802',50,1,3,'Llamada a Escribir en EDTE',8016,0,0,0,0,60,60);
insert into isys_querys_tx values ('12802',60,8021,1,'select proc_respuesta_edte_12802(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('12802',1000,19,1,'select sp_procesa_respuesta_cola_motor_original(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,0,0);

CREATE or replace FUNCTION proc_prepara_grabacion_edte_12802(varchar) RETURNS varchar AS $$
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
	id1	varchar;
   	aux	varchar;
	rut_emisor1 varchar; 

	dia_emision1	integer;
	fecha_ingreso1	timestamp;
	dia1	integer;
	digest1	varchar;
	input1	varchar;
BEGIN
    xml2:=xml1; 
    xml2:=put_campo(xml2,'__SECUENCIAOK__','0');

    	data1:=get_campo('INPUT',xml2);
	input1:=decode(data1,'hex');
	rut_emisor1:=split_part(get_xml('RutEmisor'::varchar,input1),'-',1);
	--FAY 2019-10-30 si no viene FchInicio no es un rcf valido
	if (is_number(replace(get_xml('FchInicio',input1),'-','')) is false) then
		xml2 := logapp(xml2,'RCF invalido no viene FchInicio valida '||get_xml('FchInicio',input1)||' '||get_campo('URI_IN',xml2)||' Se borra');
		xml2 := put_campo(xml2,'RESPUESTA','Status: 200 OK');
        	xml2 := put_campo(xml2,'__SECUENCIAOK__','1000');
		return xml2;
	end if;


	dia_emision1:=replace(get_xml('FchInicio',input1),'-','')::integer;
	fecha_ingreso1:=now();
	dia1:=to_char(fecha_ingreso1,'YYYYMMDD')::integer;
	digest1:=get_xml('DigestValue',input1);
        uri1:=get_campo('URI_IN',xml2);

	--Insertamos
	insert into rcf_data (rut_emisor,dia_emision,uri,fecha_ingreso,dia,digest,secuencial) values (rut_emisor1::bigint,dia_emision1,uri1,fecha_ingreso1,dia1,digest1,coalesce(nullif(get_xml('SecEnvio',input1),''),'1')::integer);
	--FAY-DAO 2020-02-27 ya no se necesita grabar en uri_key2
	--insert into uri_key2 (fecha,uri,key,canal) values (now(),uri1,digest1,'E');

	xml2:=put_campo(xml2,'RUT_EMISOR',rut_emisor1);	
	xml2:=put_campo(xml2,'FECHA_EMISION',dia1::varchar);
	xml2:=put_campo(xml2,'TIPO_DTE','RCF');
	xml2:=put_campo(xml2,'FOLIO',dia_emision1::varchar);
	xml2:=put_campo(xml2,'RUT_RECEPTOR','66666666');
	xml2:=put_campo(xml2,'EVENTO','FRM');
	xml2 := graba_bitacora(xml2,'FRM');


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
                xml2 := put_campo_ctx(xml2,'__ETAPA1__','OK');
		xml2 := put_campo(xml2,'RESPUESTA','Status: 400 NK');
        	xml2 := put_campo(xml2,'__SECUENCIAOK__','1000');
                return xml2;
        end if;
    end if;


    --20150224 FAY Si no viene URI no se puede publicar
    if (length(uri1)=0) then
	xml2 := logapp(xml2,'No viene URI_IN, no se puede publicar');
        xml2 := put_campo(xml2,'__EDTE_AERCF_OK__','NO');
	xml2 := put_campo(xml2,'RESPUESTA','Status: 400 NK');
        xml2 := put_campo(xml2,'__SECUENCIAOK__','1000');
	return xml2;	
    end if;
   
    --rut emisor
    xml2 := put_campo(xml2,'RUT_EMISOR',split_part(get_xml('RutEmisor'::varchar,decode(data1,'hex')::varchar),'-',1));
    --Si ya tiene el evento ENVIADO_EDTE_AERCF, no envio el mandato
    tabla_traza1:=get_tabla_traza(uri1);
    --begin
               execute 'select * from '||tabla_traza1||' where uri=$1 and evento=''ENVIADO_EDTE_AERCF''' into stTraza using uri1;
               --Si no esta el evento..
               if stTraza.uri is not null then
        		xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
        		xml2 := put_campo(xml2,'__EDTE_AERCF_OK__','OK');
			xml2 := logapp(xml2,'Uri '||uri1||' AERCF ya enviado al EDTE');
			xml2 := put_campo(xml2,'RESPUESTA','Status: 200 OK');
        		xml2 := put_campo(xml2,'__SECUENCIAOK__','1000');
			--xml2 := sp_procesa_respuesta_cola_motor(xml2);
		        return xml2;
               end if;
    xml2 := put_campo(xml2,'TX','8016'); 

    --Si ya vengo de publicar el Documento y tengo el INPUT_CUSTODIUM, no lo vuelvo a parsear
    if (get_campo('INPUT_CUSTODIUM',xml2)='') then
	    --Ya se parseo el DTE
	    host1:=split_part(split_part(uri1,'//',2),'/',1);
 
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

    file1:=replace(replace(replace(replace(uri1,':','%3A'),'/','%2F'),'?','%3F'),'=','%3D');
    xml2:=put_campo(xml2,'ALMACEN','/opt/acepta/enviodte/work/sii/aercf/escribiendo_motor/'||file1);
    xml2:=logapp(xml2,'EDTE AERCF: '||get_campo('ALMACEN',xml2));




    xml2:=put_campo(xml2,'SCRIPT_EDTE','/bin/mv /opt/acepta/enviodte/work/sii/aercf/escribiendo_motor/'||file1||' /opt/acepta/enviodte/work/sii/aercf/pendiente/'||file1);
    --xml2:=put_campo(xml2,'SCRIPT_EDTE','echo 1');

    xml2:=logapp(xml2,'EDTE RCF Script:'||get_campo('SCRIPT_EDTE',xml2));
    --Voy siempre a la IP del EDTE
    --if (get_campo('MODO_QA',xml2)='ON') then
   
     --Si es CGE
    xml2:=logapp(xml2,'rut_emisor rcf '||get_campo('RUT_EMISOR',xml2)||'  uri  '||uri1 );
    xml2:=put_campo(xml2,'RUT_CGE',get_campo('RUT_EMISOR',xml2));
    xml2:=verifica_evento_cge(xml2);

    if (get_campo('EVENTO_CGE',xml2)='SI') then
    	xml2:=put_campo(xml2,'__IP_CONEXION_CLIENTE__','172.16.10.181');
    	xml2:=put_campo(xml2,'__IP_PORT_CLIENTE__','8015');
    else
    	xml2:=put_campo(xml2,'__IP_CONEXION_CLIENTE__',get_campo('__IP_CONEXION_CLIENTE__',get_parametros_motor('','EDTE')));
    	xml2:=put_campo(xml2,'__IP_PORT_CLIENTE__','8015');
    end if;
    xml2:=logapp(xml2,'EDTE AERCF: Envia Data Directo '||get_campo('__IP_CONEXION_CLIENTE__',xml2)||' '||uri1);
    xml2:=put_campo(xml2,'__SECUENCIAOK__','50');
    xml2 := put_campo(xml2,'_STS_FILE_','');
    xml2 := put_campo(xml2,'__EDTE_AERCF_OK__','NK');
    return xml2;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION proc_respuesta_edte_12802(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2        varchar;
    data1       varchar;
    sts1	varchar;
    publicado1	varchar;
    xml3	varchar;
    cola1       bigint;
    nombre_tabla1   varchar;
    uri1    varchar;
    rut1    varchar;
    tx1     varchar;
    id1     varchar;
    codigo1 varchar;


BEGIN
        --Cambio la respuesta de cuadratura por la respuesta original
        xml2:=xml1;
	sts1:=get_campo('_STS_FILE_',xml2);
	if (sts1='FILE_YA_EXISTE') then
		xml2 := logapp(xml2,'EDTE AERCF:File ya existe en EDTE');	
    		xml2 := put_campo(xml2,'__EDTE_AERCF_OK__','OK');
		xml2 := graba_bitacora(xml2,'ENVIADO_EDTE_AERCF');
	--	codigo1:=get_campo('CODIGO_TXEL_IECV',xml2);
	elsif (sts1='OK') then
                xml2 := logapp(xml2,'EDTE:OK Escritos '||get_campo('_STS_FILE_BYTES_WRITTEN_',xml2)||' ContentLength:'||get_campo('CONTENT_LENGTH',xml2)||' Largo Data:'||get_campo('LEN_INPUT_CUSTODIUM',xml2));
    		xml2 := put_campo(xml2,'__EDTE_AERCF_OK__','OK');
		xml2 := graba_bitacora(xml2,'ENVIADO_EDTE_AERCF');
	else
                xml2 := logapp(xml2,'EDTE AERCF:Falla EDTE Directo '||get_campo('_STS_FILE_',xml2));
    		xml2 := put_campo(xml2,'__EDTE_AERCF_OK__','NK');
		xml2 := graba_bitacora(xml2,'FALLA_ENVIADO_EDTE_AERCF');
        end if;
        xml2 := put_campo(xml2,'_STS_FILE_','');

	--Si falla, ponemos el mensaje en la cola para procesamiento posterior
	if (get_campo('__EDTE_AERCF_OK__',xml2)<>'OK') then
		xml2 := put_campo(xml2,'RESPUESTA','Status: 400 NK');
	--Envio consumo folios exitoso
	else
		xml2:=logapp(xml2,'Se borra AERCF edte de la cola');
		xml2 := put_campo(xml2,'RESPUESTA','Status: 200 OK');
	end if;
        xml2 := put_campo(xml2,'__SECUENCIAOK__','1000');
        return xml2;
END;
$$ LANGUAGE plpgsql;

