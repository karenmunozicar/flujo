CREATE OR REPLACE FUNCTION public.proc_consulta_publicacion_112704(character varying)
 RETURNS character varying
 LANGUAGE plpgsql
AS $function$
DECLARE
    xml1        alias for $1;
        xml2    varchar;
    data1       varchar;
    file1       varchar;
    sts         integer;
    host1       varchar;
    header1     varchar;
   largo1       integer;
    pos_final1 integer;
    pos_inicial1 integer;
    dominio1 varchar;
fecha1  varchar;
directorio1 varchar;
tabla_traza1    varchar;
uri1    varchar;
stTraza traza.traza%ROWTYPE;
	rut_emisor1	bigint;
	tipo_dte1	bigint;
	folio1		bigint;
	monto1		bigint;
	campo		RECORD;
	i1	integer;
	xml3	varchar;
	json_aux1	json;
	rut1		varchar;
BEGIN
    xml2:=xml1;
    xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
    xml2:=logapp(xml2,'URI_IN='||get_campo('URI_IN',xml2));

    --Si es un get salgo altiro
    if (get_campo('REQUEST_METHOD',xml2)='GET') then
        if ((strpos(get_campo('HTTP_USER_AGENT',xml2),'check_http')>0) or (length(get_campo('QUERY_STRING',xml2))=0)) then
                xml2 := logapp(xml2,'Nagios Check o GET sin datos, se ignora');
                --Se Responde OK
                xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK'||chr(10)||
                'Content-type: text/html; charset=iso-8859-1'||chr(10)||
                'Content-length: 0'||chr(10)||
                'Vary: Accept-Encoding'||chr(10)||chr(10));
        	xml2 := put_campo(xml2,'__PUBLICADO_OK__','FALLA');
                --xml2 := sp_procesa_respuesta_cola_motor(xml2);
                --xml2 := put_campo_ctx(xml2,'__ETAPA1__','OK');
                return xml2;
        end if;
    end if;
    uri1:=get_campo('URI_IN',xml2);

    --20150224 FAY Si no viene URI no se puede publicar
    if (length(uri1)=0) then
        xml2 := logapp(xml2,'No viene URI_IN, no se puede publicar');
        xml2 := put_campo(xml2,'__PUBLICADO_OK__','FALLA');
        return xml2;
    end if;


    --FAY 2015-03-26 Solo para EMITIDOS
    if (strpos(uri1,'?k=')>0) then
	    fecha1:=get_fecha_uri(uri1);
	    --Verifico sie debo buscar en trazas antiguas
            tabla_traza1:=get_tabla_traza(uri1);
	    select * into campo from config_tabla_traza where periodo_desde<=fecha1::integer and periodo_hasta>=fecha1::integer;
	    --Si debo hacer lo mismo
	    if (found and campo.parametro is null) then
		   if get_campo('FLAG_FILE_CUS',xml2)='NO' then
			xml2 := logapp(xml2,'Uri '||uri1||' no se lee la traza');
		   else
 	            --Si ya tiene el evento PUB en traza, no se publica
	            begin
        	       execute 'select * from '||tabla_traza1||' where uri=$1 and evento=''PUB''' into stTraza using uri1;
	               --Si no esta el evento..
        	       if stTraza.uri is not null then
                	        xml2 := put_campo(xml2,'__PUBLICADO_OK__','SI');
                        	xml2 := logapp(xml2,'Uri '||uri1||' ya publicado');
	                        return xml2;
        	       end if;
	            exception WHEN OTHERS THEN
        	       select * into stTraza from traza.traza where uri=uri1 and evento='PUB';
	               if found then
        	                --Si no esta el evento PUB vamos a publicar
                        	xml2 := put_campo(xml2,'__PUBLICADO_OK__','SI');
                	        xml2 := logapp(xml2,'Uri '||uri1||' ya publicado*');
                        	return xml2;
	               end if;
        	    end;
		   end if;
	     --Si es de una traza antigua >=2014
	     elsif is_number(split_part(campo.parametro,'_',2)) then
		xml2:=put_campo(xml2,'TABLA_TRAZA',tabla_traza1);
		xml2:=put_campo(xml2,'__SECUENCIAOK__',split_part(campo.parametro,'_',2));
		xml2 := logapp(xml2,'URI='||uri1||' Valida proxy en Amazon');
		return xml2;	
	     else
		xml2 := logapp(xml2,'lee_traza_evento');
		json_aux1:=lee_traza_evento(uri1,'PUB');
                if (get_json('status',json_aux1)='OK') then
                --if (json_aux1::varchar<>'{}') then
                       xml2 := put_campo(xml2,'__PUBLICADO_OK__','SI');
                       xml2 := logapp(xml2,'Uri '||uri1||' ya publicado');
                       return xml2;
                end if;
	     end if;
    end if;

    return proc_consulta_publicacion_112704_2(xml2);
END;
$function$
