--Publica documento
delete from isys_querys_tx where llave='12781';

--Chequea si tiene que ir al SII
insert into isys_querys_tx values ('12781',10,1,1,'select check_si_sii_12781(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);
--Llama al Flujo de Reclamo
insert into isys_querys_tx values ('12781',12,1,8,'Llamada Reclamo',16201,0,0,0,0,14,14);
--Valida respuesta sii
insert into isys_querys_tx values ('12781',14,1,1,'select valida_respuesta_sii_12781(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

--Llamamos a Escribir Directo
insert into isys_querys_tx values ('12781',15,1,1,'select proc_prepara_grabacion_edte_12781(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('12781',50,1,3,'Llamada a Escribir en EDTE',8016,0,0,0,0,60,60);
insert into isys_querys_tx values ('12781',60,1,1,'select proc_respuesta_edte_12781(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

CREATE or replace FUNCTION valida_respuesta_sii_12781(json) RETURNS json AS $$
DECLARE
        json1   alias for $1;
        json2   json;
        resp1   varchar;
        json_out        json;
        evento1 varchar;
        v_tipo_dte      varchar;
        v_folio         varchar;
        v_rutEmisor     varchar;
        campoE  record;
        campo1  record;
        ws_codResp      varchar;
        ws_descResp     varchar;
        xml3    varchar;
        v_cod_txel      bigint;
BEGIN
        json2:=json1;
        json2:=put_json(json2,'__SECUENCIAOK__','0');

	if get_json('RESPUESTA',json2)<>'Status: 200 OK' then
                json2:=put_json(json2,'__EDTE_ARM_OK__','NK');
                json2:=put_json(json2,'__MENSAJE_10K__','Falla Comunicación SII');
                return json2;
        end if;
	json2:=logjson(json2,'__ESTADO_RECLAMO__='||get_json('__ESTADO_RECLAMO__',json2));
	if get_json('__ESTADO_RECLAMO__',json2)='SI' then
                json2:=put_json(json2,'__SECUENCIAOK__','15');
	elsif get_json('__ESTADO_RECLAMO__',json2)='BORRAR' then
                json2:=put_json(json2,'__EDTE_ARM_OK__','BORRAR');
	else
                json2:=put_json(json2,'__EDTE_ARM_OK__','NK');
	end if; 

        return json2;
END;$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION check_si_sii_12781(json) RETURNS json AS $$
DECLARE
	json1	alias for $1;
	json2	json;
	data1	varchar;
	flag_reclamo	varchar;
	input1	varchar;
	tipo_dte1	varchar;
	folio1		varchar;
	rut1		varchar;
	json_in		json;
	evento1		varchar;	
	port varchar;
BEGIN
	json2:=json1;
	--Solo ARM
	if (get_json('SOLO_ARM',json2)='SI') then
		json2:=logjson(json2,'Solo se envia el ARM');	
		--Vamos solo a enviar el ARM
		json2:=put_json(json2,'__SECUENCIAOK__','15');	
		return json2;
	end if;
	
	data1:=get_json('INPUT',json2);
	
	flag_reclamo:='NO';
	input1:=decode(data1,'hex');
	if strpos(input1,'<NombreDA>ReclamarDTE</NombreDA>')>0 then
                flag_reclamo:=split_part(split_part(split_part(input1,'<NombreDA>ReclamarDTE</NombreDA>',2),'<ValorDA>',2),'</ValorDA>',1);
	end if;
	json2:=put_json(json2,'FLAG_RECLAMO',flag_reclamo);

	--DAO-2017-11-20 Si no es un DTE reclamable en el SII, solo hace el ARM
        if(flag_reclamo='NO' or flag_reclamo='' or get_json('TIPO_DTE',json2) not in ('33','34','43')) then
		json2:=put_json(json2,'__SECUENCIAOK__','15');	
                json2:=logjson(json2,'ARM NO va al SII');
                return json2;
        end if;
        json2:=logjson(json2,'ARM SI va al SII');
	evento1:='ERM';
        json2:=put_json(json2,'EVENTO_RECLAMO',evento1);
        json2:=put_json(json2,'__SECUENCIAOK__','12');
        json2:=put_json(json2,'__FLAG_16210__','NO_BORRAR');
        return json2;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION proc_prepara_grabacion_edte_12781(varchar) RETURNS varchar AS $$
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
        xml2 := put_campo(xml2,'__EDTE_OK__','NO');
	return xml2;	
    end if;


    --Si ya tiene el evento EMA, no envio el mandato
    tabla_traza1:=get_tabla_traza(uri1);
    begin
               execute 'select * from '||tabla_traza1||' where uri=$1 and evento=''ENVIO_ARM_DIRECTO_EDTE''' into stTraza using uri1;
               --Si no esta el evento..
               if stTraza.uri is not null then
        		xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
                        xml2 := put_campo(xml2,'__EDTE_ARM_OK__','SI');
			xml2 := logapp(xml2,'Uri '||uri1||' ARM ya enviado al EDTE');
			--Si ya existe el envio de mandato
                	if (get_campo('__FLAG_REINTENTO_ARM__',xml2)='SI') then
                        	id1:=get_campo('__ID_DTE__',xml2);
	                        --Si viene de un reintento, aumento reintentos
        	                xml2:=logapp(xml2,'Se borra mandato edte de la cola');
                	        execute 'delete from '||get_campo('__COLA_MOTOR__',xml2)||' where id='||id1;
	                end if;

		        return xml2;
               end if;
    exception WHEN OTHERS THEN
               select * into stTraza from traza.traza where uri=uri1 and evento='ENVIADO_EDTE_ARM';
               if not found then
                        --Si no esta el evento PUB vamos a publicar
        		xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
                        xml2 := put_campo(xml2,'__EDTE_ARM_OK__','SI');
			xml2 := logapp(xml2,'Uri '||uri1||' ARM ya enviado al EDTE*');
		        return xml2;
               end if;
    end;
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
    xml2:=put_campo(xml2,'ALMACEN','/opt/acepta/enviodte/work/inter/enviorecibos/escribiendo_motor/'||file1);
    xml2:=logapp(xml2,'EDTE ARM: '||get_campo('ALMACEN',xml2));

    xml2:=put_campo(xml2,'SCRIPT_EDTE','/bin/mv /opt/acepta/enviodte/work/inter/enviorecibos/escribiendo_motor/'||file1||' /opt/acepta/enviodte/work/inter/enviorecibos/pendiente/'||file1);
    --xml2:=put_campo(xml2,'SCRIPT_EDTE','echo 1');

    xml2:=logapp(xml2,'EDTE ARM Script:'||get_campo('SCRIPT_EDTE',xml2));
    --Voy siempre a la IP del EDTE
    
    --Si no viene especificado
    if (get_campo('__IP_CONEXION_CLIENTE__',xml2)='') then
	    xml2:=put_campo(xml2,'__IP_CONEXION_CLIENTE__',get_campo('__IP_CONEXION_CLIENTE__',get_parametros_motor('','EDTE')));
    end if;
    xml2:=logapp(xml2,'EDTE ARM: Envia Data Directo '||get_campo('__IP_CONEXION_CLIENTE__',xml2)||' '||uri1);
    xml2:=put_campo(xml2,'__SECUENCIAOK__','50');
    xml2 := put_campo(xml2,'_STS_FILE_','');
    return xml2;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION proc_respuesta_edte_12781(varchar) RETURNS varchar AS $$
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
	codigo1	varchar;
	id1	varchar;
	tipo_dte1	integer;
	fecha_emision1	varchar;
	folio1	bigint;
	folio2	varchar;
	monto_total1 bigint;
	rut_emisor1	integer;
	rut_receptor1	integer;
	dia1	varchar;
	tipo1	varchar;

BEGIN
        --Cambio la respuesta de cuadratura por la respuesta original
        xml2:=xml1;
       	xml2 := put_campo(xml2,'__EDTE_ARM_OK__','NO');
	sts1:=get_campo('_STS_FILE_',xml2);
	if (sts1='FILE_YA_EXISTE') then
		xml2 := logapp(xml2,'EDTE ARM:File ya existe en EDTE');	
        	xml2 := put_campo(xml2,'__EDTE_ARM_OK__','SI');
		xml2 := graba_bitacora(xml2,'ENVIO_ARM_DIRECTO_EDTE');
		--Actulizo la tabla de recibidos para marcar el ARM
		codigo1:=get_campo('CODIGO_TXEL_ARM',xml2);
		rut1:=get_campo('RUT_EMISOR_ARM',xml2);
		tipo1:=get_campo('TIPO_DTE_ARM',xml2);
		folio2:=get_campo('FOLIO_ARM',xml2);

		begin
			update dte_recibidos set estado_arm='SI',uri_arm=get_campo('URI_IN',xml2),fecha_arm=now(),mensaje_arm=get_campo('RECINTO',xml2) where rut_emisor=rut1::bigint and tipo_dte=tipo1::integer and folio=folio2::bigint;
		EXCEPTION WHEN OTHERS THEN
			xml2:=logapp(xml2,'EDTE ARM: FALLA  Update dte_recibidos uri_arm='||get_campo('URI_IN',xml2)||' CodigoTxel='||codigo1||' rut_emisor='||rut1||' tipo_dte='||tipo1||' folio='||folio2);
			
        		xml2 := put_campo(xml2,'__EDTE_ARM_OK__','NO');
			return xml2;
		end;
		--codigo_txel=codigo1::bigint;
		--Agregamos la estadistica de ARM
		xml2:=put_campo(xml2,'CANAL','RECIBIDOS');
		xml2:=put_campo(xml2,'FECHA_PUBLICACION',to_char(now(),'YYYYMMDD')::varchar);
	elsif (sts1='OK') then
                xml2 := logapp(xml2,'EDTE ARM:OK Escritos '||get_campo('_STS_FILE_BYTES_WRITTEN_',xml2)||' ContentLength:'||get_campo('CONTENT_LENGTH',xml2)||' Largo Data:'||get_campo('LEN_INPUT_CUSTODIUM',xml2));
        	xml2 := put_campo(xml2,'__EDTE_ARM_OK__','SI');
		xml2 := graba_bitacora(xml2,'ENVIO_ARM_DIRECTO_EDTE');
		--Actulizo la tabla de recibidos para marcar el ARM
		codigo1:=get_campo('CODIGO_TXEL_ARM',xml2);
		rut1:=get_campo('RUT_EMISOR_ARM',xml2);
		tipo1:=get_campo('TIPO_DTE_ARM',xml2);
		folio2:=get_campo('FOLIO_ARM',xml2);
		xml2:=logapp(xml2,'EDTE ARM: Update dte_recibidos uri_arm='||get_campo('URI_IN',xml2)||' CodigoTxel='||codigo1||' rut_emisor='||rut1||' tipo_dte='||tipo1||' folio='||folio2);
		begin
			update dte_recibidos set estado_arm='SI',uri_arm=get_campo('URI_IN',xml2),fecha_arm=now(),mensaje_arm=get_campo('RECINTO',xml2) where rut_emisor=rut1::bigint and tipo_dte=tipo1::integer and folio=folio2::bigint and uri_arm is null returning tipo_dte,fecha_emision,folio,monto_total,rut_emisor,rut_receptor,uri,dia into tipo_dte1,fecha_emision1,folio1,monto_total1,rut_emisor1,rut_receptor1,uri1,dia1;
			xml2:=logapp(xml2,'EDTE ARM: Paso 1');
		EXCEPTION WHEN OTHERS THEN
			xml2:=logapp(xml2,'EDTE ARM: FALLA  Update dte_recibidos uri_arm='||get_campo('URI_IN',xml2)||' CodigoTxel='||codigo1||' rut_emisor='||rut1||' tipo_dte='||tipo1||' folio='||folio2);
			
        		xml2 := put_campo(xml2,'__EDTE_ARM_OK__','NO');
			return xml2;
		end;
	else
                xml2 := logapp(xml2,'EDTE ARM:Falla EDTE Directo '||get_campo('_STS_FILE_',xml2));
        	xml2 := put_campo(xml2,'__EDTE_ARM_OK__','NO');
		xml2 := graba_bitacora(xml2,'FALLA_ENVIO_ARM_DIRECTO_EDTE');
        end if;
        xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
        xml2 := put_campo(xml2,'_STS_FILE_','');

        return xml2;
END;
$$ LANGUAGE plpgsql;

