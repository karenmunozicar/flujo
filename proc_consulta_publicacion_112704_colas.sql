CREATE OR REPLACE FUNCTION public.proc_consulta_publicacion_112704_colas(character varying)
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
	rut_emisor1	varchar;
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
    uri1:=get_campo('URI_IN',xml2);

    xml2:=logapp(xml2,'URI_IN='||uri1);
	

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

    xml2 := put_campo(xml2,'__FLAG_CLIENTE_COMUNIDAD__','');

    --Saco los parametros del publicador para usarlos posteriormente.
    xml2:=logapp(xml2,'DTE no publicado (o Recibido) URI_IN='||get_campo('URI_IN',xml2)||' XML_FLAGS='||get_campo('XML_FLAGS',xml2)||' RUT_EMISOR='||get_campo('RUT_EMISOR',xml2)||' TIPO_DTE='||get_campo('TIPO_DTE',xml2));
    if (get_fecha_uri(uri1)::integer>=1701) then
    	xml2:=get_parametros_motor(xml2,'PUBLICADOR_2017');
    else
    	xml2:=get_parametros_motor(xml2,'PUBLICADOR');
    end if;

    xml2 := put_campo(xml2,'__SECUENCIAOK__','40');

    --FAY-DAO 2018-03-12 Solo para Emitidos que no esten retenido previamente
    --FAY-DAO 20200422 si la emision es por escritorio, no se aplican las reglas de PRE-EMISION
    if (strpos(uri1,'?k=')>0 and strpos(get_campo('XML_FLAGS',xml2),'RETENIDO-')=0 and get_campo('__FLAG_PUB_10K__',xml2)<>'SI') then
	--Si tiene el rut controller pre-emision se valida aca
	rut_emisor1:=get_campo('RUT_EMISOR',xml2);
	--DAO 20190712 Agregamos que el Tipo Dte sea numerico
	if is_number(rut_emisor1) and is_number(get_campo('TIPO_DTE',xml2))then
		select * into campo from dominios_maestro_clientes where rut_emisor=rut_emisor1::integer and pre_emision='SI';
		if found then
			xml2:=logapp(xml2,'Vamos a Revisar Controller PRE-EMISION '||uri1);
			xml2 := put_campo(xml2,'__SECUENCIAOK__','30');
			return xml2;
		end if;
	end if;
    end if; 

    xml2:=proc_prepara_graba_directo_almacen_colas_112704(xml2);
    return xml2;
END;    
$function$
