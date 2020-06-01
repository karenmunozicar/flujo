--Consulta Estado Reclamo SII
delete from isys_querys_tx where llave='16103';

insert into isys_querys_tx values ('16103',10,45,1,'select armo_consulta_sii_16103(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('16103',20,1,2,'Servicio de SII 172.16.14.88',4013,100,101,0,0,30,30);
insert into isys_querys_tx values ('16103',30,45,1,'select proceso_respuesta_sii_16103(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('16103',1000,19,1,'select sp_procesa_respuesta_cola_motor88_json(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,0,0);

CREATE or replace FUNCTION armo_consulta_sii_16103(varchar) RETURNS varchar AS $$
DECLARE
        xml1   alias for $1;
        xml2   varchar;

        json_in json;
	port	varchar;
	campo	record;
	uri1	varchar;
BEGIN
	xml2:=xml1;

	--Consultamos si el dte_recibido ya tiene la fecha
	uri1:=get_campo('URI_IN',xml2);
	select * into campo from dte_recibidos where uri=uri1 and fecha_recepcion_sii is not null;
	if found then
		xml2:=logapp(xml2,'Dte Recibido '||uri1||' ya tiene fecha del sii');
		xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
		xml2 := put_campo(xml2,'__SECUENCIAOK__','1000');
                return xml2;
	end if;
	

	json_in:='{"rutEmisor":"'||get_campo('RUT_EMISOR',xml2)||'","dvEmisor":"'||modulo11(get_campo('RUT_EMISOR',xml2))||'","tipoDoc":"'||get_campo('TIPO_DTE',xml2)||'","folio":"'||get_campo('FOLIO',xml2)||'","RUT_OWNER":"'||get_campo('RUT_RECEPTOR',xml2)||'"}';
	
	--port:=nextval('correlativo_servicio_sii')::varchar;
/*
	port:=get_ipport_sii();
       if (port='') then
              --Si no hay puertos libres...
               xml2:=logapp(xml2,'No hay puertos libres');
               xml2:=put_campo(xml2,'RESPUESTA','Status: 400 NK');
               xml2 := put_campo(xml2,'__SECUENCIAOK__','1000');
               return xml2;
        end if;
*/


        --xml2:=get_parametros_motor(xml2,'SERVICIO_SII_JSON');
        --xml2:=put_campo(xml2,'__IP_PORT_CLIENTE__',port);
        --xml2:=put_campo(xml2,'IP_PORT_CLIENTE',port);
        xml2:=put_campo(xml2,'__IP_CONEXION_CLIENTE__','interno.acepta.com');
	xml2:=put_campo(xml2,'__IP_PORT_CLIENTE__','8080');
	/*
        xml2:=put_campo(xml2,'__IP_CONEXION_CLIENTE__',split_part(port,':',1));
        xml2:=put_campo(xml2,'__IP_PORT_CLIENTE__',split_part(port,':',2));
        xml2:=put_campo(xml2,'IPPORT_SII',port);
	*/

	xml2:=logapp(xml2,'FECHA_SII: '||json_in::varchar||' RUT_RECEPTOR='||get_campo('RUT_RECEPTOR',xml2));

	xml2 := put_campo(xml2,'__SECUENCIAOK__','20');
        xml2:=put_campo(xml2,'INPUT','POST /sii/fecha_recepcion HTTP/1.1'||chr(10)||'User-Agent: curl/7.26.0'||chr(10)||'Host: '||get_campo('__IP_CONEXION_CLIENTE__',xml2)||':'||get_campo('__IP_PORT_CLIENTE__',xml2)||chr(10)||'Accept: '||chr(42)||'/'||chr(42)||chr(10)||'Content-Type: application/json; charset=ISO-8859-1'||chr(10)||'Content-Length: '||length(json_in::varchar)::varchar||chr(10)||chr(10)||json_in::varchar);
	return xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION proceso_respuesta_sii_16103(varchar) RETURNS varchar AS $$
DECLARE
        xml1   alias for $1;
        xml2 varchar;
	resp1	varchar;
	json_out	json;
	fecha1	varchar;
	fecha2	timestamp;
	tipo_dte1	integer;
	folio1	bigint;
	rut_emisor1	integer;
	campo record;

	rut_receptor1	integer;
	uri1		varchar;
	fecha_cola1	timestamp;
	codigo_txel1	bigint;
	referencias1	json;
	data_dte1	varchar;
	json_par1	json;
	json3		json;
	xml3		varchar;
	cola1		bigint;
	nombre_tabla1	varchar;
	query1		varchar;
	tx1	varchar;
	monto1	bigint;
	p1	varchar;
	p2	varchar;
	p3	varchar;
	p4	varchar;
	p5	varchar;
	envio_erp1	varchar;
	id_cola1	bigint;
        id1     bigint;
BEGIN
	xml2:=xml1;
	resp1:=get_campo('RESPUESTA',xml2);
	
	xml2 :=put_campo(xml2,'__SECUENCIAOK__','1000');	
	xml2:=logapp(xml2,'FECHA_SII:Respuesta SII '||replace(resp1,chr(10),'')||' RUT_RECEPTOR='||get_campo('RUT_RECEPTOR',xml2));
	
	if(strpos(resp1,'HTTP/1.0 200')=0 and strpos(resp1,'HTTP/1.1 200')=0) then
		--perform libera_ipport_sii(get_campo('IPPORT_SII',xml2),'FALLA');
                xml2:=put_campo(xml2,'RESPUESTA','Status: 400 NK');
		return xml2;
	end if;
	BEGIN
		json_out:=split_part(resp1,chr(10)||chr(10),2)::json;
	EXCEPTION WHEN OTHERS THEN
		xml2:=logapp(xml2,'Falla JSON');
		--perform libera_ipport_sii(get_campo('IPPORT_SII',xml2),'FALLA');
		xml2:=put_campo(xml2,'RESPUESTA','Status: 400 NK');
		return xml2;
	END;

	--perform libera_ipport_sii(get_campo('IPPORT_SII',xml2),'OK');
	if(get_json('fecha_sii',json_out)='SIN_FECHA') then
		xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
                xml2:=logapp(xml2,'FECHA_SII: Sin Fecha Recepcion SII');
                xml2:=put_campo(xml2,'URI_IN',get_campo('URI_IN',xml2));
                xml2:=put_campo(xml2,'CANAL','RECIBIDOS');
                xml2:=put_campo(xml2,'FECHA_EVENTO',now()::varchar);
                xml2:=put_campo(xml2,'EVENTO','FSII_ERROR');
                xml2:=put_campo(xml2,'COMENTARIO_TRAZA','El SII (consultarFechaRecepcionSii) no contesta la fecha de recepcion del Documento.');
                xml2:=graba_bitacora(xml2,'FSII_ERROR');
                return xml2;

		xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');	
		xml2:=logapp(xml2,'FECHA_SII: Sin Fecha Recepcion SII');
		return xml2;
	end if;

	fecha1:=get_json('fecha_sii',json_out);
	BEGIN
		fecha2:=to_timestamp(fecha1,'DD-MM-YYYY HH24:MI:SS');
	EXCEPTION WHEN OTHERS THEN
		xml2:=logapp(xml2,'FECHA_SII: Fecha Invalida');
                xml2:=put_campo(xml2,'RESPUESTA','Status: 400 NK');
		return xml2;
	END;
	folio1:=get_campo('FOLIO',xml2);
	tipo_dte1:=get_campo('TIPO_DTE',xml2);
	rut_emisor1:=get_campo('RUT_EMISOR',xml2);

	--update dte_recibidos set fecha_recepcion_sii=fecha2,dia_recepcion_sii=to_char(fecha2::timestamp,'YYYYMMDD')::integer,fecha_ult_modificacion=now() where rut_emisor=rut_emisor1 and tipo_dte=tipo_dte1 and folio=folio1 and fecha_recepcion_sii is null;


	--DAO-CONTROLLER
	update dte_recibidos set fecha_recepcion_sii=fecha2,dia_recepcion_sii=to_char(fecha2::timestamp,'YYYYMMDD')::integer,fecha_ult_modificacion=now() where rut_emisor=rut_emisor1 and tipo_dte=tipo_dte1 and folio=folio1 and fecha_recepcion_sii is null returning rut_receptor,uri,codigo_txel,data_dte,referencias,monto_total,parametro1,parametro2,parametro3,parametro4,parametro5,envio_erp into rut_receptor1,uri1,codigo_txel1,data_dte1,referencias1,monto1,p1,p2,p3,p4,p5,envio_erp1;
	if found then
		--DAO 20181112 Si llega la fecha de recepcion y tenemos encolado un envio al ERP, le bajamos los reintentos
		if envio_erp1='SI' then
			--update colas_motor_generica set reintentos=0 where uri=uri1 and categoria='REVISA_ENVIO_ERP' returning id into id_cola1;
			update colas_motor_generica set reintentos=0 where uri=uri1 and categoria='REVISA_ENVIO_ERP';
			--Se elimina el returning xq a veces traia mas de 1 y se caia
			if found then
				xml2:=logapp(xml2,'Se baja Reintentos a la Categoria REVISA_ENVIO_ERP ');
			end if;
		end if;

		--Se graba un evento con la fecha SII
		xml2:=put_campo(xml2,'URI_IN',uri1);
		xml2:=put_campo(xml2,'CANAL','RECIBIDOS');
		xml2:=put_campo(xml2,'FECHA_EVENTO',fecha2::varchar);
		xml2:=put_campo(xml2,'EVENTO','FSII');
		xml2:=put_campo(xml2,'COMENTARIO_TRAZA','Fecha Consultada en SII (consultarFechaRecepcionSii)');
		xml2:=graba_bitacora(xml2,'FSII');

		xml2:=logapp(xml2,'FECHA_SII: Fecha Actualizada');

		--DAO-CONTROLLER
		--DAO Revisamos si hay alguna regla para este cliente que incluya como filtro la fecha de recepcion
		xml2:=logapp(xml2,'Reviso en el Controller '||rut_receptor1::varchar);
		--DAO-FAY 2018-11-20 Se revisan todas las reglas que tengan ese filtro y se insertan tantas veces como reglas existan
		for campo in select * from controller_detalle_regla_10k where id_cabecera in (select id from controller_cabecera_regla_10k where canal='RECIBIDOS' and rut_empresa=rut_receptor1) and filtro_xml='FECHA_RECEPCION_SII' loop
		--select * into campo from controller_detalle_regla_10k where id_cabecera in (select id from controller_cabecera_regla_10k where canal='RECIBIDOS' and rut_empresa=rut_receptor1) and filtro_xml='FECHA_RECEPCION_SII';
			execute 'select '''||fecha2::varchar||'''::timestamp + interval '''||campo.valor::varchar||' days''' into fecha_cola1;
			xml2:=logapp(xml2,'FECHA_RECEPCION_SII_CONTROLLER');	
			--Insertamos para que se ejecute en el futuro
			xml3:='';
			xml3:=put_campo(xml3,'TX','6001');
			xml3:=put_campo(xml3,'_ORIGEN_CONTROLLER_','FECHA_SII');
			xml3:=put_campo(xml3,'tipo_tx','valida_reglas_cabecera_controller_6001');
			xml3:=put_campo(xml3,'URI_IN',uri1);
			xml3:=put_campo(xml3,'CANAL','RECIBIDOS');
			xml3:=put_campo(xml3,'RUT_EMISOR',rut_emisor1::varchar);
			xml3:=put_campo(xml3,'RUT_RECEPTOR',rut_receptor1::varchar);
			xml3:=put_campo(xml3,'TIPO_DTE',tipo_dte1::varchar);
			xml3:=put_campo(xml3,'FOLIO',folio1::varchar);
			xml3:=put_campo(xml3,'CODIGO_TXEL',codigo_txel1::varchar);
			xml3:=put_campo(xml3,'FECHA_INGRESO_COLA',fecha_cola1::varchar);
			xml3:=put_campo(xml3,'REFERENCIAS_JSON',referencias1::varchar);
			xml3:=put_campo(xml3,'DATA_DTE',data_dte1);
			xml3:=put_campo(xml3,'MONTO_TOTAL',monto1::varchar);
			xml3:=put_campo(xml3,'FECHA_RECEPCION_SII',fecha2::varchar);
			xml3:=put_campo(xml3,'PARAMETRO1',p1::varchar);
			xml3:=put_campo(xml3,'PARAMETRO2',p2::varchar);
			xml3:=put_campo(xml3,'PARAMETRO3',p3::varchar);
			xml3:=put_campo(xml3,'PARAMETRO4',p4::varchar);
			xml3:=put_campo(xml3,'PARAMETRO5',p5::varchar);
			xml3:=put_campo(xml3,'ID_REGLA_CONTROLLER',campo.id_cabecera::varchar);
			cola1:=nextval('id_cola_procesamiento');
			tx1:='30';
			nombre_tabla1:='cola_motor_'||cola1::varchar;
			query1:='insert into ' || nombre_tabla1 || ' (fecha,uri,reintentos,data,tx,rut_emisor,reproceso,categoria, nombre_cola) values ( '''||fecha_cola1||'''::timestamp,'||quote_literal(uri1)||',0,'||quote_literal(xml3)||','||tx1||','||quote_literal(rut_emisor1::varchar)||',''NO'',''CONTROLLER_FECHA'','|| quote_literal(nombre_tabla1) ||') returning id';
			execute query1 into id1;
			xml2:=logapp(xml2,'Se ingresa en colas con fecha='||fecha_cola1::varchar||' para procesar regla con fecha de recepcion '||uri1||' idcola='||id1::varchar||' Regla='||campo.id_cabecera::varchar);
		end loop;
	end if;
	xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');

	
	return xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION insert_cola_fecha_rec_sii_16103(varchar,varchar,varchar,varchar,varchar,varchar) RETURNS varchar AS $$
DECLARE
        codigo_txel1    alias for $1;
        rut_emisor1     alias for $2;
        tipo_dte1       alias for $3;
        folio1          alias for $4;
        rut_receptor1   alias for $5;
        uri1            alias for $6;
        xml3    varchar;
        id1     bigint;
        tx1     varchar;
        nombre_tabla1   varchar;
        query1  varchar;
        cola1   varchar;
BEGIN
                xml3:='';
                xml3:=put_campo(xml3,'TX','16103');
                xml3:=put_campo(xml3,'URI_IN',uri1);
                xml3:=put_campo(xml3,'CANAL','RECIBIDOS');
                xml3:=put_campo(xml3,'RUT_EMISOR',rut_emisor1);
                xml3:=put_campo(xml3,'RUT_RECEPTOR',rut_receptor1);
                xml3:=put_campo(xml3,'TIPO_DTE',tipo_dte1);
                xml3:=put_campo(xml3,'FOLIO',folio1);
                xml3:=put_campo(xml3,'CODIGO_TXEL',codigo_txel1);
		xml3:=put_campo(xml3,'FECHA_INGRESO_COLA',now()::varchar);
                cola1:=nextval('id_cola_sii');
                tx1:='30';
                nombre_tabla1:='cola_sii_'||cola1::varchar;
                query1:='insert into ' || nombre_tabla1 || ' (fecha,uri,reintentos,data,tx,rut_emisor,reproceso,categoria, nombre_cola) values ( now(),'||quote_literal(uri1)||',0,'||quote_literal(xml3)||','||tx1||','||quote_literal(rut_emisor1::varchar)||',''NO'',''FECHA_RECEPCION_SII'','|| quote_literal(nombre_tabla1) ||') returning id';
                execute query1 into id1;
                if id1 is not null then
                        return 'URI='||uri1||' '||nombre_tabla1||' se graba Evento para consultar Fecha Recepcion SII';
                else
                        return 'FALLA';
                end if;
END;
$$ LANGUAGE plpgsql;

