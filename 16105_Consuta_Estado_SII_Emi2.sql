--Consulta Estado Reclamo SII
delete from isys_querys_tx where llave='16105';

insert into isys_querys_tx values ('16105',10,1,1,'select armo_consulta_sii_16105(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('16105',20,1,2,'Servicio de SII 172.16.14.88',4013,100,101,0,0,30,30);
insert into isys_querys_tx values ('16105',30,1,1,'select proceso_respuesta_sii_16105(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('16105',1000,19,1,'select sp_procesa_respuesta_cola_motor88_json(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,0,0);

CREATE or replace FUNCTION armo_consulta_sii_16105(varchar) RETURNS varchar AS $$
DECLARE
        xml1   alias for $1;
        xml2   varchar;

        json_in json;
	port varchar;
BEGIN
	xml2:=xml1;

	json_in:='{"RutCompania": "'||get_campo('RUT_EMISOR',xml2)||'","DvCompania":"'||modulo11(get_campo('RUT_EMISOR',xml2))||'","RutReceptor":"'||get_campo('RUT_RECEPTOR',xml2)||'","DvReceptor":"'||modulo11(get_campo('RUT_RECEPTOR',xml2))||'","TipoDte":"'||get_campo('TIPO_DTE',xml2)||'","FolioDte":"'||get_campo('FOLIO',xml2)||'","FechaEmisionDte":"'||get_campo('FECHA_EMISION',xml2)||'","MontoDte":"'||get_campo('MONTO_TOTAL',xml2)||'","RUT_OWNER":"'||get_campo('RUT_EMISOR',xml2)||'"}';
	
	xml2:=logapp(xml2,'SII: '||json_in::varchar||' URI='||get_campo('URI_IN',xml2));

	xml2 := put_campo(xml2,'__SECUENCIAOK__','20');
        xml2:=get_parametros_motor(xml2,'SERVICIO_SII_JSON');
	port:=get_ipport_sii();
        if (port='') then
               --Si no hay puertos libres...
               xml2:=logapp(xml2,'No hay puertos libres');
               xml2:=put_campo(xml2,'RESPUESTA','Status: 400 NK');
               xml2 := put_campo(xml2,'__SECUENCIAOK__','1000');
               return xml2;
        end if;

        xml2:=put_campo(xml2,'__IP_CONEXION_CLIENTE__',split_part(port,':',1));
        xml2:=put_campo(xml2,'__IP_PORT_CLIENTE__',split_part(port,':',2));
        xml2:=put_campo(xml2,'IPPORT_SII',port);

        --xml2:=get_parametros_motor(xml2,'SERVICIO_SII_JSON');
        --xml2:=put_campo(xml2,'__IP_PORT_CLIENTE__',port);
        --xml2:=put_campo(xml2,'IP_PORT_CLIENTE',port);

	--xml2:=put_campo(xml2,'__IP_PORT_CLIENTE__',port);
        --xml2:=put_campo(xml2,'IP_PORT_CLIENTE',port);

        xml2:=put_campo(xml2,'INPUT','POST /estado_dte HTTP/1.1'||chr(10)||'User-Agent: curl/7.26.0'||chr(10)||'Host: '||get_campo('__IP_CONEXION_CLIENTE__',xml2)||':'||get_campo('__IP_PORT_CLIENTE__',xml2)||chr(10)||'Accept: '||chr(42)||'/'||chr(42)||chr(10)||'Content-Type: application/json; charset=ISO-8859-1'||chr(10)||'Content-Length: '||length(json_in::varchar)::varchar||chr(10)||chr(10)||json_in::varchar);
	return xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION proceso_respuesta_sii_16105(varchar) RETURNS varchar AS $$
DECLARE
        xml1   alias for $1;
        xml2 varchar;
	resp1	varchar;
	json_out	json;
	j1	json;
	n1	varchar;
	j2	json;
	j3	json;
	j4	json;
	xml3	varchar;
	aux	varchar;
	lista1	json;
	i	integer;
	json_par1	json;
	json_curl	json;
	evento1		varchar;
	lista2	json;
	jaux	json;
	l	integer;
	trackid	varchar;
	nombre_tabla1	varchar;
	cola1	varchar;	
	query1	varchar;
	tx1	varchar;
	output1	varchar;
	resp_est	varchar;
        resp_cod	varchar;
        glosa_es	varchar;
        glosa_er	varchar;

	id1	varchar;	
BEGIN
	xml2:=xml1;
	xml2 := put_campo(xml2,'__SECUENCIAOK__','1000');
        output1:=get_campo('RESPUESTA',xml2);
        xml2:=logapp(xml2,'SII json='||replace(output1,chr(10),''));
	if(strpos(output1,'HTTP/1.0 200')=0) then
		perform libera_ipport_sii(get_campo('IPPORT_SII',xml2),'FALLA');
                xml2:=logapp(xml2,'Falla Respuesta del SII '||output1);
                xml2:=put_campo(xml2,'RESPUESTA','Status: 400 NK');
                return xml2;
        end if;

        --Si no es un json, reintentamos
        begin
                json_out:=split_part(output1,chr(10)||chr(10),2)::json;
                j4:=get_first_key_json(get_first_key_json(json_out::varchar));
        exception when others then
		perform libera_ipport_sii(get_campo('IPPORT_SII',xml2),'FALLA');
                xml2:=logapp(xml2,'Respuesta SII no es un json' );
                xml2:=put_campo(xml2,'RESPUESTA','Status: 400 NK');
                return xml2;
        end;
	perform libera_ipport_sii(get_campo('IPPORT_SII',xml2),'OK');

	if strpos(get_campo('URI_IN',xml2),'REPROCESO_ID__')>0 then
		xml2:=logapp(xml2,'REPROCESO_ID__ repro_get_estado_sii');
		id1:=split_part(get_campo('URI_IN',xml2),'__',2);
		if is_number(id1) then
			update repro_get_estado_sii set estado=j4::varchar,fecha=now() where id=id1::integer; 
		end if;
		xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
		return xml2;
	end if;
        resp_est:=get_json('ESTADO',j4);
        resp_cod:=get_json('ERR_CODE',j4);
        glosa_es:=get_json('GLOSA_ESTADO',j4);
        glosa_er:=get_json('GLOSA_ERR',j4);
        if(resp_cod<>'') then
                --resp_est:='FALLA';
                if(resp_est in ('DOK','MMC','ANC','MMD','TMC','AND','TMD')) then
                        xml2:=logapp(xml2,'DTE Documento Recibido por el SII. Datos Coinciden con los Registrados. ');
                        --xml2:=put_campo(xml2,'COMENTARIO_TRAZA','Glosa: '||glosa_es||' ('||resp_est||')');
                        --xml2:=put_campo(xml2,'EVENTO','ASI');
			--Insertamos en las Colas para que se envie por intercambio
			j3:=lee_traza_evento(get_campo('URI_IN',xml2),'ERE');
			--Solo enviamos intercambio si no se ha enviado
			if (get_json('uri',j3)='') then
				xml3:='';
				xml3:=put_campo(xml3,'TX','12791');
				xml3:=put_campo(xml3,'URI_IN',lower_dominio_uri(get_campo('URI_IN',xml2)));
				xml3:=put_campo(xml3,'CANAL','EMITIDOS');
				xml3:=put_campo(xml3,'RUT_EMISOR',get_campo('RUT_EMISOR',xml2));
				xml3:=put_campo(xml3,'FLAG_EVENTO_REE','NO');
				xml3:=put_campo(xml3,'FECHA_INGRESO_COLA',now()::varchar);
				tx1:='30';
				cola1:=nextval('id_cola_procesamiento');
				nombre_tabla1:='cola_motor_'||cola1::varchar;
				--perform logfile('16102: '||i::varchar||' graba en cola88');
			
				query1:=' insert into ' || nombre_tabla1 || ' (fecha,uri,reintentos,data,tx,rut_emisor,reproceso,categoria,nombre_cola) values ( now(),'||quote_literal(lower_dominio_uri(get_campo('URI_IN',xml2)))||',0,'||quote_literal(xml3)||','||tx1||','||quote_literal(get_campo('RUT_EMISOR',xml2))||',''NO'',''REENVIO_INTER'','||quote_literal(nombre_tabla1)||');';
				json_par1:=get_parametros_motor_json('{}','BASE_COLAS');
				json_curl:=query_db_json(get_json('__IP_CONEXION_CLIENTE__',json_par1),get_json('__IP_PORT_CLIENTE__',json_par1)::integer,query1);
	                        if(get_json('STATUS',json_curl)<>'SIN_DATA') then
                	                xml2:=logapp(xml2,'Falla Grabar Intercambio en Colas88');
                			xml2:=put_campo(xml2,'RESPUESTA','Status: 400 NK');
			                return xml2;
                        	else
                                	xml2:=logapp(xml2,'Intercambio Encolado correctamente en Colas88');
	                        end if;	
			else
				xml2:=logapp(xml2,'Intercambio ya enviado');
			end if;
			xml3:='';
			xml3:=put_campo(xml3,'RUT_EMISOR',get_campo('RUT_EMISOR',xml2));
			xml3:=put_campo(xml3,'RUT_OWNER',get_campo('RUT_EMISOR',xml2));
			--xml3:=put_campo(xml3,'FECHA_EMISION',get_campo('FECHA_EMISION',xml2));
			xml3:=put_campo(xml3,'FECHA_EMISION',to_char(to_timestamp(get_campo('FECHA_EMISION',xml2),'DD-MM-YYYY'),'YYYY-MM-DD'));
			xml3:=put_campo(xml3,'RUT_RECEPTOR',get_campo('RUT_RECEPTOR',xml2));
			xml3:=put_campo(xml3,'FECHA_EVENTO',now()::varchar);
			xml3:=put_campo(xml3,'FOLIO',get_campo('FOLIO',xml2));
			xml3:=put_campo(xml3,'TIPO_DTE',get_campo('TIPO_DTE',xml2));
			xml3:=put_campo(xml3,'CANAL','EMITIDOS');
			xml3:=put_campo(xml3,'URI_IN',get_campo('URI_IN',xml2));
			
			--Si tiene algun mensaje lo grabo
                	if(resp_est in ('MMC','ANC','MMD','TMC','AND','TMD','FAN')) then
				xml3:=put_campo(xml3,'COMENTARIO_TRAZA','TrackID: '||get_campo('TRACK_ID',xml2)||chr(10)||glosa_er||' ('||resp_cod||')');
			else
				xml3:=put_campo(xml3,'COMENTARIO_TRAZA','TrackID: '||get_campo('TRACK_ID',xml2));
			end if;
			xml3:=put_campo(xml3,'EVENTO','ASI');
			xml2:=logapp(xml2,'xml3='||replace(xml3,'###',' - '));
			xml3:=actualiza_estado_dte(xml3);
			--Si no encuentro el emitido, es porque se aprobo, lo libero
			if (get_campo('EMI_NOT_FOUND',xml3)='SI') then
				xml2:=logapp(xml2,'Emitido no encontrado, debe estar aprobado '||get_campo('URI_IN',xml2));
				xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
				return xml2;
			end if;
			xml3:=graba_bitacora(xml3,'ASI');
			xml2:=logapp(xml2,get_campo('_LOG_',xml3));
                	xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
			return xml2;
                elsif(resp_est in ('DNK','FAU','NA','FAN')) then
                        xml2:=logapp(xml2,'DTE Documento Recibido por el SII pero Datos NO Coinciden con los registrados. ');
		        xml3:='';
                        xml3:=put_campo(xml3,'RUT_EMISOR',get_campo('RUT_EMISOR',xml2));
                        xml3:=put_campo(xml3,'RUT_OWNER',get_campo('RUT_EMISOR',xml2));
                        --xml3:=put_campo(xml3,'FECHA_EMISION',get_campo('FECHA_EMISION',xml2));
			xml3:=put_campo(xml3,'FECHA_EMISION',to_char(to_timestamp(get_campo('FECHA_EMISION',xml2),'DD-MM-YYYY'),'YYYY-MM-DD'));
                        xml3:=put_campo(xml3,'RUT_RECEPTOR',get_campo('RUT_RECEPTOR',xml2));
                        xml3:=put_campo(xml3,'FECHA_EVENTO',now()::varchar);
                        xml3:=put_campo(xml3,'FOLIO',get_campo('FOLIO',xml2));
                        xml3:=put_campo(xml3,'TIPO_DTE',get_campo('TIPO_DTE',xml2));
                        xml3:=put_campo(xml3,'CANAL','EMITIDOS');
                        xml3:=put_campo(xml3,'URI_IN',get_campo('URI_IN',xml2));
                        xml3:=put_campo(xml3,'COMENTARIO_TRAZA','TrackID: '||get_campo('TRACK_ID',xml2)||chr(10)||glosa_er);
			if (resp_est in ('FAU','NA')) then
				evento1:='FAU';
			else
				evento1:='RSI';
			end if;
                       	xml3:=put_campo(xml3,'EVENTO',evento1);
                        xml2:=logapp(xml2,'xml3='||replace(xml3,'###',' - '));
                        xml3:=actualiza_estado_dte(xml3);
                        --Si no encuentro el emitido, es porque se aprobo, lo libero
                        if (get_campo('EMI_NOT_FOUND',xml3)='SI') then
                                xml2:=logapp(xml2,'Emitido no encontrado, debe estar aprobado '||get_campo('URI_IN',xml2));
                                xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
                                return xml2;
                        end if;
                        xml3:=graba_bitacora(xml3,evento1);
                        xml2:=logapp(xml2,get_campo('_LOG_',xml3));
                        xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
                        return xml2;
		/*
                elsif(resp_est in ('FAU','NA')) then
                        --Aun no llega al sii, le damos tiempo
                        if(now()-get_campo('FECHA_INGRESO_COLA',xml2)::timestamp>interval '1 days') then
                                xml2 := put_campo(xml2,'RESPUESTA','Status: 200 OK');
                                xml2:=logapp(xml2,'DTE por 1 dias, se borra de las cola');
                                xml2:=put_campo(xml2,'EVENTO','RSI');
                        else
                                xml2 := put_campo(xml2,'RESPUESTA','Status: 444 NK');
                                xml2:=put_campo(xml2,'EVENTO',resp_est);
                        end if;
                        --xml2:=put_campo(xml2,'COMENTARIO_TRAZA','Glosa: '||glosa_er||' ('||resp_cod||')');
                        --xml2:=graba_bitacora(xml2,resp_est);
                        xml2 := put_campo(xml2,'MENSAJE_XML_FLAGS',glosa_er);
		*/
                else
                       xml2 := put_campo(xml2,'RESPUESTA','Status: 400 NK');
                       xml2 := logapp(xml2,'Falla Consulta SII');
                       return xml2;
                end if;
        else
                --Lo graba en la cola para procesamiento posterior
                xml2 := logapp(xml2,'Falla Consulta SII');
                xml2 := put_campo(xml2,'RESPUESTA','Status: 400 NK');
                return xml2;
        end if;
	return xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION insert_cola_estado_sii_rec_16105(varchar,varchar,varchar,varchar,varchar,varchar,varchar,varchar) RETURNS varchar AS $$
DECLARE
	rut_emisor1	alias for $1;
	tipo_dte1	alias for $2;
	folio1		alias for $3;
	rut_rec1	alias for $4;
	fecha_emi1	alias for $5;
	monto1		alias for $6;
	uri1		alias for $7;
	trackid		alias for $8;
	xml3	varchar;
	tx1	varchar;
	nombre_tabla1	varchar;
	query1	varchar;
	cola1	varchar;
	id1	bigint;
	campo	record;
	aux1	varchar;
BEGIN
		aux1:=rut_emisor1||'_'||tipo_dte1||'_'||folio1;
		--Debemos verificar que no exista la uri en las colas
		select * into campo from colas_motor_generica where uri=uri1 and categoria='ESTADO_SII';
		if found then
			return 'REPETIDO_'||aux1;
		end if;
                xml3:='';
                xml3:=put_campo(xml3,'TX','16105');
                xml3:=put_campo(xml3,'CANAL','EMITIDOS');
                xml3:=put_campo(xml3,'RUT_EMISOR',rut_emisor1);
                xml3:=put_campo(xml3,'RUT_RECEPTOR',rut_rec1);
                xml3:=put_campo(xml3,'TIPO_DTE',tipo_dte1);
                xml3:=put_campo(xml3,'FOLIO',folio1);
                xml3:=put_campo(xml3,'MONTO_TOTAL',monto1);
                xml3:=put_campo(xml3,'FECHA_EMISION',fecha_emi1);
                xml3:=put_campo(xml3,'URI_IN',uri1);
                xml3:=put_campo(xml3,'TRACK_ID',trackid);
		xml3:=put_campo(xml3,'FECHA_INGRESO_COLA',now()::varchar);
                cola1:=nextval('id_cola_sii');
                tx1:='10';
                nombre_tabla1:='cola_sii_'||cola1::varchar;
                query1:='insert into ' || nombre_tabla1 || ' (fecha,reintentos,data,tx,rut_emisor,reproceso,categoria, nombre_cola,uri) values ( now(),0,'||quote_literal(xml3)||','||tx1||','||quote_literal(rut_emisor1::varchar)||',''NO'',''ESTADO_SII'','|| quote_literal(nombre_tabla1) ||','''||uri1||''') returning id';
		execute query1 into id1;
		if id1 is not null then
                        return 'OK_'||aux1;
		else
			return 'FALLA_'||aux1;
                end if;
END;
$$ LANGUAGE plpgsql;

