--Consulta Estado Reclamo SII
delete from isys_querys_tx where llave='16102';

insert into isys_querys_tx values ('16102',10,1,1,'select armo_consulta_sii_16102(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('16102',20,1,2,'Servicio de SII 172.16.14.88',4013,100,101,0,0,30,30);
insert into isys_querys_tx values ('16102',30,1,1,'select proceso_respuesta_sii_16102(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('16102',1000,19,1,'select sp_procesa_respuesta_cola_motor88_json(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,0,0);

CREATE or replace FUNCTION armo_consulta_sii_16102(varchar) RETURNS varchar AS $$
DECLARE
        xml1   alias for $1;
        xml2   varchar;

        json_in json;
BEGIN
	xml2:=xml1;
	xml2:=put_campo(xml2,'TRACK_ID',split_part(get_campo('TRACK_ID',xml2),chr(10),1));

	json_in:='{"RutCompania":"'||get_campo('RUT_EMISOR',xml2)||'","DvCompania":"'||modulo11(get_campo('RUT_EMISOR',xml2))||'","TrackId":"'||get_campo('TRACK_ID',xml2)||'"}';
	
	xml2:=logapp(xml2,'SII: '||json_in::varchar);

	xml2 := put_campo(xml2,'__SECUENCIAOK__','20');
        xml2:=get_parametros_motor(xml2,'SERVICIO_SII_JSON');
        xml2:=put_campo(xml2,'INPUT','POST /estado_trackid HTTP/1.1'||chr(10)||'User-Agent: curl/7.26.0'||chr(10)||'Host: '||get_campo('__IP_CONEXION_CLIENTE__',xml2)||':'||get_campo('__IP_PORT_CLIENTE__',xml2)||chr(10)||'Accept: '||chr(42)||'/'||chr(42)||chr(10)||'Content-Type: application/json; charset=ISO-8859-1'||chr(10)||'Content-Length: '||length(json_in::varchar)::varchar||chr(10)||chr(10)||json_in::varchar);
	return xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION proceso_respuesta_sii_16102(varchar) RETURNS varchar AS $$
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
BEGIN
	xml2:=xml1;
	resp1:=get_campo('RESPUESTA',xml2);
	
	xml2 :=put_campo(xml2,'__SECUENCIAOK__','1000');	
	xml2:=logapp(xml2,'Respuesta SII '||replace(resp1,chr(10),''));
	--{"registroreclamodteservice.xsd2.xsd:RESPUESTA": {"registroreclamodteservice.xsd2.xsd:RESP_BODY": {"TIPO_DOCTO": "52", "INFORMADOS": "9", "ACEPTADOS": "9", "RECHAZADOS": "0", "REPAROS": "0"}, "registroreclamodteservice.xsd2.xsd:RESP_HDR": {"TRACKID": "2099040022", "ESTADO": "EPR", "GLOSA": "Envio Procesado", "NUM_ATENCION": "689729   ( 2017/04/13 10:48:00)"}}}
	
	if(strpos(resp1,'HTTP/1.0 200')=0) then
                xml2:=put_campo(xml2,'RESPUESTA','Status: 400 NK');
		return xml2;
	end if;

	--Si no podemos consultar porque cambiaron algo en el sii
	
	
	--Si no es un json, reintentamos
	begin
		json_out:=split_part(resp1,chr(10)||chr(10),2)::json;
		j4:=get_first_key_json(get_first_key_json(json_out::varchar));
	exception when others then
		xml2:=logapp(xml2,'Respuesta SII no es un json' );
		xml2:=put_campo(xml2,'RESPUESTA','Status: 400 NK');
		return xml2;
	end;
	
	if (get_json('ESTADO',j4)='-6' and strpos(get_json('GLOSA',j4),'USUARIO NO AUTORIZADO')>0) then
		--por ahora borramos
               	xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
		return xml2;
	end if;
	--Verificamos si el envio fue aceptado
	--j1:=get_first_key_json(json_out::varchar);
	xml2:=logapp(xml2,'Json '||j4::varchar);
	lista1:=get_campo('LISTA_DTE',xml2)::json;
	trackid:=trim(replace(split_part(split_part(json_out::varchar,'"TRACKID":',2),'",',1),'"',''));
	if(trackid<>get_campo('TRACK_ID',xml2)) then
		xml2:=logapp(xml2,'TRACK_ID no corresponde ('||trackid||')-('||get_campo('TRACK_ID',xml2)||')');
		xml2:=put_campo(xml2,'RESPUESTA','Status: 444 NK');
		return xml2;
	end if;

	--Si es una lista el tipo dte de respuesta del sii, hacemos un ciclo
	lista2:='[]';
	if (is_json_list(get_json('TIPO_DOCTO',j4)) is false) then
		lista2:=put_json_list(lista2,j4);
	else
		l:=0;
		n1:=get_json_index(get_json('TIPO_DOCTO',j4)::json,l);
		while n1::varchar<>'' loop
			jaux:='{}';
			jaux:=put_json(jaux,'TIPO_DOCTO',n1);
			jaux:=put_json(jaux,'INFORMADOS',get_json_index(get_json('INFORMADOS',j4)::json,l));	
			jaux:=put_json(jaux,'ACEPTADOS',get_json_index(get_json('ACEPTADOS',j4)::json,l));	
			jaux:=put_json(jaux,'REPAROS',get_json_index(get_json('REPAROS',j4)::json,l));	
			jaux:=put_json(jaux,'RECHAZADOS',get_json_index(get_json('RECHAZADOS',j4)::json,l));	
			lista2:=put_json_list(lista2,jaux);
			l:=l+1;
			n1:=get_json_index(get_json('TIPO_DOCTO',j4)::json,l);
		end loop;
	end if;
	json_par1:=get_parametros_motor_json('{}','BASE_COLAS');
	query1:='';
	
	l:=0;
	n1:=get_json_index(lista2,l);
	while n1::varchar<>'' loop
		j1:=n1::json;
		xml2:=logapp(xml2,'Procesando J1='||j1::varchar);

		evento1:=null;
		if (get_json('INFORMADOS',j1)='') then
			xml2:=logapp(xml2,'No viene campo INFORMADOS, se ignora');
                        l:=l+1;
                        n1:=get_json_index(lista2,l);
                        continue;
		--Si los informados son aceptados, marcamos la lista
		elsif get_json('ACEPTADOS',j1)=get_json('INFORMADOS',j1) then
			evento1:='ASI';
		elsif get_json('REPAROS',j1)=get_json('INFORMADOS',j1) then
			evento1:='CSI';
		elsif get_json('RECHAZADOS',j1)=get_json('INFORMADOS',j1) then
			evento1:='RSI';
		--Si estan reparados y aceptados, pero todos los damos por ASI
		elsif (get_json('REPAROS',j1)::integer+get_json('ACEPTADOS',j1)::integer=get_json('INFORMADOS',j1)::integer) then
			evento1:='ASI';
		--Si algunos estan rechazados y aceptados, ignoramos la respuesta porque no podemos marcar y seeguimos con el siguiente para que se borre
		elsif (get_json('REPAROS',j1)::integer+get_json('ACEPTADOS',j1)::integer+get_json('RECHAZADOS',j1)::integer=get_json('INFORMADOS',j1)::integer) then
			xml2:=logapp(xml2,'No se puede marcar rechazados y aprobados, se ignora');
			l:=l+1;
			n1:=get_json_index(lista2,l);
			continue;
		else
			--Aun el sii no responde esperamos
			xml2:=logapp(xml2,'SII aun no responde el total, esperamos');
			xml2:=put_campo(xml2,'RESPUESTA','Status: 444 NK');	
			return xml2;
		end if;
		if (evento1 is not null) then
			i:=0;
			aux:=get_json_index(lista1::json,i);
			-- recorro los Documentos Pendientes
			while length(aux)>0 loop
				if(get_json('TIPO_DOCTO',j1)<>get_json('TIPO_DTE',aux::json)) then
					i:=i+1;
					aux:=get_json_index(lista1::json,i);
					continue;
				end if;				

				j3:=lee_traza_evento(get_json('URI',aux::json),'ERE');
				--Solo enviamos intercambio si no se ha enviado
				if (get_json('uri',j3)='') then
					--Insertamos en las Colas para que se envie por intercambio
					xml3:='';
					xml3:=put_campo(xml3,'TX','12791');
					xml3:=put_campo(xml3,'URI_IN',lower_dominio_uri(get_json('URI',aux::json)));
					xml3:=put_campo(xml3,'CANAL','EMITIDOS');
					xml3:=put_campo(xml3,'RUT_EMISOR',get_campo('RUT_EMISOR',xml2));
					xml3:=put_campo(xml3,'FLAG_EVENTO_REE','NO');
					tx1:='30';
                			cola1:=nextval('id_cola_procesamiento_colas');
                			nombre_tabla1:='cola_motor_'||cola1::varchar;
					if (evento1 in ('CSI','ASI')) then
						query1:=query1||' insert into ' || nombre_tabla1 || ' (fecha,uri,reintentos,data,tx,rut_emisor,reproceso,categoria,nombre_cola) values ( now(),'||quote_literal(lower_dominio_uri(get_json('URI',aux::json)))||',0,'||quote_literal(xml3)||','||tx1||','||quote_literal(get_campo('RUT_EMISOR',xml2))||',''NO'',''REENVIO_INTER'','||quote_literal(nombre_tabla1)||');';
					end if;
					/*json_curl:=query_db_json(get_json('__IP_CONEXION_CLIENTE__',json_par1),get_json('__IP_PORT_CLIENTE__',json_par1)::integer,'insert into ' || nombre_tabla1 || ' (fecha,uri,reintentos,data,tx,rut_emisor,reproceso,categoria,nombre_cola) values ( now(),'||quote_literal(lower_dominio_uri(get_json('URI',aux::json)))||',0,'||quote_literal(xml3)||','||tx1||','||quote_literal(get_campo('RUT_EMISOR',xml2))||',''NO'',''REENVIO_INTER'','||quote_literal(nombre_tabla1)||')');
					if(get_json('STATUS',json_curl)<>'SIN_DATA') then
						xml2:=logapp(xml2,'Falla Grabar Intercambio en Colas88');
						xml2:=put_campo(xml2,'RESPUESTA','Status: 400 NK');	
						return xml2;
					end if;
					*/
				else
					xml2:=logapp(xml2,'Intercambio ya enviado');
				end if;

				xml3:='';
				xml3:=put_campo(xml3,'RUT_EMISOR',get_campo('RUT_EMISOR',xml2));
				xml3:=put_campo(xml3,'RUT_OWNER',get_campo('RUT_EMISOR',xml2));
				xml3:=put_campo(xml3,'FECHA_EMISION',get_json('FECHA_EMISION',aux::json));
				xml3:=put_campo(xml3,'RUT_RECEPTOR',get_json('RUT_RECEPTOR',aux::json));
				xml3:=put_campo(xml3,'FECHA_EVENTO',now()::varchar);
				xml3:=put_campo(xml3,'FOLIO',get_json('FOLIO',aux::json));
				xml3:=put_campo(xml3,'TIPO_DTE',get_json('TIPO_DTE',aux::json));
				xml3:=put_campo(xml3,'CANAL','EMITIDOS');
				xml3:=put_campo(xml3,'URI_IN',get_json('URI',aux::json));
				xml3:=put_campo(xml3,'COMENTARIO_TRAZA','TrackID: '||get_campo('TRACK_ID',xml2));
				xml3:=put_campo(xml3,'EVENTO',evento1);
				xml2:=logapp(xml2,'xml3='||replace(xml3,'###',' - '));
				xml3:=actualiza_estado_dte(xml3);   
				--Si no encuentro el emitido, es porque se aprobo, lo libero
				if (get_campo('EMI_NOT_FOUND',xml3)='SI') then
					xml2:=logapp(xml2,'Emitido no encontrado, debe estar aprobado '||get_json('URI_IN',aux::json));
					xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');	
					return xml2;
				end if;
				xml3:=graba_bitacora(xml3,evento1);
				xml2:=logapp(xml2,get_campo('_LOG_',xml3));
				
				i:=i+1;
				aux:=get_json_index(lista1::json,i);
			end loop;
		end if;
		if (evento1 in ('CSI','ASI') and query1<>'') then
			--Inserto todo de una vez
			json_curl:=query_db_json(get_json('__IP_CONEXION_CLIENTE__',json_par1),get_json('__IP_PORT_CLIENTE__',json_par1)::integer,query1);
			if(get_json('STATUS',json_curl)<>'SIN_DATA') then
				raise notice 'json_curl=% %',json_curl,query1;
				xml2:=logapp(xml2,'Falla Grabar Intercambio en Colas88');
				RAISE EXCEPTION 'Falla Encolar intercambio 16102' USING ERRCODE = 20000;
				return xml2;
			else
				xml2:=logapp(xml2,'Intercambio Encolado correctamente en Colas88');
			end if;
		end if;
		l:=l+1;
		n1:=get_json_index(lista2,l);
	end loop;
	xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');	
	return xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION insert_cola_estado_sii_emi_16102(varchar,varchar,varchar) RETURNS varchar AS $$
DECLARE
	trackid		alias for $1;
	rut_emisor1     alias for $2;
	data_json	alias for $3;
	xml3	varchar;
	tx1	varchar;
	nombre_tabla1	varchar;
	query1	varchar;
	cola1	varchar;
	id1	bigint;
BEGIN
                xml3:='';
                xml3:=put_campo(xml3,'TX','16102');
                xml3:=put_campo(xml3,'CANAL','EMITIDOS');
                xml3:=put_campo(xml3,'RUT_EMISOR',rut_emisor1);
                xml3:=put_campo(xml3,'TRACK_ID',trackid);
                xml3:=put_campo(xml3,'lista_dte',data_json);
		xml3:=put_campo(xml3,'FECHA_INGRESO_COLA',now()::varchar);
                cola1:=nextval('id_cola_sii');
                tx1:='10';
                nombre_tabla1:='cola_sii_'||cola1::varchar;
                query1:='insert into ' || nombre_tabla1 || ' (fecha,reintentos,data,tx,rut_emisor,reproceso,categoria, nombre_cola) values ( now(),0,'||quote_literal(xml3)||','||tx1||','||quote_literal(rut_emisor1::varchar)||',''NO'',''ESTADO_SII'','|| quote_literal(nombre_tabla1) ||') returning id';
		execute query1 into id1;
		if id1 is not null then
                        return 'TRACK_ID='||trackid||' se graba Evento para consultar estado';
		else
			return 'FALLA';
                end if;
END;
$$ LANGUAGE plpgsql;

