delete from isys_querys_tx where llave='12701';

-- Prepara llamada al AML


insert into isys_querys_tx values ('12701',10,45,1,'select proc_traductor_fcgi_12701(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

--Los eventos que empiecen con ECM_XXXX y el LMA se van a actualizar a el sistema ECM
insert into isys_querys_tx values ('12701',15,30,1,'select actualiza_evento_ecm_12701(''$$__JSONCOMPLETO__$$'') as __JSON__',0,0,0,1,1,-1,0);

--Flujo consulta bitacora
insert into isys_querys_tx values ('12701',70,1,8,'Flujo Consulta Bitacora',7010,0,0,1,1,0,0);
--Actualizacion estado LCE en base LCE
--insert into isys_querys_tx values ('12701',90,16,1,'select lce_actualiza_estado(''$$URI_IN$$'',''$$EVENTO$$'') as __RESPUESTA_LCE__' ,0,0,0,1,1,91,91);
insert into isys_querys_tx values ('12701',90,16,1,'select lce.actualiza_estado_libro_lce(''$$URI_IN$$'',''$$EVENTO$$'') as __RESPUESTA_LCE__' ,0,0,0,1,1,91,91);
insert into isys_querys_tx values ('12701',91,1,1,'select Valida_lce_actualiza_estado(''$$__XMLCOMPLETO__$$'') as __XML__' ,0,0,0,1,1,-1,0);

--Traza CGE
insert into isys_querys_tx values ('12701',390,1,2,'Llamada a Traza CGE',8881,103,101,0,0,410,410);
--Traza Normal
insert into isys_querys_tx values ('12701',400,1,2,'Llamada a Traza',8880,103,101,0,0,410,410);
insert into isys_querys_tx values ('12701',401,1,2,'Llamada a Traza',9880,103,106,0,0,410,410);
insert into isys_querys_tx values ('12701',410,45,1,'select proc_respuesta_traza_12701(''$$__XMLCOMPLETO__$$'') as __XML__',0,0,0,1,1,-1,0);

--Flujo Control Basura
insert into isys_querys_tx values ('12701',500,1,8,'Flujo 9999 Basura',9999,0,0,1,1,100,100);

--Secuencia para WebIECV
insert into isys_querys_tx values ('12701',600,13,1,'select actualiza_estado_libro(''$$__JSONCOMPLETO__$$'') as __JSON__',0,0,0,1,1,610,610);
insert into isys_querys_tx values ('12701',610,45,1,'select procesa_respuesta_webiecv_12701(''$$__XMLCOMPLETO__$$'') as __XML__',0,0,0,1,1,-1,0);

--Parsea respuesta para FastCGI
insert into isys_querys_tx values ('12701',100,45,1,'select proc_respuesta_fcgi_12701(''$$__XMLCOMPLETO__$$'') as __XML_NUEVO__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('12701',700,1,8,'Flujo 7000 Nueva Cuadratura',7000,0,0,1,1,0,0);
insert into isys_querys_tx values ('12701',800,45,1,'select proc_test(''$$__XMLCOMPLETO__$$'') as __XML__',0,0,0,1,1,0,0);

CREATE or replace FUNCTION actualiza_evento_ecm_12701(json) RETURNS json as $$
declare
        json1        alias for $1;
       	json2    json;
	aux1	varchar;
	id_detalle1	bigint;
	campo	record;
	id_ori1	varchar;
	adicional1	varchar;
	bit1	bit(32);
	rut_emisor1	varchar;
	uri1	varchar;
	ciclo1	varchar;
	id1	bigint;
	campo_aux	record;
begin
    --Llega el eveno 
    json2:=json1;
    --json2:=logjson(json2,'Evento ECM= '||json2::varchar);
	
	rut_emisor1:=get_json('RUT_EMISOR',json2);
	if is_number(rut_emisor1) is false then
		rut_emisor1:=-1;
	end if;


    --Se graba en el detalle
    aux1:=split_part(split_part(get_json('INPUT',json2),'&id_ecm=',2),'&',1);
    if is_number(aux1) is false then
	json2:=logjson(json2,'ECM= Evento sin id ecm');
	--Para el caso de ENTEL - ACM - vienen sin id_ecm. Se considerara llave (uri-ciclo)
	if (get_json('EVENTO',json2)='LMA' and (get_json('RUT_EMISOR',json2)='96806980' or get_json('RUT_EMISOR',json2)='96697410')) then
			uri1:=get_json('URI_IN',json2);
			json2:=logjson(json2,'EVENTO_LMA_ENTEL'||uri1);
			select valor into bit1 from eventos_ecm where rut_owner=-1 and codigo='LMA';

			--update send_mail_cabecera set estado='LEIDO',fecha_actualizacion=now(),eventos=(coalesce(eventos,0)::bit(32)#bit1)::bigint,adicional=coalesce(adicional,'')||'<FLMA>'||now()::varchar||'</FLMA>' where rut_owner=rut_emisor1::bigint and documento=uri1 and ciclo is not null and estado<>'LEIDO' returning id into id1;
                	--insert into send_mail_detalle(id,fecha_ingreso,dia,estado) values (id1,now(),to_char(now(),'YYYYMMDD')::integer,get_json('EVENTO',json2)) returning id_detalle into id_detalle1;
			--DAO 20171124
			for campo_aux in select * from send_mail_cabecera where rut_owner=rut_emisor1::bigint and documento=uri1 and ciclo is not null and estado<>'LEIDO' loop
				update send_mail_cabecera set estado='LEIDO',fecha_actualizacion=now(),eventos=(coalesce(eventos,0)::bit(32)#bit1)::bigint,adicional=coalesce(adicional,'')||'<FLMA>'||now()::varchar||'</FLMA>' where id=campo_aux.id;
				insert into send_mail_detalle(id,fecha_ingreso,dia,estado) values (campo_aux.id,now(),to_char(now(),'YYYYMMDD')::integer,get_json('EVENTO',json2)) returning id_detalle into id_detalle1;
			end loop;
			--json2:=logjson(json2,'EVENTO_LMA_ENTEL'||uri1||' ID='||id_detalle1::varchar);
			json2:=put_json(json2,'RESPUESTA','Status: 302 Found'||chr(10)||'Location: https://traza.acepta.com/imgs/blank.png'||chr(10)||chr(10));
			json2:=put_json(json2,'__SECUENCIAOK__','0');
			RETURN json2;
    	end if;
    else
	--Insertamos el detalle
        select * into campo from send_mail_detalle where id=aux1::bigint and estado=get_json('EVENTO',json2);
        if found then
                 --Ya se encuentra grabado
                 json2:=logjson(json2,'ECM= Ya esta grabado el detalle id2='||aux1::varchar);
                 id_detalle1:=campo.id_detalle;
       else
                insert into send_mail_detalle(id,fecha_ingreso,dia,estado) values (aux1::bigint,now(),to_char(now(),'YYYYMMDD')::integer,get_json('EVENTO',json2)) returning id_detalle into id_detalle1;
                 json2:=logjson(json2,'ECM= Se inserta detalle id='||aux1::varchar||' id_detalle='||id_detalle1::varchar);
       end if;


       --Cuaquier evento que llegue, se marca como estado leido
       select valor into bit1 from eventos_ecm where rut_owner=rut_emisor1::bigint and codigo=get_json('EVENTO',json2);
       if not found then
		--Preguntamos por el generico
       		select valor into bit1 from eventos_ecm where rut_owner=-1 and codigo=get_json('EVENTO',json2);
       end if;
       if found then
		update send_mail_cabecera set estado='LEIDO',fecha_actualizacion=now(),eventos=(coalesce(eventos,0)::bit(32)#bit1)::bigint where id=aux1::bigint returning adicional into adicional1;
	        if found then
      			id_ori1:=get_xml('ID_ORI',adicional1);
	                json2:=logjson(json2,'ECM= Actualiza estado id2 '||aux1::varchar||' id_ori1='||id_ori1::varchar);
        	        if (is_number(id_ori1)) then
               		  	update send_mail_cabecera set estado='LEIDO',fecha_actualizacion=now() where id=id_ori1::bigint and estado<>'LEIDO';
	                 end if;
      		end if;
	else
		json2:=logjson(json2,'ECM= Evento no configurado en eventos_ecm '||get_json('EVENTO',json2)||' '||rut_emisor1);
	end if;
  end if;

    --Se contesta OK para que se borre el evento
    if get_json('EVENTO',json2) = 'LMA' then
    	json2:=put_json(json2,'RESPUESTA','Status: 302 Found'||chr(10)||'Location: https://traza.acepta.com/imgs/blank.png'||chr(10)||chr(10));
    else
    	json2:=put_json(json2,'RESPUESTA','Status: 200 OK'||chr(10)||
		 'Content-type: text/html; charset=iso-8859-1'||chr(10)||
		 'Content-length: 0'||chr(10)||
		 'Vary: Accept-Encoding'||chr(10)||chr(10));
    end if;
    json2:=put_json(json2,'__SECUENCIAOK__','0');
    RETURN json2;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION proc_test(varchar) RETURNS varchar AS $$
declare
	 xml1        alias for $1;
	xml2	varchar;
begin
    xml2:=xml1;
    xml2:=logapp(xml2,'******');
    RETURN xml2;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION Valida_lce_actualiza_estado(varchar) RETURNS varchar AS $$
declare
         xml1        alias for $1;
        xml2    varchar;
begin
    xml2:=xml1;
        xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
        if get_campo('__RESPUESTA_LCE__',xml2)='200' then
                xml2:=logapp(xml2,'LCE actualzado URI='||get_campo('URI_IN',xml2)||' Estado='||get_campo('EVENTO',xml2));
                xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK'||chr(10)||
                 'Content-type: text/html; charset=iso-8859-1'||chr(10)||
                 'Content-length: 0'||chr(10)||
                 'Vary: Accept-Encoding'||chr(10)||chr(10));

        else
                xml2:=logapp(xml2,'LCE Falla actualizacion URI='||get_campo('URI_IN',xml2)||' Estado='||get_campo('EVENTO',xml2));
                xml2:=put_campo(xml2,'RESPUESTA','Status: 400 NOK'||chr(10)||
                 'Content-type: text/html; charset=iso-8859-1'||chr(10)||
                 'Content-length: 0'||chr(10)||
                 'Vary: Accept-Encoding'||chr(10)||chr(10));

        end if;
    RETURN xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION proc_traductor_fcgi_12701(varchar) RETURNS varchar AS $$
DECLARE
    xml1	alias for $1;
    xml2	varchar;
    data1	varchar;
    data_hex2	varchar;
    file1	varchar;
    sts		integer;
    header1	varchar;
    url1	varchar;
    host1	varchar;
BEGIN
    xml2:=xml1;
    --perform logfile('proc_traductor_fcgi_12701 '||replace(xml1,chr(10),''));
    --Si no viene INPUT es un GET
    if length(get_campo('INPUT',xml2))=0 then
    	data1:=get_campo('QUERY_STRING',xml2);
    else
	data_hex2:=get_campo('INPUT',xml2);
	--FAY 2019-02-01 esta conversion genera caracteres \302 en el comentario traza
        --intentamos primero con la funcion decode_hex que deja correctamente el comentario traza
	BEGIN
    		data1:=decode_hex(data_hex2);
	EXCEPTION WHEN OTHERS THEN
		xml2:=logapp('Falla Funcion decode_hex');
    		data1:=decode(data_hex2,'hex');
		--Cambio los \012 por chr(10)
		data1:=encode(data1::bytea,'escape');
	END;
    end if;
    
    --Log inteligente
    if (get_campo('REQUEST_METHOD',xml2)='GET') then
	xml2:=logapp(xml2,'data1='||data1);
    end if;

    url1:=get_campo('SCRIPT_URL',xml2);
    host1:=get_campo('HTTP_HOST',xml2);
    --xml2:=logapp(xml2,'HTTP_USER_AGENT='||get_campo('HTTP_USER_AGENT',xml2));
    xml2 := put_campo(xml2,'HTTP_CONTENT_TYPE',get_campo('CONTENT_TYPE',xml2));
    xml2 := put_campo(xml2,'HTTP_CONTENT_LENGTH',get_campo('CONTENT_LENGTH',xml2))i;

    if (strpos(data1,'tipo_tx=PruebasBitacora&')>0) then
        xml2 := put_campo(xml2,'__SECUENCIAOK__','70');
        xml2 := logapp(xml2,'Log Prueba bitacora: *******'||get_campo('__SECUENCIAOK__',xml2));
    --AGA Si vienen POST para la nueva cuadratura...
    elsif (strpos(data1,'app=cuadratura')>0) then 
        xml2 := put_campo(xml2,'__SECUENCIAOK__','700');
	xml2 := put_campo(xml2,'INPUT',data1);
	return xml2;
     --Test
    elsif (strpos(data1,'test=1')>0) then 
        xml2 := put_campo(xml2,'__SECUENCIAOK__','800');
	return xml2; 
    --Si vienen los eventos de traza
    elsif (strpos(data1,'<trace source=')>0) then

	xml2:=logapp(xml2,'Procesos Activos :'||get_campo('__PROC_ACTIVOS__',xml2));


	--El servicio esta en esa URL
	url1:='/tproxy/put';
	host1:='traza.acepta.com:8880';
    	xml2 := put_campo(xml2,'__SECUENCIAOK__','400');
	--Cambio los \012 por chr(10)
	--raise notice 'INPUT=%',data1;
	--Esta linea solo se usa si falla el primer encode
	--data1:=encode(data1::bytea,'escape');
	xml2 := logapp(xml2,'TRAZA='||data1);
	xml2 := put_campo(xml2,'INPUT',data1);
	xml2 := procesa_evento_traza(xml2);

        --Si es un LCE actualizamos la tabla en la base de libros.
        if (get_campo('TIPO',xml2) = 'LCE') then
                xml2:=put_campo(xml2,'__SECUENCIAOK__','90');
                xml2:=logapp(xml2,'LCE: Vamos a base de libros, actualizar libro uri-->'||get_campo('URI_IN',xml2));
                return xml2;
        end if;

	--Si el evento ya esta registrado, no vamos a traza para no dejar la caga
	if (get_campo('EVENTO_REPETIDO',xml2)='SI') then
                xml2 := logapp(xml2,'NO vamos a traza por evento repetido, se contesta OK');
                xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
    		xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK'||chr(10)||
		 'Content-type: text/html; charset=iso-8859-1'||chr(10)||
		 'Content-length: 0'||chr(10)||
		 'Vary: Accept-Encoding'||chr(10)||chr(10));
                return xml2;
        end if;

	if (get_campo('FALLA_TRAZA',xml2)='SI') then
		xml2 := logapp(xml2,'NO vamos a traza y no contesto');
    		xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
		return xml2;
	end if;
	--Si es un evento CGE vamos a la traza antigua
	xml2:=put_campo(xml2,'RUT_CGE',get_campo('RUT_OWNER',xml2));
	xml2:=verifica_evento_cge(xml2);	
	if (get_campo('EVENTO_CGE',xml2)='SI') then
		--FAY,RME ya no vamos a traza antigua 20150209
		xml2 := logapp(xml2,'Contesto 200 OK');
		xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK'||chr(10)||
                	'Content-type: text/html; charset=iso-8859-1'||chr(10)||
	                'Content-length: 0'||chr(10)||
        	        'Vary: Accept-Encoding'||chr(10)||chr(10));
		xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
		return xml2;
		--xml2:=logapp(xml2,'Evento CGE');
    		xml2 := put_campo(xml2,'__SECUENCIAOK__','390');
		host1:='cge-traza.acepta.com:8880';
        	header1:='POST '||url1||' HTTP/1.1'||chr(10)||'Host: '||host1||chr(10)||'Content-Type: text/xml'||chr(10)||'Content-Length: '||get_campo('CONTENT_LENGTH',xml2)||chr(10)||chr(10);
        	xml2:=put_campo(xml2,'INPUT',encode(header1::bytea,'hex')||data_hex2);
		--xml2:=logapp(xml2,'DATA='||get_campo('INPUT',xml2));
		return xml2;
	end if;
        header1:='POST '||url1||' HTTP/1.1'||chr(10)||'Host: '||host1||chr(10)||'Content-Type: '||get_campo('CONTENT_TYPE',xml2)||chr(10)||'Content-Length: '||get_campo('CONTENT_LENGTH',xml2)||chr(10)||chr(10);
        xml2:=put_campo(xml2,'INPUT',encode(header1::bytea,'hex')||data_hex2);

	--Si es un libro de compra venta, vamos a traza igual
	if (get_campo('TIPO_OPERACION',xml2) in ('COMPRA','VENTA')) then
		xml2 := logapp(xml2,'Libro '||get_campo('TIPO_OPERACION',xml2));
		xml2 := put_campo(xml2,'ESTADO_LIBRO',get_campo('EVENTO',xml2));
		xml2 := put_campo(xml2,'CODIGO_LIBRO','-1');
		xml2 := put_campo(xml2,'PERIODO',get_campo('FOLIO',xml2));
		xml2 := put_campo(xml2,'URI_LIBRO_IECV',get_campo('URI_IN',xml2));
		xml2 := put_campo(xml2,'__SECUENCIAOK__','600');
		return xml2;
	end if;

	/*
	--El evento EMA esta con error en TRAZA antigua, no se carga
	if (get_campo('EVENTO',xml2)='EMA') then	
		xml2 := logapp(xml2,'Evento EMA no va a traza, responde OK');
		--Se Responde OK
		xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK'||chr(10)||
                'Content-type: text/html; charset=iso-8859-1'||chr(10)||
                'Content-length: 0'||chr(10)||
                'Vary: Accept-Encoding'||chr(10)||chr(10));
		xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
		return xml2;
	end if;
	*/
		
	--FAY,RME ya no vamos a traza antigua 20150209
	xml2 := logapp(xml2,'Contesto 200 OK');
	xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK'||chr(10)||
                'Content-type: text/html; charset=iso-8859-1'||chr(10)||
                'Content-length: 0'||chr(10)||
                'Vary: Accept-Encoding'||chr(10)||chr(10));
	xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
	return xml2;
		
	/*
	--Llamamos la funcion que verifica si el evento tienen DND antiguo o no
	xml2:=verifica_evento_dnd(xml2);
	--Solo vamos a traza si en un evento necesario para el DND
	if (get_campo('EVENTO_DND',xml2)='NO') then
		xml2 := logapp(xml2,'No vamos a traza, Rut sin DND');
		--Se Responde OK
		xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK'||chr(10)||
                'Content-type: text/html; charset=iso-8859-1'||chr(10)||
                'Content-length: 0'||chr(10)||
                'Vary: Accept-Encoding'||chr(10)||chr(10));
		xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
		return xml2;
	end if;
	return xml2;
	*/
    --Si es del DND
    elsif (strpos(get_campo('REQUEST_URI',xml2),'TraceBoot')>0) then
        xml2:=logapp(xml2,replace(xml2,'###','$-$'));
	if (get_campo('REQUEST_METHOD',xml2)='GET') then
		url1=get_campo('REQUEST_URI',xml2);
	/*
		url1=get_campo('REQUEST_URI',xml2)||
			case when length(get_campo('QUERY_STRING',xml2))>0 then '?'||get_campo('QUERY_STRING',xml2) 
			else '' end;
	*/
		xml2:=logapp(xml2,'URL CA4DND '||url1);
		host1:='traza.acepta.com';
        	header1:='GET '||url1||' HTTP/1.1'||chr(10)||'Host: '||host1||chr(10)||'Content-Type: text/html'||chr(10)||'Content-Length: 0'||chr(10)||chr(10);
	else
		url1=get_campo('REQUEST_URI',xml2);
		xml2:=logapp(xml2,'URL CA4DND '||url1);
		host1:='traza.acepta.com';
        	header1:='POST '||url1||' HTTP/1.1'||chr(10)||'Host: '||host1||chr(10)||'Content-Type: text/html'||chr(10)||'Content-Length: '||length(data1)||chr(10)||chr(10)||data1;
	end if;
	/*
	if (strpos(get_campo('QUERY_STRING',xml2),'sync')>0) then
	elsif (strpos(get_campo('QUERY_STRING',xml2),'commit')>0) then
	else
		url1:=get_campo('PATH_INFO',xml2);
		xml2:=logapp(xml2,'URL CA4DND '||url1);
		host1:='traza.acepta.com';
        	header1:='GET '||url1||' HTTP/1.1'||chr(10)||'Host: '||host1||chr(10)||'Content-Type: text/html'||chr(10)||'Content-Length: 0'||chr(10)||chr(10);
	end if;
	*/
	xml2:=logapp(xml2,'Mensaje TraceBoot/ca4dnd '||header1);
        xml2:=put_campo(xml2,'INPUT',encode(header1::bytea,'hex')::varchar);
        xml2:=put_campo(xml2,'TIPO_TX','CA4DND');
	xml2 := put_campo(xml2,'__SECUENCIAOK__','401');
	return xml2;
		
    --Si es un GET  de Traza y no es de nagios
    elsif ((get_campo('REQUEST_METHOD',xml2)='GET') and (strpos(get_campo('HTTP_USER_AGENT',xml2),'check_http')=0) and
	   (strpos(get_campo('SCRIPT_NAME',xml2),'/tproxy/put')>0 or 
	    strpos(get_campo('QUERY_STRING',xml2),'source=ACM')>0 or 
	    strpos(get_campo('REQUEST_URI',xml2),'/tproxy/put')>0 or 
	    strpos(get_campo('REQUEST_URI',xml2),'/traza')>0)
	  ) then
	xml2:=logapp(xml2,'Evento GET');
	--El servicio esta en esa URL
	url1:='/tproxy/put?';
	host1:='traza.acepta.com:8880';
	xml2 := put_campo(xml2,'__SECUENCIAOK__','400');
	--Cambio los \012 por chr(10)
	--Si viene del fast cgi, entonces viene con DATA_INPUT, sino lo sacamos de query string
	data1:=get_campo('DATA_INPUT',xml2);
	if length(data1)=0 then
		data1:=get_campo('QUERY_STRING',xml2);
	end if;
	xml2 := logapp(xml2,'GET TRAZA='||data1);
	--Si viene &amp; , lo cambiamos por &
	if (strpos(data1,'&amp;')>0) then
		data1:=replace(data1,'&amp;','&');
	end if;
	xml2 := put_campo(xml2,'INPUT',data1);
	xml2 := procesa_evento_traza_get(xml2);
	--El evento de Lectura y los que comienzan con ECM_ van al sistema ECM
 	if get_campo('EVENTO',xml2) = 'LMA' or strpos(get_campo('EVENTO',xml2),'ECM_')>0 then
		xml2 := logapp(xml2,'Se va al sistema ECM');
		xml2 := put_campo(xml2,'__SECUENCIAOK__','15');
		return xml2;
	end if;

	--Si el evento ya esta registrado, no vamos a traza para no dejar la caga
	if (get_campo('EVENTO_REPETIDO',xml2)='SI') then
                xml2 := logapp(xml2,'NO vamos a traza por evento repetido, se contesta OK');
                xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
    		xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK'||chr(10)||
		 'Content-type: text/html; charset=iso-8859-1'||chr(10)||
		 'Content-length: 0'||chr(10)||
		 'Vary: Accept-Encoding'||chr(10)||chr(10));
		if get_campo('EVENTO',xml2) = 'LMA' then
			xml2:=put_campo(xml2,'RESPUESTA','Status: 302 Found'||chr(10)||
                	 'Location: https://traza.acepta.com/imgs/blank.png'||chr(10)||chr(10));
		end if;
                return xml2;
        end if;

	--FAY,RME ya no vamos a traza antigua 20150209
	xml2 := logapp(xml2,'Contesto 200 OK');
	xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK'||chr(10)||
                'Content-type: text/html; charset=iso-8859-1'||chr(10)||
                'Content-length: 0'||chr(10)||
                'Vary: Accept-Encoding'||chr(10)||chr(10));
	if get_campo('EVENTO',xml2) = 'LMA' then
		xml2:=put_campo(xml2,'RESPUESTA','Status: 302 Found'||chr(10)||
               	 'Location: https://traza.acepta.com/imgs/blank.png'||chr(10)||chr(10));

	end if;
	xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
	return xml2;

	/*
	if (get_campo('FALLA_TRAZA',xml2)='SI') then
		xml2 := logapp(xml2,'NO vamos a traza');
    		xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
		return xml2;
	end if;
	--El evento EMA esta con error en TRAZA antigua, no se carga
	if (get_campo('EVENTO',xml2)='EMA') then	
		xml2 := logapp(xml2,'Evento EMA no va a traza, responde OK');
		--Se Responde OK
		xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK'||chr(10)||
                'Content-type: text/html; charset=iso-8859-1'||chr(10)||
                'Content-length: 0'||chr(10)||
                'Vary: Accept-Encoding'||chr(10)||chr(10));
		xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
		return xml2;
	end if;
        header1:='GET '||url1||data1||' HTTP/1.1'||chr(10)||'Host: '||host1||chr(10)||'Content-Type: '||get_campo('CONTENT_TYPE',xml2)||chr(10)||'Content-Length: 0'||chr(10)||chr(10);
	xml2:=logapp(xml2,'Mensaje GET '||header1);
        xml2:=put_campo(xml2,'INPUT',encode(header1::bytea,'hex')::varchar);

	--Llamamos la funcion que verifica si el evento tienen DND o no
	xml2:=verifica_evento_dnd(xml2);
	--Solo vamos atraza si en un evento necesario para el DND
	if (get_campo('EVENTO_DND',xml2)='NO') then
		xml2 := logapp(xml2,'No vamos a traza, Rut sin DND');
		--Se Responde OK
		xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK'||chr(10)||
                'Content-type: text/html; charset=iso-8859-1'||chr(10)||
                'Content-length: 0'||chr(10)||
                'Vary: Accept-Encoding'||chr(10)||chr(10));
		xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
		return xml2;
	end if;
	return xml2;
	*/
    --Si es GET del nagios para controlor
    elsif ((get_campo('REQUEST_METHOD',xml2)='GET') and (strpos(get_campo('HTTP_USER_AGENT',xml2),'check_http')>0)) then
		xml2 := logapp(xml2,'Nagios Check');
		--Se Responde OK
		xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK'||chr(10)||
                'Content-type: text/html; charset=iso-8859-1'||chr(10)||
                'Content-length: 0'||chr(10)||
                'Vary: Accept-Encoding'||chr(10)||chr(10));
		xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
		return xml2;

    --Si viene un archivo de ReportSIILibroCompraVentaProducerBean
    elsif (strpos(data1,'ReportSIILibroCompraVentaProducerBean')>0) then
	xml2:=logapp(xml2,'Recibe ReportSIILibroCompraVenta');	
	xml2:=procesa_evento_traza_libro(xml2);
    	xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
	return xml2;	
    elsif (get_campo('CATEGORIA',xml2)='RECLAMO_SII') then
	xml2:=logapp(xml2,'RECLAMO SII:Recibe Respuesta Lista Eventos SII');
	
        xml2 :=put_campo(xml2,'JSON_OUT',get_campo('response',xml2));
	xml2:=logapp(xml2,'RECLAMO_SII:'||get_campo('response',xml2));
        xml2 :=parseo_respuesta_sii_16100(xml2);
	if (strpos(get_campo('RESPUESTA',xml2),'Status: 200 OK')>0 or strpos(get_campo('RESPUESTA',xml2),'Status: 444')>0) then
		xml2:=logapp(xml2,'RECLAMO SII: Status OK');
		xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK'||chr(10)||
               'Content-type: text/html; charset=iso-8859-1'||chr(10)||
                'Content-length: 0'||chr(10)||
                'Vary: Accept-Encoding'||chr(10)||chr(10));
	else
		xml2:=logapp(xml2,'RECLAMO SII: Status Falla');
		xml2:=put_campo(xml2,'RESPUESTA','Status: 400 NK'||chr(10)||
               'Content-type: text/html; charset=iso-8859-1'||chr(10)||
                'Content-length: 0'||chr(10)||
                'Vary: Accept-Encoding'||chr(10)||chr(10));
	end if;
	xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
	return xml2;
    else
	--Si no es nada de lo de arriba, es basura
    	xml2 := put_campo(xml2,'__SECUENCIAOK__','500');
	xml2 := logapp(xml2,'Recibe Tx No Identificada (12701)');
        xml2:=put_campo(xml2,'INPUT',replace(xml2,'###','$-$'));
	return xml2;
    end if;
    --Debo Agregar el header a INPUT para que el resto funcione OK
    header1:='POST '||url1||' HTTP/1.1'||chr(10)||'Host: '||host1||chr(10)||'Content-Type: '||get_campo('CONTENT_TYPE',xml2)||chr(10)||'Content-Length: '||get_campo('CONTENT_LENGTH',xml2)||chr(10)||chr(10);
    xml2:=put_campo(xml2,'INPUT',header1||data1);
    RETURN xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION proc_respuesta_traza_12701(varchar) RETURNS varchar AS $$
DECLARE
    xml1	alias for $1;
    xml2	varchar;
	data1	varchar;
	status1	varchar;
	respuesta1	varchar;
BEGIN
	--Cambio la respuesta de cuadratura por la respuesta original
	xml2:=xml1;
	respuesta1:='';
	if (get_campo('TIPO_TX',xml2)='CA4DND') then
		
		--Si es una respuesta del resource (para Webiecv, contestamos no chunked)
		xml2:=logapp(xml2,get_campo('REQUEST_URI',xml2));
		if (strpos(get_campo('REQUEST_URI',xml2),'TraceBoot/resource')>0) then
			xml2:=logapp(xml2,'Cambia Respuesta Chunked');
			--xml2:=respuesta_no_chunked(xml2);
		end if;
		--Para todos cambia la respuesta a no chunked
		xml2:=logapp(xml2,'Cambia Respuesta Chunked');
		xml2:=respuesta_no_chunked(xml2);
		data1:=get_campo('RESPUESTA_HEX',xml2);
		xml2:=logapp(xml2,data1);


		--Si es del DND responde igual que traza	
		if (strpos(data1,encode('200 OK'::bytea,'hex'))>0) then
			xml2 := logapp(xml2,'Respuesta CA4DND OK');
    			xml2:=put_campo(xml2,'RESPUESTA_HEX',replace(data1,encode('HTTP/1.1 200 OK'::bytea,'hex')::varchar,encode('Status: 200 OK'::bytea,'hex')::varchar));
			xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
        		--xml2:=logapp(xml2,replace(xml2,'###','$-$'));
			return xml2;
		else
			status1:='Status: 400 Rechazado'||chr(10)||'Content-type: text/html; charset=iso-8859-1'||chr(10)||'Content-length: '||length(respuesta1)||chr(10);
			xml2 := logapp(xml2,'Falla Envio a Traza DND');
    			xml2:=put_campo(xml2,'RESPUESTA',status1||chr(10)||respuesta1);
		end if;
		xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
		return xml2;
	end if;
        --xml2:=logapp(xml2,replace(xml2,'###','$-$'));
        data1:=get_campo('RESPUESTA',xml2);
	
    	if (strpos(data1,'200 OK')>0) then
		xml2 := logapp(xml2,'Evento Traza Enviado OK');
		status1:='Status: 200 OK'||chr(10)||
		 'Content-type: text/html; charset=iso-8859-1'||chr(10)||
		 'Content-length: '||length(respuesta1)||chr(10)||
		 'Vary: Accept-Encoding'||chr(10);
		
	else
		status1:='Status: 400 Rechazado'||chr(10)||'Content-type: text/html; charset=iso-8859-1'||chr(10)||'Content-length: '||length(respuesta1)||chr(10);
		xml2 := logapp(xml2,'Falla Envio a Traza');
		xml2:=logapp(xml2,data1);
	end if;
	xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
    	xml2:=put_campo(xml2,'RESPUESTA',status1||chr(10)||respuesta1);
	--xml2 := logapp(xml2,status1||chr(10)||respuesta1);
	return xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION proc_respuesta_fcgi_12701(varchar) RETURNS varchar AS $$
DECLARE
    xml1	alias for $1;
    xml2	varchar;
    data1	varchar;
    file1	varchar;
    status1	varchar;
    sts		integer;
    respuesta1	varchar;
    output1	varchar;
BEGIN
    xml2:='';

    data1:=get_campo('RESPUESTA',xml1);
    respuesta1:=split_part(data1,chr(10)||chr(10),2);
    xml2 := put_campo(xml2,'__SECUENCIAOK__','0');


    if (strpos(data1,'200 OK')>0) then
	status1:='Status: 200 OK'||chr(10)||
		 'Content-type: text/html'||chr(10)||
		 'Content-Location: '||get_campo('URI',xml1)||chr(10)||
		 'Content-length: '||length(respuesta1)||chr(10);
	xml2 := logapp(xml2,'Respuesta Servicio 200 OK');
    else
	--Si es un estado EDTE no conteste, el EDTE acepta cualqueir respuesta como valida
	if get_campo('TIPO_TX',xml2)='ESTADO_EDTE' then
    		xml2:=put_campo(xml2,'RESPUESTA','');
		xml2 := logapp(xml2,'No responde a EDTE');
		return xml2;
	else
		status1:='Status: 400 Rechazado'||chr(10)||'Content-type: text/html'||chr(10)||'Content-length: '||length(respuesta1)||chr(10);
		xml2 := logapp(xml2,'Respuesta Servicio 400 Rechazado (127010)');
	end if;
    end if;
    xml2:=put_campo(xml2,'RESPUESTA',status1||chr(10)||respuesta1);
    --En caso de ser un reproceso, debo agregar los datos al XML de salida
    xml2:=put_campo(xml2,'_ID_REPROCESO_',get_campo('_ID_REPROCESO_',xml1));
    xml2:=put_campo(xml2,'_ESTADO_REPROCESO_',get_campo('_ESTADO_REPROCESO_',xml1));
    RETURN xml2;
END;
$$ LANGUAGE plpgsql;



CREATE or replace FUNCTION procesa_respuesta_webiecv_12701(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2        varchar;
    data1       varchar;
    file1       varchar;
    status1     varchar;
    sts         integer;
    respuesta1  varchar;
    output1     varchar;
BEGIN
    xml2:='';

    data1:=get_campo('RESPUESTA_IECV',xml1);
    respuesta1:=split_part(data1,chr(10)||chr(10),2);
    xml2 := put_campo(xml2,'__SECUENCIAOK__','0');

    if (strpos(data1,'200 OK')>0) then
        status1:='Status: 200 OK'||chr(10)||
                 'Content-type: text/html'||chr(10)||
                 'Content-Location: '||get_campo('URI',xml1)||chr(10)||
                 'Content-length: '||length(respuesta1)||chr(10);
        xml2 := logapp(xml2,'IECV: Respuesta Servicio 200 OK');
    else
        status1:='Status: 400 Rechazado'||chr(10)||'Content-type: text/html'||chr(10)||'Content-length: '||length(respuesta1)||chr(10);
        xml2 := logapp(xml2,'IECV: Respuesta Servicio 400 Rechazado (127010)');
    end if;
    xml2:=put_campo(xml2,'RESPUESTA',status1||chr(10)||respuesta1);
    --En caso de ser un reproceso, debo agregar los datos al XML de salida
    xml2:=put_campo(xml2,'_ID_REPROCESO_',get_campo('_ID_REPROCESO_',xml1));
    xml2:=put_campo(xml2,'_ESTADO_REPROCESO_',get_campo('_ESTADO_REPROCESO_',xml1));
    RETURN xml2;

END;
$$ LANGUAGE plpgsql;

