--Publica documento
delete from isys_querys_tx where llave='12771';

insert into isys_querys_tx values ('12771',5,19,1,'select control_flujo_80101(''$$__JSONCOMPLETO__["__PROC_ACTIVOS__","TX","REQUEST_URI","__ARGV__","__CATEGORIA_COLA__","__FLUJO_ACTUAL__"]$$''::json) as __json__',0,0,0,1,1,-1,10);

insert into isys_querys_tx values ('12771',15,19,1,'select pivote_12771(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('12771',17,1,1,'select lee_datos_base_12771(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('12771',19,1,8,'Flujo LeeTraza ',8070,0,0,0,0,25,25);

insert into isys_querys_tx values ('12771',25,19,1,'select lista_mandato_v2_12771(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

--Arma lista de mails y genera contador en 0
insert into isys_querys_tx values ('12771',10,19,1,'select lista_mandato_12771(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

--Armo correo para el mail[contador]
insert into isys_querys_tx values ('12771',20,19,1,'select arma_correo_mandato_12771(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

--Envia mandato mail[contador] al ECM
insert into isys_querys_tx values ('12771',30,19,1,'select envia_correo_12771(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

--Borra de la cola
insert into isys_querys_tx values ('12771',40,19,1,'select sp_procesa_respuesta_cola_motor88_json(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

CREATE or replace FUNCTION  pivote_12771(json) RETURNS json AS $$
DECLARE
        json1   alias for $1;
        json2   json;
        json3   json;
	data1	varchar;
	uri1	varchar;
	largo1 	integer;	
	pos_inicial1	integer;
	pos_final1	integer;
BEGIN
	json2:=json1;
	json2:=put_json(json2,'__SECUENCIAOK__','0');
        --Solo si tiene DTE con mandato
        if (get_json('__DTE_CON_MANDATO__',json2)<>'SI') then
                return json2;
        end if;
        uri1:=get_json('URI_IN',json2);
    	if (length(uri1)=0) then
		json2 := logjson(json2,'No viene URI_IN, no se puede publicar');
		json2 := put_json(json2,'__MENSAJE_10K__','DTE sin Uri');
		json2 := put_json(json2,'__EDTE_MANDATO_OK__','NO');
		json2:=logjson(json2,'Se borra mandato edte de la cola');
		json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
		return sp_procesa_respuesta_cola_motor88_json(json2);
	end if;
	--Si hay q hacer proxy, solo leemos los EMA
	json2:=put_json(json2,'FILTRO_LEE_TRAZA_HEX',encode_hex(' evento=''EMA'' '));

	if (get_json('INPUT_CUSTODIUM',json2)='') then
        	data1:=get_json('INPUT',json2);
            	if (length(data1)=0) then
			json2:=logjson(json2,'Obtengo INPUT_CUSTOIUM '|| uri1);
                	--Sacamos la data del almacen
                   	json3:=put_json('{}','uri',uri1);
                   	data1:=get_input_almacen(json3::varchar);
                   	if (length(data1)=0) then
				json2 := logjson(json2,'Falla Lectura de la uri en el almacen');
				json2 := put_json(json2,'__EDTE_MANDATO_OK__','NO');
				json2 := put_json(json2,'__MENSAJE_10K__','Falla Lectura de DTE en Almacen');
				json2:=put_json(json2,'RESPUESTA','Status: 400 NK');
				return sp_procesa_respuesta_cola_motor88_json(json2);
                   	end if;
	     	else
			json2:=logjson(json2,'Viene INPUT '|| uri1);
                	--Nuevo Procedimiento
                    	largo1:=get_json('CONTENT_LENGTH',json2)::integer*2;
                    	--Busco donde empieza <?xml version
                    	pos_inicial1:=strpos(data1,'3c3f786d6c2076657273696f6e3d');
                    	--Buscamos al reves donde esta el primer signo > que en hex es 3e
                    	--Como se pone un reverse se busca e3
                    	pos_final1:=largo1-pos_inicial1+4-strpos(reverse(data1),'e3');
                    	data1:=substring(data1,pos_inicial1,pos_final1);
            	end if;
            	json2:=put_json(json2,'INPUT_CUSTODIUM',data1);
		json2:=put_json(json2,'__SECUENCIAOK__','17');
		json2:=logjson(json2,'Vamos a motor a parsear');
	else
		if(get_json('__FLAG_PUB_10K__',json2)<>'SI' and get_json('__FLAG_REENVIO_MANDATO__',json2)<>'SI') then
			json2:=logjson(json2,'Vamos a leer la traza para el proxy');
			json2:=put_json(json2,'__SECUENCIAOK__','19');
		else
			return lista_mandato_v2_12771(json2);
		end if;
    	end if;
    	return json2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION  lee_datos_base_12771(json) RETURNS json AS $$
DECLARE
        json1   alias for $1;
        json2   json;
	uri1	varchar;
	xml3	varchar;
BEGIN
	json2:=json1;
        json2:=put_json(json2,'__SECUENCIAOK__','0');
        uri1:=get_json('URI_IN',json2);
	xml3:='';
	--Agregamos el TAG URI para el Parseo de Datos 
	xml3:=put_campo(xml3,'INPUT',get_json('INPUT_CUSTODIUM',json2)||encode(('<URI>filename="'||uri1||'"</URI>')::bytea,'hex'));
	xml3:=put_campo(xml3,'URI_IN',uri1);
	xml3:=reglas.parseo_datos(xml3);
	xml3:=put_campo(xml3,'INPUT','');
	--Saco los datos que necesito y los copio al json	
	json2:=put_json(json2,'MANDATO_EMAIL',get_campo('MANDATO_EMAIL',xml3));
	json2:=put_json(json2,'TOTAL_CASILLAS',get_campo('TOTAL_CASILLAS',xml3));
	json2:=put_json(json2,'RUT_EMISOR',get_campo('RUT_EMISOR',xml3));
	json2:=put_json(json2,'RUT_EMISOR_DV',get_campo('RUT_EMISOR_DV',xml3));
	json2:=put_json(json2,'TIPO_DTE',get_campo('TIPO_DTE',xml3));
	json2:=put_json(json2,'FOLIO',get_campo('FOLIO',xml3));
	json2:=put_json(json2,'RUT_RECEPTOR',get_campo('RUT_RECEPTOR',xml3));
	json2:=put_json(json2,'RUT_RECEPTOR_DV',get_campo('RUT_RECEPTOR_DV',xml3));
	json2:=put_json(json2,'FECHA_EMISION',get_campo('FECHA_EMISION',xml3));
	json2:=put_json(json2,'DOMINIO',get_campo('DOMINIO',xml3));
	json2:=put_json(json2,'MANDATO_MAIL_EMISOR',get_campo('MANDATO_MAIL_EMISOR',xml3));
	json2:=put_json(json2,'XML_FILTROS_ADICIONALES',get_campo('XML_FILTROS_ADICIONALES',xml3));
	json2:=put_json(json2,'MANDATO_CICLO',get_campo('MANDATO_CICLO',xml3));
	xml3:=reglas.maestro_clientes(xml3);
	json2:=logjson(json2,get_campo('_LOG_',xml3));
	json2:=put_json(json2,'DTE_MANDATO',get_campo('DTE_MANDATO',xml3));
	json2:=put_json(json2,'DTE_MANDATO_PDF',get_campo('DTE_MANDATO_PDF',xml3));
	json2:=put_json(json2,'DTE_MANDATO_XML',get_campo('DTE_MANDATO_XML',xml3));
	json2:=put_json(json2,'DTE_MANDATO_PDF_CLAVE',get_campo('DTE_MANDATO_PDF_CLAVE',xml3));
	json2:=put_json(json2,'DTE_MANDATO_PDF_TIPO_CLAVE',get_campo('DTE_MANDATO_PDF_TIPO_CLAVE',xml3));
	json2:=put_json(json2,'XSL_MANDATO',get_campo('XSL_MANDATO',xml3));
	--RME 20170713 Se agregan los datos de Casilla Digital
	json2:=put_json(json2,'FLAG_CASILLA_DIGITAL',get_campo('FLAG_CASILLA_DIGITAL',xml3));
	if(get_json('__FLAG_PUB_10K__',json2)<>'SI' and get_json('__FLAG_REENVIO_MANDATO__',json2)<>'SI') then
		json2:=put_json(json2,'__SECUENCIAOK__','19');
	else
        	json2:=put_json(json2,'__SECUENCIAOK__','25');
	end if;
	return json2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION  lista_mandato_v2_12771(json) RETURNS json AS $$
DECLARE
	json1	alias for $1;
	json2 	json;
	total1	integer;
	uri1	varchar;
	data1	varchar;
	mail1	varchar;
	campo	record;
	json4	json;
	sts1	varchar;
	pos	integer;
	subject1	varchar;
	jtraza	json;
	aux1	varchar;
	i	integer;
	flag_envio_mandato	boolean;
	flag_cd 	json;
	json_aux	json;
	lista_mail1	json;
	flag_ya_enviado1	boolean;
BEGIN
	json2:=json1;
        json2:=put_json(json2,'__SECUENCIAOK__','0');
        uri1:=get_json('URI_IN',json2);
        data1:=get_json('INPUT_CUSTODIUM',json2);
	--json2:=logjson(json2,'DATA1='||data1::varchar);

   	--Sacamos el subject
   	pos=strpos(data1,encode('>Subject_Mail<','hex'));
   	if (pos=0) then
        	subject1:=split_part(split_part(data1,encode('<DatoAdjunto nombre="Subject_Mail">','hex'),2),encode('</DatoAdjunto>','hex'),1);
   	else
        	subject1:=split_part(split_part(substring(data1,pos,length(data1)),encode('<ValorDA>','hex'),2),encode('</ValorDA>','hex'),1);
   	end if;
    	mail1:=get_json('MANDATO_EMAIL',json2);
   	json2:=logjson(json2,'URI '||uri1||' FLAG_CASILLA_DIGITAL -->'||get_json('FLAG_CASILLA_DIGITAL',json2));

    	--RME 20170713 Se manejan las excepciones de Casilla Digital.
    	if get_json('FLAG_CASILLA_DIGITAL',json2)<>'' then
		json_aux:=decode(get_json('FLAG_CASILLA_DIGITAL',json2),'hex')::varchar::json;
		if (get_json(get_json('TIPO_DTE',json2),json_aux)<>'') then
			flag_cd:=get_json(get_json('TIPO_DTE',json2),json_aux)::json;
			subject1:=encode(get_json('ASUNTO',flag_cd)::bytea,'hex');
			json2:=put_json(json2,'MANDATO_MAIL_EMISOR',get_json('REMITENTE',flag_cd));
			aux1:=get_json('DTE_MANDATO',json2);
			if get_json('MANDATO',flag_cd)='SI' and strpos(aux1,'ALL')=0 and strpos(aux1,get_json('TIPO_DTE',json2))=0 then	
				json2:=put_json(json2,'DTE_MANDATO','ALL');
				--solo se envia a casilla
				mail1:=get_json('RUT_RECEPTOR',json2)||'@casilladigital.cl';
			else
				json2:=logjson(json2,'Falla condicion 2');
			end if;
		else
			json2:=logjson(json2,'Falla condicion 1');
		end if;
    	end if;	

    	--Mandatos que vienen desde Escritorio
    	if(get_json('__FLAG_PUB_10K__',json2)='SI') then
		-- Puede venir por PANTALLA
		json2:=logjson(json2,'Entra a __FLAG_PUB_10K__');
		mail1:=get_json('mailMandato',json2);
		subject1:=encode(('Documento de ACEPTA COM S A: Folio '||ltrim(get_json('FOLIO',json2),'0')||' de '||get_json('FECHA_EMISION',json2))::bytea,'hex');
		json2:=put_json(json2,'MANDATO_MAIL_EMISOR','noreply@acepta.com');
    	end if;
    	json2:=put_json(json2,'SUBJECT',subject1);
    	json2:=logjson(json2,'Mandatos '||mail1||' TotalCasillas='||get_json('TOTAL_CASILLAS',json2)||' URI='||uri1);

    	i:=0;
    	lista_mail1:='[]'; 
	--Para verificar que se envia algun mandato
	flag_envio_mandato:=false;
    	for campo in select distinct trim(mail) as mail from (select * from regexp_split_to_table(mail1,'[\,,\;]') mail) x where length(mail)>0 loop
		flag_envio_mandato:=true;
		json2:=logjson(json2,'Mandato='||campo.mail);
		--Validamos el correo
		if (valida_email(campo.mail) is false) then
			json2:=logjson(json2,'Mail Invalido '||campo.mail);
			--Si viene desde las colas
			if (get_json('__FLAG_REINTENTO_MANDATO__',json2)='SI') then
			    json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
			    return sp_procesa_respuesta_cola_motor88_json(json2);
			end if;
			continue;
		end if;

		json2:=logjson(json2,'__FLAG_REENVIO_MANDATO__='||get_json('__FLAG_REENVIO_MANDATO__',json2));
		if(get_json('__FLAG_PUB_10K__',json2)<>'SI' and get_json('__FLAG_REENVIO_MANDATO__',json2)<>'SI') then
			--Revisamos dentro de los datos leidos
			if get_json('STATUS_LEE_TRAZA',json2)='OK' then
				jtraza:=get_json('RESPUESTA_LEE_TRAZA',json2)::json;
				json2:=logjson(json2,'Verificamos que no tenga evento en la traza para ese correo '||jtraza::varchar);
        			i:=0;
				aux1:=get_json_index(jtraza,i);
				flag_ya_enviado1:=False;
				while(aux1<>'') loop
					if get_json('evento',aux1::json)='EMA' and strpos(get_json('comentario1',aux1::json),campo.mail)>0 then
						json2:=logjson(json2,'Mail ya Enviado OK');
						json2:=put_json(json2,'__EDTE_MANDATO_OK__','SI');
						--Si ya existe el envio de mandato
						if (get_json('__FLAG_REINTENTO_MANDATO__',json2)='SI') then
						    	json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
						    	return sp_procesa_respuesta_cola_motor88_json(json2);
						end if;
						flag_ya_enviado1:=True;
						EXIT;
					end if;
					i:=i+1;
					aux1:=get_json_index(jtraza,i);
				end loop;
        		end if;
			if flag_ya_enviado1 then
				continue;
			end if;
		end if;
		lista_mail1:=put_json_list(lista_mail1,'"'||campo.mail||'"');
    	end loop;

    	json2:=logjson(json2,'Lista Mail '||lista_mail1::varchar);
    	json2:=put_json(json2,'__SECUENCIAOK__','20');
    	json2:=put_json(json2,'LISTA_MANDATO',lista_mail1::varchar);
    	json2:=put_json(json2,'CONTADOR_MANDATO','0');
    	return json2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION  lista_mandato_12771(json) RETURNS json AS $$
DECLARE
	json1	alias for $1;
	json2 	json;
	total1	integer;
	uri1	varchar;
	hash1	varchar;
	ver_dcto_attrib	varchar;
	data1	varchar;
	largo1	integer;
	pos_inicial1	integer;
	pos_final1	integer;
	mail1	varchar;
	campo	record;
	json4	json;
	sts1	varchar;
	pos	integer;
	subject1	varchar;
	json_out1	json;
	comentario1	varchar;
	xml3	varchaR;
	cola1	varchar;
	nombre_tabla1	varchar;
	tx1	varchar;
	rut1	varchar;	
	evento1	varchar;
	id1	varchar;
	aux1	varchar;
	comentario_traza1	varchar;
	json3	json;
	jsonsts1	json;
	i	integer;
	xml4	varchar;
	url_get1	varchar;
	data_lma	varchar;
	json_par1	json;
	flag_envio_mandato	boolean;
        razon_social_receptor1  varchar;
	rut_aux1	varchar;
	json_par2	json;
	j3	json;
	flag_cd 	json;
	id_ecm1		varchar;
	uri_short1	varchar;
	
	juri	json;
	encr1	varchar;
	json_aux	json;
	lista_mail1	json;
BEGIN
	json2:=json1;
	/*
	if exists(select 1 from tmp3) is false then
		insert into tmp3 values (get_json('URI_IN',json2));
		return pivote_12771(json2);
	end if;*/
	return pivote_12771(json2);
        json2:=put_json(json2,'__SECUENCIAOK__','0');


        --Solo si tiene DTE con mandato
	if (get_json('__DTE_CON_MANDATO__',json2)<>'SI') then
        	return json2;
	end if;
	
        uri1:=get_json('URI_IN',json2);

    --20150224 FAY Si no viene URI no se puede publicar
    if (length(uri1)=0) then
	json2 := logjson(json2,'No viene URI_IN, no se puede publicar');
	json2 := put_json(json2,'__MENSAJE_10K__','DTE sin Uri');
        json2 := put_json(json2,'__EDTE_MANDATO_OK__','NO');
        json2:=logjson(json2,'Se borra mandato edte de la cola');
      	json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
        json2:=put_json(json2,'__SECUENCIAOK__','40');
	return json2;	
    end if;


    if (get_json('INPUT_CUSTODIUM',json2)='') then
            data1:=get_json('INPUT',json2);
	    if (length(data1)=0) then
		   --Sacamos la data del almacen
		   json3:=put_json('{}','uri',uri1);
		   data1:=get_input_almacen(json3::varchar);
		   if (length(data1)=0) then
			json2 := logjson(json2,'Falla Lectura de la uri en el almacen');
		 	json2 := put_json(json2,'__EDTE_MANDATO_OK__','NO');
			json2 := put_json(json2,'__MENSAJE_10K__','Falla Lectura de DTE en Almacen');
		        id1:=get_json('__ID_DTE__',json2);
                        --Si viene de un reintento, aumento reintentos
                        json2:=logjson(json2,'Aumenta Reintentos Envio de Mandato Edte');
                        execute 'update '||get_json('__COLA_MOTOR__',json2)||' set reintentos=reintentos+1 where id='||id1;
			return json2;
		   end if;
		   data1:=data1||encode(('<URI>filename="'||uri1||'"</URI>')::bytea,'hex');
		   --Este caso no tenia input, asi lo proceso nuevamente
		   xml3:='';
		   xml3:=put_campo(xml3,'INPUT',data1);
		   xml3:=put_campo(xml3,'URI_IN',uri1);
		   json_par2:=get_parametros_motor_json('{}','BASE_MOTOR');
		   --json3:=query_db_json('172.16.14.177',8001,'select reglas.parseo_datos('||quote_literal(xml3)||') as xml3');
		   json3:=query_db_json(get_json('__IP_CONEXION_CLIENTE__',json_par2),get_json('__IP_PORT_CLIENTE__',json_par2)::integer,'select reglas.parseo_datos('||quote_literal(xml3)||') as xml3');
		   if (get_json('STATUS',json3)<>'OK') then
			json2 := logjson(json2,'Falla ejecucion reglas.parseo_datos');
			json2 := put_json(json2,'__MENSAJE_10K__','Falla ejecucion, por favor reintente.');
		 	json2 := put_json(json2,'__EDTE_MANDATO_OK__','NO');
		        id1:=get_json('__ID_DTE__',json2);
                        --Si viene de un reintento, aumento reintentos
                        json2:=logjson(json2,'Aumenta Reintentos Envio de Mandato Edte');
                        execute 'update '||get_json('__COLA_MOTOR__',json2)||' set reintentos=reintentos+1 where id='||id1;
			return json2;
		   end if;
		   --Corrijo el xml
		   data1:=split_part(data1,encode(('<URI>filename="')::bytea,'hex'),1);
		   xml3:=get_json('xml3',json3);
		   xml3:=put_campo(xml3,'INPUT','');
		   --Saco los datos que necesito y los copio al json	
		   json2:=put_json(json2,'MANDATO_EMAIL',get_campo('MANDATO_EMAIL',xml3));
		   json2:=put_json(json2,'TOTAL_CASILLAS',get_campo('TOTAL_CASILLAS',xml3));
		   json2:=put_json(json2,'RUT_EMISOR',get_campo('RUT_EMISOR',xml3));
		   json2:=put_json(json2,'RUT_EMISOR_DV',get_campo('RUT_EMISOR_DV',xml3));
		   json2:=put_json(json2,'TIPO_DTE',get_campo('TIPO_DTE',xml3));
		   json2:=put_json(json2,'FOLIO',get_campo('FOLIO',xml3));
		   json2:=put_json(json2,'RUT_RECEPTOR',get_campo('RUT_RECEPTOR',xml3));
		   json2:=put_json(json2,'RUT_RECEPTOR_DV',get_campo('RUT_RECEPTOR_DV',xml3));
		   json2:=put_json(json2,'FECHA_EMISION',get_campo('FECHA_EMISION',xml3));
		   json2:=put_json(json2,'DOMINIO',get_campo('DOMINIO',xml3));
		   json2:=put_json(json2,'MANDATO_MAIL_EMISOR',get_campo('MANDATO_MAIL_EMISOR',xml3));
		   json2:=put_json(json2,'XML_FILTROS_ADICIONALES',get_campo('XML_FILTROS_ADICIONALES',xml3));
		   json2:=put_json(json2,'MANDATO_CICLO',get_campo('MANDATO_CICLO',xml3));
		   json_par2:=get_parametros_motor_json('{}','BASE_MOTOR');
		   json3:=query_db_json(get_json('__IP_CONEXION_CLIENTE__',json_par2),get_json('__IP_PORT_CLIENTE__',json_par2)::integer,'select reglas.maestro_clientes('||quote_literal(xml3)||') as xml3');
		   --json3:=query_db_json('172.16.14.177',8001,'select reglas.maestro_clientes('||quote_literal(xml3)||') as xml3');
		   if (get_json('STATUS',json3)<>'OK') then
			json2 := logjson(json2,'Falla Lectura del maestro de clientes');
			json2 := put_json(json2,'__MENSAJE_10K__','Falla ejecucion, por favor reintente*.');
		 	json2 := put_json(json2,'__EDTE_MANDATO_OK__','NO');
		        id1:=get_json('__ID_DTE__',json2);
                        --Si viene de un reintento, aumento reintentos
                        json2:=logjson(json2,'Aumenta Reintentos Envio de Mandato Edte');
                        execute 'update '||get_json('__COLA_MOTOR__',json2)||' set reintentos=reintentos+1 where id='||id1;
			return json2;
		   end if;
		   xml3:=get_json('xml3',json3);
		   --Leo datos del maestro de lciente
		   --perform logfile('MANDATO 3 '||xml3);
		   json2:=put_json(json2,'DTE_MANDATO',get_campo('DTE_MANDATO',xml3));
		   json2:=put_json(json2,'DTE_MANDATO_PDF',get_campo('DTE_MANDATO_PDF',xml3));
		   json2:=put_json(json2,'DTE_MANDATO_XML',get_campo('DTE_MANDATO_XML',xml3));
		   json2:=put_json(json2,'DTE_MANDATO_PDF_CLAVE',get_campo('DTE_MANDATO_PDF_CLAVE',xml3));
		   json2:=put_json(json2,'DTE_MANDATO_PDF_TIPO_CLAVE',get_campo('DTE_MANDATO_PDF_TIPO_CLAVE',xml3));
		   json2:=put_json(json2,'XSL_MANDATO',get_campo('XSL_MANDATO',xml3));
		   --RME 20170713 Se agregan los datos de Casilla Digital
		   json2:=put_json(json2,'FLAG_CASILLA_DIGITAL',get_campo('FLAG_CASILLA_DIGITAL',xml3));		
	    else
	            --Nuevo Procedimiento
        	    largo1:=get_json('CONTENT_LENGTH',json2)::integer*2;
	            --Busco donde empieza <?xml version
        	    pos_inicial1:=strpos(data1,'3c3f786d6c2076657273696f6e3d');
	            --Buscamos al reves donde esta el primer signo > que en hex es 3e
        	    --Como se pone un reverse se busca e3
	            --xml2:=logapp(xml2,'MAGIA:'||strpos(reverse(data1),'e3')::varchar);
        	    pos_final1:=largo1-pos_inicial1+4-strpos(reverse(data1),'e3');
	            data1:=substring(data1,pos_inicial1,pos_final1);
	    end if;
	    json2:=put_json(json2,'INPUT_CUSTODIUM',data1);
    else
            data1:=get_json('INPUT_CUSTODIUM',json2);
    end if;

   --Sacamos el subject
   pos=strpos(data1,encode('>Subject_Mail<','hex'));
   if (pos=0) then
          subject1:=split_part(split_part(data1,encode('<DatoAdjunto nombre="Subject_Mail">','hex'),2),encode('</DatoAdjunto>','hex'),1);
   else
         subject1:=split_part(split_part(substring(data1,pos,length(data1)),encode('<ValorDA>','hex'),2),encode('</ValorDA>','hex'),1);
   end if;
	
    mail1:=get_json('MANDATO_EMAIL',json2);


   json2:=logjson(json2,'FLAG_CASILLA_DIGITAL -->'||get_json('FLAG_CASILLA_DIGITAL',json2));


    --RME 20170713 Se manejan las excepciones de Casilla Digital.
    if get_json('FLAG_CASILLA_DIGITAL',json2)<>'' then
	json_aux:=decode(get_json('FLAG_CASILLA_DIGITAL',json2),'hex')::varchar::json;
	if (get_json(get_json('TIPO_DTE',json2),json_aux)<>'') then
		flag_cd:=get_json(get_json('TIPO_DTE',json2),json_aux)::json;
		subject1:=encode(get_json('ASUNTO',flag_cd)::bytea,'hex');
		json2:=put_json(json2,'MANDATO_MAIL_EMISOR',get_json('REMITENTE',flag_cd));
		aux1:=get_json('DTE_MANDATO',json2);
		if get_json('MANDATO',flag_cd)='SI' and strpos(aux1,'ALL')=0 and strpos(aux1,get_json('TIPO_DTE',json2))=0 then	
			json2:=put_json(json2,'DTE_MANDATO','ALL');
			--solo se envia a casilla
			mail1:=get_json('RUT_RECEPTOR',json2)||'@casilladigital.cl';
		else
			json2:=logjson(json2,'Falla condicion 2');
		end if;
	else
		json2:=logjson(json2,'Falla condicion 1');
	end if;
    end if;	

    --Mandatos que vienen desde Escritorio
    if(get_json('__FLAG_PUB_10K__',json2)='SI') then
        -- Puede venir por PANTALLA
	json2:=logjson(json2,'Entra a __FLAG_PUB_10K__');
        mail1:=get_json('mailMandato',json2);
        subject1:=encode(('Documento de ACEPTA COM S A: Folio '||ltrim(get_json('FOLIO',json2),'0')||' de '||get_json('FECHA_EMISION',json2))::bytea,'hex');
        json2:=put_json(json2,'MANDATO_MAIL_EMISOR','noreply@acepta.com');
    end if;
    json2:=put_json(json2,'SUBJECT',subject1);

    json2:=logjson(json2,'Mandatos '||mail1||' TotalCasillas='||get_json('TOTAL_CASILLAS',json2)||' URI='||uri1);
    i:=0;

    lista_mail1:='[]'; 

    --Para verificar que se envia algun mandato
    flag_envio_mandato:=false;
    for campo in select distinct trim(mail) as mail from (select * from regexp_split_to_table(mail1,'[\,,\;]') mail) x where length(mail)>0 loop
        flag_envio_mandato:=true;
    	json2:=logjson(json2,'Mandato='||campo.mail);
	--Validamos el correo
	if (valida_email(campo.mail) is false) then
		json2:=logjson(json2,'Mail Invalido '||campo.mail);
		--Si viene desde las colas
    		if (get_json('__FLAG_REINTENTO_MANDATO__',json2)='SI') then
		    id1:=get_json('__ID_DTE__',json2);
        	    --Si viene de un reintento, aumento reintentos
	      	    json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
        	    json2:=put_json(json2,'__SECUENCIAOK__','40');
		    return json2;
		end if;
		continue;
	end if;

	json2:=logjson(json2,'__FLAG_REENVIO_MANDATO__='||get_json('__FLAG_REENVIO_MANDATO__',json2));
	if(get_json('__FLAG_PUB_10K__',json2)<>'SI' and get_json('__FLAG_REENVIO_MANDATO__',json2)<>'SI') then
		json2:=logjson(json2,'Verificamos que no tenga evento en la traza para ese correo');
		--Verificamos que no tenga evento en la traza para ese correo
		json_par2:=get_parametros_motor_json('{}','BASE_MOTOR');
		json_out1:=query_db_json(get_json('__IP_CONEXION_CLIENTE__',json_par2),get_json('__IP_PORT_CLIENTE__',json_par2)::integer,'select lee_traza_evento_comentario1('||quote_literal(uri1)||','||quote_literal('EMA')||','||quote_literal(campo.mail)||')');
		--json_out1:=query_db_json('172.16.14.177','8001','select lee_traza_evento_comentario1('||quote_literal(uri1)||','||quote_literal('EMA')||','||quote_literal(campo.mail)||')');
		if (get_json('STATUS',json_out1)='OK') then
			--Si hay datos, es porque ya se envio el correo
			aux1:=get_json('uri',get_json('lee_traza_evento_comentario1',json_out1)::json);
			--Ya esta el evento
			if (length(aux1)>0) then
	        	        json2:=logjson(json2,'Mail ya Enviado OK');
	                	json2:=put_json(json2,'__EDTE_MANDATO_OK__','SI');
			        --Si ya existe el envio de mandato
        	                if (get_json('__FLAG_REINTENTO_MANDATO__',json2)='SI') then
                	                id1:=get_json('__ID_DTE__',json2);
                        	        --Si viene de un reintento, aumento reintentos
                                	json2:=logjson(json2,'Se borra mandato edte de la cola');
	                                execute 'delete from '||get_json('__COLA_MOTOR__',json2)||' where id='||id1;
        	                end if;
			       continue;
			end if;
		end if;
	end if;
	lista_mail1:=put_json_list(lista_mail1,'"'||campo.mail||'"');
    end loop;

    json2:=logjson(json2,'Lista Mail '||lista_mail1::varchar);
    json2:=put_json(json2,'__SECUENCIAOK__','20');
    json2:=put_json(json2,'LISTA_MANDATO',lista_mail1::varchar);
    json2:=put_json(json2,'CONTADOR_MANDATO','0');
    return json2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION  arma_correo_mandato_12771(json) RETURNS json AS $$
DECLARE
        json1   alias for $1;
        json2   json;
        total1  integer;
        uri1    varchar;
        hash1   varchar;
        ver_dcto_attrib varchar;
        data1   varchar;
        largo1  integer;
        pos_inicial1    integer;
        pos_final1      integer;
        mail1   varchar;
        campo   record;
        json4   json;
        sts1    varchar;
        pos     integer;
        subject1        varchar;
        json_out1       json;
        comentario1     varchar;
        xml3    varchaR;
        cola1   varchar;
        nombre_tabla1   varchar;
        tx1     varchar;
        rut1    varchar;
        evento1 varchar;
        id1     varchar;
        aux1    varchar;
        comentario_traza1       varchar;
        json3   json;
        jsonsts1        json;
        i       integer;
        xml4    varchar;
        url_get1        varchar;
        data_lma        varchar;
        json_par1       json;
        flag_envio_mandato      boolean;
        razon_social_receptor1  varchar;
        rut_aux1        varchar;
        json_par2       json;
        j3      json;
        flag_cd         json;
        id_ecm1         varchar;
        uri_short1      varchar;
        juri    json;
        encr1   varchar;
        json_aux        json;
	correo1	varchar;
	contador_mail1	integer;
BEGIN
        json2:=json1;
        json2:=put_json(json2,'__SECUENCIAOK__','0');

	--Verifico si hay correos por enviar en la lista
	contador_mail1:=get_json('CONTADOR_MANDATO',json2)::integer;
	correo1:=get_json_index(get_json('LISTA_MANDATO',json2)::json,contador_mail1);

	if (correo1='') then
	      	json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
		json2:=logjson(json2,'No hay mas mails por enviar');	
		json2:=put_json(json2,'__SECUENCIAOK__','40');	
		return json2;
	end if;
	uri1:=get_json('URI_IN',json2);
	data1:=get_json('INPUT_CUSTODIUM',json2);
	subject1:=get_json('SUBJECT',json2);
		
    	json2:=logjson(json2,'Mandato Casilla --> '||correo1||' URI='||uri1);
	hash1 := encripta_hash_evento_VDC(split_part(uri1,'v01/',2)||'&v_origen='||get_json('RUT_EMISOR',json2)||'&v_tipo_dte='||get_json('TIPO_DTE',json2)||'&v_folio='||get_json('FOLIO',json2)||'&v_email='||trim(correo1)||'&v_evento=CVD'||'&v_receptor='||get_json('RUT_RECEPTOR',json2)||'&v_fecha_emi='||get_json('FECHA_EMISION',json2));
	ver_dcto_attrib:=split_part(uri1,'v01/',1)||'eventos/'||hash1;
        --se limpia el atributo EMAIL
        data1:=regexp_replace(data1,encode('<Attribute Type="EMAIL">'::bytea,'hex')::varchar||'.*'||encode('</Attribute>'::bytea,'hex')::varchar,'');
        --se agrega el mail que corresponde a esta "vuelta" y Atributo nuevo para boton "VER DOCUMENTO"
	if strpos(data1,'<Attributes>')>0 then
        	data1:=split_part(data1,encode('</Attributes>','hex'),1)||encode('<Attribute Type="EMAIL">'::bytea,'hex')::varchar || encode(trim(correo1)::bytea,'hex')::varchar||encode('</Attribute>'::bytea,'hex')::varchar||encode((chr(10)||'<Attribute Type="URIBOTON">')::bytea,'hex')::varchar||encode(ver_dcto_attrib::bytea,'hex')::varchar||encode('</Attribute>'::bytea,'hex')::varchar||encode((chr(10)||'</Attributes>')::bytea,'hex')||split_part(data1,encode('</Attributes>','hex'),2);
	else
        	data1:=split_part(data1,encode('<Content>','hex'),1)||encode('<Attributes><Attribute Type="EMAIL">'::bytea,'hex')::varchar || encode(trim(correo1)::bytea,'hex')::varchar||encode('</Attribute>'::bytea,'hex')::varchar||encode((chr(10)||'<Attribute Type="URIBOTON">')::bytea,'hex')::varchar||encode(ver_dcto_attrib::bytea,'hex')::varchar||encode('</Attribute>'::bytea,'hex')::varchar||encode((chr(10)||'</Attributes><Content>')::bytea,'hex')||split_part(data1,encode('<Content>','hex'),2);
	end if;

	--Enviamos el mandato
	json4:='{}';
	json4:=put_json(json4,'uri',uri1);
	json4:=put_json(json4,'flag_data_xml','SI');
	json4:=put_json(json4,'INPUT_CUSTODIUM',data1);
	json4:=put_json(json4,'ADICIONAL',get_json('XML_FILTROS_ADICIONALES',json2));
	json4:=put_json(json4,'CICLO',get_json('MANDATO_CICLO',json2));
	json4:=put_json(json4,'RUT_RECEPTOR',get_json('RUT_RECEPTOR',json2));
	json4:=put_json(json4,'subject_hex',subject1);
	--json4:=put_json(json4,'from',get_json('MANDATO_MAIL_EMISOR',json2));
	--DAO 20190309 Para que puedan enviar con alias 
	json4:=put_json(json4,'from',replace(replace(get_json('MANDATO_MAIL_EMISOR',json2),'&lt'||chr(6),'<'),'&gt'||chr(6),'>'));
	json4:=put_json(json4,'to',correo1);
	json4:=put_json(json4,'tipo_envio','XSL');
	--Buscamos el xsl que le corresponde
	json4:=put_json(json4,'file_xsl','/opt/acepta/motor/xsl/'||get_json('DOMINIO',json2)||'/xsl/mail.xsl');
	--Si tiene XSL_MANDATO = SI sacamos el XSL completo desde la BD.
	if get_json('XSL_MANDATO',json2) ='SI' then
		json2:=logjson(json2,'Mandato con Casilla de la BD'); 
		json4:=put_json(json4,'flag_xls_bd','SI');
	end if;

	json_par1:=get_parametros_motor_json('{}','SERVIDOR_CORREO');
	--json4:=put_json(json4,'return_path','confirmacion_envio@custodium.com');
	--json4:=put_json(json4,'ip_envio','172.16.14.82');
	json4:=put_json(json4,'return_path',get_json('PARAMETRO_RUTA',json_par1));
	json4:=put_json(json4,'ip_envio',get_json('__IP_CONEXION_CLIENTE__',json_par1));
	comentario_traza1:='Recibe: '||correo1;
	

	--Verifico si se adjunta el PDF con clave
	aux1:=get_json('DTE_MANDATO_PDF_CLAVE',json2);
	if (strpos(aux1,'ALL')>0 or strpos(aux1,get_json('TIPO_DTE',json2))>0) then
		--Seteo adjuntar el PDF
		json2:=logjson(json2,'Adjunta pdf con Clave '||aux1);
		json4:=put_json(json4,'nombre_pdf',get_json('RUT_EMISOR',json2)||'_'||get_json('TIPO_DTE',json2)||'_'||get_json('FOLIO',json2));
		json4:=put_json(json4,'adjunta_pdf','SI');
		if (get_json('DTE_MANDATO_PDF_TIPO_CLAVE',json2)='RUT') then
			json4:=put_json(json4,'clave_pdf',get_json('RUT_RECEPTOR',json2));
			comentario_traza1:=comentario_traza1||' (Adjunta PDF con Clave)';
		else
			comentario_traza1:=comentario_traza1||' (Adjunta PDF)';
		end if;
	else
		--Verifico si viene mandando con pdf
		aux1:=get_json('DTE_MANDATO_PDF',json2);
		if (strpos(aux1,'ALL')>0 or strpos(aux1,get_json('TIPO_DTE',json2))>0) then
			--Seteo adjuntar el PDF
			json4:=put_json(json4,'nombre_pdf',get_json('RUT_EMISOR',json2)||'_'||get_json('TIPO_DTE',json2)||'_'||get_json('FOLIO',json2));
			json2:=logjson(json2,'Adjunta pdf  '||aux1);
			json4:=put_json(json4,'adjunta_pdf','SI');
			comentario_traza1:=comentario_traza1||' (Adjunta PDF)';
		else
			--Si no tiene activamos DTE_MANDATO, no enviamos nada
			aux1:=get_json('DTE_MANDATO',json2);
			--Si no tiene activado los mandatos, solo para los que no vienen por pantalla
			if (strpos(aux1,'ALL')=0 and strpos(aux1,get_json('TIPO_DTE',json2))=0 and get_json('__FLAG_PUB_10K__',json2)<>'SI') then
				--No se envia nada
				json2:=logjson(json2,'Cliente sin Mandato Activado en Maestro de Clientes');
				json2:=put_json(json2,'__EDTE_MANDATO_OK__','NO');
				--Si ya existe el envio de mandato
				if (get_json('__FLAG_REINTENTO_MANDATO__',json2)='SI') then
					--id1:=get_json('__ID_DTE__',json2);
					--Si viene de un reintento, aumento reintentos
					--json2:=logjson(json2,'Se borra mandato edte de la cola');
					--execute 'delete from '||get_json('__COLA_MOTOR__',json2)||' where id='||id1;
	      	    			json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
        	    			json2:=put_json(json2,'__SECUENCIAOK__','40');
					return json2;
				end if;
			       return json2;
			end if;
		end if;

		
	end if;
	--DAO 20181016 - Se agrega el poder adjuntar XML en un mandato
        aux1:=get_json('DTE_MANDATO_XML',json2);
        if (strpos(aux1,'ALL')>0 or strpos(aux1,get_json('TIPO_DTE',json2))>0) then
                json2:=logjson(json2,'Adjuntamos XML Mandato');
                --json4:=put_json(json4,'adjunta_xml','SI');
		--DAO la funcionalidad de reenvio envia solo DTE sin sobre //Requermiento BTG 20190312
                json4:=put_json(json4,'adjunta_xml','enviar_mail_con_xml');
                json4:=put_json(json4,'nombre_xml',get_json('RUT_EMISOR',json2)||'_'||get_json('TIPO_DTE',json2)||'_'||get_json('FOLIO',json2));
		comentario_traza1:=comentario_traza1||' (Adjunta XML)';
        end if;


	i:=contador_mail1+1;
	url_get1:=replace(split_part(uri1,'?k=',1)||'_m'||i::varchar||to_char(now(),'YYYYMMDDHH24MISSMS'),'v01','mandato');
	json2:=put_json(json2,'URL_GET_MANDATO',url_get1);

	if (get_json('__FLAG_REENVIO_MANDATO__',json2)='SI') then
		j3=put_json('{}','E',get_json('RUT_EMISOR',json2));
		j3=put_json(j3,'T',get_json('TIPO_DTE',json2));
		j3=put_json(j3,'F',get_json('FOLIO',json2));
		j3=put_json(j3,'FE',get_json('FECHA_EMISION',json2));
		j3=put_json(j3,'C','EMITIDOS');
		j3=put_json(j3,'U',uri1);
		j3=put_json(j3,'R',get_json('RUT_RECEPTOR',json2));
		j3=put_json(j3,'EO','XMS');
		j3=put_json(j3,'EN','XMF');
		json4:=put_json(json4,'msg_id','<JCP'||encripta_hash_evento_VDC2(j3::varchar)||'@motor2.acepta.com>');
	else
		j3=put_json('{}','E',get_json('RUT_EMISOR',json2));
		j3=put_json(j3,'T',get_json('TIPO_DTE',json2));
		j3=put_json(j3,'F',get_json('FOLIO',json2));
		j3=put_json(j3,'FE',get_json('FECHA_EMISION',json2));
		j3=put_json(j3,'C','EMITIDOS');
		j3=put_json(j3,'U',uri1);
		j3=put_json(j3,'R',get_json('RUT_RECEPTOR',json2));
		j3=put_json(j3,'EO','EMS');
		j3=put_json(j3,'EN','EMF');
		json4:=put_json(json4,'msg_id','<JCP'||encripta_hash_evento_VDC2(j3::varchar)||'@motor2.acepta.com>');
	end if;
		
	json4:=put_json(json4,'url_traza',get_json('__VALOR_PARAM__',json_par1));
	if (get_json('__FLAG_REENVIO_MANDATO__',json2)='SI') then
		evento1:='XMA';
	else
		evento1:='EMA';
	end if;
	json4:=put_json(json4,'evento_ema','<trace source="MANDATO2" version="1.1"><node name="'||evento1||'" stamp="'||to_char(now(),'YYYY-MM-DD')||'T'||to_char(now(),'HH24:MI:SS')||'" owner="' ||get_json('RUT_EMISOR_DV',json2)||'"><keys><key name="rutEmisor" value="'||get_json('RUT_EMISOR_DV',json2)||'"/><key name="tipoDTE" value="'||get_json('TIPO_DTE',json2)||'"/><key name="folio" value="'||get_json('FOLIO',json2)||'"/><key name="fchEmis" value="'||get_json('FECHA_EMISION',json2)||'"/></keys><attrs><attr key="code">'||get_json('TIPO_DTE',json2)||'</attr><attr key="url">'||uri1||'</attr><attr key="relatedUrl">'||url_get1||'</attr><attr key="orig">'||get_json('RUT_EMISOR_DV',json2)||'</attr><attr key="dest">'||get_json('RUT_RECEPTOR_DV',json2)||'</attr><attr key="tag">'||get_json('FOLIO',json2)||'</attr><attr key="data"></attr><attr key="comment">'||comentario_traza1||'</attr></attrs></node></trace>');
	--Solo en caso de que falle el envio de correo porque el mail es invalido
	json4:=put_json(json4,'evento_mail_fallido','<trace source="MANDATO2" version="1.1"><node name="MAIL_MANDATO_INVALIDO" stamp="'||to_char(now(),'YYYY-MM-DD')||'T'||to_char(now(),'HH24:MI:SS')||'" owner="' ||get_json('RUT_EMISOR_DV',json2)||'"><keys><key name="rutEmisor" value="'||get_json('RUT_EMISOR_DV',json2)||'"/><key name="tipoDTE" value="'||get_json('TIPO_DTE',json2)||'"/><key name="folio" value="'||get_json('FOLIO',json2)||'"/><key name="fchEmis" value="'||get_json('FECHA_EMISION',json2)||'"/></keys><attrs><attr key="code">'||get_json('TIPO_DTE',json2)||'</attr><attr key="url">'||uri1||'</attr><attr key="relatedUrl"></attr><attr key="orig">'||get_json('RUT_EMISOR_DV',json2)||'</attr><attr key="dest">'||get_json('RUT_RECEPTOR_DV',json2)||'</attr><attr key="tag">'||get_json('FOLIO',json2)||'</attr><attr key="data"></attr><attr key="comment">Correo Mandato Invalido '||correo1||'</attr></attrs></node></trace>');

	--Obtenemos el correlativo del ecm
	id_ecm1:=get_id_ecm()::varchar;
	json4:=put_json(json4,'ID_ECM',id_ecm1);

	--Creamos un evento para el ECM
	data_lma := encripta_hash_evento_VDC('uri='||uri1||'&owner='||get_json('RUT_EMISOR',json2)||'&rutEmisor='||get_json('RUT_EMISOR',json2)||'&tipoDTE='||get_json('TIPO_DTE',json2)||'&folio='||get_json('FOLIO',json2)||'&mail='||trim(correo1)||'&type=LMA'||'&rutRecep='||get_json('RUT_RECEPTOR',json2)||'&fchEmis='||get_json('FECHA_EMISION',json2)||'&relatedUrl=&comment=Mail Leído por '||trim(correo1)||'&url_redirect=https://traza.acepta.com/imgs/blank.png&id_ecm='||id_ecm1||'&');
	--Creamos el json para generar la URI del evento
	juri:='{}';
	juri:=put_json(juri,'id',id_ecm1::varchar||'_LMA');
	juri:=put_json(juri,'cliente','mandato');
	--Esta es la URI que se necesita hacer redirect	
	juri:=put_json(juri,'url','https://traza.acepta.com/imgs/blank.png');
	--Esta es la URL donde se posteara el evento
	juri:=put_json(juri,'url_get','http://servicios.acepta.com/traza?hash=');
	--Esta es la data
	juri:=put_json(juri,'data_get',data_lma);
	--Se indica al servicio que la data viene encriptada
	juri:=put_json(juri,'flag_data_encriptada','SI');
	juri:=sp_crea_url_short(juri);
	--Agrega el ID_S3 al json2
	json2:=put_json(json2,'ID_S3',concatena_id_s3(get_json('ID_S3',juri),get_json('ID_S3',json2)));
	json2:=put_json(json2,'ID_SHORT',concatena_id_s3(quote_literal(id_ecm1::varchar||'_LMA'),get_json('ID_SHORT',json2)));
	--FAY-DAO 2018-10-19 Obtengo el ID de la cola donde se grabo la url short, para que la siguiente se guarde en la misma tabla en caso de que falle el envio del correo, se pueda borrar de una sola tabla
	json2:=put_json(json2,'SEQ_COLA_S3',get_json('SEQ_COLA_S3',juri));
	json2 := logjson(json2,get_json('_LOG_',juri));
	--Se obtiene la URL donde se gatillara el evento
	uri_short1:=get_json('url_short',juri);
	json2:=logjson(json2,'URI_SHORT LMA='||uri_short1::varchar);
	if uri_short1='' then
		json2 := logjson(json2,'Falla generacion uri corta');
		json2 := put_json(json2,'__MENSAJE_10K__','Falla Envío.');
		json2 := put_json(json2,'__EDTE_MANDATO_OK__','NO');
		json2:=put_json(json2,'RESPUESTA','Status: 400 NK');
        	json2:=put_json(json2,'__SECUENCIAOK__','40');
		return json2;	
	else
		json4:=put_json(json4,'evento_lma',uri_short1);
	end if;
			
	--Generamos la data encriptada que se enviara cuando se ejecute el evento
	encr1:=encripta_hash_evento_VDC('uri='||uri1||'&owner='||get_json('RUT_EMISOR',json2)||'&rutEmisor='||get_json('RUT_EMISOR',json2)||'&tipoDTE='||get_json('TIPO_DTE',json2)||'&folio='||get_json('FOLIO',json2)||'&mail='||trim(correo1)||'&type=CVD'||'&rutRecep='||get_json('RUT_RECEPTOR',json2)||'&fchEmis='||get_json('FECHA_EMISION',json2)||'&relatedUrl=&comment=Usuario '||trim(correo1)||' visualiza Documento.&url_redirect='||uri1||'&id_ecm='||id_ecm1||'&');
	--Creamos el json para generar la URI del evento
	juri:='{}';
	juri:=put_json(juri,'id',id_ecm1::varchar||'_CVD');
	juri:=put_json(juri,'cliente','mandato');
	--Esta es la URI que se necesita hacer redirect	
	juri:=put_json(juri,'url',uri1);
	--Esta es la URL donde se posteara el evento
	juri:=put_json(juri,'url_get','http://servicios.acepta.com/traza?hash=');
	--Esta es la data
	juri:=put_json(juri,'data_get',encr1);
	--Se indica al servicio que la data viene encriptada
	juri:=put_json(juri,'flag_data_encriptada','SI');
	--Identificamos la cola donde queremos grabar el url_short (la misma que la anterior)
	juri:=put_json(juri,'SEQ_COLA_MOTOR_S3',get_json('SEQ_COLA_S3',json2));
	juri:=sp_crea_url_short(juri);
	--Limpiamos la secuencia
	json2:=put_json(json2,'SEQ_COLA_S3','');
	--Almacenamos donde se guardo en la cola s3
	json2:=put_json(json2,'COLA_S3',get_json('COLA_S3',juri));	

	--Agrega el ID_S3 al json2
	json2:=put_json(json2,'ID_S3',concatena_id_s3(get_json('ID_S3',juri),get_json('ID_S3',json2)));
	json2:=put_json(json2,'ID_SHORT',concatena_id_s3(quote_literal(id_ecm1::varchar||'_CVD'),get_json('ID_SHORT',json2)));

	--Se obtiene la URL donde se gatillara el evento
	uri_short1:=get_json('url_short',juri);
	json2 := logjson(json2,get_json('_LOG_',juri));
	
	--perform logfile('DAO_URI_SHORT1='||juri::varchar);
	json2:=logjson(json2,'URI_SHORT CVD='||uri_short1::varchar||' ID_S3='||get_json('ID_S3',json2));
	if uri_short1='' then
		json2 := logjson(json2,'Falla generacion uri corta');
		json2 := put_json(json2,'__MENSAJE_10K__','Falla Envío.');
		json2 := put_json(json2,'__EDTE_MANDATO_OK__','NO');
		json2:=put_json(json2,'RESPUESTA','Status: 400 NK');
		json2:=put_json(json2,'__SECUENCIAOK__','40');
		return json2;	
	else
		json4:=put_json(json4,'evento_ecm_link',uri_short1);
	end if;

	--Vamos a enviar el correo
	json2:=put_json(json2,'JSON4',json4::varchar);
	json2:=put_json(json2,'__SECUENCIAOK__','30');
	return json2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION envia_correo_12771(json) RETURNS json AS $$
DECLARE
        json1   alias for $1;
        json2   json;
        total1  integer;
        uri1    varchar;
        hash1   varchar;
        ver_dcto_attrib varchar;
        data1   varchar;
        largo1  integer;
        pos_inicial1    integer;
        pos_final1      integer;
        mail1   varchar;
        campo   record;
        json4   json;
        sts1    varchar;
        pos     integer;
        subject1        varchar;
        json_out1       json;
        comentario1     varchar;
        xml3    varchaR;
        cola1   varchar;
        nombre_tabla1   varchar;
        tx1     varchar;
        rut1    varchar;
        evento1 varchar;
        id1     varchar;
        aux1    varchar;
        comentario_traza1       varchar;
        json3   json;
        jsonsts1        json;
        i       integer;
        xml4    varchar;
        url_get1        varchar;
        data_lma        varchar;
        json_par1       json;
        flag_envio_mandato      boolean;
        razon_social_receptor1  varchar;
        rut_aux1        varchar;
        json_par2       json;
        j3      json;
        flag_cd         json;
        id_ecm1         varchar;
        uri_short1      varchar;
        juri    json;
        encr1   varchar;
        json_aux        json;
	correo1	varchar;
	contador_mail1	integer;
BEGIN
        json2:=json1;
        json2:=put_json(json2,'__SECUENCIAOK__','0');
		
	json4:=get_json('JSON4',json2)::json;
	uri1:=get_json('URI_IN',json2);
        contador_mail1:=get_json('CONTADOR_MANDATO',json2)::integer;
	i:=contador_mail1+1;
        correo1:=get_json_index(get_json('LISTA_MANDATO',json2)::json,contador_mail1);

        if (get_json('RUT_EMISOR',json2) = '99520000'  and  get_json('FLAG_ANULA',json2) = 'SI')  then  --pruebas copec anulacion guia
                --Se busca razon social del Receptor.
                json2:=logjson(json2,'RUt Receptor -->'||get_json('RUT_RECEPTOR',json2));
		rut_aux1:=get_json('RUT_RECEPTOR',json2);
                select nombre into razon_social_receptor1 from contribuyentes where rut_emisor = rut_aux1::integer;
                if not found then
                        razon_social_receptor1:='';
                end if;

                json2:=logjson(json2,'Razon social receptor-->'||razon_social_receptor1);
                json4:=put_json(json4,'RAZON_SOCIAL_RECEPTOR',razon_social_receptor1);

                json2:=logjson(json2,'SEND MAIL DE COPEC '||'SUBJECT-->'||get_json('MANDATO_SUBJECT',json2));
                json4:=put_json(json4,'tipo_envio','XSL');
                json4:=put_json(json4,'subject_hex',encode(get_json('MANDATO_SUBJECT',json2)::bytea,'hex'));
                json4:=put_json(json4,'file_xsl','/opt/acepta/motor/xsl/'||get_json('DOMINIO',json2)||'/xsl/mail_anulagde.xsl');
                json4:=put_json(json4,'INPUT_CUSTODIUM','');
--              json4:=put_json(json4,'INPUT','');
                jsonsts1:=send_mail_python3(json4::varchar);
        else
                --perform logfile('send_mail_python2('''||json4::varchar||''')');
		--raise notice 'send_mail_python2=%',json4;
		json4:=put_json(json4,'CATEGORIA','MANDATO');
		json4:=put_json(json4,'RUT_OWNER',get_json('RUT_EMISOR',json2));
		json4:=put_json(json4,'ip_envio','http://interno.acepta.com:8080/sendmail');
		if get_json('rutUsuario',json2)='17597643' then
			perform logfile('select send_mail_python2_colas('''||json4::varchar||''');');
		end if;
		jsonsts1:=send_mail_python2_colas(json4::varchar);
                --jsonsts1:=send_mail_python2(json4::varchar);
        end if;

	json2:=logjson(json2,'Respuesta jsonsts1='||get_json('status',jsonsts1)||' Mensaje='||get_json('mensaje',jsonsts1)||' retorno='||get_json('retorno_send_mail',jsonsts1)||' Confirmado='||get_json('confirmacion',jsonsts1)||' msg-id='||get_json('msg-id',jsonsts1));
--	jsonsts1:=send_mail_python2(json4::varchar);
	if (get_json('status',jsonsts1)='OK') then
		json2:=put_json(json2,'__EDTE_MANDATO_OK__','SI');
		json2:=logjson(json2,'Mandato Enviado OK');
		--Se limpian los ID para que no se borren en el proximo
		--json2:=logjson(json2,'ID_SHORT='||get_json('ID_SHORT',json2)||' ID_s3='||get_json('ID_S3',json2));
		json2:=put_json(json2,'ID_S3','');
		json2:=put_json(json2,'COLA_S3','');
		json2:=put_json(json2,'ID_SHORT','');
		
		--Voy a grabar al S3 el mandato
		aux1:=get_json('html',jsonsts1);
		if (length(aux1)>0) then
			xml4:='';
			--Para grabar en el S3 ponemos la misma URI con el MX al final
			xml4:=put_campo(xml4,'URI_IN',get_json('URL_GET_MANDATO',json2));
			xml4:=put_campo(xml4,'INPUT_CUSTODIUM',aux1);
			xml4:=put_campo(xml4,'RUT_EMISOR',get_json('RUT_EMISOR',json2));
			xml4:=graba_documento_s3(xml4);
			json2 := logjson(json2,get_campo('_LOG_',xml4));
			--json2:=logjson(json2,'Se graba html en s3 en '||get_campo('URI_IN',xml4));
		end if;

		--Voy por el siguiente
		json2:=put_json(json2,'CONTADOR_MANDATO',(contador_mail1+1)::varchar);
		--Volvemos a la secuencia de armado de correo
		json2:=put_json(json2,'__SECUENCIAOK__','20');
		return json2;
	else
		--Si falla en envio de mail ,se borra de las colas del S3 para no grabar basura
		if (get_json('ID_S3',json2)<>'') then
			json2:=logjson(json2,'BORRA ID_SHORT='||get_json('ID_SHORT',json2)||' ID_s3='||get_json('ID_S3',json2)||' Cola='||get_json('COLA_S3',json2));
			execute 'delete from '||get_json('COLA_S3',json2)||' where id in ('||get_json('ID_S3',json2)||')';
			execute 'delete from short_url where url_short in ('||get_json('ID_SHORT',json2)||')';
			--Se limpian los ID para que no se borren en el proximo
			json2:=put_json(json2,'ID_S3','');
			json2:=put_json(json2,'COLA_S3','');
			json2:=put_json(json2,'ID_SHORT','');
		end if;
		if (get_json('status',jsonsts1)='FALLA_MAIL_INVALIDO') then	
			json2:=logjson(json2,'Mail invalido');
			--Voy por el siguiente
			json2:=put_json(json2,'CONTADOR_MANDATO',(contador_mail1+1)::varchar);
			--Volvemos a la secuencia de armado de correo
			json2:=put_json(json2,'__SECUENCIAOK__','20');
			return json2;
		end if;
		json2:=logjson(json2,'Falla Mail');
		json2:=put_json(json2,'__EDTE_MANDATO_OK__','NO');
		if (get_json('__FLAG_REINTENTO_MANDATO__',json2)<>'SI') then
			xml3:='';
                        xml3:=put_campo(xml3,'TX','12771');
                        xml3:=put_campo(xml3,'URI_IN',uri1);
                        xml3:=put_campo(xml3,'MANDATO_EMAIL',correo1);
                        xml3:=put_campo(xml3,'INPUT_CUSTODIUM',get_json('INPUT_CUSTODIUM',json2));
                        xml3:=put_campo(xml3,'MANDATO_MAIL_EMISOR',get_json('MANDATO_MAIL_EMISOR',json2));
                        xml3:=put_campo(xml3,'DOMINIO',get_json('DOMINIO',json2));
			xml3:=put_campo(xml3,'FOLIO',get_json('FOLIO',json2));
			xml3:=put_campo(xml3,'RUT_EMISOR',get_json('RUT_EMISOR',json2));
			xml3:=put_campo(xml3,'RUT_EMISOR_DV',get_json('RUT_EMISOR_DV',json2));
			xml3:=put_campo(xml3,'TIPO_DTE',get_json('TIPO_DTE',json2));
			xml3:=put_campo(xml3,'FECHA_EMISION',get_json('FECHA_EMISION',json2));
			xml3:=put_campo(xml3,'RUT_RECEPTOR',get_json('RUT_RECEPTOR',json2));
			xml3:=put_campo(xml3,'RUT_RECEPTOR_DV',get_json('RUT_RECEPTOR_DV',json2));
                        xml3:=put_campo(xml3,'__FLAG_REINTENTO_MANDATO__','SI');
                        xml3:=put_campo(xml3,'__DTE_CON_MANDATO__','SI');
			xml3:=put_campo(xml3,'DTE_MANDATO_PDF_CLAVE',get_json('DTE_MANDATO_PDF_CLAVE',json2));
			xml3:=put_campo(xml3,'DTE_MANDATO_PDF',get_json('DTE_MANDATO_PDF',json2));
			xml3:=put_campo(xml3,'DTE_MANDATO_XML',get_json('DTE_MANDATO_XML',json2));
			xml3:=put_campo(xml3,'DTE_MANDATO',get_json('DTE_MANDATO',json2));
			xml3:=put_campo(xml3,'FECHA_INGRESO_COLA',now()::varchar);


                        cola1:=nextval('id_cola_procesamiento');
                        nombre_tabla1:='cola_motor_'||cola1::varchar;
                        rut1:=get_json('RUT_EMISOR',json2);
                        tx1:='20';
                        json2 := logjson(json2,'MANDATO: Graba uri '||uri1||' en cola '||nombre_tabla1);
                        execute 'insert into ' || nombre_tabla1 || ' (fecha,uri,reintentos,data,tx,rut_emisor,reproceso,categoria) values ( now(),'||quote_literal(uri1)||',0,'||quote_literal(xml3)||','||tx1||','||quote_literal(rut1)||',''NO'',''MANDATO'');';
                        --return json2;
			--Voy por el siguiente
			json2:=put_json(json2,'CONTADOR_MANDATO',(contador_mail1+1)::varchar);
			--Volvemos a la secuencia de armado de correo
			json2:=put_json(json2,'__SECUENCIAOK__','20');
                else
	      	    	json2:=put_json(json2,'RESPUESTA','Status: 400 NK');
			json2:=put_json(json2,'__SECUENCIAOK__','40');
                end if;
		return json2;
	end if;
END;
$$ LANGUAGE plpgsql;

