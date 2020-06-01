--Publica documento
delete from isys_querys_tx where llave='12770';

--Enviar mail
insert into isys_querys_tx values ('12770',20,19,1,'select envia_mandato_12770(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);
--insert into isys_querys_tx values ('12770',30,19,1,'select borra_mandato_12770(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,0,0);

CREATE or replace FUNCTION  envia_mandato_12770(json) RETURNS json AS $$
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
BEGIN
	json2:=json1;
        json2:=put_json(json2,'__SECUENCIAOK__','0');


/*
	if (get_json('__FLAG_REINTENTO_MANDATO__',json2)<>'SI') then
		select * into campo from mandato_test limit 1;
	        if found then
        	        json2:=put_json(json2,'__SECUENCIAOK__','40');
                	return json2;
	        end if;

        	insert into mandato_test (i) values (1);
	end if;
	*/


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
	json2:=sp_procesa_respuesta_cola_motor88_json(json2);
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
        json2:=put_json(json2,'MANDATO_MAIL_EMISOR','acepta@acepta.com');
    end if;


    json2:=logjson(json2,'Mandatos '||mail1||' TotalCasillas='||get_json('TOTAL_CASILLAS',json2)||' URI='||uri1);
    i:=0;
    --Para verificar que se envia algun mandato
    flag_envio_mandato:=false;
    for campo in select trim(mail) as mail from (select * from regexp_split_to_table(mail1,'[\,,\;]') mail) x where length(mail)>0 loop
        flag_envio_mandato:=true;
    	json2:=logjson(json2,'Mandato='||campo.mail);
	--Validamos el correo
	if (valida_email(campo.mail) is false) then
		json2:=logjson(json2,'Mail Invalido '||campo.mail);
		--Si viene desde las colas
    		if (get_json('__FLAG_REINTENTO_MANDATO__',json2)='SI') then
		    id1:=get_json('__ID_DTE__',json2);
        	    --Si viene de un reintento, aumento reintentos
	            --json2:=logjson(json2,'Se borra mandato edte de la cola');
        	    --execute 'delete from '||get_json('__COLA_MOTOR__',json2)||' where id='||id1;
	      	    json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
		    json2:=sp_procesa_respuesta_cola_motor88_json(json2);
		end if;
		continue;
	end if;

	if(get_json('__FLAG_PUB_10K__',json2)<>'SI' and get_json('__FLAG_REENVIO_MANDATO__',json2)<>'SI') then
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
	
    --for campo in select 'fernando.arancibia@acepta.com'::varchar as mail  loop
    json2:=logjson(json2,'Mandato Casilla --> '||campo.mail||' URI='||uri1);
	hash1 := encripta_hash_evento_VDC(split_part(uri1,'v01/',2)||'&v_origen='||get_json('RUT_EMISOR',json2)||'&v_tipo_dte='||get_json('TIPO_DTE',json2)||'&v_folio='||get_json('FOLIO',json2)||'&v_email='||trim(campo.mail)||'&v_evento=CVD'||'&v_receptor='||get_json('RUT_RECEPTOR',json2)||'&v_fecha_emi='||get_json('FECHA_EMISION',json2));
	ver_dcto_attrib:=split_part(uri1,'v01/',1)||'eventos/'||hash1;
        --se limpia el atributo EMAIL
        data1:=regexp_replace(data1,encode('<Attribute Type="EMAIL">'::bytea,'hex')::varchar||'.*'||encode('</Attribute>'::bytea,'hex')::varchar,'');
        --se agrega el mail que corresponde a esta "vuelta" y Atributo nuevo para boton "VER DOCUMENTO"
        data1:=split_part(data1,encode('</Attributes>','hex'),1)||encode('<Attribute Type="EMAIL">'::bytea,'hex')::varchar || encode(trim(campo.mail)::bytea,'hex')::varchar||encode('</Attribute>'::bytea,'hex')::varchar||encode((chr(10)||'<Attribute Type="URIBOTON">')::bytea,'hex')::varchar||encode(ver_dcto_attrib::bytea,'hex')::varchar||encode('</Attribute>'::bytea,'hex')::varchar||encode((chr(10)||'</Attributes>')::bytea,'hex')||split_part(data1,encode('</Attributes>','hex'),2);

	--Enviamos el mandato
	json4:='{}';
	json4:=put_json(json4,'uri',uri1);
	json4:=put_json(json4,'flag_data_xml','SI');
	json4:=put_json(json4,'INPUT_CUSTODIUM',data1);
	json4:=put_json(json4,'ADICIONAL',get_json('XML_FILTROS_ADICIONALES',json2));
	json4:=put_json(json4,'CICLO',get_json('MANDATO_CICLO',json2));
	json4:=put_json(json4,'RUT_RECEPTOR',get_json('RUT_RECEPTOR',json2));
	json4:=put_json(json4,'subject_hex',subject1);
	json4:=put_json(json4,'from',get_json('MANDATO_MAIL_EMISOR',json2));
	json4:=put_json(json4,'to',campo.mail);
	--json4:=put_json(json4,'to','fernando.arancibia@acepta.com');
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
	comentario_traza1:='Recibe: '||campo.mail;
	

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
					json2:=sp_procesa_respuesta_cola_motor88_json(json2);
				end if;
			       return json2;
			end if;
		end if;
	end if;
	--DAO 20181016 - Se agrega el poder adjuntar XML en un mandato
	aux1:=get_json('DTE_MANDATO_XML',json2);
        if (strpos(aux1,'ALL')>0 or strpos(aux1,get_json('TIPO_DTE',json2))>0) then
                json2:=logjson(json2,'Adjuntamos XML Mandato');
                json4:=put_json(json4,'adjunta_xml','SI');
                json4:=put_json(json4,'nombre_xml',get_json('RUT_EMISOR',json2)||'_'||get_json('TIPO_DTE',json2)||'_'||get_json('FOLIO',json2));
        end if;

	--Que envie el evento
	--if (campo.mail<>'fernando.arancibia@acepta.com') then
		--Definimos la URL de donde se almacena el html del mandato
		i:=i+1;
		url_get1:=replace(split_part(uri1,'?k=',1)||'_m'||i::varchar||to_char(now(),'YYYYMMDDHH24MISSMS'),'v01','mandato');
		--if (get_json('rutUsuario',json2)='7621836') then

		if (get_json('__FLAG_REENVIO_MANDATO__',json2)='SI') then
			--json4:=put_json(json4,'msg_id','<ACP'||encripta_hash_evento_VDC(get_json('RUT_EMISOR',json2)||'##'||get_json('TIPO_DTE',json2)||'##'||get_json('FOLIO',json2)||'##'||get_json('FECHA_EMISION',json2)||'##'||uri1||'####EMITIDOS##'||get_json('RUT_RECEPTOR',json2)||'##XMS##XMF##')||'@motor2.acepta.com>');
			/*
			json3:=put_json(json3,'RUT_EMISOR',get_json('E',id1::json));
                        json3:=put_json(json3,'TIPO_DTE',get_json('T',id1::json));
                        json3:=put_json(json3,'FOLIO',get_json('F',id1::json));
                        json3:=put_json(json3,'FECHA_EMISION',get_json('FE',id1::json));
                        json3:=put_json(json3,'uri_dte',get_json('U',id1::json));
                        json3:=put_json(json3,'CANAL',get_json('C',id1::json));
                        json3:=put_json(json3,'RUT_RECEPTOR',get_json('R',id1::json));
                        json3:=put_json(json3,'eok',get_json('EO',id1::json));
                        json3:=put_json(json3,'enk',get_json('EN',id1::json));
                        json3:=put_json(json3,'edelay',get_json('EN',id1::json));
                        json3:=put_json(json3,'comentario',get_json('CO',id1::json));
                        json3:=put_json(json3,'id_send_mail',get_json('ID',id1::json));
	*/

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
			--json4:=put_json(json4,'msg_id','<ACP'||encripta_hash_evento_VDC(get_json('RUT_EMISOR',json2)||'##'||get_json('TIPO_DTE',json2)||'##'||get_json('FOLIO',json2)||'##'||get_json('FECHA_EMISION',json2)||'##'||uri1||'####EMITIDOS##'||get_json('RUT_RECEPTOR',json2)||'##EMS##EMF##')||'@motor2.acepta.com>');
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
		--else
			
		end if;
		
		--perform logfile('msg_id='||get_json('msg_id',json4));
		--else
		--	json4:=put_json(json4,'msg_id','<MND-DTE-'||get_json('RUT_EMISOR_DV',json2)||'-'||get_json('RUT_RECEPTOR_DV',json2)||'-'||get_json('TIPO_DTE',json2)||'-'||get_json('FOLIO',json2)||'-'||to_char(now(),'YYYYMMDD')||'@motor2.acepta.com>');
		--end if;
		--json4:=put_json(json4,'url_traza','http://motor-prod.acepta.com:8082/motor/traza.fcgi');
		json4:=put_json(json4,'url_traza',get_json('__VALOR_PARAM__',json_par1));
		if (get_json('__FLAG_REENVIO_MANDATO__',json2)='SI') then
			evento1:='XMA';
		else
			evento1:='EMA';
		end if;
		json4:=put_json(json4,'evento_ema','<trace source="MANDATO2" version="1.1"><node name="'||evento1||'" stamp="'||to_char(now(),'YYYY-MM-DD')||'T'||to_char(now(),'HH24:MI:SS')||'" owner="' ||get_json('RUT_EMISOR_DV',json2)||'"><keys><key name="rutEmisor" value="'||get_json('RUT_EMISOR_DV',json2)||'"/><key name="tipoDTE" value="'||get_json('TIPO_DTE',json2)||'"/><key name="folio" value="'||get_json('FOLIO',json2)||'"/><key name="fchEmis" value="'||get_json('FECHA_EMISION',json2)||'"/></keys><attrs><attr key="code">'||get_json('TIPO_DTE',json2)||'</attr><attr key="url">'||uri1||'</attr><attr key="relatedUrl">'||url_get1||'</attr><attr key="orig">'||get_json('RUT_EMISOR_DV',json2)||'</attr><attr key="dest">'||get_json('RUT_RECEPTOR_DV',json2)||'</attr><attr key="tag">'||get_json('FOLIO',json2)||'</attr><attr key="data"></attr><attr key="comment">'||comentario_traza1||'</attr></attrs></node></trace>');
		--Solo en caso de que falle el envio de correo porque el mail es invalido
		json4:=put_json(json4,'evento_mail_fallido','<trace source="MANDATO2" version="1.1"><node name="MAIL_MANDATO_INVALIDO" stamp="'||to_char(now(),'YYYY-MM-DD')||'T'||to_char(now(),'HH24:MI:SS')||'" owner="' ||get_json('RUT_EMISOR_DV',json2)||'"><keys><key name="rutEmisor" value="'||get_json('RUT_EMISOR_DV',json2)||'"/><key name="tipoDTE" value="'||get_json('TIPO_DTE',json2)||'"/><key name="folio" value="'||get_json('FOLIO',json2)||'"/><key name="fchEmis" value="'||get_json('FECHA_EMISION',json2)||'"/></keys><attrs><attr key="code">'||get_json('TIPO_DTE',json2)||'</attr><attr key="url">'||uri1||'</attr><attr key="relatedUrl"></attr><attr key="orig">'||get_json('RUT_EMISOR_DV',json2)||'</attr><attr key="dest">'||get_json('RUT_RECEPTOR_DV',json2)||'</attr><attr key="tag">'||get_json('FOLIO',json2)||'</attr><attr key="data"></attr><attr key="comment">Correo Mandato Invalido '||campo.mail||'</attr></attrs></node></trace>');

		--Obtenemos el correlativo del ecm
		id_ecm1:=get_id_ecm()::varchar;
		json4:=put_json(json4,'ID_ECM',id_ecm1);
		--Se le agrega el evento de lectura al html resultante
		--data_lma := encripta_hash_evento_VDC('uri='||uri1||'&owner='||get_json('RUT_EMISOR',json2)||'&rutEmisor='||get_json('RUT_EMISOR',json2)||'&tipoDTE='||get_json('TIPO_DTE',json2)||'&folio='||get_json('FOLIO',json2)||'&mail='||trim(campo.mail)||'&type=LMA'||'&rutRecep='||get_json('RUT_RECEPTOR',json2)||'&fchEmis='||get_json('FECHA_EMISION',json2)||'&relatedUrl=&comment=Mail Leído por '||trim(campo.mail)||'&url_redirect=https://traza.acepta.com/imgs/blank.png&id_ecm='||id_ecm1||'&');
		--json4:=put_json(json4,'evento_lma','https://tracker.acepta.com/traza/'||data_lma);

		--Creamos un evento para el ECM
		--if (campo.mail='daniela.ahumada@acepta.com') then
			data_lma := encripta_hash_evento_VDC('uri='||uri1||'&owner='||get_json('RUT_EMISOR',json2)||'&rutEmisor='||get_json('RUT_EMISOR',json2)||'&tipoDTE='||get_json('TIPO_DTE',json2)||'&folio='||get_json('FOLIO',json2)||'&mail='||trim(campo.mail)||'&type=LMA'||'&rutRecep='||get_json('RUT_RECEPTOR',json2)||'&fchEmis='||get_json('FECHA_EMISION',json2)||'&relatedUrl=&comment=Mail Leído por '||trim(campo.mail)||'&url_redirect=https://traza.acepta.com/imgs/blank.png&id_ecm='||id_ecm1||'&');
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
			json2 := logjson(json2,get_json('_LOG_',juri));
			--Se obtiene la URL donde se gatillara el evento
			uri_short1:=get_json('url_short',juri);
			json2:=logjson(json2,'URI_SHORT LMA='||uri_short1::varchar);
			if uri_short1='' then
				json2 := logjson(json2,'Falla generacion uri corta');
				json2 := put_json(json2,'__MENSAJE_10K__','Falla Envío.');
				json2 := put_json(json2,'__EDTE_MANDATO_OK__','NO');
				json2:=put_json(json2,'RESPUESTA','Status: 400 NK');
				json2:=sp_procesa_respuesta_cola_motor88_json(json2);
				return json2;	
			else
				json4:=put_json(json4,'evento_lma',uri_short1);
			end if;
			
			--Generamos la data encriptada que se enviara cuando se ejecute el evento
			encr1:=encripta_hash_evento_VDC('uri='||uri1||'&owner='||get_json('RUT_EMISOR',json2)||'&rutEmisor='||get_json('RUT_EMISOR',json2)||'&tipoDTE='||get_json('TIPO_DTE',json2)||'&folio='||get_json('FOLIO',json2)||'&mail='||trim(campo.mail)||'&type=CVD'||'&rutRecep='||get_json('RUT_RECEPTOR',json2)||'&fchEmis='||get_json('FECHA_EMISION',json2)||'&relatedUrl=&comment=Usuario '||trim(campo.mail)||' visualiza Documento.&url_redirect='||uri1||'&id_ecm='||id_ecm1||'&');
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
			juri:=sp_crea_url_short(juri);
			--Se obtiene la URL donde se gatillara el evento
			uri_short1:=get_json('url_short',juri);
			
			--perform logfile('DAO_URI_SHORT1='||juri::varchar);
			json2:=logjson(json2,'URI_SHORT CVD='||uri_short1::varchar);
			if uri_short1='' then
				json2 := logjson(json2,'Falla generacion uri corta');
				json2 := put_json(json2,'__MENSAJE_10K__','Falla Envío.');
				json2 := put_json(json2,'__EDTE_MANDATO_OK__','NO');
				json2:=put_json(json2,'RESPUESTA','Status: 400 NK');
				json2:=sp_procesa_respuesta_cola_motor88_json(json2);
				return json2;	
			else
				json4:=put_json(json4,'evento_ecm_link',uri_short1);
			end if;
		--end if;
		

	--else
	--end if;
	--perform logfile('send_mail_python1='||json4::varchar);
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
		jsonsts1:=send_mail_python2_colas(json4::varchar);
                --jsonsts1:=send_mail_python2(json4::varchar);
        end if;

	json2:=logjson(json2,'Respuesta jsonsts1='||get_json('status',jsonsts1)||' Mensaje='||get_json('mensaje',jsonsts1)||' retorno='||get_json('retorno_send_mail',jsonsts1)||' Confirmado='||get_json('confirmacion',jsonsts1)||' msg-id='||get_json('msg-id',jsonsts1));
--	jsonsts1:=send_mail_python2(json4::varchar);
	if (get_json('status',jsonsts1)='OK') then
		json2:=put_json(json2,'__EDTE_MANDATO_OK__','SI');
		json2:=logjson(json2,'Mandato Enviado OK');
		
		--perform logfile('send_mail_python1 html2='||get_json('html2',jsonsts1));
		--perform logfile('send_mail_python1 html='||get_json('html',jsonsts1));
		
		--Voy a grabar al S3 el mandato
		aux1:=get_json('html',jsonsts1);
		if (length(aux1)>0) then
			xml4:='';
			--Para grabar en el S3 ponemos la misma URI con el MX al final
			xml4:=put_campo(xml4,'URI_IN',split_part(uri1,'?k=',1)||'_m'||i::varchar||to_char(now(),'YYYYMMDDHH24MISSMS'));
			xml4:=put_campo(xml4,'INPUT_CUSTODIUM',aux1);
			xml4:=put_campo(xml4,'RUT_EMISOR',get_json('RUT_EMISOR',json2));
			xml4:=graba_documento_s3(xml4);
			json2:=logjson(json2,'Se graba html en s3 en '||get_campo('URI_IN',xml4));
		end if;
		/*
		BEGIN
			json4:=put_json(json4,'CATEGORIA','MANDATO');
			json4:=put_json(json4,'RUT_OWNER',get_json('RUT_EMISOR',json2));
			json4:=put_json(json4,'ip_envio','http://interno.acepta.com:8080/sendmail');
			json4:=put_json(json4,'to','fernando.arancibia@acepta.com');
			jsonsts1:=send_mail_python2_colas(json4::varchar);
			if (get_json('status',jsonsts1)='OK') then
				json2:=logjson(json2,'OK send_mail_python2_colas');
			else
				json2:=logjson(json2,'Falla send_mail_python2_colas 1 '||jsonsts1::varchar);
			end if;
		EXCEPTION WHEN OTHERS THEN
			json2:=logjson(json2,'Falla send_mail_python2_colas');
		END;
		*/
	else
		if (get_json('status',jsonsts1)='FALLA_MAIL_INVALIDO') then	
			json2:=logjson(json2,'Mail invalido, se borra');
            		json2:=logjson(json2,'Se borra mandato edte de la cola');
		    	json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
			json2:=sp_procesa_respuesta_cola_motor88_json(json2);
			return json2;
		end if;
		json2:=logjson(json2,'Falla Mail');
		perform logfile('select send_mail_python2_colas('''||json4::varchar||''')');
		json2:=put_json(json2,'__EDTE_MANDATO_OK__','NO');
		if (get_json('__FLAG_REINTENTO_MANDATO__',json2)<>'SI') then
			xml3:='';
                        xml3:=put_campo(xml3,'TX','12770');
                        xml3:=put_campo(xml3,'URI_IN',uri1);
                        xml3:=put_campo(xml3,'MANDATO_EMAIL',campo.mail);
                        xml3:=put_campo(xml3,'INPUT_CUSTODIUM',data1);
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
			xml3:=put_campo(xml3,'DTE_MANDATO',get_json('DTE_MANDATO',json2));
			xml3:=put_campo(xml3,'FECHA_INGRESO_COLA',now()::varchar);


                        cola1:=nextval('id_cola_procesamiento');
                        nombre_tabla1:='cola_motor_'||cola1::varchar;
                        rut1:=get_json('RUT_EMISOR',json2);
                        tx1:='20';
                        json2 := logjson(json2,'MANDATO: Graba uri '||uri1||' en cola '||nombre_tabla1);
                        execute 'insert into ' || nombre_tabla1 || ' (fecha,uri,reintentos,data,tx,rut_emisor,reproceso,categoria) values ( now(),'||quote_literal(uri1)||',0,'||quote_literal(xml3)||','||tx1||','||quote_literal(rut1)||',''NO'',''MANDATO'');';
                        --return json2;
                else
                        --id1:=get_json('__ID_DTE__',json2);
                        --Si viene de un reintento, aumento reintentos
                        --json2:=logjson(json2,'Aumenta Reintentos Envio de Mandato Edte');
                        --execute 'update '||get_json('__COLA_MOTOR__',json2)||' set reintentos=reintentos+1 where id='||id1;
	      	    	json2:=put_json(json2,'RESPUESTA','Status: 400 NK');
			json2:=sp_procesa_respuesta_cola_motor88_json(json2);
                        --return json2;
                end if;
	end if;
    end loop;
    --json2:=put_json(json2,'__SECUENCIAOK__','0');
    --Si venia de la cola y envie bien, se borra
    if (get_json('__FLAG_REINTENTO_MANDATO__',json2)='SI' and (get_json('__EDTE_MANDATO_OK__',json2)='SI' or flag_envio_mandato is false)) then
	    --id1:=get_json('__ID_DTE__',json2);
            --Si viene de un reintento, aumento reintentos
            json2:=logjson(json2,'Se borra mandato edte de la cola');
      	    json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
	    json2:=sp_procesa_respuesta_cola_motor88_json(json2);
            --execute 'delete from '||get_json('__COLA_MOTOR__',json2)||' where id='||id1;
	    --if (get_json('BD_ORIGEN',json2)='172.16.14.88') then
    end if;
    return json2;	
END;
$$ LANGUAGE plpgsql;


	
	


