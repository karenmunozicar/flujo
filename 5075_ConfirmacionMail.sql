delete from isys_querys_tx where llave='5075';

--Sacamos el LOG
--insert into isys_querys_tx values ('5075',1,9,16,'LOG_JSON',0,0,0,1,1,5,5);

--Vamos al base de send_mail
insert into isys_querys_tx values ('5075',5,30,1,'select graba_confirmacion_send_mail_5075(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

--Vamos a las colas a desencriptar xq el openssl falla a veces(siempre)
insert into isys_querys_tx values ('5075',10,19,1,'select desencripta_id_5075(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,15);
-- Prepara llamada al AML
insert into isys_querys_tx values ('5075',15,45,1,'select confirmacion_mail_5075(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('5075',20,10,1,'$$QUERY_DATA$$',0,0,0,9,1,10,10);
insert into isys_querys_tx values ('5075',40,10,1,'$$QUERY_DATA$$',0,0,0,9,1,0,0);

CREATE or replace FUNCTION desencripta_id_5075(json) RETURNS json AS $$
DECLARE
    json1       alias for $1;
    json2   json;
        id1     varchar;
BEGIN
	json2:=json1;
        --Remplazamos los ; 
        json2:=replace(json2::varchar,'\u0006',';')::json;
        json2:=put_json(json2,'__SECUENCIAOK__','15');

	if(get_json('categoria',json2)='VERIFICA_DTE_MOTOR') then
		return json2;
	end if;
	--Solo procesamos las confirmacion de lectura
        if (get_json('delivery-status',json2)='') then
        	json2:=put_json(json2,'__SECUENCIAOK__','0');
                json2:=logjson(json2,'Confirmacion sin delivery-status, no se procesa');
                json2:=response_requests_6000('1','Confirmacion sin delivery-status','',json2);
                return json2;
        end if;
        if (get_json('rfc822-headers',json2)='') then
        	json2:=put_json(json2,'__SECUENCIAOK__','0');
                json2:=logjson(json2,'Confirmacion sin rfc822-headers, no se procesa');
                json2:=response_requests_6000('1','Confirmacion sin rfc822-headers','',json2);
                return json2;
        end if;
	if(get_json('categoria',json2)='CONFIRMACION_LECTURA_EDTE') then
		return json2;
	end if;

	id1:=get_json('Message-ID',get_json('rfc822-headers',json2)::json);
        id1:=split_part(split_part(id1,'<',2),'@',1);
	--json2:=logjson(json2,'id1='||id1||' '||get_json('rfc822-headers',json2));
	if (substring(id1,1,3)='ACP') then
                id1:=substring(id1,4);
		BEGIN
                	id1:=desencripta_hash_evento_VDC(id1::varchar);
			if id1='' then
				id1:=desencripta_hash_evento_vdc_bash(id1::text);
			end if;
		EXCEPTION WHEN OTHERS THEN
			id1:=desencripta_hash_evento_vdc_bash(id1::text);
		END;
		json2:=put_json(json2,'DATA_DECRYPT',encode_hex(id1::varchar));
		json2:=logjson(json2,'ACP DATA_DECRYPT desencripta_hash_evento_VDC colas');
	elsif (substring(id1,1,3)='JCP') then
                json2:=logjson(json2,'Formato ID JSON');
                id1:=substring(id1,4);
		BEGIN
                	id1:=desencripta_hash_evento_VDC(id1::varchar);
			if id1='' then
				id1:=desencripta_hash_evento_vdc_bash(id1::text);
			end if;
		EXCEPTION WHEN OTHERS THEN
			id1:=desencripta_hash_evento_vdc_bash(id1::text);
		END;
		json2:=put_json(json2,'DATA_DECRYPT',encode_hex(id1::varchar));
		json2:=logjson(json2,'JCP DATA_DECRYPT desencripta_hash_evento_VDC colas');
	end if;
	return json2;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION confirmacion_mail_5075(json) RETURNS json AS $$
DECLARE
    json1       alias for $1;
        json2   json;
        json3   json;
        json4   json;
        json5   json;
        campo   record;
        xml4    varchar;
        id1     varchar;
        accion1 varchar;
        action1 varchar;
        status1 varchar;

	--EDTE
	json_par1	json;
	json_resp1	json;
	json_edte3	json;
	server_remoto1	varchar;
	aux1	varchar;
	json_aux1	json;
	lista1	json;
	folio1	varchar;
	tipo_dte1	varchar;
	rut1	varchar;	
	rut_rec1	varchar;
	i	integer;
	j	integer;
	s1	varchar;
	rut_edte1	varchar;
	uri1		varchar;
	fecha1		varchar;
	tabla_traza1	varchar;
	total1	integer;
	monto1	varchar;
BEGIN
        json2:=json1;
	--Remplazamos los ; 
	--perform logfile('confirmacion_mail_5075 ');
	json2:=replace(json2::varchar,'\u0006',';')::json;
        json2:=put_json(json2,'__SECUENCIAOK__','0');
        --json2:=logjson(json2,'confirmacion_mail_5075 INPUT='||json2::varchar);
	--json2:=logjson(json2,'id1=VACIO '||get_json('rfc822-headers',json2));

	--Esta transaccion verifica que si la URL ya esta en la traza terminada, se ignora este correo
	if(get_json('categoria',json2)='VERIFICA_DTE_MOTOR') then
		--perform logfile('confirmacion_mail_5075 VERIFICA_DTE_MOTOR');
		rut1:=split_part(get_json('RUT_EMISOR',json2),'-',1);
		fecha1:=get_json('FECHA_EMISION',json2);
		tipo_dte1:=get_json('TIPO_DTE',json2);
		monto1:=get_json('MONTO_TOTAL',json2);
		folio1:=get_json('FOLIO',json2);
		uri1:=get_json('URIP',json2);	

		--Si existe en dte_recibidos, no recibo el DTE
		select * into campo from dte_recibidos where rut_emisor=rut1::integer and tipo_dte=tipo_dte1::integer and folio=folio1::bigint;
		if found then
			if campo.fecha_emision<>fecha1 or campo.monto_total::varchar<>monto1 then
				json2:=logjson(json2,'Monto o Fecha Emision Distinta '||uri1);
				json2:=response_requests_6000('2','Dte Monto o Fecha Distinta '||uri1,'',json2);	
			else
				json2:=response_requests_6000('1','Dte ya recibido '||uri1,'',json2);
			end if;
		else
			json2:=logjson(json2,'Dte no recibido');
			json2:=response_requests_6000('2','Dte no procesado '||uri1,'',json2);
		end if;

		--perform logfile('confirmacion_mail_5075 VERIFICA_DTE_MOTOR Fin');
		return json2;

	end if;


        --Solo procesamos las confirmacion de lectura
        if (get_json('delivery-status',json2)='') then
                json2:=logjson(json2,'Confirmacion sin delivery-status, no se procesa');
                json2:=response_requests_6000('1','Confirmacion sin delivery-status','',json2);
                return json2;
        end if;
        if (get_json('rfc822-headers',json2)='') then
                json2:=logjson(json2,'Confirmacion sin rfc822-headers, no se procesa');
                json2:=response_requests_6000('1','Confirmacion sin rfc822-headers','',json2);
                return json2;
        end if;
	
	id1:=get_json('Message-ID',get_json('rfc822-headers',json2)::json);
        id1:=split_part(split_part(id1,'<',2),'@',1);
	json2:=logjson(json2,'id1='||id1);

	if(get_json('categoria',json2)='CONFIRMACION_LECTURA_EDTE') then
		--perform logfile('confirmacion_mail_5075 CONFIRMACION_LECTURA_EDTE');
		s1:=get_json('Subject',get_json('rfc822-headers',json2)::json);
		rut1:=split_part(id1,'-',2)||'-'||split_part(id1,'-',3);
		rut_edte1:=rut1;
		json2:=logjson(json2,'CONFIRMACION_LECTURA_EDTE '||id1||' rut='||rut1);
		
		BEGIN
			json_resp1:=get_json('RES_JSON_1',json2)::json;
		EXCEPTION WHEN OTHERS THEN
			json2:=response_requests_6000('2','Falla Conexion EDTE3','',json2);
			return json2;
		END;
		json2:=logjson(json2,'CONFIRMACION_LECTURA_EDTE RESP'||json_resp1::varchar);
		if get_json('data',json_resp1)='' then
                	json2:=response_requests_6000('1','No se encuentra msgid, se elimina','',json2);
			return json2;
		end if;		

		--json2:=response_requests_6000('2','Parseo de ID EDTE '||get_json('data',json_resp1)||' '||id1,'',json2);
		--perform logfile('confirmacion_mail_5075 parser_edte_msgid');
		json_edte3:=parser_edte_msgid(get_json('data',json_resp1),id1);	
		if(json_edte3::varchar='{}' or get_json('LISTA',json_edte3)='') then
			--Dejar en 1
			json2:=logjson(json2,'CONFIRMACION_LECTURA_EDTE Data parseada '||get_json('data',json_resp1)||' '||id1);
                	json2:=response_requests_6000('2','Falla parseo de ID EDTE','',json2);
			return json2;
		end if;
		i:=0;
		j:=0;
		rut1:=get_json('RUT_EMISOR',json_edte3);
		rut_rec1:=get_json('RUT_RECEPTOR',json_edte3);
		lista1:=get_json('LISTA',json_edte3);
		aux1:=get_json_index(lista1,i);
		while(aux1<>'') loop
			json3:='{}';
			json3:=put_json(json3,'CANAL','EMITIDOS');
			tipo_dte1:=get_json('TIPO_DTE',aux1::json);
			folio1:=get_json('FOLIO',aux1::json);
			if(strpos(id1,'EnvioDTE')>0) then
				json3:=put_json(json3,'RUT_EMISOR',rut1);
				json3:=put_json(json3,'RUT_RECEPTOR',rut_rec1);
				--perform logfile('confirmacion_mail_5075 dte_emitidos '||rut1::varchar||' '||folio1::varchar);
				select * into campo from dte_emitidos where rut_emisor=rut1::bigint and tipo_dte=tipo_dte1::integer and folio=folio1::bigint;
				--perform logfile('confirmacion_mail_5075 dte_emitidos '||rut1::varchar||' '||folio1::varchar||' Fin');
				if not found then
					json2:=logjson(json2,'CONFIRMACION_LECTURA_EDTE rut_emisor='||rut1||' tipo_dte='||tipo_dte1||' folio='||folio1||' No encontrado en dte_emitidos');
					i:=i+1;
					aux1:=get_json_index(lista1,i);
					continue;
				end if;
				--Entrega exitosa al receptor
				json3:=put_json(json3,'eok','ERS');
				--Entrega fallida al receptor
				json3:=put_json(json3,'enk','ERF');
				--Entrega retrasada al receptor
				json3:=put_json(json3,'edelay','ERY');
				json2:=logjson(json2,'CONFIRMACION_LECTURA_EDTE rut_emisor='||rut1||' tipo_dte='||tipo_dte1||' folio='||folio1||' encontrado OK uri='||campo.uri);
			elsif(strpos(id1,'EnvioRecibos')>0) then  
				json3:=put_json(json3,'RUT_EMISOR',rut_rec1);
				json3:=put_json(json3,'RUT_RECEPTOR',rut1);
				--perform logfile('confirmacion_mail_5075 dte_recibidos '||rut_rec1::varchar||' '||folio1::varchar);
				select * into campo from dte_recibidos where rut_emisor=rut_rec1::bigint and tipo_dte=tipo_dte1::integer and folio=folio1::bigint;
				--perform logfile('confirmacion_mail_5075 dte_recibidos '||rut_rec1::varchar||' '||folio1::varchar||' Fin');
				if not found then
					json2:=logjson(json2,'CONFIRMACION_LECTURA_EDTE rut_emisor='||rut1||' tipo_dte='||tipo_dte1||' folio='||folio1||' No encontrado en dte_recibidos');
					i:=i+1;
					aux1:=get_json_index(lista1,i);
					continue;
				end if;
				--Entrega exitosa al receptor
				json3:=put_json(json3,'eok','ARMS');
				--Entrega fallida al receptor
				json3:=put_json(json3,'enk','ARMF');
				--Entrega retrasada al receptor
				json3:=put_json(json3,'edelay','ARMF');
				json2:=logjson(json2,'CONFIRMACION_LECTURA_EDTE rut_emisor='||rut_rec1||' tipo_dte='||tipo_dte1||' folio='||folio1||' encontrado OK uri='||campo.uri);
			else
				i:=i+1;
				aux1:=get_json_index(lista1,i);
				continue;
			end if;
			json3:=put_json(json3,'TIPO_DTE',tipo_dte1);
			json3:=put_json(json3,'FOLIO',folio1);
			json3:=put_json(json3,'FECHA_EMISION',campo.fecha_emision::varchar);
			json3:=put_json(json3,'uri_dte',campo.uri);
			
			--perform logfile('confirmacion_mail_5075 evento_confirmacion_mail_5075');
			json_aux1:=evento_confirmacion_mail_5075(json2,json3);	
			--perform logfile('confirmacion_mail_5075 evento_confirmacion_mail_5075 Fin');
			json2:=logjson(json2,'CONFIRMACION_LECTURA_EDTE '||get_json('_LOG_',json_aux1));

			i:=i+1;
			aux1:=get_json_index(lista1,i);
		end loop;

		--Borramos el ID de la tabla del EDTE
		if(strpos(id1,'EnvioDTE')>0) then
			json2:=logjson(json2,'Borro ID del EDTE='||id1||' rut='||rut_edte1);
        		json2:=put_json(json2,'__SECUENCIAOK__','40');
			json2:=put_json(json2,'QUERY_DATA','delete from enviodtes where id='''||id1||''' and rutemisor='''||rut_edte1||'''');
		elsif(strpos(id1,'EnvioRecibos')>0 and strpos(s1,'Acuse de Recibo de Mercaderias')>0) then  
			json2:=logjson(json2,'Borro ID del EDTE='||id1||' rut='||rut_edte1);
        		json2:=put_json(json2,'__SECUENCIAOK__','40');
			json2:=put_json(json2,'QUERY_DATA','delete from enviorecibos where id='''||id1||''' and rutresponde='''||rut_edte1||'''');
		end if;	

                json2:=response_requests_6000('1','Evento OK','',json2);
                json2:=logjson(json2,'Evento OK');
		return json2;
	end if;

	if (substring(id1,1,3)='ACP') then
		id1:=substring(id1,4);
		if get_json('DATA_DECRYPT',json2)<>'' then
			json2:=logjson(json2,'ACP Ocupo DATA_DECRYPT');
			id1:=decode_hex(get_json('DATA_DECRYPT',json2));
		else
			json2:=logjson(json2,'ACP SIN DATA_DECRYPT');
			raise notice 'desencripta_hash_evento_VDC ACP';
			id1:=desencripta_hash_evento_VDC(id1);
		end if;
		json2:=logjson(json2,'DATA_DECRYPT ID1='||id1);
		if (strpos(id1,'##')>0) then
			json3:='{}';
			json3:=put_json(json3,'RUT_EMISOR',split_part(id1,'##',1));
			json3:=put_json(json3,'TIPO_DTE',split_part(id1,'##',2));
			json3:=put_json(json3,'FOLIO',split_part(id1,'##',3));
			json3:=put_json(json3,'FECHA_EMISION',split_part(id1,'##',4));
			json3:=put_json(json3,'uri_dte',split_part(id1,'##',5));
			json3:=put_json(json3,'evento_confirmacion',split_part(id1,'##',6));
			json3:=put_json(json3,'CANAL',split_part(id1,'##',7));
			json3:=put_json(json3,'RUT_RECEPTOR',split_part(id1,'##',8));
			if (get_json('evento_confirmacion',json3)='') then
				json3:=put_json(json3,'eok',split_part(id1,'##',9));
				json3:=put_json(json3,'enk',split_part(id1,'##',10));
				json3:=put_json(json3,'edelay',split_part(id1,'##',10));
				json3:=put_json(json3,'comentario',split_part(id1,'##',11));
				json3:=put_json(json3,'id_send_mail',split_part(id1,'##',12));
        			json2:=logjson(json2,'Eventos Nuevos');
			else
				json3:=put_json(json3,'eok',get_json('evento_confirmacion',json3));
				json3:=put_json(json3,'enk',get_json('evento_confirmacion',json3));
				json3:=put_json(json3,'edelay',get_json('evento_confirmacion',json3));
			end if;
		else
			json3:='{}';
			json3:=put_json(json3,'RUT_EMISOR',split_part(id1,'_',1));
			json3:=put_json(json3,'TIPO_DTE',split_part(id1,'_',2));
			json3:=put_json(json3,'FOLIO',split_part(id1,'_',3));
			json3:=put_json(json3,'FECHA_EMISION',split_part(id1,'_',4));
			json3:=put_json(json3,'uri_dte',split_part(id1,'_',5));
			json3:=put_json(json3,'evento_confirmacion',split_part(id1,'_',6));
			json3:=put_json(json3,'CANAL',split_part(id1,'_',7));
			json3:=put_json(json3,'RUT_RECEPTOR',split_part(id1,'_',8));
			if (get_json('evento_confirmacion',json3)='') then
				json3:=put_json(json3,'eok',split_part(id1,'_',9));
				json3:=put_json(json3,'enk',split_part(id1,'_',10));
				json3:=put_json(json3,'edelay',split_part(id1,'_',10));
        			json2:=logjson(json2,'Eventos Nuevos');
			else
				json3:=put_json(json3,'eok',get_json('evento_confirmacion',json3));
				json3:=put_json(json3,'enk',get_json('evento_confirmacion',json3));
				json3:=put_json(json3,'edelay',get_json('evento_confirmacion',json3));
			end if;
		end if;
                --json2:=response_requests_6000('1','Evento Repetido','',json2);
		--return json2;
	--Nueva version con json
	elsif (substring(id1,1,3)='JCP') then
		json2:=logjson(json2,'Formato ID JSON');
		id1:=substring(id1,4);
		if get_json('DATA_DECRYPT',json2)<>'' then
			json2:=logjson(json2,'JCP Ocupo DATA_DECRYPT');
			id1:=decode_hex(get_json('DATA_DECRYPT',json2));
		else
			json2:=logjson(json2,'JCP SIN DATA_DECRYPT');
			--raise notice 'desencripta_hash_evento_VDC JCP %',json2;
			id1:=desencripta_hash_evento_VDC(id1);
		end if;
		json2:=logjson(json2,'DATA_DECRYPT ID1='||id1);
		if is_json_dict(id1) then
			json3:='{}';
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
		else
			json2:=logjson(json2,'Formato ID no json, error '||id1::varchar);
			json2:=response_requests_6000('1','Se ignora no json','',json2);
			return json2;
		end if;
	else
                json2:=logjson(json2,'Confirmacion sin ACP, se ignora');
                json2:=response_requests_6000('1','Confirmacion sin ACP, se ignora','',json2);
                return json2;
	end if;

	return evento_confirmacion_mail_5075(json2,json3);

        --return json2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION evento_confirmacion_mail_5075(json,json) RETURNS json AS $$
DECLARE
    json1       alias for $1;
        json2   json;
        json_data alias for $2;
        json3   json;
        json4   json;
        json5   json;
        campo   record;
        xml4    varchar;
        id1     varchar;
        accion1 varchar;
        action1 varchar;
        status1 varchar;
        json_par1       json;
        json_resp1      json;
        json_edte3      json;
        server_remoto1  varchar;
        lista1  json;
	nombre_tabla1	varchar;
	--id1	bigint;
	xml7	varchar;
	xml8	varchar;
BEGIN
	json2:=json1;
	json3:=json_data;

        json4:=get_json('delivery-status',json2)::json;
        xml4:='';
        xml4:=put_campo(xml4,'FECHA_EMISION',get_json('FECHA_EMISION',json3));
        xml4:=put_campo(xml4,'RUT_EMISOR',get_json('RUT_EMISOR',json3));
	--RME 20161026 Se agrega RUT_OWNER para graba_eventos_erp
	xml4:=put_campo(xml4,'RUT_OWNER',get_json('RUT_EMISOR',json3));
	--Si tengo la fecha de reproceso
	if (get_json('FECHA_REPROCESO',json4)<>'') then
		xml4:=put_campo(xml4,'FECHA_EVENTO',get_json('FECHA_REPROCESO',json4)::varchar);
	else
		xml4:=put_campo(xml4,'FECHA_EVENTO',now()::varchar);
	end if;
        xml4:=put_campo(xml4,'RUT_RECEPTOR',get_json('RUT_RECEPTOR',json3));
        xml4:=put_campo(xml4,'URI_IN',get_json('uri_dte',json3));

	json3:=put_json(json3,'evento_confirmacion',get_json('eok',json3));
        action1:=get_json('Action',json4);
        if (action1='failed') then
                accion1:='Fallida';
		json3:=put_json(json3,'evento_confirmacion',get_json('enk',json3));
        elsif (action1='delayed') then
                accion1:='Atrasado';
		json3:=put_json(json3,'evento_confirmacion',get_json('edelay',json3));
        elsif (action1='delivered') then
                accion1:='Exitosa';
        elsif (action1='relayed') then
                accion1:='Retransmitida';
        elsif (action1='expanded') then
                accion1:='Expandido';
	else
		accion1:=action1;
        end if;
        status1:=split_part(get_json('Diagnostic-Code',json4),';',2);
	--Las confirmaciones de envio del bcc no van
	--if (strpos(get_json('Original-Recipient',json4),'fernando.arancibia@acepta.com')>0) then
        --        json2:=response_requests_6000('1','Se ignora Evento para BCC','',json2);
        --        json2:=logjson(json2,'Se ignora Evento para BCC');
	--	return json2;
	--end if;

	if(trim(split_part(get_json('Original-Recipient',json4),';',2))='') then
		--Si no viene... lo descartamos
                json2:=logjson(json2,'Confirmacion sin Original-Recipient, se ignora');
                json2:=response_requests_6000('1','Confirmacion sin Original-Recipient, se ignora','',json2);
                return json2;
	end if;


	server_remoto1:=split_part(get_json('Remote-MTA',json4),';',2);
	if (server_remoto1='') then
		server_remoto1:=split_part(get_json('Reporting-MTA',json4),';',2);
	end if;

	--Si tiene comentario se agrega a la traza
	
        xml4:=put_campo(xml4,'COMENTARIO_TRAZA',replace(case when get_json('comentario',json_data)='' then '' else get_json('comentario',json_data)||chr(10) end||'Receptor: '||split_part(get_json('Original-Recipient',json4),';',2)||chr(10)||'Entrega: '||accion1||' Estado: '||get_json('Status',json4)||chr(10)||'Servidor Informante: '||split_part(get_json('Reporting-MTA',json4),';',2)||chr(10)||'Servidor Remoto: '||server_remoto1||chr(10)||'Codigo de Respuesta: '||status1||'.',chr(39),' '));
        xml4:=put_campo(xml4,'FOLIO',get_json('FOLIO',json3));
        xml4:=put_campo(xml4,'TIPO_DTE',get_json('TIPO_DTE',json3));
        xml4:=put_campo(xml4,'CANAL',get_json('CANAL',json3));
	xml4:=put_campo(xml4,'__FLUJO_ACTUAL__',get_json('__FLUJO_ACTUAL__',json1));
	----perform logfile('Graba Evento '||get_json('evento_confirmacion',json3)||' '||xml4);

	----perform logfile('Graba Evento evento='||get_json('evento_confirmacion',json3)||' xml4='||xml4::varchar);
	--perform logfile('confirmacion_mail_5075 graba_bitacora '||get_json('evento_confirmacion',json3)||' '||get_json('uri_dte',json3));

	--FAY-DAO 20210116 grabamos en las colas el graba bitacora
        --xml4:=graba_bitacora(xml4,get_json('evento_confirmacion',json3));
	nombre_tabla1:='cola_motor_4';
	
	--FAY 20210219 para ir cambiando la traza vamos por evento subiendolo
	
	--if (get_json('evento_confirmacion',json3)='EMS' and not exists (select 1 from tmp_fay)) then
	json2:='{}';
	
	--FAY-DAO No se graba evento si no hay URI
	if get_campo('URI_IN',xml4)<>'' and strpos(get_campo('URI_IN',xml4),'http')>0 then
		xml4:=graba_bitacora_aws(xml4,get_json('evento_confirmacion',json3));
		json2:=logjson(json2,get_campo('_LOG_',xml4));
	else
		json2:=logjson(json2,'No grabamos evento, no hay URI');
	end if;

	--Se limpia el json
	--json2:=logjson(json2,get_json('_LOG_',json1));
        json2:=put_json(json2,'__SECUENCIAOK__','0');
	json2:=put_json(json2,'__FLUJO_ACTUAL__',get_json('__FLUJO_ACTUAL__',json1));
	return response_requests_6000('1','Evento OK','',json2);
	/*
	json2:=logjson(json2,get_campo('_LOG_',xml4));
        if (get_campo('EVENTO_REPETIDO',xml4)='SI') then
                json2:=response_requests_6000('1','Evento Repetido','',json2);
                json2:=logjson(json2,'Evento Repetido');
        else
                json2:=response_requests_6000('1','Evento OK','',json2);
                json2:=logjson(json2,'Evento OK');
        end if;
	*/
        return json2;
END;
$$ LANGUAGE plpgsql;


--En la base --psql -h 172.16.14.94 -p 5433 sendmail -f
CREATE or replace FUNCTION graba_confirmacion_send_mail_5075(json) RETURNS json AS $$
DECLARE
    json1       alias for $1;
    json2   json;
    json4   json;
	accion1	varchar;
	server_remoto1	varchar;
	status1		varchar;
	id1	varchar;
	id2	varchar;
	action1	varchar;
	campo record;
	diag1	varchar;
	id_detalle1	bigint;
	adicional1	varchar;
	id_ori1	varchar;
	s1	varchar;
	rut1	varchar;
	rut_edte1	varchar;
BEGIN
	json2:=json1;
	json2:=replace(json2::varchar,'\u0006',';')::json;
        json2:=put_json(json2,'__SECUENCIAOK__','10');
        --json2:=logjson(json2,'Procesa Confirmaccion base send_mail '||json2::varchar);
        --Solo procesamos las confirmacion de lectura
        if (get_json('delivery-status',json2)='') then
                json2:=logjson(json2,'Confirmacion sin delivery-status, no se procesa');
                return json2;
        end if;
        if (get_json('rfc822-headers',json2)='') then
                json2:=logjson(json2,'Confirmacion sin rfc822-headers, no se procesa');
                return json2;
        end if;
	
	id1:=get_json('Message-ID',get_json('rfc822-headers',json2)::json);
        id1:=split_part(split_part(id1,'<',2),'@',1);

	if(get_json('categoria',json2)='CONFIRMACION_LECTURA_EDTE') then
		s1:=get_json('Subject',get_json('rfc822-headers',json2)::json);
		rut1:=split_part(id1,'-',2)||'-'||split_part(id1,'-',3);
		rut_edte1:=rut1;
		json2:=logjson(json2,'CONFIRMACION_LECTURA_EDTE '||id1||' rut='||rut1);
		--Viene del EDTE la confirmacion...
		if(strpos(id1,'EnvioDTE')>0) then
        		json2:=put_json(json2,'__SECUENCIAOK__','20');
			json2:=put_json(json2,'QUERY_DATA','select substring(data::varchar,3) as data from enviodtes where id='''||id1||''' and rutemisor='''||rut1||'''');
		elsif(strpos(id1,'EnvioRecibos')>0 and strpos(s1,'Acuse de Recibo de Mercaderias')>0) then  
        		json2:=put_json(json2,'__SECUENCIAOK__','20');
			json2:=put_json(json2,'QUERY_DATA','select substring(data::varchar,3) as data from enviorecibos where id='''||id1||''' and rutresponde='''||rut1||'''');
		elsif(strpos(id1,'RespuestaDTE')>0) then  
        		json2:=put_json(json2,'__SECUENCIAOK__','0');
			--Las confirmacion de envio de los CRT se ignorar
			json2:=response_requests_6000('1','Se ignora la confirmacion de envio del CRT','',json2);
			return json2;
		else
        		json2:=put_json(json2,'__SECUENCIAOK__','0');
			json2:=response_requests_6000('1','No reconozco prefijo Message-ID','',json2);
			return json2;
		end if;
		return json2;
	end if;

	id2:='';
	if (substring(id1,1,3)='JCP') then
                id1:=substring(id1,4);
		BEGIN
			raise notice 'desencripta_hash_evento_VDC1';
			id1:=desencripta_hash_evento_VDC(id1);
		EXCEPTION WHEN OTHERS THEN
			id1:='';
		END;	
		if (is_json_dict(id1)) then
			id2:=get_json('ID',id1::json);
			--20180831 FAY-DAO-MDA Si viene MSG_ORI no se necesita marcar en la traza
			if(get_json('MSG_ORI',id1::json)<>'') then
        			json2:=put_json(json2,'__SECUENCIAOK__','0');
				json2:=response_requests_6000('1','No requiere grabar Traza, Marcado ECM OK','',json2);
			end if;
			
			/*
			--Si viene msg original , lo reemplazamos para el marcado en la traza
			if(get_json('MSG_ORI',id1::json)<>'') then
				json2:=logjson(json2,'Se reemplaza el Msg Original MSG_ORI='||get_json('MSG_ORI',id1::json)||' Message-ID='||id1);
				json2:=put_json(json2,'categoria','CONFIRMACION_LECTURA_EDTE');
				json2:=put_json(json2,'rfc822-headers',
					put_json(get_json('rfc822-headers',json2)::json,'Message-ID',get_json('MSG_ORI',id1::json)));
			end if;
			*/
		else
			json2:=logjson(json2,'Formato id1 no JSON '||id1);	
		end if;
        elsif (substring(id1,1,3)='ACP') then
                id1:=substring(id1,4);
                id1:=desencripta_hash_evento_VDC(id1);
                if (strpos(id1,'##')>0) then
			--Solo obtengo el ID de la base send mail
			--Se cambia al 12
			if (is_number(split_part(id1,'##',12))) then
				id2:=split_part(id1,'##',12);
			elsif (is_number(split_part(id1,'##',13))) then
				id2:=split_part(id1,'##',13);
			elsif (is_number(split_part(id1,'##',11))) then
				id2:=split_part(id1,'##',11);
			end if;
		end if;
	else
       		json2:=logjson(json2,'Sin prefijo ACP');
	end if;	

	if (is_number(id2)) then
       		json2:=logjson(json2,'Confirmacion Mail con ID send_mail '||id2::varchar||' msg_id='||id1);
		--Inserto y actualizo el status de la base send_mail
		json4:=get_json('delivery-status',json2)::json;
		diag1:=get_json('Diagnostic-Code',json4);
		action1:=get_json('Action',json4);
		status1:=split_part(split_part(diag1,'; ',2),' ',1);
		server_remoto1:=get_json('Remote-MTA',json4);
		if (server_remoto1='') then
			server_remoto1:=get_json('Reporting-MTA',json4);
		end if;
		select * into campo from send_mail_detalle where id=id2::bigint and estado=diag1 and remoto=server_remoto1;
		if found then
			--Ya se encuentra grabado
			json2:=logjson(json2,'Ya esta grabado el detalle id2='||id2::varchar);
			id_detalle1:=campo.id_detalle;
		else
			insert into send_mail_detalle(id,fecha_ingreso,dia,receptor,accion,estado,informante,remoto,codigo) values (id2::bigint,now(),to_char(now(),'YYYYMMDD')::integer,get_json('Original-Recipient',json4),action1,diag1,get_json('Reporting-MTA',json4),server_remoto1,status1) returning id_detalle into id_detalle1;
			json2:=logjson(json2,'Se inserta detalle id_detalle='||id_detalle1::varchar);
		end if;

		--Busco la clasificacion que corresponde
		select * into campo from estado_mail where action1 ilike '%'||accion||'%' and diag1 ilike texto  order by id limit 1;
		if found then
			update send_mail_cabecera set estado=campo.codigo,fecha_actualizacion=now(),diagnostico='<Accion>'||coalesce(action1,'')||'</Accion><Diagnostico>'||coalesce(diag1,'')||'</Diagnostico><ID_Detalle>'||id_detalle1::varchar||'</ID_Detalle>' where id=id2::bigint and estado<>campo.codigo and estado not in ('250-OK','LEIDO') returning adicional into adicional1;
			if found then
				id_ori1:=get_xml('ID_ORI',adicional1);
				json2:=logjson(json2,'Actualiza estado id2 '||id2::varchar||' id_ori1='||id_ori1::varchar);
				if (is_number(id_ori1)) then
					update send_mail_cabecera set estado=campo.codigo,fecha_actualizacion=now(),diagnostico='<Accion>'||coalesce(action1,'')||'</Accion><Diagnostico>'||coalesce(diag1,'')||'</Diagnostico><ID_Detalle>'||id_detalle1::varchar||'</ID_Detalle>' where id=id_ori1::bigint and estado<>campo.codigo and estado not in ('250-OK','LEIDO');
				end if;
			end if;
		else
			update send_mail_cabecera set estado='999-SinEstado',fecha_actualizacion=now() where id=id2::bigint and estado<>'999-SinEstado' and estado not in ('250-OK','LEIDO') returning adicional into adicional1;
			if found then
				id_ori1:=get_xml('ID_ORI',adicional1);
				json2:=logjson(json2,'.Actualiza estado id2 '||id2::varchar||' id_ori1='||id_ori1::varchar);
				if (is_number(id_ori1)) then
					update send_mail_cabecera set estado='999-SinEstado',fecha_actualizacion=now() where id=id_ori1::bigint and estado<>'999-SinEstado' and estado not in ('250-OK','LEIDO');
				end if;
			end if;
		end if;
		return json2;
	else
		json2:=logjson(json2,'ID no numerico '||id2::varchar||'--'||id1::varchar);
	end if;
        return json2;
END;
$$ LANGUAGE plpgsql;
