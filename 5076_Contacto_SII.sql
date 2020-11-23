delete from isys_querys_tx where llave='5076';

insert into isys_querys_tx values ('5076',5,1,16,'LOG_JSON',0,0,0,1,1,10,10);
-- Prepara llamada al AML
insert into isys_querys_tx values ('5076',10,1,1,'select contacto_sii_5076(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

CREATE or replace FUNCTION contacto_sii_5076(json) RETURNS json AS $$
DECLARE
    json1       alias for $1;
        json2   json;
        json3   json;
        json4   json;
        json5   json;
        campo   record;
        campo1   record;
        campo_m   record;
        xml4    varchar;
        id1     varchar;
        accion1 varchar;
        action1 varchar;
        status1 varchar;
	delete1	boolean;
	json_par1	json;
	server_remoto1	varchar;
	delete2 boolean;

	boleta1		varchar;
	boleta_xml1		varchar;
	rut_emisor1	varchar;
	rut_receptor1	varchar;
	monto1		varchar;
	direccion1	varchar;
	giro1		varchar;
	folio1		varchar;
	fono1		varchar;
	fecha_emi1	varchar;
	fecha_emi2	timestamp;
	uri1		varchar;
	rut1		varchar;

	guion_largo1	varchar;
	header1	json;
	subject1	varchar;
	content1	varchar;
	aux2		varchar;
	aux		varchar;
	i		integer;
	j		integer;
	flag		boolean;
	lista1		json;
	per2		varchar;
	dias1		varchar;
	dia2		varchar;
	tipo_dte1	varchar;
	texto1		varchar;
	rut_cesionario1	varchar;
	fecha_cesion1	varchar;
	aux_tot_cesion1	varchar;
	aux_tot_cesion_orig1	varchar;
	monto_cesion1	varchar;
	flag1		boolean;
	fecha1		varchar;
	ces1		varchar;
	total1		varchar;
	
	data_dte1	varchar;
	cod_txel1	varchar;
	flag_origen1	varchar;
	texto2		varchar;
	rut_cedente1	varchar;
	
	xml3	varchar;
BEGIN
        json2:=json1;
	--Remplazamos los ; 
	json2:=logjson(json2,'Entra a 5076');
	json2:=replace(json2::varchar,'\u0006',';')::json;
        json2:=put_json(json2,'__SECUENCIAOK__','0');
        --json2:=logjson(json2,'JSON INPUT='||json2::varchar);

	header1:=get_json('header',json2)::json;
	subject1:=get_json('subject',header1);
	
	--Tenemos un resultado de una cesion, debemos marcar los eventos que correspondan
	if(get_json('categoria',json2)='BORRAR') then
		return response_requests_6000('1','Categoria de Borrado','',json2);
	elsif(get_json('categoria',json2)='RESULTADO_CESION') then
		--Obtenemos los datos
		aux2:=decode(get_json('data',json2),'hex');
		rut1:=get_json('rut_receptor',json2);
		--Si es cliente el rut receptor
		select * into campo_m from maestro_clientes where rut_emisor=rut1::integer;
		if not found then
			--Si no existe el maestro de cliente, ignoramos el correo de cesion
			return response_requests_6000('2','No existe en Maestro de Clientes '||rut1,'',json2);
		end if;

		--Si no esta aceptada la cesion	
		if (strpos(aux2,'Anotacion de Cesion Aceptada')=0) then
			return response_requests_6000('2','Cesion No aceptada '||rut1,'',json2);
		end if;
		if (strpos(aux2,'FACTURA ELECTRONICA N\260 ')>0) then
			tipo_dte1:='33';
		elsif (strpos(aux2,'FACTURA EXENTA ELECTRONICA N\260 ')>0) then
			tipo_dte1:='34';
		end if;
		--Sacamos los datos necesario
		folio1:=trim(split_part(split_part(aux2,'N\260 ',2),' -',1));
		rut_emisor1:=split_part(split_part(aux2,'Emisor  : ',2),'-',1);
		perform logfile('5076= '||coalesce(folio1,'')||','||coalesce(rut_emisor1,'')||','||coalesce(rut1,'')||','||coalesce(tipo_dte1,''));
		--Grabamos el evento en la traza
		texto1:='Cedido a: '||split_part(split_part(aux2,'Cedido a:  ',2),'\015\012',1);
		texto2:='Cedido por: '||split_part(split_part(aux2,'Cedido por:  ',2),'\015\012',1);

		rut_cesionario1:=split_part(split_part(texto1,'Cedido a: ',2),'-',1);
		rut_cedente1:=split_part(split_part(texto2,'Cedido por: ',2),'-',1);

		fecha_emi1:=split_part(split_part(aux2,'Fecha Emision: ',2),'\015\012',1);
		fecha_cesion1:=split_part(split_part(aux2,'Fecha de la Cesion: ',2),'\015\012',1);
		monto_cesion1:=split_part(split_part(aux2,'Monto Cedido: $ ',2),' ',1);
	        xml4:='';
	        xml4:=put_campo(xml4,'FECHA_EMISION',fecha_emi1);
	        xml4:=put_campo(xml4,'RUT_EMISOR',rut_emisor1::varchar);
        	xml4:=put_campo(xml4,'RUT_OWNER',rut1::varchar);
		aux:=split_part(split_part(aux2,'Fecha de Recepcion     : ',2),'\015\012',1);
	        xml4:=put_campo(xml4,'FECHA_EVENTO',to_timestamp(aux,'DD/MM/YYYY HH24:MI:SS')::varchar);
	        xml4:=put_campo(xml4,'RUT_RECEPTOR',rut1::varchar);
	        xml4:=put_campo(xml4,'COMENTARIO_TRAZA',texto1||chr(10)||texto2||chr(10)||'Monto Cedido: $ '||monto_cesion1::varchar||chr(10)||'Fecha Cesión: '||fecha_cesion1||chr(10)||'Identificador de Envio :'||split_part(split_part(aux2,'Identificador de Envio : ',2),'\015\012',1));
	        xml4:=put_campo(xml4,'FOLIO',folio1);
	        xml4:=put_campo(xml4,'TIPO_DTE',tipo_dte1);
	        xml4:=put_campo(xml4,'URL_GET',get_json('uri',json2));
	        xml4:=put_campo(xml4,'CANAL','RECIBIDOS');

		--Busco el DTE recibido que esta cedido
		json2:=logjson(json2,'RACS Busco Dte recibido '||rut_emisor1::varchar||' '||tipo_dte1::varchar||' '||folio1::varchar);
		select data_dte,codigo_txel,uri into campo1 from dte_recibidos where rut_emisor=rut_emisor1::integer and tipo_dte=tipo_dte1::integer and folio=folio1::bigint and rut_receptor=rut1::integer;
		if found then
			flag1:=true;
			flag_origen1:='RECIBIDOS';			
			json2:=logjson(json2,'RACS Dte recibido');
		else
			rut_emisor1:=rut_emisor1||'-'||modulo11(rut_emisor1);
			select data_dte,id as codigo_txel,uri into campo1 from dte_pendientes_recibidos where rut_emisor=rut_emisor1 and tipo_dte=tipo_dte1 and folio=folio1 and rut_receptor=rut1::integer;
			if found then
				flag1:=true;
				flag_origen1:='PENDIENTES';
				json2:=logjson(json2,'RACS Dte Pendiente');
			else
				--Se inserta en dte_pendientes_recibidos
				uri1:='http://'||case when coalesce(campo_m.dominio,'')='' then 'webdte' else campo_m.dominio end||to_char(now(),'YYMM')||'.acepta.com/v01/'||genera_uri2(split_part(rut_emisor1,'-',1),tipo_dte1::varchar,folio1::varchar,split_part(fecha_emi1,' ',1),split_part(split_part(aux2,'Monto Total: $ ',2),'\015\012',1),'R');
				--Lo insertamos en los pendientes
				insert into dte_pendientes_recibidos(fecha_ingreso,tipo_dte,folio,fecha_emision,fecha_recepcion_sii,rut_emisor,rut_receptor,nombre_emisor,monto_total,dia,dia_emision,uri) 
				values(now(),tipo_dte1::varchar,folio1::varchar,fecha_emi1,to_timestamp(aux,'DD/MM/YYYY HH24:MI:SS')::varchar,rut_emisor1,rut1::integer,split_part(split_part(split_part(aux2,'Emisor  : ',2),'  ',2),'\015\012',1),split_part(split_part(aux2,'Monto Total: $ ',2),'\015\012',1),to_char(now(),'YYYYMMDD')::integer,replace(fecha_emi1,'-','')::integer,uri1) returning data_dte,id as codigo_txel,uri into campo1;
				flag_origen1:='PENDIENTES';
				flag1:=true;
				json2:=logjson(json2,'RACS Dte Pendiente Insertado');
			end if;
		end if;
				
		--Actualizamos dte_recibidos
		if (get_xml('total_cesiones',coalesce(campo1.data_dte,''))='') then
			i:='1';
			aux_tot_cesion1='<total_cesiones>1</total_cesiones>';
			aux_tot_cesion_orig1='';
		else
			--Debemos verificar que no este registrada la misma cesion
			i:=1;
			aux:=get_xml('Cesion_'||i::varchar,campo1.data_dte);
			while (aux<>'') loop
				fecha1:=get_xml('Fecha',aux);
				monto1:=get_xml('Monto',aux);
				ces1:=get_xml('RutCesionario',aux);
				if (fecha1=fecha_cesion1 and monto1=monto_cesion1 and rut_cesionario1=ces1) then
					flag1:=false;
					exit;
				end if;
				i:=i+1;
				aux:=get_xml('Cesion_'||i::varchar,campo1.data_dte);
			end loop;
			
			aux_tot_cesion_orig1='<total_cesiones>'||get_xml('total_cesiones',campo1.data_dte)||'</total_cesiones>';
			i:=(get_xml('total_cesiones',campo1.data_dte)::integer+1)::varchar;
			aux_tot_cesion1='<total_cesiones>'||i||'</total_cesiones>';
		end if;

		--Si la cesion no esta registrada..
		if (flag1) then
			if (flag_origen1='RECIBIDOS') then
				json2:=logjson(json2,'RACS Actualizamos recibidos '||campo1.uri);
				update dte_recibidos set data_dte=replace(coalesce(data_dte,''),aux_tot_cesion_orig1,'')||aux_tot_cesion1||'<Cesion_'||i||'><EstadoCesion>CEDIDO</EstadoCesion><Cedente>'||texto2||'</Cedente><Cesionario>'||texto1||'</Cesionario><url_cesion>'||get_json('uri',json2)||'</url_cesion><RutCesionario>'||rut_cesionario1||'</RutCesionario><Fecha>'||fecha_cesion1||'</Fecha><Monto>'||monto_cesion1||'</Monto></Cesion_'||i::varchar||'>' where codigo_txel=campo1.codigo_txel;
			else
				json2:=logjson(json2,'RACS Actualizamos pendiente '||campo1.uri);
				update dte_pendientes_recibidos  set data_dte=replace(coalesce(data_dte,''),aux_tot_cesion_orig1,'')||aux_tot_cesion1||'<Cesion_'||i||'><EstadoCesion>CEDIDO</EstadoCesion><Cedente>'||texto2||'</Cedente><Cesionario>'||texto1||'</Cesionario><url_cesion>'||get_json('uri',json2)||'</url_cesion><RutCesionario>'||rut_cesionario1||'</RutCesionario><Fecha>'||fecha_cesion1||'</Fecha><Monto>'||monto_cesion1||'</Monto></Cesion_'||i::varchar||'>' where id=campo1.codigo_txel;
			end if;

			xml4:=put_campo(xml4,'URI_IN',campo1.uri);
			xml4:=graba_bitacora(xml4,'RACS');
			json2:=logjson(json2,'RACS '||get_campo('_LOG_',xml4));
			if (get_campo('EVENTO_REPETIDO',xml4)='SI') then
				return response_requests_6000('1','Evento Repetido '||rut1||' '||campo1.uri,'',json2);
			else
				return response_requests_6000('1','Evento OK '||rut1||' '||campo1.uri,'',json2);
			end if;
		else
			return response_requests_6000('1','Cesion ya registrada '||rut1,'',json2);
		end if;
		return response_requests_6000('2','Todo OK '||rut1,'',json2);
	elsif(get_json('categoria',json2)='AVISO_RCF') then
		--Debo verificar si el cliente tiene activo los RCF y ademas si tiene efectuados los RCF
		--{'categoria': 'AVISO_RCF', 'header': {'from': 'control_cof@sii.cl', 'fecha': '10 Apr 2016 03:15:39 -0300', 'Auto-Submitted': None, 'to': 'ALONSO.OVALLE.R@GMAIL.COM, CONTACTO_ADMIN@CUSTODIUM.COM, \r\n\tRECEPCION@CUSTODIUM.COM', 'msg-id': '<17036437.1460269815783.JavaMail.dte@dte>', 'Content-Type': 'text/html', 'subject': '=?ISO8859-1?Q?Debe_presentar_su_Reporte_de_Consumo_de_Foli?=\r\n =?ISO8859-1?Q?os_(RCOF)_de_Boleta_Electr=F3nica_de_76381966-3?='}, 'uri': 'https://servicios.acepta.com/bh/76381966-3/AvisoRCF_76381966-3_201611221523.html', 'periodos': [{'periodo': '2016-02', 'dias': '1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29 '}, {'periodo': '2016-03', 'dias': '1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31 '}], 'rut_cliente': '76381966-3'}
		rut1:=split_part(get_json('rut_cliente',json2),'-',1);
		uri1:=get_json('uri',json2);
		--Buscamos si tiene activo el RCF en el maestro de clientes
		select * into campo from maestro_clientes where rut_emisor=rut1::integer;
		if not found then
			--Si no existe el maestro de cliente, ignoramos el correo de aviso
			return response_requests_6000('1','No existe en Maestro de Clientes '||rut1,'',json2);
		end if;
		if (campo.estado<>'ACTIVO_QBIS') then
			--Si no existe el maestro de cliente, ignoramos el correo de aviso
			return response_requests_6000('1','Cliente no activo en Maestro de Clientes '||rut1,'',json2);
		end if;
		--Verificamos que este activo
		if (campo.rcf is null or campo.rcf<>'SI') then
			--Lo activamos y notificamos a Sistemas
			update maestro_clientes set rcf='SI' where rut_emisor=rut1::integer;
			perform send_mail_python('Se activo RCF para el cliente '||rut1||chr(10)||uri1,'Activacion RCF Cliente '||rut1,'sistemas@acepta.com','acepta@acepta.com');
			return response_requests_6000('1','Se activa RCF para '||rut1||' en Maestro de Clientes ','',json2);
		end if;

		--Recorremos los dias para ver si ya estan ok los RCF
		content1:='';
		lista1:=get_json('periodos',json2);
		i:=0;
		flag:=false;
		aux:=get_json_index(lista1,i);
		while (aux<>'') loop
			--Revimos los rcf para ver si esta aprobados por el SII
			per2:=replace(get_json('periodo',aux::json),'-','');
			j:=1;
			dias1:=trim(get_json('dias',aux::json));
			aux2:=split_part(dias1,',',j);
			while (aux2<>'') loop
				dia2:=per2||lpad(aux2,2,'0');
				select * into campo from rcf_data where rut_emisor=rut1::integer and dia_emision=dia2::integer;
				if not found then
					flag:=true;
					content1:=content1||' El dia '||dia2||' no tiene RCF'||chr(10);
				end if;
				if (strpos(campo.estado_sii,'ACEPTADO')=0) then
					flag:=true;
					content1:=content1||' El dia '||dia2||' tiene un RCF no aceptado '||coalesce(campo.estado_sii,'')||chr(10);
				end if;
				j:=j+1;
				aux2:=split_part(dias1,',',j);
			end loop;
			i:=i+1;
			aux:=get_json_index(lista1,i);
		end loop;
	
		if (flag) then
			content1:='RCF pendientes, para rut '||rut1||chr(10)||content1||' URI Aviso='||uri1;
			--Si esta activo y no hemos hecho la pega, ticket a sistemas
			perform send_mail_python(content1,'RCF Pendientes para Cliente '||rut1,'sistemas@acepta.com','acepta@acepta.com');
			return response_requests_6000('1','RCF pendientes, se envia mail a sistemas RUT '||rut1||' , RCF pendientes','',json2);
		end if;
		
		--Se borra, esta todo ok
		return response_requests_6000('1','Aviso RCF OK','',json2);
		
	elsif(get_json('categoria',json2)='AVISO_WEBIECV') then
		--Grabamos el aviso webiecv
		insert into aviso_maestro_clientes (rut,fecha,mensaje,uri,estado,categoria) values (split_part(get_json('rut_cliente',json2),'-',1)::integer,now(),'Aviso Libro Mensual '||get_json('periodo',json2),get_json('uri',json2),null,'AVISO_WEBIECV');
        	return response_requests_6000('1','Aviso Registrado OK','',json2);
	elsif(get_json('categoria',json2)='BOLETA_HONORARIO') then
		--Boletas
		boleta1:=decode(get_json('data_pdf',json2),'hex');
		boleta_xml1:=decode(get_json('data_xml',json2),'hex');
		guion_largo1:=decode('e28892','hex');
		
		rut_emisor1:=split_part(split_part(boleta_xml1,'<rutEmisor>',2),'</rutEmisor>',1);
		rut_receptor1:=split_part(split_part(boleta_xml1,'<rutReceptor>',2),'</rutReceptor>',1);
		monto1:=split_part(split_part(boleta_xml1,'<totalHonorarios>',2),'</totalHonorarios>',1);
		giro1:=split_part(split_part(boleta_xml1,'<actividadEconomica>',2),'</actividadEconomica>',1);
		fono1:=split_part(split_part(boleta_xml1,'<telefonoEmisor>',2),'</telefonoEmisor>',1);
		direccion1:=split_part(split_part(boleta_xml1,'<domicilioEmisor>',2),'</domicilioEmisor>',1);
		BEGIN
			fecha_emi1:=split_part(split_part(boleta_xml1,'<fechaBoleta>',2),'</fechaBoleta>',1)::date::varchar;
		EXCEPTION WHEN OTHERS THEN
			fecha_emi1:='';
		END;
		folio1:=split_part(split_part(boleta_xml1,'<numeroBoleta>',2),'</numeroBoleta>',1);
		
		/*
		--Parseamos los Datos
		rut_emisor1:=trim(replace(split_part(split_part(boleta1,'RUT: ',2),guion_largo1,1),'.',''));
		rut_receptor1:=trim(replace(split_part(split_part(boleta1,'Rut: ',2),guion_largo1,1),'.',''));
		monto1:=trim(replace(split_part(split_part(boleta1,'Total Honorarios $:',2),'\012',1),'.',''));
		--direccion1:=trim(split_part(split_part(boleta1,'Domicilio: ',2),'\012',1));
		giro1:=trim(split_part(split_part(boleta1,'GIRO(S): ',2),'\012',1))||' '||
			trim(split_part(split_part(boleta1,'GIRO(S): ',2),'\012',2));
		fono1:=trim(split_part(split_part(boleta1,'TELEFONO: ',2),'\012',1));
		direccion1:=trim(split_part(split_part(split_part(boleta1,'GIRO(S): ',2),'\012          ',2),'\012',1));
		fecha_emi1:=trim(split_part(split_part(boleta1,'Fecha / Hora Emisi\303\263n:',2),'\012',1));
		folio1:=trim(split_part(split_part(split_part(boleta1,'RUT: ',1),'N \302\260 ',2),'\012',1));
		if(folio1='' or folio1 is null) then
			folio1:=trim(split_part(split_part(split_part(boleta1,'RUT: ',1),'N\302\260',2),'\012',1));
		end if;
		*/
		json2:=logjson(json2,'DATOS PDF ='||rut_emisor1||' '||rut_receptor1||' '||monto1||' '||fono1||' '||direccion1||' '||fecha_emi1||' '||folio1);

		/*
		begin 
			fecha_emi2:=to_timestamp(fecha_emi1,'DD/MM/YYYY HH24:MI');
			fecha_emi1:=to_char(fecha_emi2,'YYYY-MM-DD');
		exception when others then
			fecha_emi1:='';
		end;	*/
		uri1:=get_json('uri_pdf',json2);
	
		json2:=logjson(json2,'DAO_DATOS_BOLETA_HON rut_emisor1='||rut_emisor1||' rut_receptor1='||rut_receptor1||' monto1='||monto1||' fecha_emi1='||fecha_emi1||' direccion1='||direccion1||' giro1='||giro1||' fono1='||fono1||' folio1='||folio1);

		if(rut_emisor1='' or rut_receptor1='' or monto1='' or is_number(rut_emisor1) is false or is_number(rut_receptor1) is false or is_number(monto1) is false or is_number(folio1) is false) then
        		return response_requests_6000('2','Error Boleta emisor['||rut_emisor1||'] receptor['||rut_receptor1||'] monto['||monto1||'] folio['||folio1||'] ','',json2);
		end if;

		select * into campo from dte_boletas_honorario_generica where rut_emisor=rut_emisor1::integer and tipo_dte=1 and folio=folio1::integer;
		if not found then
			insert into dte_boletas_honorario_generica(fecha_ingreso,mes,dia,tipo_dte,folio,fecha_emision,mes_emision,dia_emision,rut_emisor,rut_receptor,monto_total,estado,uri,estado_sii,data_dte,uri_xml) 
			values(now(),to_char(now(),'YYYYMM')::integer,to_char(now(),'YYYYMMDD')::integer,1,folio1::integer,fecha_emi1,to_char(fecha_emi2,'YYYYMM')::integer,to_char(fecha_emi2,'YYYYMMDD')::integer,rut_emisor1::integer,rut_receptor1::integer,monto1::integer,'BOLETA_GRABADA_OK',uri1,'BOLETA_GRABADA_OK','<Giro>'||coalesce(giro1,'')||'</Giro><Direccion>'||coalesce(direccion1,'')||'</Direccion><Fono>'||coalesce(fono1,'')||'</Fono>',get_json('uri_xml',json2));

			--Enviamos al ERP En caso de que corresponda
			xml3:=put_campo(xml3,'URI_IN',uri1);
		        xml3:=put_campo(xml3,'RUT_EMISOR',rut_emisor1::varchar);
			xml3:=put_campo(xml3,'RUT_RECEPTOR',rut_receptor1::varchar);
			xml3:=put_campo(xml3,'FOLIO',folio1::varchar);
			xml3:=put_campo(xml3,'PDF_ALMACEN',get_json('data_pdf',json2));
			xml3:=put_campo(xml3,'XML_ALMACEN',get_json('data_xml',json2));
			xml3:=graba_envio_erp_boleta_honorario(xml3);
			json2:=logjson(json2,'LOG graba_envio_erp_boleta_honorario='||get_campo('_LOG_',xml3));

        		return response_requests_6000('1','Datos OK rut_emisor1='||rut_emisor1||' rut_receptor1='||rut_receptor1||' monto1='||monto1||' fecha_emi2='||fecha_emi2,'',json2);
		else
        		return response_requests_6000('1','Boleta ya registrada rut_emisor1='||rut_emisor1||' rut_receptor1='||rut_receptor1||' monto1='||monto1||' fecha_emi2='||fecha_emi2,'',json2);
		end if;
	elsif(get_json('categoria',json2)='ANULACION_BOLETA_HONORARIO') then
		--Boletas
		boleta1:=decode(get_json('data',json2),'hex');
		guion_largo1:=decode('e28892','hex');
		
		--json2:=logjson(json2,'DAO_DATOS_BOLETA_HON data_pdf='||boleta1);

		--Parseamos los Datos
		--rut_emisor1:=get_json('rut_cliente',json2);
		rut_emisor1:=get_json('rut_emisor',json2);
		folio1:=trim(split_part(split_part(split_part(boleta1,'Rut N',1),'N\260 ',2),', ha sido',1));
		if(folio1='' or folio1 is null) then
			folio1:=trim(split_part(split_part(split_part(boleta1,'Rut N',1),'N\260',2),', ha sido',1));
		end if;
		
		select * into campo from dte_boletas_honorario_generica where rut_emisor=rut_emisor1::integer and tipo_dte=1 and folio=folio1::integer;
		if found then
			--Anulamos
			update dte_boletas_honorario_generica set data_dte=coalesce(data_dte,'')||'<uri_anulacion>'||get_json('uri',json2)||'</uri_anulacion>',estado_sii='ANULADA' where codigo_txel=campo.codigo_txel and estado_sii<>'ANULADA';
        		return response_requests_6000('1','Boleta Anulada OK '||rut_emisor1||' '||folio1,'',json2);
		else
        		return response_requests_6000('2','No se encuentra Boleta para anular '||rut_emisor1||' '||folio1,'',json2);
		end if;
	elsif(get_json('categoria',json2)='SOLICITA_ANULACION_BOLETA_HONORARIO') then
		--Boletas
                boleta1:=decode(get_json('data',json2),'hex');
                guion_largo1:=decode('e28892','hex');

                --json2:=logjson(json2,'DAO_DATOS_BOLETA_HON data_pdf='||boleta1);

                --Parseamos los Datos
                --rut_emisor1:=get_json('rut_cliente',json2);
                rut_emisor1:=get_json('rut_emisor',json2);
                folio1:=trim(split_part(split_part(split_part(boleta1,'Rut N',1),'N\260 ',2),', emitida por,',1));
                if(folio1='' or folio1 is null) then
                        folio1:=trim(split_part(split_part(split_part(boleta1,'Rut N',1),'N\260',2),', emitida por,',1));
                end if;

                select * into campo from dte_boletas_honorario_generica where rut_emisor=rut_emisor1::integer and tipo_dte=1 and folio=folio1::integer;
                if found then
                        --Anulamos
                        update dte_boletas_honorario_generica set data_dte=coalesce(data_dte,'')||'<uri_solicita_anulacion>'||get_json('uri',json2)||'</uri_solicita_anulacion>' where codigo_txel=campo.codigo_txel and strpos(data_dte,'uri_solicita_anulacion')=0;
                        return response_requests_6000('1','Solicita Anulacion OK '||rut_emisor1||' '||folio1,'',json2);
                else
                        return response_requests_6000('2','No se encuentra Boleta para adjuntar Solicita Anulacion  '||rut_emisor1||' '||folio1,'',json2);
                end if;
	else
        	return response_requests_6000('2','Categoria no definida','',json2);
	end if;



        return json2;
END;
$$ LANGUAGE plpgsql;

