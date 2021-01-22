delete from isys_querys_tx where llave='3000';

--insert into isys_querys_tx values ('3000',5,29,1,'select graba_mail_s3_3000(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
--vamos a las colas de motor1
--insert into isys_querys_tx values ('3000',5,1901,1,'select graba_mail_s3_3000(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('3000',5,19,1,'select graba_mail_s3_3000(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
--Para hacer log de todo
--insert into isys_querys_tx values ('3000',10,30,1,'select inserta_mail_3000(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('3000',10,19,1,'select inserta_mail_3000(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

--psql -h 172.16.14.94 -p 5433 sendmail -f
CREATE OR REPLACE FUNCTION graba_mail_s3_3000(varchar)
 RETURNS varchar
AS $$
DECLARE
	xml1	alias for $1;
	xml2	varchar;
	id1	bigint;
	xml3	varchar;
BEGIN
	xml2:=xml1;
	xml2 := put_campo(xml2,'RUT_EMISOR',split_part(get_campo('RUT_OWNER',xml2),'-',1));
	if(is_number(get_campo('ID_ECM',xml2))) then
		id1:=get_campo('ID_ECM',xml2)::bigint;
		xml2:=logapp(xml2,'ID_ECM='||id1::varchar);
	else
		id1:=get_id_ecm();
		--nextval('correlativo_send_mail');
	end if;
	xml2 := put_campo(xml2,'ID_MAIL',id1::varchar);
	xml2 := put_campo(xml2,'URI_IN','http://correo'||to_char(now(),'YYMM')||'.acepta.com/mail/'||to_char(now(),'DD')||id1::varchar||'_'||md5(get_campo('msg_id',xml2)));
	
	xml2:=graba_documento_s3(xml2);	
	xml2 := put_campo(xml2,'__SECUENCIAOK__','10');

	--FAY-DAO 2019-10-28 se optimiza la salida de la base de datos de manera de no retornar los campos que no se actualicen en el procesador, ya que este los tiene en memoria y solo los cambia si se devuelven
	xml3:='';
	xml3:=put_campo(xml3,'ID_MAIL',id1::varchar);
	xml3:=put_campo(xml3,'URI_IN','http://correo'||to_char(now(),'YYMM')||'.acepta.com/mail/'||to_char(now(),'DD')||id1::varchar||'_'||md5(get_campo('msg_id',xml2)));
	xml3:=put_campo(xml3,'__SECUENCIAOK__','10');
	xml3:=put_campo(xml3,'RUT_EMISOR',get_campo('RUT_EMISOR',xml2));
	xml3:=put_campo(xml3,'_LOG_',get_campo('_LOG_',xml2));
	return xml3;
	
END;
$$
LANGUAGE plpgsql;

--Se ejecuta en la base de mail para parsear los datos dentro del correo
create OR REPLACE FUNCTION public.parser_mail(correo1 text,j2 text)
 RETURNS json
 as $$
import cjson
import email
jout=cjson.decode(j2)
correo=correo1.decode('hex')
try:
	m=email.message_from_string(correo)
except:
        jout["ESTADO"]="FALLA"
        jout["MSG"]="Falla parseo de correo"
        return cjson.encode(jout)

#Obtenemos los datos que necesitamos del correo
jout["ESTADO"]="OK"
#jout["to"]=m["To"]
jout["cc"]=m["Cc"]
jout["bcc"]=m["Bcc"]
jout["subject"]=m["Subject"]
if "from_hex" not in jout:
        jout["from_hex"]=m["From"].encode('hex')
jout["from"]=m["From"]
jout["msg_id"]=m["Message-ID"]
return cjson.encode(jout)
$$
LANGUAGE plpythonu;



CREATE OR REPLACE FUNCTION public.inserta_mail_3000(json)
 RETURNS json
AS $$
DECLARE
	json1 		alias for $1;
	json2		json;
        i       bigint;
        id      bigint;
        cola    varchar;
	jout 	json;
	id_cola	bigint;
	msg_id1	varchar;
	categoria1	varchar;
	respuesta1	varchar;
	from1		varchar;
	j3		json;
	uri1		varchar;
	desen_msgid1	varchar;
	json_msgid1	json;
	ciclo1	varchar;
	adicional1	varchar;
	to_anterior1	varchar;
	to_actual1	varchar;
	rut_owner1	varchar;
	prioridad1	varchar;
	input1		varchar;
	to1	varchar;
	query1	varchar;
	flag1	varchar;
BEGIN
	json2:=json1;
	categoria1:=get_json('CATEGORIA',json2);
	--to_anterior1:=get_json('to',json2);
	input1:=get_json('INPUT_CUSTODIUM',json2);
	json2:=put_json(json2,'INPUT_CUSTODIUM','');
	
	--FAY-DAO 2020-04-14 
	if input1='' then
		json2:=logjson(json2,'Correo viene vacio');	
		jout:='{}';
        	jout:=put_json(jout,'CODIGO_RESPUESTA','2');
	        jout:=put_json(jout,'MENSAJE_RESPUESTA','El correo viene vacio');
        	jout:=put_json(jout,'URI',get_json('URI_IN',json2));

		respuesta1:='Status: 200' ||chr(10)||'Content-type: json'||chr(10)||chr(10)||jout::varchar;
		j3:=put_json('{}','RESPUESTA',respuesta1::varchar);
		j3:=put_json(j3,'_LOG_',get_json('_LOG_',json2));
		j3:=put_json(j3,'__FLUJO_ACTUAL__',get_json('__FLUJO_ACTUAL__',json2));
		j3:=put_json(j3,'__IDPROC__',get_json('__IDPROC__',json2));
        	return j3;
	end if;

	json2:=parser_mail(input1,json2::varchar);
	--to_actual1:=get_json('to',json2);

	--json2:=logjson(json2,'inserta_mail_3000 to_anterior='||to_anterior1||' actual='||to_actual1);	

	msg_id1:=get_json('msg_id',json2);
        i:=get_json('ID_MAIL',json2)::bigint;
        id:=nextval('id_colas_ecm');

	if (get_json('from_hex',json2)<>'') then
		from1:=decode(get_json('from_hex',json2),'hex');
	else
		from1:=get_json('from',json2);
	end if;
	if (strpos(from1,'<')>0) then
		from1:=split_part(split_part(from1,'<',2),'>',1);
	end if;
	json_msgid1:=null;	
	if (strpos(msg_id1,'<ACP')>0) then
		--desen_msgid1:=desencripta_hash_evento_VDC(split_part(split_part(msg_id1,'<ACP',2),'@',1));
		--20200324 FAY DAO, se cambia la funcion para que no haga un exec y mejore el performance
		desen_msgid1:=desencripta_hash_evento_vdc_python(split_part(split_part(msg_id1,'<ACP',2),'@',1));
		uri1:=split_part(desen_msgid1,'##',5);
	elsif (strpos(msg_id1,'<JCP')>0) then
		--desen_msgid1:=desencripta_hash_evento_VDC(split_part(split_part(msg_id1,'<JCP',2),'@',1));
		--20200324 FAY DAO, se cambia la funcion para que no haga un exec y mejore el performance
		desen_msgid1:=desencripta_hash_evento_vdc_python(split_part(split_part(msg_id1,'<JCP',2),'@',1));
		json_msgid1:=desen_msgid1::json;
		uri1:=get_json('U',desen_msgid1::json);
	elsif (get_json('DOCUMENTO',json2)<>'') then
		json2:=logjson(json2,'DOCUMENTO='||get_json('DOCUMENTO',json2));
		uri1:=get_json('DOCUMENTO',json2);
	end if;

	rut_owner1:=split_part(get_json('RUT_OWNER',json2),'-',1);
	if(is_number(rut_owner1) is false) then
		rut_owner1:='';
	end if;
	to1:=get_json('to',json2);
	--json2:=put_json(json2,'RUT_OWNER',rut_owner1);

	ciclo1:=get_json('CICLO',json2);
	if(is_number(ciclo1) is false) then
		ciclo1:='';
	end if;

	json2:=logjson(json2,'Inserto ID='||coalesce(i::varchar,'')||' msg_id='||coalesce(msg_id1,'')||' CATEGORIA='||coalesce(categoria1,'')||' en cola '||coalesce(id::varchar,'')||' '||rut_owner1||' Datos='||coalesce(desen_msgid1::varchar,'')|| ' PROCESOS='||get_json('__PROC_ACTIVOS__',json2));

	adicional1:=replace(replace(get_json('ADICIONAL',json2),'\\/','/'),chr(39),chr(39)||chr(39));
	adicional1:=replace(adicional1,'\/','/');
	--DAO 20180725-- Para DEC agregamos el Subject dentro de los Adicionales
	if strpos(get_json('CATEGORIA',json2),'DEC')>0 then
		adicional1:=adicional1||'<SUBJECT>'||replace(replace(get_json('subject',json2),'\\/','/'),chr(39),chr(39)||chr(39))||'</SUBJECT>';
	end if;

	flag1:='';
	--FAY 2021-01-15 se borra la ' para que no se caiga el insert
	adicional1:=replace(adicional1,chr(39),'');
	to1:=replace(to1,chr(39),'');
	from1:=replace(from1,chr(39),'');
	--Si viene con error de origen, solo se registra y no se envia
	if strpos(adicional1,'<ERROR_ORIGEN>')>0 then
		flag1:='SI';
		BEGIN
			query1:='insert into send_mail_cabecera_'||to_char(now(),'YYMM')||' (id,fecha_ingreso,dia,msg_id,estado,de,para,cc,bcc,dominio,uri,categoria,rut_owner,ciclo,adicional,documento,json_msg_id) values ('||i::varchar||','''||now()::varchar||'''::timestamp,'||to_char(now(),'YYYYMMDD')||','''||msg_id1||''','''||get_xml('ERROR_ORIGEN',adicional1)||''','''||from1||''','''||to1||''','''||get_json('cc',json2)||''','''||get_json('bcc',json2)||''','''||lower(split_part(split_part(to1,'@',2),'>',1))||''','''||get_json('URI_IN',json2)||''','''||get_json('CATEGORIA',json2)||''',nullif('''||rut_owner1||''','''')::bigint,nullif('''||coalesce(ciclo1::varchar,'')||''','''')::bigint,'''||coalesce(adicional1,'')||''','''||coalesce(uri1,'')||''',nullif('''||coalesce(json_msgid1::varchar,'')||''','''')::json ) returning id';
		EXCEPTION WHEN OTHERS THEN
			json2:=logjson(json2,'Falla insert tabla mensual .'||SQLERRM);
			query1:='insert into send_mail_cabecera (id,fecha_ingreso,dia,msg_id,estado,de,para,cc,bcc,dominio,uri,categoria,rut_owner,ciclo,adicional,documento,json_msg_id) values ('||i::varchar||','''||now()::varchar||'''::timestamp,'||to_char(now(),'YYYYMMDD')||','''||msg_id1||''','''||get_xml('ERROR_ORIGEN',adicional1)||''','''||from1||''','''||to1||''','''||get_json('cc',json2)||''','''||get_json('bcc',json2)||''','''||lower(split_part(split_part(to1,'@',2),'>',1))||''','''||get_json('URI_IN',json2)||''','''||get_json('CATEGORIA',json2)||''',nullif('''||rut_owner1||''','''')::bigint,nullif('''||coalesce(ciclo1::varchar,'')||''','''')::bigint,'''||coalesce(adicional1,'')||''','''||coalesce(uri1,'')||''',nullif('''||coalesce(json_msgid1::varchar,'')||''','''')::json ) returning id';
		END;
	else
		BEGIN
			query1:='insert into send_mail_cabecera_'||to_char(now(),'YYMM')||' (id,fecha_ingreso,dia,msg_id,estado,de,para,cc,bcc,dominio,uri,categoria,rut_owner,ciclo,adicional,documento,json_msg_id) values ('||i::varchar||','''||now()::varchar||'''::timestamp,'||to_char(now(),'YYYYMMDD')||','''||msg_id1||''',''POR_ENVIAR'','''||from1||''','''||to1||''','''||get_json('cc',json2)||''','''||get_json('bcc',json2)||''','''||lower(split_part(split_part(to1,'@',2),'>',1))||''','''||get_json('URI_IN',json2)||''','''||get_json('CATEGORIA',json2)||''',nullif('''||rut_owner1||''','''')::bigint,nullif('''||coalesce(ciclo1::varchar,'')||''','''')::bigint,'''||coalesce(adicional1,'')||''','''||coalesce(uri1,'')||''',nullif('''||coalesce(json_msgid1::varchar,'')||''','''')::json ) returning id';
		EXCEPTION WHEN OTHERS THEN
			json2:=logjson(json2,'Falla insert tabla mensual '||SQLERRM);
			query1:='insert into send_mail_cabecera (id,fecha_ingreso,dia,msg_id,estado,de,para,cc,bcc,dominio,uri,categoria,rut_owner,ciclo,adicional,documento,json_msg_id) values ('||i::varchar||','''||now()::varchar||'''::timestamp,'||to_char(now(),'YYYYMMDD')||','''||msg_id1||''',''POR_ENVIAR'','''||from1||''','''||to1||''','''||get_json('cc',json2)||''','''||get_json('bcc',json2)||''','''||lower(split_part(split_part(to1,'@',2),'>',1))||''','''||get_json('URI_IN',json2)||''','''||get_json('CATEGORIA',json2)||''',nullif('''||rut_owner1||''','''')::bigint,nullif('''||coalesce(ciclo1::varchar,'')||''','''')::bigint,'''||coalesce(adicional1,'')||''','''||coalesce(uri1,'')||''',nullif('''||coalesce(json_msgid1::varchar,'')||''','''')::json ) returning id';
		END;
	end if;
	
	j3=json2;
	j3:=put_json(j3,'RUT_OWNER',rut_owner1);
	j3:=put_json(j3,'INPUT_CUSTODIUM',input1);
	j3:=put_json(j3,'_LOG_','');
	j3:=put_json(j3,'QUERY_SMC',encode_hex(query1));
	j3:=put_json(j3,'ERROR_ORIGEN',flag1);
	--Si viene para casilla digital, no se envia
	if (strpos(to1,'@casilladigital.cl')>0) then
		cola:='cola_cd_'||id::varchar;
		execute 'insert into '||cola||' (id,fecha,data,reintentos,casilla,categoria) values ('||i||',now(),'||quote_literal(j3)::varchar||',0,'||quote_literal('casilladigital.cl')||',''CD'');';
	else
		--Obtenemos la prioridad de la categoria
		select prioridad::varchar into prioridad1 from categorias_send_mail where categoria=categoria1;
		if not found then
			prioridad1:='100';
		end if;
		cola:='cola_send_mail_'||id::varchar;
		execute 'insert into '||cola||' (id,fecha,data,reintentos,categoria,nombre_cola,message_id,prioridad) values ('||i||',now(),'||quote_literal(j3)||'::json,0,'||quote_literal(categoria1)||','||quote_literal(cola)||','||quote_literal(msg_id1)||','||prioridad1||') returning id 'into id_cola;
	end if;

	jout:='{}';
	jout:=put_json(jout,'CODIGO_RESPUESTA','1');
	jout:=put_json(jout,'COLA',cola);
	jout:=put_json(jout,'ID_COLA',id_cola::varchar);
	jout:=put_json(jout,'URI',get_json('URI_IN',json2));
	respuesta1:='Status: 200' ||chr(10)||'Content-type: json'||chr(10)||chr(10)||jout::varchar;
	j3:=put_json('{}','RESPUESTA',respuesta1::varchar);
	j3:=put_json(j3,'_LOG_',get_json('_LOG_',json2));
	j3:=put_json(j3,'__FLUJO_ACTUAL__',get_json('__FLUJO_ACTUAL__',json2));
	j3:=put_json(j3,'__IDPROC__',get_json('__IDPROC__',json2));
        return j3;
END;
$$
LANGUAGE plpgsql;

