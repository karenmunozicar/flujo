delete from isys_querys_tx where llave='7010';

insert into isys_querys_tx values ('7010',10,1,1,'select proc_procesa_bitacora_7010(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('7010',12,11,1,'select proc_procesa_bitacora_7010(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
--Copia desde Traza Antigua registro de la URI
insert into isys_querys_tx values ('7010',20,1,10,'/opt/acepta/motor/Procesos/copia_registro_traza_antigua.sh $$URI_IN$$ $$TABLA_TRAZA$$',0,0,0,1,1,10,10);

--Bases Traza
--Traza 2014
insert into isys_querys_tx values ('7010',2014,38,1,'select proc_procesa_bitacora_7010_Amazon2(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
--Traza 2015
insert into isys_querys_tx values ('7010',2015,37,1,'select proc_procesa_bitacora_7010_Amazon2(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
--Traza 2016
insert into isys_querys_tx values ('7010',2016,36,1,'select proc_procesa_bitacora_7010_Amazon2(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
--Traza 2017
insert into isys_querys_tx values ('7010',2017,33,1,'select proc_procesa_bitacora_7010_Amazon2(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
--Traza 2018
insert into isys_querys_tx values ('7010',2018,46,1,'select proc_procesa_bitacora_7010_Amazon2(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
--Traza 2019
insert into isys_querys_tx values ('7010',2019,49,1,'select proc_procesa_bitacora_7010_Amazon2(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
--Traza 2020
insert into isys_querys_tx values ('7010',2020,50,1,'select proc_procesa_bitacora_7010_Amazon2(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);



CREATE or replace FUNCTION proc_procesa_bitacora_7010(varchar) RETURNS varchar AS $$
DECLARE
	xml1	alias for $1;
	xml2	varchar;
	data1	varchar;
        temp    integer;
	resp1	varchar;
	respuesta1	varchar;
	output1 varchar;
	query1  varchar;
	url1    varchar;
	query2  varchar;
	header1 varchar;
	jsonq	varchar;
	relacionados1 varchar;
	mes1	varchar;
	dominio1	varchar;
	fecha1		varchar;
	tabla_traza1	varchar;
	total1	integer;
	xml4	varchar;
	json1	json;
	json2	json;
	t2	timestamp;
	t1	timestamp;
	json_par1	json;
	xml3	varchar;
	json_traza_traza1	json;
	json_aux1		json;
	lista1		json;
	query_json1	json;
	parametro1	varchar;
BEGIN

	xml2:=xml1;
	
	--FAY 2015-04-09 Si la base de replica esta OK vamos a hacer esta consulta a esa base
	/*
	if (verifica_base_replica()='OK') then
		xml2	:=logapp(xml2,'Se Procesa en base de replica');
		xml2	:=put_campo(xml2,'__SECUENCIAOK__','12');
		return xml2;
	end if;
	*/

	--Si viene de respuesta de Traza Antigua, mostramos que hizo
	if get_campo('BUSCA_TRAZA_ANTIGUA',xml2)='SI' then
		xml2:=logapp(xml2,'Respuesta Traza Antigua '||get_campo('RESPUESTA_SYSTEM',xml2));
	end if;
	--xml2:=logapp(xml2,'comienzo_bitacora_get_datos');	
    	data1	:=get_campo('INPUT',xml2);
	xml2:=logapp(xml2,'data1='||data1);
	xml2	:=put_campo(xml2,'__SECUENCIAOK__','0');
	--url del documento a obtener
	if (strpos(data1,'url=')>0) then
		url1:= split_part(data1,'url=',2);
	end if; 
	xml2:=logapp(xml2,'URI='||url1);
	xml2:=put_campo(xml2,'URI_IN',url1);

	dominio1:=split_part(split_part(url1,'//',2),'.',1);
	fecha1:=substring(dominio1,length(dominio1)-3,4);	
	
	tabla_traza1:=get_tabla_traza(url1);
	xml3:=parametros_traza(tabla_traza1);
	xml2 := get_parametros_motor(xml2,'TRAZA_ANTIGUA');

	--if (is_number(fecha1)) then
		--A partir de esta fecha todo es traza antigua
		--if (fecha1::integer<1309) then
			--Buscamons en traza antigua
	if (get_campo('PARAMETRO_TRAZA',xml3)='TRAZA_ANTIGUA') then
			--xml2 := get_parametros_motor(xml2,'TRAZA_ANTIGUA');
			json1:=query_db_json(get_campo('__IP_CONEXION_CLIENTE__',xml2),get_campo('__IP_PORT_CLIENTE__',xml2)::integer,'select proc_get_traza_antigua('''||url1||''')');
			if (get_json('STATUS',json1)='OK') then
				query1:=get_json('proc_get_traza_antigua',json1);

				--Si no hay eventos en la traza antigua, contestamos con los valores de la tabla traza.traza
				if query1<>'' then
					--Verificamos que si existen eventos nuevos los sacamos desde la traza.traza
					execute 'select array_to_json(array_agg(row_to_json(r))) from (SELECT to_char(fecha,''YYYY-MM-DD HH24:MI:SS'') as processedDate,canal,grupo,case when trz.evento in (''CONTROLLER'',''ADJ'') then trz.comentario2 else descripcion1 end as description,comentario1 as comment2,comentario2 as commentFragment ,icono as icon ,url_get as url  from (select distinct fecha,rut_emisor,canal,comentario1||case when comentario_erp is null then '''' else chr(10)||comentario_erp end as comentario1,comentario2,url_get,evento from traza.traza where uri= $1  and evento <>''EMI'') trz join traza.config on trz.evento=traza.config.evento left join recipient_traza_historico recipient  on trz.rut_emisor=recipient.rut  order by orden,fecha) r' into json_traza_traza1 using url1;
					if (json_traza_traza1 is not null) then
						--Debo agregar los eventos nuevos a la traza antigua
						json_aux1:=query1::json;
						--Volvemos a hacer la lista para contestar
						lista1:='[]';
						--se agrega el header 
						lista1:=put_json_list(lista1,get_json_index(json_aux1,0));
						--se agregan los relacionados
						lista1:=put_json_list(lista1,get_json_index(json_aux1,1));
						--se agregan los eventos
						lista1:=put_json_list(lista1,json_merge_lists(get_json_index(json_aux1,2)::varchar,json_traza_traza1::varchar));
						query1:=lista1::varchar;	
					end if;	
					xml2:=logapp(xml2,'respuesta traza antigua'||query1);
	       				resp1:='Status: 200 OK'||chr(10)||
			                 'Content-type: application/json;charset=UTF-8;'||chr(10)||
			                 'Content-length: '||length(query1)||chr(10);
					xml2:=put_campo(xml2,'RESPUESTA',resp1||chr(10)||query1);
					xml2 := put_campo(xml2,'__SOCKET_RESPONSE__','RESPUESTA');
				    	RETURN xml2;
				else
					--Si no hay eventos antiguos, los obtengo de la traza local
					xml3:=put_campo(xml3,'TRAZA_REMOTO','NO');
				end if;
			end if;
		--end if;
	end if;


	--xml2:=logapp(xml2,'DAOTRAZA tabla_traza1='||coalesce(tabla_traza1,''));
	
	--Si la tabla traza es traza.traza o traza.traza_1401, entonces nos vamos a amazon RDS para sacar la info
	--if (tabla_traza1 in ('traza.traza','traza.traza_1401','traza.traza_1402','traza.traza_1403','traza.traza_1404','traza.traza_1405','traza.traza_1406','traza.traza_1407','traza.traza_1408','traza.traza_1409','traza.traza_1410','traza.traza_1501','traza.traza_1411','traza.traza_1502','traza.traza_1412','traza.traza_1503','traza.traza_1504','traza.traza_1505','traza.traza_1506','traza.traza_1507','traza.traza_1508','traza.traza_1509','traza.traza_1510')) then

	if (get_campo('TRAZA_REMOTO',xml3)='SI') then
		--xml2:=logapp(xml2,'REMOTO: Vamos Traza a motor.cv7s4kltouy4.us-east-1.rds.amazonaws.com');
		xml2:=logapp(xml2,'REMOTO: Vamos Traza a '||xml3::varchar);
		xml4:='';
		xml4:=put_campo(xml4,'INPUT','url='||url1);
		xml4:=put_campo(xml4,'FLAG_ESCRITORIO',get_campo('FLAG_ESCRITORIO',xml2));
		--Armamos paquete para ir a amazon a traves del proxy
		--select query_db_json('172.16.10.91','8015','select proc_procesa_bitacora_7010(''INPUT[]=url=http://cencosudretail1409.acepta.com/v01/B6F70B486825F5F6F9AE3277EA1B89D7DDC6859E?k=2ac44a1b7d60b807bc294cf84e29995b###TABLA_TRAZA[]=traza_global###'')')
		xml2:=logapp(xml2,'URL='||url1||' '||get_campo('PARAMETRO_TRAZA',xml3)||' '||get_campo('FLAG_ESCRITORIO',xml2));
		--20200110 Cambiamos para que vaya a una secuencia para no cargar la base
		parametro1:=get_campo('PARAMETRO_TRAZA',xml3);
		if get_campo('FLAG_ESCRITORIO',xml2)='SI' and is_number(split_part(parametro1,'_',2)) then
			xml2:=logapp(xml2,'URL='||url1||' vamos a la base de amazon que corresponda');
			--Antes de ir a la base de Amazon sacamos los Relacionados
			relacionados1 := array_to_json(array_agg(row_to_json(related))) from (select r.folio, cfg.descripcion1 as tipodte, r.url_relacion as url, cfg.icono  from (select * from documentos_relacionados where url = url1 ) r left join traza.config cfg  on r.tipo_dte = cfg.evento ) related;
			xml2:=put_campo(xml2,'RELACIONADOS',relacionados1);
			xml2:=put_campo(xml2,'__SECUENCIAOK__',split_part(parametro1,'_',2));
			return xml2;
		end if;

		t1:=clock_timestamp();
		json1:=query_db_json(get_campo('__IP_CONEXION_CLIENTE__',xml3),get_campo('__IP_PORT_CLIENTE__',xml3)::integer,'select proc_procesa_bitacora_7010_Amazon('||quote_literal(xml4)||')');
		t2:=clock_timestamp();
		begin
			json2:=get_json('proc_procesa_bitacora_7010_amazon',json1);
		EXCEPTION WHEN OTHERS THEN
			xml2:=logapp(xml2,'REMOTO: ERROR EN JSON Respuesta='||coalesce(json1::varchar,'NULO'));
			--Buscamons en traza antigua
			xml2 := get_parametros_motor(xml2,'TRAZA_ANTIGUA');
			json1:=query_db_json(get_campo('__IP_CONEXION_CLIENTE__',xml2),get_campo('__IP_PORT_CLIENTE__',xml2)::integer,'select proc_get_traza_antigua('''||url1||''')');
			if (get_json('STATUS',json1)='OK') then
				query1:=get_json('proc_get_traza_antigua',json1);
				xml2:=logapp(xml2,'respuesta traza antigua'||query1);
       				resp1:='Status: 200 OK'||chr(10)||
		                 'Content-type: application/json;charset=UTF-8;'||chr(10)||
		                 'Content-length: '||length(query1)||chr(10);
				xml2:=put_campo(xml2,'RESPUESTA',resp1||chr(10)||query1);
				xml2 := put_campo(xml2,'__SOCKET_RESPONSE__','RESPUESTA');
			    	RETURN xml2;
			end if;
		end;
		--MDA 2018-06-18 si no vienen relacionados se buscan aca
		--if(get_json('RELACIONADOS',json2)='[ ]')then
			relacionados1 := array_to_json(array_agg(row_to_json(related))) from (select r.folio, cfg.descripcion1 as tipodte, r.url_relacion as url, cfg.icono  from (select * from documentos_relacionados where url = url1 ) r left join traza.config cfg  on r.tipo_dte = cfg.evento ) related;
			--json2:=put_json(json2,'RELACIONADOS','');
			json2:=put_json(json2,'RELACIONADOS',relacionados1);
		--end if;
		xml2:=logapp(xml2,'REMOTO: Respuesta='||json2::varchar);
			resp1:='Status: 200 OK'||chr(10)||
				 'Content-type: application/json;charset=UTF-8;'||chr(10)||
				 'Content-length: '||octet_length(json2::varchar)||chr(10);
			xml2:=put_campo(xml2,'RESPUESTA',resp1||chr(10)||json2::varchar);
			xml2 := put_campo(xml2,'__SOCKET_RESPONSE__','RESPUESTA');
			xml2:=logapp(xml2,'REMOTO: Fin Llamada  Tiempo='||(t2-t1)::varchar);
			return xml2;	
	

		--xml2:=query_remoto_c('proxy_amazon_netglobalis.rds.amazonaws.com','5432','traza','motor','motoracepta','select proc_procesa_bitacora_7010('||quote_literal(xml2)||')',xml2);
		--json1:=query_db_json('172.16.10.91','8015','select proc_procesa_bitacora_7010('||quote_literal(xml4)||')');


		--xml2:=query_remoto_c('motor.cv7s4kltouy4.us-east-1.rds.amazonaws.com','5432','traza','motor','motoracepta','select proc_procesa_bitacora_7010('||quote_literal(xml2)||')',xml2);
		--xml2:=logapp(xml2,'REMOTO: Fin Llamada Respuesta='||replace(replace(xml2,'###','&&&'),chr(10),'\n')||' Tiempo='||(t2-t1)::varchar);
		--return xml2;
	end if;

	xml2:=put_campo(xml2,'TABLA_TRAZA',tabla_traza1);
	xml2:=logapp(xml2,'Tabla Traza '||tabla_traza1);

	--FAY 2018-02-26 se genera el dato orden para que saque primero los que tengan rut receptor
	execute 'select row_to_json(head) FROM (select to_char(coalesce(fecha_ingreso,fecha),''YYYY-MM-DD HH24:MI:SS'') as nodeTimeStamp,replace(folio,''-'','''') as nodeId,cfg.descripcion1 as nodeLabel,trz.rut_emisor||''-''||modulo11(trz.rut_emisor::varchar) as noOwnerUid,case when recipient.name is null then (select nombre from contribuyentes where rut_emisor=trz.rut_emisor limit 1) else recipient.name end as noOwner,recipient.email as noOwnerMail,receptor.rut||''-''||modulo11(receptor.rut::varchar) as ownerUid,receptor.name||''.'' as owner,case when receptor.recipt_type is null then ''(receptor manual)'' when receptor.recipt_type=''M'' then ''(receptor manual)'' else receptor.email end as ownerMail,cfg.icono as nodeIcon, cfg2.descripcion1 as eventoemisor,
--Si es un libro de compras, la fecha de emision viene en el folio
case tipo_dte when ''COMPRA'' then (replace(folio,''-'','''')||''01'')::integer 
	      when ''VENTA'' then  (replace(folio,''-'','''')||''01'')::integer
else to_char(coalesce(fecha_ingreso,fecha),''YYYYMMDD'')::integer end as timeStamp from (select *,case when rut_receptor is not null then 0 else 99 end as orden from '||tabla_traza1||' where  uri= $1 and length(folio)>0 and fecha_emision>0 order by orden,fecha_ingreso limit 1) trz left join recipient_traza_historico recipient  on trz.rut_emisor= recipient.rut left join recipient_traza_historico receptor on receptor.rut=trz.rut_receptor left join traza.config cfg on trz.tipo_dte=cfg.evento left join traza.config cfg2 on cfg2.evento=trz.evento ) head' into header1 using url1;

	/*
	--Si no hay header y la uri es antigua buscamos en traza old
	if (header1 is null) then
		json_par1:=
	end if;	
	*/

	xml2:=logapp(xml2,'header1='||header1);
	xml2:=logapp(xml2,'head='||header1);
	
	--listado de documentos relacionados
	relacionados1 := array_to_json(array_agg(row_to_json(related))) from (select r.folio, cfg.descripcion1 as tipodte, r.url_relacion as url, cfg.icono  from (select * from documentos_relacionados where url = url1 ) r left join traza.config cfg  on r.tipo_dte = cfg.evento ) related;
	xml2:=logapp(xml2,'relacionados1='||relacionados1);

	execute 'select array_to_json(array_agg(row_to_json(r))) from (SELECT to_char(fecha,''YYYY-MM-DD HH24:MI:SS'') as processedDate,canal,grupo,case when trz.evento in (''CONTROLLER'',''ADJ'') then decode_utf8(trz.comentario2) else replace(descripcion1,chr(10),''<br>'') end as description,replace(comentario1,chr(10),''<br>'')  as comment2,comentario2 as commentFragment ,icono as icon ,url_get as url  from (select distinct fecha,rut_emisor,canal,decode_utf8(comentario1)||case when comentario_erp is null then '''' else chr(10)||decode_utf8(comentario_erp) end as comentario1,decode_utf8(comentario2) as comentario2,url_get,evento from '||tabla_traza1||' where uri= $1  and evento <>''EMI'') trz join traza.config on trz.evento=traza.config.evento left join recipient_traza_historico recipient  on trz.rut_emisor=recipient.rut  order by orden,fecha) r' into query2 using url1;
	--xml2:=logapp(xml2,'Eventos='||query2);
	

	--se ordenan las queris en un arreglo json, el primer elemento es la info de cabecera, el segundo un arreglo con los eventos
	--si una query no arroja resultados se envia arreglo vacio.
	query1 := '[';
	IF (header1 IS NOT NULL) THEN
		query1:=query1||header1||',';
	END IF;
	IF (relacionados1 IS NOT NULL) THEN
		query1 := query1||relacionados1||',';
	ELSE
		query1 := query1||'[],';
	END IF;
	IF (query2 IS NOT NULL) THEN
		query1:= query1 || query2;
	END IF;

	query1 := query1 || ']';


	--Traza Responsiva 
	if(get_campo('FLAG_ESCRITORIO',xml2)='SI') then
		xml2:=logapp(xml2,'TRAZA FLAG_ESCRITORIO');
                query_json1:='{}';
		xml2:=logapp(xml2,'header1='||header1);
		
                IF (header1 IS NOT NULL) THEN
                        query_json1:=put_json(query_json1,'HEADER',header1);
                else
                        query_json1:=put_json(query_json1,'HEADER','');
                end if;
                if(relacionados1 is not null) then
                        query_json1:=put_json(query_json1,'RELACIONADOS',relacionados1);
                else
                        query_json1:=put_json(query_json1,'RELACIONADOS','');
                end if;
                IF (query2 IS NOT NULL) THEN
                        query_json1:=put_json(query_json1,'EVENTOS',query2);
                else
                        query_json1:=put_json(query_json1,'EVENTOS','');
                end if;

                 --'Content-type: application/json;charset=UTF-8;'||chr(10)||
                resp1:='Status: 200 OK'||chr(10)||
                 'Content-type: application/json;'||chr(10)||
                 'Content-length: '||octet_length(query_json1::varchar)||chr(10);
                xml2:=put_campo(xml2,'RESPUESTA',resp1||chr(10)||query_json1::varchar);
                --xml2:=put_campo(xml2,'RESPUESTA',query_json1::varchar);
                xml2 := put_campo(xml2,'__SOCKET_RESPONSE__','RESPUESTA');
                return xml2;
        end if;


	--Si la respuesta de la traza es nula, lo buscamos en las colas
	/*
	if (query1='[[],]') then
		json_par1:=get_parametros_motor_json('{}','BASE_COLAS');
		json3:=query_db_json(get_json('__IP_CONEXION_CLIENTE__',json_par1),get_json_int('__IP_PORT_CLIENTE__',json_par1)::integer,'select row_to_json(sql) from (select fecha,uri,reintentos,xml_flags,tx,rut_emisor,categoria,rut_receptor,tipo_dte,folio,get_campo(''CONTENIDO'',data) as contenido  from colas_motor_generica where uri='||quote_literal(url1)||' and xml_flags is null) sql');
		if (get_json('STATUS',json3)='OK') then
			json3:=get_json('row_to_json',json3)::json;
			--Armo la respuesta de la traza
			execute 'select row_to_json(head) FROM (select to_char('||quote_literal(get_json('fecha',json3))||'::timestamp,''YYYY-MM-DD HH24:MI:SS'') as nodeTimeStamp,(||'quote_literal(replace(get_json('folio',json3),'-',''))||' as nodeId,''DTE Ingresado'' as nodeLabel,'||quote_literal(get_json('rut_emisor',json3)||'-'||modulo11(get_json('rut_emisor',json3)))|| ' as noOwnerUid,(select nombre from contribuyentes where rut_emisor=trz.rut_emisor limit 1) else recipient.name end as noOwner,recipient.email as noOwnerMail,receptor.rut||''-''||modulo11(receptor.rut::varchar) as ownerUid,receptor.name||''.'' as owner,case when receptor.recipt_type is null then ''(receptor manual)'' when receptor.recipt_type=''M'' then ''(receptor manual)'' else receptor.email end as ownerMail,cfg.icono as nodeIcon, cfg2.descripcion1 as eventoemisor,
--Si es un libro de compras, la fecha de emision viene en el folio
case tipo_dte when ''COMPRA'' then (replace(folio,''-'','''')||''01'')::integer 
	      when ''VENTA'' then  (replace(folio,''-'','''')||''01'')::integer
else to_char(coalesce(fecha_ingreso,fecha),''YYYYMMDD'')::integer end as timeStamp from (select * from '||tabla_traza1||' where  uri= $1 and length(folio)>0 and fecha_emision>0 order by fecha_ingreso limit 1) trz left join recipient_traza_historico recipient  on trz.rut_emisor= recipient.rut left join recipient_traza_historico receptor on receptor.rut=trz.rut_receptor left join traza.config cfg on trz.tipo_dte=cfg.evento left join traza.config cfg2 on cfg2.evento=trz.evento ) head' into header1 using url1;
			
		end if;
	end if;

	*/
	xml2:=logapp(xml2,'respuesta query'||query1);
	--envio respuesta content tipe para json es application/json, en otro caso la respuesta llega como error
        resp1:='Status: 200 OK'||chr(10)||
                 'Content-type: application/json;charset=UTF-8;'||chr(10)||
                 'Content-length: '||length(query1)||chr(10);
	xml2:=put_campo(xml2,'RESPUESTA',resp1||chr(10)||query1);
	--xml2:=put_campo(xml2,'RESPUESTA',query1);
	xml2 := put_campo(xml2,'__SOCKET_RESPONSE__','RESPUESTA');
        xml2 := logapp(xml2,'Respuesta Servicio 200 OK');
	xml2 := logapp(xml2,'JSONSend '||query1);
	xml2 := logapp(xml2,'fin_bitacora_get_datos');
    	RETURN xml2;
	
END;
$$ LANGUAGE plpgsql;
