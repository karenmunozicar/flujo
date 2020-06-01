CREATE or replace FUNCTION proc_procesa_bitacora_7010_Amazon(varchar) RETURNS json AS $$
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
	json1	json;
	query_json1	json;
BEGIN

	xml2:=xml1;
	--xml2:=logapp(xml2,'comienzo_bitacora_get_datos');	
    	data1	:=get_campo('INPUT',xml2);
	xml2	:=put_campo(xml2,'__SECUENCIAOK__','0');
	--url del documento a obtener
	if (strpos(data1,'url=')>0) then
		url1:= split_part(data1,'url=',2);
	end if; 
	xml2:=put_campo(xml2,'URI_IN',url1);

	--tabla_traza1:=get_tabla_traza(url1);
	tabla_traza1:='traza_global';
	
	--tabla_traza1:='traza.traza';

	execute 'select row_to_json(head) FROM (select to_char(coalesce(fecha_ingreso,fecha),''YYYY-MM-DD HH24:MI:SS'') as nodeTimeStamp,replace(folio,''-'','''') as nodeId,cfg.descripcion1 as nodeLabel,recipient.rut||''-''||modulo11(recipient.rut::varchar) as noOwnerUid,recipient.name as noOwner,recipient.email as noOwnerMail,receptor.rut||''-''||modulo11(receptor.rut::varchar) as ownerUid,receptor.name as owner,case when receptor.recipt_type is null then ''(receptor manual)'' when receptor.recipt_type=''M'' then ''(receptor manual)'' else receptor.email end as ownerMail,cfg.icono as nodeIcon, cfg2.descripcion1 as eventoemisor,
--Si es un libro de compras, la fecha de emision viene en el folio
case tipo_dte when ''COMPRA'' then (replace(folio,''-'','''')||''01'')::integer 
	      when ''VENTA'' then  (replace(folio,''-'','''')||''01'')::integer
else to_char(coalesce(fecha_ingreso,fecha),''YYYYMMDD'')::integer end as timeStamp from (select * from '||tabla_traza1||' where  uri= $1 and length(folio)>0 and fecha_emision>0 order by fecha_ingreso limit 1) trz left join recipient_traza_historico recipient  on trz.rut_emisor= recipient.rut left join recipient_traza_historico receptor on receptor.rut=trz.rut_receptor left join config cfg on trz.tipo_dte=cfg.evento left join config cfg2 on cfg2.evento=trz.evento ) head' into header1 using url1;

	--listado de documentos relacionados
	relacionados1 := array_to_json(array_agg(row_to_json(related))) from (select r.folio, cfg.descripcion1 as tipodte, r.url_relacion as url, cfg.icono  from (select * from documentos_relacionados where url = url1 ) r left join config cfg  on r.tipo_dte = cfg.evento ) related;
	xml2:=logapp(xml2,'relacionados1='||relacionados1);

	execute 'select array_to_json(array_agg(row_to_json(r))) from (SELECT to_char(fecha,''YYYY-MM-DD HH24:MI:SS'') as processedDate,canal,grupo,case when trz.evento in (''CONTROLLER'',''ADJ'') then trz.comentario2 else descripcion1 end as description,comentario1 as comment2,comentario2 as commentFragment ,icono as icon ,url_get as url  from (select distinct fecha,rut_emisor,canal,comentario1||case when comentario_erp is null then '''' else chr(10)||comentario_erp end as comentario1,comentario2,url_get,evento from '||tabla_traza1||' where uri= $1  and evento <>''EMI'') trz join config on trz.evento=config.evento left join recipient_traza_historico recipient  on trz.rut_emisor=recipient.rut  order by orden,fecha) r' into query2 using url1;
	--se ordenan las queris en un arreglo json, el primer elemento es la info de cabecera, el segundo un arreglo con los eventos
	--si una query no arroja resultados se envia arreglo vacio.
	--Traza Responsiva
        if(get_campo('FLAG_ESCRITORIO',xml2)='SI') then
                xml2:=logapp(xml2,'TRAZA FLAG_ESCRITORIO');
                query_json1:='{}';
                xml2:=logapp(xml2,'header1='||header1);

		--20200110 si vienen relacionados prevalecen
		if get_campo('RELACIONADOS',xml2)<>'' then
			relacionados1:=get_campo('RELACIONADOS',xml2);
		end if;
		select row_to_json(sql) into query_json1  from (select coalesce(header1,'{}')::json as "HEADER",coalesce(relacionados1,'[]')::json as "RELACIONADOS",coalesce(query2,'[]')::json as "EVENTOS") sql;

/*
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
		*/
		return query_json1;
	else
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
		return query1;
	end if;
	
END;
$$ LANGUAGE plpgsql;
CREATE or replace FUNCTION proc_procesa_bitacora_7010_Amazon2(varchar) RETURNS varchar AS $$
DECLARE
        xml1    alias for $1;
        xml2    varchar;
	query_json1	json;
	resp1	varchar;
BEGIN
        xml2:=xml1;
	query_json1:=proc_procesa_bitacora_7010_Amazon(xml2);
	resp1:='Status: 200 OK'||chr(10)||
	 'Content-type: json;'||chr(10)||
	 'Content-length: '||octet_length(query_json1::varchar)||chr(10);
	xml2:=put_campo(xml2,'RESPUESTA',resp1||chr(10)||query_json1::varchar);
	return xml2;
END;
$$ LANGUAGE plpgsql;
