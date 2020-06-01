--Publica documento
delete from isys_querys_tx where llave='12789';
insert into isys_querys_tx values ('12789',2,1,1,'select proc_pre_procesa_get_xml_12789(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
--Obtiene el DTE Original con la entrada URI_IN
insert into isys_querys_tx values ('12789',5,1,8,'GET XML desde Almacen',12705,0,0,1,1,10,10);
insert into isys_querys_tx values ('12789',6,1,8,'GET XML desde Almacen',7010,0,0,1,1,0,0);

insert into isys_querys_tx values ('12789',10,1,1,'select proc_procesa_get_xml_12789(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

--Busco Info de XSL en ca4webv3
insert into isys_querys_tx values ('12789',12,1,8,'GET Info XSL desde Almacen',12705,0,0,1,1,14,14);
insert into isys_querys_tx values ('12789',14,1,1,'select proc_procesa_info_xsl_12789(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);


insert into isys_querys_tx values ('12789',15,1,8,'GET XSL desde Almacen',12705,0,0,1,1,20,20);

insert into isys_querys_tx values ('12789',20,1,1,'select proc_procesa_get_html_12789(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

CREATE or replace FUNCTION proc_pre_procesa_get_xml_12789(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
        xml2    varchar;
	uri1	varchar;
	stResp	respaldo_dte%ROWTYPE;
	data1	varchar;
	pos_final1	integer;
	pos_inicial1	integer;
	largo1	integer;
	json3	json;
	xml_doc1 varchar;
	uri_xsl1 varchar;
begin
   xml2:=xml1;
   --http://acepta1505.acepta.com/v01/B6BA6ECE03F9294719A436B414F50A7E21E45105?k=ad1de08989e69efe15cdd512eb67dd3e
   xml2:=get_parametros_get(xml2);
   if (strpos(get_campo('REQUEST_URI',xml2),'traza')>0) then
	xml2:=put_campo(xml2,'INPUT',split_part(get_campo('REQUEST_URI',xml2),'&',1));
        xml2:=put_campo(xml2,'__SECUENCIAOK__','6');
        return xml2;
   end if;
   uri1:=decodifica_url(split_part(get_campo('REQUEST_URI',xml2),'url=',2));
   xml2:=put_campo(xml2,'URI_IN',uri1);


   /*
   xml2:=put_campo(xml2,'__SECUENCIAOK__','5');
   --Busco si esta en respaldo_dte primero
   select * into stResp from respaldo_dte where uri=uri1;
   if found then
	data1:=get_campo('INPUT',stResp.data);
	largo1:=get_campo('CONTENT_LENGTH',stResp.data)::integer*2;
        --Busco donde empieza <?xml version
        pos_inicial1:=strpos(data1,'3c3f786d6c2076657273696f6e3d');
        --Buscamos al reves donde esta el primer signo > que en hex es 3e
        --Como se pone un reverse se busca e3
        pos_final1:=largo1-pos_inicial1+4-strpos(reverse(data1),'e3');
        data1:=substring(data1,pos_inicial1,pos_final1);

    	xml2:=put_campo(xml2,'FLAG_GET_XML_RESPALDO','SI');
    	
	xml2:=put_campo(xml2,'XML_ALMACEN',data1);
    	xml2:=put_campo(xml2,'__SECUENCIAOK__','10');
	xml2:=put_campo(xml2,'FALLA_CUSTODIUM','NO');
	xml2:=logapp(xml2,'Obtiene XML desde respaldo_dte');

	return xml2;
   end if;
   */
   json3:=put_json('{}','uri',uri1);
   data1:=get_input_almacen(json3::varchar);
   if (length(data1)=0) then
	    xml2:=logapp(xml2,'Falla Lectura Almacen');
	   return xml2;
   end if;
   xml2:=put_campo(xml2,'DTE_XML_ALMACEN',data1);

   xml_doc1 := decode(data1, 'hex');
   --Obtengo la URI del XSL (si es que sirve)
   uri_xsl1:=split_part(split_part(xml_doc1,'href="',2),'"',1);
   xml2:=put_campo(xml2,'URI_XSL',uri_xsl1);
/*
   json3:=put_json('{}','uri',uri_xsl1);
    xml2:=logapp(xml2,'URI XSL='||uri_xsl1);
   data1:=get_input_almacen(json3::varchar);
   if (length(data1)=0) then
	    xml2:=logapp(xml2,'Falla Lectura Almacen XSL');
	   return xml2;
   end if;

   xml2:=put_campo(xml2,'XML_ALMACEN',data1);
*/

   xml2:=put_campo(xml2,'__SECUENCIAOK__','20');
   xml2:=put_campo(xml2,'FALLA_CUSTODIUM','NO');
   xml2:=put_campo(xml2,'FLAG_GET_XML_RESPALDO','SI');
   --xml2:=put_campo(xml2,'FLAG_GET_XML_RESPALDO','NO');
   --xml2:=logapp(xml2,'Vamos a buscar el DTE al Almacen='||uri1);
   return xml2;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION proc_procesa_get_xml_12789(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
        xml2    varchar;
	rut1	varchar;
	xml_doc1	varchar;
	salida varchar;
	json2	varchar;
	aux1	varchar;
	pos1	integer;
	xmlalmacen1	varchar;
	html_doc1	varchar;
	stData		cache_xsl%ROWTYPE;
	dominio1	varchar;
	tipo_dte1	varchar;
	uri_in1		varchar;
	uri_xsl1	varchar;
BEGIN
    xml2:=xml1;
    xml2:=put_campo(xml2,'__SECUENCIAOK__','0');
    json2:=decode(get_campo('JSON_IN',xml2),'hex');    

    uri_in1:=get_campo('URI_IN',xml2);
    
    --Verifico si viene correctamete el DTE
    if (get_campo('FALLA_CUSTODIUM',xml2)='SI') then
	xml2:=logapp(xml2,'DTE no leido desde almacen URI='||uri_in1);
	html_doc1:=encode('<h2>DTE no encontrado</h2>'::bytea,'hex');
        xml2:=put_campo(xml2,'RESPUESTA_HEX',encode(('Status: 404 NK'||chr(10)|| 'Content-type: text/html; charset=iso-8859-1'||chr(10)|| 'Content-length: '||(length(html_doc1)/2)::varchar||chr(10)||chr(10))::bytea,'hex')::varchar||html_doc1);
	INSERT INTO status_visualizador VALUES(now(),uri_in1,null,split_part(split_part(uri_in1,'//',2),'.',1), null, 'FALLA_GET_XML_ALMACEN',null,get_campo('FLAG_GET_XML_RESPALDO',xml2));
	return xml2;
    end if;
    
    xmlalmacen1:=get_campo('XML_ALMACEN',xml2);
    xml2:=put_campo(xml2,'DTE_XML_ALMACEN',xmlalmacen1);
    xml_doc1 := decode(xmlalmacen1, 'hex');

    xml2:=put_campo(xml2,'XML_ALMACEN','');

    --Obtengo la URI del XSL (si es que sirve)
    uri_xsl1:=split_part(split_part(xml_doc1,'href="',2),'"',1);	
    xml2:=put_campo(xml2,'XSL_URI_DEFECTO',uri_xsl1);
    tipo_dte1:=get_xml('TipoDTE',xml_doc1);
    xml2:=put_campo(xml2,'TIPO_DTE',tipo_dte1);
    dominio1:=split_part(split_part(uri_in1,'//',2),'.',1);
    xml2:=put_campo(xml2,'DOMINIO',dominio1);

    --Si existe en el cache de XSL, entonces lo leemos de aca
    select * into stData from cache_xsl where dominio=dominio1 and tipo_dte=tipo_dte1;
    if found then
        html_doc1 := xml_2_html_hex(xmlalmacen1,stData.xsl);
	--if(uri_xsl1='http://acepta1505.acepta.com/styles/dtes/dtes.xsl') then
		--xml2:=logapp(xml2,'URI XSL ACEPTA');
		html_doc1:=replace(html_doc1,encode('espaciochico'::bytea,'hex'),'');
		html_doc1:=replace(html_doc1,encode('espacio'::bytea,'hex'),'');
	--end if;
	xml2:=logapp(xml2,'Obtiene XSL desde cache_xsl para '||dominio1||' dte='||tipo_dte1);
	xml2:=put_campo(xml2,'RESPUESTA_HEX',encode(('Status: 200 OK'||chr(10)|| 'Content-Disposition: inline'||chr(10)||'Content-type: text/html; charset=UTF-8'||chr(10)|| 'Content-length: '||(length(html_doc1)/2)::varchar||chr(10)||chr(10))::bytea,'hex')::varchar||html_doc1);
	INSERT INTO status_visualizador VALUES(now(),uri_in1,uri_xsl1,dominio1, tipo_dte1, 'OK','SI',get_campo('FLAG_GET_XML_RESPALDO',xml2));
	return xml2;
    end if;

   
    --Busco la informacion del xsl para este DTE
    xml2:=put_campo(xml2,'TAG_HOST_12705',split_part(split_part(uri_in1,'http://',2),'/',1));
    xml2:=put_campo(xml2,'TAG_URL_12705','/ca4webv3/InfoView?url='||uri_in1);
    xml2:=put_campo(xml2,'FALLA_CUSTODIUM','');
    xml2:=put_campo(xml2,'__SECUENCIAOK__','12');
    xml2:=logapp(xml2,'Buscamos Info del XSL en http://'||get_campo('TAG_HOST_12705',xml2)||'/ca4webv3/InfoView?url='||uri_in1);
   return xml2;

END;
$$ LANGUAGE plpgsql;


--Revisa si viene el Info del ca4webv3
CREATE or replace FUNCTION proc_procesa_info_xsl_12789(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
	xml2	varchar;
	uri_xsl1	varchar;
	html_doc1	varchar;
	aux1	varchar;
	pos1	integer;
	data1	varchar;

begin
	xml2:=xml1;
    --Verifico si viene correctamete el Info
    if (get_campo('FALLA_CUSTODIUM',xml2)='SI') then
	xml2:=logapp(xml2,'Falla en obtencion de XSL '||get_campo('TAG_HOST_12705',xml2)||get_campo('TAG_URL_12705',xml2));
	html_doc1:=encode('<h2>Falla en obtencion de XSL</h2>'::bytea,'hex');
        xml2:=put_campo(xml2,'RESPUESTA_HEX',encode(('Status: 404 NK'||chr(10)|| 'Content-type: text/html; charset=iso-8859-1'||chr(10)|| 'Content-length: '||(length(html_doc1)/2)::varchar||chr(10)||chr(10))::bytea,'hex')::varchar||html_doc1);
	INSERT INTO status_visualizador VALUES(now(),get_campo('URI_IN',xml2),null,get_campo('DOMINIO',xml2), get_campo('TIPO_DTE',xml2), 'FALLA_GET_INFO_VIEW','NO',get_campo('FLAG_GET_XML_RESPALDO',xml2));
	return xml2;
    end if;

    --Buscamos el XSL del DTE en <View> <Default>true</Default> <ViewType>HtmlView</ViewType> <StyleUri>http://acepta1505.acepta.com/styles/dtes/dtes.xsl</StyleUri> </View>
    data1:=decode(get_campo('XML_ALMACEN',xml2),'hex');
    pos1:=strpos(data1,'<ViewType>HtmlView</ViewType>');
    if (pos1>0) then
		uri_xsl1:=get_xml('StyleUri',substring(data1,pos1,length(data1)));
		xml2:=logapp(xml2,'Usamos XSL desde Info '||uri_xsl1);
		xml2:=put_campo(xml2,'FLAG_CACHE','SI');
    else
		--Usamos el xsl por defecto
		uri_xsl1:=get_campo('XSL_URI_DEFECTO',xml2);
		xml2:=logapp(xml2,'Usamos XSL desde DTE, no estaba en Info '||uri_xsl1);
		xml2:=logapp(xml2,'Respuesta Info='||get_campo('XML_ALMACEN',xml2));
    end if;

    --Vamos a buscar el XSL
    --Setemos los parametros para el flujo 12705 y que traiga el XSL en el XML_ALMACEN
    xml2:=put_campo(xml2,'URL_XSL',uri_xsl1);
    xml2:=put_campo(xml2,'__SECUENCIAOK__','15');
    xml2:=put_campo(xml2,'TAG_HOST_12705',split_part(split_part(uri_xsl1,'http://',2),'/',1));
    aux1:=split_part(uri_xsl1,'http://',2);
    pos1:=strpos(aux1,'/');
    xml2:=put_campo(xml2,'TAG_URL_12705',substring(aux1,pos1,length(aux1)));
    xml2:=put_campo(xml2,'FALLA_CUSTODIUM','');
    xml2:=put_campo(xml2,'XML_ALMACEN','');
    xml2:=logapp(xml2,'Buscamos XSL en Almacen '||uri_xsl1);
   return xml2;
END;
$$ LANGUAGE plpgsql;



CREATE or replace FUNCTION proc_procesa_get_html_12789(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
        xml2    varchar;
	rut1	varchar;
	xml_doc1	varchar;
	salida varchar;
	json2	varchar;
	aux1	varchar;
	xsl_doc1	varchar;
	html_doc1	varchar;
	uri_xsl1	varchar;
BEGIN
    xml2:=xml1;
    xml2:=put_campo(xml2,'__SECUENCIAOK__','0');
    json2:=decode(get_campo('JSON_IN',xml2),'hex');
    xml2:=logapp(xml2,'proc_procesa_get_html_12789');
    uri_xsl1:=get_campo('URI_XSL',xml2);
    xml2:=logapp(xml2,'URI XSL=' || uri_xsl1);
    
    --Verifico si viene correctamete el DTE
    if (get_campo('FALLA_CUSTODIUM',xml2)='SI') then
	xml2:=logapp(xml2,'No encuentra el XSL en el almacen XSL='||get_campo('URL_XSL',xml2));
	html_doc1:=encode('Falla lectura de XSL'::bytea,'hex');
        xml2:=put_campo(xml2,'RESPUESTA_HEX',encode(('Status: 404 NK'||chr(10)|| 'Content-type: text/html; charset=iso-8859-1'||chr(10)|| 'Content-length: '||(length(html_doc1)/2)::varchar||chr(10)||chr(10))::bytea,'hex')::varchar||html_doc1);
	INSERT INTO status_visualizador VALUES(now(),get_campo('URI_IN',xml2),uri_xsl1,get_campo('DOMINIO',xml2), get_campo('TIPO_DTE',xml2), 'FALLA_GET_XSL_ALMACEN','NO',get_campo('FLAG_GET_XML_RESPALDO',xml2));
	return xml2;
    end if;

    
    xml_doc1 := get_campo('DTE_XML_ALMACEN',xml2);
    --xsl_doc1 := get_campo('XML_ALMACEN',xml2);
		
    if (get_campo('FLAG_CACHE',xml2)='SI') then
	    xml2:=logapp(xml2,'Grabamos cache_xsl para dominio='||get_campo('DOMINIO',xml2)||' Tipo_dte='||get_campo('TIPO_DTE',xml2));
	    --Inserto el cache
	    insert into cache_xsl (fecha,url,xsl,dominio,tipo_dte) values (now(),uri_xsl1,xsl_doc1,get_campo('DOMINIO',xml2),get_campo('TIPO_DTE',xml2));
    end if;

    html_doc1 := xml_2_html2_hex(xml_doc1,uri_xsl1);
    --html_doc1 := xml_2_html_hex(xml_doc1,xsl_doc1);
    --if(uri_xsl1='http://acepta1505.acepta.com/styles/dtes/dtes.xsl') then
	--html_doc1:=replace(html_doc1,encode('espaciochico'::bytea,'hex'),'');
	--html_doc1:=replace(html_doc1,encode('espacio'::bytea,'hex'),'');
    --end if;
    xml2:=put_campo(xml2,'RESPUESTA_HEX',encode(('Status: 200 OK'||chr(10)|| 'Content-type: text/html; charset=UTF-8;'||chr(10)|| 'Content-length: '||(length(html_doc1)/2)::varchar||chr(10)||chr(10))::bytea,'hex')::varchar||html_doc1);

	INSERT INTO status_visualizador VALUES(now(),get_campo('URI_IN',xml2),uri_xsl1,get_campo('DOMINIO',xml2), get_campo('TIPO_DTE',xml2), 'OK','NO',get_campo('FLAG_GET_XML_RESPALDO',xml2));
   return xml2;
END;
$$ LANGUAGE plpgsql;
