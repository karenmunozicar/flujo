--Publica documento
delete from isys_querys_tx where llave='12763';
--Obtiene el DTE Original con la entrada URI_IN
insert into isys_querys_tx values ('12763',5,1,8,'GET XML desde Almacen',12705,0,0,1,1,10,10);

insert into isys_querys_tx values ('12763',10,1,1,'select proc_procesa_get_xml_mandato_12763(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

--Envia Mandato a EDTE
insert into isys_querys_tx values ('12763',20,1,8,'Llamada Renvio Mandato EDTE',12783,0,0,0,0,30,30);

--Respondemos la respuesta de la Aplicacion
insert into isys_querys_tx values ('12763',30,1,1,'select proc_procesa_resp_mandato_edte_12763(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

CREATE or replace FUNCTION proc_procesa_resp_mandato_edte_12763(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2	varchar;
	json2	varchar;
begin
	xml2:=xml1;
	--json2:=hex_2_ascii(get_campo('JSON_IN',xml2));
	json2:='{}';
	if (get_campo('__EDTE_REENVIO_MANDATO_OK__',xml2)<>'SI') then
		xml2:=response_requests_5000('2', 'Falla Envio de Mandato','',xml2,json2);
		xml2:=logapp(xml2,'REENVIO MANDATO: Falla Envio '||get_campo('URI_IN',xml2));
	else
		xml2:=response_requests_5000('2', 'Mandato Enviado OK','Mandato Enviado OK',xml2,json2);
		xml2:=logapp(xml2,'REENVIO MANDATO: Mandato Enviado OK'||get_campo('URI_IN',xml2));
		--Graba en la bitacora el renvio de mandato sin proxy
		xml2:=put_campo(xml2,'COMENTARIO_TRAZA','Re-envio de mandato a '||get_campo('MAIL_REENVIO_MANDATO',xml2));
		xml2:=graba_bitacora(xml2,'EMA2');
	end if;
	return xml2;
end;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION proc_procesa_get_xml_mandato_12763(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
        xml2    varchar;
	rut1	varchar;
	data1	varchar;
	salida varchar;
	datos_mandato varchar;
	mail_envio_mdto varchar;
	pos1	integer;
	pos2	integer;
	json2	varchar;
	data2	varchar;
BEGIN
    xml2:=xml1;
    xml2:=logapp(xml2,'REENVIO MANDATO: COMIENZA FLUJO 12763');
    json2:='{}';
    if (get_campo('__FLUJO_ENTRADA__',xml2)='6000') then
    	mail_envio_mdto:=get_campo('MAILMANDATO',xml2);
    else
    	mail_envio_mdto:=json_get('mailMandato',hex_2_ascii(get_campo('JSON_IN',xml2)));
    end if;
    --Verifico si viene correctamete el DTE
    if (get_campo('FALLA_CUSTODIUM',xml2)='SI') then
	xml2:=logapp(xml2,'REENVIO MANDATO: DTE no leido desde almacen URI='||get_campo('URI_IN',xml2));
	xml2:=response_requests_5000('200', 'Falla lectura de DTE desde Almacen','', xml2,json2);
		
	return xml2;
    end if;
    /*rut1:=replace(get_campo('RUT',xml2),'-','');
    if (is_number(rut1) is false) then
	xml2:=logapp(xml2,'REENVIO MANDATO: RUT no numerico URI='||get_campo('URI_IN',xml2)||' RUT='||rut1);
	return xml2;
    end if;*/
	
   --Rescato XML y otros datos
   data1 :=get_campo('XML_ALMACEN',xml2);

   --Sacamos los datos del XML
	

/*
   --Si no viene nada...
   if(strpos(data1,encode('<Attribute Type="EMAIL">','hex'))=0) then
	--inserto todo
	datos_mandato:=encode(('<DatosAdjuntos><NombreDA>Mail_Receptor</NombreDA><ValorDA>'||mail_envio_mdto||'</ValorDA></DatosAdjuntos><DatosAdjuntos><NombreDA>Mail_Emisor</NombreDA><ValorDA>'||case when length(get_xml('CorreoEmisor',data1))=0 then 'acepta@acepta.com' else get_xml('CorreoEmisor',data1) end||'</ValorDA></DatosAdjuntos><DatosAdjuntos><NombreDA>Subject_Mail</NombreDA><ValorDA>Dte de '||get_xml('RznSoc',data1)||': N° '||get_xml('Folio',data1)||' de '||get_xml('FchEmis',data1)||'</ValorDA></DatosAdjuntos>')::bytea,'hex');
	data1 := split_part(data1,encode('</DTE>','hex'), 1) ||encode('</DTE>','hex') || datos_mandato || encode('</Content>','hex') ||  split_part(data1,encode('</Content>','hex'), 2);
	xml2:=put_campo(xml2,'INPUT_CUSTODIUM',data1);
	xml2:=put_campo(xml2,'LEN_INPUT_CUSTODIUM',length(data1)::varchar);
	xml2:=put_campo(xml2,'__DTE_CON_MANDATO__','SI');
	xml2:=put_campo(xml2,'__SECUENCIAOK__','20');
	return xml2;
   end if;

*/

    --Borramos todos los EMAIL que existan en el DTE
    data1:=regexp_replace(data1,encode('<Attribute Type="EMAIL">'::bytea,'hex')::varchar||'.*'||encode('</Attribute>'::bytea,'hex')::varchar,'');
   --Chequeo si vienen Datos Adjuntos--
   --Si vienen--
	/*
   if(strpos(data1,encode('<Attribute Type="EMAIL">','hex'))>0) then
	--Borramos todos los EMAIL que existan en el DTE
	data1:=regexp_replace(data1,encode('<Attribute Type="EMAIL">'::bytea,'hex')::varchar||'.*'||encode('</Attribute>'::bytea,'hex')::varchar,'');
	
	data1:=split_part(data1,encode('</Attributes>','hex'),1)||encode('<Attribute Type="EMAIL">'::bytea,'hex')::varchar || encode(mail_envio_mdto::bytea,'hex')::varchar||encode('</Attribute>'::bytea,'hex')::varchar||encode((chr(10)||'</Attributes>')::bytea,'hex')||split_part(data1,encode('</Attributes>','hex'),2);

	--Sacamos la posicion 
	--pos1:=strpos(data1,encode('<Attribute Type="EMAIL">'::bytea,'hex'));
	--Calculamos el largo
	--pos1:=pos1+length('<Attribute Type="EMAIL">')*2+strpos(split_part(data1,encode('<Attribute Type="EMAIL">','hex'),2),encode('</Attribute>'::bytea,'hex'))+length('</Attribute>')*2;
	
	--data1 := split_part(data1, encode('<Attribute Type="EMAIL">'::bytea,'hex')::varchar, 1) || encode('<Attribute Type="EMAIL">'::bytea,'hex')::varchar || encode(mail_envio_mdto::bytea,'hex')::varchar || encode('</Attribute>'::bytea,'hex')::varchar ||  substring(data1,pos1-1,length(data1));	
	xml2:=put_campo(xml2,'INPUT_CUSTODIUM',data1);
	xml2:=put_campo(xml2,'LEN_INPUT_CUSTODIUM',length(data1)::varchar);
	xml2:=put_campo(xml2,'__DTE_CON_MANDATO__','SI');
	xml2:=put_campo(xml2,'__SECUENCIAOK__','20');
   else
	*/
	data1:=split_part(data1,encode('</Attributes>','hex'),1)||encode('<Attribute Type="EMAIL">'::bytea,'hex')::varchar || encode(mail_envio_mdto::bytea,'hex')::varchar||encode('</Attribute>'::bytea,'hex')::varchar||encode((chr(10)||'</Attributes>')::bytea,'hex')||split_part(data1,encode('</Attributes>','hex'),2);
	
	data2:=decode(data1,'hex');
	datos_mandato:=encode(('<DatosAdjuntos><NombreDA>Mail_Receptor</NombreDA><ValorDA>'||mail_envio_mdto||'</ValorDA></DatosAdjuntos><DatosAdjuntos><NombreDA>Mail_Emisor</NombreDA><ValorDA>'||case when length(get_xml('CorreoEmisor',data2))=0 then 'acepta@acepta.com' else get_xml('CorreoEmisor',data2) end||'</ValorDA></DatosAdjuntos><DatosAdjuntos><NombreDA>Subject_Mail</NombreDA><ValorDA>Dte de '||get_xml('RznSoc',data2)||': Folio '||get_xml('Folio',data2)||' de '||get_xml('FchEmis',data2)||'</ValorDA></DatosAdjuntos>')::bytea,'hex');

	data1 := split_part(data1,encode('</DTE>','hex'), 1) ||encode('</DTE>','hex') || datos_mandato || encode('</Content>','hex') ||  split_part(data1,encode('</Content>','hex'), 2);

	--Le agrego custodium-uri si no lo trae
	if (strpos(data1,encode('item name="custodium-uri">'::bytea,'hex')::varchar)=0) then
		data1:=replace(data1,encode('</Process>'::bytea,'hex')::varchar,encode(('<item name="custodium-uri">'||get_campo('URI_IN',xml2)||'</item>'||chr(10)||'</Process>')::bytea,'hex'));
	end if;

        xml2:=put_campo(xml2,'INPUT_CUSTODIUM',data1);
        xml2:=put_campo(xml2,'LEN_INPUT_CUSTODIUM',length(data1)::varchar);
        xml2:=put_campo(xml2,'__DTE_CON_MANDATO__','SI');
        xml2:=put_campo(xml2,'__SECUENCIAOK__','20');

	xml2:=put_campo(xml2,'MAIL_REENVIO_MANDATO',mail_envio_mdto::varchar);
   --end if;
    
   return xml2;

/*
   if(strpos(data1,encode('<NombreDA>Mail_Receptor</NombreDA>','hex'))>0) then
	--Sacamos la posicion 
	pos1:=strpos(data1,encode('<NombreDA>Mail_Receptor</NombreDA>','hex'));
	pos2:=strpos(substring(data1,pos1,length(data1)),encode('</ValorDA>'::bytea,'hex'))-1;
	
	data1 := split_part(data1, encode('<NombreDA>Mail_Receptor</NombreDA>'::bytea,'hex'), 1) || encode('<NombreDA>Mail_Receptor</NombreDA>'::bytea,'hex')||encode((chr(10)||'<ValorDA>')::bytea,'hex')||encode(mail_envio_mdto::bytea,'hex') || encode('</ValorDA>'::bytea,'hex') ||  substring(data1,pos1+pos2+length('</ValorDA>')*2,length(data1)*2);	
	xml2:=put_campo(xml2,'INPUT_CUSTODIUM',data1);
	xml2:=put_campo(xml2,'LEN_INPUT_CUSTODIUM',length(data1)::varchar);
	xml2:=put_campo(xml2,'__DTE_CON_MANDATO__','SI');
	xml2:=put_campo(xml2,'__SECUENCIAOK__','20');
	xml2:=logapp(xml2,'DATA1='||data1);
	return xml2;
   end if;

   return xml2;

*/



/*
   else
   --Si no vienen--
   end if;

   return xml2;
*/
END;
$$ LANGUAGE plpgsql;

