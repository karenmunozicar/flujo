--Publica documento
delete from isys_querys_tx where llave='12764';
insert into isys_querys_tx values ('12764',5,19,1,'select control_flujo_80101(''$$__JSONCOMPLETO__["__PROC_ACTIVOS__","TX","REQUEST_URI","__ARGV__","__CATEGORIA_COLA__","__FLUJO_ACTUAL__"]$$''::json) as __json__',0,0,0,1,1,-1,10);
--Obtiene el DTE Original con la entrada URI_IN
insert into isys_querys_tx values ('12764',10,1,8,'GET XML desde Almacen',12705,0,0,1,1,15,15);

insert into isys_querys_tx values ('12764',15,19,1,'select proc_procesa_get_xml_mandato_12764(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

--Envia Mandato a EDTE
insert into isys_querys_tx values ('12764',20,1,8,'Llamada Renvio Intercambio EDTE',12784,0,0,0,0,30,30);
--Respondemos la respuesta de la Aplicacion
insert into isys_querys_tx values ('12764',30,19,1,'select proc_procesa_resp_mandato_edte_12764(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

CREATE or replace FUNCTION proc_procesa_resp_mandato_edte_12764(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2	varchar;
	json2	varchar;
	xml7	varchar;
	id1	bigint;
	xml9	varchar;
begin
	xml2:=xml1;
	json2:='{}';
	if (get_campo('__EDTE_REENVIO_INTER_OK__',xml2)<>'SI') then
		xml2:=response_requests_5000('200', 'Falla Envio de Intercambio','', xml2,json2);
		xml2:=logapp(xml2,'REENVIO INTER: Falla Envio '||get_campo('URI_IN',xml2));
	else
		--Se graba el evento de reenvio
		if (get_campo('FLAG_EVENTO_REE',xml2)<>'NO') then
			--FAY-DAO 20210117 Encolamos el graba_bitacora
        		xml7:=put_campo('','TX','8060');
			xml7:=put_campo(xml7,'CATEGORIA','MOTOR');
			xml7:=put_campo(xml7,'SUB_CATEGORIA','GRABA_BITACORA_12764');
			xml7:=put_campo(xml7,'URI_IN',get_campo('URI_IN',xml2));
			xml9:=put_campo(xml2,'XML_ALMACEN','');
			xml9:=put_campo(xml9,'INPUT_CUSTODIUM','');
			xml9:=put_campo(xml9,'RESPUESTA','');
			xml9:=put_campo(xml9,'SCRIPT_EDTE','');
			xml9:=put_campo(xml9,'ALMACEN','');
			xml9:=put_campo(xml9,'FILE','');
			xml7:=put_campo(xml7,'QUERY',encode_hex('select graba_bitacora('''||replace(xml9,chr(39),'')||''',''REE'')'));
        		execute 'insert into cola_motor_10 (fecha,uri,reintentos,data,tx,rut_emisor,reproceso,categoria) values (now(),'||quote_literal(get_campo('URI_IN',xml7))||',0,'||quote_literal(xml7)||','||'10'||',null,''NO'',''ACT_REMOTO'') returning id' into id1;
        		xml2:=logapp(xml2,'Inserto GRABA_BITACORA_12764 '||get_campo('URI_IN',xml7)||' id='||id1::varchar);
		else
			xml2:=logapp(xml2,'Sin GRABA_BITACORA_12764');
			--xml2:=graba_bitacora(xml2,'REE');
		end if;
		xml2:=response_requests_5000('1', 'Intercambio Enviado OK','Intercambio Enviado OK', xml2,json2);
		xml2:=logapp(xml2,'REENVIO INTER: Intercambio Enviado OK'||get_campo('URI_IN',xml2));
		--Graba en la bitacora el renvio de mandato sin proxy
		--xml2:=put_campo(xml2,'COMENTARIO_TRAZA','Re-envio de Intercambio a '||json_get('mailMandato',hex_2_ascii(get_campo('JSON_IN',xml2))));
		--xml2:=graba_bitacora(xml2,'EMA2');
	end if;
	return xml2;
end;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION proc_procesa_get_xml_mandato_12764(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
        xml2    varchar;
	rut1	varchar;
	data1	varchar;
	salida varchar;
	pos1	integer;
	pos2	integer;
	json2	varchar;
BEGIN
    xml2:=xml1;
    xml2:=logapp(xml2,'REENVIO INTER: COMIENZA FLUJO 12764');
    json2:='{}';
    --Verifico si viene correctamete el DTE
    if (get_campo('FALLA_CUSTODIUM',xml2)='SI') then
	xml2:=logapp(xml2,'REENVIO INTER: DTE no leido desde almacen URI='||get_campo('URI_IN',xml2));
	xml2:=response_requests_5000('200', 'Falla lectura de DTE desde Almacen','', xml2,json2);
	return xml2;
    end if;
	
   --Rescato XML y otros datos
   data1 :=get_campo('XML_ALMACEN',xml2);

   --Rescato RUT_RECEPTOR del DTE para verificar si 
   xml2:=put_campo(xml2,'RUT_EMISOR',split_part(get_xml('RUTEmisor',decode(data1,'hex')::varchar),'-',1));

	xml2:=put_campo(xml2,'INPUT_CUSTODIUM',data1);
	xml2:=put_campo(xml2,'LEN_INPUT_CUSTODIUM',length(data1)::varchar);
	xml2:=put_campo(xml2,'__DTE_CON_MANDATO__','SI');
	xml2:=put_campo(xml2,'__SECUENCIAOK__','20');
   return xml2;
END;
$$ LANGUAGE plpgsql;

