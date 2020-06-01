delete from isys_querys_tx where llave='8040';

--Proceso el DTE REcibido
insert into isys_querys_tx values ('8040',50,1,1,'select envia_sii_rec_8040(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('8040',60,1,1,'select genera_crt_8030(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('8040',1000,19,1,'select sp_procesa_respuesta_cola_motor88_json(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,0,0);


CREATE or replace FUNCTION envia_sii_rec_8040(varchar) RETURNS varchar AS $$
DECLARE
        xml1        alias for $1;
        xml2    varchar;
	json_par1	json;
	json_aux	json;
	json_in		json;
	json_script1	json;
	resp_est	varchar;
	resp_cod	varchar;	
	glosa_es	varchar;
	glosa_er	varchar;
	output1		varchar;
	fecha1		varchar;
BEGIN
    xml2:=xml1;
	xml2 := put_campo(xml2,'__SECUENCIAOK__','1000');

	json_par1:=get_parametros_motor_json('{}','BASE_COLAS');
	
	fecha1:=to_char(get_campo('FECHA_EMISION',xml2)::timestamp,'DD-MM-YYYY');

	json_in:='{"RutCompania":"'||get_campo('RUT_EMISOR',xml2)||'","DvCompania":"'||modulo11(get_campo('RUT_EMISOR',xml2))||'","RutReceptor":"'||get_campo('RUT_RECEPTOR',xml2)||'","DvReceptor":"'||modulo11(get_campo('RUT_RECEPTOR',xml2))||'","TipoDte":"'||get_campo('TIPO_DTE',xml2)||'","FolioDte":"'||get_campo('FOLIO',xml2)||'","FechaEmisionDte":"'||fecha1||'","MontoDte":"'||get_campo('MONTO_TOTAL',xml2)||'","RUT_OWNER":"'||get_campo('RUT_RECEPTOR',xml2)||'"}';

	xml2:=logapp(xml2,'SII json='||json_in::varchar);
	json_aux:=query_db_json(get_json('__IP_CONEXION_CLIENTE__',json_par1),get_json_int('__IP_PORT_CLIENTE__',json_par1)::integer,'select curl_python(''escritorio.acepta.com:2020/estado_dte'','''||json_in::varchar||''')');
        if(get_json('STATUS',json_aux)<>'OK') then
		xml2 := logapp(xml2,'Falla Consulta SII');
		xml2 := put_campo(xml2,'RESPUESTA','Status: 400 NK');
		return xml2;
	end if;
        json_script1:=get_json('curl_python',json_aux);
        output1:=get_json('output',json_script1);
	resp_est:=get_xml_hex1('ESTADO',output1);
	resp_cod:=get_xml_hex1('ERR_CODE',output1);
	glosa_es:=get_xml_hex1('GLOSA_ESTADO',output1);
	glosa_er:=get_xml_hex1('GLOSA_ERR',output1);
	xml2:=logapp(xml2,'SII output1='||decode(output1,'hex')::varchar);
        if(resp_cod<>'') then
		if(resp_est='DOK') then
                        xml2:=put_campo(xml2,'COMENTARIO_TRAZA','Glosa: '||glosa_es||' ('||resp_est||')');
                        xml2:=put_campo(xml2,'EVENTO','ASI');
                        xml2:=actualiza_estado_dte(xml2);
                        xml2:=graba_bitacora(xml2,'ASI');
			xml2 := put_campo(xml2,'RESPUESTA','Status: 200 OK');
			return xml2;
		elsif(resp_est='DNK') then
                        xml2:=put_campo(xml2,'COMENTARIO_TRAZA','Glosa: '||glosa_es||' ('||resp_est||')');
                        xml2:=put_campo(xml2,'EVENTO','RSI');
                        xml2:=actualiza_estado_dte(xml2);
                        xml2:=graba_bitacora(xml2,'RSI');
			xml2 := put_campo(xml2,'RESPUESTA','Status: 200 OK');
			return xml2;
		else
			xml2 := logapp(xml2,'Falla Consulta SII');
			xml2 := put_campo(xml2,'RESPUESTA','Status: 400 NK');
			return xml2;
		end if;
	else
		xml2 := logapp(xml2,'Falla Consulta SII');
		xml2 := put_campo(xml2,'RESPUESTA','Status: 400 NK');
		return xml2;
	end if;
	return xml2;
END;
$$ LANGUAGE plpgsql;

