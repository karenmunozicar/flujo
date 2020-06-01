insert into isys_querys_tx values ('12731',10,1,1,'select proc_procesa_12731(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,90,90);
--Generico Llamada de SCRIPT
insert into isys_querys_tx values ('12731',90,1,10,'$$SCRIPT$$',0,0,0,1,1,100,100);
--insert into isys_querys_tx values ('12730',90,1,1,'select test(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,100,100);
--Respuesta de correo.
insert into isys_querys_tx values ('12731',100,1,1,'select resp_procesa_12731(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

CREATE or replace FUNCTION proc_procesa_12731(varchar) RETURNS varchar AS $$
DECLARE
        xml1    alias for $1;
        xml2    varchar;
        --Correo
        --id_message varchar;
        v_subject varchar;
        --Data Envia Correo
        v_data  varchar;

        --CUERPO
        v_cuerpo varchar;
        v_razsoc varchar;
	v_desc_dte varchar;
	v_monto bigint;
	v_desc_evento	varchar;
	v_uri		varchar;
        ----

BEGIN

        xml2:='';
        xml2:=xml1;
        --v_cuerpo := get_campo ('CUERPO',xml2);
        v_subject := 'Notificacion de Documentos Electronicos Tipo Documento:' || get_campo('TIPO_DTE',xml2) || ' Emisor: ' || get_campo('RUT_EMISOR',xml2) || ' Receptor: '|| get_campo('RUT_RECEPTOR',xml2);
        v_subject := codifica_url(v_subject);
	v_uri := get_campo('URI_IN',xml2);
	select descripcion into v_desc_dte from tipo_dte where codigo = get_campo('TIPO_DTE',xml2)::integer;
        select razon_social into v_razsoc from maestro_clientes where rut_emisor = get_campo('RUT_EMISOR',xml2)::integer;
	select monto_total into v_monto from dte_emitidos where uri = v_uri; 
	select descripcion1 into v_desc_evento from traza.config where evento=get_campo('EVENTO',xml2);
        --v_cuerpo := 'Estimados:' || chr(10) || 'Se informa documento "' || v_cuerpo || '" Evento:' || get_campo('EVENTO',xml2);
	v_cuerpo := 'SE&#209;OR(ES):'|| chr(10) ||'Se adjunta '|| v_desc_dte || '.' || chr(10)|| 'Emitida por: ' || v_razsoc;
	v_cuerpo:= v_cuerpo || chr(10) || 'Folio : ' || get_campo('FOLIO',xml2) || chr(10) || 'Monto : ' || v_monto || chr(10) || 'URI:' || get_campo('URI_IN',xml2);
	v_cuerpo := v_cuerpo || chr(10) || 'Glosa : ' || v_desc_evento; 
	--v_cuerpo := || chr(10)|| '';
        v_cuerpo:= codifica_url(v_cuerpo);
        --insert into jsprueba select 'cuerpo:' || v_cuerpo;
        v_data:='mailfrom='||'noreply@acepta.com'||'&mailto='||replace(get_campo('MAIL_TO',xml2),';',',')|| '&subject='||v_subject||'&pdf=&xml=&nombre_adjunto=&cuerpo='||v_cuerpo;
        --POST
        xml2:=put_campo(xml2,'SCRIPT','/opt/acepta/motor/Procesos/envio_mail_reglas_distribucion.sh "' || v_data || '"');
        xml2:=logapp(xml2,'SCRIPT MAILJSE'||get_campo('SCRIPT',xml2));
        return xml2;
END;
$$ LANGUAGE plpgsql;
CREATE or replace FUNCTION resp_procesa_12731(varchar) RETURNS varchar AS $$
DECLARE
        xml1    alias for $1;
        xml2    varchar;
	v_destinatarios	varchar;
BEGIN
        xml2:='';
        xml2:=xml1;
	v_destinatarios := replace(get_campo('MAIL_TO',xml2),';',chr(10));
        xml2 := put_campo(xml2,'RESPUESTA','Status: 200 OK');
	xml2 := put_campo(xml2,'COMENTARIO_TRAZA',v_destinatarios);
        xml2 := sp_procesa_respuesta_cola_motor(xml2);
        xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
        xml2 := graba_bitacora(xml2,'SEND_MAIL_NOTIFICACION');


        return xml2;
END;
$$ LANGUAGE plpgsql;

