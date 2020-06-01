delete from isys_querys_tx where llave='12735';

insert into isys_querys_tx values ('12735',10,19,1,'select envia_mail_12735(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,1000);

insert into isys_querys_tx values ('12735',1000,1,1,'select sp_procesa_respuesta_cola_motor_original(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,0,0);



CREATE or replace FUNCTION pivote_borrado_12735(varchar) RETURNS varchar AS $$
DECLARE
        xml2        alias for $1;
        xml3    varchar;
BEGIN
        xml3:=xml2;
        xml3:=logapp(xml3,'BD_ORIGEN='||get_campo('_CATEGORIA_BD_',xml2));
        if(get_campo('_CATEGORIA_BD_',xml2)='COLAS')then
                xml3 := put_campo(xml3,'__SECUENCIAOK__','0');
		return sp_procesa_respuesta_cola_motor_original(xml3);
        else
                xml3 := put_campo(xml3,'__SECUENCIAOK__','1000');
        end if;
        return xml3;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION envia_mail_12735(varchar) RETURNS varchar AS $$
DECLARE
	xml1		alias for $1;
	xml2		varchar;
	json3		json;
	jsend1		json;
BEGIN
	xml2:=xml1;
	
	BEGIN
		json3:=decode_hex(get_campo('INPUT',xml2))::json;
	EXCEPTION WHEN OTHERS THEN
		xml2 := put_campo(xml2,'RESPUESTA','Status: 444 NK');
		xml2:=logapp(xml2,'No se logra castear a json el INPUT ');
		return pivote_borrado_12735(xml2);
	END;
	
	jsend1:=send_mail_python2_colas(json3::varchar);
        if (get_json('status',jsend1)='OK') then
		xml2:=logapp(xml2,'Envio Correo Exitoso');
		xml2 := put_campo(xml2,'RESPUESTA','Status: 200 OK');
        else
		xml2:=logapp(xml2,'Falla Envio jsend1='||jsend1::varchar);
		xml2 := put_campo(xml2,'RESPUESTA','Status: 400 NK');
        end if;

	return pivote_borrado_12735(xml2);
END;
$$ LANGUAGE plpgsql;

