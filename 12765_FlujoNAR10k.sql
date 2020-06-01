delete from isys_querys_tx where llave='12765';

insert into isys_querys_tx values ('12765',10,1,1,'select get_xml_NAR_5000(''$$__XMLCOMPLETO__$$'',decode(''$$JSON_IN$$'',''hex'')::varchar) as __xml__',0,0,0,1,1,-1,0);

--insert into isys_querys_tx values ('12765',20,1,10,'$$SCRIPT$$',0,0,0,1,1,30,30);
insert into isys_querys_tx values ('12765',20,1,2,'Servicio de Firma 192.168.3.17',4013,109,106,0,0,30,30);

insert into isys_querys_tx values ('12765',30,1,1,'select get_xml_NAR_resp_5000(''$$__XMLCOMPLETO__$$'',decode(''$$JSON_IN$$'',''hex'')::varchar) as __xml__',0,0,0,1,1,-1,0);

--Se envia al EDTE
insert into isys_querys_tx values ('12765',50,1,8,'Llamada NAR EDTE',12779,0,0,0,0,65,65);
--Publicamos el NAR
insert into isys_querys_tx values ('12765',65,1,8,'Publica DTE',12704,0,0,0,0,70,70);

--Validamos la publicacion
insert into isys_querys_tx values ('12765',70,1,1,'select valida_publicacion_nar_12765(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

CREATE or replace FUNCTION valida_publicacion_nar_12765(varchar) RETURNS varchar AS $$
declare
        xml1    alias for $1;
        xml2    varchar;
        json2   varchar;
        rut1    varchar;
        stContribuyente contribuyentes%ROWTYPE;
        mail1   varchar;
begin
        xml2:=xml1;
        xml2:=put_campo(xml2,'__SECUENCIAOK__','0');
        --json2:=decode(get_campo('JSON_IN',xml2),'hex')::varchar;
	json2:='{}';
        if (get_campo('__PUBLICADO_OK__',xml2)<>'SI') then
                  xml2:=response_requests_5000('2', 'Falla Publicacion de NAR', '',xml2,json2);
                  return xml2;
        end if;

        --Si lo escribi bien en el EDTE
        if (get_campo('__EDTE_NAR_OK__',xml2)<>'SI') then
                  xml2:=response_requests_5000('2', 'Falla Envio de NAR', '',xml2,json2);
                  return xml2;
        end if;

	/*
        --2015-04-30 FAY,RME Se graba inmediatamente el Evento NAR para el DTE recibido
        rut1:=get_campo('RUT_EMISOR',xml2);
        select * into stContribuyente from contribuyentes where rut_emisor=rut1::integer;
        if not found then
                xml2:=logapp(xml2,'NAR: Rut Emisor del DTE Recibido no registrado en contribuyentes');
                mail1:='Sin mail de intercambio';
        else
                mail1:=stContribuyente.email;
        end if;
        xml2:=put_campo(xml2,'COMENTARIO_TRAZA','Recibe: '||mail1||chr(10)||get_campo('RECINTO',xml2)||
'.');
        --Se asignan las uris para grabar el evento en traza
        xml2:=put_campo(xml2,'URL_GET',get_campo('URI_IN',xml2));
        xml2:=put_campo(xml2,'URI_IN',get_campo('URI_DTE',xml2));
        xml2:=graba_bitacora2(xml2,'NAR');
        --Vuelo a dejar en uri_in la uri del NAR
        xml2:=put_campo(xml2,'URI_IN',get_campo('URL_GET',xml2));
	*/

        xml2:=response_requests_5000('1', 'NAR firmado',get_campo('URI_IN',xml2),xml2,json2);
        return xml2;
END;
$$ LANGUAGE plpgsql;

