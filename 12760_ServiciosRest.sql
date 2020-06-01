delete from isys_querys_tx where llave='12760';
--Busca URI
insert into isys_querys_tx values ('12760',10,1,1,'select sp_busca_uri_12760(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,-1);
--Busca XML
insert into isys_querys_tx values ('12760',20,1,8,'GET XML desde Almacen',12705,0,0,1,1,30,30);
--Procesa dependiendo de la TX
insert into isys_querys_tx values ('12760',30,1,1,'select proc_servicios_rest_12760(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,99);
--RESPUESTA DEL SERVICIO.
insert into isys_querys_tx values ('12760',99,1,1,'select proc_respuesta_servicios_rest_12760(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
--RechazoDocumento
insert into isys_querys_tx values ('12760',50,1,2,'RECHAZO',12761,103,101,0,0,99,99);
--Servicio de Rechazo.
insert into isys_querys_tx values ('12760',90,1,2,'Rechazo ca4xml',4013,103,101,0,0,98,98);
--Script Curl Rechazo
insert into isys_querys_tx values ('12760',91,1,10,'$$SCRIPT$$',0,0,0,1,1,98,98);
--Revisa Respuesta.
insert into isys_querys_tx values ('12760',98,1,1,'select sp_out_rechazo_documento(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,99,99);
CREATE or replace FUNCTION proc_servicios_rest_12760(varchar) RETURNS varchar AS $$
DECLARE
        xml1    alias for $1;
        xml2    varchar;
        v_uri   varchar;
        tipo_tx varchar;
	v_dte   varchar;
        v_reintentos varchar;
BEGIN
        xml2:=xml1;
        xml2:=get_parametros_get(xml2);
        tipo_tx:=get_campo('tipo_tx',xml2);
	v_reintentos:=get_campo('REINTENTOS',xml2);

        if (v_reintentos='') then
                v_reintentos='0';
        end if;
	
        xml2:=logapp(xml2,'Reintentos: '|| v_reintentos);
        v_dte := split_part(get_campo('XML_ALMACEN',xml2),encode('<DTE ','hex'),2);
	-- Se valida si es un DTE valido, se reintenta hasta 3 veces       
	if (char_length(v_dte)=0) then
		if (v_reintentos::integer<3) then
       			 xml2:=put_campo(xml2,'__SECUENCIAOK__','20');
               		 v_reintentos:=(v_reintentos::integer+1)::varchar;
               		 xml2:=put_campo(xml2,'REINTENTOS',v_reintentos);
		end if;
       	end if;

        if (tipo_tx='AMP_Valida_Doc') then
                xml2:=logapp(xml2,'Tx AMP_Valida_Doc');
                xml2:=sp_amp_consulta_documento(xml2);
		xml2:=put_campo(xml2,'__SECUENCIAOK__','99');
        elsif (tipo_tx='AMP_Rechaza_Doc') then
                xml2:=logapp(xml2,'Tx AMP_Rechaza_Doc');
                xml2:=sp_in_rechazo_documento(xml2);

        else
		xml2:=logapp(xml2,'Tx Desconocido ['||tipo_tx||']');
                xml2:=put_campo(xml2,'ESTADO_SERVICIO','NOK');
                xml2:=put_campo(xml2,'COD_RESPUESTA','');
                xml2:=put_campo(xml2,'__SECUENCIAOK__','99');
        end if;

        return xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION proc_respuesta_servicios_rest_12760(varchar) RETURNS varchar AS $$
DECLARE
        xml1    alias for $1;
        xml2    varchar;
        tipo_tx varchar;
        estado_servicio varchar;
        v_respuesta varchar;
BEGIN
        xml2:=xml1;
        xml2:=get_parametros_get(xml2);
        estado_servicio:=get_campo('ESTADO_SERVICIO',xml2);
        tipo_tx:=get_campo('tipo_tx',xml2);

        if (tipo_tx='AMP_Valida_Doc') then
                if(estado_servicio='OK') then


                else

                        if (get_campo('COD_RESPUESTA',xml2)<>'') then

                                v_respuesta:='<URI>'||'</URI>'||chr(10)||'<Cod_Respuesta>'|| get_campo('COD_RESPUESTA',xml2) ||'</Cod_Respuesta>' ||chr(10)|| '<Desc_Respuesta>' || get_campo('DESC_RESPUESTA',xml2) ||'</Desc_Respuesta>' ||chr(10)|| '<XML64>' || '</XML64>';
                                xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK'||chr(10)||'Content-type: application/xml'||chr(10)||'Content-length: '||length(v_respuesta)||chr(10)||chr(10)||v_respuesta);

                        else
                                v_respuesta:='<URI>'||'</URI>'||chr(10)||'<Cod_Respuesta>'||'99</Cod_Respuesta>' ||chr(10)|| '<Desc_Respuesta>' || 'Error</Desc_Respuesta>' ||chr(10)|| '<XML64>' || '</XML64>';
                                xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK'||chr(10)||'Content-type: application/xml'||chr(10)||'Content-length: '||length(v_respuesta)||chr(10)||chr(10)||v_respuesta);
                        end if;
                end if;
        elseif(tipo_tx='AMP_Rechaza_Doc') then
                if(estado_servicio='ROK') then
                v_respuesta:='<Cod_Respuesta>'||'1</Cod_Respuesta>' ||chr(10)|| '<Desc_Respuesta>Rechazo Generado</Desc_Respuesta>';
                else
                v_respuesta:='<Cod_Respuesta>'||'99</Cod_Respuesta>' ||chr(10)|| '<Desc_Respuesta>' || 'Error Al generar Rechazo</Desc_Respuesta>';
                end if;

                xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK'||chr(10)||'Content-type: application/xml'||chr(10)||'Content-length: '||length(v_respuesta)||chr(10)||chr(10)||v_respuesta);
        end if;
        return xml2;
END;
$$ LANGUAGE plpgsql;

