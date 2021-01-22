delete from isys_querys_tx where llave='12777';

-- Prepara llamada al AML
--insert into isys_querys_tx values ('12777',5,1,1,'select proc_procesa_input_12777(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0); 
insert into isys_querys_tx values ('12777',5,10,1,'select proc_procesa_input_12777(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0); 
--insert into isys_querys_tx values ('12777',10,1,8,'Ejecuta Traza',12701,0,0,1,1,20,20);
insert into isys_querys_tx values ('12777',10,1,8,'Ejecuta Traza',112703,0,0,1,1,20,20);

--Se ejecuta en la Base del EDTE3
insert into isys_querys_tx values ('12777',20,10,1,'select public.sp_procesa_respuesta_cola_edte3(''$$__ID_DTE__$$'',''$$NOMBRE_COLA$$'',''$$RESPUESTA$$'') as __xml__',0,0,0,1,1,0,0); 

CREATE or replace FUNCTION proc_procesa_input_12777(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
        xml2    varchar;
BEGIN
    xml2:=xml1;

    
    xml2:=put_campo(xml2,'COMENTARIO1',replace(get_campo('COMENTARIO1',xml2),chr(92),''));
    xml2:=logapp(xml2,'DIGEST='||substring(get_campo('DIGEST',xml2),3));
    --Armo el INPUT 
    --xml2:=put_campo(xml2,'INPUT',encode(('<?xml version=''1.0'' encoding=''UTF-8''?><trace source="EnvioDTE" version="1.1"><node name="'||get_campo('EVENTO',xml2)||'" stamp="'||get_campo('FECHA',xml2)||'" owner="'||get_campo('OWNER',xml2)||'"><keys><key name="rutEmisor" value="'||get_campo('RUT_EMISOR',xml2)||'"/><key name="tipoDTE" value="'||get_campo('TIPO_DTE',xml2)||'"/><key name="folio" value="'||get_campo('FOLIO',xml2)||'"/><key name="fchEmis" value="'||get_campo('FECHA_EMISION',xml2)||'"/></keys><attrs><attr key="code">'||get_campo('TIPO_DTE',xml2)||'</attr><attr key="url">'||get_campo('URI',xml2)||'</attr><attr key="relatedUrl">'||get_campo('RELATED_URI',xml2)||'</attr><attr key="orig">'||get_campo('RUT_EMISOR',xml2)||'</attr><attr key="dest">'||get_campo('RUT_RECEPTOR',xml2)||'</attr><attr key="tag">'||get_campo('FOLIO',xml2)||'</attr><attr key="trackid">'||get_campo('TRACK_ID',xml2)||'</attr><attr key="comment">'||get_campo('COMENTARIO1',xml2)||'</attr><attr key="data">'||encode(decode(substring(get_campo('DIGEST',xml2),3),'hex'),'base64')||'</attr></attrs></node></trace>')::bytea,'hex')::varchar);
    xml2:=put_campo(xml2,'INPUT',encode(('<?xml version=''1.0'' encoding=''UTF-8''?><trace source="EnvioDTE" version="1.1"><node name="'||get_campo('EVENTO',xml2)||'" stamp="'||get_campo('FECHA',xml2)||'" owner="'||get_campo('OWNER',xml2)||'"><keys><key name="rutEmisor" value="'||get_campo('RUT_EMISOR',xml2)||'"/><key name="tipoDTE" value="'||get_campo('TIPO_DTE',xml2)||'"/><key name="folio" value="'||get_campo('FOLIO',xml2)||'"/><key name="fchEmis" value="'||get_campo('FECHA_EMISION',xml2)||'"/></keys><attrs><attr key="code">'||get_campo('TIPO_DTE',xml2)||'</attr><attr key="url">'||get_campo('URI',xml2)||'</attr><attr key="relatedUrl">'||get_campo('RELATED_URI',xml2)||'</attr><attr key="orig">'||get_campo('RUT_EMISOR',xml2)||'</attr><attr key="dest">'||get_campo('RUT_RECEPTOR',xml2)||'</attr><attr key="tag">'||get_campo('FOLIO',xml2)||'</attr><attr key="trackid">'||get_campo('TRACK_ID',xml2)||'</attr><attr key="comment">'||get_campo('COMENTARIO1',xml2)||'</attr><attr key="data">'||decode_hex_base64(substring(get_campo('DIGEST',xml2),3))||'</attr></attrs></node></trace>')::bytea,'hex')::varchar);
   --xml2:=logapp(xml2,'INPUT='||get_campo('INPUT',xml2));

   xml2 := put_campo(xml2,'__SECUENCIAOK__','10');
   return xml2;
END;
$$ LANGUAGE plpgsql;
