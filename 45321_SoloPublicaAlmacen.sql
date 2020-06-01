delete from isys_querys_tx where llave='45321';
insert into isys_querys_tx values ('45321',20,1,8,'Publica',112704,0,0,0,0,100,100);
insert into isys_querys_tx values ('45321',100,1,1,'select revisa_publicacion_45321(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,0,0);

CREATE or replace FUNCTION revisa_publicacion_45321(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2    varchar;
BEGIN
    xml2:=xml1;
    if (get_campo('__PUBLICADO_OK__',xml2)='SI') then
	xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
    else
	xml2:=put_campo(xml2,'RESPUESTA','Status: 444 NK');
    end if;
    xml2:=sp_procesa_respuesta_cola_motor_original(xml2);
    return xml2;
END;
$$ LANGUAGE plpgsql;


