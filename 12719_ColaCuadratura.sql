--Publica documento
delete from isys_querys_tx where llave='12719';

--Llamamos a Escribir Directo
--insert into isys_querys_tx values ('12719',40,1,1,'select proc_cuadratura_in_12179(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('12719',50,1,2,'Llamada a Cuadratura',4011,100,101,0,0,60,60);
insert into isys_querys_tx values ('12719',60,1,1,'select proc_respuesta_cuadratura_12719(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

/*
CREATE or replace FUNCTION proc_cuadratura_in_12179(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
        xml2    varchar;
BEGIN
    xml2:=xml1;
    
    return xml2;
END;
$$ LANGUAGE plpgsql;
*/

CREATE or replace FUNCTION proc_respuesta_cuadratura_12719(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2        varchar;
        data1   varchar;
	sts1	varchar;
	publicado1	varchar;
	id1	varchar;
BEGIN
        --Cambio la respuesta de cuadratura por la respuesta original
        xml2:=xml1;
	id1:=get_campo('__ID_DTE__',xml2);
	xml2:=logapp(xml2,get_campo('INPUT',xml2));
	--Si me va bien borro
	if (strpos(get_campo('RESPUESTA',xml2),'200 OK')>0) then
		xml2:=logapp(xml2,'Respuesta Cuadratura OK');	
		delete from cola_motor_cuadratura where id=id1::bigint;
	else
		xml2:=logapp(xml2,'Falla Respuesta Cuadratura');	
		 xml2:=logapp(xml2,get_campo('RESPUESTA',xml2));
		update cola_motor_cuadratura set reintentos=reintentos+1 where id=id1::bigint;
	end if;
        return xml2;
END;
$$ LANGUAGE plpgsql;

