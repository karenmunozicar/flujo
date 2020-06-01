--Publica documento
delete from isys_querys_tx where llave='12725';

insert into isys_querys_tx values ('12725',10,7,1,'select procesa_acm_12725(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,0,0);
--insert into isys_querys_tx values ('12725',20,45,1,'select sp_procesa_respuesta_cola_motor_original(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,0,0);

/*
--Vamos a insertar a WebIECV
insert into isys_querys_tx values ('12725',10,7,1,'select sp_inserta_dte_acm (''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,30,30);
--Verifica el resultado de la insercion
insert into isys_querys_tx values ('12725',30,7,1,'select sp_respuesta_acm_12725(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,0,0);
--insert into isys_querys_tx values ('12725',30,1,1,'select sp_respuesta_acm_12725(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,0,0);

*/



CREATE or replace FUNCTION procesa_acm_12725(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2        varchar;
    id1         varchar;
        status1 varchar;
        cola1   varchar;
BEGIN
        --Cambio la respuesta de ACM por la respuesta original
        xml2:=xml1;
	xml2:=sp_inserta_dte_acm(xml2);
	return sp_procesa_respuesta_cola_motor_original(xml2);	
END;
$$ LANGUAGE plpgsql;


/*

CREATE or replace FUNCTION sp_respuesta_acm_12725(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2        varchar;
    id1		varchar;
	status1	varchar;
	cola1	varchar;
BEGIN
        --Cambio la respuesta de ACM por la respuesta original
        xml2:=xml1;
	id1:=get_campo('__ID_DTE__',xml2);
        cola1:=get_campo('__COLA_MOTOR__',xml2);
	status1:=get_campo('__STATUS_ACM__',xml2);

	--Si me va bien borro
	if (status1='OK') then
		xml2 := logapp(xml2,'ACM OK RUT_EMISOR='||get_campo('RUT_EMISOR_DV',xml2)||' Tipo_DTE='||get_campo('TIPO_DTE',xml2)||' Folio='||get_campo('FOLIO',xml2)||' URI='||get_campo('URI_IN',xml2)||' CICLO='||get_campo('MANDATO_CICLO',xml2)||' id='||id1::varchar);
		--delete from cola_motor_cuadratura where id=id1::bigint;
		execute 'delete from '||cola1||' where id='||id1;
	else
		xml2 := logapp(xml2,'ACM FALLA ('||status1||') RUT_EMISOR='||get_campo('RUT_EMISOR_DV',xml2)||' Tipo_DTE='||get_campo('TIPO_DTE',xml2)||' Folio='||get_campo('FOLIO',xml2)||' URI='||get_campo('URI_IN',xml2)||' CICLO='||get_campo('MANDATO_CICLO',xml2)||' id='||id1::varchar);
		--update cola_motor_cuadratura set reintentos=reintentos+1 where id=id1::bigint;
		execute 'update '||cola1||' set reintentos=10,fecha_act=now() where id='||id1;
	end if;
        return xml2;
END;
$$ LANGUAGE plpgsql;

*/
