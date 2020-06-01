--Publica documento
delete from isys_querys_tx where llave='12722';

--Llamamos a Escribir Direco en cuadratura_indexer
insert into isys_querys_tx values ('12722',40,5,1,'select inserta_cuadratura_indexer_event(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,50,50);
insert into isys_querys_tx values ('12722',50,1,1,'select proc_respuesta_cuadratura_12722(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('12722',60,19,1,'select proc_respuesta_cuadratura_12722_original(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,0,0);

CREATE OR REPLACE FUNCTION proc_respuesta_cuadratura_12722(character varying) RETURNS varchar AS $$
declare
        xml1    alias for $1;
        xml2    varchar;
        id1     varchar;
        cola1   varchar;
        codigo1 varchar;
        json1   json;
        xml3    varchar;
begin
        xml2:=xml1;
        --Si viene de la base nueva de colas vamos a ejecutar alla el borrado
        if (get_campo('BD_ORIGEN',xml2)='172.16.10.132') then
		xml2 := put_campo(xml2,'__SECUENCIAOK__','60');
		/*
                xml3 := get_parametros_motor('','BASE_COLAS');
                xml2 := logapp(xml2,'Vamos a Borrar en Base_colas');
                json1:=query_db_json(get_campo('__IP_CONEXION_CLIENTE__',xml3),get_campo('__IP_PORT_CLIENTE__',xml3)::integer,'select proc_respuesta_cuadratura_12722_original('||quote_literal(xml2)||')');
                return get_json('proc_respuesta_cuadratura_12722_original',json1);
		*/
		return xml2;
        else
                --Sino ejecutamos aca en la base actual
		xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
                return proc_respuesta_cuadratura_12722_original(xml2);
        end if;
end;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION proc_respuesta_cuadratura_12722_original(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2        varchar;
    id1		varchar;
	status1	varchar;
	cola1	varchar;
BEGIN
        --Cambio la respuesta de cuadratura por la respuesta original
        xml2:=xml1;
	id1:=get_campo('__ID_DTE__',xml2);
	status1:=get_campo('__STATUS_CUADRATURA__',xml2);
	cola1:=get_campo('__COLA_MOTOR__',xml2);
	--xml2:=logapp(xml2,'Status Cuadratura='||status1);
	--Si me va bien borro
	if (status1='OK') then
		xml2 := logapp(xml2,'Evento OK Cuadratura CGE RUT_EMISOR='||get_campo('RUT_EMISOR_DV',xml2)||' Tipo_DTE='||get_campo('TIPO_DTE',xml2)||' Folio='||get_campo('FOLIO',xml2)||' URI='||get_campo('URI_IN',xml2));
		--delete from cola_motor_cuadratura where id=id1::bigint;
		execute 'delete from '||cola1||' where id='||id1;
	else
		xml2 := logapp(xml2,'Evento FALLA Cuadratura ('||status1||') CGE RUT_EMISOR='||get_campo('RUT_EMISOR_DV',xml2)||' Tipo_DTE='||get_campo('TIPO_DTE',xml2)||' Folio='||get_campo('FOLIO',xml2)||' URI='||get_campo('URI_IN',xml2)||' Error='||get_campo('ERROR',xml2));
		--update cola_motor_cuadratura set reintentos=reintentos+1 where id=id1::bigint;
		execute 'update '||cola1||' set reintentos=reintentos+1 where id='||id1;
	end if;
        return xml2;
END;
$$ LANGUAGE plpgsql;

