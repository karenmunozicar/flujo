delete from isys_querys_tx where llave='12734';

--
insert into isys_querys_tx values ('12734',10,1,8,'Obtiene el XML',12705,0,0,1,1,15,15);

insert into isys_querys_tx values ('12734',15,13,1,'select parseo_datos_webiecv_new(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,20,20);

--Vamos a insertar a WebIECV CGE
insert into isys_querys_tx values ('12734',20,13,1,'select sp_inserta_docto_new(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,30,30);
--Verifica el resultado de la insercion
insert into isys_querys_tx values ('12734',30,1,1,'select sp_respuesta_webiecv_12734(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,0,0);

CREATE or replace FUNCTION sp_respuesta_webiecv_12734(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2        varchar;
    id1         varchar;
    status1     varchar;
    status2     varchar;
    cola1       varchar;
    rut_emisor1 integer;
    tipo_dte1   integer;
    folio1      bigint;
BEGIN
    --Cambio la respuesta de cuadratura por la respuesta original
    xml2:=xml1;
    id1:=get_campo('__ID_DTE__',xml2);
    cola1:=get_campo('__COLA_MOTOR__',xml2);
    status1:=get_campo('__STATUS_WEBIECV__',xml2);
    status2:=get_campo('__STATUS_WEBIECV_NEW__',xml2);
    --Si me va bien borro

    --RME solo para limpiar colas

    if (status2='OK') then

        xml2 := logapp(xml2,'IECV2 ('||status2||') RUT_EMISOR='||get_campo('RUT_EMISOR_DV',xml2)||' Tipo_DTE='||get_campo('TIPO_DTE',xml2)||' Folio='||get_campo('FOLIO',xml2)||' URI='||get_campo('URI_IN',xml2)||' CANAL'||get_campo('CANAL',xml2)||' Imptos ' || get_campo('IMPUESTOS_DTE',xml2));
        execute 'delete from '||cola1||' where id='||id1;
        --Generamos un evento de WEBIECV
        if (get_campo('CANAL',xml2)='EMITIDOS') then
	    /*
            rut_emisor1:=get_campo('RUT_EMISOR',xml2)::integer;
            tipo_dte1:=get_campo('TIPO_DTE',xml2)::integer;
            folio1:=get_campo('FOLIO',xml2)::bigint;
            if (get_campo('IMPUESTOS_DTE',xml2)<>'') then
                --FAY-RME-KMS 2015-10-23 Se agrega actualizacion de impuestos en DTE_Emitidos
                update dte_emitidos set impuestos=get_campo('IMPUESTOS_DTE',xml2)::json where rut_emisor=rut_emisor1 and tipo_Dte=tipo_dte1 and folio=folio1;
                if not found then
                    xml2 := logapp(xml2,'IECV2 No graba Impuestos  URI='||get_campo('URI_IN',xml2)||' IMPUESTOS_DTE='||get_campo('IMPUESTOS_DTE',xml2));
                end if;
            end if;
	    */
            xml2:=graba_bitacora(xml2,'CDV');
	--2015-11-30 RME-FAY se actualizan los impuestos en dte_recibidos
	elsif (get_campo('CANAL',xml2)='RECIBIDOS') then
	    /*
	    rut_emisor1:=get_campo('RUT_EMISOR',xml2)::integer;
            tipo_dte1:=get_campo('TIPO_DTE',xml2)::integer;
            folio1:=get_campo('FOLIO',xml2)::bigint;
	    if (get_campo('IMPUESTOS_DTE',xml2)<>'') then
		update dte_recibidos set impuestos=get_campo('IMPUESTOS_DTE',xml2)::json where rut_emisor=rut_emisor1 and tipo_Dte=tipo_dte1 and folio=folio1;
                if not found then
                    xml2 := logapp(xml2,'IECV2 No graba Impuestos en Recibidos  URI='||get_campo('URI_IN',xml2)||' IMPUESTOS_DTE='||get_campo('IMPUESTOS_DTE',xml2));
		    end if;
	    end if;
	    */
	    xml2:=graba_bitacora(xml2,'CDC');
        else
	    xml2 := logapp(xml2,'IECV2 CANAL INVALIDO');
            --xml2:=graba_bitacora(xml2,'CDC');
        end if;
    elsif (status2='DOC_EXISTE') then
            xml2 := logapp(xml2,'IECV2 BORRA_COLA RUT_EMISOR='||get_campo('RUT_EMISOR_DV',xml2)|| ' Tipo_DTE='||get_campo('TIPO_DTE',xml2)||' Folio='||get_campo('FOLIO',xml2)||' URI='||get_campo('URI_IN',xml2)|| ' Porque ya existe en la Base de Datos... ' );

            execute 'delete from '||cola1||' where id='||id1;
	--Si el DTE no debe considerarse
    elsif (status2='__BASURA_CON_URI__') then
            xml2 := logapp(xml2,'IECV2 BORRA_COLA No se considera RUT_EMISOR='||get_campo('RUT_EMISOR_DV',xml2)|| ' Tipo_DTE='||get_campo('TIPO_DTE',xml2)||' Folio='||get_campo('FOLIO',xml2)||' URI='||get_campo('URI_IN',xml2)|| ' Porque ya existe en la Base de Datos... ' );
            execute 'delete from '||cola1||' where id='||id1;
	
   else
            xml2 := logapp(xml2,'IECV2 FALLA ('||status1||','||status2||') RUT_EMISOR='||get_campo('RUT_EMISOR_DV',xml2)||' Tipo_DTE='||get_campo('TIPO_DTE',xml2)||' Folio='||get_campo('FOLIO',xml2)||' URI='||get_campo('URI_IN',xml2));
            execute 'update '||cola1||' set reintentos=reintentos+1 where id='||id1;
    end if;
    return xml2;
END;
$$ LANGUAGE plpgsql;

