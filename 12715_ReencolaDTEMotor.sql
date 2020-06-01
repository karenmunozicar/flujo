--Publica documento
delete from isys_querys_tx where llave='12715';
--Obtiene el DTE Original con la entrada URI_IN
insert into isys_querys_tx values ('12715',5,1,8,'GET XML Custodium desde Almacen',12716,0,0,1,1,10,10);

insert into isys_querys_tx values ('12715',10,1,1,'select proc_graba_dte_cola_motor_12715(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,0,0);

CREATE or replace FUNCTION proc_graba_dte_cola_motor_12715(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
        xml2    varchar;
	rut1	varchar;
	data1	varchar;
BEGIN
    xml2:=xml1;

    --Verifico si viene correctamete el DTE
    if (get_campo('FALLA_CUSTODIUM',xml2)='SI') then
	xml2:=logapp(xml2,'DTE no leido desde almacen URI='||get_campo('URI_IN',xml2));
	return xml2;
    end if;

    --Viene la TX
    if (length(get_campo('TX_IN',xml2))=0) then
	xml2:=logapp(xml2,'No viene TX_IN URI='||get_campo('URI_IN',xml2));
	return xml2;
    end if;
    xml2:=put_campo(xml2,'INPUT',get_campo('XML_ALMACEN',xml2));
    xml2:=put_campo(xml2,'TX',get_campo('TX_IN',xml2));

    xml2:=logapp(xml2,'TX='||get_campo('TX',xml2));
    xml2:=logapp(xml2,'Largo INPUT ='||length(get_campo('INPUT',xml2))::varchar);
    xml2:=logapp(xml2,decode(get_campo('XML_ALMACEN',xml2),'hex')::varchar);

    --xml2:=sp_inserta_data(xml2);
    xml2:=logapp(xml2,'URI Insertada*');
    
    return xml2;
END;
$$ LANGUAGE plpgsql;
