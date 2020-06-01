--Publica documento
delete from isys_querys_tx where llave='12792';

--URI_IN,
insert into isys_querys_tx values ('12792',10,1,8,'Obtiene el XML',12705,0,0,1,1,20,20);
--XML_ALMACEN
insert into isys_querys_tx values ('12792',20,1,1,'select sp_marca_ind_servicio_cge_12792(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,0,0);


CREATE or replace FUNCTION sp_marca_ind_servicio_cge_12792(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2        varchar;
    id1		varchar;
	status1	varchar;
	cola1	varchar;
	data1	varchar;
	ind_serv1	varchar;
	uri1	varchar;
BEGIN
        --Cambio la respuesta de cuadratura por la respuesta original
        xml2:=xml1;
	id1:=get_campo('__ID_DTE__',xml2);
	cola1:=get_campo('__COLA_MOTOR__',xml2);
	status1:=get_campo('FALLA_CUSTODIUM',xml2);
	xml2:=put_campo(xml2,'__SECUENCIAOK__','0');
	
	--Si me va bien borro
	if (status1='SI') then
		xml2:= logapp(xml2,'12792 -Falla Custodium');
		execute 'update '||cola1||' set reintentos=reintentos+1 where id='||id1;
	else
		--Obtengo el ind de servicio
		data1:= decode(get_campo('XML_ALMACEN',xml2),'hex');
		ind_serv1:=get_xml('IndServicio',data1);
		uri1:=get_campo('URI_IN',xml2);
		update dte_emitidos_cge set ind_servicio=ind_serv1 where uri=uri1 and estado_cge='SI_ELECTRONICO2';
		xml2:= logapp(xml2,'12792 - MArca Ind Servicio OK');
		execute 'delete from '||cola1||' where id='||id1;
	end if;
        return xml2;
END;
$$ LANGUAGE plpgsql;

