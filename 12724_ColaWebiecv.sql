--Publica documento
delete from isys_querys_tx where llave='12724';

--Busca DTE en respaldo_dte
--insert into isys_querys_tx values ('12724',5,1,1,'select sp_busca_dte_respaldo_12724(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

--Obtengo el XML de la URI en la cola
--Vamos a insertar a WebIECV
--insert into isys_querys_tx values ('12724',10,1,8,'Obtiene el XML',12705,0,0,1,1,22,22);
--insert into isys_querys_tx values ('12724',20,3,1,'select sp_inserta_docto(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,21,21);
--insert into isys_querys_tx values ('12724',21,13,1,'select parseo_datos_webiecv_new(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,22,22);

insert into isys_querys_tx values ('12724',10,19,1,'select get_input_almacen(''{"uri":"$$URI_IN$$"}'') as xml_almacen',0,0,0,1,1,22,22);

insert into isys_querys_tx values ('12724',22,13,1,'select sp_inserta_docto_new(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,30,30);

/*
--Vamos a insertar a WebIECV CGE
insert into isys_querys_tx values ('12724',12,1,8,'Obtiene el XML',12705,0,0,1,1,25,25);
insert into isys_querys_tx values ('12724',25,8,1,'select sp_inserta_docto(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,27,27);
--insert into isys_querys_tx values ('12724',26,13,1,'select parseo_datos_webiecv_new(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,27,27);
insert into isys_querys_tx values ('12724',27,13,1,'select sp_inserta_docto_new(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,30,30);
*/
--Verifica el resultado de la insercion
insert into isys_querys_tx values ('12724',30,8022,1,'select sp_respuesta_webiecv_12724(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,0,0);

CREATE or replace FUNCTION sp_busca_dte_respaldo_12724(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2        varchar;
    uri1	varchar;
	stResp	respaldo_dte%ROWTYPE;
BEGIN
        --Cambio la respuesta de cuadratura por la respuesta original
        xml2:=xml1;
	uri1:=get_campo('URI_IN',xml2);
	select * into stResp from respaldo_dte where uri=uri1;
	if found then
		xml2:=logapp(xml2,'Obtiene el XML de respaldo_dte');
		xml2:=put_campo(xml2,'XML_ALMACEN',get_campo('INPUT',stResp.data));
		if (get_campo('CANAL',xml2)='EMITIDOS') then
			xml2:=put_campo(xml2,'RUT_CGE',get_campo('RUT_EMISOR',xml2));
		else
			xml2:=put_campo(xml2,'RUT_CGE',get_campo('RUT_RECEPTOR',xml2));
		end if;
		xml2:=verifica_evento_cge(xml2);
		if (get_campo('EVENTO_CGE',xml2)='SI') then
			xml2:=put_campo(xml2,'__SECUENCIAOK__','25');
			xml2:=logapp(xml2,'WEBIECV: DTE de CGE');
		else
			xml2:=put_campo(xml2,'__SECUENCIAOK__','21');
			xml2:=logapp(xml2,'WEBIECV: DTE Normal');
		end if;
	else
		--Va a buscar el DTE al almacen
		xml2:=logapp(xml2,'Busca XML al almacen');
		if (get_campo('CANAL',xml2)='EMITIDOS') then
			xml2:=put_campo(xml2,'RUT_CGE',get_campo('RUT_EMISOR',xml2));
		else
			xml2:=put_campo(xml2,'RUT_CGE',get_campo('RUT_RECEPTOR',xml2));
		end if;
		xml2:=verifica_evento_cge(xml2);
		if (get_campo('EVENTO_CGE',xml2)='SI') then
			xml2:=put_campo(xml2,'__SECUENCIAOK__','12');
			xml2:=logapp(xml2,'WEBIECV: DTE de CGE');
		else
			xml2:=put_campo(xml2,'__SECUENCIAOK__','10');
			xml2:=logapp(xml2,'WEBIECV: DTE Normal RUT_CGE='||get_campo('RUT_CGE',xml2));
		end if;
	end if;
        return xml2;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION sp_respuesta_webiecv_12724(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2        varchar;
    id1		varchar;
	status1	varchar;
	status2	varchar;
	cola1	varchar;
	rut_emisor1	integer;
	tipo_dte1	integer;
	folio1	bigint;
BEGIN
        --Cambio la respuesta de cuadratura por la respuesta original
        xml2:=xml1;
	id1:=get_campo('__ID_DTE__',xml2);
	cola1:=get_campo('__COLA_MOTOR__',xml2);
	--status1:=get_campo('__STATUS_WEBIECV__',xml2);
	status1:='OK';
	status2:=get_campo('__STATUS_WEBIECV_NEW__',xml2);
	--Si me va bien borro

        if (status2='DOC_EXISTE') then
            xml2 := logapp(xml2,'WebIECV BORRA_COLA RUT_EMISOR='||get_campo('RUT_EMISOR_DV',xml2)||' Tipo_DTE='||get_campo('TIPO_DTE',xml2)||' Folio='||get_campo('FOLIO',xml2)||' URI='||get_campo('URI_IN',xml2)|| ' Porque ya existe en la Base de Datos... ID='||id1::varchar );

            --execute 'update '||cola1||' set reintentos=100 where id='||id1;
            execute 'delete from '||cola1||' where id='||id1;
            return xml2;
        end if;

	--RME solo para limpiar colas
	-- status2:='NOK';

	if (status1='OK' and status2='OK') then
		
		xml2 := logapp(xml2,'WebIECV ('||status1||','||status2||') RUT_EMISOR='||get_campo('RUT_EMISOR_DV',xml2)||' Tipo_DTE='||get_campo('TIPO_DTE',xml2)||' Folio='||get_campo('FOLIO',xml2)||' URI='||get_campo('URI_IN',xml2)||' ID='||id1::varchar);
		execute 'delete from '||cola1||' where id='||id1;
		--Generamos un evento de WEBIECV
		if (get_campo('CANAL',xml2)='EMITIDOS') then
			rut_emisor1:=get_campo('RUT_EMISOR',xml2)::integer;
			tipo_dte1:=get_campo('TIPO_DTE',xml2)::integer;
			folio1:=get_campo('FOLIO',xml2)::bigint;
			if (get_campo('IMPUESTOS_DTE',xml2)<>'') then
				--FAY-RME-KMS 2015-10-23 Se agrega actualizacion de impuestos en DTE_Emitidos
				update dte_emitidos set impuestos=get_campo('IMPUESTOS_DTE',xml2)::json where rut_emisor=rut_emisor1 and tipo_Dte=tipo_dte1 and folio=folio1;
				if not found then
					xml2 := logapp(xml2,'WebIECV No graba Impuestos  URI='||get_campo('URI_IN',xml2)||' IMPUESTOS_DTE='||get_campo('IMPUESTOS_DTE',xml2));
				end if;
			end if;
			xml2:=graba_bitacora(xml2,'CDV');
		else
			xml2:=graba_bitacora(xml2,'CDC');
		end if;
	--Si hay que desechar el DTE
	elsif (status2='__BASURA_CON_URI__') then
		xml2 := logapp(xml2,'WebIECV ('||status1||','||status2||') RUT_EMISOR='||get_campo('RUT_EMISOR_DV',xml2)||' Tipo_DTE='||get_campo('TIPO_DTE',xml2)||' Folio='||get_campo('FOLIO',xml2)||' URI='||get_campo('URI_IN',xml2)||' Se Borra DTE ID='||id1::varchar);
		execute 'delete from '||cola1||' where id='||id1;
			
	else
		xml2 := logapp(xml2,'WebIECV FALLA ('||status1||','||status2||') RUT_EMISOR='||get_campo('RUT_EMISOR_DV',xml2)||' Tipo_DTE='||get_campo('TIPO_DTE',xml2)||' Folio='||get_campo('FOLIO',xml2)||' URI='||get_campo('URI_IN',xml2)||' ID='||id1::varchar);
		 execute 'update '||cola1||' set reintentos=reintentos+1 where id='||id1;
	end if;
        return xml2;
END;
$$ LANGUAGE plpgsql;

