--Publica documento
delete from isys_querys_tx where llave='13999';


--Obtengo el XML de la URI en la cola
insert into isys_querys_tx values ('13999',10,1,8,'Obtiene el XML',12705,0,0,1,1,20,20);
insert into isys_querys_tx values ('13999',20,1,1,'select sp_reprocesa_imptos_13999(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,21,21);
insert into isys_querys_tx values ('13999',21,1,1,'select sp_reprocesa_resp_13999(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,0,0);


CREATE or replace FUNCTION sp_reprocesa_imptos_13999(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2        varchar;
    uri1	varchar;
    id1		varchar;
    cola1	varchar;
    data_hex1	varchar;
    data1	varchar;
    data3	varchar;
    rut_emisor1	integer;
    tipo_dte1   integer;
    folio1	integer;
    cod_cliente1 varchar;
    len	integer;
   nPos integer;
--Para procesar Imptos
        totalImpuestos integer;
        tag1 varchar;
        tipo_imp1 varchar;
        tasa1 varchar;
        valor1 varchar;
        json3             json = '{}';
         imptos1           json = '[]';

BEGIN
        --Cambio la respuesta de cuadratura por la respuesta original
        xml2:=xml1;
	uri1:=get_campo('URI_IN',xml2);
	id1:=get_campo('__ID_DTE__',xml2);
        cola1:=get_campo('__COLA_MOTOR__',xml2);
	
	data_hex1 := get_campo('XML_ALMACEN',xml2);
        data1:= decode(data_hex1,'hex');
	--Tiene impuestos
	if get_xml('ImptoReten',data1) = ''  and get_campo('CANAL',xml2)='EMITIDOS' then
		--No tiene impuestos entonces salgo
		xml2:=logapp(xml2,'DTe sin Impuestos ' || uri1);
		execute 'delete from '||cola1||' where id='||id1;	
		return xml2;
	end if;

	rut_emisor1:=split_part(get_xml('RUTEmisor',data1),'-',1);
	tipo_dte1:=get_xml('TipoDTE',data1)::integer;
	folio1:=get_xml('Folio',data1)::integer;
	cod_cliente1:=get_xml('CdgIntRecep',data1);

    --RME 20151216 Se agrega el parser de los Impuestos.
    --Proceso TAG Totales completo
    data3:= get_xml('Totales',data1);
    len:=length(data3);
    nPos:=0;
    --totalImpuestos:=0;
    while (length(data3)>0) loop

        --Ubico primer TAG
        tag1:=split_part(split_part(data3,'<',2),'>',1);
         xml2:=logapp(xml2,'Procesando Impuestos...'||tag1);
        if length(tag1)=0 then
            exit;
        end if;
        --Si es Impuesto tiene tramiento distinto
        if (tag1='ImptoReten') then
            --Veo el tipo de impuesto
            tipo_imp1:=get_xml('TipoImp',data3);
            if is_number(tipo_imp1) is FALSE then
                tipo_imp1:='0';
                xml2:=logapp(xml2,'CODIGO imptos NO numerico '||tipo_imp1);
            end if;
            tasa1:=get_xml('TasaImp',data3);
            --Si no viene valor de tasa o no es numerico asumimos 0
             if (is_numeric(tasa1) is false) then
                  tasa1:='0.0';
             end if;
            --Si algun valor no es numerico no lo inserta
            valor1:=get_xml('MontoImp',data3);
             if (is_number(valor1) is false) then
                 xml2:=logapp(xml2,'Impuesto Retenido NO NUMERICO '||tipo_imp1||','||tasa1||','||valor1||);
             else
                 --Se completa la lista de impuestos para el DTE
                 json3:='{}';
                 json3:=put_json(json3,'TIPO',tipo_imp1::varchar);
                 json3:=put_json(json3,'TASA',tasa1::varchar);
                 json3:=put_json(json3,'MONTO',valor1::varchar);
                 imptos1:=put_json_list(imptos1,json3);
             end if;
        end if;

        --Me salto el tag encontrado
        tag1:='</'||tag1||'>';
        --Nos saltamos el tag final
        nPos:=strpos(data3,tag1)+length(tag1);
        --Avanzamos en el texto
        data3:=substring(data3,nPos,len);

    end loop;
    --Se guardan los impuestos del DTE, para hacer el update en tabla correspondiente
    xml2:=put_campo(xml2,'IMPUESTOS_DTE',imptos1::varchar);

	xml2:=logapp(xml2,'DATOS REPROCESO-->'|| rut_emisor1::varchar || '-->'||tipo_dte1::varchar||'-->'||folio1::varchar|| '-->'||cod_cliente1);


	if (get_campo('CANAL',xml2)='EMITIDOS') then
		update dte_emitidos set impuestos=imptos1 where rut_emisor=rut_emisor1 and tipo_Dte=tipo_dte1 and folio=folio1;
	else
	       update dte_recibidos set impuestos=imptos1, data_dte= case when cod_cliente1<>'' then '<Codigo_Cliente>'||cod_cliente1||'<\Codigo_Cliente>' else null end  where rut_emisor=rut_emisor1 and tipo_Dte=tipo_dte1 and folio=folio1;
	end if;
	
	

	if found then
		xml2:=put_campo(xml2,'FLAG_EXITO','SI');
		xml2:=logapp(xml2,'Exito, se elimina reg');
--		 execute 'delete from '||cola1||' where id='||id1;
--                return xml2;
	else
		xml2:=put_campo(xml2,'FLAG_EXITO','NO');
		xml2:=logapp(xml2,'Falla se aumenta reintento');
--		execute 'update '||cola1||' set reintentos=reintentos+1 where id='||id1; 
	end if;

        return xml2;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION sp_reprocesa_resp_13999(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2        varchar;
    id1		varchar;
    cola1	varchar;	
BEGIN
	xml2:=xml1;
        id1:=get_campo('__ID_DTE__',xml2);
        cola1:=get_campo('__COLA_MOTOR__',xml2);
	xml2:=logapp(xml2,'Borrando -->' ||id1 ||' de '||cola1);

	if get_campo('FLAG_EXITO',xml2) ='SI' then
		execute 'delete from '||cola1||' where id='||id1;
	else
		execute 'update '||cola1||' set reintentos=reintentos+1 where id='||id1;
	end if;	

        return xml2;
END;
$$ language plpgsql;
