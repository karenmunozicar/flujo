delete from isys_querys_tx where llave='12711';

insert into isys_querys_tx values ('12711',10,1,1,'select proc_procesa_rcf_externo_12711(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);


insert into isys_querys_tx values ('12711',100,1,1,'select proc_respuesta_rcf_externo_12711(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);


CREATE or replace FUNCTION proc_procesa_rcf_externo_12711(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
        xml2		varchar;
	data1		varchar;
	data_hex2	varchar;
	folio_inicio1	bigint;
	folio_fin1	bigint;
	cantidad1	integer;
	monto_neto1	bigint;
	monto_iva1	bigint;
	monto_total1	bigint;
	rut_emisor	integer;
	dia		integer;
	anulados	integer;
	tipo_dte	integer;
	externo		integer;
	part1		integer;
	param1		varchar;
	respuesta1	varchar;
	resp1		varchar;
	
BEGIN
    xml2:=xml1; 
    --POr defecto paramos la ejecucion
    xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
	if length(get_campo('INPUT',xml2))=0 then
                data1:=get_campo('QUERY_STRING',xml2);
         else
                data_hex2:=get_campo('INPUT',xml2);
                data1:=decode(data_hex2,'hex');
         end if;
	--xml2:=logapp(xml2,'data ---'||data1);
	part1:=1;	
        param1 := split_part(data1,'&',1);
        --raise notice 'param1=(%)',param1;
        while param1 <> '' loop
             xml2 := put_campo(xml2,split_part(param1,'=',1),split_part(param1,'=',2));
             part1 := part1 + 1;
             param1 := split_part(data1,'&',part1);
        end loop;
--	xml2:=logapp(xml2,'folioooo---'||folio_inicio1||'---');
	
	--obtengo los datos desde xml2
	folio_inicio1 :=get_campo('FOLIO_INICIO',xml2);
	folio_fin1 :=get_campo('FOLIO_FIN',xml2);
	cantidad1 :=get_campo('CANTIDAD',xml2);
	monto_neto1 :=get_campo('MONTO_NETO',xml2);
	monto_iva1 :=get_campo('MONTO_IVA',xml2);
	monto_total1 :=get_campo('MONTO_TOTAL',xml2);
	rut_emisor :=get_campo('RUT_EMISOR',xml2);
	dia :=get_campo('DIA',xml2);
	anulados :=get_campo('ANULADOS',xml2);
	tipo_dte :=get_campo('TIPO_DTE',xml2);
	externo :=get_campo('EXTERNO',xml2);


	insert into rango_boletas values (folio_inicio1,folio_fin1,cantidad1,monto_neto1,monto_iva1,monto_total1,rut_emisor,dia,anulados,tipo_dte,externo);
	
	resp1:='OK, registro ingresado';
	respuesta1:='Status: 200 OK'||chr(10)||
                 'Content-type: application/json;charset=UTF-8;'||chr(10)||
                 'Content-length: '||length(resp1)||chr(10)||chr(10)||resp1;

        xml2 := put_campo(xml2,'__SOCKET_RESPONSE__','RESPUESTA');
        xml2 := logapp(xml2,'Respuesta Servicio 200 OK');
   	xml2:=put_campo(xml2,'RESPUESTA',respuesta1); 
   return xml2;

END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION proc_respuesta_rcf_externo_sii_12711(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2    varchar;
	resp1	varchar;
	respuesta1	varchar;
	status1	varchar;
	datos1	varchar;
	rutEmiSDV varchar;
BEGIN
    xml2:=xml1;
     --Leemos la respuesta
    resp1:=get_campo('RESPUESTA',xml2);
    if (strpos(resp1,'200 OK')>0) then

    end if;

	xml2:=put_campo(xml2,'RESPUESTA',status1||chr(10)||respuesta1);
	RETURN xml2;
END;
$$ LANGUAGE plpgsql;


