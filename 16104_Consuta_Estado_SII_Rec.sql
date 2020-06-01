--Consulta Estado Reclamo SII
delete from isys_querys_tx where llave='16104';

insert into isys_querys_tx values ('16104',10,1,1,'select armo_consulta_sii_16104(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('16104',20,1,2,'Servicio de SII 172.16.14.88',4013,100,101,0,0,30,30);
insert into isys_querys_tx values ('16104',30,1,1,'select proceso_respuesta_sii_16104(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('16104',1000,19,1,'select sp_procesa_respuesta_cola_motor88_json(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,0,0);

CREATE or replace FUNCTION armo_consulta_sii_16104(varchar) RETURNS varchar AS $$
DECLARE
        xml1   alias for $1;
        xml2   varchar;

        json_in json;
	port	varchar;
BEGIN
	xml2:=xml1;

	json_in:='{"RutCompania": "'||get_campo('RUT_EMISOR',xml2)||'","DvCompania":"'||modulo11(get_campo('RUT_EMISOR',xml2))||'","RutReceptor":"'||get_campo('RUT_RECEPTOR',xml2)||'","DvReceptor":"'||modulo11(get_campo('RUT_RECEPTOR',xml2))||'","TipoDte":"'||get_campo('TIPO_DTE',xml2)||'","FolioDte":"'||get_campo('FOLIO',xml2)||'","FechaEmisionDte":"'||get_campo('FECHA_EMISION',xml2)||'","MontoDte":"'||get_campo('MONTO_TOTAL',xml2)||'","RUT_OWNER":"'||get_campo('RUT_RECEPTOR',xml2)||'"}';
	
	xml2:=logapp(xml2,'SII: '||json_in::varchar);

	xml2 := put_campo(xml2,'__SECUENCIAOK__','20');
	port:=get_ipport_sii();
        if (port='') then
               --Si no hay puertos libres...
               xml2:=logapp(xml2,'No hay puertos libres');
               xml2:=put_campo(xml2,'RESPUESTA','Status: 400 NK');
               xml2 := put_campo(xml2,'__SECUENCIAOK__','1000');
               return xml2;
        end if;
        xml2:=put_campo(xml2,'__IP_CONEXION_CLIENTE__',split_part(port,':',1));
        xml2:=put_campo(xml2,'__IP_PORT_CLIENTE__',split_part(port,':',2));
        xml2:=put_campo(xml2,'IPPORT_SII',port);


        --xml2:=get_parametros_motor(xml2,'SERVICIO_SII_JSON');
	--xml2:=put_campo(xml2,'__IP_PORT_CLIENTE__',port);
        --xml2:=put_campo(xml2,'IP_PORT_CLIENTE',port);

        xml2:=put_campo(xml2,'INPUT','POST /estado_dte HTTP/1.1'||chr(10)||'User-Agent: curl/7.26.0'||chr(10)||'Host: '||get_campo('__IP_CONEXION_CLIENTE__',xml2)||':'||get_campo('__IP_PORT_CLIENTE__',xml2)||chr(10)||'Accept: '||chr(42)||'/'||chr(42)||chr(10)||'Content-Type: application/json; charset=ISO-8859-1'||chr(10)||'Content-Length: '||length(json_in::varchar)::varchar||chr(10)||chr(10)||json_in::varchar);
	return xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION proceso_respuesta_sii_16104(varchar) RETURNS varchar AS $$
DECLARE
        xml1   alias for $1;
        xml2 varchar;
	resp1	varchar;
	json_out	json;
	j1	json;
	n1	varchar;
	j2	json;
	j3	json;
	j4	json;
	xml3	varchar;
	aux	varchar;
	lista1	json;
	i	integer;
	json_par1	json;
	json_curl	json;
	evento1		varchar;
	lista2	json;
	jaux	json;
	l	integer;
	trackid	varchar;
	nombre_tabla1	varchar;
	cola1	varchar;	
	query1	varchar;
	tx1	varchar;
BEGIN
	xml2:=xml1;
	xml2:=verifica_resp_sii_8030(xml2);
	if(get_campo('__SECUENCIAOK__',xml2)='60') then
		xml2 := put_campo(xml2,'RESPUESTA','Status: 200 OK');
	end if;
	
	xml2 :=put_campo(xml2,'__SECUENCIAOK__','1000');	
	return xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION insert_cola_estado_sii_rec_16104(varchar,varchar,varchar,varchar,varchar,varchar,varchar) RETURNS varchar AS $$
DECLARE
	rut_emisor1	alias for $1;
	tipo_dte1	alias for $2;
	folio1		alias for $3;
	rut_rec1	alias for $4;
	fecha_emi1	alias for $5;
	monto1		alias for $6;
	uri1		alias for $7;
	xml3	varchar;
	tx1	varchar;
	nombre_tabla1	varchar;
	query1	varchar;
	cola1	varchar;
	id1	bigint;
BEGIN
                xml3:='';
                xml3:=put_campo(xml3,'TX','16104');
                xml3:=put_campo(xml3,'CANAL','RECIBIDOS');
                xml3:=put_campo(xml3,'RUT_EMISOR',rut_emisor1);
                xml3:=put_campo(xml3,'RUT_RECEPTOR',rut_rec1);
                xml3:=put_campo(xml3,'TIPO_DTE',tipo_dte1);
                xml3:=put_campo(xml3,'FOLIO',folio1);
                xml3:=put_campo(xml3,'MONTO_TOTAL',monto1);
                xml3:=put_campo(xml3,'FECHA_EMISION',fecha_emi1);
                xml3:=put_campo(xml3,'URI_IN',uri1);
		xml3:=put_campo(xml3,'FECHA_INGRESO_COLA',now()::varchar);
                cola1:=nextval('id_cola_sii');
                tx1:='10';
                nombre_tabla1:='cola_sii_'||cola1::varchar;
                query1:='insert into ' || nombre_tabla1 || ' (fecha,reintentos,data,tx,rut_emisor,reproceso,categoria, nombre_cola,uri) values ( now(),0,'||quote_literal(xml3)||','||tx1||','||quote_literal(rut_emisor1::varchar)||',''NO'',''ESTADO_SII'','|| quote_literal(nombre_tabla1) ||','''||uri1||''') returning id';
		execute query1 into id1;
		if id1 is not null then
                        return 'Se graba Evento para consultar estado';
		else
			return 'FALLA';
                end if;
END;
$$ LANGUAGE plpgsql;

