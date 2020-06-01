CREATE or replace FUNCTION proxy_traza_amazon_112704(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2    	varchar;
    tabla_traza1    varchar;
    uri1    varchar;
    stTraza record;
BEGIN
    	    xml2:=xml1;
	    uri1:=get_campo('URI_IN',xml2);
	    tabla_traza1:=get_campo('TABLA_TRAZA',xml2);
	    execute 'select * from '||tabla_traza1||' where uri=$1 and evento=''PUB''' into stTraza using uri1;
	    --Si no esta el evento..
	    if stTraza.uri is not null then
	    	xml2:=put_campo(xml2,'__SECUENCIAOK__','0');
		xml2 := put_campo(xml2,'__PUBLICADO_OK__','SI');
		xml2 := logapp(xml2,'Uri '||uri1||' ya publicado');
		return xml2;
	    end if;
	    xml2:=put_campo(xml2,'__SECUENCIAOK__','20');
	    return xml2;
END;
$$ LANGUAGE plpgsql;

