delete from isys_querys_tx where llave='12751';

--.--
insert into isys_querys_tx values ('12751',10,1,1,'select reprocesos_eventos_12751(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,0,0);



CREATE or replace FUNCTION reprocesos_eventos_12751(varchar) RETURNS varchar AS $$
DECLARE
        xml1            alias for $1;
        xml2            varchar;
BEGIN
        truncate table eventos_x_enviar_reprocesos_historio;
        xml2:=' ' ;
	xml2:=logapp(xml2,'Reproceso Eventos Emitidos:');
	return xml2;
	--Insertamos Emitidos
        xml2 := sp_eventos_erp_reprocesos(xml2,'EMITIDOS');
        --Insertamos Recibidos
        xml2 := sp_eventos_erp_reprocesos(xml2,'RECIBIDOS');
        --Marcamos el reproceso?.-

        return xml2;
END;
$$ LANGUAGE plpgsql;
