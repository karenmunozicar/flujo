delete from isys_querys_tx where llave='6006';

insert into isys_querys_tx values ('6006',10,1,8,'Ejecuta 6001',6001,0,0,0,0,20,20);
--insert into isys_querys_tx values ('6006',20,1,1,'select sp_procesa_respuesta_cola_motor_original(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,0,0);
insert into isys_querys_tx values ('6006',20,1,1,'select sp_respuesta_flujo_6006(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,0,0);

CREATE OR REPLACE FUNCTION sp_respuesta_flujo_6006(varchar)
 RETURNS varchar
 LANGUAGE plpgsql
AS $function$
declare
        xml1    alias for $1;
        xml2    varchar;
begin
	xml2:=xml1;
	perform logfile_icar('en 6006 con xml: '||xml2);
	if get_campo('CODIGO_RESPUESTA',xml2)='1' then
		xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
	--else
	--	xml2:=put_campo(xml2,'RESPUESTA','Status: 300 NK');
	end if;
	 perform logfile_icar('en 6006 antes de llamar a sp_procesa_respuesta_cola_motor_original con '||xml2);
	xml2:=sp_procesa_respuesta_cola_motor_original(xml2);
	perform logfile_icar('en 6006 respuesta de sp_procesa_respuesta_cola_motor_original '||xml2);
	return xml2;
end;
$function$
