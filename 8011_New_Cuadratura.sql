delete from isys_querys_tx where llave='8011';

insert into isys_querys_tx values ('8011',20,1,1,'select proc_procesa_consulta_cuadratura_8011(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

CREATE or replace FUNCTION proc_procesa_consulta_cuadratura_8011(varchar) RETURNS varchar AS $$
DECLARE
	xml1	alias for $1;
	xml2	varchar;
	data1	varchar;
	output1	varchar;
	tx1	varchar;
	rut1	varchar;
	ciclo1	varchar;
	fec_desde1	varchar;
	fec_hasta1	varchar;

BEGIN
	--tipo_tx=cuadraturaBusqBas&rutEmp=98878765-3&ciclo=&fecDesde=06-08-2013&fecHasta=28-08-2013   Ejemplo DataInput
	xml2:=xml1;
    	data1	:=get_campo('INPUT',xml2);
	xml2	:=put_campo(xml2,'__SECUENCIAOK__','0');
	--xml2 := put_campo(xml2,'RESPUESTA','TEST');
	tx1	:=split_part(data1,'&',1);
	rut1	:=split_part(data1,'&',2);
	ciclo1	:=split_part(data1,'&',3);
	fec_desde1:=split_part(data1,'&',4);
	fec_hasta1:=split_part(data1,'&',5);
	
	raise notice 'JCC_8011 tx1=% - rut1=% - ciclo1=% ',tx1,rut1,ciclo1;

	output1:='<reportePeriodo>
		    <ciclo></ciclo>
		    <fecDesde>20-01-2000</fecDesde>
		    <fecHasta>17-08-2013</fecHasta>       
		    <aceptadosNac>8950</aceptadosNac>
		    <aceptadosExp>136</aceptadosExp>
		    <rechazadosNac>475</rechazadosNac>
		    <rechazadosExp>80</rechazadosExp>
		    <pendientesNac>997928</pendientesNac>
		    <pendientesExp>27</pendientesExp>
		    <emitidosBol>240121</emitidosBol>
		</reportePeriodo>';

	xml2 := put_campo(xml2,'STATUS_HTTP','200 OK');
	xml2:=put_campo(xml2,'OUTPUT',output1);
        --xml2 := responde_aml(xml2);
	--xml2:=put_campo(xml2,'STATUS_HTTP','200 OK');
    	--xml2:=responde_http_8011(xml2);
	
/*
    xml2 := put_campo(xml2,'RESPUESTA','<?xml version=''1.0'' encoding=''ISO-8859-1''?>
<busquedabasicaResponse>
    <reportePeriodo>
        <totalNacional>36038</totalNacional>
        <totalExportacion>1</totalExportacion>
        <totalBoleta>192</totalBoleta>
        <totalTodos>32231</totalTodos>
        <aceptadoNacional>22040</aceptadoNacional>
        <aceptadoExportacion> 1</aceptadoExportacion>
        <aceptadoTodos> 22041</aceptadoTodos>
        <rechazadoNacional>213</rechazadoNacional>
        <rechazadoExportacion>0</rechazadoExportacion>
        <rechazadoTodos>213</rechazadoTodos>
        <pendienteNacional>9408</pendienteNacional>
        <pendienteExportacion>0</pendienteExportacion>
        <pendienteTodos>9408</pendienteTodos>
    </reportePeriodo>
</busquedabasicaResponse>
');
*/
    RETURN xml2;
END;
$$ LANGUAGE plpgsql;
