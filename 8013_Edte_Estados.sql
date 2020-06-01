delete from isys_querys_tx where llave='8013';

-- Prepara llamada a Webiecv 
insert into isys_querys_tx values ('8013',20,1,1,'select proc_procesa_eventos_edte_8013(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

--Llamada a cuadratura
insert into isys_querys_tx values ('8013',30,1,2,'Llamada a Cuadratura',4005,100,101,0,0,40,40);

--Respuesta del AML
insert into isys_querys_tx values ('8013',40,1,1,'select proc_procesa_respuesta_edte_8013(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

/*
FORMULARIO DTE EMITIDO

===========================================================
Nombre		Descripcion
===========================================================
_CODEVE		Codigo del evento
_FCHEVE		Fecha del evento (dd/MM/yyyy HH:mm)
_CMTEVE		Comentarios del evento (Opcional)
_TIPDOC		Tipo del DTE
_DOCFOL		Folio del DTE
_RUTEMI		RUT del emisor del DTE
_RUTREC		RUT del receptor del DTE
_DOCMNTTOT	Monto Total del DTE (Opcional)
_DOCMNTNTO	Monto Neto del DTE (Opcional)
_DOCMNTEXE	Monto Exento del DTE (Opcional)
_DOCMNTIVA	IVA del DTE (Opcional)
_DOCREFS	Referencias del DTE (Opcional)
_DOCURI		URI del DTE (Opcional)
_DOCTRKID	TrackID del DTE (Opcional)
_DOCCODENV	Codigo de envio del DTE (Opcional)
============================================================


FORMULARIO DTE RECIBIDO

============================================================
Nombre		Descripcion
============================================================
_CODEVE		Codigo del evento
_FCHEVE		Fecha del evento (dd/MM/yyyy HH:mm)
_CMTEVE		Comentarios del evento (Opcional)
_TIPDOC		Tipo del DTE
_DOCFOL		Folio del DTE
_RUTREC		RUT del emisor del DTE
_DOREMPRUT	RUT del receptor del DTE
_DORMNTTOT	Monto Total del DTE (Opcional)
_DORMNTNTO	Monto Neto del DTE (Opcional)
_DORMNTEXE	Monto Exento del DTE (Opcional)
_DORMNTIVA	IVA del DTE (Opcional)
_DORREFS	Referencias del DTE (Opcional)
_DORURI		URI del DTE (Opcional)
_DORTRKID	TrackID del DTE (Opcional)
_DORCODENV	Codigo de envio del DTE (Opcional)
============================================================
*/

CREATE or replace FUNCTION proc_procesa_eventos_edte_8013(varchar) RETURNS varchar AS $$
DECLARE
    xml1	alias for $1;
    stDte_emi   dte_emitidos%ROWTYPE;
    stDte_rec   dte_recibidos%ROWTYPE;
    xml2	varchar;
    data1	varchar;
    sts		varchar;
    param1	varchar;
    part1	integer;
    folio1	varchar;
    tipo_dte1	varchar;
    rut_emisor1 varchar;
    estado_sii1	varchar;
    estado1	varchar;
BEGIN
    xml2:=xml1;
   /*********************************************************/
    --Va a Cuadratura sin hacer nada...
    xml2 := put_campo(xml2,'__SECUENCIAOK__','30');
    RETURN xml2;
   /*********************************************************/
    --Viene con header, se saca para que no falla
    data1:=split_part(get_campo('INPUT',xml2),chr(10)||chr(10),2);
--raise notice 'data1=%',data1;

    --Cambia el HOST para ir a Cuadratura
    --data1:=regexp_replace(data1,'\nHost: [0-9\.]+|[:alnum:]\.]+\n',chr(10)||'Host: cc-cuadindexer.acepta.com');
    --xml2:=put_campo(xml2,'INPUT',data1);

    --se parsea el mensaje desde el EDTE y se dejan en la bolsa todos los campos
    --raise notice 'data1=(%)',ltrim(data1,' ');
    part1 :=1;
    param1 := split_part(data1,'&',part1);
    --raise notice 'param1=(%)',param1;
    while param1 <> '' loop
	xml2 := put_campo(xml2,split_part(param1,'=',1),split_part(param1,'=',2));
	part1 := part1 + 1;
	param1 := split_part(data1,'&',part1);
    end loop;

    select descripcion into estado1 from estado_dte where codigo= get_campo('_CODEVE',xml2);
    if not found then
	estado1:='ESTADO_NO_REGISTRADO_'||get_campo('_CODEVE',xml2);
    end if;

    --Si el estado tiene algo de SII, lo guardo en estado_sii
    if (strpos(estado1,'SII')>0) then
	estado_sii1:=estado1;
    else
	estado_sii1:='';
    end if;
    xml2:=put_campo(xml2,'ESTADO_SII',estado_sii1);
	

    --raise notice '_RUTEMI=%',get_campo('_RUTEMI',xml2);
    --Se busca _RUTEMI para saber si es un evento de DTE emitido o Recibido
    if get_campo('_RUTEMI',xml2) = '' then
	--DATA_IN=_DORURI=http%3A%2F%2Fpruebasbinaria1310.acepta.com%2Fv01%2F8b1350775b682391c519958e805abf2719136785&_DORFOL=23&_DORMNTIVA=0&_DORMNTTOT=100000&_DORREFS=Nro.Ref%3A+1+Tipo%3A+801+Folio%3A+123+Fecha%3A+2013-10-24+Razon+Ref%3A+OC%2FHES%3A123&_DORMNTEXE=100000&_CODEVE=PUB&_RUTREC=99596430-9&_TIPDOC=34&_DOREMPRUT=93603000-9&_FCHEVE=04%2F11%2F2013+16%3A41
	xml2:=logapp(xml2,'DATA_IN='||data1);
	--raise notice 'No viene _RUTEMI';	
	--DTE Recibido
	--rut_emisor1 := split_part(get_campo('_RUTREC',xml2),'-',1); --Ojo Rut Emisor viene en _RUTREC 
        if length(get_campo('_DORFOL',xml2))>0 then
		folio1 := get_campo('_DORFOL',xml2);
		--raise notice 'Viene _DORFOL %',folio1;	
	else
        	folio1 := get_campo('_DOCFOL',xml2);
		--raise notice 'No Viene _DORFOL %',folio1;	
	end if;
	xml2:=put_campo(xml2,'RUT_EMISOR',split_part(get_campo('_RUTREC',xml2),'-',1));
	xml2:=put_campo(xml2,'RUT_RECEPTOR',split_part(get_campo('_DOREMPRUT',xml2),'-',1));
	xml2:=put_campo(xml2,'MONTO_NETO',get_campo('_DORMNTNTO',xml2));
	xml2:=put_campo(xml2,'MONTO_TOTAL',get_campo('_DORMNTTOT',xml2));

	rut_emisor1 := get_campo('RUT_EMISOR',xml2);
        tipo_dte1 := get_campo('_TIPDOC',xml2);
	xml2:=put_campo(xml2,'CANAL','RECIBIDOS');
	xml2:=put_campo(xml2,'TIPO_DTE',tipo_dte1);
	xml2:=put_campo(xml2,'FOLIO',folio1);
	--raise notice 'rut_emisor1=% tipo_dte1=% folio1=%',rut_emisor1,tipo_dte1,folio1;
	--se busca el registro para actualizar
	select * into stDte_rec from dte_recibidos where rut_emisor = rut_emisor1::integer  and tipo_dte = tipo_dte1::integer and folio = folio1::integer;
	if not found then
    		--No voy a cuadratura
		xml2 := logapp(xml2,'NO VOY A CUADRATURA no existe en dte_recibidos rut_emisor='||rut_emisor1::varchar||'tipo_dte1='||tipo_dte1::varchar||' folio1='||folio1::varchar);
	    	--xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
	    	--xml2 := put_campo(xml2,'RESPUESTA','');
		--return xml2;
		/*
		--Lo inserta en la tabla
		--raise notice 'inserta dte';
		--xml2:=insert_dte(xml2);	
		--raise notice 'inserta dte ok';
                stDte_rec.codigo_txel:=get_campo('CODIGO_TXEL',xml2)::bigint;
                stDte_rec.fecha_ingreso:=now();
                stDte_rec.tipo_dte=tipo_dte1;
                stDte_rec.folio=folio1;
                stDte_rec.fecha_emision:=to_char(now(),'YYYYMMDD');
                stDte_rec.fecha_vencimiento=to_char(now(),'YYYYMMDD');
                stDte_rec.rut_emisor:=get_campo('RUT_EMISOR',xml2);
                stDte_rec.rut_receptor:=rut_emisor1;
                stDte_rec.monto_neto:=set_to_null(get_campo('MONTO_NETO',xml2));
                stDte_rec.monto_total:=set_to_null(get_campo('MONTO_TOTAL',xml2));
		*/
	else
        	--Se actualiza estado del DTE en dte_recibidos
		update dte_recibidos set estado = estado1, fecha_ult_modificacion = now(),estado_sii=case when estado_sii1<>'' then estado_sii1 else estado_sii end where codigo_txel = stDte_rec.codigo_txel; 
	end if;
	
	--se graba en la bitacora el evento recibido
	insert into bitacora (codigo_txel,
		fecha_ingreso,
		tipo_dte,
		folio,
		fecha_emision,
		fecha_vencimiento,
		rut_emisor,
		rut_receptor,
		monto_neto,
		monto_total,
		fecha_actualizacion,
		estado,
		canal) values (
                stDte_rec.codigo_txel,
                stDte_rec.fecha_ingreso,
                stDte_rec.tipo_dte,
                stDte_rec.folio,
                stDte_rec.fecha_emision,
                stDte_rec.fecha_vencimiento,
                stDte_rec.rut_emisor,
                stDte_rec.rut_receptor,
                stDte_rec.monto_neto,
                stDte_rec.monto_total,
                current_timestamp,
		estado1,
		'RECIBIDOS');
    		xml2 := logapp(xml2,'Cuadratura ESTADO='||estado1||' Emisor='||rut_emisor1||' Canal=RECIBIDOS Folio='||folio1);
		--raise notice 'paso2';
    else
	--DTE Emitido
	rut_emisor1 := split_part(get_campo('_RUTEMI',xml2),'-',1);
        if length(get_campo('_DOCFOL',xml2))>0 then
        	folio1 := get_campo('_DOCFOL',xml2);
	else
        	folio1 := get_campo('_DORFOL',xml2);
	end if;

        tipo_dte1 := get_campo('_TIPDOC',xml2);
	--se busca el registro para actualizar
	select * into stDte_emi from dte_emitidos where rut_emisor = rut_emisor1::integer  and tipo_dte = tipo_dte1::integer and folio = folio1::integer;
	if not found then
    		--No voy a cuadratura
		xml2 := logapp(xml2,'NO DEBERIA IR A CUADRATURA no existe en dte_emitidos rut_emisor='||rut_emisor1::varchar||'tipo_dte1='||tipo_dte1::varchar||' folio1='||folio1::varchar);
	    	--xml2 := put_campo(xml2,'__SECUENCIAOK__','30');
		--return xml2;
	end if;

	--se actualiza estado en dte_emitidos
	update dte_emitidos set estado = estado1, fecha_ult_modificacion = now(),
	estado_sii = case when estado_sii not in ('RECHAZADO_POR_EL_SII','CESION_DE_DTE_RECHAZADA_POR_EL_SII','ACEPTADO_POR_EL_SII', 'ACEPTADO_CON_REPAROS_POR_EL_SII') and length(get_campo('ESTADO_SII',xml2))>0 then get_campo('ESTADO_SII',xml2) else estado_sii end
	where codigo_txel = stDte_emi.codigo_txel;

	--se graba en la bitacora el evento recibido
        insert into bitacora (codigo_txel,
                fecha_ingreso,
                tipo_dte,
                folio,
                fecha_emision,
                fecha_vencimiento,
                rut_emisor,
                rut_receptor,
                monto_neto,
                monto_total,
                fecha_actualizacion,
                estado,
                canal) values (
                stDte_emi.codigo_txel,
                stDte_emi.fecha_ingreso,
                stDte_emi.tipo_dte,
                stDte_emi.folio,
                stDte_emi.fecha_emision,
                stDte_emi.fecha_vencimiento,
                stDte_emi.rut_emisor,
                stDte_emi.rut_receptor,
                stDte_emi.monto_neto,
                stDte_emi.monto_total,
                current_timestamp,
                estado1,
                'EMITIDOS');
    		xml2 := logapp(xml2,'Cuadratura ESTADO='||estado1||' Emisor='||rut_emisor1||' Canal=EMITIDOS Folio='||folio1);

    end if;

    --Va a Cuadratura
    xml2 := put_campo(xml2,'__SECUENCIAOK__','30');

    RETURN xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION proc_procesa_respuesta_edte_8013(varchar) RETURNS varchar AS $$
DECLARE
    xml1	alias for $1;
    xml2	varchar;
    data1	varchar;
BEGIN
    xml2:=xml1;
    --data1:=get_campo('INPUT',xml2);

    --Limpio el INPUT para el LOG
    xml2 := put_campo(xml2,'INPUT','CLEAN');

    --Respondo lo que viene
    xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
	--raise notice 'JCC_xml2 =%',xml2;
    RETURN xml2;
END;
$$ LANGUAGE plpgsql;
