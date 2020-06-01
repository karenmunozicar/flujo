--PIVOTE CUADRO1 EMITIDOS
CREATE or replace FUNCTION cuadro1emitidos1(json) RETURNS json AS $$
DECLARE
        json2   json;
        json1   alias for $1;
        v_fecha_inicio      integer;
        v_fecha_fin         integer;
        fecha_in1       varchar;
        json3       json;
        json4       json;
        json5       json;
        texto_ref1      varchar;
BEGIN
        json2:=json1;
        json2:=put_json(json2,'__CUADRO__','1');

        --Ingresamos las variables como las esperan las funciones
        --perform logfile('FAY json2='||json2::varchar);
        json2:=put_json(json2,'tipoFecha',get_json('TIPO_FECHA',json2));
        json2:=put_json(json2,'fstart',get_json('FSTART',json2));
        json2:=put_json(json2,'fend',get_json('FEND',json2));
        json2:=put_json(json2,'tipo',get_json('TIPO_DTE',json2));
        json2:=put_json(json2,'tipo_dte_filtro',get_json('TIPO_DTE',json2));
        json2:=put_json(json2,'rut_emisor_filtro',get_json('RUT_EMISOR',json2));
        ----------------------------------------------------------------------
        json2:=corrige_fechas(json2);

        texto_ref1:='';
        if (get_json('TIPO_REFERENCIA',json2)<>'') then
                texto_ref1:=' <b>Referencia:</b> '||(select replace(initcap(descripcion),'_',' ') from detalle_parametros where id_parametro=110 and codigo=get_json('TIPO_REFERENCIA',json2));
        end if;
        --Agregamos el Texto de Criterio de Busqueda
        json2:=put_json(json2,'criterio_busqueda','<div id=''div_criterio''><b>Desde: </b>'||substring(get_json('fstart',json2),1,4)||'-'||substring(get_json('fstart',json2),5,2)||'-'||substring(get_json('fstart',json2),7,2)
                ||' <b>Hasta: </b>'||substring(get_json('fend',json2),1,4)||'-'||substring(get_json('fend',json2),5,2)||'-'||substring(get_json('fend',json2),7,2)||' <b>Tipo Fecha: </b>'||get_json('TIPO_FECHA',json2)||' <b>Tipo Dte: </b>'||case when get_json('TIPO_DTE',json2)='*' then 'Todos Dte (No incluye Boletas)' else (select replace(initcap(descripcion),'_',' ') from tipo_dte where codigo::varchar=get_json('TIPO_DTE',json2)) end||texto_ref1||case when get_json('PARAMETRO_ADICIONAL',json2)<>'' then ' <b>Adicional:</b> '||initcap(get_json('PARAMETRO_ADICIONAL',json2)) else '' end ||'</div>');

        v_fecha_inicio:=get_json('fstart',json2)::integer;
        v_fecha_fin:=get_json('fend',json2)::integer;
        fecha_in1:=genera_in_fechas(v_fecha_inicio::integer,v_fecha_fin::integer);
        if (fecha_in1 is null) then
                json2:=response_requests_6000('2', 'Rango de Fechas Invalida','',json2);
                return json2;
        end if;

        json2:=put_json(json2,'LLAMA_FLUJO','SI');
        --if (get_json('rutUsuario',json2)='17597643') then
                json2:=put_json(json2,'__SECUENCIAOK__','14730');
        --else
        --        json2:=put_json(json2,'__SECUENCIAOK__','14710');
        --end if;
        return json2;
END;
$$ LANGUAGE plpgsql;

--PIVOTE CUADRO2 EMITIDOS
CREATE or replace FUNCTION cuadro2emitidos1(json) RETURNS json AS $$
DECLARE
        json2   json;
        json1   alias for $1;
        v_fecha_inicio      integer;
        v_fecha_fin         integer;
        fecha_in1       varchar;
        json3       json;
        json4       json;
        json5       json;
        texto_ref1      varchar;
BEGIN
        json2:=json1;
        json2:=put_json(json2,'__CUADRO__','2');

        --Ingresamos las variables como las esperan las funciones
        json2:=put_json(json2,'tipoFecha',get_json('TIPO_FECHA',json2));
        json2:=put_json(json2,'fstart',get_json('FSTART',json2));
        json2:=put_json(json2,'fend',get_json('FEND',json2));
        json2:=put_json(json2,'tipo',get_json('TIPO_DTE',json2));
        json2:=put_json(json2,'tipo_dte_filtro',get_json('TIPO_DTE',json2));
        json2:=put_json(json2,'rut_emisor_filtro',get_json('RUT_EMISOR',json2));
        json2:=put_json(json2,'grupo_tot',get_json('ejex',json2));
        json2:=put_json(json2,'evento_tot',get_json('ejey',json2));
        ----------------------------------------------------------------------
        json2:=corrige_fechas(json2);
        texto_ref1:='';
        if (get_json('TIPO_REFERENCIA',json2)<>'') then
                texto_ref1:=' <b>Referencia:</b> '||(select replace(initcap(descripcion),'_',' ') from detalle_parametros where id_parametro=110 and codigo=get_json('TIPO_REFERENCIA',json2));
        end if;

        --Agregamos el Texto de Criterio de Busqueda
        json2:=put_json(json2,'criterio_busqueda','<div id=''div_criterio''><b>Desde: </b>'||substring(get_json('fstart',json2),1,4)||'-'||substring(get_json('fstart',json2),5,2)||'-'||substring(get_json('fstart',json2),7,2)
                ||' <b>Hasta: </b>'||substring(get_json('fend',json2),1,4)||'-'||substring(get_json('fend',json2),5,2)||'-'||substring(get_json('fend',json2),7,2)||' <b>Tipo Fecha: </b>'||get_json('TIPO_FECHA',json2)||' <b>Tipo Dte: </b>'||case when get_json('TIPO_DTE',json2)='*' then 'Todos Dte (No incluye Boletas)' else (select replace(initcap(descripcion),'_',' ') from tipo_dte where codigo::varchar=get_json('TIPO_DTE',json2)) end||' '||' <b>Estado: </b>'||get_json('ejey',json2)||' <b>Grupo: </b>'||initcap(replace(get_json('ejex',json2),'_',' '))||texto_ref1||case when get_json('PARAMETRO_ADICIONAL',json2)<>'' then ' <b>Adicional:</b> '||initcap(get_json('PARAMETRO_ADICIONAL',json2)) else '' end ||'</div>');



        v_fecha_inicio:=get_json('fstart',json2)::integer;
        v_fecha_fin:=get_json('fend',json2)::integer;
        fecha_in1:=genera_in_fechas(v_fecha_inicio::integer,v_fecha_fin::integer);
        if (fecha_in1 is null) then
                json2:=response_requests_6000('2', 'Rango de Fechas Invalida','',json2);
                return json2;
        end if;
        json2:=put_json(json2,'LLAMA_FLUJO','SI');
        --if (get_json('rutUsuario',json2)='17597643') then
                json2:=put_json(json2,'__SECUENCIAOK__','14730');
        --else
        --        json2:=put_json(json2,'__SECUENCIAOK__','14710');
        --end if;
        return json2;

END;
$$ LANGUAGE plpgsql;

delete from isys_querys_tx where llave='14730';
delete from isys_querys_tx where llave='14731';
delete from isys_querys_tx where llave='14732';
delete from isys_querys_tx where llave='14733';

--Consultamos en la base de traza si el DTE ya esta publicado
insert into isys_querys_tx values ('14730',5,9,1,'select pivote_14730(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('14730',10,1,9,'<EJECUTA0>14731</EJECUTA0><EJECUTA1>14732</EJECUTA1><EJECUTA2>14733</EJECUTA2><TIMEOUT>15</TIMEOUT>',0,0,0,1,1,20,20);

insert into isys_querys_tx values ('14730',12,1,8,'Llamada Flujo 14731',14731,0,0,0,0,20,20);
insert into isys_querys_tx values ('14730',14,1,8,'Llamada Flujo 14732',14732,0,0,0,0,20,20);
insert into isys_querys_tx values ('14730',16,1,8,'Llamada Flujo 14733',14733,0,0,0,0,20,20);

--Pivote para armar respuesta
insert into isys_querys_tx values ('14730',20,9,1,'select valida_respuesta_14730(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);
--FUNCION valida_respuesta_cuadro2_14730
--FUNCION valida_respuesta_cuadro1_excepciones_14730
--FUNCION select_suma_emision_actual_14730 
--Voy a la base de los emitidos Redshift
insert into isys_querys_tx values ('14731',10,9,1,'select select_cuadro_1_emitidos_redshift_14731(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);
--Cuenta Emiutidos Redshift Tipo9 = Salida json
insert into isys_querys_tx values ('14731','20',23,1,'$$QUERY_RS$$',0,0,0,9,1,30,30);
insert into isys_querys_tx values ('14731','30',9,1,'select respuesta_cuadro_1_emitidos_redshift_14731(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

--Voy a la base de los emitidos Redshift
insert into isys_querys_tx values ('14732',10,9,1,'select select_cuadro_1_boletas_redshift_14732(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('14732','20',35,1,'$$QUERY_RS$$',0,0,0,9,1,30,30);
insert into isys_querys_tx values ('14732','30',9,1,'select respuesta_cuadro_1_boletas_redshift_14732(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

--Voy a la base de las excepciones Redshift
insert into isys_querys_tx values ('14733',10,9,1,'select select_cuadro_1_excepciones_redshift_14733(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('14733','20',22,1,'$$QUERY_RS$$',0,0,0,9,1,30,30);
insert into isys_querys_tx values ('14733','30',9,1,'select respuesta_cuadro_1_excepciones_redshift_14733(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

CREATE or replace FUNCTION pivote_14730(json) RETURNS json AS $$
declare
        json2   json;
        json1   alias for $1;
	grupo_tot1	varchar;
	v_fecha_inicio	integer;
	v_fecha_fin	integer;
begin
	json2:=json1;	
        v_fecha_inicio:=get_json('fstart',json2)::integer;
        v_fecha_fin:=get_json('fend',json2)::integer;
	--Si no tengo que ir al Redshift, solo el dia de hoy
	if (v_fecha_inicio>=to_char(now(),'YYYYMMDD')::integer and v_fecha_fin>=to_char(now(),'YYYYMMDD')::integer and get_json('TIPO_FECHA',json2)<>'Emision') then
		if(get_json('__CUADRO__',json2)='1') then
			--Limpiamos para el cuadro 1, el evento_tot y grupo_tot para que no altere
			json2 := put_json(json2,'evento_tot','');
			json2 := put_json(json2,'grupo_tot','');
		end if;
		--No hay resultado anterior
		json2:=put_json(json2,'EMITIDOS','[]');
		json2:=put_json(json2,'BOLETAS','[]');
		json2:=put_json(json2,'EXCEPCIONES','[]');
		json2:=valida_respuesta_14730(json2);
		return json2;
	end if;

	if(get_json('__CUADRO__',json2)='1') then
		--Limpiamos para el cuadro 1, el evento_tot y grupo_tot para que no altere
		json2 := put_json(json2,'evento_tot','');
		json2 := put_json(json2,'grupo_tot','');
		--Vamos al multihilo
		--json2 := put_json(json2,'MULTI_PROCESO','<EJECUTA0>14731</EJECUTA0><EJECUTA1>14732</EJECUTA1><EJECUTA2>14733</EJECUTA2><TIMEOUT>15</TIMEOUT>');
		json2 := put_json(json2,'__SECUENCIAOK__','10');	
		return json2;
	else
		grupo_tot1:=get_json('grupo_tot',json2);
	
		if (get_json('evento_tot',json2) in ('Error de Esquema','Folios Duplicados','En Espera','Documentos Reprocesados','Documentos Repetidos','Rechazados por Acepta')) then
			--json2:=select_cuadro_1_excepciones_redshift_14733(json2);	
			json2 := put_json(json2,'__SECUENCIAOK__','16');
			return json2;
        	elsif (grupo_tot1 in ('documentos_nacionales','documentos_exportacion')) then
			--json2:=select_cuadro_1_emitidos_redshift_14731(json2);
			json2 := put_json(json2,'__SECUENCIAOK__','12');
			return json2;
        	elsif (grupo_tot1='documentos_boletas') then
			--json2:=select_cuadro_1_boletas_redshift_14732(json2);
			json2 := put_json(json2,'__SECUENCIAOK__','14');
			return json2;
		else
			--json2:=select_cuadro_1_emitidos_redshift_14731(json2);
			json2 := put_json(json2,'__SECUENCIAOK__','12');
			return json2;
		end if;
	end if;
/*	
        if (grupo_tot1 in ('documentos_nacionales','documentos_exportacion')) then
		json2:=select_cuadro_1_emitidos_redshift_14731(json2);
		json2 := put_json(json2,'__SECUENCIAOK__','20');
		return json2;
        elsif (grupo_tot1='documentos_boletas') then
		json2:=select_cuadro_1_boletas_redshift_14732(json2);
		json2 := put_json(json2,'__SECUENCIAOK__','20');
		return json2;
        end if;
	json2 := put_json(json2,'__SECUENCIAOK__','0');
	return json2;
*/
end;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION valida_respuesta_cuadro2_14730(json) RETURNS json AS $$
DECLARE
        json2   json;
        json1   alias for $1;
        lista_emi       json;
        lista_bol       json;
	json_resp	json;
	json5		json;
	json_resp1	json;
	json_resp2	json;
	json_resp3	json;
	json_resp4	json;
	json_resp5	json;
	json_resp6	json;
        json_aux1       json;
        json_pend       json;
        tipo_dte1       varchar;
        texto1          varchar;
        select_1        varchar;
        json3   json;
        json4   json;
        v_fecha_inicio  integer;
        v_fecha_fin     integer;
        i       integer;
        aux     varchar;
	grupo_tot1	varchar;
	evento_tot1	varchar;
	campo	record;
	estado1	varchar;
BEGIN
        json2:=json1;
        json2 := put_json(json2,'__SECUENCIAOK__','0');
	grupo_tot1:=get_json('grupo_tot',json2);
	evento_tot1:=get_json('evento_tot',json2);

        json2:=corrige_fechas(json2);
        v_fecha_inicio:=get_json('fstart',json2)::integer;
        v_fecha_fin:=get_json('fend',json2)::integer;


	begin
		lista_emi:=get_json('EMITIDOS',json2);
	exception when others then
		begin
			lista_emi:=get_json('EXCEPCIONES',json2);
		exception when others then
			lista_emi:='[]';
		end;
	end;
	begin
		lista_bol:=get_json('BOLETAS',json2);	
	exception when others then
		lista_bol:='[]';
	end;
	

        --Verificamos si debo contar los emitidos del dia de hoy que no estan en el redshift
        if (get_json('TIPO_FECHA',json2)='Emision' or (get_json('TIPO_FECHA',json2)='Recepcion' and v_fecha_fin>=to_char(now(),'YYYYMMDD')::integer)) then
		if (grupo_tot1<>'documentos_boletas') then
                	json5:=select_suma_emision_actual_14730(json2);
        	        if (json5 is not null) then
                	        --Para las excepciones, sume diferente
                        	if (get_json('tipo_tx',json2)='select_cuadro_1_excepciones') then
                                	lista_emi:=suma_json2(lista_emi,json5,'["count"]','["tipo_dte","estado"]');
	                        else
        	                        lista_emi:=suma_json2(lista_emi,json5,'["count"]','["tipo_dte","estado_sii","estado_inter","estado_nar","estado_reclamo"]');
                	        end if;
	                end if;
		else
	                json2:=put_json(json2,'TIPO_SUMA','BOLETAS');
        	        json5:=select_suma_emision_actual_14730(json2);
	                if (json5 is not null) then
        	                lista_bol:=suma_json2(lista_bol,json5,'["count"]','["tipo_dte"]');
                	end if;
		end if;
        end if;


        --Juntar los json
	if (grupo_tot1='documentos_nacionales') then
		json_resp:='{"estado":"","factura_electronica__link__33":"0","factura_exenta__link__34":"0","liquidacion_factura__link__43":"0","factura_de_compra__link__46":"0","guias_despacho__link__52":"0","nota_debito__link__56":"0","nota_credito__link__61":"0","total":"0"}';
	elsif (grupo_tot1='documentos_exportacion') then
		json_resp:='{"estado":"","factura_exportacion__link__110":"0","nota_debito_exportacion__link__111":"0","nota_credito_exportacion__link__112":"0","total":"0"}';
	elsif (grupo_tot1='documentos_boletas') then
		json_resp:='{"estado":"","boleta_electronica__link__39":"0","boleta_exenta__link__41":"0","total":"0"}';
	end if;

	if (evento_tot1='Emitidos') then
		json_resp:=put_json(json_resp,'estado','Emitidos__info__EMITIDO');
	elsif (evento_tot1='Aceptados') then
		json_resp:=put_json(json_resp,'estado','Aprobado SII__info__ACEPTADO_POR_EL_SII');
		json_resp1:=put_json(json_resp,'estado','Aprobado SII con Reparos__info__ACEPTADO_CON_REPAROS_POR_EL_SII');
	elsif (evento_tot1='Rechazados') then
		json_resp:=put_json(json_resp,'estado','Rechazado SII__info__RECHAZADO_POR_EL_SII');
		json_resp1:=put_json(json_resp,'estado','Rechazados por Recepcion de Correo__info__RECHAZADO_POR_SERVIDOR_CORREO');
		json_resp2:=put_json(json_resp,'estado','Rechazados por Notificacion Comercial__info__RECHAZADO_CON_NOTIFICACION_COMERCIAL');
		json_resp3:=put_json(json_resp,'estado','Rechazados por Notificacion Tecnica__info__RECHAZADO_CON_NOTIFICACION_TECNICA');
		json_resp4:=put_json(json_resp,'estado','Reclamados por Contenido__info__RECHAZO_DE_CONTENIDO_DE_DOCUMENTO');
		json_resp5:=put_json(json_resp,'estado','Reclamados por Mercaderia (Total)__info__RECLAMO_FALTA_TOTAL_DE_MERCADERIA');
		json_resp6:=put_json(json_resp,'estado','Reclamados por Mercaderia (Parcial)__info__RECLAMO_FALTA_PARCIAL_DE_MERCADERIA');
	elsif (evento_tot1='Pendientes') then
		json_resp:=put_json(json_resp,'estado','Pendiente Intercambio__info__PENDIENTE_INTER');
		json_resp1:=put_json(json_resp,'estado','Pendientes SII__info__PENDIENTE_SII');
	elsif (evento_tot1='En Espera') then
		json_resp:=put_json(json_resp,'estado','En Espera__info__DTE_EN_ESPERA');
		--DAO Agregamos NOTA_CREDITO_ESPERA_REFERENCIA
		json_resp1:=put_json(json_resp,'estado','Notas en Espera__info__NOTA_CREDITO_ESPERA_REFERENCIA');
	elsif (evento_tot1='Folios Duplicados') then
		json_resp:=put_json(json_resp,'estado','Folios Duplicados__info__DTE_YA_ACEPTADO_SII');
	elsif (evento_tot1='Documentos Reprocesados') then
		json_resp:=put_json(json_resp,'estado','Documentos Reprocesados__info__REPROCESA_DTE_RECHAZADA_SII');
	elsif (evento_tot1='Documentos Repetidos') then
		json_resp:=put_json(json_resp,'estado','Documentos Repetidos__info__DTE_REPETIDO');
	elsif (evento_tot1='Error de Esquema') then
		json_resp:=put_json(json_resp,'estado','Error de Esquema__info__FALLA_ESQUEMA_DTE');
	elsif (evento_tot1='Rechazados por Acepta') then
		--DAO Agregamos los Documentos Rechazados por ACEPTA
		json_resp:=put_json(json_resp,'estado','DTE Sin Referencia__info__NOTA_CREDITO_SIN_REFERENCIA');
		json_resp1:=put_json(json_resp,'estado','DTE Referencia DTE Rechazado__info__NOTA_CREDITO_REFERENCIA_RECHAZADA');
	end if;		

	if (grupo_tot1<>'documentos_boletas') then
	        i:=0;
        	json3:=lista_emi;
	        aux:=get_json_index(json3,i);
        	while (aux<>'') loop
	                json_aux1:=aux::json;
			tipo_dte1:=get_json('tipo_dte',json_aux1);
			estado1:=get_json('estado',json_aux1);
			if (grupo_tot1='documentos_exportacion') then
				--Leo el tipo de dte
				select * into campo from tipo_dte where codigo=tipo_dte1::integer and codigo in (110,111,112);
			else
				select * into campo from tipo_dte where codigo=tipo_dte1::integer and codigo not in (110,111,112);
			end if;
			if not found then
				--json2:=logjson(json2,'No deberia ocurrir tipo_dte no definido en tipo_dte '||tipo_dte1);
				--continue;
        	        	i:=i+1;
	                	aux:=get_json_index(json3,i);
				continue;
			end if;
			texto1:=campo.descripcion||'__link__'||tipo_dte1;
			if (evento_tot1='Emitidos') then
				json_resp:=put_json(json_resp,texto1,(get_json(texto1,json_resp)::bigint+get_json('count',json_aux1)::bigint)::varchar);
				json_resp:=put_json(json_resp,'total',(get_json('total',json_resp)::bigint+get_json('count',json_aux1)::bigint)::varchar);
			elsif (evento_tot1='Aceptados') then
				if(get_json('estado_sii',json_aux1) in ('ACEPTADO_CON_REPAROS_POR_EL_SII')) then
					json_resp1:=put_json(json_resp1,texto1,(get_json(texto1,json_resp1)::integer+get_json('count',json_aux1)::bigint)::varchar);
					json_resp1:=put_json(json_resp1,'total',(get_json('total',json_resp1)::integer+get_json('count',json_aux1)::bigint)::varchar);
				end if;
				if (get_json('estado_sii',json_aux1) in ('ACEPTADO_POR_EL_SII')) then
					json_resp:=put_json(json_resp,texto1,(get_json(texto1,json_resp)::bigint+get_json('count',json_aux1)::bigint)::varchar);
					json_resp:=put_json(json_resp,'total',(get_json('total',json_resp)::bigint+get_json('count',json_aux1)::bigint)::varchar);
				end if;
			elsif (evento_tot1='Rechazados') then
				if(get_json('estado_sii',json_aux1) in ('RECHAZADO_POR_EL_SII'))  then
					json_resp:=put_json(json_resp,texto1,(get_json(texto1,json_resp)::bigint+get_json('count',json_aux1)::bigint)::varchar);
					json_resp:=put_json(json_resp,'total',(get_json('total',json_resp)::bigint+get_json('count',json_aux1)::bigint)::varchar);
				end if;
				if (get_json('estado_inter',json_aux1) in ('RECHAZADO_CON_NOTIFICACION_TECNICA'))  then
					json_resp3:=put_json(json_resp3,texto1,(get_json(texto1,json_resp3)::integer+get_json('count',json_aux1)::bigint)::varchar);
					json_resp3:=put_json(json_resp3,'total',(get_json('total',json_resp3)::integer+get_json('count',json_aux1)::bigint)::varchar);
				end if;
				if (get_json('estado_inter',json_aux1) in ('RECHAZADO_POR_SERVIDOR_CORREO'))  then
					json_resp1:=put_json(json_resp1,texto1,(get_json(texto1,json_resp1)::integer+get_json('count',json_aux1)::bigint)::varchar);
					json_resp1:=put_json(json_resp1,'total',(get_json('total',json_resp1)::integer+get_json('count',json_aux1)::bigint)::varchar);
				end if;
				if (get_json('estado_inter',json_aux1) in ('RECHAZADO_CON_NOTIFICACION_COMERCIAL'))  then
					json_resp2:=put_json(json_resp2,texto1,(get_json(texto1,json_resp2)::integer+get_json('count',json_aux1)::bigint)::varchar);
					json_resp2:=put_json(json_resp2,'total',(get_json('total',json_resp2)::integer+get_json('count',json_aux1)::bigint)::varchar);
				end if;
				if (get_json('estado_nar',json_aux1) in ('RECHAZO_DE_CONTENIDO_DE_DOCUMENTO'))  then
					json_resp4:=put_json(json_resp4,texto1,(get_json(texto1,json_resp4)::integer+get_json('count',json_aux1)::bigint)::varchar);
					json_resp4:=put_json(json_resp4,'total',(get_json('total',json_resp4)::integer+get_json('count',json_aux1)::bigint)::varchar);
				end if;
				if (get_json('estado_reclamo',json_aux1) in ('RECLAMO_FALTA_TOTAL_DE_MERCADERIA'))  then
					json_resp5:=put_json(json_resp5,texto1,(get_json(texto1,json_resp5)::integer+get_json('count',json_aux1)::bigint)::varchar);
					json_resp5:=put_json(json_resp5,'total',(get_json('total',json_resp5)::integer+get_json('count',json_aux1)::bigint)::varchar);
				end if;
				if (get_json('estado_reclamo',json_aux1) in ('RECLAMO_FALTA_PARCIAL_DE_MERCADERIA'))  then
					json_resp6:=put_json(json_resp6,texto1,(get_json(texto1,json_resp6)::integer+get_json('count',json_aux1)::bigint)::varchar);
					json_resp6:=put_json(json_resp6,'total',(get_json('total',json_resp6)::integer+get_json('count',json_aux1)::bigint)::varchar);
				end if;
			elsif (tipo_dte1 not in ('110','111','112','39','41') and evento_tot1='Pendientes') then
				if (get_json('estado_inter',json_aux1) in ('ENVIADO_POR_INTERCAMBIO','ENTREGA_DE_DTE_POR_INTERCAMBIO_EXITOSA')) then
					json_resp:=put_json(json_resp,texto1,(get_json(texto1,json_resp)::bigint+get_json('count',json_aux1)::bigint)::varchar);
					json_resp:=put_json(json_resp,'total',(get_json('total',json_resp)::bigint+get_json('count',json_aux1)::bigint)::varchar);
				end if;
				if (get_json('estado_sii',json_aux1) in ('PROCESADO_POR_EL_SII','ENVIADO_AL_SII','')) then
					json_resp1:=put_json(json_resp1,texto1,(get_json(texto1,json_resp1)::integer+get_json('count',json_aux1)::bigint)::varchar);
					json_resp1:=put_json(json_resp1,'total',(get_json('total',json_resp1)::integer+get_json('count',json_aux1)::bigint)::varchar);
				end if;
			--DAO Agregamos los Documentos Rechazados por ACEPTA
			elsif (evento_tot1='Rechazados por Acepta' and estado1 in ('NOTA_CREDITO_SIN_REFERENCIA','NOTA_CREDITO_REFERENCIA_RECHAZADA')) then
				if(estado1 in ('NOTA_CREDITO_SIN_REFERENCIA')) then
					json_resp:=put_json(json_resp,texto1,(get_json(texto1,json_resp)::bigint+get_json('count',json_aux1)::bigint)::varchar);
					json_resp:=put_json(json_resp,'total',(get_json('total',json_resp)::bigint+get_json('count',json_aux1)::bigint)::varchar);
				else
					json_resp1:=put_json(json_resp1,texto1,(get_json(texto1,json_resp1)::bigint+get_json('count',json_aux1)::bigint)::varchar);
					json_resp1:=put_json(json_resp1,'total',(get_json('total',json_resp1)::bigint+get_json('count',json_aux1)::bigint)::varchar);
				end if;
			elsif (evento_tot1='En Espera' and estado1 in ('DTE_EN_ESPERA','NOTA_CREDITO_ESPERA_REFERENCIA')) then
			--elsif (evento_tot1='En Espera' and estado1 in ('DTE_EN_ESPERA')) then
				--DAO Agregamos NOTA_CREDITO_ESPERA_REFERENCIA
				if(estado1 in ('DTE_EN_ESPERA')) then
					json_resp:=put_json(json_resp,texto1,(get_json(texto1,json_resp)::bigint+get_json('count',json_aux1)::bigint)::varchar);
					json_resp:=put_json(json_resp,'total',(get_json('total',json_resp)::bigint+get_json('count',json_aux1)::bigint)::varchar);
				/*elsif(estado1 in ('NOTA_CREDITO_SIN_REFERENCIA')) then
					json_resp2:=put_json(json_resp2,texto1,(get_json(texto1,json_resp2)::bigint+get_json('count',json_aux1)::bigint)::varchar);
					json_resp2:=put_json(json_resp2,'total',(get_json('total',json_resp2)::bigint+get_json('count',json_aux1)::bigint)::varchar);
				*/
				else
					json_resp1:=put_json(json_resp1,texto1,(get_json(texto1,json_resp1)::bigint+get_json('count',json_aux1)::bigint)::varchar);
					json_resp1:=put_json(json_resp1,'total',(get_json('total',json_resp1)::bigint+get_json('count',json_aux1)::bigint)::varchar);
				end if;

			elsif (evento_tot1='Folios Duplicados' and estado1 in ('DTE_YA_ACEPTADO_SII')) then
					json_resp:=put_json(json_resp,texto1,(get_json(texto1,json_resp)::bigint+get_json('count',json_aux1)::bigint)::varchar);
					json_resp:=put_json(json_resp,'total',(get_json('total',json_resp)::bigint+get_json('count',json_aux1)::bigint)::varchar);
			elsif (evento_tot1='Documentos Reprocesados' and estado1 in ('REPROCESA_DTE_RECHAZADA_SII')) then
					json_resp:=put_json(json_resp,texto1,(get_json(texto1,json_resp)::bigint+get_json('count',json_aux1)::bigint)::varchar);
					json_resp:=put_json(json_resp,'total',(get_json('total',json_resp)::bigint+get_json('count',json_aux1)::bigint)::varchar);
			elsif (evento_tot1='Documentos Repetidos' and estado1 in ('DTE_REPETIDO')) then
					json_resp:=put_json(json_resp,texto1,(get_json(texto1,json_resp)::bigint+get_json('count',json_aux1)::bigint)::varchar);
					json_resp:=put_json(json_resp,'total',(get_json('total',json_resp)::bigint+get_json('count',json_aux1)::bigint)::varchar);
			elsif (evento_tot1='Error de Esquema' and estado1 in ('FALLA_ESQUEMA_DTE')) then
					json_resp:=put_json(json_resp,texto1,(get_json(texto1,json_resp)::bigint+get_json('count',json_aux1)::bigint)::varchar);
					json_resp:=put_json(json_resp,'total',(get_json('total',json_resp)::bigint+get_json('count',json_aux1)::bigint)::varchar);
			end if;
	
        	        i:=i+1;
                	aux:=get_json_index(json3,i);
	        end loop;
	else
	        json3:=lista_bol;
        	i:=0;
	        aux:=get_json_index(json3,i);
		--raise notice 'json3=%',json3;
        	while (aux<>'') loop
	                json_aux1:=aux::json;
			tipo_dte1:=get_json('tipo_dte',json_aux1);
			--raise notice 'tipo_dte1=%',tipo_dte1;
			--Leo el tipo de dte
			select * into campo from tipo_dte where codigo=tipo_dte1::integer;
			if not found then
				raise notice 'No deberia ocurrir tipo_dte no definido en tipo_dte %',tipo_dte1;
				json2:=logjson(json2,'No deberia ocurrir tipo_dte no definido en tipo_dte '||tipo_dte1);
	                	i:=i+1;
	        	        aux:=get_json_index(json3,i);
				continue;
			end if;
			--raise notice 'json_resp=%',json_resp;
			texto1:=campo.descripcion||'__link__'||tipo_dte1;
			--raise notice 'texto1=%',texto1;
			json_resp:=put_json(json_resp,texto1,(get_json(texto1,json_resp)::bigint+get_json('count',json_aux1)::bigint)::varchar);
			json_resp:=put_json(json_resp,'total',(get_json('total',json_resp)::bigint+get_json('count',json_aux1)::bigint)::varchar);
	                i:=i+1;
        	        aux:=get_json_index(json3,i);
	        end loop	;
	end if;

	select_1:='['||json_resp::varchar;
	--Armar la lista de salida
	if (json_resp1 is not null) then
		select_1:=select_1||','||json_resp1::varchar;
	end if; 
	if (json_resp2 is not null) then
		select_1:=select_1||','||json_resp2::varchar;
	end if; 
	if (json_resp3 is not null) then
		select_1:=select_1||','||json_resp3::varchar;
	end if; 
	if (json_resp4 is not null) then
		select_1:=select_1||','||json_resp4::varchar;
	end if; 
	if (json_resp5 is not null) then
		select_1:=select_1||','||json_resp5::varchar;
	end if; 
	if (json_resp6 is not null) then
		select_1:=select_1||','||json_resp6::varchar;
	end if; 
	select_1:=select_1||']';

        json4:='[]';
        json5:='{}';
        json5:=put_json(json5,'criterio_busqueda',get_json('criterio_busqueda',json2));
        json5:=put_json(json5,'id','Cuadro2');
        json5:=put_json(json5,'tipo','2');
        json5:=put_json(json5,'data',select_1::varchar);
        json5:=put_json(json5,'uri',coalesce((select replace(remplaza_tags_6000(href,json2),'NO_BUSCAR','') from menu_info_10k where id2='buscarNEW_emitidos'),''));
        json5:=put_json(json5,'uri_ant',coalesce((select remplaza_tags_6000(href,json2) from menu_info_10k where id2='cuadro1emitidos'),''));
        json5:=put_json(json5,'criterio_busqueda',get_json('criterio_busqueda',json2));
        json4:=put_json_list(json4,json5);
        return response_requests_6000('1', '', json4::varchar, json2);

--        json2:=response_requests_6000('1', '', select_1::Varchar, json2);
--        RETURN json2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION valida_respuesta_cuadro1_excepciones_14730(json) RETURNS json AS $$
DECLARE
	json2	json;
	json1	alias for $1;
    	json_emi	json;
    	json_bol	json;
	lista_emi	json;
	lista_bol	json;
	json_ace	json;
	json_rec	json;
	json_rech_ac	json;
	json_aux1	json;
	json_pend	json;
	json_esquema	json;
	json5		json;
	tipo_dte1	varchar;
	texto1		varchar;
	select_1	varchar;
	json3	json;
	v_fecha_inicio	integer;
	v_fecha_fin	integer;
	i	integer;
	aux	varchar;
	estado1	varchar;
BEGIN
	json2:=json1;
	json2 := put_json(json2,'__SECUENCIAOK__','0');

	begin
		lista_emi:=get_json('EXCEPCIONES',json2);
	exception when others then
		lista_emi:='[]';
	end;
	begin
		lista_bol:=get_json('BOLETAS',json2);	
	exception when others then
		lista_bol:='[]';
	end;

        json2:=corrige_fechas(json2);
        v_fecha_inicio:=get_json('fstart',json2)::integer;
        v_fecha_fin:=get_json('fend',json2)::integer;

        --Verificamos si debo contar los emitidos del dia de hoy que no estan en el redshift
        if (get_json('TIPO_FECHA',json2)='Emision' or (get_json('TIPO_FECHA',json2)='Recepcion' and v_fecha_fin>=to_char(now(),'YYYYMMDD')::integer)) then
		--perform logfile('flag_excepciones '||get_json('TIPO_SUMA',json2));
		json2:=put_json(json2,'TIPO_SUMA','');
		json2:=put_json(json2,'flag_excepciones','SI');
                json5:=select_suma_emision_actual_14730(json2);
		--perform logfile('FAY select_suma_emision_actual_14730='||json5);
                if (json5 is not null) then
                        lista_emi:=suma_json2(lista_emi,json5,'["count"]','["tipo_dte","estado"]');
                        --Para las excepciones, sume diferente
                        --if (get_json('tipo_tx',json2)='select_cuadro_1_excepciones') then
                        --        lista_emi:=suma_json2(lista_emi,json5,'["count"]','["tipo_dte","estado"]');
                        --else
                        --        lista_emi:=suma_json2(lista_emi,json5,'["count"]','["tipo_dte","estado_sii","estado_inter"]');
                        --end if;
                end if;
        end if;


	--Juntar los json	
        json_emi:='{"evento":"Documentos Reprocesados","documentos_nacionales__link":"0","documentos_exportacion__link":"0","documentos_boletas__link":"0","documentos_totales":"0"}';
        json_ace:='{"evento":"Folios Duplicados","documentos_nacionales__link":"0","documentos_exportacion__link":"0","documentos_boletas__link":"-","documentos_totales":"0"}';
        json_rec:='{"evento":"En Espera","documentos_nacionales__link":"0","documentos_exportacion__link":"0","documentos_boletas__link":"-","documentos_totales":"0"}';
        json_pend:='{"evento":"Documentos Repetidos","documentos_nacionales__link":"0","documentos_exportacion__link":"0","documentos_boletas__link":"-","documentos_totales":"0"}';
	json_esquema:='{"evento":"Error de Esquema","documentos_nacionales__link":"0","documentos_exportacion__link":"0","documentos_boletas__link":"-","documentos_totales":"0"}';
	--DAO Agregamos los Documentos Rechazados por ACEPTA
        json_rech_ac:='{"evento":"Rechazados por Acepta","documentos_nacionales__link":"0","documentos_exportacion__link":"0","documentos_boletas__link":"-","documentos_totales":"0"}';
        i:=0;
        json3:=lista_emi;
        aux:=get_json_index(json3,i);
        while (aux<>'') loop
                json_aux1:=aux::json;
                tipo_dte1:=get_json('tipo_dte',json_aux1);
                if (tipo_dte1 in ('110','111','112'))then
                        texto1:='documentos_exportacion__link';
                elsif (tipo_dte1 in ('39','41'))then
                        texto1:='documentos_boletas__link';
                else
                        texto1:='documentos_nacionales__link';
                end if;

                --Emitidos son todos
		estado1:=get_json('estado',json_aux1);
		if (estado1 in ('REPROCESA_DTE_RECHAZADA_SII')) then
	                json_emi:=put_json(json_emi,texto1,(get_json(texto1,json_emi)::bigint+get_json('count',json_aux1)::bigint)::varchar);
        	        json_emi:=put_json(json_emi,'documentos_totales',(get_json('documentos_totales',json_emi)::bigint+get_json('count',json_aux1)::bigint)::varchar);
		elsif (estado1 in ('DTE_YA_ACEPTADO_SII')) then
                        json_ace:=put_json(json_ace,texto1,(get_json(texto1,json_ace)::bigint+get_json('count',json_aux1)::bigint)::varchar);
                        json_ace:=put_json(json_ace,'documentos_totales',(get_json('documentos_totales',json_ace)::bigint+get_json('count',json_aux1)::bigint)::varchar);
		--DAO, Agregamos NOTA_CREDITO_ESPERA_REFERENCIA
		elsif (estado1 in ('DTE_EN_ESPERA','NOTA_CREDITO_ESPERA_REFERENCIA')) then
		--elsif (estado1 in ('DTE_EN_ESPERA','NOTA_CREDITO_ESPERA_REFERENCIA','NOTA_CREDITO_SIN_REFERENCIA')) then
		--elsif (estado1 in ('DTE_EN_ESPERA')) then
                        json_rec:=put_json(json_rec,texto1,(get_json(texto1,json_rec)::bigint+get_json('count',json_aux1)::bigint)::varchar);
                        json_rec:=put_json(json_rec,'documentos_totales',(get_json('documentos_totales',json_rec)::bigint+get_json('count',json_aux1)::bigint)::varchar);
		elsif (estado1 in ('NOTA_CREDITO_SIN_REFERENCIA','NOTA_CREDITO_REFERENCIA_RECHAZADA')) then
                        json_rech_ac:=put_json(json_rech_ac,texto1,(get_json(texto1,json_rech_ac)::bigint+get_json('count',json_aux1)::bigint)::varchar);
                        json_rech_ac:=put_json(json_rech_ac,'documentos_totales',(get_json('documentos_totales',json_rech_ac)::bigint+get_json('count',json_aux1)::bigint)::varchar);
			
		elsif (estado1 in ('DTE_REPETIDO')) then
                        json_pend:=put_json(json_pend,texto1,(get_json(texto1,json_pend)::bigint+get_json('count',json_aux1)::bigint)::varchar);
                        json_pend:=put_json(json_pend,'documentos_totales',(get_json('documentos_totales',json_pend)::bigint+get_json('count',json_aux1)::bigint)::varchar);
		elsif (estado1 in ('FALLA_ESQUEMA_DTE')) then
                        json_esquema:=put_json(json_esquema,texto1,(get_json(texto1,json_esquema)::bigint+get_json('count',json_aux1)::bigint)::varchar);
                        json_esquema:=put_json(json_esquema,'documentos_totales',(get_json('documentos_totales',json_esquema)::bigint+get_json('count',json_aux1)::bigint)::varchar);
		end if;
                i:=i+1;
                aux:=get_json_index(json3,i);
        end loop;
        
        select_1:='['||json_emi::varchar||','||json_ace::varchar||','||json_rec::varchar||','||json_pend::varchar||','||json_esquema::varchar||','||json_rech_ac||']';

	return select_1;

END;
$$ LANGUAGE plpgsql;


--UNIMOS JSONS (HOY-EMITIDOS-BOLETAS)
CREATE or replace FUNCTION valida_respuesta_14730(json) RETURNS json AS $$
DECLARE
	json2	json;
	json1	alias for $1;
    	json_emi	json;
    	json_bol	json;
	lista_emi	json;
	lista_bol	json;
	json_ace	json;
	json_rec	json;
	json_aux1	json;
	json_pend	json;
	json4		json;
	json5		json;
	tipo_dte1	varchar;
	texto1		varchar;
	select_1	varchar;
	json3	json;
	v_fecha_inicio	integer;
	v_fecha_fin	integer;
	i	integer;
	aux	varchar;
BEGIN
	json2:=json1;
	json2 := put_json(json2,'__SECUENCIAOK__','0');

	--Cuadro 1 excepciones
	--if (get_json('tipo_tx',json2)='select_cuadro_1_excepciones') then
	--	json2:=logjson(json2,'Ejecuta valida_respuesta_cuadro1_excepciones');
	--	return valida_respuesta_cuadro1_excepciones_14730(json2);
	--Cuadro 2
	--elsif (get_json('tipo_tx',json2)<>'select_documentos_x_rut_resumen_totalizados') then
	if(get_json('__CUADRO__',json2)='2') then
		json2:=logjson(json2,'Ejecuta valida_respuesta_cuadro2_14730');
		return valida_respuesta_cuadro2_14730(json2);
	end if;

	begin
		lista_emi:=get_json('EMITIDOS',json2);
	exception when others then
		lista_emi:='[]';
	end;
	begin
		lista_bol:=get_json('BOLETAS',json2);	
	exception when others then
		lista_bol:='[]';
	end;
	--perform logfile('LISTAEMI FALLA lista_emi='||lista_emi::varchar);

        json2:=corrige_fechas(json2);
        v_fecha_inicio:=get_json('fstart',json2)::integer;
        v_fecha_fin:=get_json('fend',json2)::integer;


	
	--Verificamos si debo contar los emitidos del dia de hoy que no estan en el redshift
        if (get_json('TIPO_FECHA',json2)='Emision' or (get_json('TIPO_FECHA',json2)='Recepcion' and v_fecha_fin>=to_char(now(),'YYYMMDD')::integer)) then
                json5:=select_suma_emision_actual_14730(json2);
		--perform logfile('LISTAEMI FALLA json5='||json5::varchar);
                if (json5 is not null) then
                        --Para las excepciones, sume diferente
                        if (get_json('tipo_tx',json2)='select_cuadro_1_excepciones') then
                                lista_emi:=suma_json2(lista_emi,json5,'["count"]','["tipo_dte","estado"]');
                        else
                                lista_emi:=suma_json2(lista_emi,json5,'["count"]','["tipo_dte","estado_sii","estado_inter","estado_nar","estado_reclamo"]');
                        end if;
                end if;
		--perform logfile('LISTAEMI FALLA json5='||lista_emi::varchar);

		json2:=put_json(json2,'TIPO_SUMA','BOLETAS');
		json5:=select_suma_emision_actual_14730(json2);
		--perform logfile('BOLETA FALLA json5='||json5::varchar);
		if (json5 is not null) then
                        lista_bol:=suma_json2(lista_bol,json5,'["count"]','["tipo_dte"]');
		end if;
		--perform logfile('BOLETA FALLA SUMA lista_bol='||lista_bol::varchar);
        end if;


	--Juntar los json	
        json_emi:='{"evento":"Emitidos","documentos_nacionales__link":"0","documentos_exportacion__link":"0","documentos_boletas__link":"0","documentos_totales":"0"}';
        json_ace:='{"evento":"Aceptados","documentos_nacionales__link":"0","documentos_exportacion__link":"0","documentos_boletas__link":"-","documentos_totales":"0"}';
        json_rec:='{"evento":"Rechazados","documentos_nacionales__link":"0","documentos_exportacion__link":"0","documentos_boletas__link":"-","documentos_totales":"0"}';
        json_pend:='{"evento":"Pendientes","documentos_nacionales__link":"0","documentos_exportacion__link":"0","documentos_boletas__link":"-","documentos_totales":"0"}';
        i:=0;
        json3:=lista_emi;
        aux:=get_json_index(json3,i);
        while (aux<>'') loop
                --raise notice 'aux=%',aux;
                json_aux1:=aux::json;
                tipo_dte1:=get_json('tipo_dte',json_aux1);
                if (tipo_dte1 in ('110','111','112'))then
                        texto1:='documentos_exportacion__link';
                elsif (tipo_dte1 in ('39','41'))then
                        texto1:='documentos_boletas__link';
                else
                        texto1:='documentos_nacionales__link';
                end if;

                --Emitidos son todos
                json_emi:=put_json(json_emi,texto1,(get_json(texto1,json_emi)::bigint+get_json('count',json_aux1)::bigint)::varchar);
                json_emi:=put_json(json_emi,'documentos_totales',(get_json('documentos_totales',json_emi)::bigint+get_json('count',json_aux1)::bigint)::varchar);
                --Sumo el resultado que necesito
                if (get_json('estado_sii',json_aux1) in ('RECHAZADO_POR_EL_SII')) then
                        json_rec:=put_json(json_rec,texto1,(get_json(texto1,json_rec)::bigint+get_json('count',json_aux1)::bigint)::varchar);
                        json_rec:=put_json(json_rec,'documentos_totales',(get_json('documentos_totales',json_rec)::bigint+get_json('count',json_aux1)::bigint)::varchar);
		end if;
                if (get_json('estado_inter',json_aux1) in ('RECHAZADO_CON_NOTIFICACION_TECNICA','RECHAZADO_POR_SERVIDOR_CORREO','RECHAZADO_CON_NOTIFICACION_COMERCIAL')) then
                        json_rec:=put_json(json_rec,texto1,(get_json(texto1,json_rec)::bigint+get_json('count',json_aux1)::bigint)::varchar);
                        json_rec:=put_json(json_rec,'documentos_totales',(get_json('documentos_totales',json_rec)::bigint+get_json('count',json_aux1)::bigint)::varchar);
		end if;
                if (get_json('estado_nar',json_aux1) in ('RECHAZO_DE_CONTENIDO_DE_DOCUMENTO')) then
                        json_rec:=put_json(json_rec,texto1,(get_json(texto1,json_rec)::bigint+get_json('count',json_aux1)::bigint)::varchar);
                        json_rec:=put_json(json_rec,'documentos_totales',(get_json('documentos_totales',json_rec)::bigint+get_json('count',json_aux1)::bigint)::varchar);
		end if;
                if (get_json('estado_reclamo',json_aux1) in ('RECLAMO_FALTA_PARCIAL_DE_MERCADERIA','RECLAMO_FALTA_TOTAL_DE_MERCADERIA')  ) then
                        json_rec:=put_json(json_rec,texto1,(get_json(texto1,json_rec)::bigint+get_json('count',json_aux1)::bigint)::varchar);
                        json_rec:=put_json(json_rec,'documentos_totales',(get_json('documentos_totales',json_rec)::bigint+get_json('count',json_aux1)::bigint)::varchar);
		end if;
                if (get_json('estado_sii',json_aux1) in ('ACEPTADO_POR_EL_SII','ACEPTADO_CON_REPAROS_POR_EL_SII')) then
                        json_ace:=put_json(json_ace,texto1,(get_json(texto1,json_ace)::bigint+get_json('count',json_aux1)::bigint)::varchar);
                        json_ace:=put_json(json_ace,'documentos_totales',(get_json('documentos_totales',json_ace)::bigint+get_json('count',json_aux1)::bigint)::varchar);
		end if;
                if (tipo_dte1 not in ('110','111','112','39','41') and (get_json('estado_sii',json_aux1) in ('PROCESADO_POR_EL_SII','ENVIADO_AL_SII',''))) then
                        json_pend:=put_json(json_pend,texto1,(get_json(texto1,json_pend)::bigint+get_json('count',json_aux1)::bigint)::varchar);
                        json_pend:=put_json(json_pend,'documentos_totales',(get_json('documentos_totales',json_pend)::bigint+get_json('count',json_aux1)::bigint)::varchar);
                end if;
                if (tipo_dte1 not in ('110','111','112','39','41') and (get_json('estado_inter',json_aux1) in ('ENVIADO_POR_INTERCAMBIO','ENTREGA_DE_DTE_POR_INTERCAMBIO_EXITOSA'))) then
                        json_pend:=put_json(json_pend,texto1,(get_json(texto1,json_pend)::bigint+get_json('count',json_aux1)::bigint)::varchar);
                        json_pend:=put_json(json_pend,'documentos_totales',(get_json('documentos_totales',json_pend)::bigint+get_json('count',json_aux1)::bigint)::varchar);
                end if;
                i:=i+1;
                aux:=get_json_index(json3,i);
        end loop;
        
	json3:=lista_bol;
	i:=0;
        aux:=get_json_index(json3,i);
        while (aux<>'') loop
                --raise notice 'aux=%',aux;
                json_aux1:=aux::json;
                --tipo_dte1:=get_json('tipo_dte',json_aux1);
                texto1:='documentos_boletas__link';

                --Emitidos son todos
                json_emi:=put_json(json_emi,texto1,(get_json(texto1,json_emi)::bigint+get_json('count',json_aux1)::bigint)::varchar);
                json_emi:=put_json(json_emi,'documentos_totales',(get_json('documentos_totales',json_emi)::bigint+get_json('count',json_aux1)::bigint)::varchar);
                i:=i+1;
                aux:=get_json_index(json3,i);
	end loop;

        select_1:='['||json_emi::varchar||',{"evento":"","documentos_nacionales__link":"","documentos_exportacion__link":"","documentos_boletas__link":"","documentos_totales":""},'||json_ace::varchar||','||json_rec::varchar||','||json_pend::varchar||']';

	json4:='[]';
	json5:='{}';
        json5:=put_json(json5,'id','Cuadro1');
        json5:=put_json(json5,'tipo','1');
        json5:=put_json(json5,'data',select_1::varchar);
        json5:=put_json(json5,'uri',coalesce((select remplaza_tags_6000(href,json2) from menu_info_10k where id2='cuadro2emitidos'),''));
	json5:=put_json(json5,'criterio_busqueda',get_json('criterio_busqueda',json2));
        json4:=put_json_list(json4,json5);
        json3:=valida_respuesta_cuadro1_excepciones_14730(json2);
--        json3:=split_part(get_json('RESPUESTA',json3)::varchar,chr(10)||chr(10),2);
--        if(get_json('CODIGO_RESPUESTA',json3)='1') then
       	json5:='{}';
        json5:=put_json(json5,'id','Cuadro1');
        json5:=put_json(json5,'tipo','1');
        json5:=put_json(json5,'data',json3::varchar);
        json5:=put_json(json5,'uri',coalesce((select remplaza_tags_6000(href,json2) from menu_info_10k where id2='cuadro2emitidos'),''));
	json5:=put_json(json5,'criterio_busqueda',get_json('criterio_busqueda',json2));
        json4:=put_json_list(json4,json5);
        --end if;
        return response_requests_6000('1', '', json4::varchar, json2);

--        json2:=response_requests_6000('1', '', select_1::Varchar, json2);
--        RETURN json2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION select_suma_emision_actual_14730(json) RETURNS json AS $$
DECLARE
    json2                json;
    json1                alias for $1;
    v_rut_emisor        integer;
    v_fecha_inicio      integer;
    v_fecha_fin         integer;
    v_resultado         varchar;
        tipo_dia1       varchar;
        v_parametro_var varchar;
        aux             varchar;
        campo   record;
        v_parametro_tipo_dte    varchar;
        v_rut_usuario   integer;
        select1         varchar;
        tmp1    varchar;
        v_parametro_rut_emisor  varchar;
        query1  varchar;
        query2  varchar;
        query_parte1    varchar;
        json3   json;
        json4   json;
        json5   json;
        fecha_fin1      integer;
        aux1    varchar;
        aux2    varchar;
        json_aux1       json;
        json_aux2       json;
        json_aux3       json;
        tipoFecha1      varchar;
        i       integer;
        fecha_in1       varchar;
        rut_idx1        varchar;
        json_emi        json;
        json_ace        json;
        json_rec        json;
        json_pend       json;
        json_par1       json;
        select_1        varchar;
        texto1  varchar;
        tipo_dte1       varchar;
	dia1		varchar;
   	monto_cantidad1  varchar;
        v_parametro_referencias1        varchar;
        v_parametro_adicional1  varchar;
	tabla1		varchar;
BEGIN
        json2:=json1;

	json2:=logjson(json2,'Ejecuta select_cuadro_1_emitidos_redshift_14731');
        v_rut_emisor:=get_json('rutCliente',json2)::integer;
        v_rut_usuario:=get_json('rutUsuario',json2)::integer;

        json2:=corrige_fechas(json2);
        v_fecha_inicio:=get_json('fstart',json2)::integer;
        v_fecha_fin:=get_json('fend',json2)::integer;

        fecha_in1:=genera_in_fechas(v_fecha_inicio::integer,v_fecha_fin::integer);
        if (fecha_in1 is null) then
                json2:=response_requests_6000('2', 'Rango de Fechas Invalida','',json2);
                return json2;
        end if;

        tipo_dia1:=get_json('TIPO_FECHA',json2);
        if (tipo_dia1='Emision') then
                tipo_dia1:='E';
                tipoFecha1:='_emision ';
        else
                tipo_dia1:='A';
                tipoFecha1:=' ';
        end if;

        --Agrega parametro tipo_dte
        v_parametro_tipo_dte:='';
        aux:=get_json('tipo_dte_filtro',json2);
        --Si no es numerico
        v_parametro_tipo_dte:=obtiene_filtro_perfilamiento_usuario_6000(v_rut_emisor,v_rut_usuario,'tipo_dte',aux);
        json2:=logjson(json2,'PARAMETRO v_parametro_tipo_dte='||coalesce(v_parametro_tipo_dte,'vacio'));

        v_parametro_rut_emisor:=get_json('TAG_RUT_EMISOR',obtiene_filtro_perfilamiento_rut_emisor_6000(v_rut_emisor,v_rut_usuario,'rut_emisor',get_json('rut_emisor_filtro',json2)));

        rut_idx1:='';
        rut_idx1:=replace(v_parametro_rut_emisor,'rut_emisor',' and rut');

        --Si tiene parametros adicionales, los usamos para filtrar la query
        --parametro1='E512' and parametro2='ERP'
        v_parametro_var:=' ';
        for campo in select lower(parametro) as parametro from filtros_rut where rut_emisor=v_rut_emisor loop
                aux:=get_json(campo.parametro,json2);
                tmp1:=obtiene_filtro_perfilamiento_usuario_6000(v_rut_emisor,v_rut_usuario,upper(campo.parametro),aux);
                json2:=logjson(json2,'PARAMETRO '||campo.parametro||'='||coalesce(tmp1,'vacio')||' aux='||aux);
                /*if (length(aux)>0 and aux not in ('*','undefined')) then
                        v_parametro_var:=v_parametro_var||' and '||campo.parametro||'='||quote_literal(aux)||' ';
                end if;*/
                v_parametro_var:=v_parametro_var|| ' ' ||tmp1;
        end loop;

	if (get_json('MONTO_CANTIDAD',json2)='MONTO') then
		monto_cantidad1:=' sum(monto_total) as count ';
	else
		monto_cantidad1:=' count(*) ';
	end if;


	--Armo la busqueda para las refernecias
        v_parametro_referencias1:='';
        if (get_json('TIPO_REFERENCIA',json2)<>'*' and get_json('TIPO_REFERENCIA',json2)<>'') then
                v_parametro_referencias1:=' and strpos(referencias::varchar,''"Tipo":"'||get_json('TIPO_REFERENCIA',json2)||'"'')>0 ';
        end if;

        v_parametro_adicional1:='';
        if(get_json('PARAMETRO_ADICIONAL',json2)<>'') then
                v_parametro_adicional1:=' and strpos(data_dte,''<'||get_json('PARAMETRO_ADICIONAL',json2)||'>'')>0 ';
        end if;


	dia1:=to_char(now(),'YYYYMMDD');
	--Si tiene que sumar BOLETAS
	if (get_json('TIPO_SUMA',json2)='BOLETAS') then
        		v_parametro_referencias1:='';
	        	--query1:='select array_to_json(array_agg(row_to_json(sql))) from (select '||monto_cantidad1||',tipo_dte::varchar from dte_boletas_generica where dia='||dia1||' and '||v_parametro_rut_emisor||' '||v_parametro_tipo_dte||'  and dia'||tipoFecha1||'>='|| v_fecha_inicio::varchar||' and dia'||tipoFecha1||'<='||v_fecha_fin::varchar||' '||v_parametro_var||v_parametro_referencias1||v_parametro_adicional1||'  group by 2 order by 1) sql';
			--FAY dia1 es hoy que se guarda en la tabla de boletas diarias
	        	query1:='select array_to_json(array_agg(row_to_json(sql))) from (select '||monto_cantidad1||',tipo_dte::varchar from dte_boletas_diarias where dia='||dia1||' and '||v_parametro_rut_emisor||' '||v_parametro_tipo_dte||'  and dia'||tipoFecha1||'>='|| v_fecha_inicio::varchar||' and dia'||tipoFecha1||'<='||v_fecha_fin::varchar||' '||v_parametro_var||v_parametro_referencias1||v_parametro_adicional1||'  group by 2 order by 1) sql';
	else
		--Si es un error (cuadro1 o cuadro2) 
		--perform logfile('evento_tot='||get_json('evento_tot',json2)||' '||get_json('flag_excepciones',json2));
		if (get_json('evento_tot',json2) in ('Error de Esquema','Folios Duplicados','En Espera','Documentos Reprocesados','Documentos Repetidos') or get_json('flag_excepciones',json2)='SI') then
		        query1:='select array_to_json(array_agg(row_to_json(sql))) from ( select '||monto_cantidad1||',tipo_dte::varchar,coalesce(estado,'''') as estado from dte_emitidos_errores where dia='||dia1||' and  '||v_parametro_rut_emisor||' '||v_parametro_tipo_dte||'  and dia'||tipoFecha1||'>='|| v_fecha_inicio::varchar||' and dia'||tipoFecha1||'<='||v_fecha_fin::varchar||' '||v_parametro_var||'  group by 2,3 order by 1) sql';
			--perform logfile('cuadro errores='||query1);
		else
			--Solo sacamos de la tabla actual
			tabla1:='dte_emitidos_'||to_char(now(),'YYMM');
	        	query1:='select array_to_json(array_agg(row_to_json(sql))) from (select '||monto_cantidad1||',tipo_dte::varchar,coalesce(estado_sii,'''') as estado_sii,coalesce(estado_inter,'''') as estado_inter,coalesce(estado_nar,'''') as estado_nar,coalesce(estado_reclamo,'''') as estado_reclamo from '||tabla1||' where dia='||dia1||' and '||v_parametro_rut_emisor||' '||v_parametro_tipo_dte||'  and dia'||tipoFecha1||'>='|| v_fecha_inicio::varchar||' and dia'||tipoFecha1||'<='||v_fecha_fin::varchar||' '||v_parametro_var||v_parametro_referencias1||v_parametro_adicional1|| '  group by 2,3,4,5,6 order by 1) sql';
		end if;
	end if;
	--perform logfile('INI SUMA_ACTUAL id='||get_json('_id_fmw_',json2)||' query1='||query1);
	execute query1 into json4;
	--perform logfile('FIN SUMA_ACTUAL id='||get_json('_id_fmw_',json2));
	return json4;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION respuesta_cuadro_1_emitidos_redshift_14731(json) RETURNS json AS $$
DECLARE
    json2                json;
    json1                alias for $1;
	json3	json;
	lista1	json;
BEGIN
	json2:=json1;
	json2 := put_json(json2,'__SECUENCIAOK__','0');
	--perform logfile('CUADRO1_EMI '||get_json('RES_JSON_1',json2));
	--Si no viene RES_JSON_1 fallo la busqueda
	if (get_json('RES_JSON_1',json2)='') then
		json2:=logjson(json2,'Falla Busqueda de Emitidos en BASE_REDSHIFT_EMITIDOS.');
		json2:=put_json(json2,'EMITIDOS','[]');
		return json2;
        end if;
	json3:=get_json('RES_JSON_1',json2);
	if (get_json('STATUS',json3)<>'OK') then
		json2:=logjson(json2,'Falla Busqueda de Emitidos en BASE_REDSHIFT_EMITIDOS');
		json2:=put_json(json2,'EMITIDOS','[]');
		return json2;
        end if;
        if (get_json('TOTAL_REGISTROS',json3)='1') then
                lista1:='['||json3||']';
        else
                lista1:=get_json('LISTA',json3);
        end if;
	json2:=put_json(json2,'EMITIDOS',lista1::varchar);	
	return json2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION respuesta_cuadro_1_excepciones_redshift_14733(json) RETURNS json AS $$
DECLARE
    json2                json;
    json1                alias for $1;
        json3   json;
        lista1  json;
BEGIN
        json2:=json1;
        json2 := put_json(json2,'__SECUENCIAOK__','0');
        --perform logfile('CUADRO1_EMI '||get_json('RES_JSON_1',json2));
        json3:=get_json('RES_JSON_1',json2);
        if (get_json('STATUS',json3)<>'OK') then
                json2:=logjson(json2,'Falla Busqueda de Emitidos en BASE_REDSHIFT_EMITIDOS');
                json2:=put_json(json2,'EXCEPCIONES','[]');
                return json2;
        end if;
        if (get_json('TOTAL_REGISTROS',json3)='1') then
                lista1:='['||json3||']';
        else
                lista1:=get_json('LISTA',json3);
        end if;
        json2:=put_json(json2,'EXCEPCIONES',lista1::varchar);
        return json2;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION respuesta_cuadro_1_boletas_redshift_14732(json) RETURNS json AS $$
DECLARE
    json2                json;
    json1                alias for $1;
        json3   json;
	j3	json;
        lista1  json;
BEGIN
        json2:=json1;
        json2 := put_json(json2,'__SECUENCIAOK__','0');
        --perform logfile('CUADRO1_EMI '||get_json('RES_JSON_1',json2));
        json3:=get_json('RES_JSON_1',json2);
        if (get_json('STATUS',json3)<>'OK') then
                json2:=logjson(json2,'Falla Busqueda de Emitidos en BASE_REDSHIFT_EMITIDOS');
                json2:=put_json(json2,'BOLETAS','[]');
                return json2;
        end if;
	BEGIN
		j3:=json3::json;	
	EXCEPTION WHEN OTHERS THEN
		json2:=logjson(json2,'No existen Boletas...');
		json2:=put_json(json2,'BOLETAS','[]');
                return json2;
	END;
        if (get_json('TOTAL_REGISTROS',json3)='1') then
                lista1:='['||json3||']';
        else
                lista1:=get_json('LISTA',json3);
        end if;
        json2:=put_json(json2,'BOLETAS',lista1::varchar);
        return json2;
END;
$$ LANGUAGE plpgsql;


--CUADROS EMITIDOS
CREATE or replace FUNCTION select_cuadro_1_emitidos_redshift_14731(json) RETURNS json AS $$
DECLARE
    json2                json;
    json1                alias for $1;
    v_rut_emisor        integer;
    v_fecha_inicio      integer;
    v_fecha_fin         integer;
    v_resultado         varchar;
        tipo_dia1       varchar;
        v_parametro_var varchar;
        aux             varchar;
        campo   record;
        v_parametro_tipo_dte    varchar;
        v_rut_usuario   integer;
        select1         varchar;
        tmp1    varchar;
        v_parametro_rut_emisor  varchar;
        query1  varchar;
        query2  varchar;
        query_parte1    varchar;
        json3   json;
        json4   json;
        json5   json;
        fecha_fin1      integer;
        aux1    varchar;
        aux2    varchar;
        json_aux1       json;
        json_aux2       json;
        json_aux3       json;
        tipoFecha1      varchar;
        i       integer;
        fecha_in1       varchar;
        rut_idx1        varchar;
        json_emi        json;
        json_ace        json;
        json_rec        json;
        json_pend       json;
        json_par1       json;
        select_1        varchar;
        texto1  varchar;
        tipo_dte1       varchar;
	lista1	json;
        v_parametro_referencias1        varchar;
        v_parametro_adicional1  varchar;
BEGIN
        json2:=json1;

	json2:=logjson(json2,'Ejecuta select_cuadro_1_emitidos_redshift_14731');
        v_rut_emisor:=get_json('rutCliente',json2)::integer;
        v_rut_usuario:=get_json('rutUsuario',json2)::integer;

        json2:=corrige_fechas(json2);
        v_fecha_inicio:=get_json('fstart',json2)::integer;
        v_fecha_fin:=get_json('fend',json2)::integer;

        fecha_in1:=genera_in_fechas(v_fecha_inicio::integer,v_fecha_fin::integer);
        if (fecha_in1 is null) then
                json2:=response_requests_6000('2', 'Rango de Fechas Invalida','',json2);
                return json2;
        end if;

        tipo_dia1:=get_json('TIPO_FECHA',json2);
        if (tipo_dia1='Emision') then
                tipo_dia1:='E';
                tipoFecha1:='_emision ';
        else
                tipo_dia1:='A';
                tipoFecha1:=' ';
        end if;

        --Agrega parametro tipo_dte
        v_parametro_tipo_dte:='';
        aux:=get_json('tipo_dte_filtro',json2);
        --Si no es numerico
        v_parametro_tipo_dte:=obtiene_filtro_perfilamiento_usuario_6000(v_rut_emisor,v_rut_usuario,'tipo_dte',aux);
        json2:=logjson(json2,'PARAMETRO v_parametro_tipo_dte='||coalesce(v_parametro_tipo_dte,'vacio'));

        v_parametro_rut_emisor:=get_json('TAG_RUT_EMISOR',obtiene_filtro_perfilamiento_rut_emisor_6000(v_rut_emisor,v_rut_usuario,'rut_emisor',get_json('rut_emisor_filtro',json2)));

        rut_idx1:='';
        rut_idx1:=replace(v_parametro_rut_emisor,'rut_emisor',' and rut');

        --Si tiene parametros adicionales, los usamos para filtrar la query
        --parametro1='E512' and parametro2='ERP'
        v_parametro_var:=' ';
        for campo in select lower(parametro) as parametro from filtros_rut where rut_emisor=v_rut_emisor loop
                aux:=get_json(campo.parametro,json2);
                tmp1:=obtiene_filtro_perfilamiento_usuario_6000(v_rut_emisor,v_rut_usuario,upper(campo.parametro),aux);
                --json2:=logjson(json2,'PARAMETRO '||campo.parametro||'='||coalesce(tmp1,'vacio'));
                json2:=logjson(json2,'PARAMETRO EMI '||campo.parametro||'='||coalesce(tmp1,'vacio')||' aux='||aux);
                /*if (length(aux)>0 and aux not in ('*','undefined')) then
                        v_parametro_var:=v_parametro_var||' and '||campo.parametro||'='||quote_literal(aux)||' ';
                end if;*/
                v_parametro_var:=v_parametro_var|| ' ' ||tmp1;
        end loop;

        --Armo la busqueda para las refernecias
        v_parametro_referencias1:='';
        if (get_json('TIPO_REFERENCIA',json2)<>'*' and get_json('TIPO_REFERENCIA',json2)<>'') then
                v_parametro_referencias1:=' and strpos(referencias::varchar,''"Tipo":"'||get_json('TIPO_REFERENCIA',json2)||'"'')>0 ';
        end if;

	v_parametro_adicional1:='';
	if(get_json('PARAMETRO_ADICIONAL',json2)<>'') then
		v_parametro_adicional1:=' and strpos(data_dte,''<'||get_json('PARAMETRO_ADICIONAL',json2)||'>'')>0 ';
	end if;


        --Saco los estados del redshift
	if (get_json('MONTO_CANTIDAD',json2)='MONTO') then
		query1:='select sum(monto_total) as count,tipo_dte,estado_sii,estado_inter,estado_nar,estado_reclamo from dte_emitidos where '||v_parametro_rut_emisor||' '||v_parametro_tipo_dte||'  and dia'||tipoFecha1||'>='|| v_fecha_inicio::varchar||' and dia'||tipoFecha1||'<='||v_fecha_fin::varchar||' '||v_parametro_var||v_parametro_referencias1||v_parametro_adicional1||'  group by 2,3,4,5,6 order by 1';
	else
		query1:='select count(*),tipo_dte,estado_sii,estado_inter,estado_nar,estado_reclamo from dte_emitidos where '||v_parametro_rut_emisor||' '||v_parametro_tipo_dte||'  and dia'||tipoFecha1||'>='|| v_fecha_inicio::varchar||' and dia'||tipoFecha1||'<='||v_fecha_fin::varchar||' '||v_parametro_var||v_parametro_referencias1||v_parametro_adicional1||'  group by 2,3,4,5,6 order by 1';
	end if;

	json2:=put_json(json2,'QUERY_RS',query1);
	json2 := put_json(json2,'__SECUENCIAOK__','20');
	return json2;
END;
$$ LANGUAGE plpgsql;


--CUADROS EXCEPCIONES
CREATE or replace FUNCTION select_cuadro_1_excepciones_redshift_14733(json) RETURNS json AS $$
DECLARE
    json2                json;
    json1                alias for $1;
    v_rut_emisor        integer;
    v_fecha_inicio      integer;
    v_fecha_fin         integer;
    v_resultado         varchar;
        tipo_dia1       varchar;
        v_parametro_var varchar;
        aux             varchar;
        campo   record;
        v_parametro_tipo_dte    varchar;
        v_rut_usuario   integer;
        select1         varchar;
        tmp1    varchar;
        v_parametro_rut_emisor  varchar;
        query1  varchar;
        query2  varchar;
        query_parte1    varchar;
        json3   json;
        json4   json;
        json5   json;
        fecha_fin1      integer;
        aux1    varchar;
        aux2    varchar;
        json_aux1       json;
        json_aux2       json;
        json_aux3       json;
        tipoFecha1      varchar;
        i       integer;
        fecha_in1       varchar;
        rut_idx1        varchar;
        json_emi        json;
        json_ace        json;
        json_rec        json;
        json_pend       json;
        json_par1       json;
        select_1        varchar;
        texto1  varchar;
        tipo_dte1       varchar;
	lista1	json;
BEGIN
        json2:=json1;

	json2:=logjson(json2,'Ejecuta select_cuadro_1_emitidos_redshift_14731');
        v_rut_emisor:=get_json('rutCliente',json2)::integer;
        v_rut_usuario:=get_json('rutUsuario',json2)::integer;

        json2:=corrige_fechas(json2);
        v_fecha_inicio:=get_json('fstart',json2)::integer;
        v_fecha_fin:=get_json('fend',json2)::integer;

        fecha_in1:=genera_in_fechas(v_fecha_inicio::integer,v_fecha_fin::integer);
        if (fecha_in1 is null) then
                json2:=response_requests_6000('2', 'Rango de Fechas Invalida','',json2);
                return json2;
        end if;

        tipo_dia1:=get_json('TIPO_FECHA',json2);
        if (tipo_dia1='Emision') then
                tipo_dia1:='E';
                tipoFecha1:='_emision ';
        else
                tipo_dia1:='A';
                tipoFecha1:=' ';
        end if;

        --Agrega parametro tipo_dte
        v_parametro_tipo_dte:='';
        aux:=get_json('tipo_dte_filtro',json2);
        --Si no es numerico
        v_parametro_tipo_dte:=obtiene_filtro_perfilamiento_usuario_6000(v_rut_emisor,v_rut_usuario,'tipo_dte',aux);
        json2:=logjson(json2,'PARAMETRO v_parametro_tipo_dte='||coalesce(v_parametro_tipo_dte,'vacio'));

        v_parametro_rut_emisor:=get_json('TAG_RUT_EMISOR',obtiene_filtro_perfilamiento_rut_emisor_6000(v_rut_emisor,v_rut_usuario,'rut_emisor',get_json('rut_emisor_filtro',json2)));

        rut_idx1:='';
        rut_idx1:=replace(v_parametro_rut_emisor,'rut_emisor',' and rut');

        --Si tiene parametros adicionales, los usamos para filtrar la query
        --parametro1='E512' and parametro2='ERP'
        v_parametro_var:=' ';
        for campo in select lower(parametro) as parametro from filtros_rut where rut_emisor=v_rut_emisor loop
                aux:=get_json(campo.parametro,json2);
                tmp1:=obtiene_filtro_perfilamiento_usuario_6000(v_rut_emisor,v_rut_usuario,upper(campo.parametro),aux);
                --json2:=logjson(json2,'PARAMETRO '||campo.parametro||'='||coalesce(tmp1,'vacio'));
                json2:=logjson(json2,'PARAMETRO EMI '||campo.parametro||'='||coalesce(tmp1,'vacio')||' aux='||aux);
                /*if (length(aux)>0 and aux not in ('*','undefined')) then
                        v_parametro_var:=v_parametro_var||' and '||campo.parametro||'='||quote_literal(aux)||' ';
                end if;*/
                v_parametro_var:=v_parametro_var|| ' ' ||tmp1;
        end loop;

	if (get_json('MONTO_CANTIDAD',json2)='MONTO') then
		query1:='select sum(monto_total) as count,tipo_dte,estado from dte_emitidos_errores where '||v_parametro_rut_emisor||' '||v_parametro_tipo_dte||'  and dia'||tipoFecha1||'>='|| v_fecha_inicio::varchar||' and dia'||tipoFecha1||'<='||v_fecha_fin::varchar||' '||v_parametro_var||'  group by 2,3 order by 1';
	else
		query1:='select count(*),tipo_dte,estado from dte_emitidos_errores where '||v_parametro_rut_emisor||' '||v_parametro_tipo_dte||'  and dia'||tipoFecha1||'>='|| v_fecha_inicio::varchar||' and dia'||tipoFecha1||'<='||v_fecha_fin::varchar||' '||v_parametro_var||'  group by 2,3 order by 1';
	end if;
	json2:=put_json(json2,'QUERY_RS',query1);
	json2 := put_json(json2,'__SECUENCIAOK__','20');
	return json2;
END;
$$ LANGUAGE plpgsql;

--CUADROS BOLETAS
CREATE or replace FUNCTION select_cuadro_1_boletas_redshift_14732(json) RETURNS json AS $$
DECLARE
    json2                json;
    json1                alias for $1;
    v_rut_emisor        integer;
    v_fecha_inicio      integer;
    v_fecha_fin         integer;
    v_resultado         varchar;
        tipo_dia1       varchar;
        v_parametro_var varchar;
        aux             varchar;
        campo   record;
        v_parametro_tipo_dte    varchar;
        v_rut_usuario   integer;
        select1         varchar;
        tmp1    varchar;
        v_parametro_rut_emisor  varchar;
        query1  varchar;
        query2  varchar;
        query_parte1    varchar;
        json3   json;
        json4   json;
        json5   json;
        fecha_fin1      integer;
        aux1    varchar;
        aux2    varchar;
        json_aux1       json;
        json_aux2       json;
        json_aux3       json;
        tipoFecha1      varchar;
        i       integer;
        fecha_in1       varchar;
        rut_idx1        varchar;
        json_emi        json;
        json_ace        json;
        json_rec        json;
        json_pend       json;
        json_par1       json;
        select_1        varchar;
        texto1  varchar;
        tipo_dte1       varchar;
	lista1	json;
	v_parametro_referencias1	varchar;
	v_parametro_adicional1  varchar;
BEGIN
        json2:=json1;

	json2:=logjson(json2,'Ejecuta select_cuadro_1_boletas_redshift_14731');
        v_rut_emisor:=get_json('rutCliente',json2)::integer;
        v_rut_usuario:=get_json('rutUsuario',json2)::integer;

        json2:=corrige_fechas(json2);
        v_fecha_inicio:=get_json('fstart',json2)::integer;
        v_fecha_fin:=get_json('fend',json2)::integer;

        fecha_in1:=genera_in_fechas(v_fecha_inicio::integer,v_fecha_fin::integer);
        if (fecha_in1 is null) then
                json2:=response_requests_6000('2', 'Rango de Fechas Invalida','',json2);
                return json2;
        end if;

        tipo_dia1:=get_json('TIPO_FECHA',json2);
        if (tipo_dia1='Emision') then
                tipo_dia1:='E';
                tipoFecha1:='_emision ';
        else
                tipo_dia1:='A';
                tipoFecha1:=' ';
        end if;

        --Agrega parametro tipo_dte
        v_parametro_tipo_dte:='';
        aux:=get_json('tipo_dte_filtro',json2);
        --Si no es numerico
        v_parametro_tipo_dte:=obtiene_filtro_perfilamiento_usuario_6000(v_rut_emisor,v_rut_usuario,'tipo_dte',aux);
        json2:=logjson(json2,'PARAMETRO v_parametro_tipo_dte='||coalesce(v_parametro_tipo_dte,'vacio'));

        v_parametro_rut_emisor:=get_json('TAG_RUT_EMISOR',obtiene_filtro_perfilamiento_rut_emisor_6000(v_rut_emisor,v_rut_usuario,'rut_emisor',get_json('rut_emisor_filtro',json2)));

        rut_idx1:='';
        rut_idx1:=replace(v_parametro_rut_emisor,'rut_emisor',' and rut');

        --Si tiene parametros adicionales, los usamos para filtrar la query
        --parametro1='E512' and parametro2='ERP'
        v_parametro_var:=' ';
        for campo in select lower(parametro) as parametro from filtros_rut where rut_emisor=v_rut_emisor loop
                aux:=get_json(campo.parametro,json2);
                tmp1:=obtiene_filtro_perfilamiento_usuario_6000(v_rut_emisor,v_rut_usuario,upper(campo.parametro),aux);
                --json2:=logjson(json2,'PARAMETRO '||campo.parametro||'='||coalesce(tmp1,'vacio'));
                json2:=logjson(json2,'PARAMETRO BOL '||campo.parametro||'='||coalesce(tmp1,'vacio')||' aux='||aux);
                /*if (length(aux)>0 and aux not in ('*','undefined')) then
                        v_parametro_var:=v_parametro_var||' and '||campo.parametro||'='||quote_literal(aux)||' ';
                end if;*/
                v_parametro_var:=v_parametro_var|| ' ' ||tmp1;
        end loop;

        --Armo la busqueda para las refernecias
        v_parametro_referencias1:='';
        --if (get_json('TIPO_REFERENCIA',json2)<>'*' and get_json('TIPO_REFERENCIA',json2)<>'') then
        --       v_parametro_referencias1:=' and strpos(referencias::varchar,''"Tipo":"'||get_json('TIPO_REFERENCIA',json2)||'"'')>0 ';
        --end if;

        v_parametro_adicional1:='';
        if(get_json('PARAMETRO_ADICIONAL',json2)<>'') then
                v_parametro_adicional1:=' and strpos(data_dte,''<'||get_json('PARAMETRO_ADICIONAL',json2)||'>'')>0 ';
        end if;


        --Saco los estados del redshift
        --query1:='select count(*),tipo_dte,estado_sii,estado_inter from dte_boletas where '||v_parametro_rut_emisor||' '||v_parametro_tipo_dte||'  and dia'||tipoFecha1||'>='|| v_fecha_inicio::varchar||' and dia'||tipoFecha1||'<='||v_fecha_fin::varchar||' '||v_parametro_var||'  group by 2,3,4';
	if (get_json('MONTO_CANTIDAD',json2)='MONTO') then
	        query1:='select sum(monto_total) as count,tipo_dte from dte_boletas where '||v_parametro_rut_emisor||' '||v_parametro_tipo_dte||'  and dia'||tipoFecha1||'>='|| v_fecha_inicio::varchar||' and dia'||tipoFecha1||'<='||v_fecha_fin::varchar||' '||v_parametro_var||v_parametro_referencias1||v_parametro_adicional1||'  group by 2';
	else
	        query1:='select count(*),tipo_dte from dte_boletas where '||v_parametro_rut_emisor||' '||v_parametro_tipo_dte||'  and dia'||tipoFecha1||'>='|| v_fecha_inicio::varchar||' and dia'||tipoFecha1||'<='||v_fecha_fin::varchar||' '||v_parametro_var||v_parametro_referencias1||v_parametro_adicional1||'  group by 2';
	end if;
	json2:=put_json(json2,'QUERY_RS',query1);
	json2 := put_json(json2,'__SECUENCIAOK__','20');
	return json2;
END;
$$ LANGUAGE plpgsql;
