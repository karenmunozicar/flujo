/*
[{"estado":"Emitidos__info__EMITIDO","factura_afecta__link__33":400,"factura_exenta__link__34":0,"liquidacion_factura__link__43":0,"factura_compra__link__46":0,"guia_despacho__link__52":0,"nota_debito__link__56":0,"nota_credito__link__61":0,"total":400}]

[{"estado":"Aprobado SII__info__ACEPTADO_POR_EL_SII","factura_afecta__link__33":384,"factura_exenta__link__34":0,"liquidacion_factura__link__43":0,"factura_compra__link__46":0,"guia_despacho__link__52":0,"nota_debito__link__56":0,"nota_credito__link__61":0,"total":384},{"estado":"Aprobado SII con Reparos__info__ACEPTADO_CON_REPAROS_POR_EL_SII","factura_afecta__link__33":16,"factura_exenta__link__34":0,"liquidacion_factura__link__43":0,"factura_compra__link__46":0,"guia_despacho__link__52":0,"nota_debito__link__56":0,"nota_credito__link__61":0,"total":16}]

[{"estado":"Pendiente Intercambio__info__PENDIENTE_INTER","factura_afecta__link__33":214,"factura_exenta__link__34":0,"liquidacion_factura__link__43":0,"factura_compra__link__46":0,"guia_despacho__link__52":0,"nota_debito__link__56":0,"nota_credito__link__61":0,"total":214},{"estado":"Pendientes SII__info__PENDIENTE_SII","factura_afecta__link__33":0,"factura_exenta__link__34":0,"liquidacion_factura__link__43":0,"factura_compra__link__46":0,"guia_despacho__link__52":0,"nota_debito__link__56":0,"nota_credito__link__61":0,"total":0}]

[{"estado":"Rechazado SII__info__RECHAZADO_POR_EL_SII","factura_afecta__link__33":0,"factura_exenta__link__34":0,"liquidacion_factura__link__43":0,"factura_compra__link__46":0,"guia_despacho__link__52":0,"nota_debito__link__56":0,"nota_credito__link__61":0,"total":0},{"estado":"Rechazados por Recepcion de Correo__info__RECHAZADO_POR_SERVIDOR_CORREO","factura_afecta__link__33":1,"factura_exenta__link__34":0,"liquidacion_factura__link__43":0,"factura_compra__link__46":0,"guia_despacho__link__52":0,"nota_debito__link__56":0,"nota_credito__link__61":0,"total":1},{"estado":"Rechazados por Notificacion Comercial__info__RECHAZADO_CON_NOTIFICACION_COMERCIAL","factura_afecta__link__33":5,"factura_exenta__link__34":0,"liquidacion_factura__link__43":0,"factura_compra__link__46":0,"guia_despacho__link__52":0,"nota_debito__link__56":0,"nota_credito__link__61":0,"total":5},{"estado":"Rechazados por Notificacion Tecnica__info__RECHAZADO_CON_NOTIFICACION_TECNICA","factura_afecta__link__33":1,"factura_exenta__link__34":0,"liquidacion_factura__link__43":0,"factura_compra__link__46":0,"guia_despacho__link__52":0,"nota_debito__link__56":0,"nota_credito__link__61":0,"total":1}]
*/

--PIVOTE CUADROS EMITIDOS
CREATE or replace FUNCTION cuadro1emitidos(json) RETURNS json AS $$
DECLARE
        json2   json;
        json1   alias for $1;
    v_fecha_inicio      integer;
    v_fecha_fin         integer;
	fecha_in1	varchar;
BEGIN
        json2:=json1;
	--perform logfile('JSON2.0='||get_json('parametro1',json2));
        json2:=corrige_fechas(json2);

/*
	if (get_json('rutUsuario',json2)<>'7621836') then
		if (get_json('tipo_tx',json2)='select_documentos_x_rut_resumen_totalizados') then
			return select_documentos_x_rut_resumen_totalizados_6000(json2);
		else
			return select_doc_emitidos_por_dte_6000_new(json2);
		end if;
	end if;
*/


        v_fecha_inicio:=get_json('fstart',json2)::integer;
        v_fecha_fin:=get_json('fend',json2)::integer;
	fecha_in1:=genera_in_fechas(v_fecha_inicio::integer,v_fecha_fin::integer);
        if (fecha_in1 is null) then
                json2:=response_requests_6000('2', 'Rango de Fechas Invalida','',json2);
                return json2;
        end if;

	--Si fend=hoy y fstart=hoy vamos a la funcion antigua
        if (v_fecha_inicio>=to_char(now(),'YYYYMMDD')::integer and v_fecha_fin>=to_char(now(),'YYYYMMDD')::integer) then
		if (get_json('tipo_tx',json2)='select_documentos_x_rut_resumen_totalizados') then	
			--Cuadro 1
	                return select_documentos_x_rut_resumen_totalizados_6000(json2);
		elsif (get_json('tipo_tx',json2)='select_cuadro_1_excepciones') then
			return select_cuadro_1_excepciones_6000(json2);
		else
			--Cuadro 2
			return select_doc_emitidos_por_dte_6000_new(json2);
		end if;
	else
        	json2:=put_json(json2,'LLAMA_FLUJO','SI');
	        json2:=put_json(json2,'__SECUENCIAOK__','13710');
        end if;
	--perform logfile('JSON2='||get_json('parametro1',json2));
        return json2;
END;
$$ LANGUAGE plpgsql;




delete from isys_querys_tx where llave='13710';
delete from isys_querys_tx where llave='13711';
delete from isys_querys_tx where llave='13712';

--Consultamos en la base de traza si el DTE ya esta publicado
insert into isys_querys_tx values ('13710',5,1,1,'select pivote_13710(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('13710',10,1,9,'<EJECUTA0>13711</EJECUTA0><EJECUTA1>13712</EJECUTA1><TIMEOUT>15</TIMEOUT>',0,0,0,1,1,20,20);
insert into isys_querys_tx values ('13710',20,1,1,'select valida_respuesta_13710(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

--Voy a la base de los emitidos Redshift
insert into isys_querys_tx values ('13711',10,1,1,'select select_cuadro_1_emitidos_redshift_13711(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);
--Voy a la base de los emitidos Redshift
insert into isys_querys_tx values ('13712',10,1,1,'select select_cuadro_1_boletas_redshift_13712(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

CREATE or replace FUNCTION pivote_13710(json) RETURNS json AS $$
declare
        json2   json;
        json1   alias for $1;
	grupo_tot1	varchar;
begin
	json2:=json1;	
	--perform logfile('JSON3='||get_json('parametro1',json2));
        if (get_json('tipo_tx',json2) in ('select_documentos_x_rut_resumen_totalizados','select_cuadro_1_excepciones')) then
		--Limpiamos para el cuadro 1, el evento_tot y grupo_tot para que no altere
		json2 := put_json(json2,'evento_tot','');
		json2 := put_json(json2,'grupo_tot','');
		--Vamos al multihilo
		json2 := put_json(json2,'__SECUENCIAOK__','10');	
		--perform logfile('JSON4='||get_json('parametro1',json2));
		return json2;
	end if;
	
	 grupo_tot1:=get_json('grupo_tot',json2);
        if (grupo_tot1 in ('documentos_nacionales','documentos_exportacion')) then
		json2:=select_cuadro_1_emitidos_redshift_13711(json2);
		json2 := put_json(json2,'__SECUENCIAOK__','20');
		return json2;
        elsif (grupo_tot1='documentos_boletas') then
		json2:=select_cuadro_1_boletas_redshift_13712(json2);
		json2 := put_json(json2,'__SECUENCIAOK__','20');
		return json2;
        end if;
	json2 := put_json(json2,'__SECUENCIAOK__','0');
	return json2;
end;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION valida_respuesta_cuadro2_13710(json) RETURNS json AS $$
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
        json_aux1       json;
        json_pend       json;
        tipo_dte1       varchar;
        texto1          varchar;
        select_1        varchar;
        json3   json;
        json_hoy1       json;
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

        --Si fend es hoy saco los valore de hoy para sumar
        if (v_fecha_fin>=to_char(now(),'YYYYMMDD')::integer) then
                json3:=json2;
                json3:=put_json(json3,'fstart',to_char(now(),'YYYY-MM-DD'));
                --raise notice 'fstart=%',get_json('fend',json2);
                json_hoy1:=select_doc_emitidos_por_dte_6000_new(json3);
                json_hoy1:=split_part(get_json('RESPUESTA',json_hoy1),chr(10)||chr(10),2)::json;
                if (get_json('CODIGO_RESPUESTA',json_hoy1)='1') then
                        json_hoy1:=get_json('RESPUESTA',json_hoy1)::json;
			json_hoy1:=replace(json_hoy1::varchar,'factura_afecta__link__33','factura_electronica__link__33');
			json_hoy1:=replace(json_hoy1::varchar,'boleta_afecta__link__39','boleta_electronica__link__39');
			json_hoy1:=replace(json_hoy1::varchar,'guia_despacho__link__52','guias_despacho__link__52');
			json_hoy1:=replace(json_hoy1::varchar,'factura_compra__link__46','factura_de_compra__link__46');
		else
			json_hoy1:=null;
                end if;
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
	

        --Verificamos si debo contar los emitidos del dia de hoy que no estan en el redshift
        if (get_json('tipoFecha',json2)='Emision' and json_hoy1 is null) then
		if (grupo_tot1<>'documentos_boletas') then
                	--perform logfile('CUADRO1 SUMA_EMISION lista1='||lista_emi::varchar);
                	json5:=select_suma_emision_actual_13710(json2);
	                --perform logfile('CUADRO1 SUMA_EMISION json5='||json5::varchar);
        	        if (json5 is not null) then
                	        --Para las excepciones, sume diferente
                        	if (get_json('tipo_tx',json2)='select_cuadro_1_excepciones') then
                                	lista_emi:=suma_json2(lista_emi,json5,'["count"]','["tipo_dte","estado"]');
	                        else
        	                        lista_emi:=suma_json2(lista_emi,json5,'["count"]','["tipo_dte","estado_sii","estado_inter"]');
                	        end if;
	                end if;
		else
                	--perform logfile('CUADRO1 RESULTADO lista1='||lista_emi::varchar);
                	--perform logfile('CUADRO1 SUMA_EMISION lista_bol='||lista_bol::varchar);
	                json2:=put_json(json2,'TIPO_SUMA','BOLETAS');
        	        json5:=select_suma_emision_actual_13710(json2);
                	--perform logfile('CUADRO1 SUMA_EMISION json5='||json5::varchar);
	                if (json5 is not null) then
        	                lista_bol:=suma_json2(lista_bol,json5,'["count"]','["tipo_dte","estado_sii","estado_inter"]');
                	end if;
	                --perform logfile('CUADRO1 SUMA_EMISION lista_bol='||lista_bol::varchar);
		end if;
        end if;


        json2:=logjson(json2,'JUNTAMOS JSONS  json_hoy1='||json_hoy1::varchar);
	--raise notice 'json_hoy1=%',json_hoy1;


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
	elsif (evento_tot1='Pendientes') then
		json_resp:=put_json(json_resp,'estado','Pendiente Intercambio__info__PENDIENTE_INTER');
		json_resp1:=put_json(json_resp,'estado','Pendientes SII__info__PENDIENTE_SII');
	elsif (evento_tot1='En Espera') then
		json_resp:=put_json(json_resp,'estado','En Espera__info__DTE_EN_ESPERA');
	elsif (evento_tot1='Folios Duplicados') then
		json_resp:=put_json(json_resp,'estado','Folios Duplicados__info__DTE_YA_ACEPTADO_SII');
	elsif (evento_tot1='Documentos Reprocesados') then
		json_resp:=put_json(json_resp,'estado','Documentos Reprocesados__info__REPROCESA_DTE_RECHAZADA_SII');
	elsif (evento_tot1='Documentos Repetidos') then
		json_resp:=put_json(json_resp,'estado','Documentos Repetidos__info__DTE_REPETIDO');
	elsif (evento_tot1='Error de Esquema') then
		json_resp:=put_json(json_resp,'estado','Error de Esquema__info__FALLA_ESQUEMA_DTE');
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
			--perform logfile('CUADRO '||grupo_tot1||' texto1='||texto1||' get_json(texto1,json_resp)='||get_json(texto1,json_resp)||' count='||get_json('count',json_aux1)||' evento_tot1='||evento_tot1||' estado1='||estado1);
			if (evento_tot1='Emitidos') then
				--[{"estado":"Emitidos__info__EMITIDO","factura_afecta__link__33":400,"factura_exenta__link__34":0,"liquidacion_factura__link__43":0,"factura_compra__link__46":0,"guia_despacho__link__52":0,"nota_debito__link__56":0,"nota_credito__link__61":0,"total":400}]
				--[{"estado":"Aprobado SII__info__ACEPTADO_POR_EL_SII","factura_afecta__link__33":384,"factura_exenta__link__34":0,"liquidacion_factura__link__43":0,"factura_compra__link__46":0,"guia_despacho__link__52":0,"nota_debito__link__56":0,"nota_credito__link__61":0,"total":384},{"estado":"Aprobado SII con Reparos__info__ACEPTADO_CON_REPAROS_POR_EL_SII","factura_afecta__link__33":16,"factura_exenta__link__34":0,"liquidacion_factura__link__43":0,"factura_compra__link__46":0,"guia_despacho__link__52":0,"nota_debito__link__56":0,"nota_credito__link__61":0,"total":16}]
				--raise notice '% % get_json(texto1,json_resp)=% %',grupo_tot1,texto1,get_json(texto1,json_resp),get_json('count',json_aux1);
				json_resp:=put_json(json_resp,texto1,(get_json(texto1,json_resp)::integer+get_json('count',json_aux1)::integer)::varchar);
				json_resp:=put_json(json_resp,'total',(get_json('total',json_resp)::integer+get_json('count',json_aux1)::integer)::varchar);
			--end if;
			elsif (evento_tot1='Aceptados') then
				if(get_json('estado_sii',json_aux1) in ('ACEPTADO_CON_REPAROS_POR_EL_SII')) then
					json_resp1:=put_json(json_resp1,texto1,(get_json(texto1,json_resp1)::integer+get_json('count',json_aux1)::integer)::varchar);
					json_resp1:=put_json(json_resp1,'total',(get_json('total',json_resp1)::integer+get_json('count',json_aux1)::integer)::varchar);
				end if;
				if (get_json('estado_sii',json_aux1) in ('ACEPTADO_POR_EL_SII')) then
					json_resp:=put_json(json_resp,texto1,(get_json(texto1,json_resp)::integer+get_json('count',json_aux1)::integer)::varchar);
					json_resp:=put_json(json_resp,'total',(get_json('total',json_resp)::integer+get_json('count',json_aux1)::integer)::varchar);
				end if;
			elsif (evento_tot1='Rechazados') then
				if(get_json('estado_sii',json_aux1) in ('RECHAZADO_POR_EL_SII'))  then
					json_resp:=put_json(json_resp,texto1,(get_json(texto1,json_resp)::integer+get_json('count',json_aux1)::integer)::varchar);
					json_resp:=put_json(json_resp,'total',(get_json('total',json_resp)::integer+get_json('count',json_aux1)::integer)::varchar);
				end if;
				if (get_json('estado_inter',json_aux1) in ('RECHAZADO_CON_NOTIFICACION_TECNICA'))  then
					json_resp3:=put_json(json_resp3,texto1,(get_json(texto1,json_resp3)::integer+get_json('count',json_aux1)::integer)::varchar);
					json_resp3:=put_json(json_resp3,'total',(get_json('total',json_resp3)::integer+get_json('count',json_aux1)::integer)::varchar);
				end if;
				if (get_json('estado_inter',json_aux1) in ('RECHAZADO_POR_SERVIDOR_CORREO'))  then
					json_resp1:=put_json(json_resp1,texto1,(get_json(texto1,json_resp1)::integer+get_json('count',json_aux1)::integer)::varchar);
					json_resp1:=put_json(json_resp1,'total',(get_json('total',json_resp1)::integer+get_json('count',json_aux1)::integer)::varchar);
				end if;
				if (get_json('estado_inter',json_aux1) in ('RECHAZADO_CON_NOTIFICACION_COMERCIAL'))  then
					json_resp2:=put_json(json_resp2,texto1,(get_json(texto1,json_resp2)::integer+get_json('count',json_aux1)::integer)::varchar);
					json_resp2:=put_json(json_resp2,'total',(get_json('total',json_resp2)::integer+get_json('count',json_aux1)::integer)::varchar);
				end if;
			elsif (tipo_dte1 not in ('110','111','112','39','41') and evento_tot1='Pendientes') then
				if (get_json('estado_inter',json_aux1) in ('ENVIADO_POR_INTERCAMBIO','ENTREGA_DE_DTE_POR_INTERCAMBIO_EXITOSA')) then
					json_resp:=put_json(json_resp,texto1,(get_json(texto1,json_resp)::integer+get_json('count',json_aux1)::integer)::varchar);
					json_resp:=put_json(json_resp,'total',(get_json('total',json_resp)::integer+get_json('count',json_aux1)::integer)::varchar);
				end if;
				if (get_json('estado_sii',json_aux1) in ('PROCESADO_POR_EL_SII','')) then
					json_resp1:=put_json(json_resp1,texto1,(get_json(texto1,json_resp1)::integer+get_json('count',json_aux1)::integer)::varchar);
					json_resp1:=put_json(json_resp1,'total',(get_json('total',json_resp1)::integer+get_json('count',json_aux1)::integer)::varchar);
				end if;
			elsif (evento_tot1='En Espera' and estado1 in ('DTE_EN_ESPERA')) then
					json_resp:=put_json(json_resp,texto1,(get_json(texto1,json_resp)::integer+get_json('count',json_aux1)::integer)::varchar);
					json_resp:=put_json(json_resp,'total',(get_json('total',json_resp)::integer+get_json('count',json_aux1)::integer)::varchar);
			elsif (evento_tot1='Folios Duplicados' and estado1 in ('DTE_YA_ACEPTADO_SII')) then
					json_resp:=put_json(json_resp,texto1,(get_json(texto1,json_resp)::integer+get_json('count',json_aux1)::integer)::varchar);
					json_resp:=put_json(json_resp,'total',(get_json('total',json_resp)::integer+get_json('count',json_aux1)::integer)::varchar);
			elsif (evento_tot1='Documentos Reprocesados' and estado1 in ('REPROCESA_DTE_RECHAZADA_SII')) then
					json_resp:=put_json(json_resp,texto1,(get_json(texto1,json_resp)::integer+get_json('count',json_aux1)::integer)::varchar);
					json_resp:=put_json(json_resp,'total',(get_json('total',json_resp)::integer+get_json('count',json_aux1)::integer)::varchar);
			elsif (evento_tot1='Documentos Repetidos' and estado1 in ('DTE_REPETIDO')) then
					json_resp:=put_json(json_resp,texto1,(get_json(texto1,json_resp)::integer+get_json('count',json_aux1)::integer)::varchar);
					json_resp:=put_json(json_resp,'total',(get_json('total',json_resp)::integer+get_json('count',json_aux1)::integer)::varchar);
			elsif (evento_tot1='Error de Esquema' and estado1 in ('FALLA_ESQUEMA_DTE')) then
					--perform logfile('Error de Esquema texto1='||texto1||' '||get_json(texto1,json_resp)||' '||get_json('count',json_aux1));
					json_resp:=put_json(json_resp,texto1,(get_json(texto1,json_resp)::integer+get_json('count',json_aux1)::integer)::varchar);
					json_resp:=put_json(json_resp,'total',(get_json('total',json_resp)::integer+get_json('count',json_aux1)::integer)::varchar);
			end if;
	
        	        i:=i+1;
                	aux:=get_json_index(json3,i);
	        end loop;
	else
		--perform logfile('Cuenta BOLETAS');
	        json3:=lista_bol;
        	i:=0;
	        aux:=get_json_index(json3,i);
        	while (aux<>'') loop
                	--raise notice 'aux=%',aux;
	                json_aux1:=aux::json;
			tipo_dte1:=get_json('tipo_dte',json_aux1);
			--Leo el tipo de dte
			select * into campo from tipo_dte where codigo=tipo_dte1::integer;
			if not found then
				json2:=logjson(json2,'No deberia ocurrir tipo_dte no definido en tipo_dte '||tipo_dte1);
				continue;
			end if;
			texto1:=campo.descripcion||'__link__'||tipo_dte1;
			--perform logfile('Cuenta BOL red '||texto1||' '||json_resp::varchar);
			json_resp:=put_json(json_resp,texto1,(get_json(texto1,json_resp)::integer+get_json('count',json_aux1)::integer)::varchar);
			json_resp:=put_json(json_resp,'total',(get_json('total',json_resp)::integer+get_json('count',json_aux1)::integer)::varchar);
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
	select_1:=select_1||']';

        if (json_hoy1 is not null) then
		--perform logfile('Suma Dia de Hoy');
		--perform logfile('select_1='||select_1::varchar);
		--perform logfile('json_hoy1='||json_hoy1::varchar);
                select_1:=suma_json(select_1::json,json_hoy1)::json;
        end if;

        json2:=response_requests_6000('1', '', select_1::Varchar, json2);
        RETURN json2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION valida_respuesta_cuadro1_excepciones(json) RETURNS json AS $$
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
	json_esquema	json;
	json5		json;
	tipo_dte1	varchar;
	texto1		varchar;
	select_1	varchar;
	json3	json;
	json_hoy1	json;
	v_fecha_inicio	integer;
	v_fecha_fin	integer;
	i	integer;
	aux	varchar;
	estado1	varchar;
BEGIN
	json2:=json1;
	json2 := put_json(json2,'__SECUENCIAOK__','0');

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

        json2:=corrige_fechas(json2);
        v_fecha_inicio:=get_json('fstart',json2)::integer;
        v_fecha_fin:=get_json('fend',json2)::integer;

	--Si fend es hoy saco los valore de hoy para sumar
        if (v_fecha_fin>=to_char(now(),'YYYYMMDD')::integer) then
                json3:=json2;
                json3:=put_json(json3,'fstart',to_char(now(),'YYYY-MM-DD'));
                --raise notice 'fstart=%',get_json('fend',json2);
                json_hoy1:=select_cuadro_1_excepciones_6000(json3);
                json_hoy1:=split_part(get_json('RESPUESTA',json_hoy1),chr(10)||chr(10),2)::json;
                if (get_json('CODIGO_RESPUESTA',json_hoy1)='1') then
                        json_hoy1:=get_json('RESPUESTA',json_hoy1)::json;
		else
			json_hoy1:=null;
                end if;
        end if;

        --Verificamos si debo contar los emitidos del dia de hoy que no estan en el redshift
        if (get_json('tipoFecha',json2)='Emision' and json_hoy1 is null) then
                --perform logfile('CUADRO1 SUMA_EMISION lista1='||lista_emi::varchar);
                json5:=select_suma_emision_actual_13710(json2);
                --perform logfile('CUADRO1 SUMA_EMISION json5='||json5::varchar);
                if (json5 is not null) then
                        --Para las excepciones, sume diferente
                        if (get_json('tipo_tx',json2)='select_cuadro_1_excepciones') then
                                lista_emi:=suma_json2(lista_emi,json5,'["count"]','["tipo_dte","estado"]');
                        else
                                lista_emi:=suma_json2(lista_emi,json5,'["count"]','["tipo_dte","estado_sii","estado_inter"]');
                        end if;
                end if;
		/*
                perform logfile('CUADRO1 RESULTADO lista1='||lista_emi::varchar);
                perform logfile('CUADRO1 SUMA_EMISION lista_bol='||lista_bol::varchar);
                json2:=put_json(json2,'TIPO_SUMA','BOLETAS');
                json5:=select_suma_emision_actual_13710(json2);
                perform logfile('CUADRO1 SUMA_EMISION json5='||json5::varchar);
                if (json5 is not null) then
                        lista_bol:=suma_json2(lista_bol,json5,'["count"]','["tipo_dte","estado_sii","estado_inter"]');
                end if;
                perform logfile('CUADRO1 SUMA_EMISION lista_bol='||lista_bol::varchar);
		*/
        end if;


	json2:=logjson(json2,'JUNTAMOS JSONS  json_hoy1='||json_hoy1::varchar);


	--Juntar los json	
        json_emi:='{"evento":"Documentos Reprocesados","documentos_nacionales__link":"0","documentos_exportacion__link":"0","documentos_boletas__link":"0","documentos_totales":"0"}';
        json_ace:='{"evento":"Folios Duplicados","documentos_nacionales__link":"0","documentos_exportacion__link":"0","documentos_boletas__link":"-","documentos_totales":"0"}';
        json_rec:='{"evento":"En Espera","documentos_nacionales__link":"0","documentos_exportacion__link":"0","documentos_boletas__link":"-","documentos_totales":"0"}';
        json_pend:='{"evento":"Documentos Repetidos","documentos_nacionales__link":"0","documentos_exportacion__link":"0","documentos_boletas__link":"-","documentos_totales":"0"}';
	json_esquema:='{"evento":"Error de Esquema","documentos_nacionales__link":"0","documentos_exportacion__link":"0","documentos_boletas__link":"-","documentos_totales":"0"}';
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

		--perform logfile('EXCEPCIONES '||tipo_dte1||' '||texto1||' '||get_json('estado',json_aux1)||' '||get_json('count',json_aux1)||'+'||get_json(texto1,json_emi));
                --Emitidos son todos
		estado1:=get_json('estado',json_aux1);
		if (estado1 in ('REPROCESA_DTE_RECHAZADA_SII')) then
	                json_emi:=put_json(json_emi,texto1,(get_json(texto1,json_emi)::integer+get_json('count',json_aux1)::integer)::varchar);
        	        json_emi:=put_json(json_emi,'documentos_totales',(get_json('documentos_totales',json_emi)::integer+get_json('count',json_aux1)::integer)::varchar);
		elsif (estado1 in ('DTE_YA_ACEPTADO_SII')) then
                        json_ace:=put_json(json_ace,texto1,(get_json(texto1,json_ace)::integer+get_json('count',json_aux1)::integer)::varchar);
                        json_ace:=put_json(json_ace,'documentos_totales',(get_json('documentos_totales',json_ace)::integer+get_json('count',json_aux1)::integer)::varchar);
		elsif (estado1 in ('DTE_EN_ESPERA')) then
                        json_rec:=put_json(json_rec,texto1,(get_json(texto1,json_rec)::integer+get_json('count',json_aux1)::integer)::varchar);
                        json_rec:=put_json(json_rec,'documentos_totales',(get_json('documentos_totales',json_rec)::integer+get_json('count',json_aux1)::integer)::varchar);
		elsif (estado1 in ('DTE_REPETIDO')) then
                        json_pend:=put_json(json_pend,texto1,(get_json(texto1,json_pend)::integer+get_json('count',json_aux1)::integer)::varchar);
                        json_pend:=put_json(json_pend,'documentos_totales',(get_json('documentos_totales',json_pend)::integer+get_json('count',json_aux1)::integer)::varchar);
		elsif (estado1 in ('FALLA_ESQUEMA_DTE')) then
                        json_esquema:=put_json(json_esquema,texto1,(get_json(texto1,json_esquema)::integer+get_json('count',json_aux1)::integer)::varchar);
                        json_esquema:=put_json(json_esquema,'documentos_totales',(get_json('documentos_totales',json_esquema)::integer+get_json('count',json_aux1)::integer)::varchar);
		end if;
                i:=i+1;
                aux:=get_json_index(json3,i);
        end loop;
        
	/*
	json3:=lista_bol;
	i:=0;
        aux:=get_json_index(json3,i);
        while (aux<>'') loop
                --raise notice 'aux=%',aux;
                json_aux1:=aux::json;
                --tipo_dte1:=get_json('tipo_dte',json_aux1);
                texto1:='documentos_boletas__link';

                --Emitidos son todos
                json_emi:=put_json(json_emi,texto1,(get_json(texto1,json_emi)::integer+get_json('count',json_aux1)::integer)::varchar);
                json_emi:=put_json(json_emi,'documentos_totales',(get_json('documentos_totales',json_emi)::integer+get_json('count',json_aux1)::integer)::varchar);
                i:=i+1;
                aux:=get_json_index(json3,i);
	end loop;
	*/
	

        select_1:='['||json_emi::varchar||','||json_ace::varchar||','||json_rec::varchar||','||json_pend::varchar||','||json_esquema::varchar||']';
        --select_1:='['||json_emi::varchar||','||json_ace::varchar||','||json_rec::varchar||','||json_pend::varchar||']';

        if (json_hoy1 is not null) then
                select_1:=suma_json(select_1::json,json_hoy1)::json;
        end if;

        json2:=response_requests_6000('1', '', select_1::Varchar, json2);
        RETURN json2;
END;
$$ LANGUAGE plpgsql;


--UNIMOS JSONS (HOY-EMITIDOS-BOLETAS)
CREATE or replace FUNCTION valida_respuesta_13710(json) RETURNS json AS $$
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
	json5		json;
	tipo_dte1	varchar;
	texto1		varchar;
	select_1	varchar;
	json3	json;
	json_hoy1	json;
	v_fecha_inicio	integer;
	v_fecha_fin	integer;
	i	integer;
	aux	varchar;
BEGIN
	json2:=json1;
	json2 := put_json(json2,'__SECUENCIAOK__','0');

	--Cuadro 1 excepciones
	if (get_json('tipo_tx',json2)='select_cuadro_1_excepciones') then
		json2:=logjson(json2,'Ejecuta valida_respuesta_cuadro1_excepciones');
		return valida_respuesta_cuadro1_excepciones(json2);
	--Cuadro 2
	elsif (get_json('tipo_tx',json2)<>'select_documentos_x_rut_resumen_totalizados') then
		json2:=logjson(json2,'Ejecuta valida_respuesta_cuadro2_13710');
		return valida_respuesta_cuadro2_13710(json2);
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

        json2:=corrige_fechas(json2);
        v_fecha_inicio:=get_json('fstart',json2)::integer;
        v_fecha_fin:=get_json('fend',json2)::integer;

	--Si fend es hoy saco los valore de hoy para sumar
        if (v_fecha_fin>=to_char(now(),'YYYYMMDD')::integer) then
                json3:=json2;
                json3:=put_json(json3,'fstart',to_char(now(),'YYYY-MM-DD'));
                --raise notice 'fstart=%',get_json('fend',json2);
                json_hoy1:=select_documentos_x_rut_resumen_totalizados_6000(json3);
                json_hoy1:=split_part(get_json('RESPUESTA',json_hoy1),chr(10)||chr(10),2)::json;
                if (get_json('CODIGO_RESPUESTA',json_hoy1)='1') then
                        json_hoy1:=get_json('RESPUESTA',json_hoy1)::json;
		else
			json_hoy1:=null;
                end if;
        end if;

	--Verificamos si debo contar los emitidos del dia de hoy que no estan en el redshift
        if (get_json('tipoFecha',json2)='Emision' and json_hoy1 is null) then
                --perform logfile('CUADRO1 SUMA_EMISION lista1='||lista_emi::varchar);
                json5:=select_suma_emision_actual_13710(json2);
                --perform logfile('CUADRO1 SUMA_EMISION json5='||json5::varchar);
                if (json5 is not null) then
                        --Para las excepciones, sume diferente
                        if (get_json('tipo_tx',json2)='select_cuadro_1_excepciones') then
                                lista_emi:=suma_json2(lista_emi,json5,'["count"]','["tipo_dte","estado"]');
                        else
                                lista_emi:=suma_json2(lista_emi,json5,'["count"]','["tipo_dte","estado_sii","estado_inter"]');
                        end if;
                end if;
                --perform logfile('CUADRO1 RESULTADO lista1='||lista_emi::varchar);

                --perform logfile('CUADRO1 SUMA_EMISION lista_bol='||lista_bol::varchar);
		json2:=put_json(json2,'TIPO_SUMA','BOLETAS');
		json5:=select_suma_emision_actual_13710(json2);
                --perform logfile('CUADRO1 SUMA_EMISION json5='||json5::varchar);
		if (json5 is not null) then
                        lista_bol:=suma_json2(lista_bol,json5,'["count"]','["tipo_dte","estado_sii","estado_inter"]');
		end if;
                --perform logfile('CUADRO1 SUMA_EMISION lista_bol='||lista_bol::varchar);
        end if;

	json2:=logjson(json2,'JUNTAMOS JSONS  json_hoy1='||json_hoy1::varchar);


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

		--perform logfile('Procesando '||tipo_dte1||' '||texto1||' '||get_json('estado_sii',json_aux1)||' '||get_json('estado_inter',json_aux1)||' '||get_json('count',json_aux1)||'+'||get_json(texto1,json_emi));
                --Emitidos son todos
                json_emi:=put_json(json_emi,texto1,(get_json(texto1,json_emi)::integer+get_json('count',json_aux1)::integer)::varchar);
                json_emi:=put_json(json_emi,'documentos_totales',(get_json('documentos_totales',json_emi)::integer+get_json('count',json_aux1)::integer)::varchar);
                --Sumo el resultado que necesito
                if (get_json('estado_sii',json_aux1) in ('RECHAZADO_POR_EL_SII') or get_json('estado_inter',json_aux1) in ('RECHAZADO_CON_NOTIFICACION_TECNICA','RECHAZADO_POR_SERVIDOR_CORREO','RECHAZADO_CON_NOTIFICACION_COMERCIAL')) then
                        json_rec:=put_json(json_rec,texto1,(get_json(texto1,json_rec)::integer+get_json('count',json_aux1)::integer)::varchar);
                        json_rec:=put_json(json_rec,'documentos_totales',(get_json('documentos_totales',json_rec)::integer+get_json('count',json_aux1)::integer)::varchar);
		end if;
                if (get_json('estado_sii',json_aux1) in ('ACEPTADO_POR_EL_SII','ACEPTADO_CON_REPAROS_POR_EL_SII')) then
                        json_ace:=put_json(json_ace,texto1,(get_json(texto1,json_ace)::integer+get_json('count',json_aux1)::integer)::varchar);
                        json_ace:=put_json(json_ace,'documentos_totales',(get_json('documentos_totales',json_ace)::integer+get_json('count',json_aux1)::integer)::varchar);
		end if;
                if (tipo_dte1 not in ('110','111','112','39','41') and (get_json('estado_sii',json_aux1) in ('PROCESADO_POR_EL_SII','') or get_json('estado_inter',json_aux1) in ('ENVIADO_POR_INTERCAMBIO','ENTREGA_DE_DTE_POR_INTERCAMBIO_EXITOSA'))) then
			--perform logfile('json_pend Suma '||get_json('rutUsuario',json2)||' '||get_json('count',json_aux1)||'+'||get_json(texto1,json_pend)||' '||tipo_dte1);
                        json_pend:=put_json(json_pend,texto1,(get_json(texto1,json_pend)::integer+get_json('count',json_aux1)::integer)::varchar);
                        json_pend:=put_json(json_pend,'documentos_totales',(get_json('documentos_totales',json_pend)::integer+get_json('count',json_aux1)::integer)::varchar);

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
                json_emi:=put_json(json_emi,texto1,(get_json(texto1,json_emi)::integer+get_json('count',json_aux1)::integer)::varchar);
                json_emi:=put_json(json_emi,'documentos_totales',(get_json('documentos_totales',json_emi)::integer+get_json('count',json_aux1)::integer)::varchar);
                i:=i+1;
                aux:=get_json_index(json3,i);
	end loop;

        select_1:='['||json_emi::varchar||',{"evento":"","documentos_nacionales__link":"","documentos_exportacion__link":"","documentos_boletas__link":"","documentos_totales":""},'||json_ace::varchar||','||json_rec::varchar||','||json_pend::varchar||']';

        if (json_hoy1 is not null) then
                select_1:=suma_json(select_1::json,json_hoy1)::json;
        end if;

        json2:=response_requests_6000('1', '', select_1::Varchar, json2);
        RETURN json2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION select_suma_emision_actual_13710(json) RETURNS json AS $$
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
        json_hoy1       json;
        json_emi        json;
        json_ace        json;
        json_rec        json;
        json_pend       json;
        json_par1       json;
        select_1        varchar;
        texto1  varchar;
        tipo_dte1       varchar;
	dia1		varchar;
BEGIN
        json2:=json1;

	json2:=logjson(json2,'Ejecuta select_cuadro_1_emitidos_redshift_13711');
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

        tipo_dia1:=get_json('tipoFecha',json2);
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

	dia1:=to_char(now(),'YYYYMMDD');
	--Si tiene que sumar BOLETAS
	if (get_json('TIPO_SUMA',json2)='BOLETAS') then
	        	query1:='select array_to_json(array_agg(row_to_json(sql))) from (select count(*),tipo_dte::varchar,coalesce(estado_sii,'''') as estado_sii,coalesce(estado_inter,'''') as estado_inter from dte_boletas_generica where dia='||dia1||' and '||v_parametro_rut_emisor||' '||v_parametro_tipo_dte||'  and dia'||tipoFecha1||'>='|| v_fecha_inicio::varchar||' and dia'||tipoFecha1||'<='||v_fecha_fin::varchar||' '||v_parametro_var||'  group by 2,3,4 order by 1) sql';
	else
		--Si es un error (cuadro1 o cuadro2) 
		if (get_json('evento_tot',json2) in ('Error de Esquema','Folios Duplicados','En Espera','Documentos Reprocesados','Documentos Repetidos') or get_json('tipo_tx',json2)='select_cuadro_1_excepciones') then
		        query1:='select array_to_json(array_agg(row_to_json(sql))) from ( select count(*),tipo_dte::varchar,coalesce(estado,'''') as estado from dte_emitidos_errores where dia='||dia1||' and  '||v_parametro_rut_emisor||' '||v_parametro_tipo_dte||'  and dia'||tipoFecha1||'>='|| v_fecha_inicio::varchar||' and dia'||tipoFecha1||'<='||v_fecha_fin::varchar||' '||v_parametro_var||'  group by 2,3 order by 1) sql';
		else
	        	query1:='select array_to_json(array_agg(row_to_json(sql))) from (select count(*),tipo_dte::varchar,coalesce(estado_sii,'''') as estado_sii,coalesce(estado_inter,'''') as estado_inter from dte_emitidos where dia='||dia1||' and '||v_parametro_rut_emisor||' '||v_parametro_tipo_dte||'  and dia'||tipoFecha1||'>='|| v_fecha_inicio::varchar||' and dia'||tipoFecha1||'<='||v_fecha_fin::varchar||' '||v_parametro_var||'  group by 2,3,4 order by 1) sql';
		end if;
	end if;

	execute query1 into json4;
	return json4;
END;
$$ LANGUAGE plpgsql;


--CUADROS EMITIDOS
CREATE or replace FUNCTION select_cuadro_1_emitidos_redshift_13711(json) RETURNS json AS $$
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
        json_hoy1       json;
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

	--perform logfile('JSON6='||get_json('parametro1',json2));
	--perform logfile('JSON6.1='||get_json('PARAMETRO1',json2));
	json2:=logjson(json2,'Ejecuta select_cuadro_1_emitidos_redshift_13711');
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

        tipo_dia1:=get_json('tipoFecha',json2);
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

	--Si es un error (cuadro1 o cuadro2) 
	if (get_json('evento_tot',json2) in ('Error de Esquema','Folios Duplicados','En Espera','Documentos Reprocesados','Documentos Repetidos') or get_json('tipo_tx',json2)='select_cuadro_1_excepciones') then
        	json_par1:=get_parametros_motor_json('{}','BASE_REDSHIFT_RECIBIDOS');
	        query1:='select count(*),tipo_dte,estado from dte_emitidos_errores where '||v_parametro_rut_emisor||' '||v_parametro_tipo_dte||'  and dia'||tipoFecha1||'>='|| v_fecha_inicio::varchar||' and dia'||tipoFecha1||'<='||v_fecha_fin::varchar||' '||v_parametro_var||'  group by 2,3 order by 1';
		--perform logfile('QUERY '||query1);
	else
        	--Saco los estados del redshift
        	json_par1:=get_parametros_motor_json('{}','BASE_REDSHIFT_EMITIDOS');
	        query1:='select count(*),tipo_dte,estado_sii,estado_inter from dte_emitidos where '||v_parametro_rut_emisor||' '||v_parametro_tipo_dte||'  and dia'||tipoFecha1||'>='|| v_fecha_inicio::varchar||' and dia'||tipoFecha1||'<='||v_fecha_fin::varchar||' '||v_parametro_var||'  group by 2,3,4 order by 1';
	end if;

	json4:='{}';
	json4:=put_json(json4,'_LOG_',get_json('_LOG_',json2));
        json3:=query_db_json(get_json('__IP_CONEXION_CLIENTE__',json_par1),get_json_int('__IP_PORT_CLIENTE__',json_par1)::integer,query1);
	json4:=logjson(json4,'Busqueda Emitidos '||json3::varchar);
        if (get_json('STATUS',json3)<>'OK') then
		json4:=logjson(json4,'Falla Busqueda de Emitidos en BASE_REDSHIFT_EMITIDOS');
		json4:=put_json(json4,'EMITIDOS','[]');
		return json4;
        end if;
	

	if (get_json('TOTAL_REGISTROS',json3)='1') then
		lista1:='['||json3||']';
	else
		lista1:=get_json('LISTA',json3);
	end if;	
	json4:=put_json(json4,'EMITIDOS',lista1);
	return json4;
END;
$$ LANGUAGE plpgsql;

--CUADROS BOLETAS
CREATE or replace FUNCTION select_cuadro_1_boletas_redshift_13712(json) RETURNS json AS $$
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
        json_hoy1       json;
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
	--perform logfile('JSON7='||get_json('parametro1',json2));
	--perform logfile('JSON7.1='||get_json('PARAMETRO1',json2));

	json2:=logjson(json2,'Ejecuta select_cuadro_1_boletas_redshift_13711');
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

        tipo_dia1:=get_json('tipoFecha',json2);
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
		--perform logfile('PARAMETRO BOL '||campo.parametro||'='||coalesce(tmp1,'vacio')||' aux='||aux||' json2='||json2::varchar);
                json2:=logjson(json2,'PARAMETRO BOL '||campo.parametro||'='||coalesce(tmp1,'vacio')||' aux='||aux);
                /*if (length(aux)>0 and aux not in ('*','undefined')) then
                        v_parametro_var:=v_parametro_var||' and '||campo.parametro||'='||quote_literal(aux)||' ';
                end if;*/
                v_parametro_var:=v_parametro_var|| ' ' ||tmp1;
        end loop;

        --Saco los estados del redshift
        --query1:='select count(*),tipo_dte,estado_sii,estado_inter from dte_boletas where '||v_parametro_rut_emisor||' '||v_parametro_tipo_dte||'  and dia'||tipoFecha1||'>='|| v_fecha_inicio::varchar||' and dia'||tipoFecha1||'<='||v_fecha_fin::varchar||' '||v_parametro_var||'  group by 2,3,4';
        query1:='select count(*),tipo_dte from dte_boletas where '||v_parametro_rut_emisor||' '||v_parametro_tipo_dte||'  and dia'||tipoFecha1||'>='|| v_fecha_inicio::varchar||' and dia'||tipoFecha1||'<='||v_fecha_fin::varchar||' '||v_parametro_var||'  group by 2';

        json_par1:=get_parametros_motor_json('{}','BASE_REDSHIFT_BOLETAS');
	json4:='{}';
	json4:=put_json(json4,'_LOG_',get_json('_LOG_',json2));
        json3:=query_db_json(get_json('__IP_CONEXION_CLIENTE__',json_par1),get_json_int('__IP_PORT_CLIENTE__',json_par1)::integer,query1);
	json4:=logjson(json4,'Busqueda Boletas '||json3::varchar);
        if (get_json('STATUS',json3)<>'OK') then
		json4:=put_json(json4,'BOLETAS','[]');	
		json4:=logjson(json4,'Falla Busqueda de Boletas en BASE_REDSHIFT_BOLETAS');
		return json4;
        end if;
	if (get_json('TOTAL_REGISTROS',json3)='1') then
		lista1:='['||json3||']';
	else
		lista1:=get_json('LISTA',json3);
	end if;	

        json4:=put_json(json4,'BOLETAS',lista1);

	return json4;
END;
$$ LANGUAGE plpgsql;
