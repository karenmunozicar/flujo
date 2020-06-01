--PIVOTE CUADRO RECLAMO RECIBIDOS
CREATE or replace FUNCTION cuadro_recibidos_reclamos(json) RETURNS json AS $$
DECLARE
        json2   json;
        json1   alias for $1;
    	v_fecha_inicio      integer;
    	v_fecha_fin         integer;
	fecha_in1	varchar;
	json3       json;
    	json4       json;
    	json5       json;
	texto_ref1	varchar;
BEGIN
        json2:=json1;
	json2:=put_json(json2,'__CUADRO__','1');

	--Ingresamos las variables como las esperan las funciones
	json2:=put_json(json2,'tipoFecha',get_json('TIPO_FECHA',json2));
        json2:=put_json(json2,'fstart',get_json('FSTART',json2));
        json2:=put_json(json2,'fend',get_json('FEND',json2));
        json2:=put_json(json2,'tipo',get_json('TIPO_DTE',json2));
        json2:=put_json(json2,'tipo_dte_filtro',get_json('TIPO_DTE',json2));
        json2:=put_json(json2,'rut_emisor_filtro',get_json('RUT_EMISOR',json2));
	----------------------------------------------------------------------
        json2:=corrige_fechas(json2);

	--Agregamos el Texto de Criterio de Busqueda
	json2:=put_json(json2,'criterio_busqueda','<div id=''div_criterio''><b>Desde: </b>'||substring(get_json('fstart',json2),1,4)||'-'||substring(get_json('fstart',json2),5,2)||'-'||substring(get_json('fstart',json2),7,2)
		||' <b>Hasta: </b>'||substring(get_json('fend',json2),1,4)||'-'||substring(get_json('fend',json2),5,2)||'-'||substring(get_json('fend',json2),7,2)||' <b>Tipo Fecha: </b>'||get_json('TIPO_FECHA',json2)||' <b>Tipo Dte: </b>'||case when get_json('TIPO_DTE',json2)='*' then 'Todos Dte (No incluye Boletas)' else (select replace(initcap(descripcion),'_',' ') from tipo_dte where codigo::varchar=get_json('TIPO_DTE',json2)) end||'</div>');

        v_fecha_inicio:=get_json('fstart',json2)::integer;
        v_fecha_fin:=get_json('fend',json2)::integer;
	fecha_in1:=genera_in_fechas(v_fecha_inicio::integer,v_fecha_fin::integer);
        if (fecha_in1 is null) then
                json2:=response_requests_6000('2', 'Rango de Fechas Invalida','',json2);
                return json2;
        end if;
        	
	json2:=put_json(json2,'LLAMA_FLUJO','SI');
	json2:=put_json(json2,'__SECUENCIAOK__','14810');
	return json2;
END;
$$ LANGUAGE plpgsql;

--PIVOTE CUADRO RECLAMO RECIBIDOS
CREATE or replace FUNCTION cuadro2_recibidos_reclamos(json) RETURNS json AS $$
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
        ----------------------------------------------------------------------
        json2:=corrige_fechas(json2);

        --Agregamos el Texto de Criterio de Busqueda
        json2:=put_json(json2,'criterio_busqueda','<div id=''div_criterio''><b>Desde: </b>'||substring(get_json('fstart',json2),1,4)||'-'||substring(get_json('fstart',json2),5,2)||'-'||substring(get_json('fstart',json2),7,2)
                ||' <b>Hasta: </b>'||substring(get_json('fend',json2),1,4)||'-'||substring(get_json('fend',json2),5,2)||'-'||substring(get_json('fend',json2),7,2)||' <b>Tipo Fecha: </b>'||get_json('TIPO_FECHA',json2)||' <b>Tipo Dte: </b>'||case when get_json('TIPO_DTE',json2)='*' then 'Todos Dte (No incluye Boletas)' else (select replace(initcap(descripcion),'_',' ') from tipo_dte where codigo::varchar=get_json('TIPO_DTE',json2)) end||'</div>');

        v_fecha_inicio:=get_json('fstart',json2)::integer;
        v_fecha_fin:=get_json('fend',json2)::integer;
        fecha_in1:=genera_in_fechas(v_fecha_inicio::integer,v_fecha_fin::integer);
        if (fecha_in1 is null) then
                json2:=response_requests_6000('2', 'Rango de Fechas Invalida','',json2);
                return json2;
        end if;

        json2:=put_json(json2,'LLAMA_FLUJO','SI');
        json2:=put_json(json2,'__SECUENCIAOK__','14810');
        return json2;
END;
$$ LANGUAGE plpgsql;

delete from isys_querys_tx where llave='14810';
delete from isys_querys_tx where llave='14811';
delete from isys_querys_tx where llave='14812';

insert into isys_querys_tx values ('14810',10,1,9,'<EJECUTA0>14811</EJECUTA0><EJECUTA1>14812</EJECUTA1><TIMEOUT>15</TIMEOUT>',0,0,0,1,1,20,20);

insert into isys_querys_tx values ('14810',20,9,1,'select respuesta_recibidos_reclamos_14810(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('14811',10,9,1,'select select_recibidos_redshift_14811(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('14812',10,9,1,'select select_pendientes_recibidos_14812(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

CREATE or replace FUNCTION respuesta2_recibidos_reclamos_14810(json) RETURNS json AS $$
DECLARE
        json2   json;
        json1   alias for $1;
        lista_rec       json;
        lista_pen       json;
        json_resp       json;
        json5           json;
        json_resp1      json;
        json_resp2      json;
        json_resp3      json;
        json_aux1       json;
        json_pend       json;
	json_patron	json;
        tipo_dte1       varchar;
        texto1          varchar;
        select_1        varchar;
        json3   json;
        json4   json;
        v_fecha_inicio  integer;
        v_fecha_fin     integer;
        i       integer;
        aux     varchar;
        grupo_tot1      varchar;
        evento_tot1     varchar;
        campo   record;
        estado1 varchar;
	sufijo1	varchar;
BEGIN
        json2:=json1;
        json2 := put_json(json2,'__SECUENCIAOK__','0');
        grupo_tot1:=get_json('grupo_tot',json2);
        evento_tot1:=get_json('evento_tot',json2);

        json2:=corrige_fechas(json2);
        v_fecha_inicio:=get_json('fstart',json2)::integer;
        v_fecha_fin:=get_json('fend',json2)::integer;

        begin
                lista_rec:=get_json('RECIBIDOS_RS',json2);
        exception when others then
                begin
                	lista_rec:=get_json('RECIBIDOS_RS',json2);
                exception when others then
                        lista_rec:='[]';
                end;
        end;
        begin
                lista_pen:=get_json('PENDIENTES_RECIBIDOS',json2);
        exception when others then
                lista_pen:='[]';
        end;
	
	--perform logfile('DAO_14810 JSON Pen  '||lista_pen::varchar);
	--perform logfile('DAO_14810 JSON Rec  '||lista_rec::varchar);

        if (get_json('TIPO_FECHA',json2) in ('Emision','recepcion_sii') or (get_json('TIPO_FECHA',json2)='Recepcion' and v_fecha_fin>=to_char(now(),'YYYMMDD')::integer)) then
                json5:=select_suma_emision_actual_14810(json2);
                if (json5 is not null) then
                        lista_rec:=suma_json2(lista_rec,json5,'["count"]','["tipo_dte","estado_sii","estado_nar","estado_arm","estado_reclamo","estado_rev"]');
                end if;
        end if;

	json_patron:='{"estado":"","factura_electronica__info__33__link":0, "factura_exenta__info__34__link":0,"boleta_electronica__info__39__link":0, "boleta_exenta__info__41__link":0,"liquidacion_factura__info__43__link":0, "factura_de_compra__info__46__link":0, "guias_despacho__info__52__link":0, "nota_debito__info__56__link":0, "nota_credito__info__61__link":0, "factura_exportacion__info__110__link":0, "nota_debito_exportacion__info__111__link":0,"nota_credito_exportacion__info__112__link":0,"total":0}';
        if(get_json('ejey',json2)='Pendientes') then
                json_resp1:=put_json(json_patron,'estado','Pendientes de Respuesta del SII__info__PPPI');
                json_resp2:=put_json(json_patron,'estado','Pendientes de Recepcion en Acepta__info__PREA');
                json_resp3:=put_json(json_patron,'estado','Pendientes de ARM__info__NARM');
        elsif(get_json('ejey',json2)='Aceptado por el SII') then
                json_resp1:=put_json(json_patron,'estado','Aceptado por el SII__info__RASI');
        elsif(get_json('ejey',json2)='Por Intercambio') then
                json_resp1:=put_json(json_patron,'estado','Documento Recibido (Todos)__info__REMI');
        end if;

	if(get_json('ejey',json2)='DTE_SOLO_SII') then
		sufijo1:='1';
        elsif(get_json('ejey',json2)='DTE_SII_ACEPTA') then
		sufijo1:='2';
        elsif(get_json('ejey',json2)='DTE_SOLO_ACEPTA') then
		sufijo1:='3';
        end if;

        if(get_json('ejex',json2)='aceptados') then
                json_resp1:=put_json(json_patron,'estado','Aceptación Contenido del Documento__info__RACD_'||sufijo1);
                json_resp2:=put_json(json_patron,'estado','Otorga recibo de Mercadería o Servicio__info__RERM_'||sufijo1);
                json_resp3:=put_json(json_patron,'estado','Aceptados Automáticamente(+8 días)__info__RXAU_'||sufijo1);
        elsif(get_json('ejex',json2)='reclamados') then
                json_resp1:=put_json(json_patron,'estado','Rechazo Contenido del Documento__info__RRCD_'||sufijo1);
                json_resp2:=put_json(json_patron,'estado','Reclamo Parcial de mercadería__info__RRFP_'||sufijo1);
                json_resp3:=put_json(json_patron,'estado','Reclamo Total de mercadería__info__RRFT_'||sufijo1);
        elsif(get_json('ejex',json2)='sin_accion') then
                json_resp1:=put_json(json_patron,'estado','Sin Acción__info__RXSA_'||sufijo1);
        elsif(get_json('ejex',json2)='revisados') then
                json_resp1:=put_json(json_patron,'estado','Revisados__info__RRE1_'||sufijo1);
        elsif(get_json('ejex',json2)='no_aplica') then
                json_resp1:=put_json(json_patron,'estado','No Aplica__info__RXNA_'||sufijo1);
        end if;

        i:=0;
        json3:=lista_rec;
        aux:=get_json_index(json3,i);
        while (aux<>'') loop
        	json_aux1:=aux::json;
                tipo_dte1:=get_json('tipo_dte',json_aux1);
                select * into campo from tipo_dte where codigo=tipo_dte1::integer;
		if not found then
			i:=i+1;
			aux:=get_json_index(json3,i);
			continue;
		end if;
                texto1:=campo.descripcion||'__info__'||tipo_dte1||'__link';
        	if(get_json('ejex',json2)='aceptados') then
			if(get_json('estado_nar',json_aux1)='ACEPTACION_DE_CONTENIDO_DE_DOCUMENTO') then
                		json_resp1:=put_json(json_resp1,texto1,(get_json(texto1,json_resp1)::integer+get_json('count',json_aux1)::bigint)::varchar);
				json_resp1:=put_json(json_resp1,'total',(get_json('total',json_resp1)::integer+get_json('count',json_aux1)::bigint)::varchar);
			end if;
			if(get_json('estado_reclamo',json_aux1)='OTORGA_RECIBO_DE_MERCADERIA_O_SERVICIO') then
                                json_resp2:=put_json(json_resp2,texto1,(get_json(texto1,json_resp2)::integer+get_json('count',json_aux1)::bigint)::varchar);
				json_resp2:=put_json(json_resp2,'total',(get_json('total',json_resp2)::integer+get_json('count',json_aux1)::bigint)::varchar);
			end if;
			if(get_json('estado_reclamo',json_aux1)='ACEPTADO_AUTOMATICO') then
                                json_resp3:=put_json(json_resp3,texto1,(get_json(texto1,json_resp3)::integer+get_json('count',json_aux1)::bigint)::varchar);
				json_resp3:=put_json(json_resp3,'total',(get_json('total',json_resp3)::integer+get_json('count',json_aux1)::bigint)::varchar);
			end if;
        	elsif(get_json('ejex',json2)='reclamados') then
			if(get_json('estado_nar',json_aux1)in ('RECHAZO_DE_CONTENIDO_DE_DOCUMENTO')) then
                		json_resp1:=put_json(json_resp1,texto1,(get_json(texto1,json_resp1)::integer+get_json('count',json_aux1)::bigint)::varchar);
				json_resp1:=put_json(json_resp1,'total',(get_json('total',json_resp1)::integer+get_json('count',json_aux1)::bigint)::varchar);
			end if;
			if(get_json('estado_reclamo',json_aux1) in ('RECLAMO_FALTA_PARCIAL_DE_MERCADERIA')) then
                                json_resp2:=put_json(json_resp2,texto1,(get_json(texto1,json_resp2)::integer+get_json('count',json_aux1)::bigint)::varchar);
				json_resp2:=put_json(json_resp2,'total',(get_json('total',json_resp2)::integer+get_json('count',json_aux1)::bigint)::varchar);
			end if;
			if(get_json('estado_reclamo',json_aux1) in ('RECLAMO_FALTA_TOTAL_DE_MERCADERIA')) then
				json_resp3:=put_json(json_resp3,texto1,(get_json(texto1,json_resp3)::integer+get_json('count',json_aux1)::bigint)::varchar);
				json_resp3:=put_json(json_resp3,'total',(get_json('total',json_resp3)::integer+get_json('count',json_aux1)::bigint)::varchar);
			end if;
		elsif(get_json('ejex',json2)='sin_accion') then
                	json_resp1:=put_json(json_resp1,texto1,(get_json(texto1,json_resp1)::integer+get_json('count',json_aux1)::bigint)::varchar);
			json_resp1:=put_json(json_resp1,'total',(get_json('total',json_resp1)::integer+get_json('count',json_aux1)::bigint)::varchar);
		elsif(get_json('ejex',json2)='revisados') then
                	json_resp1:=put_json(json_resp1,texto1,(get_json(texto1,json_resp1)::integer+get_json('count',json_aux1)::bigint)::varchar);
			json_resp1:=put_json(json_resp1,'total',(get_json('total',json_resp1)::integer+get_json('count',json_aux1)::bigint)::varchar);
		elsif(get_json('ejex',json2)='no_aplica') then
                	json_resp1:=put_json(json_resp1,texto1,(get_json(texto1,json_resp1)::integer+get_json('count',json_aux1)::bigint)::varchar);
			json_resp1:=put_json(json_resp1,'total',(get_json('total',json_resp1)::integer+get_json('count',json_aux1)::bigint)::varchar);
        	elsif(get_json('ejey',json2)='Pendientes') then
			if(get_json('estado_sii',json_aux1)='') then
                		json_resp1:=put_json(json_resp1,texto1,(get_json(texto1,json_resp1)::integer+get_json('count',json_aux1)::bigint)::varchar);
				json_resp1:=put_json(json_resp1,'total',(get_json('total',json_resp1)::integer+get_json('count',json_aux1)::bigint)::varchar);
			end if;
			if(get_json('estado_arm',json_aux1)<>'SI' and get_json('tipo_dte',json_aux1) in ('33','52')) then				
				json_resp3:=put_json(json_resp3,texto1,(get_json(texto1,json_resp3)::integer+get_json('count',json_aux1)::bigint)::varchar);
				json_resp3:=put_json(json_resp3,'total',(get_json('total',json_resp3)::integer+get_json('count',json_aux1)::bigint)::varchar);
			end if;
        	elsif(get_json('ejey',json2)='Por Intercambio') then
                	json_resp1:=put_json(json_resp1,texto1,(get_json(texto1,json_resp1)::integer+get_json('count',json_aux1)::bigint)::varchar);
			json_resp1:=put_json(json_resp1,'total',(get_json('total',json_resp1)::integer+get_json('count',json_aux1)::bigint)::varchar);
        	elsif(get_json('ejey',json2)='Aceptado por el SII') then
                	json_resp1:=put_json(json_resp1,texto1,(get_json(texto1,json_resp1)::integer+get_json('count',json_aux1)::bigint)::varchar);
			json_resp1:=put_json(json_resp1,'total',(get_json('total',json_resp1)::integer+get_json('count',json_aux1)::bigint)::varchar);
		end if;
		i:=i+1;
		aux:=get_json_index(json3,i);
	end loop;

        i:=0;
        json3:=lista_pen;
        aux:=get_json_index(json3,i);
        while (aux<>'') loop
        	json_aux1:=aux::json;
                tipo_dte1:=get_json('tipo_dte',json_aux1);
                select * into campo from tipo_dte where codigo=tipo_dte1::integer;
                if not found then
                	json2:=logjson(json2,'No deberia ocurrir tipo_dte no definido en tipo_dte '||tipo_dte1);
                        i:=i+1;
                        aux:=get_json_index(json3,i);
                        continue;
                end if;
                texto1:=campo.descripcion||'__info__'||tipo_dte1||'__link';
        	if(get_json('ejey',json2)='Pendientes') then
                        json_resp2:=put_json(json_resp2,texto1,(get_json(texto1,json_resp2)::integer+get_json('count',json_aux1)::bigint)::varchar);
			json_resp2:=put_json(json_resp2,'total',(get_json('total',json_resp2)::integer+get_json('count',json_aux1)::bigint)::varchar);
        	elsif(get_json('ejex',json2)='aceptados') then
			if(get_json('estado_nar',json_aux1)='ACEPTACION_DE_CONTENIDO_DE_DOCUMENTO') then
                		json_resp1:=put_json(json_resp1,texto1,(get_json(texto1,json_resp1)::integer+get_json('count',json_aux1)::bigint)::varchar);
				json_resp1:=put_json(json_resp1,'total',(get_json('total',json_resp1)::integer+get_json('count',json_aux1)::bigint)::varchar);
			end if;
			if(get_json('estado_reclamo',json_aux1)='OTORGA_RECIBO_DE_MERCADERIA_O_SERVICIO') then
                                json_resp2:=put_json(json_resp2,texto1,(get_json(texto1,json_resp2)::integer+get_json('count',json_aux1)::bigint)::varchar);
				json_resp2:=put_json(json_resp2,'total',(get_json('total',json_resp2)::integer+get_json('count',json_aux1)::bigint)::varchar);
			end if;
        	elsif(get_json('ejex',json2)='reclamados') then
			if(get_json('estado_nar',json_aux1)in ('RECHAZO_DE_CONTENIDO_DE_DOCUMENTO')) then
                		json_resp1:=put_json(json_resp1,texto1,(get_json(texto1,json_resp1)::integer+get_json('count',json_aux1)::bigint)::varchar);
				json_resp1:=put_json(json_resp1,'total',(get_json('total',json_resp1)::integer+get_json('count',json_aux1)::bigint)::varchar);
			end if;
			if(get_json('estado_reclamo',json_aux1) in ('RECLAMO_FALTA_PARCIAL_DE_MERCADERIA')) then
                                json_resp2:=put_json(json_resp2,texto1,(get_json(texto1,json_resp2)::integer+get_json('count',json_aux1)::bigint)::varchar);
				json_resp2:=put_json(json_resp2,'total',(get_json('total',json_resp2)::integer+get_json('count',json_aux1)::bigint)::varchar);
			end if;
			if(get_json('estado_reclamo',json_aux1) in ('RECLAMO_FALTA_TOTAL_DE_MERCADERIA')) then
				json_resp3:=put_json(json_resp3,texto1,(get_json(texto1,json_resp3)::integer+get_json('count',json_aux1)::bigint)::varchar);
				json_resp3:=put_json(json_resp3,'total',(get_json('total',json_resp3)::integer+get_json('count',json_aux1)::bigint)::varchar);
			end if;
		elsif(get_json('ejex',json2)='sin_accion') then
                	json_resp1:=put_json(json_resp1,texto1,(get_json(texto1,json_resp1)::integer+get_json('count',json_aux1)::bigint)::varchar);
			json_resp1:=put_json(json_resp1,'total',(get_json('total',json_resp1)::integer+get_json('count',json_aux1)::bigint)::varchar);
		elsif(get_json('ejex',json2)='revisados') then
                	json_resp1:=put_json(json_resp1,texto1,(get_json(texto1,json_resp1)::integer+get_json('count',json_aux1)::bigint)::varchar);
			json_resp1:=put_json(json_resp1,'total',(get_json('total',json_resp1)::integer+get_json('count',json_aux1)::bigint)::varchar);
		elsif(get_json('ejex',json2)='no_aplica') then
                	json_resp1:=put_json(json_resp1,texto1,(get_json(texto1,json_resp1)::integer+get_json('count',json_aux1)::bigint)::varchar);
			json_resp1:=put_json(json_resp1,'total',(get_json('total',json_resp1)::integer+get_json('count',json_aux1)::bigint)::varchar);
		end if;
                i:=i+1;
                aux:=get_json_index(json3,i);
        end loop;

        select_1:='['||json_resp1::varchar;
        --Armar la lista de salida
        if (json_resp2 is not null) then
                select_1:=select_1||','||json_resp2::varchar;
        end if;
        if (json_resp3 is not null) then
                select_1:=select_1||','||json_resp3::varchar;
        end if;
        select_1:=select_1||']';

        json4:='[]';
        json5:='{}';
        json5:=put_json(json5,'criterio_busqueda',get_json('criterio_busqueda',json2));
        json5:=put_json(json5,'id','Cuadro2');
        json5:=put_json(json5,'tipo','2');
        json5:=put_json(json5,'data',select_1::varchar);
        json5:=put_json(json5,'uri',coalesce((select replace(remplaza_tags_6000(href,json2),'NO_BUSCAR','') from menu_info_10k where id2='buscarNEW_recibidos'),''));
        json5:=put_json(json5,'uri_ant',coalesce((select remplaza_tags_6000(href,json2) from menu_info_10k where id2='cuadro_recibidos_reclamos'),''));
        json5:=put_json(json5,'criterio_busqueda',get_json('criterio_busqueda',json2));
        json4:=put_json_list(json4,json5);
        return response_requests_6000('1', '', json4::varchar, json2);

END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION respuesta_recibidos_reclamos_14810(json) RETURNS json AS $$
DECLARE
        json2   json;
        json1   alias for $1;
        json_rec        json;
        json_pen        json;
        lista_rec       json;
        lista_pen       json;
        json4           json;
        json5           json;
        tipo_dte1       varchar;
        texto1          varchar;
        select_1        varchar;
        json3   json;
        v_fecha_inicio  integer;
        v_fecha_fin     integer;
        i       integer;
        aux     varchar;


	c1_aceptados1	integer;
	c1_pendientes1	integer;
	c1_intercambio1	integer;

	c2_json_aceptados	json;
	c2_json_reclamados	json;
	c2_json_sin_accion	json;
	c2_json_revisados	json;
	c2_json_no_aplica	json;
	c2_json_total		json;

	json_aux1		json;
	base1	varchar;
BEGIN
        json2:=json1;
        json2 := put_json(json2,'__SECUENCIAOK__','0');

	if(get_json('__CUADRO__',json2)='2') then 
		return respuesta2_recibidos_reclamos_14810(json2);
	end if;

        begin
                lista_rec:=get_json('RECIBIDOS_RS',json2);
        exception when others then
                lista_rec:='[]';
        end;
        begin
                lista_pen:=get_json('PENDIENTES_RECIBIDOS',json2);
        exception when others then
                lista_pen:='[]';
        end;

	--perform logfile('DAO_14810 JSON Pen  '||lista_pen::varchar);
	--perform logfile('DAO_14810 JSON Rec  '||lista_rec::varchar);
	

        json2:=corrige_fechas(json2);
        v_fecha_inicio:=get_json('fstart',json2)::integer;
        v_fecha_fin:=get_json('fend',json2)::integer;


        --Verificamos si debo contar los emitidos del dia de hoy que no estan en el redshift
        if (get_json('TIPO_FECHA',json2) in ('Emision','recepcion_sii') or (get_json('TIPO_FECHA',json2)='Recepcion' and v_fecha_fin>=to_char(now(),'YYYMMDD')::integer)) then
                json5:=select_suma_emision_actual_14810(json2);
                if (json5 is not null) then
                        lista_rec:=suma_json2(lista_rec,json5,'["count"]','["tipo_dte","estado_sii","estado_nar","estado_arm","estado_reclamo","estado_rev"]');
                end if;
        end if;

        --Juntar los json
	--{"aceptados__link":"Aceptados","aceptados__DTE_SOLO_SII__link":"5","aceptados__DTE_SII_ACEPTA__link":"55","aceptados__DTE_SOLO_ACEPTA__link":"0","aceptados__TOTAL__link":"60"}	

	c2_json_aceptados:='{"aceptados__link":"Aceptados","aceptados__DTE_SOLO_SII__link":"0","aceptados__DTE_SII_ACEPTA__link":"0","aceptados__DTE_SOLO_ACEPTA__link":"0","aceptados__TOTAL__link":"0"}';
	c2_json_reclamados:='{"reclamados__link":"Reclamados","reclamados__DTE_SOLO_SII__link":"0","reclamados__DTE_SII_ACEPTA__link":"0","reclamados__DTE_SOLO_ACEPTA__link":"0","reclamados__TOTAL__link":"0"}';
	c2_json_sin_accion:='{"sin_accion__link":"Sin Acción","sin_accion__DTE_SOLO_SII__link":"0","sin_accion__DTE_SII_ACEPTA__link":"0","sin_accion__DTE_SOLO_ACEPTA__link":"0","sin_accion__TOTAL__link":"0"}';
	c2_json_revisados:='{"revisados__link":"Revisados","revisados__DTE_SOLO_SII__link":"0","revisados__DTE_SII_ACEPTA__link":"0","revisados__DTE_SOLO_ACEPTA__link":"0","revisados__TOTAL__link":"0"}';
	c2_json_no_aplica:='{"no_aplica__link":"No Aplica","no_aplica__DTE_SOLO_SII__link":"0","no_aplica__DTE_SII_ACEPTA__link":"0","no_aplica__DTE_SOLO_ACEPTA__link":"0","no_aplica__TOTAL__link":"0"}';
	c2_json_total:='{"total__link":"Total","total__DTE_SOLO_SII__link":"0","total__DTE_SII_ACEPTA__link":"0","total__DTE_SOLO_ACEPTA__link":"0","total__TOTAL__link":"0"}';
	
	c1_intercambio1:=0;
	c1_pendientes1:=0;
	c1_aceptados1:=0;

        i:=0;
        json3:=lista_rec;
        aux:=get_json_index(json3,i);
        while (aux<>'') loop
                json_aux1:=aux::json;
	
		--Intercambio Todos
		c1_intercambio1:=c1_intercambio1+get_json('count',json_aux1)::bigint;
		--Sumo el resultado que necesito
                if (get_json('estado_sii',json_aux1) in ('RECHAZADO_POR_EL_SII','')) then
                        c1_pendientes1:=c1_pendientes1+get_json('count',json_aux1)::integer;
                elsif (get_json('estado_sii',json_aux1)='ACEPTADO_POR_EL_SII') then
                        c1_aceptados1:=c1_aceptados1+get_json('count',json_aux1)::integer;
                end if;
                if (get_json('estado_arm',json_aux1)<>'SI' and get_json('tipo_dte',json_aux1) in ('33','52')) then
                        c1_pendientes1:=c1_pendientes1+get_json('count',json_aux1)::integer;
                end if;

		--Cuadro 2 - Reclamos
		if(get_json('tipo_dte',json_aux1) in ('33','34','43')) then
			--En el SII y en ACEPTA
			if(get_json('estado_sii',json_aux1)='ACEPTADO_POR_EL_SII') then
				base1:='__DTE_SII_ACEPTA__link';
				--Confirmado en SII - Aceptados 
				if(get_json('estado_reclamo',json_aux1) in ('OTORGA_RECIBO_DE_MERCADERIA_O_SERVICIO','ACEPTADO_AUTOMATICO')) then
					texto1:='aceptados'||base1;
					c2_json_aceptados:=put_json(c2_json_aceptados,texto1,(get_json(texto1,c2_json_aceptados)::bigint+get_json('count',json_aux1)::bigint)::varchar);
					texto1:='aceptados__TOTAL__link';
					c2_json_aceptados:=put_json(c2_json_aceptados,texto1,(get_json(texto1,c2_json_aceptados)::bigint+get_json('count',json_aux1)::bigint)::varchar);
					texto1:='total'||base1;
					c2_json_total:=put_json(c2_json_total,texto1,(get_json(texto1,c2_json_total)::bigint+get_json('count',json_aux1)::bigint)::varchar);
				end if;
				if(get_json('estado_nar',json_aux1)='ACEPTACION_DE_CONTENIDO_DE_DOCUMENTO') then
					texto1:='aceptados'||base1;
					c2_json_aceptados:=put_json(c2_json_aceptados,texto1,(get_json(texto1,c2_json_aceptados)::bigint+get_json('count',json_aux1)::bigint)::varchar);
					texto1:='aceptados__TOTAL__link';
					c2_json_aceptados:=put_json(c2_json_aceptados,texto1,(get_json(texto1,c2_json_aceptados)::bigint+get_json('count',json_aux1)::bigint)::varchar);
					texto1:='total'||base1;
					c2_json_total:=put_json(c2_json_total,texto1,(get_json(texto1,c2_json_total)::bigint+get_json('count',json_aux1)::bigint)::varchar);
				end if;
				--Confirmado en SII - Reclamados
				if(get_json('estado_reclamo',json_aux1) in ('RECLAMO_FALTA_PARCIAL_DE_MERCADERIA','RECLAMO_FALTA_TOTAL_DE_MERCADERIA')) then
					texto1:='reclamados'||base1;
					c2_json_reclamados:=put_json(c2_json_reclamados,texto1,(get_json(texto1,c2_json_reclamados)::bigint+get_json('count',json_aux1)::bigint)::varchar);
					texto1:='reclamados__TOTAL__link';
					c2_json_reclamados:=put_json(c2_json_reclamados,texto1,(get_json(texto1,c2_json_reclamados)::bigint+get_json('count',json_aux1)::bigint)::varchar);
					texto1:='total'||base1;
					c2_json_total:=put_json(c2_json_total,texto1,(get_json(texto1,c2_json_total)::bigint+get_json('count',json_aux1)::bigint)::varchar);
				end if;
				if( get_json('estado_nar',json_aux1)='RECHAZO_DE_CONTENIDO_DE_DOCUMENTO') then
					texto1:='reclamados'||base1;
					c2_json_reclamados:=put_json(c2_json_reclamados,texto1,(get_json(texto1,c2_json_reclamados)::bigint+get_json('count',json_aux1)::bigint)::varchar);
					texto1:='reclamados__TOTAL__link';
					c2_json_reclamados:=put_json(c2_json_reclamados,texto1,(get_json(texto1,c2_json_reclamados)::bigint+get_json('count',json_aux1)::bigint)::varchar);
					texto1:='total'||base1;
					c2_json_total:=put_json(c2_json_total,texto1,(get_json(texto1,c2_json_total)::bigint+get_json('count',json_aux1)::bigint)::varchar);
				end if;
				--Confirmado en SII - Sin Accion
				if(get_json('estado_reclamo',json_aux1) in ('','SIN_RECLAMO_SII') and get_json('estado_nar',json_aux1)='') then
					texto1:='sin_accion'||base1;
					c2_json_sin_accion:=put_json(c2_json_sin_accion,texto1,(get_json(texto1,c2_json_sin_accion)::bigint+get_json('count',json_aux1)::bigint)::varchar);
					texto1:='sin_accion__TOTAL__link';
					c2_json_sin_accion:=put_json(c2_json_sin_accion,texto1,(get_json(texto1,c2_json_sin_accion)::bigint+get_json('count',json_aux1)::bigint)::varchar);
					texto1:='total'||base1;
					c2_json_total:=put_json(c2_json_total,texto1,(get_json(texto1,c2_json_total)::bigint+get_json('count',json_aux1)::bigint)::varchar);
				end if;
				--Confirmado en SII - Revisados
				if(get_json('estado_rev',json_aux1)='SI') then
					texto1:='revisados'||base1;
					c2_json_revisados:=put_json(c2_json_revisados,texto1,(get_json(texto1,c2_json_revisados)::bigint+get_json('count',json_aux1)::bigint)::varchar);
					texto1:='revisados__TOTAL__link';
					c2_json_revisados:=put_json(c2_json_revisados,texto1,(get_json(texto1,c2_json_revisados)::bigint+get_json('count',json_aux1)::bigint)::varchar);
					texto1:='total'||base1;
					c2_json_total:=put_json(c2_json_total,texto1,(get_json(texto1,c2_json_total)::bigint+get_json('count',json_aux1)::bigint)::varchar);
				end if;
			--No aceptado en el SII (Solo en Acepta sin confirmacion SII)
			else
				base1:='__DTE_SOLO_ACEPTA__link';
				--Lo que estan solo en acepta
				--No Confirmado en SII - Aceptados
				if(get_json('estado_reclamo',json_aux1) in ('OTORGA_RECIBO_DE_MERCADERIA_O_SERVICIO','ACEPTADO_AUTOMATICO')) then
					texto1:='aceptados'||base1;
					c2_json_aceptados:=put_json(c2_json_aceptados,texto1,(get_json(texto1,c2_json_aceptados)::bigint+get_json('count',json_aux1)::bigint)::varchar);
					texto1:='aceptados__TOTAL__link';
					c2_json_aceptados:=put_json(c2_json_aceptados,texto1,(get_json(texto1,c2_json_aceptados)::bigint+get_json('count',json_aux1)::bigint)::varchar);
					texto1:='total'||base1;
					c2_json_total:=put_json(c2_json_total,texto1,(get_json(texto1,c2_json_total)::bigint+get_json('count',json_aux1)::bigint)::varchar);
				end if;
				if(get_json('estado_nar',json_aux1)='ACEPTACION_DE_CONTENIDO_DE_DOCUMENTO') then
					texto1:='aceptados'||base1;
					c2_json_aceptados:=put_json(c2_json_aceptados,texto1,(get_json(texto1,c2_json_aceptados)::bigint+get_json('count',json_aux1)::bigint)::varchar);
					texto1:='aceptados__TOTAL__link';
					c2_json_aceptados:=put_json(c2_json_aceptados,texto1,(get_json(texto1,c2_json_aceptados)::bigint+get_json('count',json_aux1)::bigint)::varchar);
					texto1:='total'||base1;
					c2_json_total:=put_json(c2_json_total,texto1,(get_json(texto1,c2_json_total)::bigint+get_json('count',json_aux1)::bigint)::varchar);
				end if;
				--No Confirmado en SII - Reclamados
				if(get_json('estado_reclamo',json_aux1) in ('RECLAMO_FALTA_PARCIAL_DE_MERCADERIA','RECLAMO_FALTA_TOTAL_DE_MERCADERIA')) then
					texto1:='reclamados'||base1;
					c2_json_reclamados:=put_json(c2_json_reclamados,texto1,(get_json(texto1,c2_json_reclamados)::bigint+get_json('count',json_aux1)::bigint)::varchar);
					texto1:='reclamados__TOTAL__link';
					c2_json_reclamados:=put_json(c2_json_reclamados,texto1,(get_json(texto1,c2_json_reclamados)::bigint+get_json('count',json_aux1)::bigint)::varchar);
					texto1:='total'||base1;
					c2_json_total:=put_json(c2_json_total,texto1,(get_json(texto1,c2_json_total)::bigint+get_json('count',json_aux1)::bigint)::varchar);
				end if;
				if(get_json('estado_nar',json_aux1)='RECHAZO_DE_CONTENIDO_DE_DOCUMENTO') then
					texto1:='reclamados'||base1;
					c2_json_reclamados:=put_json(c2_json_reclamados,texto1,(get_json(texto1,c2_json_reclamados)::bigint+get_json('count',json_aux1)::bigint)::varchar);
					texto1:='reclamados__TOTAL__link';
					c2_json_reclamados:=put_json(c2_json_reclamados,texto1,(get_json(texto1,c2_json_reclamados)::bigint+get_json('count',json_aux1)::bigint)::varchar);
					texto1:='total'||base1;
					c2_json_total:=put_json(c2_json_total,texto1,(get_json(texto1,c2_json_total)::bigint+get_json('count',json_aux1)::bigint)::varchar);
				end if;
				--No Confirmado en SII - Sin Accion
				if(get_json('estado_reclamo',json_aux1)in ('','SIN_RECLAMO_SII') and get_json('estado_nar',json_aux1)='') then
					texto1:='sin_accion'||base1;
					c2_json_sin_accion:=put_json(c2_json_sin_accion,texto1,(get_json(texto1,c2_json_sin_accion)::bigint+get_json('count',json_aux1)::bigint)::varchar);
					texto1:='sin_accion__TOTAL__link';
					c2_json_sin_accion:=put_json(c2_json_sin_accion,texto1,(get_json(texto1,c2_json_sin_accion)::bigint+get_json('count',json_aux1)::bigint)::varchar);
					texto1:='total'||base1;
					c2_json_total:=put_json(c2_json_total,texto1,(get_json(texto1,c2_json_total)::bigint+get_json('count',json_aux1)::bigint)::varchar);
				end if;
				--No Confirmado en SII - Revisados
				if(get_json('estado_rev',json_aux1)='SI') then
					texto1:='revisados'||base1;
					c2_json_revisados:=put_json(c2_json_revisados,texto1,(get_json(texto1,c2_json_revisados)::bigint+get_json('count',json_aux1)::bigint)::varchar);
					texto1:='revisados__TOTAL__link';
					c2_json_revisados:=put_json(c2_json_revisados,texto1,(get_json(texto1,c2_json_revisados)::bigint+get_json('count',json_aux1)::bigint)::varchar);
					texto1:='total'||base1;
					c2_json_total:=put_json(c2_json_total,texto1,(get_json(texto1,c2_json_total)::bigint+get_json('count',json_aux1)::bigint)::varchar);
				end if;
			end if; 
		--Los Tipo DTE que no aplican
		else
			--Confirmado en SII - No Aplica
			if(get_json('estado_sii',json_aux1)='ACEPTADO_POR_EL_SII') then
				base1:='__DTE_SII_ACEPTA__link';
				texto1:='no_aplica'||base1;
				c2_json_no_aplica:=put_json(c2_json_no_aplica,texto1,(get_json(texto1,c2_json_no_aplica)::bigint+get_json('count',json_aux1)::bigint)::varchar);
				texto1:='no_aplica__TOTAL__link';
				c2_json_no_aplica:=put_json(c2_json_no_aplica,texto1,(get_json(texto1,c2_json_no_aplica)::bigint+get_json('count',json_aux1)::bigint)::varchar);
				texto1:='total'||base1;
				c2_json_total:=put_json(c2_json_total,texto1,(get_json(texto1,c2_json_total)::bigint+get_json('count',json_aux1)::bigint)::varchar);
			--No Confirmado en SII - No Aplica
			else
				base1:='__DTE_SOLO_ACEPTA__link';
				texto1:='no_aplica'||base1;
				c2_json_no_aplica:=put_json(c2_json_no_aplica,texto1,(get_json(texto1,c2_json_no_aplica)::bigint+get_json('count',json_aux1)::bigint)::varchar);
				texto1:='no_aplica__TOTAL__link';
				c2_json_no_aplica:=put_json(c2_json_no_aplica,texto1,(get_json(texto1,c2_json_no_aplica)::bigint+get_json('count',json_aux1)::bigint)::varchar);
				texto1:='total'||base1;
				c2_json_total:=put_json(c2_json_total,texto1,(get_json(texto1,c2_json_total)::bigint+get_json('count',json_aux1)::bigint)::varchar);
			end if;
		end if;

                i:=i+1;
                aux:=get_json_index(json3,i);
        end loop;

	--Ahora se recorren los pendientes de recepcion en Acepta (No Recibido en ACEPTA)
        json3:=lista_pen;
        i:=0;
        aux:=get_json_index(json3,i);
        while (aux<>'') loop
                json_aux1:=aux::json;
                --tipo_dte1:=get_json('tipo_dte',json_aux1);
                c1_pendientes1:=c1_pendientes1+get_json('count',json_aux1)::integer;
		--Cuadro 2 - Reclamos
		base1:='__DTE_SOLO_SII__link';
		if(get_json('tipo_dte',json_aux1) in ('33','34','43')) then
			--En el SII y en ACEPTA
			-- No Recibido en ACEPTA - Aceptados
			if(get_json('estado_reclamo',json_aux1) in ('OTORGA_RECIBO_DE_MERCADERIA_O_SERVICIO','ACEPTADO_AUTOMATICO')) then
				texto1:='aceptados'||base1;
				c2_json_aceptados:=put_json(c2_json_aceptados,texto1,(get_json(texto1,c2_json_aceptados)::bigint+get_json('count',json_aux1)::bigint)::varchar);
				texto1:='aceptados__TOTAL__link';
				c2_json_aceptados:=put_json(c2_json_aceptados,texto1,(get_json(texto1,c2_json_aceptados)::bigint+get_json('count',json_aux1)::bigint)::varchar);
				texto1:='total'||base1;
				c2_json_total:=put_json(c2_json_total,texto1,(get_json(texto1,c2_json_total)::bigint+get_json('count',json_aux1)::bigint)::varchar);
			end if;
			if(get_json('estado_nar',json_aux1)='ACEPTACION_DE_CONTENIDO_DE_DOCUMENTO') then
				texto1:='aceptados'||base1;
				c2_json_aceptados:=put_json(c2_json_aceptados,texto1,(get_json(texto1,c2_json_aceptados)::bigint+get_json('count',json_aux1)::bigint)::varchar);
				texto1:='aceptados__TOTAL__link';
				c2_json_aceptados:=put_json(c2_json_aceptados,texto1,(get_json(texto1,c2_json_aceptados)::bigint+get_json('count',json_aux1)::bigint)::varchar);
				texto1:='total'||base1;
				c2_json_total:=put_json(c2_json_total,texto1,(get_json(texto1,c2_json_total)::bigint+get_json('count',json_aux1)::bigint)::varchar);
			end if;
			-- No Recibido en ACEPTA - Reclamados
			if(get_json('estado_reclamo',json_aux1) in ('RECLAMO_FALTA_PARCIAL_DE_MERCADERIA','RECLAMO_FALTA_TOTAL_DE_MERCADERIA')) then
				texto1:='reclamados'||base1;
				c2_json_reclamados:=put_json(c2_json_reclamados,texto1,(get_json(texto1,c2_json_reclamados)::bigint+get_json('count',json_aux1)::bigint)::varchar);
				texto1:='reclamados__TOTAL__link';
				c2_json_reclamados:=put_json(c2_json_reclamados,texto1,(get_json(texto1,c2_json_reclamados)::bigint+get_json('count',json_aux1)::bigint)::varchar);
				texto1:='total'||base1;
				c2_json_total:=put_json(c2_json_total,texto1,(get_json(texto1,c2_json_total)::bigint+get_json('count',json_aux1)::bigint)::varchar);
			end if;
			if(get_json('estado_nar',json_aux1)='RECHAZO_DE_CONTENIDO_DE_DOCUMENTO') then
				texto1:='reclamados'||base1;
				c2_json_reclamados:=put_json(c2_json_reclamados,texto1,(get_json(texto1,c2_json_reclamados)::bigint+get_json('count',json_aux1)::bigint)::varchar);
				texto1:='reclamados__TOTAL__link';
				c2_json_reclamados:=put_json(c2_json_reclamados,texto1,(get_json(texto1,c2_json_reclamados)::bigint+get_json('count',json_aux1)::bigint)::varchar);
				texto1:='total'||base1;
				c2_json_total:=put_json(c2_json_total,texto1,(get_json(texto1,c2_json_total)::bigint+get_json('count',json_aux1)::bigint)::varchar);
			end if;
			-- No Recibido en ACEPTA - Sin Accion
			if(get_json('estado_reclamo',json_aux1)in ('','SIN_RECLAMO_SII') and get_json('estado_nar',json_aux1)='') then
				texto1:='sin_accion'||base1;
				c2_json_sin_accion:=put_json(c2_json_sin_accion,texto1,(get_json(texto1,c2_json_sin_accion)::bigint+get_json('count',json_aux1)::bigint)::varchar);
				texto1:='sin_accion__TOTAL__link';
				c2_json_sin_accion:=put_json(c2_json_sin_accion,texto1,(get_json(texto1,c2_json_sin_accion)::bigint+get_json('count',json_aux1)::bigint)::varchar);
				texto1:='total'||base1;
				c2_json_total:=put_json(c2_json_total,texto1,(get_json(texto1,c2_json_total)::bigint+get_json('count',json_aux1)::bigint)::varchar);
			end if;
			-- No Recibido en ACEPTA - Revisados
			if(get_json('estado_rev',json_aux1)='SI') then
				texto1:='revisados'||base1;
				c2_json_revisados:=put_json(c2_json_revisados,texto1,(get_json(texto1,c2_json_revisados)::bigint+get_json('count',json_aux1)::bigint)::varchar);
				texto1:='revisados__TOTAL__link';
				c2_json_revisados:=put_json(c2_json_revisados,texto1,(get_json(texto1,c2_json_revisados)::bigint+get_json('count',json_aux1)::bigint)::varchar);
				texto1:='total'||base1;
				c2_json_total:=put_json(c2_json_total,texto1,(get_json(texto1,c2_json_total)::bigint+get_json('count',json_aux1)::bigint)::varchar);
			end if;
		-- No Recibido en ACEPTA - DTE que no aplican
		else
			texto1:='no_aplica'||base1;
			c2_json_no_aplica:=put_json(c2_json_no_aplica,texto1,(get_json(texto1,c2_json_no_aplica)::bigint+get_json('count',json_aux1)::bigint)::varchar);
			texto1:='no_aplica__TOTAL__link';
			c2_json_no_aplica:=put_json(c2_json_no_aplica,texto1,(get_json(texto1,c2_json_no_aplica)::bigint+get_json('count',json_aux1)::bigint)::varchar);
			texto1:='total'||base1;
			c2_json_total:=put_json(c2_json_total,texto1,(get_json(texto1,c2_json_total)::bigint+get_json('count',json_aux1)::bigint)::varchar);
		end if;

                i:=i+1;
                aux:=get_json_index(json3,i);
        end loop;

	json3:='['||c2_json_aceptados::varchar||','||c2_json_reclamados::varchar||','||c2_json_sin_accion::varchar||','||c2_json_revisados::varchar||','||c2_json_no_aplica::varchar||','||c2_json_total::varchar||']';

	select_1:='[{"estado":"Pendientes","n__link":'||c1_pendientes1::varchar||'},{"estado":"Por Intercambio","n__link":'||c1_intercambio1::varchar||'}, {"estado":"Aceptado por el SII","n__link":'||c1_aceptados1::varchar||'}]';

        json4:='[]';
        json5:='{}';
        json5:=put_json(json5,'id','Cuadro1');
        json5:=put_json(json5,'tipo','1');
        json5:=put_json(json5,'data',select_1::varchar);
	json5:=put_json(json5,'uri',coalesce((select remplaza_tags_6000(href,json2) from menu_info_10k where id2='cuadro2_recibidos_reclamos'),''));
        json5:=put_json(json5,'criterio_busqueda',get_json('criterio_busqueda',json2));
        json4:=put_json_list(json4,json5);
        json5:='{}';
        json5:=put_json(json5,'id','Cuadro2');
        json5:=put_json(json5,'tipo','14810');
        json5:=put_json(json5,'data',json3::varchar);
	json5:=put_json(json5,'uri',coalesce((select remplaza_tags_6000(href,json2) from menu_info_10k where id2='cuadro2_recibidos_reclamos'),''));
        json5:=put_json(json5,'criterio_busqueda',get_json('criterio_busqueda',json2));
        json4:=put_json_list(json4,json5);
        --end if;
        return response_requests_6000('1', '', json4::varchar, json2);
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION select_suma_emision_actual_14810(json) RETURNS json AS $$
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

	json_rut1	json;
	flag_rut_receptor1	varchar;
	v_parametro_rut_receptor	varchar;
	filtro_cuadro2	varchar;
BEGIN
        json2:=json1;

	json2:=logjson(json2,'Ejecuta select_suma_emision_actual_14810');
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
	elsif(tipo_dia1='recepcion_sii') then
		tipo_dia1:='R';
		tipoFecha1:='_recepcion_sii';
        else
                tipo_dia1:='A';
                tipoFecha1:=' ';
        end if;
	
        json_rut1:=obtiene_filtro_perfilamiento_rut_receptor_6000(v_rut_emisor::bigint,get_json('rutUsuario',json2)::bigint,'rut_emisor',get_json('rut_receptor_filtro',json2));
        flag_rut_receptor1:=get_json('FLAG_RUT_RECEPTOR',json_rut1);
        v_parametro_rut_receptor:=get_json('TAG_RUT_RECEPTOR',json_rut1);
        v_parametro_rut_emisor:=replace(v_parametro_rut_receptor,'receptor','emisor');
        json2:=logjson(json2,'v_parametro_rut_receptor='||v_parametro_rut_receptor);
        rut_idx1:='';
        rut_idx1:=replace(v_parametro_rut_emisor,'rut_emisor',' and rut');
        --Agrega parametro tipo_dte
        v_parametro_tipo_dte:='';
        aux:=get_json('tipo_dte_filtro',json2);
        --Si no es numerico
        v_parametro_tipo_dte:=obtiene_filtro_perfilamiento_usuario_6000(v_rut_emisor::bigint,get_json('rutUsuario',json2)::bigint,'tipo_dte',aux);
        json2:=logjson(json2,'PARAMETRO v_parametro_tipo_dte='||coalesce(v_parametro_tipo_dte,'vacio'));
	
	filtro_cuadro2:='';
	--Viene desde Cuadro 1.1
	if(get_json('ejey',json2)='Pendientes') then
		filtro_cuadro2:=' and (estado_sii in (''RECHAZADO_POR_EL_SII'','''') or (coalesce(estado_arm,'''')<>''SI'' and tipo_dte in (''33'',''52''))) ';
	elsif(get_json('ejey',json2)='Por Intercambio') then
		filtro_cuadro2:='';
	elsif(get_json('ejey',json2)='Aceptado por el SII') then
		filtro_cuadro2:=' and estado_sii=''ACEPTADO_POR_EL_SII''';
	end if;
	--Viene desde Cuadro 1.2
	if(get_json('ejey',json2)='DTE_SOLO_SII') then
		json4:=put_json(json4,'RECIBIDOS_RS','[]');
                return json4;
	end if;
	if(get_json('ejey',json2)='DTE_SII_ACEPTA') then
		filtro_cuadro2:=' and estado_sii=''ACEPTADO_POR_EL_SII'' ';
	elsif(get_json('ejey',json2)='DTE_SOLO_ACEPTA') then
		filtro_cuadro2:=' and (coalesce(estado_sii,'''')<>''ACEPTADO_POR_EL_SII'') ';
	end if;
	if(get_json('ejex',json2)='aceptados') then
		filtro_cuadro2:=filtro_cuadro2||' and tipo_dte in (''33'',''34'',''43'') and (estado_reclamo in (''OTORGA_RECIBO_DE_MERCADERIA_O_SERVICIO'',''ACEPTADO_AUTOMATICO'') or estado_nar=''ACEPTACION_DE_CONTENIDO_DE_DOCUMENTO'') ';
	elsif(get_json('ejex',json2)='reclamados') then
		filtro_cuadro2:=filtro_cuadro2||' and tipo_dte in (''33'',''34'',''43'') and (estado_reclamo in (''RECLAMO_FALTA_PARCIAL_DE_MERCADERIA'',''RECLAMO_FALTA_TOTAL_DE_MERCADERIA'') or estado_nar in (''RECHAZO_DE_CONTENIDO_DE_DOCUMENTO'')) ';
	elsif(get_json('ejex',json2)='sin_accion') then
		filtro_cuadro2:=filtro_cuadro2||' and tipo_dte in (''33'',''34'',''43'') and (coalesce(estado_reclamo,'''') in ('''',''SIN_RECLAMO_SII'') and coalesce(estado_nar,'''')='''') ';
	elsif(get_json('ejex',json2)='revisados') then
		filtro_cuadro2:=filtro_cuadro2||' and tipo_dte in (''33'',''34'',''43'') and strpos(data_dte,''<rev>SI</rev>'')>0 ';
	elsif(get_json('ejex',json2)='no_aplica') then
		filtro_cuadro2:=filtro_cuadro2||' and tipo_dte not in (''33'',''34'',''43'') ';
	end if;

	if (get_json('MONTO_CANTIDAD',json2)='MONTO') then
		monto_cantidad1:=' sum(monto_total) as count ';
	else
		monto_cantidad1:=' count(*) ';
	end if;

	dia1:=to_char(now(),'YYYYMMDD');
	--Solo sacamos de la tabla actual
	tabla1:='dte_recibidos_'||to_char(now(),'YYMM');
       	query1:='select array_to_json(array_agg(row_to_json(sql))) from (select '||monto_cantidad1||',estado_sii,estado_nar,estado_arm,tipo_dte,estado_reclamo,case when strpos(data_dte,''<rev>SI</rev>'')>0 then ''SI'' else ''NO'' end as estado_rev from dte_recibidos where dia='||dia1||' and '||v_parametro_rut_receptor||' '||v_parametro_tipo_dte||'  and dia'||tipoFecha1||' in '||fecha_in1||' '||filtro_cuadro2||' group by 2,3,4,5,6,7)sql';
	--perform logfile('DAO_14810 QUERY Diaria  '||query1);
	execute query1 into json4;
	--perform logfile('DAO_14810 QUERY Diaria  '||json4::varchar);
	return json4;
END;
$$ LANGUAGE plpgsql;


--Pendientes de Recepcion
CREATE or replace FUNCTION select_pendientes_recibidos_14812(json) RETURNS json AS $$
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
	json_rut1	json;
	flag_rut_receptor1	varchar;	
	v_parametro_rut_receptor	varchar;
	filtro_cuadro2	varchar;
	monto_cantidad1	varchar;
BEGIN
        json2:=json1;

	json2:=logjson(json2,'Ejecuta select_pendientes_recibidos_14812');
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
	elsif(tipo_dia1='recepcion_sii') then
                tipo_dia1:='R';
                tipoFecha1:='_recepcion_sii';
        else
                tipo_dia1:='A';
                tipoFecha1:=' ';
        end if;

	 --Soporte multirut receptor
        json_rut1:=obtiene_filtro_perfilamiento_rut_receptor_6000(v_rut_emisor::bigint,get_json('rutUsuario',json2)::bigint,'rut_emisor',get_json('rut_receptor_filtro',json2));
        flag_rut_receptor1:=get_json('FLAG_RUT_RECEPTOR',json_rut1);
        v_parametro_rut_receptor:=get_json('TAG_RUT_RECEPTOR',json_rut1);
        v_parametro_rut_emisor:=replace(v_parametro_rut_receptor,'receptor','emisor');
        json2:=logjson(json2,'v_parametro_rut_receptor='||v_parametro_rut_receptor);
        rut_idx1:='';
        rut_idx1:=replace(v_parametro_rut_emisor,'rut_emisor',' and rut');
        --Agrega parametro tipo_dte
        v_parametro_tipo_dte:='';
        aux:=get_json('tipo_dte_filtro',json2);
        --Si no es numerico
        v_parametro_tipo_dte:=obtiene_filtro_perfilamiento_usuario_6000(v_rut_emisor::bigint,get_json('rutUsuario',json2)::bigint,'tipo_dte',aux);
        json2:=logjson(json2,'PARAMETRO v_parametro_tipo_dte='||coalesce(v_parametro_tipo_dte,'vacio'));
	
	filtro_cuadro2:='';
	--Viene desde Cuadro 1.1
	if(get_json('ejey',json2)='Por Intercambio') then
		json4:=put_json(json4,'PENDIENTES_RECIBIDOS','[]');
                return json4;
	elsif(get_json('ejey',json2)='Aceptado por el SII') then
		json4:=put_json(json4,'PENDIENTES_RECIBIDOS','[]');
                return json4;
	end if;
	--Viene desde Cuadro 1.2
	if(get_json('ejey',json2) in ('DTE_SII_ACEPTA','DTE_SOLO_ACEPTA')) then
		json4:=put_json(json4,'PENDIENTES_RECIBIDOS','[]');
                return json4;
	end if;
	if(get_json('ejex',json2)='aceptados') then
		filtro_cuadro2:=filtro_cuadro2||' and tipo_dte in (''33'',''34'',''43'') and (estado_reclamo in (''OTORGA_RECIBO_DE_MERCADERIA_O_SERVICIO'',''ACEPTADO_AUTOMATICO'') or estado_nar=''ACEPTACION_DE_CONTENIDO_DE_DOCUMENTO'') ';
	elsif(get_json('ejex',json2)='reclamados') then
		filtro_cuadro2:=filtro_cuadro2||' and tipo_dte in (''33'',''34'',''43'') and (estado_reclamo in (''RECLAMO_FALTA_PARCIAL_DE_MERCADERIA'',''RECLAMO_FALTA_TOTAL_DE_MERCADERIA'') or estado_nar in (''RECHAZO_DE_CONTENIDO_DE_DOCUMENTO'')) ';
	elsif(get_json('ejex',json2)='sin_accion') then
		filtro_cuadro2:=filtro_cuadro2||' and tipo_dte in (''33'',''34'',''43'') and (coalesce(estado_reclamo,'''') in ('''',''SIN_RECLAMO_SII'') and coalesce(estado_nar,'''')='''') ';
	elsif(get_json('ejex',json2)='revisados') then
		filtro_cuadro2:=filtro_cuadro2||' and tipo_dte in (''33'',''34'',''43'') and strpos(data_dte,''<rev>SI</rev>'')>0 ';
	elsif(get_json('ejex',json2)='no_aplica') then
		filtro_cuadro2:=filtro_cuadro2||' and tipo_dte not in (''33'',''34'',''43'') ';
	end if;
	if (get_json('MONTO_CANTIDAD',json2)='MONTO') then
                monto_cantidad1:=' sum(monto_total) as count ';
        else
                monto_cantidad1:=' count(*) ';
        end if;
       	
	query1:='select array_to_json(array_agg(row_to_json(sql))) from (select '||monto_cantidad1||',tipo_dte,estado_reclamo,estado_nar,case when strpos(data_dte,''<rev>SI</rev>'')>0 then ''SI'' else ''NO'' end as estado_rev from dte_pendientes_recibidos where '||v_parametro_rut_receptor||' '||v_parametro_tipo_dte||'  and dia'||tipoFecha1||' in '||fecha_in1||' '||filtro_cuadro2||' group by 2,3,4,5)sql';
	--perform logfile('DAO_14810 QUERY Pen  '||query1);
	execute query1 into json3;
	--perform logfile('DAO_14810 QUERY Pen  '||json3::varchar);

	json4:=put_json('{}','PENDIENTES_RECIBIDOS',json3::varchar);
	return json4;
END;
$$ LANGUAGE plpgsql;


--Recibidos de RES
CREATE or replace FUNCTION select_recibidos_redshift_14811(json) RETURNS json AS $$
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
	json_rut1	json;
	flag_rut_receptor1	varchar;
	v_parametro_rut_receptor	varchar;
	filtro_cuadro2	varchar;
	now1	varchar;
	monto_cantidad1	varchar;
	now_int	integer;
BEGIN
        json2:=json1;

	json2:=logjson(json2,'Ejecuta select_cuadro_1_emitidos_redshift_14711');
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

	now_int:=to_char(now(),'YYYYMMDD')::integer;

        tipo_dia1:=get_json('TIPO_FECHA',json2);
        if (tipo_dia1='Emision') then
                tipo_dia1:='E';
                tipoFecha1:='_emision ';
	elsif(tipo_dia1='recepcion_sii') then
                tipo_dia1:='R';
                tipoFecha1:='_recepcion_sii';
        else
                tipo_dia1:='A';
                tipoFecha1:=' ';
        end if;

	 --Soporte multirut receptor
        json_rut1:=obtiene_filtro_perfilamiento_rut_receptor_6000(v_rut_emisor::bigint,get_json('rutUsuario',json2)::bigint,'rut_emisor',get_json('rut_receptor_filtro',json2));
        flag_rut_receptor1:=get_json('FLAG_RUT_RECEPTOR',json_rut1);
        v_parametro_rut_receptor:=get_json('TAG_RUT_RECEPTOR',json_rut1);
        v_parametro_rut_emisor:=replace(v_parametro_rut_receptor,'receptor','emisor');
        json2:=logjson(json2,'v_parametro_rut_receptor='||v_parametro_rut_receptor);
        rut_idx1:='';
        rut_idx1:=replace(v_parametro_rut_emisor,'rut_emisor',' and rut');
        --Agrega parametro tipo_dte
        v_parametro_tipo_dte:='';
        aux:=get_json('tipo_dte_filtro',json2);
        --Si no es numerico
        v_parametro_tipo_dte:=obtiene_filtro_perfilamiento_usuario_6000(v_rut_emisor::bigint,get_json('rutUsuario',json2)::bigint,'tipo_dte',aux);
        json2:=logjson(json2,'PARAMETRO v_parametro_tipo_dte='||coalesce(v_parametro_tipo_dte,'vacio'));

        --Saco los estados del redshift
        json_par1:=get_parametros_motor_json('{}','BASE_REDSHIFT_RECIBIDOS');
	
	filtro_cuadro2:='';
	--Viene desde Cuadro 1.1
	if(get_json('ejey',json2)='Pendientes') then
		filtro_cuadro2:=' and (estado_sii in (''RECHAZADO_POR_EL_SII'','''') or (coalesce(estado_arm,'''')<>''SI'' and tipo_dte in (''33'',''52''))) ';
	elsif(get_json('ejey',json2)='Por Intercambio') then
		filtro_cuadro2:='';
	elsif(get_json('ejey',json2)='Aceptado por el SII') then
		filtro_cuadro2:=' and estado_sii=''ACEPTADO_POR_EL_SII''';
	end if;
	--Viene desde Cuadro 1.2
	if(get_json('ejey',json2)='DTE_SOLO_SII') then
		json4:=put_json(json4,'RECIBIDOS_RS','[]');
                return json4;
	end if;
	if(get_json('ejey',json2)='DTE_SII_ACEPTA') then
		filtro_cuadro2:=' and estado_sii=''ACEPTADO_POR_EL_SII'' ';
	elsif(get_json('ejey',json2)='DTE_SOLO_ACEPTA') then
		filtro_cuadro2:=' and (coalesce(estado_sii,'''')<>''ACEPTADO_POR_EL_SII'') ';
	end if;
	if(get_json('ejex',json2)='aceptados') then
		filtro_cuadro2:=filtro_cuadro2||' and tipo_dte in (''33'',''34'',''43'') and (estado_reclamo in (''OTORGA_RECIBO_DE_MERCADERIA_O_SERVICIO'',''ACEPTADO_AUTOMATICO'') or estado_nar=''ACEPTACION_DE_CONTENIDO_DE_DOCUMENTO'') ';
	elsif(get_json('ejex',json2)='reclamados') then
		filtro_cuadro2:=filtro_cuadro2||' and tipo_dte in (''33'',''34'',''43'') and (estado_reclamo in (''RECLAMO_FALTA_PARCIAL_DE_MERCADERIA'',''RECLAMO_FALTA_TOTAL_DE_MERCADERIA'') or estado_nar in (''RECHAZO_DE_CONTENIDO_DE_DOCUMENTO'')) ';
	elsif(get_json('ejex',json2)='sin_accion') then
		filtro_cuadro2:=filtro_cuadro2||' and tipo_dte in (''33'',''34'',''43'') and (coalesce(estado_reclamo,'''') in ('''',''SIN_RECLAMO_SII'') and coalesce(estado_nar,'''')='''') ';
	elsif(get_json('ejex',json2)='revisados') then
		filtro_cuadro2:=filtro_cuadro2||' and tipo_dte in (''33'',''34'',''43'') and strpos(data_dte,''<rev>SI</rev>'')>0 ';
	elsif(get_json('ejex',json2)='no_aplica') then
		filtro_cuadro2:=filtro_cuadro2||' and tipo_dte not in (''33'',''34'',''43'') ';
	end if;

	if (get_json('MONTO_CANTIDAD',json2)='MONTO') then
                monto_cantidad1:=' sum(monto_total) as count ';
        else
                monto_cantidad1:=' count(*) ';
        end if;

	now1:=now()::varchar;
       	query1:='select '||monto_cantidad1||',estado_sii,estado_nar,estado_arm,tipo_dte,estado_reclamo,case when strpos(data_dte,''<rev>SI</rev>'')>0 then ''SI'' else ''NO'' end as estado_rev from dte_recibidos where '||v_parametro_rut_receptor||' '||v_parametro_tipo_dte||'  and dia'||tipoFecha1||' in '||fecha_in1||' '||filtro_cuadro2||' group by 2,3,4,5,6,7';
	--perform logfile('DAO_14810 QUERY REC ='||query1);

	--perform logfile('FAY '||query1);
	json4:='{}';
	json4:=put_json(json4,'_LOG_',get_json('_LOG_',json2));
	json4:=put_json(json4,'__FLUJO_ACTUAL__',get_json('__FLUJO_ACTUAL__',json2));
        json3:=query_db_json(get_json('__IP_CONEXION_CLIENTE__',json_par1),get_json_int('__IP_PORT_CLIENTE__',json_par1)::integer,query1);
	json4:=logjson(json4,'Busqueda Recibidos '||json3::varchar);
	if (get_json('STATUS',json3)='SIN_DATA') then
		json4:=logjson(json4,'Sin Datos');
		json4:=put_json(json4,'RECIBIDOS_RS','[]');
		return json4;
	end if;
	--perform logfile('DAO_14810 QUERY REC ='||json3::varchar);
        if (get_json('STATUS',json3)<>'OK') then
		json4:=logjson(json4,'Query ='||query1);
		json4:=logjson(json4,'Falla Busqueda de Recibidos en BASE_REDSHIFT_RECIBIDOS');
		json4:=put_json(json4,'RECIBIDOS_RS','[]');
		return json4;
        end if;
	
	if (get_json('TOTAL_REGISTROS',json3)='1') then
		lista1:='['||json3||']';
	else
		lista1:=get_json('LISTA',json3);
	end if;	
	json4:=put_json(json4,'RECIBIDOS_RS',lista1);
	return json4;
END;
$$ LANGUAGE plpgsql;

