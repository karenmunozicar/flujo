delete from isys_querys_tx where llave='12813';
insert into isys_querys_tx values ('12813',10,1,2,'Envia data chile compras',4013,300,101,0,0,20,20);
insert into isys_querys_tx values ('12813',20,1,1,'select procesa_resp_ms_12813(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

create or replace function privote_obtiene_compromiso_chile_compras(json)
 returns json
 as $function$
declare
        json1                 alias for $1;
        json2                 json;
        json3                 json;

        v_area_transaccional  varchar;
        v_codigo_dv           varchar;
        v_par                 varchar;
        v_cap                 varchar;
        v_are                 varchar;
        v_ejercicio           varchar;
        v_tipo                varchar;

        folio1                varchar;
	v_codigo_txel         bigint;
	v_fecha_emision       date;
begin
        json2:=json1;
        json3:='{}';
        folio1:=get_json('folio_compromiso',json2);
        v_area_transaccional:=get_json('area_transaccional', json2);
        v_codigo_dv:=get_json('codigo_dv', json2);
        v_tipo:=get_json('tipo_compromiso', json2);

        --json2:=logjson('MVG 1i - ' || json2::varchar);

        --if get_json('LLAMA_FLUJO_DEVENGO',json2)='NO' or get_json('LLAMA_FLUJO_DEVENGO',json2)='' or folio1 = '' then
	if folio1 = '' then
                return grilla_obtiene_compromiso_chile_compras(json2);
        end if;


        if folio1='' then
                json3:=put_json(json3,'flag_paginacion','SI');
                json3:=put_json(json3,'flag_paginacion_manual','NO');
                json3:=put_json(json3,'flag_tipo_cuadro','GRILLA');
                json3:=put_json(json3,'registros_por_pagina','10');
                json3:=put_json(json3,'MENSAJE_VACIO','No hay registros');
                json3:=put_json(json3,'datos_tabla', '');
                json2:=response_requests_6000('1', 'OK', '', json2);
                return json2;
        end if;

        if v_area_transaccional = '' then
                json2:=response_requests_6000('2', 'No Existe el Área Trasaccional.', '', json2);
                return json2;
        end if;

        --armo la data a enviar

        --update dp_devengo set folio_requerimiento = folio1 where codigo_dv = v_codigo_dv::integer;
        select ejercicio,dte_codigo_txel::bigint into v_ejercicio,v_codigo_txel from dp_devengo where codigo_dv = v_codigo_dv::integer;
	select fecha_emision into v_fecha_emision from dte_recibidos where codigo_txel = v_codigo_txel;

        v_par:=substring(v_area_transaccional, 1, 2);
        v_cap:=substring(v_area_transaccional, 3, 2);
        v_are:=substring(v_area_transaccional, 5, 3);

        json3:=put_json(json3,'partida', v_par);
        json3:=put_json(json3,'capitulo', v_cap);
        json3:=put_json(json3,'areaTransaccional', v_are);
        json3:=put_json(json3,'ejercicio', v_ejercicio);
        json3:=put_json(json3,'folio',folio1);
	json3:=put_json(json3, 'fechaEmision', v_fecha_emision::varchar);

        --llamo al flujo y paso los parametros del servicios para hacer la peticion
        json2:=put_json(json2,'LLAMA_FLUJO','SI');
        json2:=put_json(json2,'__SECUENCIAOK__','12813');
        if v_tipo='COM' then
                json2:=get_parametros_motor_json(json2,'OBTIENECOMPROMISOS_CHC');
        else
                json2:=get_parametros_motor_json(json2, 'OBTIENEREQUERIMIENTOS_CHC');
        end if;
        json2:=put_json(json2,'HOST_MS',get_json('__IP_CONEXION_CLIENTE__', json2));
        json2:=put_json(json2,'URI_MS',get_json('PARAMETRO_RUTA', json2));
        json2:=put_json(json2,'__TIMEOUT_SERV_PXML__','100');
        json2:=put_json(json2,'DATA_JSON',encode_hex(json3::varchar));
        json2:=put_json(json2,'LARGO_JSON',(length(encode_hex(json3::varchar))/2)::varchar);

        json2:=response_requests_6000('1', 'OK', '', json2);
        return json2;
end;
$function$ language plpgsql;

create or replace function procesa_resp_ms_12813(json)
 returns json
 as $function$
declare
        json1                            alias for $1;
        json2                            json;
        json3                            json;
        json_respuesta                   json;
        json_requerimiento               varchar;
        index_json                       integer;
        v_codigo_dv                      integer;
        resp1                            varchar;

        v_codigo_borrador                varchar;
        v_folio                          varchar;
        v_unidad_demandante              varchar;
        v_monto                          varchar;
        v_debe                           varchar;
        v_cuenta_debe                    varchar;
        v_cuenta_haber                   varchar;
        v_concepto_presupuestario_debe   varchar;
        v_concepto_presupuestario_haber  varchar;
        v_codigo_imputacion              varchar;
        v_codigo_combinacion             varchar;
        v_nombre_imputacion              varchar;
        v_tipo_detalle                   varchar;
        v_catalogos                      varchar;
        v_requerimiento                  varchar;
        v_catalogo_tipo                  varchar;
        v_catalogo_codigo                varchar;

        err_msg         varchar:='';
        msj_defecto     varchar;
        pg_context      TEXT;
begin
        json2:=json1;
        --json2:=logjson(json2,'[procesa_resp_ms_12813] json2 entrada=>'||json2::varchar);
	json2:=logjson(json2,'[procesa_resp_ms_12813] RESPUESTA='||get_json('RESPUESTA',json2));

        --v_folio:=get_json('folio', json2);

        json3:='{}';
        json3:=put_json(json3,'flag_paginacion','SI');
        json3:=put_json(json3,'flag_paginacion_manual','NO');
        json3:=put_json(json3,'flag_tipo_cuadro','GRILLA');
        json3:=put_json(json3,'registros_por_pagina','10');
        json3:=put_json(json3,'MENSAJE_VACIO','No hay registros');
        json3:=put_json(json3,'datos_tabla', '');

        resp1:=get_json('RESPUESTA',json2);
        if(strpos(resp1,'HTTP/1.1 200')=0) then
                json3:=put_json(json3,'MENSAJE_VACIO','Error conexion servicio');
                json2:=response_requests_6000('1', 'OK', json3::varchar, json2);
                return json2;
        end if;

        BEGIN
              resp1:=split_part(resp1,chr(10)||chr(10),2);
        EXCEPTION WHEN OTHERS THEN
                json3:=put_json(json3,'MENSAJE_VACIO','Error al leer respuesta servicio');
                json2:=response_requests_6000('1', 'OK', json3::varchar, json2);
                return json2;
        END;
        json2:=logjson(json2,'[procesa_resp_ms_12813] resp1='||resp1);
        resp1:=procesa_json_12813(resp1);
        json2:=logjson(json2,'[procesa_resp_ms_12813] resp2='||resp1);
--      BEGIN
                v_codigo_dv:=get_json('codigo_dv', json2);
                --delete from dp_devengo_detalle where codigo_dv = v_codigo_dv;
		-- FGE - 20191219 - Mensaje de error proviene de sigfe
		json3:=put_json(json3, 'MENSAJE_VACIO', get_json('error_descripcion',resp1::json));
                if get_json('RESPUESTA',resp1::json)='OK' then
			-- FGE - 20200608 - Agregamos tipo de demanda
			json2:=put_json(json2, 'TIPO_DEMANDA', get_json('tipoDemanda',resp1::json));

                        json_respuesta := REGEXP_REPLACE(get_json('LISTA',resp1::json)::varchar, '\d{1}\#\#', '','g')::json;
                        json2:=logjson(json2,'json_respuesta=' || json_respuesta::varchar);
                        v_codigo_dv:=get_json('codigo_dv', json2);
                        index_json:=0;
                        json_requerimiento:=get_json_index(json_respuesta, index_json);
                        while length(json_requerimiento) > 0 loop
                                v_folio:=get_json('folio', json_requerimiento::json);
                                v_requerimiento:=get_json('requerimiento', json_requerimiento::json);
                                v_unidad_demandante:=get_json('unidad_demandante', json_requerimiento::json);
                                v_monto:=get_json('monto_compromiso', json_requerimiento::json);
                                v_debe:=get_json('monto_debe', json_requerimiento::json);
                                v_cuenta_debe:=get_json('cuenta_debe', json_requerimiento::json);
                                v_cuenta_haber:=get_json('cuenta_haber', json_requerimiento::json);
                                v_concepto_presupuestario_debe:=get_json('concepto_presupuestario_debe', json_requerimiento::json);
                                v_concepto_presupuestario_haber:=get_json('concepto_presupuestario_haber', json_requerimiento::json);
                                v_codigo_imputacion:=get_json('codigo_imputacion', json_requerimiento::json);
                                v_codigo_combinacion:=get_json('codigo_combinacion', json_requerimiento::json);
                                v_nombre_imputacion:=get_json('nombre_imputacion', json_requerimiento::json);
                                v_tipo_detalle:=get_json('tipo_detalle', json_requerimiento::json);
                                v_catalogos:=get_json('catalogos', json_requerimiento::json);
                                v_catalogo_tipo:=get_json('catalogo_tipo', json_requerimiento::json);
                                v_catalogo_codigo:=get_json('catalogo_codigo', json_requerimiento::json);
                                if not exists(select 1 from dp_devengo_detalle where codigo_dv = v_codigo_dv::bigint and folio = v_folio and requerimiento = v_requerimiento and cod_imputacion = v_codigo_imputacion and cuenta_debe = v_cuenta_debe) then
                                        insert into dp_devengo_detalle (id, codigo_dv, folio, unidad_demandante, monto, debe, cuenta_debe, cuenta_haber, concepto_presupuestario_debe, concepto_presupuestario_haber, cod_imputacion, cod_combinacion, tipo_detalle, catalogo_nombre, requerimiento, nombre_imputacion, catalogo_tipo, cod_catalogo) values (default, v_codigo_dv::bigint, v_folio, v_unidad_demandante, v_monto::numeric, v_debe::numeric, v_cuenta_debe, v_cuenta_haber, v_concepto_presupuestario_debe, v_concepto_presupuestario_haber, v_codigo_imputacion, v_codigo_combinacion, v_tipo_detalle::integer, v_catalogos, v_requerimiento, v_nombre_imputacion, v_catalogo_tipo, v_catalogo_codigo);
                                end if;
                                index_json:=index_json+1;
                                json_requerimiento:=get_json_index(json_respuesta, index_json);
                        end loop;

                        --json2:=logjson(json2,'datos json3: ' || json3::varchar);
                        json2:=put_json(json2,'V_CODIGO_DV',v_codigo_dv::varchar);
			
                        --json2:=put_json(json2, 'codigo_txel', get_json('codigo_txel', json2));
                        return grilla_obtiene_compromiso_chile_compras(json2);

                        json3:=put_json(json3, 'NO_RELOAD_DIVS', put_json_list(put_json_list(put_json_list('[]', 'form_devengo_manual_cabecera'), 'form_devengo_manual_cuentas'), 'form_devengo_manual_catalogo'));

                elsif get_json('RESPUESTA',resp1::json)='SIN_DATA' then
                        json3:=put_json(json3,'MENSAJE_VACIO','No hay registros');
			-- FGE - 20191219 - Mensaje de Error de SIGFE
			json3:=put_json(json3, 'MENSAJE_VACIO', get_json('error_descripcion',resp1::json));
                elsif get_json('RESPUESTA',resp1::json)='ERROR' then
                        json3:=put_json(json3,'MENSAJE_VACIO','Ha ocurrido un error al consultar la informacion');
                end if;
/*      EXCEPTION WHEN OTHERS THEN
                json3:=put_json(json3,'MENSAJE_VACIO','Ha ocurrido un error interno');
                GET STACKED DIAGNOSTICS pg_context = PG_EXCEPTION_CONTEXT;
                err_msg:='Falló la orden SQL: '||SQLSTATE||'. El error fue: '||SQLERRM||', contexto: '||pg_context;
                json2:=logjson(json2,'[procesa_resp_ms_12813] EXCEPTION='||err_msg);
        END;
*/
        json2:=put_json(json2,'RESPUESTA_COLA','OK');
        json2:=response_requests_6000('1', 'OK', json3::varchar, json2);
        return json2;
end;
$function$ language plpgsql;


create or replace function decide_obtiene_asiento_contable(json)
  returns json
  as $function$
declare
        json1             alias for $1;
        json2             json;
        json_consulta     json;

        v_codigo_dv       varchar;
        v_codigo_txel     varchar;
        v_reg_devengo     record;
        v_reg_referencia  record;
begin
        json2:=json1;
        v_codigo_dv:=get_json('V_CODIGO_DV',json2);
        v_codigo_txel:=get_json('codigo_txel', json2);
        select codigo_dv, estado, tipo_devengo, tipo_dte, ref_codigo_dv from dp_devengo where dte_codigo_txel = v_codigo_txel and tipo_dte <> 0 into v_reg_devengo;
        if not found then
                return grilla_obtiene_compromiso_chile_compras(json2);
        -- elsif v_reg_devengo.tipo_devengo = 'AUT' then
        elsif v_reg_devengo.tipo_dte in (56, 61) then
                select codigo_dv, estado, tipo_devengo, tipo_dte from dp_devengo where codigo_dv = v_reg_devengo.ref_codigo_dv and tipo_dte <> 0 into v_reg_referencia;
                if not found then
                        return grilla_obtiene_compromiso_chile_compras(json2);
                elsif v_reg_referencia.estado = 'FINALIZADO_SIN_ERRORES' and v_reg_referencia.tipo_devengo = 'AUT' and v_reg_devengo.estado in ('BORRADOR', 'REPROCESO') then
                        perform logfile('----- FGE - Va a 12822');
                        json2:=put_json(json2, 'codigo_dv', v_codigo_dv);
                        json2:=put_json(json2, 'LLAMA_FLUJO', 'SI');
                        json2:=put_json(json2, '__SECUENCIAOK__','12822');
                        json2:=dp_consulta_contable_12822(json2); 
                else
                        return grilla_obtiene_compromiso_chile_compras(json2);
                end if;
        else
             return grilla_obtiene_compromiso_chile_compras(json2);
        end if;
        return json2;
end;
$function$ language plpgsql;


create or replace function grilla_obtiene_compromiso_chile_compras(json)
 returns json
 as $function$
declare
        json1                alias for $1;
        json2                json;
        json3                json;
        v_codigo_dv          varchar;
        v_estado             varchar;
        v_codigo_txel        varchar;
        json_form_respuesta  json;
        v_consulta           varchar;
        json_acciones        varchar;
        json_eliminar        varchar;
        v_reversa            varchar;
begin
        json2:=json1;
        json3:='{}';
        -- buscar cod_txel no codigo_dv
        v_codigo_dv:=get_json('V_CODIGO_DV',json2);
        v_codigo_txel:=get_json('codigo_txel', json2);
	json2:=logjson(json2,'v_codigo_dv='||v_codigo_dv::varchar||' codigo_txel='||v_codigo_txel);
        v_reversa:=get_json('reversa', json2);

        --if v_codigo_dv='' then
        --      v_codigo_dv:=get_json('codigo_dv',json2);
        --end if;
        --
        --
        --if v_codigo_dv = '' then
        --        select codigo_dv, estado into v_codigo_dv, v_estado from dp_devengo where dte_codigo_txel = v_codigo_txel and (case when v_reversa = 'Y' then tipo_dte = 0 else tipo_dte <> 0 end) order by codigo_dv desc limit 1;
        --else
        --        select estado into v_estado from dp_devengo where codigo_dv = v_codigo_dv::bigint;
        --end if;

        select codigo_dv, estado into v_codigo_dv, v_estado from dp_devengo where dte_codigo_txel = v_codigo_txel and (case when v_reversa = 'Y' then tipo_dte = 0 else tipo_dte <> 0 end) order by codigo_dv desc limit 1;

        json3:=put_json(json3,'flag_paginacion','SI');
        json3:=put_json(json3,'flag_paginacion_manual','NO');
        json3:=put_json(json3,'flag_tipo_cuadro','GRILLA');
        json3:=put_json(json3,'registros_por_pagina','10');
        json3:=put_json(json3,'MENSAJE_VACIO','No hay registros');

        select row_to_json(sql) from (select *, '##CONCEPTO##___##COMPROMISO##___##REQUERIMIENTO##' as id, 'true' as detailslink, 1 as repeticiones from acciones_grillas where id_pantalla = get_json('app_dinamica', json2) and valor = 'Cuentas') sql into json_acciones;

        if v_estado in ('BORRADOR', 'REPROCESO') then
                select ',' || row_to_json(sql) from (select *, 'orange' as color from acciones_grillas where id_pantalla = get_json('app_dinamica', json2) and valor = 'Eliminar') sql into json_eliminar;
        else
                json_eliminar:=',{}';
        end if;


        json3:=put_json(json3, 'datos_tabla', (select array_to_json(array_agg(row_to_json(sql))) as settings from (
                                                   select codigo_dv as info_sin_formato__codigo_dv__off,
                                                          folio as info_sin_formato__compromiso__on,
                                                          requerimiento as info_sin_formato__requerimiento__on,
                                                          --('[' ||
                                                          ('[' || replace(replace(replace(json_acciones::varchar,'##CONCEPTO##',cod_imputacion), '##COMPROMISO##', coalesce(folio, '')), '##REQUERIMIENTO##', coalesce(requerimiento, '')) || json_eliminar || ']')::json as dropdown__accion__on,
                                                          --('[' || json_eliminar || ']')::json as dropdown__eliminar__on,  --DROP_GLY__ELIMINAR__NN, --dropdown__accion__on,
                                                          cod_imputacion as info_sin_formato__concepto__on,
                                                          nombre_imputacion as info__nombre_concepto__on,
                                                          unidad_demandante as info__catalogo__on,
                                                          sum(debe) as info__monto__on
                                                   from dp_devengo_detalle
                                                   where codigo_dv = v_codigo_dv::bigint
                                                   group by cod_imputacion, nombre_imputacion, unidad_demandante, codigo_dv, folio, requerimiento
                                                   order by folio, requerimiento, nombre_imputacion) sql));

        json3:=put_json(json3, 'NO_RELOAD_DIVS', put_json_list(put_json_list(put_json_list('[]', 'form_devengo_manual_cabecera'), 'form_devengo_manual_cuentas'), 'form_devengo_manual_catalogo'));
        json_form_respuesta:=put_json('{}','LLAMA_FLUJO_DEVENGO', 'SI');
        json3:=put_json(json3,'FORM_RESPUESTA',json_form_respuesta);
	json2:=logjson(json2,'tabla_json3: ' || json3::varchar);


        json2:=put_json(json2,'RESPUESTA_COLA','OK');
        json2:=response_requests_6000('1', 'OK', json3::varchar, json2);
        return json2;
end;
$function$ language plpgsql;


create or replace function procesa_json_12813(json_in text)
returns text
as $function$
import json
json_out={}
lista=[]
#try:
if 1==1:
        json1=json_in
        json1=json1.decode('ascii', 'ignore')
        json1=json.loads(json_in, strict = 'false')
        if 'compromiso' in json1:
		if json1['compromiso'] != None:
                	folio = json1['compromiso']['folio']
			# FGE - 20200601 - Agregamos informacion del tipo de demanda
                        #json_out['tipoDemanda'] = json1['compromiso']['tipoDemanda']
                        # FGE - 20200712 - Info del tipo de demanda si no se encuentra suponemos que es tipo 01
                        if 'tipoDemanda' in json1['compromiso']:
			    json_out['tipoDemanda'] = str(json1['compromiso']['tipoDemanda'])
                        else:
                            json_out['tipoDemanda'] = '01'

			for documento in json1['compromiso']['documentos']['documento']:
				for principal in documento['principales']['principal']:
					for transaccion in principal['transaccionesPrevias']['transaccion']:
						unidadesDemandantes = ''
						for agrupacion in transaccion['agrupacionesDeImputacionesACatalogos']['agrupacion']:
							catalogosDescripcion = ''
							catalogo_tipo = ''
							catalogo_codigo = ''
							for catalogo in agrupacion['imputacionesACatalogosDeReagrupacion']['catalogo']:
								if catalogo['catalogo'] == 'UnidadesDemandantes':
									unidadesDemandantes = catalogo['descripcion']
								if ((catalogo['descripcion'] != 'No Aplica') and (catalogo['catalogo'] != 'programaPresupuestario')):
									catalogosDescripcion = '%s%s / ' % (catalogosDescripcion, catalogo['descripcion'])
								if ((catalogo['catalogo'] != 'programaPresupuestario') and (catalogo['catalogo'] != 'UnidadesDemandantes')):
									catalogo_tipo = catalogo['catalogo']
									catalogo_codigo = catalogo['elemento']
							for imputacion in agrupacion['imputacionesAConceptosPresupuestarios']['imputacion']:
								for cuentaDebe in imputacion['cuentas']['cuentasDebe']:
									jaux = {}
									jaux['folio'] = folio
									if transaccion['tipo'] == '01':
										jaux['requerimiento'] = str(transaccion['folio'])
									else:
										jaux['requerimiento'] = ''
									jaux['unidad_demandante'] = unidadesDemandantes
									jaux['catalogos'] = catalogosDescripcion[:-2]
									jaux['catalogo_tipo'] = catalogo_tipo
									jaux['catalogo_codigo'] = catalogo_codigo
									jaux['cuenta_debe'] = str(cuentaDebe['codigo'])
									jaux['cuenta_haber'] = str(imputacion['cuentas']['cuentasHaber'][0]['codigo'])
									jaux['concepto_presupuestario_debe'] = cuentaDebe['nombre']
									jaux['concepto_presupuestario_haber'] = imputacion['cuentas']['cuentasHaber'][0]['nombre']
									jaux['monto_compromiso'] = str(imputacion['monto'])
									jaux['monto_debe'] = str(0)
									jaux['codigo_imputacion'] = str(imputacion['idConcepto'])
									jaux['nombre_imputacion'] = imputacion['nombreConcepto']
									jaux['codigo_combinacion'] = str(transaccion['idCombinacion'])
									jaux['tipo_detalle'] = '2'
                                                                	lista.append(jaux)
        elif 'requerimiento' in json1:
		#FGE - 20191219 - En caso de que no venga el requerimiento....
		if json1['requerimiento'] != None:
			unidadesDemandantes = ''
			for agrupacion in json1['requerimiento']['agrupacionesDeImputacionesACatalogos']['agrupacion']:
				catalogosDescripcion = ''
				catalogo_tipo = ''
				catalogo_codigo = ''
				for catalogo in agrupacion['imputacionesACatalogosDeReagrupacion']['imputacion']:
					if catalogo['catalogo'] == 'UnidadesDemandantes':
						unidadesDemandantes = catalogo['descripcion']
					if ((catalogo['descripcion'] != 'No Aplica') and (catalogo['catalogo'] != 'programaPresupuestario')):
						catalogosDescripcion = '%s%s / ' % (catalogosDescripcion, catalogo['descripcion'])
					if ((catalogo['catalogo'] != 'programaPresupuestario') and (catalogo['catalogo'] != 'UnidadesDemandantes')):
						catalogo_tipo = catalogo['catalogo']
						catalogo_codigo = catalogo['elemento']
				for imputacion in agrupacion['imputacionesAConceptosPresupuestarios']['imputacion']:
					for cuentaDebe in imputacion['cuentas']['cuentasDebe']:
						jaux = {}
						jaux['folio'] = ''
						jaux['requerimiento'] = str(json1['requerimiento']['requerimiento'])
						jaux['unidad_demandante'] = unidadesDemandantes
						jaux['catalogos'] = catalogosDescripcion[:-2]
						jaux['catalogo_tipo'] = catalogo_tipo
						jaux['catalogo_codigo'] = catalogo_codigo
						jaux['cuenta_debe'] = str(cuentaDebe['codigo'])
						jaux['cuenta_haber'] = str(imputacion['cuentas']['cuentasHaber'][0]['codigo'])
						jaux['concepto_presupuestario_debe'] = cuentaDebe['nombre']
						jaux['concepto_presupuestario_haber'] = imputacion['cuentas']['cuentasHaber'][0]['nombre']
						jaux['monto_compromiso'] = str(imputacion['monto'])
						jaux['monto_debe'] = str(0)
						jaux['codigo_imputacion'] = str(imputacion['idConcepto'])
						jaux['nombre_imputacion'] = imputacion['nombreConcepto']
						jaux['codigo_combinacion'] = str(agrupacion['idAgrupacionAjustada'])
						jaux['tipo_detalle'] = '1'
                                        	lista.append(jaux)

        if len(lista)==0:
                json_out['RESPUESTA']='SIN_DATA'
        else:
                json_out['RESPUESTA']='OK'
                json_out['LISTA']=lista
	# FGE - 20191219 - Anexamos la descripcion de status
        if 'descripcion' in json1:
                 descripcion_extendida = ''
                 if 'errors' in json1:
                         for d_extendida in json1['errors']:
                                 descripcion_extendida = descripcion_extendida + d_extendida['descripcion'] + ', '
                         descripcion_extendida = descripcion_extendida[:-2]
                         json_out['error_descripcion'] = '%s - %s - %s' % (json1['status'], json1['descripcion'], descripcion_extendida)
#except Exception as e:
#       json_out['RESPUESTA']='ERROR'

return json.dumps(json_out)
$function$ language plpythonu;


CREATE OR REPLACE FUNCTION public.detalle_cuentas(json)
 RETURNS json
 LANGUAGE plpgsql
AS $function$
DECLARE
        json1                alias for $1;
        json2                json;
        json3                json;
        v_lista              varchar;
        v_codigo_dv          varchar;
        v_codigo_txel        varchar;
        v_cod_imputacion     varchar;
        v_cod_compromiso     varchar;
        v_cod_requerimiento  varchar;
        v_estado             varchar;
        v_reversa            varchar;
BEGIN
        json2:=json1;
        v_codigo_txel:=get_json('codigo_txel', json2);
        v_reversa:=get_json('reversa', json2);
        select codigo_dv, estado into v_codigo_dv, v_estado from dp_devengo where dte_codigo_txel = v_codigo_txel and case when v_reversa = 'Y' then tipo_dte = 0 else tipo_dte <> 0 end;

        v_cod_imputacion:=split_part(get_json('id_detalle', json2), '___', 1);
        v_cod_compromiso:=split_part(get_json('id_detalle', json2), '___', 2);
        v_cod_requerimiento:=split_part(get_json('id_detalle', json2), '___', 3);

        json3:='{}';
        json3:=put_json(json3,'flag_paginacion','NO');
        json3:=put_json(json3,'flag_paginacion_manual','NO');
        json3:=put_json(json3,'flag_tipo_cuadro','GRILLA');
        json3:=put_json(json3,'registros_por_pagina','25');
        json3:=put_json(json3,'mostrar_paginador','false'); -- no muestra el paginador del datatables
        json3:=put_json(json3,'mostrar_info','false'); -- no muestra el mensaje de paginación
        json3:=put_json(json3,'mostrar_filtros','false'); -- no muestra el buscar del plugin
        json3:=put_json(json3,'MENSAJE_VACIO','No hay registros');

        execute 'select array_to_json(array_agg(row_to_json(sql))) from (
                 select id as info_sin_formato__id__off,
                        cod_combinacion as info_sin_formato__cod_combinacion__off,
                         ''[{"id_pantalla":"devengo_manual","id_objeto":19,"titulo":"Modificar","valor":"Modificar Cuenta Contable","labels_no_editable":["ID", "MONTO", "NOMBRE_CUENTA_CONTABLE", "CUENTA_CONTABLE"],"labels_editable":null,"funcion":"modal_devengo_modifica_cuenta.showRowModal","select_inputs":null,"flag_masivo":null,"categoria":"TEXT","id_modal":"modal_devengo_modifica_cuenta","funcion_modal":null,"tx":null,"tipo":null,"href":null,"glyphicon":"Modificar","id_div":"grilla_devengo_manual","rol":null,"orden":null,"id_grupo_elementos":null,"color_icono":null,"informacion":null,"caption":null,"color":"orange"}]''::json as group_gly__modificar__' || case when v_estado in ('BORRADOR', 'REPROCESO') then 'on' else 'off' end || ',
                         cuenta_debe as info_sin_formato__cuenta_contable__on,
                         concepto_presupuestario_debe as info__nombre_cuenta_contable__on,
                         debe as info__monto__on
                from dp_devengo_detalle
                where codigo_dv = ' || v_codigo_dv || '::bigint and cod_imputacion = ''' || v_cod_imputacion || ''' and folio = ''' || v_cod_compromiso || ''' and requerimiento = ''' || v_cod_requerimiento || ''' order by cuenta_debe) sql' into v_lista;

        json3:=put_json(json3,'datos_tabla', v_lista::varchar);
        json2:=response_requests_6000('1', 'OK', json3::varchar,json2);

        RETURN json2;
END;
$function$





