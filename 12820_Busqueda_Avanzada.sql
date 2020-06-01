delete from isys_querys_tx where llave='12820';
insert into isys_querys_tx values ('12820',10,1,2,'Busqueda Avanzada de Compromisos',4013,300,101,0,0,20,20);
insert into isys_querys_tx values ('12820',20,1,1,'select procesa_resp_ms_12820(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

create or replace function dp_busqueda_avanzada(json)
 returns json
 as $function$
declare
        json1                 alias for $1;
        json2                 json;
        json3                 json;

        v_codigo_txel         varchar;
        v_area_transaccional  varchar;
        v_par                 varchar;
        v_cap                 varchar;
        v_are                 varchar;
        v_ejercicio           varchar;
        v_tipo                varchar;
        v_fecha_inicio        varchar;
        v_fecha_fin           varchar;

        v_fecha_emision       date;

        v_reg_devengo         record;

--        folio1                varchar;
begin
        json2:=json1;
	json3:='{}';
        
        v_codigo_txel := get_json('codigo_txel', json2);
        if v_codigo_txel = '' then
                json2:=response_requests_6000('2', 'No Existe el Codigo Txel.', '', json2);
                return json2;
        end if;

        select area_transaccional, periodo, ejercicio, rut_receptor, dte_codigo_txel from dp_devengo where dte_codigo_txel = v_codigo_txel and tipo_dte <> 0 into v_reg_devengo;
        select fecha_emision into v_fecha_emision from dte_recibidos where codigo_txel = v_reg_devengo.dte_codigo_txel::bigint;

        if not found then
                json2:=response_requests_6000('2', 'No Existe el Devengo', '', json2);
                return json2;
        end if;

        if v_reg_devengo.area_transaccional = '' then
                json2:=response_requests_6000('2', 'No Existe el Area Transaccional', '', json2);
                return json2;
        end if;

        v_par:=substring(v_reg_devengo.area_transaccional, 1, 2);
        v_cap:=substring(v_reg_devengo.area_transaccional, 3, 2);
        v_are:=substring(v_reg_devengo.area_transaccional, 5, 3);
        v_ejercicio:=v_reg_devengo.ejercicio::varchar;
        v_tipo:='01';
        v_fecha_inicio:=to_char(date_trunc('year', now()), 'YYYY-mm-dd');
        v_fecha_fin:=to_char(now(), 'YYYY-mm-dd');

        json3:=put_json(json3, 'partida', v_par);
        json3:=put_json(json3, 'capitulo', v_cap);
        json3:=put_json(json3, 'areaTransaccional', v_are);
        json3:=put_json(json3, 'ejercicio', v_ejercicio);
        json3:=put_json(json3, 'tipoDocumento', '1600');
        json3:=put_json(json3, 'rut', v_reg_devengo.rut_receptor::varchar || '-' || modulo11(v_reg_devengo.rut_receptor::varchar) );
        json3:=put_json(json3, 'fechaEmision', v_fecha_emision::varchar);


        --llamo al flujo y paso los parametros del servicios para hacer la peticion
        json2:=put_json(json2,'LLAMA_FLUJO','SI');
        json2:=put_json(json2,'__SECUENCIAOK__','12820');
        json2:=get_parametros_motor_json(json2,'BUSQUEDAAVANZADA_CHC');
        json2:=put_json(json2,'URI_MS','chilecompra/buscarcompromiso');
        json2:=put_json(json2,'HOST_MS','servicios.acepta.com');
        json2:=put_json(json2,'__TIMEOUT_SERV_PXML__','30');
        json2:=put_json(json2,'DATA_JSON',encode_hex(json3::varchar));
        json2:=put_json(json2,'LARGO_JSON',(length(encode_hex(json3::varchar))/2)::varchar);

        json2:=response_requests_6000('1', 'OK', '', json2);

        perform logfile('----- FGE - llamando 12820: ' || json3::varchar);

        return json2;
end;
$function$ language plpgsql;


create or replace function procesa_resp_ms_12820(json)
 returns json
 as $function$
declare
	json1           alias for $1;
	json2           json;
	json3           json;

        resp1           varchar;

        json_descargar  varchar;
        json_respuesta  json;
begin
	json2:=json1;
        json2:=regexp_replace(json2::varchar, '[^\x20-\x7f\x0d\x1b\xf1\xd1À-ü]', '', 'g')::json;
	--json2:=logjson(json2,'[procesa_resp_ms_12820] json2 entrada=>'||json2::varchar);
        perform logfile('----- FGE - 12820 - json2 entrada: ' || json2::varchar);

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
	--json2:=logjson(json2,'[procesa_resp_ms_12820] resp1='||resp1);
        --perform logfile('----- FGE - 12820 - resp1: ' || resp1::varchar);

        select row_to_json(sql) from (select *, 'orange' as color from acciones_grillas where id_pantalla = get_json('app_dinamica', json2) and valor = 'Descargar') sql into json_descargar;
        perform logfile('----- FGE - 12820 - json_descargar: ' || json_descargar::varchar);

	resp1:=procesa_json_12820(resp1, json_descargar::varchar);
        --perform logfile('----- FGE - 12820 - 2 - resp1: ' || resp1::varchar);

	if get_json('RESPUESTA',resp1::json)='OK' then
                json_respuesta := replace(REGEXP_REPLACE(get_json('LISTA',resp1::json)::varchar, '\d{1}\#\#', '','g'), '"[##acciones##]"', '[' || json_descargar || ']')::json;
                json3:=put_json(json3, 'datos_tabla', json_respuesta::varchar);


                perform logfile('----- FGE - 12820 respuesta: ' || json_respuesta::varchar);



                json3:=put_json(json3, 'NO_RELOAD_DIVS', put_json_list(put_json_list(put_json_list('[]', 'form_devengo_manual_cabecera'), 'form_devengo_manual_cuentas'), 'form_devengo_manual_catalogo'));

	elsif get_json('RESPUESTA',resp1::json)='SIN_DATA' then
		json3:=put_json(json3,'MENSAJE_VACIO','No hay registros');
	elsif get_json('RESPUESTA',resp1::json)='ERROR' then
		json3:=put_json(json3,'MENSAJE_VACIO','Ha ocurrido un error al consultar la informacion');
	end if;
	json2:=put_json(json2,'RESPUESTA_COLA','OK');
	json2:=response_requests_6000('1', 'OK', json3::varchar, json2);
        return json2;
end;
$function$ language plpgsql;

create or replace function procesa_json_12820(json_in text, acciones_in text)
returns text
as $function$
import json

json_out={}
lista = []
json1=json_in
acciones=acciones_in
json1=json1.decode('ascii', 'ignore')
json1=json.loads(json_in, strict = 'false')
if 'resumenesDeCompromisos' in json1:
        if 'compromiso' in json1['resumenesDeCompromisos']:
                if json1['resumenesDeCompromisos']['compromiso'] != None:
                        for compromiso in json1['resumenesDeCompromisos']['compromiso']:
                                temp = {}
                                temp['info_sin_formato__id__on'] = compromiso['folio']
                                temp['dropdown__accion__on'] = '[##acciones##]'
                                #temp['GROUP_GLY1__ACCION__NN'] = '[##acciones##]'
                                temp['info__titulo__on'] = '<span width="250px">%s</span>' % compromiso['titulo']
                                temp['info_sin_formato__monto__on'] = compromiso['monto']
                                temp['info__saldo__on'] = compromiso['saldo']
                                lista.append(temp)
if len(lista) > 0:
        if len(lista)==0:
                json_out['RESPUESTA']='SIN_DATA'
        else:
                json_out['RESPUESTA']='OK'
                json_out['LISTA']=lista
return json.dumps(json_out)
$function$ language plpythonu;





