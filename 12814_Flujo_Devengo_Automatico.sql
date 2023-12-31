delete from isys_querys_tx where llave='12814';
insert into isys_querys_tx values ('12814',10,1,2,'Arma el Json del Devengo y lo envia',4013,300,101,0,0,20,20);
insert into isys_querys_tx values ('12814',20,1,1,'select procesa_resp_ms_12814(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,0,0);


CREATE OR REPLACE FUNCTION public.dp_envia_devengo_12814(json)
    RETURNS json
AS $function$
declare
    json1                     alias for $1;
    json2                     json;

    v_codigo_dv               varchar;
    v_completitud             json;
    v_titulo_devengo          varchar;
    v_descripcion_devengo     varchar;
    v_reg_devengo             record;
    v_num_catalogos           integer;
    v_catalogo_propio         varchar;
    v_catalogos_propios       varchar;
    v_fecha_cumplimiento      varchar;
    v_contabiliza_iva         varchar;
    v_id_documento_ajustado   varchar:='';
    v_id_agrupacion_ajustada  varchar:='';
    v_ref_folio               varchar:='';
    v_ref_tipo_dte            integer;

    v_json_devengo            json;
    v_json_devengo_detalle    json;
    v_json_dte                json;
    v_json_devengo_sigfe      json;
    v_json_rc                 json;
    v_id_solicitud bigint;
        cod1    varchar;
        v_iva_pantalla            varchar;
        v_catalogo_propio_inversion  varchar;
    v_json_log                varchar;
begin
    json2:=json1;
    v_codigo_dv:=coalesce(get_json('codigo_dv', json2), '0');
    v_titulo_devengo:=coalesce(get_json('titulo_devengo', json2), '');
    v_descripcion_devengo:=coalesce(get_json('descripcion_devengo', json2), '');
    v_catalogo_propio:=get_json('catalogo_propio', json2);
    v_catalogo_propio_inversion:=get_json('catalogo_propio_inversion', json2);

    --raise notice '12814 -> v_codigo_dv: %', v_codigo_dv;

    if v_codigo_dv in ('0', '') then
        v_id_solicitud:=nullif(get_json('id_solicitud', json2), '')::bigint;
        -- FGE - 20200723 - Mejoramos la parte de busqueda de codigo_dv, validamos el select y devolvemos error si no se encuentra
        select get_xml('CODIGO_DEVENGO', data_dte),codigo_txel::varchar into v_codigo_dv,cod1 from dte_recibidos where codigo_txel = (select get_campo('CODIGO_TXEL', xml2) from workflow_controller where id_solicitud = v_id_solicitud)::bigint;
        if found then
            json2:=put_json(json2, 'codigo_dv', v_codigo_dv);
        else
            json2:=response_requests_6000('2', 'Imposible localizar el codigo devengo', '', json2);
            return json2;
        end if;
    end if;
    json2:=put_json(json2,'LLAMA_FLUJO','NO');

    v_completitud:=dp_verifica_completitud(json2);

    --FAY QUE ES ESTO RUBEN ???
    --update pantalla_dinamica set tipo='INPUT' where nombre='folio_desde' and strpos(condicion,'''##TIPO_DTE##''=''801''')>0;

    if v_titulo_devengo <> '' then
        update dp_devengo set titulo = substring(v_titulo_devengo, 1, 79) where codigo_dv = v_codigo_dv::bigint returning dte_codigo_txel into cod1;
    end if;

    if v_descripcion_devengo <> '' then
        update dp_devengo set descripcion = substring(v_descripcion_devengo, 1, 250) where codigo_dv = v_codigo_dv::bigint returning dte_codigo_txel into cod1;
    end if;

    if v_catalogo_propio <> '' then
        -- FGE - 20200121 - Caso especial de Capredena
        if get_json('rutCliente', json2) = '61108000' then
            update dp_devengo set catalogo_propio = (v_catalogo_propio::integer)::varchar where codigo_dv = v_codigo_dv::bigint returning dte_codigo_txel into cod1;
        else
            update dp_devengo set catalogo_propio = v_catalogo_propio where codigo_dv = v_codigo_dv::bigint returning dte_codigo_txel into cod1;
        end if;
    end if;

    -- FGE - 20200616 - Se incluye catalogo iniciativaInversion
    if v_catalogo_propio_inversion <> '' then
        update dp_devengo set catalogo_propio_inversion = v_catalogo_propio_inversion where codigo_dv = v_codigo_dv::bigint;
    end if;
    -- FGE - 20200220 - Correccion para iva institucional
    v_iva_pantalla := get_json('contabilizar_iva', json2);
    update dp_devengo set contabilizar_iva = case when v_iva_pantalla = 'on' then true else false end where codigo_dv = v_codigo_dv::bigint;

    if get_json('STATUS', v_completitud) = 'NK' then
        if cod1 is null then
                select dte_codigo_txel into cod1 from dp_devengo where codigo_dv = v_codigo_dv::bigint;
        end if;
        update dte_recibidos set data_dte = put_data_dte(data_dte, 'TIPO_DEVENGO', 'MAN') where codigo_txel = cod1::bigint;
        json2:=response_requests_6000('2', get_json('ERRORES', v_completitud), '', json2);
        return json2;
    end if;

    -- FGE - 20190531 - Verificacion de las instituciones que necesitas catalogo "propioContabilidad"
    -- FGE - 20200616 - Agregamos catalogo iniciativaInversion
    --select count(1) into v_num_catalogos from dp_catalogos, (select left(area_transaccional, 4) as codigo_institucion, ejercicio from dp_devengo where codigo_dv = v_codigo_dv::bigint) devengo where dp_catalogos.codigo_institucion = devengo.codigo_institucion and dp_catalogos.ejercicio = devengo.ejercicio and dp_catalogos.nombre_catalogo = 'propioContabilidad';
    select count(1) into v_num_catalogos from dp_catalogos, (select left(area_transaccional, 4) as codigo_institucion, ejercicio from dp_devengo where codigo_dv = v_codigo_dv::bigint) devengo where dp_catalogos.codigo_institucion = devengo.codigo_institucion and dp_catalogos.ejercicio = devengo.ejercicio and dp_catalogos.nombre_catalogo in ('propioContabilidad', 'iniciativaInversion');
    if v_num_catalogos <> 1 then
        v_catalogos_propios := 'S';
    else
        v_catalogos_propios := 'N';
    end if;

    -- FGE - 20190522 - Se incluye el codigo OC y RC para poder determinar la fecha recepcion
    select codigo_dv, dte_codigo_txel, estado, uri_dte, codigo_oc, codigo_rc, rut_emisor, tipo_dte, ref_codigo_dv, id_documento_ajustado, id_agrupacion_ajustada from dp_devengo where codigo_dv = v_codigo_dv::bigint into v_reg_devengo;

    if v_reg_devengo.tipo_dte in (0, 56, 61) and coalesce(v_reg_devengo.ref_codigo_dv::varchar, '') <> '' then
        --select to_char(to_date(fecha_emision, 'YYYY-MM-DD') + interval '30 days', 'YYYY-MM-DD') into v_fecha_cumplimiento from dte_recibidos where codigo_txel = (select dte_codigo_txel from dp_devengo where codigo_dv = v_reg_devengo.ref_codigo_dv)::bigint;
        select to_char(fecha_sii::date + interval '30 days', 'YYYY-MM-DD') into v_fecha_cumplimiento from dte_recibidos where codigo_txel = (select dte_codigo_txel::bigint from dp_devengo where codigo_dv = v_reg_devengo.ref_codigo_dv);
        select case when contabilizar_iva = true then 'Y' else 'N' end, id_documento_ajustado, id_agrupacion_ajustada, folio, tipo_dte into v_contabiliza_iva, v_id_documento_ajustado, v_id_agrupacion_ajustada, v_ref_folio, v_ref_tipo_dte from dp_devengo where codigo_dv = v_reg_devengo.ref_codigo_dv;
    end if;

    --FAY 2019-06-06
    if not found then
        json2:=response_requests_6000('2', 'Error no se encuentra el devengo en dp_devengo '||v_codigo_dv::varchar, '', json2);
        return json2;
    end if;

    -- FGE - 20210224 - Agregamos el estado GENERANDO para controlar la multiple generacion con doble click.
    if v_reg_devengo.estado in( 'EN_PROCESO' , 'FINALIZADO_SIN_ERRORES','FINALIZADO_CON_ERRORES','MANUAL_SIGFE', 'GENERANDO' ) then
        select respuesta into v_json_log from dp_medicion_servicios where devengo = v_codigo_dv::varchar order by fecha desc limit 1;
        if found then
            if get_json('codigo', v_json_log::json) <> '' then
                update dp_devengo set ticket_id = get_json('codigo', v_json_log::json), estado = 'EN_PROCESO' where codigo_dv = v_codigo_dv::bigint;
                json2:=response_requests_6000('1', 'Enviado: TrackID - ' || get_json('codigo', v_json_log::json) || ', Descripción: ' || get_json('descripcion', v_json_log::json), '', json2);
            else
                json2:=response_requests_6000('2', 'El Devengo ya fue generado anteriormente', '', json2);
            end if;
        end if;
        return json2;
    else
       perform dp_act_estado_devengo(v_reg_devengo.codigo_dv, 'GENERANDO', '');
    end if;

    -- Info del devengo
    -- FGE - 20200616 - Agregamos catalogo iniciativaInversion
    --select array_to_json(array_agg(row_to_json(sql))) into v_json_devengo from (select codigo_dv::varchar, area_transaccional, periodo::varchar, ejercicio::varchar, regexp_replace(titulo, '[^\x20-\x7f\x0d\x1b\xf1\xd1À-ü]', '', 'g') as titulo, regexp_replace(descripcion, '[^\x20-\x7f\x0d\x1b\xf1\xd1À-ü]', '', 'g') as descripcion, monto_devengo, rut_receptor::varchar || '-' || modulo11(rut_receptor::varchar) as rut_receptor, codigo_oc, folio_requerimiento, case when contabilizar_iva = true then 'Y' else 'N' end as contabilizar_iva, catalogo_propio, v_catalogos_propios as flag_catalogo_propio, ref_codigo_dv, v_id_documento_ajustado as id_documento_ajustado, v_id_agrupacion_ajustada as id_agrupacion_ajustada, v_contabiliza_iva as contabiliza_referencia, ref_codigo_dv, v_ref_folio as ref_folio, tipo_dte, v_ref_tipo_dte as ref_tipo_dte from dp_devengo where codigo_dv = v_codigo_dv::bigint) sql;
    select array_to_json(array_agg(row_to_json(sql))) into v_json_devengo from (select codigo_dv::varchar, area_transaccional, periodo::varchar, ejercicio::varchar, regexp_replace(titulo, '[^\x20-\x7f\x0d\x1b\xf1\xd1À-ü]', '', 'g') as titulo, regexp_replace(descripcion, '[^\x20-\x7f\x0d\x1b\xf1\xd1À-ü]', '', 'g') as descripcion, monto_devengo, rut_receptor::varchar || '-' || modulo11(rut_receptor::varchar) as rut_receptor, codigo_oc, folio_requerimiento, case when contabilizar_iva = true then 'Y' else 'N' end as contabilizar_iva, catalogo_propio, catalogo_propio_inversion, v_catalogos_propios as flag_catalogo_propio, ref_codigo_dv, v_id_documento_ajustado as id_documento_ajustado, v_id_agrupacion_ajustada as id_agrupacion_ajustada, v_contabiliza_iva as contabiliza_referencia, ref_codigo_dv, v_ref_folio as ref_folio, tipo_dte, v_ref_tipo_dte as ref_tipo_dte from dp_devengo where codigo_dv = v_codigo_dv::bigint) sql;

    -- Info del DTE
    if coalesce(v_reg_devengo.dte_codigo_txel,'') = '' then
            select array_to_json(array_agg(row_to_json(sql))) into v_json_dte from (select folio::varchar, tipo_dte::varchar, fecha_emision, fecha_sii, fecha_ingreso, monto_iva, monto_neto, monto_excento, v_fecha_cumplimiento as fecha_cumplimiento, get_xml('FmaPago', data_dte) as forma_pago,impuestos from dte_recibidos where uri=v_reg_devengo.uri_dte) sql;
    else
            select array_to_json(array_agg(row_to_json(sql))) into v_json_dte from (select folio::varchar, tipo_dte::varchar, fecha_emision, fecha_sii, fecha_ingreso, monto_iva, monto_neto, monto_excento, v_fecha_cumplimiento as fecha_cumplimiento, get_xml('FmaPago', data_dte) as forma_pago,impuestos from dte_recibidos where codigo_txel = v_reg_devengo.dte_codigo_txel::bigint) sql;
    end if;

    -- Info del detalle de las cuentas contables
    select array_to_json(array_agg(row_to_json(sql))) into v_json_devengo_detalle from (select folio, cod_imputacion, debe, cuenta_debe, cuenta_haber, cod_combinacion, requerimiento, tipo_detalle, catalogo_tipo, cod_catalogo,id_agrupacion_ajustada  from dp_devengo_detalle where codigo_dv = v_codigo_dv::bigint and debe<>0) sql;

    -- FGE - 20190522 - Información del RC
    select array_to_json(array_agg(row_to_json(sql))) into v_json_rc from (select id, fecha_ingreso from token_de_emitidos where rut_emisor = v_reg_devengo.rut_emisor and tipo_dte = 801 and folio = v_reg_devengo.codigo_oc and token = v_reg_devengo.codigo_rc) sql;

    --FAY-DAO 2019-06-06 Si alguna data es nula, se contesta error
    if v_json_dte is null then
        json2:=response_requests_6000('2', 'Error en encontrar el documento en los recibidos', '', json2);
        return json2;
    end if;

    if v_json_devengo is null then
        json2:=response_requests_6000('2', 'Error en encontrar devengo', '', json2);
        return json2;
    end if;

    if v_json_devengo_detalle is null then
        json2:=response_requests_6000('2', 'Error en encontrar detalle del devengo', '', json2);
        return json2;
    end if;

    v_json_devengo_sigfe:=dp_arma_json_devengo(v_json_devengo::varchar, v_json_devengo_detalle::varchar, v_json_dte::varchar, 'interDTE', v_json_rc::varchar);

    -- Cargo los parametros para el flujo y paso el json del devengo
    json2:=put_json(json2,'LLAMA_FLUJO','SI');
    json2:=put_json(json2,'__SECUENCIAOK__','12814');
    json2:=get_parametros_motor_json(json2,'REGISTRADEVENGO_CHC');
    json2:=put_json(json2,'HOST_MS',get_json('__IP_CONEXION_CLIENTE__', json2));
    json2:=put_json(json2,'URI_MS',get_json('PARAMETRO_RUTA', json2));
    json2:=put_json(json2,'DATA_JSON',encode_hex(v_json_devengo_sigfe::varchar));
    json2:=put_json(json2,'LARGO_JSON',(length(encode_hex(v_json_devengo_sigfe::varchar))/2)::varchar);

    json2:=logjson(json2,'Llama Flujo 12814');
    --json2:=response_requests_6000('2', 'Error de conexión, inténtelo nuevamente', '', json2);
    return json2;

end;
$function$ language plpgsql;


create or replace function dp_arma_json_devengo(devengo_in text, devengo_detalle_in text, dte_in text, usuario_sigfe_in text, rc_in text)
    returns text
    as $function$
import json
import datetime
import string
import os
import re

def escribe_log(txt):
        fecha=datetime.datetime.now()
        file1="/var/log/postgresql/logapp/dipres_"+(fecha.strftime("%Y%m%d"))+".log"
        with os.fdopen(os.open(file1, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0644),'w') as f:
                f.write('%s PID=%s %s\n' %(fecha.strftime("%H:%M:%S,%f"),str(os.getpid()),txt))
        os.chmod(file1, 0777)
        return 1

printable = set(string.printable)

# Primero tabla de correspondencias entre tipo_dte y el codigo de sigfe
tipo_dte={}
tipo_dte['33'] = '0102'
tipo_dte['34'] = '0202'
tipo_dte['56'] = '0502'
tipo_dte['61'] = '0402'
tipo_dte['0'] = ''

# Luego limpio la variable de salida
json_out = {}
# Convierto los parametros de entrada a json
json_devengo = json.loads(devengo_in)[0]
json_devengo_detalle = json.loads(devengo_detalle_in)
json_dte = json.loads(dte_in)[0]

temp_rc_in = rc_in
escribe_log('json_devengo='+devengo_in+' json_devengo_detalle='+devengo_detalle_in+' json_dte='+dte_in+' temp_rc_in='+str(rc_in))

if temp_rc_in != None and temp_rc_in != '':
        temp_fecha = str(json.loads(temp_rc_in)[0]['fecha_ingreso'])
        fecha_recepcion_conforme = '%s-%s-%s' % (temp_fecha[:4], temp_fecha[4:6], temp_fecha[6:8])
else:
        fecha_recepcion_conforme = None

# La fecha es la fecha de emision del documento
fecha = json_dte['fecha_emision']

#Si la fecha de ingreso es menor que la fecha de emision, entonces tomamos la fecha de emision
if json_dte['fecha_ingreso'][:10] > fecha:
        fecha_ingreso = json_dte['fecha_ingreso'][:10]
else:
        fecha_ingreso = fecha

escribe_log("Fecha="+str(fecha)+" fecha_ingreso="+str(fecha_ingreso)+" fecha_recepcion_conforme="+str(fecha_recepcion_conforme))

# Si no existe fecha de RC
if fecha_recepcion_conforme == None:
        escribe_log("fecha_recepcion_conforme == None")
        fecha_recepcion_conforme = fecha_ingreso
elif fecha_recepcion_conforme < fecha_ingreso:
        escribe_log("fecha_recepcion_conforme < fecha_ingreso")
        fecha_recepcion_conforme = fecha_ingreso
#FGE - 20200131 - Evito que Fecha RC sea del ejercicio anterior
if fecha_recepcion_conforme < datetime.date.strftime(datetime.datetime.now().replace(month=1, day=1), '%Y-%m-%d'):
        fecha_recepcion_conforme = datetime.date.strftime(datetime.datetime.now().replace(month=12, day=31), '%Y-%m-%d')

escribe_log("Fecha="+str(fecha)+" fecha_ingreso="+str(fecha_ingreso)+" fecha_recepcion_conforme="+str(fecha_recepcion_conforme))

#if temp_rc_in != None and temp_rc_in != '':
#        fecha_recepcion_conforme = json.loads(temp_rc_in)[0]['fecha_ingreso']
#else:
#        fecha_recepcion_conforme = None

# Creamos el messageID "Unico"
messageID = '%s%07d' % (datetime.datetime.now().strftime('%y%m%d%H%M%S'), int(json_devengo['codigo_dv']))

#
# Creacion de la cabecera
cabecera = {}
cabecera['ejercicio'] = json_devengo['ejercicio']
cabecera['periodo'] = json_devengo['periodo']
cabecera['proceso'] = '0301'
cabecera['usuarioSigfe'] = usuario_sigfe_in
cabecera['messageID'] = messageID  #message_id_in
# Construyo la institucion
institucion = {}
institucion['partida'] = json_devengo['area_transaccional'][0:2]
institucion['capitulo'] = json_devengo['area_transaccional'][2:4]
institucion['areaTransaccional'] = json_devengo['area_transaccional'][4:7]
cabecera['institucion'] = institucion

# Finalmente el detalle del devengo
detalles = {}
devengos = []
devengo = {}
catalogos_imputados = {"imputacion": []}
devengo['id'] = json_devengo['codigo_dv']
devengo['titulo'] = re.sub(r'[^\x20-\x7f\x0d\x1b\xf1\xd1]', '', json_devengo['titulo'])[:79]
devengo['descripcion'] = re.sub(r'[^\x20-\x7f\x0d\x1b\xf1\xd1]', '', json_devengo['descripcion'])[:249]
devengo['origenTransaccion'] = 'VERTICALES_DTE'
# Documentos
documentos = {}
# Documentos
documento = []
# Documento del devengo...
documento_devengo = {}
documento_devengo['tipo'] = tipo_dte[str(json_devengo['tipo_dte'])]

# FGE - 20200611 - Calculo de montos ahora incluye facturas mixtas
documento_devengo['monto'] = abs(json_dte['monto_neto']) + abs(json_dte['monto_excento'])
if int(json_dte['tipo_dte']) == 61:
    documento_devengo['monto'] = documento_devengo['monto'] * -1
if int(json_devengo['tipo_dte']) == 0:
    documento_devengo['monto'] = documento_devengo['monto'] * -1

if json_devengo['tipo_dte'] != 0:
    documento_devengo['numero'] = json_dte['folio']
else:
    documento_devengo['numero'] = ''

if json_devengo['tipo_dte'] != 0:
    documento_devengo['fecha'] = fecha
    documento_devengo['fechaIngreso'] = fecha_ingreso
if json_devengo['tipo_dte'] in [33, 34]:
    documento_devengo['fechaRecepcionConforme'] = fecha_recepcion_conforme
documento_devengo['descripcion'] = json_devengo['descripcion'][:249]
#documento_devengo['fechaRecepcionConforme'] = json_dte['fecha_sii'][:10]
if str(json_devengo['codigo_oc']) <> '' and str(json_devengo['codigo_oc']) != 'None':
    documento_devengo['numeroOrdenCompra'] = str(json_devengo['codigo_oc'])
#else:
#    documento_devengo['numeroOrdenCompra'] = None
if json_devengo['tipo_dte'] in [0, 56,  61]:
    documento_devengo['idDocumentoAjustado'] = json_devengo['id_documento_ajustado']
### FGE - 20190814 - Agrego el marcador si es un pago de contado
documento_devengo['compensable'] = 'true' if str(json_dte['forma_pago']) == '1' else 'false'

principales = {}
principal = []
principal_devengo = {}
if json_devengo['tipo_dte'] != 0:
    principal_devengo['id'] = json_devengo['rut_receptor']
else:
    principal_devengo['id'] = ''
cumplimientos = {}
cumplimiento = []
cumplimiento_devengo = {}
if ((json_devengo['tipo_dte'] == 56) or (json_devengo['tipo_dte'] == 61)):
    cumplimiento_devengo['fecha'] = json_dte['fecha_cumplimiento']
else:
    cumplimiento_devengo['fecha'] = datetime.datetime.strftime(datetime.datetime.strptime(json_dte['fecha_sii'][:10], '%Y-%m-%d') + datetime.timedelta(days = 30), '%Y-%m-%d')

#if json_devengo['tipo_dte'] == 33 or (json_devengo['tipo_dte'] == 56 and json_devengo['ref_tipo_dte'] == 33):
#    cumplimiento_devengo['monto'] = abs(son_dte['monto_neto'])
#elif json_devengo['tipo_dte'] == 34 or (json_devengo['tipo_dte'] == 56 and json_devengo['ref_tipo_dte'] == 34):
#    cumplimiento_devengo['monto'] = abs(json_dte['monto_excento'])
#elif json_devengo['tipo_dte'] in [0, 61] and json_devengo['ref_tipo_dte'] == 33:
#    cumplimiento_devengo['monto'] = abs(json_dte['monto_neto']) * -1
#elif json_devengo['tipo_dte'] in [0, 61] and json_devengo['ref_tipo_dte'] == 34:
#    cumplimiento_devengo['monto'] = abs(json_dte['monto_excento']) * -1

# Monto Devengo = Monto cumplimiento
cumplimiento_devengo['monto'] = documento_devengo['monto']

cumplimiento_devengo['idPrincipalRelacionado'] = json_devengo['rut_receptor']
cumplimiento.append(cumplimiento_devengo)
cumplimientos['cumplimiento'] = cumplimiento
principal_devengo['cumplimientos'] = cumplimientos
transacciones_previas = {}
transaccion = []
transaccion_lista = []
for cuenta in json_devengo_detalle:
    transaccion_temp = None
    #if json_devengo['tipo_dte'] <> 0:
    if 1==1:
        for transaccion in transaccion_lista:
            if cuenta['tipo_detalle'] == 2:
                if transaccion['folio'] == cuenta['folio'] and transaccion['tipo'] == str(cuenta['tipo_detalle']) and transaccion['idAgrupacionDeReferencia'] == cuenta['cod_combinacion']:
                    transaccion_temp = transaccion
            else:
                if transaccion['folio'] == cuenta['requerimiento'] and transaccion['tipo'] == str(cuenta['tipo_detalle']) and transaccion['idAgrupacionDeReferencia'] == cuenta['cod_combinacion']:
                    transaccion_temp = transaccion
    if transaccion_temp == None:
        transaccion_temp = {}
        if ((json_devengo['tipo_dte'] in [33, 34,0])):
            #if json_devengo['tipo_dte'] <> 0:
            if 1==1:
                if cuenta['tipo_detalle'] == 2:
                    transaccion_temp['folio'] = cuenta['folio']
                else:
                    transaccion_temp['folio'] = cuenta['requerimiento']
            transaccion_temp['tipo'] = str(cuenta['tipo_detalle'])
            transaccion_temp['idAgrupacionDeReferencia'] = cuenta['cod_combinacion']

        if json_devengo['tipo_dte'] in [0, 56, 61]:
            # FGE - 20201002 - Correccion para NC con multiples compromisos / requerimientos
            #if json_devengo['tipo_dte'] <> 0:
            if 1==1:
                if cuenta['tipo_detalle'] == 2:
                    transaccion_temp['folio'] = cuenta['folio']
                else:
                    transaccion_temp['folio'] = cuenta['requerimiento']
            # FGE - 20190621 - Agrego campos para la NC / ND
            transaccion_temp['tipo'] = str(cuenta['tipo_detalle'])
            transaccion_temp['idAgrupacionDeReferencia'] = cuenta['cod_combinacion'] 
            transaccion_temp['idAgrupacionAjustada'] = cuenta['id_agrupacion_ajustada']   
        transaccion_temp['agrupacionesDeImputacionesACatalogos'] = {'agrupacion': []}
        transaccion_lista.append(transaccion_temp)

    imputacion_temp = None

    for agrupacion in transaccion_temp['agrupacionesDeImputacionesACatalogos']['agrupacion']:
        for imputacion in agrupacion['imputacionesAConceptosPresupuestarios']['imputacion']:
            if imputacion['codigo'] == cuenta['cod_imputacion']:
                imputacion_temp = imputacion

    if imputacion_temp != None:
        contabilizacion_temp = {}
        contabilizacion_temp['cuentaDebe'] = cuenta['cuenta_debe']
        contabilizacion_temp['cuentaHaber'] = cuenta['cuenta_haber']
        if json_devengo['tipo_dte'] in [0, 61]:
            imputacion_temp['monto'] -=cuenta['debe']
            contabilizacion_temp['montoDebe'] = abs(cuenta['debe']) * -1
            contabilizacion_temp['montoHaber'] = abs(cuenta['debe']) * -1
        else:
            imputacion_temp['monto'] +=cuenta['debe']
            contabilizacion_temp['montoDebe'] = cuenta['debe']
            contabilizacion_temp['montoHaber'] = cuenta['debe']
        imputacion_temp['contabilizaciones']['contabilizacion'].append(contabilizacion_temp)
    else:
        if json_devengo['tipo_dte'] == 61 or (json_devengo['tipo_dte'] == 0 and json_devengo['ref_tipo_dte'] in [33, 34, 56]):
            transaccion_temp['agrupacionesDeImputacionesACatalogos']['agrupacion'].append(
                {'imputacionesAConceptosPresupuestarios': {
                    'imputacion': [{
                        'codigo': cuenta['cod_imputacion'],
                        'monto': abs(cuenta['debe']) * -1,
                        'contabilizaciones': {
                            'contabilizacion': [{'cuentaDebe': cuenta['cuenta_debe'], 'cuentaHaber': cuenta['cuenta_haber'], 'montoDebe': abs(cuenta['debe']) * -1, 'montoHaber': abs(cuenta['debe']) * -1}]}}]}})
        else:
            transaccion_temp['agrupacionesDeImputacionesACatalogos']['agrupacion'].append(
                {'imputacionesAConceptosPresupuestarios': {
                    'imputacion': [{
                        'codigo': cuenta['cod_imputacion'],
                        'monto': abs(cuenta['debe']),
                        'contabilizaciones': {
                            'contabilizacion': [{'cuentaDebe': cuenta['cuenta_debe'], 'cuentaHaber': cuenta['cuenta_haber'], 'montoDebe': abs(cuenta['debe']), 'montoHaber': abs(cuenta['debe'])}]}}]}})

    if cuenta['tipo_detalle'] == 1:
        catalogo_found = 'false'
        for agrupacion in transaccion_temp['agrupacionesDeImputacionesACatalogos']['agrupacion']:
            if 'imputacionesACatalogosDeReagrupacion' in agrupacion:
                for imputacion in agrupacion['imputacionesACatalogosDeReagrupacion']['imputacion']:
                    if imputacion['catalogo'] == cuenta['catalogo_tipo'] and imputacion['elemento'] == cuenta['cod_catalogo']:
                        catalogo_found = 'true'
                if catalogo_found == 'false':
                    transaccion_temp['agrupacionesDeImputacionesACatalogos']['agrupacion'][0]['imputacionesACatalogosDeReagrupacion']['imputacion'].append({"catalogo": cuenta['catalogo_tipo'], "elemento": cuenta['cod_catalogo']})
            else:
                transaccion_temp['agrupacionesDeImputacionesACatalogos']['agrupacion'][0]['imputacionesACatalogosDeReagrupacion']={'imputacion': [{"catalogo": cuenta['catalogo_tipo'], "elemento": cuenta['cod_catalogo']}]}

        catalogo_found = 'false'
        for catalogo_item in catalogos_imputados['imputacion']:
            if catalogo_item['catalogo'] == cuenta['catalogo_tipo'] and catalogo_item['elemento'] == cuenta['cod_catalogo']:
                catalogo_found = 'true'
        if catalogo_found == 'false' and cuenta['catalogo_tipo'] != '' and cuenta['catalogo_tipo'] != '':
            catalogos_imputados['imputacion'].append({"catalogo": cuenta['catalogo_tipo'], "elemento": cuenta['cod_catalogo']})

        # FGE - 20200616 - Agregamos catalogo iniciativaInversion
        if json_devengo['flag_catalogo_propio'] == 'S':
            transaccion_temp['agrupacionesDeImputacionesACatalogos']['agrupacion'][0]['imputacionesACatalogosDeReagrupacion']['imputacion'].append({"catalogo": 'iniciativaInversion', "elemento": json_devengo['catalogo_propio_inversion']})
            catalogos_imputados['imputacion'].append({"catalogo": 'iniciativaInversion', "elemento": json_devengo['catalogo_propio_inversion']})

# FGE - 20201113 - Sacamos el tipo y folio de las NC y ND y reversas
if json_devengo['tipo_dte'] in [0, 56, 61]:
    for trx_temp in transaccion_lista:
        try:
            trx_temp.pop('folio', None)
            trx_temp.pop('tipo', None)
        except:
            continue

transacciones_previas['transaccion'] = transaccion_lista
####################################################################


principal_devengo['transaccionesPrevias'] = transacciones_previas
principal.append(principal_devengo)
principales['principal'] = principal
documento_devengo['principales'] = principales

transacciones_previas['transaccion'] = transaccion_lista

impuesto = []
impuestos = {}

# FGE - 20200622 - Calculo del monto total otros impuestos. Version 2
monto_otros_impuestos = 0
if json_dte['impuestos'] != None:
    for otro_impuesto in json_dte['impuestos']:
        monto_otros_impuestos += int(otro_impuesto['MONTO'])
impuesto_devengo = {}
impuesto_devengo['codigo'] = '1'
impuesto_devengo['monto'] = str(json_dte['monto_neto']+json_dte['monto_excento'])
impuesto_devengo['montoImpuesto'] = str(int(json_dte['monto_iva']) + monto_otros_impuestos)
# FGE - 20200630 - Revision por reversa y NC/ND
if str(json_devengo['tipo_dte']) == '61':
    # # FGE - 20200707 - Ajuste para el json de nc
    # del impuesto_devengo['codigo']
    # del impuesto_devengo['monto']
    # FGE - 20201104 - Se declaran impuestos, pero vacios
    impuesto_devengo['codigo'] = ''
    impuesto_devengo['monto'] = ''
    impuesto_devengo['montoImpuesto'] = int(impuesto_devengo['montoImpuesto']) * -1
if str(json_devengo['tipo_dte']) == '0':
    del impuesto_devengo['monto']
    impuesto_devengo['codigo'] = ''
    if str(json_dte['tipo_dte']) != '61':
        #impuesto_devengo['monto'] = int(impuesto_devengo['monto']) * -1
        impuesto_devengo['montoImpuesto'] = int(impuesto_devengo['montoImpuesto']) * -1

if str(json_dte['tipo_dte']) in ['33', '61']:
    impuesto.append(impuesto_devengo)
    impuestos['impuesto'] = impuesto
else:
    impuestos['impuesto'] = []

if json_devengo['contabilizar_iva'] == 'Y' or json_devengo['contabiliza_referencia'] == 'Y':
    impuestos['contabilizaImpuestos'] = 'true'
else:
    impuestos['contabilizaImpuestos'] = 'false'

impuestos['impuesto'] = impuesto
documento_devengo['impuestos'] = impuestos

#imputacionesACatalogosContables = {}
#imputacion = []
#imputacion_devengo = {}
#imputacion.append(imputacion_devengo)
#documento_devengo['imputacionesACatalogosContables'] = imputacionesACatalogosContables
documento.append(documento_devengo)
documentos['documento'] = documento
devengo['documentos'] = documentos

#if len(catalogos_imputados['imputacion']) > 0:
#    devengo['imputacionesACatalogosContables'] = catalogos_imputados

if json_devengo['flag_catalogo_propio'] == 'S':
        devengo['imputacionesACatalogosContables'] = {"imputacion": [{"catalogo": "propioContabilidad", "elemento": "%s" % json_devengo['catalogo_propio']}]}

## FGE - 20190624 - Agrego referencia en caso de NC y ND
if json_devengo['tipo_dte'] in [0, 56, 61]:
    informacion_de_ajuste = {}
    informacion_de_ajuste['folioTransaccionAjustada'] = str(json_devengo['ref_folio'])
    if json_devengo['tipo_dte'] == 56:
        informacion_de_ajuste['tipo'] = '8'
    elif json_devengo['tipo_dte'] == 61:
        informacion_de_ajuste['tipo'] = '7'
    elif json_devengo['tipo_dte'] == 0:
        informacion_de_ajuste['tipo'] = '2'
    devengo['informacionDeAjuste'] = informacion_de_ajuste

devengos.append(devengo)
detalles['devengo'] = devengos

json_out['cabecera'] = cabecera
json_out['detalles'] = detalles

return json.dumps(json_out)
$function$ language plpythonu;


create or replace function procesa_resp_ms_12814(json)
    returns json
as $function$
declare
    json1              alias for $1;
    json2              json;
    json3              json;
    v_respuesta        varchar;
    v_descripcion      varchar;
    v_codigo_dv        varchar;
    v_track_id         varchar;
    v_dte_codigo_txel  varchar;
    xml3               varchar;

    v_reg_devengo  record;
    v_reg_dte      record;
begin
    json2:=json1;
    v_codigo_dv:=get_json('codigo_dv', json2);

    json2:=logjson(json2, 'RESPUEST JSON 12814: ' || json2::varchar);

    v_respuesta:=get_json('RESPUESTA',json2);
    if(strpos(v_respuesta,'HTTP/1.1 200')=0) then
        json3:=put_json(json3,'MENSAJE_VACIO','Error conexion servicio');
        json2:=response_requests_6000('1', 'OK', json3::varchar, json2);
        return json2;
    else
        v_track_id:='';
        v_descripcion:='';
    end if;

    BEGIN
        v_respuesta:=split_part(v_respuesta,chr(10)||chr(10),2);
    EXCEPTION WHEN OTHERS THEN
        json3:=put_json(json3,'MENSAJE_VACIO','Error al leer respuesta servicio');
        json2:=response_requests_6000('1', 'OK', json3::varchar, json2);
        perform dp_act_estado_devengo(v_codigo_dv::bigint, 'BORRADOR', '');
        return json2;
    END;

    v_track_id:=get_json('codigo', v_respuesta::json);
    v_descripcion:=get_json('descripcion', v_respuesta::json);

    select dte_codigo_txel into v_dte_codigo_txel from dp_devengo where codigo_dv = v_codigo_dv::bigint;

    if v_track_id <> '' then
        perform dp_act_estado_devengo(v_codigo_dv::bigint, 'EN_PROCESO', '');
        update dp_devengo set ticket_id = v_track_id, fecha_creacion = now(), fecha_emision = now() where codigo_dv = v_codigo_dv::bigint;
        update dte_recibidos set data_dte = put_data_dte(data_dte, 'TICKET_DEVENGO', v_track_id) where codigo_txel = v_dte_codigo_txel::bigint;
        select codigo_rc, codigo_oc, rut_emisor, rut_receptor, dte_codigo_txel, tipo_devengo from dp_devengo where codigo_dv = v_codigo_dv::bigint into v_reg_devengo;

        select folio, tipo_dte, uri from dte_recibidos where codigo_txel = v_reg_devengo.dte_codigo_txel::bigint into v_reg_dte;

        update token_de_emitidos set estado = 'PAGADO' where token = v_reg_devengo.codigo_rc and folio = v_reg_devengo.codigo_oc and rut_emisor = v_reg_devengo.rut_emisor and rut_receptor = v_reg_devengo.rut_receptor;
        json2:=response_requests_6000('1', 'Enviado: TrackID - ' || v_track_id || ', Descripción: ' || v_descripcion, '', json2);
        json2:=put_json(json2,'RESPUESTA_COLA','OK');

        xml3:='';
        xml3:=put_campo(xml3,'RUT_EMISOR', v_reg_devengo.rut_receptor::varchar); -- '96919050');
        xml3:=put_campo(xml3,'RUT_OWNER',' ' || v_reg_devengo.rut_emisor::varchar || ' ');
        xml3:=put_campo(xml3,'FECHA_EVENTO',now()::varchar);
        xml3:=put_campo(xml3,'RUT_RECEPTOR', v_reg_devengo.rut_emisor::varchar); --'7621836');
        xml3:=put_campo(xml3,'FOLIO', v_reg_dte.folio::varchar); --'113406');
        xml3:=put_campo(xml3,'TIPO_DTE', v_reg_dte.tipo_dte::varchar); --'52');
        xml3:=put_campo(xml3,'CANAL','RECIBIDOS');
        xml3:=put_campo(xml3,'URI_IN', v_reg_dte.uri); -- 'http://pruebasacepta1705.acepta.com/v01/00000000000000_1721614117_1812593659_72_?k=71954f57f2bdbaf140312c054a6513a1');
        -- FGE - 20201102 - Adicion del rut que envia el devengo
        if v_reg_devengo.tipo_devengo = 'MAN' then
            xml3:=put_campo(xml3,'COMENTARIO_TRAZA',' Exitoso TicketID: ' || v_track_id || ', Enviado por: ' || get_json('rutUsuario', json2) || '-' || modulo11(get_json('rutUsuario', json2)));
        else
            xml3:=put_campo(xml3,'COMENTARIO_TRAZA',' Exitoso TicketID: ' || v_track_id || ', Devengo Automatico');
        end if;
        xml3:=put_campo(xml3,'EVENTO','ENVIO_DEVENGO');
        xml3:=graba_bitacora(xml3,'ENVIO_DEVENGO');
    elsif v_descripcion = 'Periodo No Abierto' then
        perform dp_act_estado_devengo(v_codigo_dv::bigint, 'NO_ENVIADO', '');
        json2:=put_json(json2,'RESPUESTA_COLA','OK1');
        json2:=response_requests_6000('1', 'Período no abierto.' || chr(10) || 'El devengo generado será reenviado periodicamente hasta la apertuda del período respectivo.', '', json2);
    else
        perform dp_act_estado_devengo(v_codigo_dv::bigint, 'BORRADOR', '');
        json2:=put_json(json2,'RESPUESTA_COLA','EJERCICIO_CERRADO');
        json2:=response_requests_6000('2', 'Error: ' || v_descripcion, '', json2);
    end if;
/*
    elsif v_descripcion = 'Periodo No Abierto' then
        perform dp_act_estado_devengo(v_codigo_dv::bigint, 'NO_ENVIADO', '');
        json2:=response_requests_6000('1', 'En espera de apertura del periodo', '', json2);
        perform dp_act_estado_devengo(v_codigo_dv::bigint, 'NO_ENVIADO', '');
        json2:=response_requests_6000('1', 'En espera de apertura del periodo', '', json2);
        json2:=put_json(json2,'RESPUESTA_COLA','OK');
    else
        perform dp_act_estado_devengo(v_codigo_dv::bigint, 'BORRADOR', '');
        json2:=response_requests_6000('2', 'Error: ' || v_descripcion, '', json2);
        json2:=put_json(json2,'RESPUESTA_COLA','EJERCICIO_CERRADO');
    end if;
*/
    return json2;
end;
$function$ language plpgsql;





