
delete from isys_querys_tx where llave='13120';

insert into isys_querys_tx values ('13120',10,1,1,'select agregar_exportar_modulo_reporte_13120(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,0,0);
--insert into isys_querys_tx values ('13120',20,13,1,'select agregar_archivo_libros_mensuales(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,0,0);

CREATE OR REPLACE FUNCTION agregar_exportar_modulo_reporte_13120(i_parametros json)
    RETURNS json AS
$BODY$
    DECLARE
       v_id_reporte   reportes_10k.id_reporte%type;
       v_rut_usuario  reportes_10k.rut_usuario%type := get_json('RUT_USUARIO', i_parametros);
       v_empresa      reportes_10k.empresa%type := get_json('RUT_EMPRESA', i_parametros);
       v_canal        reportes_10k.canal%type := get_json('CANAL', i_parametros); --'WEBIECV';
       v_filtros      reportes_10k.filtros%type := get_json('FILTROS_REPORTE', i_parametros);
       v_codigo_libro numeric := get_json('CODIGO_LIBRO', i_parametros)::numeric;
       o_json         json := i_parametros;
       v_next_query   varchar := get_json('NEXT_QUERY', i_parametros);
       v_base_datos   varchar := get_json('BASE_DATOS', i_parametros);
       v_tipo_reporte varchar := get_json('TIPO_REPORTE', i_parametros);
       host1           varchar;
    BEGIN
        IF (v_canal = '') THEN
            v_canal := 'WEBIECV';
            v_next_query := 'select iecv.exportar_csv(' || v_codigo_libro || ')';
            v_base_datos := 'BASE_WEBIECV';
            v_tipo_reporte := 3; -- FIJO WEBIECV
        END IF;

	host1:=get_json('HOST',get_json('__ARGV__',i_parametros)::json);

        INSERT INTO reportes_10k (id_reporte, rut_usuario, empresa, filtros, estado, canal, uri, fecha_ingreso,host)
        VALUES (nextval('correlativo_reportes_10k'), v_rut_usuario, v_empresa, v_filtros, 'En Proceso', v_canal, null, current_timestamp,host1)
        RETURNING id_reporte INTO v_id_reporte;

        INSERT INTO procesa_reportes_10k (id_reporte,next_query,rut_usuario,fecha_ingreso,estado,veces,/*base_datos,*/ tipo_reporte,host)
        --VALUES (v_id_reporte, 'select iecv.exportar_csv(' || v_codigo_libro || ')', v_rut_usuario, current_timestamp, 'En Proceso', 0, 'BASE_WEBIECV');
        VALUES (v_id_reporte, v_next_query, v_rut_usuario, current_timestamp, 'En Proceso', 0, /*v_base_datos,*/ v_tipo_reporte::integer,host1);

        o_json := put_json(o_json, 'ID_REPORTE_MOTOR', v_id_reporte::text);

        RETURN o_json;
    END;
$BODY$
LANGUAGE plpgsql;

