delete from isys_querys_tx where llave='14001';

--Llamamos a Escribir Directo
insert into isys_querys_tx values ('14001',10,1,1,'select verifica_modulo_importacion(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

/**BEGIN PROCESA LOS DOCUMENTOS EMITIDOS*/
insert into isys_querys_tx values ('14001',30,1,1,'select procesa_emitidos(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

/**BEGIN PROCESA LOS DOCUMENTOS RECIBIDOS*/
insert into isys_querys_tx values ('14001',40,1,1,'select procesa_recibidos(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

/**BEGIN PROCESA LOS LIBROS*/
insert into isys_querys_tx values ('14001',50,13,1,'select iecv.libros_importados(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

/**BEGIN INFORMAMOS AL MODULO DE IMPORTACION MASIVA*/
--insert into isys_querys_tx values ('14001',70,27,1,'select importer.finaliza_carga(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('14001',70,27,1,'select importer.finaliza_carga(''$$__JSON3__$$''::json) as __json__',0,0,0,1,1,-1,0);

/**BEGIN VERIFICAMOS LA COLA DE PROCESO*/
insert into isys_querys_tx values ('14001',90,1,1,'select finaliza_carga(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

/**BEGIN PROCESA LA BASURA QUE LLEGASE AL MODULO*/
insert into isys_querys_tx values ('14001',100,1,1,'select basura_importacion(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

--Borra mensaje
insert into isys_querys_tx values ('14001',1000,19,1,'select sp_procesa_respuesta_cola_motor88_json(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,0,0);

create or replace FUNCTION json_importer_14001(i_parametros json)
    RETURNS json AS
$BODY$
DECLARE
    json3	json;
BEGIN
	json3:=put_json('{}','LIBRO_REPETIDO',get_json('LIBRO_REPETIDO',i_parametros));
	json3:=put_json(json3,'DTE_REPETIDO',get_json('DTE_REPETIDO',i_parametros));
	json3:=put_json(json3,'Uri',get_json('Uri',i_parametros));
	json3:=put_json(json3,'INSERT_EXITOSO',get_json('INSERT_EXITOSO',i_parametros));
	json3:=put_json(json3,'REPETIDO_MENSAJE',get_json('REPETIDO_MENSAJE',i_parametros));
	return json3;
END
$BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION verifica_modulo_importacion(i_parametros json)
    RETURNS json AS
$BODY$
DECLARE
    o_json json := i_parametros;
BEGIN
    o_json := put_json(o_json,'__SECUENCIAOK__','0');
    /**Validamos a que modulo corresponde --> Documentos(Emitidos o Recibidos) o Libros*/
    IF (get_json('MODULO', o_json) = 'DOCUMENTO') THEN
	o_json := finaliza_carga(o_json);
        --o_json := put_json(o_json,'__SECUENCIAOK__','90');
        /*o_json := put_json(o_json,'TIPO_DTE', get_json('TipoDTE', o_json));
        IF (lower(get_json('Emitido', o_json)) = 'true') THEN 
            o_json := put_json(o_json,'__SECUENCIAOK__','30');
            o_json := put_json(o_json,'CANAL','EMITIDOS');
        ELSIF (lower(get_json('Recibido', o_json)) = 'true') THEN 
            o_json := put_json(o_json,'__SECUENCIAOK__','40');
            o_json := put_json(o_json,'CANAL','RECIBIDOS');
        ELSE
            o_json := put_json(o_json,'__SECUENCIAOK__','100');
        END IF;*/
    ELSIF (get_json('MODULO', o_json) = 'LIBRO') THEN
        o_json := put_json(o_json,'__SECUENCIAOK__','50');
        IF (get_json('TipoOperacion', o_json) = '1') THEN
            o_json := put_json(o_json,'TIPO_OPERACION','COMPRA');
        ELSIF (get_json('TipoOperacion', o_json) = '2') THEN
            o_json := put_json(o_json,'TIPO_OPERACION','VENTA');
        ELSE
            o_json := put_json(o_json,'TIPO_OPERACION','NO_APLICA');
            o_json := put_json(o_json,'__SECUENCIAOK__','100');
        END IF;
    ELSE
        o_json := put_json(o_json,'__SECUENCIAOK__','100');
    END IF;

    o_json := logjson(o_json, ' Importando URI: [ ' || get_json('Uri', o_json) || ' ] ');
    RETURN o_json;
END
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION procesa_emitidos(i_parametros json)
    RETURNS json AS
$BODY$
DECLARE
    o_json json := i_parametros;
    rw_dte_emitidos dte_emitidos_importados_generica%rowtype;
    rw_dte_boletas dte_boletas_importadas_generica%rowtype;
    rw_boletas dte_boletas_generica%rowtype;
    v_tabla_insert varchar;
BEGIN
    o_json := put_json(o_json, '__SECUENCIAOK__', '0');
    o_json := put_json(o_json, 'DTE_REPETIDO', 'NO');
    o_json := logjson(o_json, ' Importando URI: [ ' || get_json('Uri', o_json) || ' ] en Emitidos ');
    /**Validamos datos obligatorios de documentos Emitidos*/
 
    -- Verificamos existencia del Documento KEY (rut_emisor, tipo_dte, folio)
    rw_dte_emitidos.rut_emisor := get_json('RutEmisor', o_json);
    rw_dte_emitidos.tipo_dte := get_json('TipoDTE', o_json);
    rw_dte_emitidos.folio := get_json('Folio', o_json);
    -- dte_boletas_importadas_generica
    IF (rw_dte_emitidos.tipo_dte in (39, 41)) THEN
        v_tabla_insert := get_tabla_boleta_emision(replace(get_json('FechaEmision', o_json), '-', ''), get_json('RutEmisor', o_json), 'SI');

        IF NOT (crea_tabla_boletas_importer(v_tabla_insert)) then
             raise notice 'FALLA CREACION DE TABLAS DE BOLETAS IMPORTER';
        END IF;

        EXECUTE format('SELECT codigo_txel, uri FROM %I WHERE rut_emisor = $1 AND tipo_dte = $2 AND folio = $3', v_tabla_insert) 
          USING rw_dte_emitidos.rut_emisor, rw_dte_emitidos.tipo_dte, rw_dte_emitidos.folio INTO rw_boletas.codigo_txel, rw_boletas.uri;

        IF rw_boletas.codigo_txel IS NOT NULL THEN
            -- DTE ya se encuentra en la Base de Datos
            o_json := logjson(o_json, ' Importando URI: [ ' || get_json('Uri', o_json) || ' ] en Emitidos - Rut Emisor [ ' || rw_dte_emitidos.rut_emisor ||
                             ' ] Tipo DTE [ ' || rw_dte_emitidos.tipo_dte ||
                             ' ] Folio [ ' || rw_dte_emitidos.folio || ' ] Ya existe en la Base de Datos - Tabla ' || v_tabla_insert);
            o_json := put_json(o_json, 'DTE_REPETIDO', 'SI');
            o_json := put_json(o_json, '__SECUENCIAOK__','70');
            o_json := put_json(o_json, 'INSERT_EXITOSO', 'SI');
            o_json := put_json(o_json, 'REPETIDO_MENSAJE', 'Documento ya se encuentra registrado');
	    o_json := put_json(o_json, '__JSON3__',json_importer_14001(o_json)::varchar);
            RETURN o_json;
        END IF;

        rw_dte_boletas.fecha_ingreso     := current_timestamp;
        rw_dte_boletas.mes               := to_char(current_timestamp, 'YYYYMM');
        rw_dte_boletas.dia               := to_char(current_timestamp, 'YYYYMMDD');
        rw_dte_boletas.tipo_dte          := rw_dte_emitidos.tipo_dte;
        rw_dte_boletas.folio             := rw_dte_emitidos.folio;
        rw_dte_boletas.fecha_emision     := get_json('FechaEmision', o_json); -- YYYY-MM-DD
        rw_dte_boletas.mes_emision       := left(replace(rw_dte_boletas.fecha_emision, '-', ''), 6); -- YYYYMM
        rw_dte_boletas.dia_emision       := replace(rw_dte_boletas.fecha_emision, '-', ''); -- YYYYMMDD
        rw_dte_boletas.fecha_vencimiento := rw_dte_boletas.fecha_emision;
        rw_dte_boletas.rut_emisor        := rw_dte_emitidos.rut_emisor;
        rw_dte_boletas.rut_receptor      := get_json('RUTRecep', o_json);
        rw_dte_boletas.monto_neto        := CASE WHEN get_json('MontoNeto', o_json) = '' THEN '0' ELSE get_json('MontoNeto', o_json) END;
        rw_dte_boletas.monto_total       := get_json('MontoTotal', o_json);
        rw_dte_boletas.estado            := 'IMPORTADO';
        rw_dte_boletas.uri               := get_json('Uri', o_json);
        rw_dte_boletas.estado_sii        := 'IMPORTADO';
        rw_dte_boletas.digest            := get_json('Digest', o_json);
        rw_dte_boletas.monto_excento     := CASE WHEN get_json('MontoExento', o_json) = '' THEN '0' ELSE get_json('MontoExento', o_json) END;
        rw_dte_boletas.monto_iva         := CASE WHEN get_json('MontoIVA', o_json) = '' THEN '0' ELSE get_json('MontoIVA', o_json) END;
 
        -- rw_dte_boletas.codigo_txel := nextval('sec_codigo_txel');
        EXECUTE format('INSERT INTO %I (codigo_txel, fecha_ingreso, mes, dia, tipo_dte, folio, fecha_emision, mes_emision, dia_emision, fecha_vencimiento, rut_emisor, rut_receptor, monto_neto, monto_total, estado, uri, estado_sii, digest, monto_excento, monto_iva) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19) RETURNING codigo_txel', v_tabla_insert)
           USING nextval('sec_codigo_txel'), rw_dte_boletas.fecha_ingreso, rw_dte_boletas.mes,rw_dte_boletas.dia,rw_dte_boletas.tipo_dte,rw_dte_boletas.folio,rw_dte_boletas.fecha_emision,rw_dte_boletas.mes_emision,rw_dte_boletas.dia_emision,rw_dte_boletas.fecha_vencimiento,rw_dte_boletas.rut_emisor,rw_dte_boletas.rut_receptor,rw_dte_boletas.monto_neto,rw_dte_boletas.monto_total,rw_dte_boletas.estado,rw_dte_boletas.uri,rw_dte_boletas.estado_sii,rw_dte_boletas.digest,rw_dte_boletas.monto_excento,rw_dte_boletas.monto_iva INTO rw_dte_boletas.codigo_txel;
        --IF found THEN
        o_json := logjson(o_json, ' Importando URI: [ ' || get_json('Uri', o_json) || ' ] en ' || v_tabla_insert || ' Codigo TXEL [ ' || rw_dte_boletas.codigo_txel || ' ]'); 
        o_json := put_json(o_json, 'INSERT_EXITOSO', 'SI');
        o_json := put_json(o_json, 'INSERT_EXITOSO_KEY', 'SI');
        --END IF;
    ELSE
        IF rw_dte_emitidos.tipo_dte = 801 THEN
           v_tabla_insert := 'de_emitidos';
        ELSE
           v_tabla_insert := get_tabla_dte_emitidos(get_json('RutEmisor', o_json), 'SI');
        END IF;

        EXECUTE format('SELECT codigo_txel, uri FROM %I WHERE rut_emisor = $1 AND tipo_dte = $2 AND folio = $3', v_tabla_insert)
          USING rw_dte_emitidos.rut_emisor, rw_dte_emitidos.tipo_dte, rw_dte_emitidos.folio INTO rw_dte_emitidos.codigo_txel, rw_dte_emitidos.uri;

        IF rw_dte_emitidos.codigo_txel IS NOT NULL THEN
        /*IF EXISTS (SELECT 1
                   FROM dte_emitidos_importados_generica
                   WHERE rut_emisor = rw_dte_emitidos.rut_emisor
                   AND tipo_dte = rw_dte_emitidos.tipo_dte
                   AND folio = rw_dte_emitidos.folio) THEN*/
            -- DTE ya se encuentra en la Base de Datos
            o_json := logjson(o_json, ' Importando URI: [ ' || get_json('Uri', o_json) || ' ] en Emitidos - Rut Emisor [ ' || rw_dte_emitidos.rut_emisor || 
                             ' ] Tipo DTE [ ' || rw_dte_emitidos.tipo_dte || 
                             ' ] Folio [ ' || rw_dte_emitidos.folio || ' ] Ya existe en la Base de Datos Tabla ' || v_tabla_insert);
            o_json := put_json(o_json, 'DTE_REPETIDO', 'SI');
            o_json := put_json(o_json,'__SECUENCIAOK__','70');
            o_json := put_json(o_json, 'REPETIDO_MENSAJE', 'Documento ya se encuentra registrado');
	    o_json := put_json(o_json, '__JSON3__',json_importer_14001(o_json)::varchar);
            RETURN o_json;
        END IF;

        rw_dte_emitidos.fecha_ingreso     := current_timestamp;
        rw_dte_emitidos.mes               := to_char(current_timestamp, 'YYYYMM');
        rw_dte_emitidos.dia               := to_char(current_timestamp, 'YYYYMMDD');
        rw_dte_emitidos.tipo_dte          := rw_dte_emitidos.tipo_dte;
        rw_dte_emitidos.folio             := rw_dte_emitidos.folio;
        rw_dte_emitidos.fecha_emision     := get_json('FechaEmision', o_json); -- YYYY-MM-DD
        rw_dte_emitidos.mes_emision       := left(replace(rw_dte_emitidos.fecha_emision, '-', ''), 6); -- YYYYMM
        rw_dte_emitidos.dia_emision       := replace(rw_dte_emitidos.fecha_emision, '-', ''); -- YYYYMMDD
        rw_dte_emitidos.fecha_vencimiento := rw_dte_emitidos.fecha_emision;
        rw_dte_emitidos.rut_emisor        := rw_dte_emitidos.rut_emisor;
        rw_dte_emitidos.rut_receptor      := get_json('RUTRecep', o_json);
        rw_dte_emitidos.monto_neto        := CASE WHEN get_json('MontoNeto', o_json) = '' THEN '0' ELSE get_json('MontoNeto', o_json) END;
        rw_dte_emitidos.monto_total       := get_json('MontoTotal', o_json);
        rw_dte_emitidos.estado            := 'IMPORTADO';
        rw_dte_emitidos.uri               := get_json('Uri', o_json);
        rw_dte_emitidos.estado_sii        := 'IMPORTADO';
        rw_dte_emitidos.digest            := get_json('Digest', o_json);
        rw_dte_emitidos.monto_excento     := CASE WHEN get_json('MontoExento', o_json) = '' THEN '0' ELSE get_json('MontoExento', o_json) END;
        rw_dte_emitidos.monto_iva         := CASE WHEN get_json('MontoIVA', o_json) = '' THEN '0' ELSE get_json('MontoIVA', o_json) END;

        IF (get_json('INSERT_EXITOSO', o_json) != 'SI') THEN
            -- rw_dte_emitidos.codigo_txel := nextval('sec_codigo_txel');
            EXECUTE format('INSERT INTO %I (codigo_txel, fecha_ingreso, mes, dia, tipo_dte, folio, fecha_emision, mes_emision, dia_emision, fecha_vencimiento, rut_emisor, rut_receptor, monto_neto, monto_total, estado, uri, estado_sii, digest, monto_excento, monto_iva) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20) RETURNING codigo_txel', v_tabla_insert)
            USING nextval('sec_codigo_txel'), rw_dte_emitidos.fecha_ingreso,rw_dte_emitidos.mes,rw_dte_emitidos.dia,rw_dte_emitidos.tipo_dte,rw_dte_emitidos.folio,rw_dte_emitidos.fecha_emision,rw_dte_emitidos.mes_emision,rw_dte_emitidos.dia_emision,rw_dte_emitidos.fecha_vencimiento,rw_dte_emitidos.rut_emisor,rw_dte_emitidos.rut_receptor,rw_dte_emitidos.monto_neto,rw_dte_emitidos.monto_total,rw_dte_emitidos.estado,rw_dte_emitidos.uri,rw_dte_emitidos.estado_sii,rw_dte_emitidos.digest,rw_dte_emitidos.monto_excento,rw_dte_emitidos.monto_iva INTO rw_dte_emitidos.codigo_txel;

            --INSERT INTO dte_emitidos_importados_generica VALUES (rw_dte_emitidos.*) RETURNING codigo_txel INTO rw_dte_boletas.codigo_txel;

            --IF found THEN
            IF rw_dte_emitidos.codigo_txel IS NOT NULL THEN
                o_json := logjson(o_json, ' Importando URI: [ ' || get_json('Uri', o_json) || ' ] en ' || v_tabla_insert || ' Codigo TXEL [ ' || rw_dte_emitidos.codigo_txel || ' ]');
                o_json := put_json(o_json, 'INSERT_EXITOSO', 'SI');
            END IF;
        END IF;

        IF (get_json('INSERT_EXITOSO_KEY', o_json) != 'SI') THEN
            INSERT INTO uri_key2 (fecha, uri, key, fecha_emision, monto_total, estado, rut_emisor, canal)
            VALUES (current_timestamp, rw_dte_emitidos.uri, substring(rw_dte_emitidos.digest,1,32), rw_dte_emitidos.fecha_emision, rw_dte_emitidos.monto_total::bigint, 'EMI', rw_dte_emitidos.rut_emisor, 'E');

            IF found THEN
                o_json := logjson(o_json, ' Importando URI: [ ' || get_json('Uri', o_json) || ' ] en dte_boletas_importadas_generica Codigo TXEL [ ' || rw_dte_boletas.codigo_txel || ' ]');
                o_json := put_json(o_json, 'INSERT_EXITOSO_KEY', 'SI');
            END IF;
        END IF;
    END IF;
    o_json := logjson(o_json, ' Importando URI: [ ' || get_json('Uri', o_json) || ' ] en Emitidos - Rut Emisor [ ' || rw_dte_emitidos.rut_emisor || 
                         ' ] Tipo DTE [ ' || rw_dte_emitidos.tipo_dte ||
                         ' ] Folio [ ' || rw_dte_emitidos.folio || ' ] Se debe agregar ');

    /**Informamos Termino de Procesamiento*/
    o_json := put_json(o_json,'__SECUENCIAOK__','70');
    o_json := put_json(o_json, '__JSON3__',json_importer_14001(o_json)::varchar);
    RETURN o_json;
END
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION procesa_recibidos(i_parametros json)
    RETURNS json AS
$BODY$
DECLARE
    o_json json := i_parametros;
    rw_dte_recibidos dte_recibidos_importados_generica%rowtype;
    v_tabla_insert varchar;
BEGIN
    o_json := put_json(o_json,'__SECUENCIAOK__','0');
    o_json := logjson(o_json, ' Importando URI: [ ' || get_json('Uri', o_json) || ' ] en Recibidos ');
    
    if(get_json('TipoDTE', o_json)='801') then
        v_tabla_insert := 'de_recibidos';
    else
        v_tabla_insert := get_tabla_dte_recibidos(get_json('RutEmisor', o_json), 'SI');
        --v_tabla_insert := get_tabla_dte_recibidos(get_json('RUTRecep', o_json), 'SI');
    end if;

    rw_dte_recibidos.rut_emisor := get_json('RutEmisor', o_json);
    rw_dte_recibidos.tipo_dte := get_json('TipoDTE', o_json);
    rw_dte_recibidos.folio := get_json('Folio', o_json);

    /**Validamos datos obligatorios de documentos Recibidos*/
    EXECUTE format('SELECT codigo_txel, uri FROM %I WHERE rut_emisor = $1 AND tipo_dte = $2 AND folio = $3', v_tabla_insert)
          USING rw_dte_recibidos.rut_emisor, rw_dte_recibidos.tipo_dte, rw_dte_recibidos.folio INTO rw_dte_recibidos.codigo_txel, rw_dte_recibidos.uri;

    IF rw_dte_recibidos.uri IS NOT NULL THEN

    /*IF EXISTS (SELECT 1
               FROM dte_recibidos_importados_generica
               WHERE rut_emisor = rw_dte_recibidos.rut_emisor
               AND tipo_dte = rw_dte_recibidos.tipo_dte
               AND folio = rw_dte_recibidos.folio) THEN
    */
        -- DTE ya se encuentra en la Base de Datos
        o_json := logjson(o_json, ' Importando URI: [ ' || get_json('Uri', o_json) || ' ] en Emitidos - Rut Emisor [ ' || rw_dte_recibidos.rut_emisor ||
                             ' ] Tipo DTE [ ' || rw_dte_recibidos.tipo_dte ||
                             ' ] Folio [ ' || rw_dte_recibidos.folio || ' ] Ya existe en la Base de Datos Tabla dte_recibidos_importados_generica');
        o_json := put_json(o_json, 'DTE_REPETIDO', 'SI');
        o_json := put_json(o_json, 'REPETIDO_MENSAJE', 'Documento ya se encuentra registrado');
        o_json := put_json(o_json,'__SECUENCIAOK__','70');
	o_json := put_json(o_json, '__JSON3__',json_importer_14001(o_json)::varchar);
        RETURN o_json;
    END IF;

    rw_dte_recibidos.mes               := to_char(current_timestamp, 'YYYYMM');
    rw_dte_recibidos.dia               := to_char(current_timestamp, 'YYYYMMDD');
    rw_dte_recibidos.tipo_dte          := rw_dte_recibidos.tipo_dte;
    rw_dte_recibidos.folio             := rw_dte_recibidos.folio;
    rw_dte_recibidos.fecha_emision     := get_json('FechaEmision', o_json); -- YYYY-MM-DD
    rw_dte_recibidos.mes_emision       := left(replace(rw_dte_recibidos.fecha_emision, '-', ''), 6); -- YYYYMM
    rw_dte_recibidos.dia_emision       := replace(rw_dte_recibidos.fecha_emision, '-', ''); -- YYYYMMDD
    rw_dte_recibidos.fecha_vencimiento := rw_dte_recibidos.fecha_emision;
    rw_dte_recibidos.rut_emisor        := rw_dte_recibidos.rut_emisor;
    rw_dte_recibidos.rut_receptor      := get_json('RUTRecep', o_json);
    rw_dte_recibidos.monto_neto        := CASE WHEN get_json('MontoNeto', o_json) = '' THEN '0' ELSE get_json('MontoNeto', o_json) END;
    rw_dte_recibidos.monto_total       := get_json('MontoTotal', o_json);
    rw_dte_recibidos.estado            := 'IMPORTADO';
    rw_dte_recibidos.uri               := get_json('Uri', o_json);
    rw_dte_recibidos.estado_sii        := 'IMPORTADO';
    rw_dte_recibidos.digest            := get_json('Digest', o_json);
    rw_dte_recibidos.monto_excento     := CASE WHEN get_json('MontoExento', o_json) = '' THEN '0' ELSE get_json('MontoExento', o_json) END;
    rw_dte_recibidos.monto_iva         := CASE WHEN get_json('MontoIVA', o_json) = '' THEN '0' ELSE get_json('MontoIVA', o_json) END;
    rw_dte_recibidos.fecha_ingreso     := current_timestamp;

    IF (get_json('INSERT_EXITOSO', o_json) != 'SI') THEN
            --rw_dte_recibidos.codigo_txel := nextval('sec_codigo_txel');
        --INSERT INTO dte_recibidos_importados_generica VALUES (rw_dte_recibidos.*) RETURNING codigo_txel INTO rw_dte_recibidos.codigo_txel;
            EXECUTE format('INSERT INTO %I (codigo_txel, fecha_ingreso, mes, dia, tipo_dte, folio, fecha_emision, mes_emision, dia_emision, fecha_vencimiento, rut_emisor, rut_receptor, monto_neto, monto_total, estado, uri, estado_sii, digest, monto_excento, monto_iva) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20) RETURNING codigo_txel', v_tabla_insert)
            USING  nextval('sec_codigo_txel'), rw_dte_recibidos.fecha_ingreso,rw_dte_recibidos.mes,rw_dte_recibidos.dia,rw_dte_recibidos.tipo_dte,rw_dte_recibidos.folio,rw_dte_recibidos.fecha_emision,rw_dte_recibidos.mes_emision,rw_dte_recibidos.dia_emision,rw_dte_recibidos.fecha_vencimiento,rw_dte_recibidos.rut_emisor,rw_dte_recibidos.rut_receptor,rw_dte_recibidos.monto_neto,rw_dte_recibidos.monto_total,rw_dte_recibidos.estado,rw_dte_recibidos.uri,rw_dte_recibidos.estado_sii,rw_dte_recibidos.digest,rw_dte_recibidos.monto_excento,rw_dte_recibidos.monto_iva INTO rw_dte_recibidos.codigo_txel;

        --IF found THEN
        IF rw_dte_recibidos.codigo_txel IS NOT NULL THEN
            o_json := logjson(o_json, ' Importando URI: [ ' || get_json('Uri', o_json) || ' ] en ' || v_tabla_insert || ' Codigo TXEL [ ' || rw_dte_recibidos.codigo_txel || ' ]');
            o_json := put_json(o_json, 'INSERT_EXITOSO', 'SI');
        END IF;
    END IF;

    IF (get_json('INSERT_EXITOSO_KEY', o_json) != 'SI') THEN
        INSERT INTO uri_key2 (fecha,uri,key,fecha_emision,monto_total,estado,rut_emisor,canal) 
        VALUES (current_timestamp, rw_dte_recibidos.uri, substring(rw_dte_recibidos.digest,1,32), rw_dte_recibidos.fecha_emision, rw_dte_recibidos.monto_total, 'REMI', rw_dte_recibidos.rut_emisor, 'R');

        IF found THEN
            o_json := logjson(o_json, ' Importando URI: [ ' || get_json('Uri', o_json) || ' ] en uri_key2 Codigo TXEL [ ' || rw_dte_recibidos.codigo_txel || ' ]');
            o_json := put_json(o_json, 'INSERT_EXITOSO_KEY', 'SI');
        END IF;
    END IF;

    /**Informamos Termino de Procesamiento*/
    o_json := put_json(o_json,'__SECUENCIAOK__','70');
    o_json := put_json(o_json, '__JSON3__',json_importer_14001(o_json)::varchar);
    RETURN o_json;
END
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION finaliza_carga(i_parametros json)
    RETURNS json AS
$BODY$
DECLARE
    o_json json := i_parametros;
    v_id_cola varchar;
    v_xml3  varchar;
    v_json json;
    v_sql varchar;
BEGIN
    o_json := put_json(o_json,'__SECUENCIAOK__','1000');
    o_json := logjson(o_json, ' Importando URI: [ ' || get_json('Uri', o_json) || ' ] en Finalizacion Prod [' ||  get_json('INSERT_EXITOSO', o_json) || ']');
    v_id_cola := get_json('__ID_DTE__',o_json);
    --v_xml3 := get_parametros_motor('','BASE_COLAS');
    /**Validamos si fue correcto el procesamiento en el Modulo de Importacion*/
    --IF (get_json('IMPORTACION_EXITOSA', o_json) = 'SI') THEN
    IF (get_json('INSERT_EXITOSO', o_json) = 'SI' OR get_json('IMPORTACION_EXITOSA', o_json) = 'SI' or get_json('PUBLICADO', o_json) in ('OK', 'YA_EXISTE')) THEN
        /**Agregar datos a la Traza*/
        o_json := put_json(o_json, 'URI_IN', get_json('Uri', o_json));
        IF get_json('PUBLICADO', o_json) = 'YA_EXISTE' THEN
          o_json := put_json(o_json, 'COMENTARIO_TRAZA', 'Documento Importado Duplicado');
        ELSE
          o_json := put_json(o_json, 'COMENTARIO_TRAZA', 'Documento Importado');
        END IF;
        o_json := put_json(o_json, 'FECHA_EVENTO', current_timestamp::varchar);
        o_json := put_json(o_json, 'TIPO_DTE', get_json('TipoDTE', o_json));
        o_json := put_json(o_json, 'RUT_EMISOR', get_json('RutEmisor', o_json));
        o_json := put_json(o_json, 'RUT_RECEPTOR', get_json('RUTRecep', o_json));
        
        o_json := graba_bitacora(o_json,'IMP');
	o_json := put_json(o_json,'RESPUESTA','Status: 200 OK');

        /**Borramos de la Cola el proceso
        v_sql := 'DELETE FROM ' || get_json('__COLA_MOTOR__', o_json) || ' WHERE id = ' || v_id_cola;
        v_json:=query_db_json(get_campo('__IP_CONEXION_CLIENTE__',v_xml3),get_campo('__IP_PORT_CLIENTE__',v_xml3)::integer,v_sql);
        --EXECUTE 'DELETE FROM ' || get_json('__COLA_MOTOR__', o_json) || ' WHERE id = ' || v_id_cola;
        o_json := logjson(o_json, 'cola motor' || get_json('__COLA_MOTOR__', o_json) || ' ');
        o_json := logjson(o_json, 'cola motor v_id_cola ' || v_id_cola || ' ');
	*/
        o_json := logjson(o_json, 'Importacion exitosa URI: [ ' || get_json('Uri', o_json) || ' ]');
    --ELSE
        /**Aumentamos los reintentos de la Cola
        v_sql := 'UPDATE ' || get_json('__COLA_MOTOR__', o_json) || ' SET reintentos = reintentos + 1 WHERE id = ' || v_id_cola;
        v_json:=query_db_json(get_campo('__IP_CONEXION_CLIENTE__',v_xml3),get_campo('__IP_PORT_CLIENTE__',v_xml3)::integer,v_sql);
        --EXECUTE 'UPDATE ' || get_json('__COLA_MOTOR__', o_json) || ' SET reintentos = reintentos + 1 WHERE id = ' || v_id_cola;
        o_json := logjson(o_json, 'Aumenta Reintentos de Importacion URI: [ ' || get_json('Uri', o_json) || ' ]');
	*/
    END IF;
    RETURN o_json;
END
$BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION basura_importacion(i_parametros json)
    RETURNS json AS
$BODY$
DECLARE
    v_sql varchar;
    o_json json := i_parametros;
    v_id_cola varchar;
    v_json json;
    v_xml3  varchar;
BEGIN
    o_json := put_json(o_json,'__SECUENCIAOK__','0');
    o_json := logjson(o_json, ' Importando URI: [ ' || get_json('Uri', o_json) || ' ] en Finalizacion con Basura -> Analizar situacion');
    v_id_cola := get_json('__ID_DTE__',o_json);
    v_xml3 := get_parametros_motor('','BASE_COLAS');

    v_sql := 'UPDATE ' || get_json('__COLA_MOTOR__', o_json) || ' SET reintentos = 11 WHERE id = ' || v_id_cola;
    v_json:=query_db_json(get_campo('__IP_CONEXION_CLIENTE__',v_xml3),get_campo('__IP_PORT_CLIENTE__',v_xml3)::integer,v_sql);
    --EXECUTE 'UPDATE ' || get_json('__COLA_MOTOR__', o_json) || ' SET reintentos = reintentos + 1 WHERE id = ' || v_id_cola;
    o_json := logjson(o_json, 'Aumenta Reintentos de Importacion URI: [ ' || get_json('Uri', o_json) || ' ]');
    RETURN o_json;
END
$BODY$
LANGUAGE plpgsql;


