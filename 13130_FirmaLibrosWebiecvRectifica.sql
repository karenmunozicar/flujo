--Publica documento
delete from isys_querys_tx where llave='13130';

-- Validamos credenciales 
insert into isys_querys_tx values ('13130',5,1,8,'Servicio de Validacion de Firma',13220,109,106,0,0,8,8);

insert into isys_querys_tx values ('13130',8,1,1,'select pivote_datos_rectificatoria(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

-- Ingresamos los libros al proceso de firma.
insert into isys_querys_tx values ('13130',10,13,1,'select iecv.ingresar_libro_rectificado(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

-- Agregamos los datos del representante legal para obtener los codigos de rectificatoria
-- Usamos Long Timeout, por el proceso de conexion y posibles clonados de libros
insert into isys_querys_tx values ('13130',20,20,1,'select iecv.agregar_datos_representante(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

CREATE OR REPLACE FUNCTION pivote_datos_rectificatoria(i_parametros json)
    RETURNS json AS
$BODY$
DECLARE
    o_json json := i_parametros;
    v_respuesta json;
    v_comentario_bitacora varchar;
BEGIN
    o_json := put_json(o_json, '__SECUENCIAOK__', '0');

    IF (get_json('FIRMA_INVALIDA', i_parametros) = '1') THEN
        v_respuesta := split_part(get_json('RESPUESTA', i_parametros),chr(10)||chr(10),2)::json;
        o_json := put_json(o_json, 'CODIGO_RESPUESTA', get_json('CODIGO_RESPUESTA', v_respuesta));
        o_json := put_json(o_json, 'MENSAJE_RESPUESTA', get_json('MENSAJE_RESPUESTA', v_respuesta));
        v_comentario_bitacora := 'Usuario no tiene firma cargada o password incorrecta';
        o_json := put_json(o_json, 'COMENTARIO_BITACORA', v_comentario_bitacora);
        RETURN o_json;
    END IF;

    -- validamos el tipo_tx, ambas tx se validan los datos de la firma
    IF (get_json('tipo_tx', o_json) = 'iecv_obtener_datos') THEN -- Tenemos el datos del representante Legal
        o_json := put_json(o_json, '__SECUENCIAOK__', '20');
    ELSIF (get_json('tipo_tx', o_json) = 'iecv_enviar_libros') THEN -- Agregar los libros para que sean firmados
        o_json := put_json(o_json, '__SECUENCIAOK__', '10');
    END IF;

    RETURN o_json;
END
$BODY$
LANGUAGE plpgsql;

