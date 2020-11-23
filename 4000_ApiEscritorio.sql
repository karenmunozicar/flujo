delete from isys_querys_tx where llave='4000';
--Para hacer log de todo
insert into isys_querys_tx values ('4000',5,9,16,'LOG_JSON',0,0,0,1,1,10,10);
insert into isys_querys_tx values ('4000',10,9,1,'select generico10k_4000(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,-1);
insert into isys_querys_tx values ('4000',20,1,8,'LLAMADA AL FLUJO 6000',6000,0,0,1,1,30,30);
insert into isys_querys_tx values ('4000',30,9,1,'select procesa_respuesta_4000(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,0,0);

CREATE or replace FUNCTION procesa_respuesta_4000(json) RETURNS json AS $$
DECLARE
    json1                alias for $1;
    json2                json;
        respuesta1       json;
        resp1            varchar;
        tipo_tx1        varchar;
BEGIN
        json2:=json1;
        BEGIN
                respuesta1:=split_part(get_json('RESPUESTA',json2),chr(10)||chr(10),2)::json;
        EXCEPTION WHEN OTHERS THEN
                return response_requests_6000('2','Falla procesar respuesta','',json2);
        END;
        resp1:=get_json('RESPUESTA',respuesta1);
        tipo_tx1:=get_json('tipo_tx',json2);
        if tipo_tx1='logIn' then
                resp1:='';
        end if;
        return response_requests_6000(get_json('CODIGO_RESPUESTA',respuesta1),get_json('MENSAJE_RESPUESTA',respuesta1),resp1,json2);
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION generico10k_4000(json) RETURNS json AS $$
DECLARE
    json1                alias for $1;
    json2                json;
        stSec   record;
        tipo_tx1        varchar;
BEGIN
        json2:=json1;
        json2:=put_json(json2,'__SECUENCIAOK__','0');
        json2:=put_json(json2,'__TIPO_RESPUESTA_API__','4000');

        tipo_tx1:=get_json('tipo_tx',json2);
        json2:=logjson(json2,'TX='||tipo_tx1||' Procesos Activos='||get_json('__PROC_ACTIVOS__',json2));

        if get_json('HTTP_X_API_KEY',json2)<>'oEUG9G7ek25pd3tM22EsW7QOtvlg4ZGr3mCepqyT' then
                return response_requests_6000('2','No Autorizado','',json2);
        end if;
        if tipo_tx1='search_emitidos' then
                tipo_tx1='pivote_busqueda_15100';
                json2:=put_json(json2,'tipo_tx',tipo_tx1);
        end if;

        select * into stSec from define_secuencia_generico10k where tipo_tx=tipo_tx1 and api;
        if not found then
                return response_requests_6000('2','Servicio '||tipo_tx1||' No Habilitado','',json2);
        end if;
        if tipo_tx1 in ('cesion','pivote_busqueda_15100') then
                json2:=put_json(json2,'CAMPOS_BUSQUEDA','x.codigo_txel,tipo_dte,folio,x.rut_emisor,x.rut_receptor,monto_neto,monto_excento,monto_iva,monto_total,x.fecha_sii,x.estado_sii,mensaje_sii,uri,referencias,x.fecha_vencimiento,estado_reclamo as reclamo_mercaderia,estado_nar as reclamo_por_contenido');
                json2:=put_json(json2,'APP','buscarNEW_emitidos');
        end if;

        json2:=put_json(json2,'aplicacion','DTE');
        json2:=put_json(json2,'host_canal','escritorio.acepta.com');
        json2:=put_json(json2,'SERVER_NAME_ORI','escritorio.acepta.com');
        json2:=put_json(json2,'__SECUENCIAOK__','20');
        return json2;
END;
$$ LANGUAGE plpgsql;

