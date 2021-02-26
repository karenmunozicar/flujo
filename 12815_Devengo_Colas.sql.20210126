delete from isys_querys_tx where llave='12815';
insert into isys_querys_tx values ('12815',10,1,1,'select crea_devengo_12815(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,1000);
insert into isys_querys_tx values ('12815',20,1,8,'Flujo Chile Compra',12813,0,0,0,0,30,30);
insert into isys_querys_tx values ('12815',30,1,1,'select valida_respuesta_chile_compra_12815(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,1000);
insert into isys_querys_tx values ('12815',40,1,8,'Flujo Devengo',12814,0,0,0,0,50,50);
insert into isys_querys_tx values ('12815',50,1,1,'select procesa_resp_devengo_12815(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,1000);

insert into isys_querys_tx values ('12815',1000,1,1,'select sp_procesa_respuesta_cola_motor_json(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,0,0);

CREATE OR REPLACE FUNCTION crea_devengo_12815(character varying) RETURNS varchar as $$
declare
        xml1    alias for $1;
        xml2    varchar;
        v_rut_emisor_devengo varchar;
        v_uri   varchar;
        v_rut_receptor_devengo varchar;
        v_cod_oc varchar;
        v_cod_rc varchar;
        v_monto_devengo varchar;
        v_periodo varchar;
        v_ejercicio varchar;
        v_tipo_dte varchar;
        v_codigo_devengo bigint;
        v_area_tx       varchar;
        v_folio_compromiso      varchar;
        tipo_devengo    varchar;
        v_json_devengo_sigfe    json;
        v_titulo_devengo       varchar;
        v_descripcion_devengo  varchar;
        v_folio_dte            varchar;
        v_contabiliza_iva      boolean;
        v_tipo_devengo         varchar;
        v_area_trx_oc          varchar;
        v_area_trx_dte         varchar;
        v_num_area_trx         integer;
        v_forma_pago           varchar;  -- FGE - 20190814 - Marcador de Forma de Pago
        json2           json;
        codigo_txel1    bigint;
        v_razon_social_receptor  varchar;
	v_codigo_txel          varchar;
	v_reg_oc                     record;
	v_reg_cliente                record;
	v_reg_indexer_hash_recibido  record;
	v_num_area	integer;	

	v_host                       varchar;
begin
        xml2:=xml1;
        xml2:=put_campo(xml2,'__SECUENCIAOK__','0');
	-- FGE - 20200515 - Obtenemos host para poder manejar las diferencias en certificacion y produccion
	v_host = replace(pg_read_file('host_maquina'),chr(10),'');

        --Se crea el registro en dp_devengo con los datos que se tienen.
        v_rut_emisor_devengo:=get_campo('RUT_RECEPTOR',xml2);
        v_uri:=get_campo('URI_IN',xml2);
        v_rut_receptor_devengo:=get_campo('RUT_EMISOR',xml2);
        v_cod_oc:=get_campo('DP_COD_OC',xml2);
	-- FGE - 20200617 - Revisamos si podemos obtener la OC si faltase
        if v_cod_oc = '' or not v_cod_oc ~ '\d+\-\d+\-(CM|SE|MC|AG|D1|CC)\d{2}' then
        	select dp_obtiene_oc_flujo(get_xml('ID_SOLICITUD_WF', data_dte)) into v_cod_oc from dte_recibidos where codigo_txel = v_codigo_txel::bigint;
        end if;

        v_folio_compromiso:=get_campo('DP_FOLIO_COMPROMISO',xml2);
        -- FGE - 20201202 - Area Trx viene ahora de una funcion
        -- v_area_tx:=get_campo('DP_AREA_TX',xml2);
        v_area_tx:=dp_obtiene_area_trx(v_cod_oc, v_rut_emisor_devengo::integer);
        
        v_cod_rc:=get_campo('DP_COD_RC',xml2);
        v_monto_devengo:=get_campo('MONTO_TOTAL',xml2);
        v_periodo:=split_part(get_campo('FECHA_EMISION',xml2),'-',2);
        v_ejercicio:=split_part(get_campo('FECHA_EMISION',xml2),'-',1);
        v_tipo_dte:=get_campo('TIPO_DTE',xml2);
        v_folio_dte:=get_campo('FOLIO', xml2);
	--Se validan datos para saber si es devengo manual o Aut.
        -- FGE - 20191016 - Tambien verifico que v_area_tx pueda incluir solamente el area transaccional
        --if v_area_tx = '' or v_folio_compromiso='' then
        tipo_devengo:='AUT';
        v_codigo_txel = coalesce(nullif(get_campo('CODIGO_TXEL', xml2), ''), '0');

	--FGE 20200515 if v_area_tx = '' or length(trim(v_area_tx)) = 3 then
	if length(v_area_tx) <> 7 then
                if v_cod_oc <> '' then
                        select area_transaccional from de_emitidos where rut_emisor = v_rut_emisor_devengo::integer and folio = v_cod_oc and coalesce(folio_compromiso, '') <> ''  into v_reg_oc;
                        if found then
                                --v_area_tx:=v_reg_oc.area_transaccional;
                                --update dte_recibidos set parametro1 = substring(v_area_tx, 5, 3) where codigo_txel = v_codigo_txel::bigint;
				if v_host = 'certificacion2' then
                                    v_area_tx:=substring(v_reg_oc.area_transaccional, 5, 3);
                                else
                                    v_area_tx:=v_reg_oc.area_transaccional;
                                end if;
				update dte_recibidos set parametro1 = v_area_tx where codigo_txel = v_codigo_txel::bigint;
                        end if;
                end if;
		-- FGE 20200515 select gob_partida_capitulo from maestro_clientes where rut_emisor = v_rut_emisor_devengo::integer into v_reg_cliente;
                if length(trim(v_area_tx)) = 3 then
                        -- FGE - 20191016 - Primero verifico que el area asignada sea un area que existe.
			/* FGE 20200515 
                        select codigo from indexer_hash_recibidos where rut_emisor = v_rut_emisor_devengo::integer and estado = 'HABILITADA' and codigo = trim(v_area_tx) into v_reg_indexer_hash_recibido;
                        if found then
                                v_area_tx:=v_reg_cliente.gob_partida_capitulo || trim(v_area_tx);
                                update dte_recibidos set parametro1 = substring(v_area_tx, 5, 3) where codigo_txel = v_codigo_txel::bigint;
                        else
                                v_area_tx:='';
                        end if;*/
			if v_host = 'certificacion2' then
                        	select codigo from indexer_hash_recibidos where rut_emisor = v_rut_emisor_devengo::integer and estado = 'HABILITADA' and codigo = trim(v_area_tx) into v_reg_indexer_hash_recibido;
                        else
                        	select codigo from indexer_hash_recibidos where rut_emisor = v_rut_emisor_devengo::integer and estado = 'HABILITADA' and codigo = get_json('gob_partida_capitulo', json2) || trim(v_area_tx) into v_reg_indexer_hash_recibido;
                        end if;
                        if found then
                                v_area_tx:=v_reg_indexer_hash_recibido.codigo;
                                update dte_recibidos set parametro1 = v_area_tx where codigo_txel = v_codigo_txel::bigint;
                        else
                                v_area_tx:='';
                        end if;
                end if;
                if v_area_tx = '' then
                        select count(1) into v_num_area from indexer_hash_recibidos where rut_emisor = v_rut_emisor_devengo::integer and estado = 'HABILITADA' and codigo <> 'SIN_DATO';
                        if v_num_area = 1 then
				--FGE 20200515
                                select codigo from indexer_hash_recibidos where rut_emisor = v_rut_emisor_devengo::integer and estado = 'HABILITADA' and codigo <> 'SIN_DATO' into v_reg_indexer_hash_recibido;
				if v_host = 'certificacion2' then
                                     v_area_tx:=get_json('gob_partida_capitulo', json2) || v_reg_indexer_hash_recibido.codigo::varchar;
                                else
                                     v_area_tx:=v_reg_indexer_hash_recibido.codigo::varchar;
                                end if;
                                --v_area_tx:=v_reg_cliente.gob_partida_capitulo || v_reg_indexer_hash_recibido.codigo::varchar;
                                --update dte_recibidos set parametro1 = substring(v_area_tx, 5, 3) where codigo_txel = v_codigo_txel::bigint;
				update dte_recibidos set parametro1 = v_area_tx where codigo_txel = v_codigo_txel::bigint;
                        end if;
                end if;
        end if;

        if v_folio_compromiso='' then
                -- TODO: buscar OC y luego verificar el folio compromiso
                if v_cod_oc <> '' then
                        select folio_compromiso from de_emitidos where (rut_emisor = v_rut_emisor_devengo::integer or rut_emisor = 61608700) and folio = v_cod_oc and coalesce(folio_compromiso, '') <> ''  into v_reg_oc;
                        if found then
                                v_folio_compromiso:=v_reg_oc.folio_compromiso;
                        else
                                tipo_devengo:='MAN';
                        end if;
                else
                        tipo_devengo:='MAN';
                end if;
        end if;

        --Se validan datos para saber si es devengo manual o Aut.
        if v_area_tx = '' or v_folio_compromiso='' then
                tipo_devengo :='MAN';
        else
                tipo_devengo :='AUT';
        end if;

        --Razon social del receptor del devengo para el titulo
        select nombre into v_razon_social_receptor from contribuyentes where rut_emisor = v_rut_receptor_devengo::integer;
        if not found then
                v_razon_social_receptor:='';
        end if;

        --Validamos si existe Devengo para esta URL
        select codigo_dv into v_codigo_devengo from dp_devengo where uri_dte=v_uri;
        if not found then
                v_titulo_devengo:= case when (v_tipo_dte::integer = 33) then 'FA' when (v_tipo_dte::integer = 34) then 'FE' when (v_tipo_dte::integer = 56) then 'ND' when (v_tipo_dte::integer = 61) then 'NC' end;
                v_titulo_devengo:= v_titulo_devengo || ' / ' || v_folio_dte || ' / ' || v_rut_receptor_devengo || ' / ' || v_cod_oc || ' / ' || v_razon_social_receptor;

                select gob_contabiliza_iva into v_contabiliza_iva from maestro_clientes where rut_emisor = v_rut_emisor_devengo::integer;
                if found then
                        if v_contabiliza_iva = true and v_tipo_dte in ('33', '43') then
                                tipo_devengo:='MAN';
                        end if;
                end if;

                xml2:=logapp(xml2,'Inserto dp_devengo uri='||v_uri||' codigo_txel='||get_campo('CODIGO_TXEL',xml2));
                codigo_txel1:=get_campo('CODIGO_TXEL',xml2)::bigint;

                select parametro1, get_xml('FmaPago', data_dte) into v_area_trx_dte, v_forma_pago from dte_recibidos where codigo_txel = codigo_txel1;
                if found then
                        if v_cod_oc <> '' and coalesce(v_area_trx_dte, '') in ('', 'SIN_DATO') then
                                select coalesce(area_transaccional, '') into v_area_trx_oc from de_emitidos where folio = v_cod_oc and rut_emisor = v_rut_emisor_devengo::integer and rut_receptor = v_rut_receptor_devengo::integer and origen = 'MP';
                                if found and v_area_trx_oc <> '' then
                                        xml2:=logapp(xml2, 'Asigno areaTrx ' || v_area_trx_oc || ' de OC ' || v_cod_oc || ' a DTE ' || codigo_txel1::varchar);
                                        update dte_recibidos set parametro1 = substring(v_area_trx_oc, 5, 3) where codigo_txel=codigo_txel1;
                                end if;
                        else
                                select count(1) into v_num_area_trx from indexer_hash_recibidos where rut_emisor = v_rut_emisor_devengo::integer;
                                if v_num_area_trx = 1 then
                                        select codigo into v_area_trx_dte from indexer_hash_recibidos where rut_emisor = v_rut_emisor_devengo::integer;
                                        xml2:=logapp(xml2, 'Asigno areaTrx ' || v_area_trx_dte || ' de indexer_hash_recibidos a DTE ' || codigo_txel1::varchar);
                                        update dte_recibidos set parametro1 = v_area_trx_dte where codigo_txel=codigo_txel1;
                                end if;
                        end if;
                end if;

                -- FGE - 20190814 - Incluyo el marcador de pago de contado
                if v_forma_pago <> '1' then
                    v_forma_pago := '2';
                end if;

                update dte_recibidos set data_dte = put_data_dte(data_dte, 'CODIGO_RC', v_cod_rc::varchar) where codigo_txel=codigo_txel1;
                insert into dp_devengo (codigo_dv,  uri_dte, rut_receptor, rut_emisor,  folio_requerimiento, fecha_creacion, estado, codigo_oc, codigo_rc,  monto_devengo, cod_moneda, periodo, ejercicio,area_transaccional, anticipo, tipo_devengo, tipo_dte, titulo, descripcion,dte_codigo_txel, forma_pago) values (default, v_uri,v_rut_receptor_devengo::integer, v_rut_emisor_devengo::integer, v_folio_compromiso,now(),'BORRADOR',v_cod_oc,v_cod_rc,v_monto_devengo::bigint, 'CLP',v_periodo::integer, v_ejercicio::integer,v_area_tx, 'N',tipo_devengo , v_tipo_dte::integer, substring(v_titulo_devengo, 1, 79), substring(v_titulo_devengo, 1, 249),get_campo('CODIGO_TXEL',xml2), v_forma_pago::integer) returning codigo_dv into v_codigo_devengo ;
                perform dp_act_estado_devengo(v_codigo_devengo::bigint, 'BORRADOR', '');
                if not found then
                        xml2:=logapp(xml2,'Error al graba dp_devengo');
                        xml2:=put_campo(xml2,'__SECUENCIAOK__','1000');
                        return xml2;
                end if;
        else
                xml2:=logapp(xml2,'Aplica Proxy v_codigo_devengo '||v_uri);
        end if;


        xml2:=logapp(xml2,'Datos UPDATE' || v_rut_emisor_devengo ||' ' || v_rut_receptor_devengo||' ' || v_tipo_dte ||' ' ||v_cod_oc ||' ' || v_cod_rc );
        --Se marca la Recepción conforme en estado EN_CURSO.
        --update token_de_emitidos set estado='EN_CURSO' where rut_emisor=v_rut_emisor_devengo::integer and rut_receptor=v_rut_receptor_devengo::integer and folio=v_cod_oc and token=v_cod_rc;
	-- FGE - 20191118 - Prevenimos la llamada al 12813 con el siguiente IF.
        if v_area_tx = '' then
               tipo_devengo = 'MAN';
        end if;
	update token_de_emitidos set estado='EN_CURSO', codigo_dv = v_codigo_devengo::bigint where rut_emisor=v_rut_emisor_devengo::integer and rut_receptor=v_rut_receptor_devengo::integer and folio=v_cod_oc and token=v_cod_rc;
	
        if (tipo_devengo ='MAN') then
                codigo_txel1:=get_campo('CODIGO_TXEL',xml2)::bigint;
                update dte_recibidos set data_dte = put_data_dte(data_dte, 'TIPO_DEVENGO', 'MAN') where codigo_txel = codigo_txel1;
                xml2:=logapp(xml2,'Devengo Manual, no sigue con los MS Area Tx -> ' || v_area_tx || ' Folio Comp-->' || v_folio_compromiso);
                xml2:=put_campo(xml2,'__SECUENCIAOK__','1000');
                return xml2;
        end if;



        --Se prepara la llamada al servicio de Sigfe

        --datos
        v_json_devengo_sigfe:='{}'::json;
        v_json_devengo_sigfe:=put_json(v_json_devengo_sigfe,'partida', substring(v_area_tx, 1, 2));
        v_json_devengo_sigfe:=put_json(v_json_devengo_sigfe,'capitulo', substring(v_area_tx, 3, 2));
        v_json_devengo_sigfe:=put_json(v_json_devengo_sigfe,'areaTransaccional', substring(v_area_tx, 5, 3));
        v_json_devengo_sigfe:=put_json(v_json_devengo_sigfe,'ejercicio', v_ejercicio);
        v_json_devengo_sigfe:=put_json(v_json_devengo_sigfe,'folio',v_folio_compromiso);

        --xml2:='{}'::json;
        xml2:=put_campo(xml2,'codigo_dv',v_codigo_devengo::varchar);
        xml2:=get_parametros_motor(xml2,'OBTIENECOMPROMISOS_CHC');
	xml2:=put_campo(xml2,'__TIMEOUT_SERV_PXML__','60');
        xml2:=put_campo(xml2,'HOST_MS',get_campo('__IP_CONEXION_CLIENTE__',xml2));
        xml2:=put_campo(xml2,'URI_MS',get_campo('PARAMETRO_RUTA',xml2));
        xml2:=put_campo(xml2,'DATA_JSON',encode_hex(v_json_devengo_sigfe::varchar));
        xml2:=put_campo(xml2,'LARGO_JSON',(length(encode_hex(v_json_devengo_sigfe::varchar))/2)::varchar);

	xml2:=logapp(xml2,'OBTIENECOMPROMISOS_CHC ='||v_json_devengo_sigfe::varchar);
        xml2:=put_campo(xml2,'__SECUENCIAOK__','20');
        return xml2;
end;
$$ LANGUAGE plpgsql;


create or replace function  valida_respuesta_chile_compra_12815 (json)
 returns json as $$
declare
        json1           alias for $1;
        json2           json;
        resp1           varchar;
        cod_devengo1    varchar;
        cod_devengo_num1        integer;
        num_detalles    integer;
        cod_oc1         varchar;
        monto_total1    integer;
        v_codigo_txel   varchar;
        cod_devengo_detalle  bigint;
begin
        json2:=json1;

        num_detalles:=0;

        json2:=put_json(json2,'__SECUENCIAOK__','0');

        json2:=logjson(json2,'INGRESO DATA -->'|| json2::varchar);

        resp1:=get_json('RESPUESTA_COLA',json2);
        if (resp1<>'OK') then
                -- FGE - 20200723 - Vamos a reintentar en lugar de pasarlo a manual
                json2:=logjson(json2,'no trae respuesta de 12813, volvemos a intentar despues en dp_devengo para codigo_dv='||cod_devengo_num1::varchar);
                json2:=put_json(json2,'RESPUESTA','Status: 444 NK');
                return sp_procesa_respuesta_cola_motor_json(json2);
                -- json2:=logjson(json2,'Falla Servicio en Flujo 12813, Devengo Manual');
                -- json2:=put_json(json2,'__SECUENCIAOK__','1000');
                -- return json2;
        end if;

        --Se validan la cantidad de registros grabados en dp_devengo_detalle para el devengo. Si hay mas de uno, es Manual.
        cod_devengo1:=get_json('V_CODIGO_DV', json2);
        if is_number (cod_devengo1) then
                cod_devengo_num1:=cod_devengo1::integer;
        else
                json2:=logjson(json2,'Codigo Devengo no es numerico -->'||cod_devengo1);
                json2:=put_json(json2,'__SECUENCIAOK__','1000');
                return json2;
        end if;

        select count(1) into num_detalles from dp_devengo_detalle where codigo_dv=cod_devengo_num1;
        select dte_codigo_txel into v_codigo_txel from dp_devengo where codigo_dv = cod_devengo_num1;

	-- FGE - 20200608 - Si es un tipo demanda = 02 entonces se va a extrapresupuesta
        if get_json('TIPO_DEMANDA', json2) <> '01' then
                 --Sin detalles, entonces se deja manual.
                 update dte_recibidos set data_dte = put_data_dte(put_data_dte(data_dte, 'TIPO_DEVENGO', 'EXT'), 'ESTADO_DEVENGO', 'EXTRA_PRESUPUESTARIO') where codigo_txel = v_codigo_txel::bigint;
                 update dp_devengo set tipo_devengo = 'EXT', estado = 'EXTRA_PRESUPUESTARIO' where codigo_dv = cod_devengo_num1;
                 json2:=logjson(json2,'DTE con devengo extra presupuestario');
                 json2:=put_json(json2,'__SECUENCIAOK__','1000');
                 return json2;
        end if;

        if (num_detalles=0) then
                --Sin detalles, entonces se deja manual.
		-- FGE - 20200608
                --update dte_recibidos set data_dte = put_data_dte(data_dte, 'TIPO_DEVENGO', 'MAN') where codigo_dv = v_codigo_txel::bigint;
                update dte_recibidos set data_dte = put_data_dte(data_dte, 'TIPO_DEVENGO', 'MAN') where codigo_txel = v_codigo_txel::bigint;
                update dp_devengo set tipo_devengo = 'MAN' where codigo_dv = cod_devengo_num1;
                json2:=logjson(json2,'Devengo sin detalles en tabla dp_devengo_detalle codigo-->'||cod_devengo1);
                json2:=put_json(json2,'__SECUENCIAOK__','1000');
                return json2;
        end if;

        -- FGE - 20201216 - Asiento Automatico
        /*
        if num_detalles > 1 then
                --Usuario debe seleccionar, queda en devengo manual
                update dte_recibidos set data_dte = put_data_dte(data_dte, 'TIPO_DEVENGO', 'MAN') where codigo_txel = v_codigo_txel::bigint;
                update dp_devengo set tipo_devengo='MAN' where codigo_dv=cod_devengo_num1;
                json2:=logjson(json2,'Devengo con mas de 1 detalle, Devengo manual  codigo-->'||cod_devengo1);
                json2:=put_json(json2,'__SECUENCIAOK__','1000');
                return json2;
        end if;


        --Devengo tiene solo un detalle, entonces se hace el devengo aut.
        json2:=put_json(json2,'codigo_dv',cod_devengo1);

        --Como solo hay un registro, se actualiza el valor de DEBE con el valor del monto neto.
        if get_json('TIPO_DTE',json2)='33' then
                monto_total1:=get_json('MONTO_NETO',json2)::bigint;
        elsif get_json('TIPO_DTE',json2)='34' then
                monto_total1:=get_json('MONTO_EXENTO',json2)::bigint;
        end if;

        update dp_devengo_detalle set debe=monto_total1 where codigo_dv=cod_devengo_num1;
        if not found then
                json2:=logjson(json2,'Falla Actualizar dp_devengo_detalle cod_dv -->'||cod_devengo1);
                json2:=put_json(json2,'__SECUENCIAOK__','1000');
                return json2;
        end if;
        */

        -- FGE - 20201217 - Ahora Dipres pide que el asiento automatico sea solamente con una imputacion contable... sin comentarios.
        select count(1) into num_detalles from (select distinct cod_imputacion from dp_devengo_detalle where codigo_dv = cod_devengo_num1) imputaciones;
        if num_detalles <> 1 then
                --Usuario debe seleccionar, queda en devengo manual
                update dte_recibidos set data_dte = put_data_dte(data_dte, 'TIPO_DEVENGO', 'MAN') where codigo_txel = v_codigo_txel::bigint;
                update dp_devengo set tipo_devengo='MAN' where codigo_dv=cod_devengo_num1;
                json2:=logjson(json2,'Devengo con mas de 1 imputacion, Devengo manual  codigo-->'||cod_devengo1);
                json2:=put_json(json2,'__SECUENCIAOK__','1000');
                return json2;
        end if;


        -- FGE - 20200829 - Asiento automatico
        json2:=put_json(json2, 'codigo_dv', cod_devengo1);
        monto_total1:=coalesce(nullif(get_json('MONTO_NETO', json2), ''), '0')::bigint + coalesce(nullif(get_json('MONTO_EXENTO', json2), ''), '0')::bigint;

        -- FGE - 20201222 - Ahora si no hay cuenta 5, entonces se utiliza cuenta 1, si no hay ninguna de las dos, entonces va a manual
        select id into cod_devengo_detalle from dp_devengo_detalle where codigo_dv = cod_devengo_num1 and cuenta_debe like '5%' limit 1;
        if not found then
            select id into cod_devengo_detalle from dp_devengo_detalle where codigo_dv = cod_devengo_num1 and cuenta_debe like '1%' limit 1;
            if not found then
                update dte_recibidos set data_dte = put_data_dte(data_dte, 'TIPO_DEVENGO', 'MAN') where codigo_txel = v_codigo_txel::bigint;
                update dp_devengo set tipo_devengo='MAN' where codigo_dv=cod_devengo_num1;
                json2:=logjson(json2,'Devengo sin cuenta 5 o 1, Devengo manual  codigo-->'||cod_devengo1);
                json2:=put_json(json2,'__SECUENCIAOK__','1000');
                return json2;
            end if;
        end if;

        update dp_devengo_detalle set debe = monto_total1 where codigo_dv = cod_devengo_num1 and id = cod_devengo_detalle;        

        --Preparo la llamada al servicio

        json2:=dp_envia_devengo_12814(json2);

        if get_json('LLAMA_FLUJO',json2)='SI' then
                json2:=put_json(json2,'__SECUENCIAOK__','40');
        else
                json2:=put_json(json2,'__SECUENCIAOK__','1000');
        end if;

        return json2;

end;
$$ LANGUAGE plpgsql;


create or replace function  procesa_resp_devengo_12815 (json)
 returns json as $$
declare
        json1           alias for $1;
        json2           json;
        cod_devengo1    varchar;
        cod_devengo_num1        integer;
        v_respuesta     varchar;
        v_ticket        varchar;
        v_uri           varchar;
        id1     varchar;
        cola1   varchar;
begin
        json2:=json1;
--      json2:=put_json(json2,'__SECUENCIAOK__','0');
        id1:=get_json('__ID_DTE__',json2);
        cola1:=get_json('__COLA_MOTOR__',json2);
        json2:=put_json(json2,'__SECUENCIAOK__','0');

        cod_devengo1:=get_json('codigo_dv',json2);
        if is_number (cod_devengo1) then
                cod_devengo_num1:=cod_devengo1::integer;
        else
                json2:=logjson(json2,'Codigo Devengo no es numerico -->'||cod_devengo1);
                return json2;
        end if;

        select ticket_id into v_ticket from dp_devengo where codigo_dv=cod_devengo_num1;
        if not found then
                json2:=put_json(json2,'RESPUESTA','Status: 444 NK');
                json2:=logjson(json2,'ticket_id no encontrado en dp_devengo para codigo_dv='||cod_devengo_num1::varchar);
                --Se adeltanta 1 hora para prox intento
                --v_uri:=get_json('URI_IN',json2);
                --execute 'update '||get_json('__COLA_MOTOR__',json2)||' set fecha= now() + interval ''1 hour'', reintentos=reintentos+1 where id='||id1;
                return sp_procesa_respuesta_cola_motor_json(json2);
        else
                if v_ticket ='' then
                        json2:=logjson(json2,'ticket_id vacio en dp_devengo para codigo_dv='||cod_devengo_num1::varchar);
                        json2:=put_json(json2,'RESPUESTA','Status: 444 NK');
                        return sp_procesa_respuesta_cola_motor_json(json2);
                        --execute 'update '||get_json('__COLA_MOTOR__',json2)||' set fecha= now() + interval ''1 hour'', reintentos=reintentos+1 where id='||id1;
                        --return json2;
                end if;
        end if;

        json2:=logjson(json2,'ticket_id ok en dp_devengo para codigo_dv='||cod_devengo_num1::varchar);
        --Exito se elimina de la cola
        --execute 'delete from '||get_json('__COLA_MOTOR__',json2)||' where id='||id1;
        json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
        return sp_procesa_respuesta_cola_motor_json(json2);
        --return json2;
end;
$$ LANGUAGE plpgsql;




