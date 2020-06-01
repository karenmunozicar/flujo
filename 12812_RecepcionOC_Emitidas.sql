delete from isys_querys_tx where llave='12812';

insert into isys_querys_tx values ('12812',10,1,1,'select procesa_oc_12812(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

create or replace function procesa_oc_12812(varchar)
 returns varchar
as $function$
declare
        xml1 alias for $1;
        xml2 varchar;

        data_json       json;
        xml_entrada     varchar;
        campo1          record;
        query1          varchar;
        aux             varchar;
        i               integer:=0;
        uri1            varchar;
        data_dte1       varchar;
        data_dte2       varchar;
        de_cod_txel     bigint;

        err_msg         varchar:='';
        msj_defecto     varchar;
        pg_context      TEXT;
begin
        xml2:=xml1;
        --xml2:=logapp(xml2,'[procesa_oc_12812] INICIO xml2='||xml2);

        xml_entrada:=decode(get_campo('INPUT',xml2),'hex');
        --xml2:=logapp(xml2,'[procesa_oc_12812] xml_entrada='||xml_entrada);
        if strpos(xml_entrada,'<item name="custodium-uri">')>0 then
                uri1:=split_part(split_part(xml_entrada,'<item name="custodium-uri">',2),'</item>',1);
                xml2:=logapp(xml2,'[procesa_oc_12812] URI='||uri1);
        else
                uri1:='';
        end if;

        --se parsea data del xml y se retorna un json
        begin
                if strpos(xml_entrada,'<DE>')=0 then
                        xml2:=logapp(xml2,'[procesa_oc_12812] No es un DE valido URI='||uri1);
                        xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
                        xml2:=sp_procesa_respuesta_cola_motor(xml2);
                        return xml2;
                end if;
                xml_entrada:='<?xml version="1.0" encoding="ISO-8859-1"?><DE>'||split_part(split_part(xml_entrada,'<DE>',2),'</DE>',1)||'</DE>';
                data_json:=parsea_datos_oc2(xml_entrada);
                xml2:=logapp(xml2,'[procesa_oc_12812] xml_entrada json='||data_json::varchar);
        exception when others then
                xml2:=logapp(xml2,'[procesa_oc_12812] falla parsea_datos_oc');
                xml2:=logapp(xml2,'[procesa_oc_12812] xml_entrada='||replace(xml_entrada,chr(10),''));
                xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
                xml2:=sp_procesa_respuesta_cola_motor(xml2);
        end;

        --validar campos json minutmos para hacer busquedas
        if is_number(split_part(get_json('RutEmisor',data_json),'-',1)) is false or is_number(split_part(get_json('RutReceptor',data_json),'-',1)) is false or is_number(get_json('TipoDTE',data_json)) is false then
                xml2:=logapp(xml2,'[procesa_oc_12812] Datos incorrectos RUT_EMISOR='||split_part(get_json('RutEmisor',data_json),'-',1)||' RUT_RECEPTOR='||split_part(get_json('RutReceptor',data_json),'-',1)||' TIPO_DTE='||get_json('TipoDTE',data_json)||' URI='||uri1);
                xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
                xml2:=sp_procesa_respuesta_cola_motor(xml2);
                return xml2;
        end if;

        xml2:=logapp(xml2,'[procesa_oc_12812] procesa documento tipo_xml='||get_json('tipo_xml',data_json)||' URI='||uri1);
        --procesamos cuando es token
        if get_json('tipo_xml',data_json)='token' then
                --Buscamos la OC en de_emitidos
                if get_json('TipoToken',data_json)='' then
                        xml2:=logapp(xml2,'[procesa_oc_12812] Datos incorrectos TipoToken='||get_json('TipoToken',data_json)||' Token='||get_json('Token',data_json)||' URI='||uri1);
                        xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
                        xml2:=sp_procesa_respuesta_cola_motor(xml2);
                        return xml2;
                end if;

                if get_json('Token',data_json)='' and get_json('Estado',data_json)<>'ANULADA' then
                        xml2:=logapp(xml2,'[procesa_oc_12812] No Es anulacion, no viene Token ESTADO='||get_json('Estado',data_json)||' URI='||uri1);
                        xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
                        xml2:=sp_procesa_respuesta_cola_motor(xml2);
                        return xml2;
                end if;
                xml2:=logapp(xml2,'[procesa_oc_12812] Token='||get_json('Token',data_json)||' ESTADO='||get_json('Estado',data_json)||' URI='||uri1);

                --select * into campo1 from de_emitidos where rut_emisor=split_part(get_json('RutEmisor',data_json),'-',1)::integer and tipo_dte=get_json('TipoDTE',data_json)::integer and folio=get_json('Folio',data_json);
                execute 'select * from de_emitidos where rut_emisor='||split_part(get_json('RutEmisor',data_json),'-',1)||' and tipo_dte='||get_json('TipoDTE',data_json)||' and folio='''||get_json('Folio',data_json)||'''' into campo1;
                --if found then
                if campo1.codigo_txel is not null then
                        if not exists (select 1 from token_de_emitidos where rut_emisor=campo1.rut_emisor and tipo_dte=campo1.tipo_dte and folio=campo1.folio and token=get_json('Token',data_json) and tipo_token=get_json('TipoToken',data_json)) then
                                --valido estados
                                if strpos(lower(get_json('Estado',data_json)),'activa')>0 then
                                        data_json:=put_json(data_json,'Estado','ACTIVA');
                                elsif strpos(lower(get_json('Estado',data_json)),'anulada')>0 then
                                        data_json:=put_json(data_json,'Estado','ANULADA');
                                else
                                        xml2:=logapp(xml2,'[procesa_oc_12812] ESTADO EN TOKEN NO ES VALIDO URI='||uri1||' data_json='||data_json::varchar);
                                        xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
                                        xml2:=sp_procesa_respuesta_cola_motor(xml2);
                                        return xml2;
                                end if;

                                begin
                                        --inserto token
                                        insert into token_de_emitidos (id,rut_emisor,rut_receptor,tipo_dte,folio,tipo_token,token,estado,observacion,tmst,fecha_ingreso,fecha_modificacion,uri) values (default,campo1.rut_emisor,campo1.rut_receptor,campo1.tipo_dte,campo1.folio,get_json('TipoToken',data_json),get_json('Token',data_json),get_json('Estado',data_json),get_json('Observacion',data_json),get_json('Tmst',data_json::json)::timestamp,to_char(now(),'YYYYMMDD')::integer,null,uri1);
                                        if not found then
                                                 xml2:=logapp(xml2,'[procesa_oc_12812] Falla insert en token_de_emitidos URI='||uri1||' data_json='||data_json::varchar);
                                                xml2:=put_campo(xml2,'RESPUESTA','Status: 444 NK');
                                                xml2:=sp_procesa_respuesta_cola_motor(xml2);
                                                return xml2;
                                        end if;
                                        xml2:=logapp(xml2,'[procesa_oc_12812] Token insertado correctamente URI='||uri1);
                                        --guardo como ultimo token recibido para mostrar en la grilla de la busqueda OC
                                        if campo1.estado='ANULADA' then
                                                xml2:=logapp(xml2,'[procesa_oc_12812] DE EN ESTADO ANULADA, NO SE CAMBIA ESTADO A='||get_json('Estado',data_json)||' URI='||uri1);
                                                update de_emitidos set referencias=put_json(referencias,'TOKEN',data_json::varchar) where codigo_txel=campo1.codigo_txel;
                                        else
                                                update de_emitidos set estado=get_json('Estado',data_json), referencias=put_json(referencias,'TOKEN',data_json::varchar) where codigo_txel=campo1.codigo_txel;
                                        end if;
                                        if not found then
                                                xml2:=logapp(xml2,'[procesa_oc_12812] FALLA al actualizar referencias con ultimo token en de_emitidos URI='||uri1||' codigo_txel='||campo1.codigo_txel::varchar||' data_json='||data_json::varchar);
                                        end if;
                                exception when others then
                                        GET STACKED DIAGNOSTICS pg_context = PG_EXCEPTION_CONTEXT;
                                        err_msg:='Falló la orden SQL: '||SQLSTATE||'. El error fue: '||SQLERRM||', contexto: '||pg_context;
                                        xml2:=logapp(xml2,'[procesa_oc_12812] FALLA INSERT token_de_emitidos data_json='||data_json::varchar||' URI='||uri1);
                                        xml2:=logapp(xml2,'[procesa_oc_12812] FALLA INSERT err_msg='||err_msg);
                                        xml2:=put_campo(xml2,'RESPUESTA','Status: 444 NK');
                                        xml2:=sp_procesa_respuesta_cola_motor(xml2);
                                        return xml2;
                                end;
                        else
                                xml2:=logapp(xml2,'[procesa_oc_12812] TOKEN DUPLICADO RUT_EMISOR='||split_part(get_json('RutEmisor',data_json),'-',1)||' TIPO_DTE='||get_json('TipoDTE',data_json)||' FOLIO='||get_json('Folio',data_json)||' TOKEN='||get_json('Token',data_json)||' TIPO_TOKEN='||get_json('TipoToken',data_json)||' URI= '||uri1);
                                xml2:=logapp(xml2,'[procesa_oc_12812] data_json='||data_json::varchar);
                                xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
                                xml2:=sp_procesa_respuesta_cola_motor(xml2);
                                return xml2;
                        end if;
                else
                        xml2:=logapp(xml2,'[procesa_oc_12812] No existe orden de compra en de_emitidos RUT_EMISOR='||split_part(get_json('RutEmisor',data_json),'-',1)||' TIPO_DTE='||get_json('TipoDTE',data_json)||' FOLIO='||get_json('Folio',data_json)||' URI='||uri1);
                        xml2:=logapp(xml2,'[procesa_oc_12812] data_json='||data_json::varchar);
                        xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
                        xml2:=sp_procesa_respuesta_cola_motor(xml2);
                        return xml2;
                end if;
        --procesamos cuando es una fecha estimada de pago
        elsif get_json('tipo_xml',data_json)='fecha_estimada_pago' then
		-- Se modifica por canal ? 20190514
                /*xml2:=logapp(xml2,'[procesa_oc_12812] dte_emitidos rut_emisor='||split_part(get_json('RutReceptor',data_json),'-',1)||' tipo_dte='||get_json('TipoDTE',data_json)||' folio='||get_json('Folio',data_json)||' URI='||uri1);
                query1:='select * from dte_emitidos where rut_emisor='||split_part(get_json('RutReceptor',data_json),'-',1)||' and tipo_dte='||get_json('TipoDTE',data_json)||' and folio='||get_json('Folio',data_json); */
		xml2:=logapp(xml2,'[procesa_oc_12812] dte_recibidos rut_receptor='||split_part(get_json('RutEmisor',data_json),'-',1)||' tipo_dte='||get_json('TipoDTE',data_json)||' folio='||get_json('Folio',data_json)||' URI='||uri1);
                --query1:='select * from dte_recibidos where rut_receptor='||split_part(get_json('RutEmisor',data_json),'-',1)||' and tipo_dte='||get_json('TipoDTE',data_json)||' and folio='||get_json('Folio',data_json); 
                -- MVG - NBV - Se agrega filtro por rut_emisor -- 20190819
		query1:='select * from dte_recibidos where rut_receptor='||split_part(get_json('RutEmisor',data_json),'-',1)||' and tipo_dte='||get_json('TipoDTE',data_json)||' and folio='||get_json('Folio',data_json)||' and rut_emisor='||split_part(get_json('RutReceptor',data_json),'-',1); 
                xml2:=logapp(xml2,'[procesa_oc_12812] query='||query1);
                execute query1 into campo1;
                if campo1.codigo_txel is not null then
                        xml2:=logapp(xml2,'[procesa_oc_12812] actualizo dte_recibidos codigo_txel='||campo1.codigo_txel::varchar||' URI='||uri1);

                        data_dte1:=put_data_dte(coalesce(campo1.data_dte,''),'Estado',get_json('Estado',data_json));
                        data_dte1:=put_data_dte(data_dte1,'FechaEstimadaPago',get_json('FechaEstimadaPago',data_json));
                        data_dte1:=put_data_dte(data_dte1,'FormaPago',get_json('FormaPago',data_json));
                        data_dte1:=put_data_dte(data_dte1,'Modelopago',get_json('Modelopago',data_json));
                        if get_xml('Observacion',data_dte1)='' then
                                data_dte1:=put_data_dte(data_dte1,'Observacion',get_json('Observacion',data_json));
                        else
                                data_dte1:=put_data_dte(data_dte1,'Observacion',(get_xml('Observacion',data_dte1)||' - '||get_json('Observacion',data_json)));
                        end if;
                        xml2:=logapp(xml2,'[procesa_oc_12812] actualizo dte_emitidos codigo_txel='||campo1.codigo_txel::varchar||' data_dte='||data_dte1);
                        --update dte_emitidos set data_dte=put_data_dte(put_data_dte(put_data_dte(put_data_dte(put_data_dte(data_dte,'Estado',get_json('Estado',data_json)),'FechaEstimadaPago',get_json('FechaEstimadaPago',data_json)),'FormaPago',get_json('FormaPago',data_json)),'Modelopago',get_json('Modelopago',data_json)),'Observacion',get_json('Observacion',data_json)) where codigo_txel=campo1.codigo_txel;
                        --update dte_emitidos set data_dte=data_dte1 where codigo_txel=campo1.codigo_txel;
			update dte_recibidos set data_dte=data_dte1 where codigo_txel=campo1.codigo_txel;

                        if not found then
                                xml2:=logapp(xml2,'[procesa_oc_12812] FALLA al actualizar data_dte codigo_txel='||campo1.codigo_txel::varchar||' data_json='||data_json::varchar||' URI='||uri1);
                        end if;
                        if campo1.referencias is not null and campo1.referencias::varchar<>'[]' then
                                aux:=get_json_index(campo1.referencias,i);
                                while(aux<>'') loop
                                        if get_json('Tipo',aux::json)='801' then
                                                xml2:=logapp(xml2,'[procesa_oc_12812] Actualizando referencia='||aux||' URI='||uri1);
                                                --select codigo_txel,data_dte into de_cod_txel,data_dte2 from de_emitidos where rut_emisor=split_part(get_json('RutEmisor',data_json),'-',1)::integer and tipo_dte=801 and folio=get_json('Folio',aux::json);
                                                execute 'select codigo_txel,data_dte from de_emitidos where rut_emisor='||split_part(get_json('RutEmisor',data_json),'-',1)||' and tipo_dte=801 and folio='''||get_json('Folio',aux::json)||'''' into de_cod_txel,data_dte2;
                                                --if found then
                                                if de_cod_txel is not null then
                                                        data_dte2:=put_data_dte(coalesce(data_dte2,''),'Estado',get_json('Estado',data_json));
                                                        data_dte2:=put_data_dte(data_dte2,'FechaEstimadaPago',get_json('FechaEstimadaPago',data_json));
                                                        data_dte2:=put_data_dte(data_dte2,'FormaPago',get_json('FormaPago',data_json));
                                                        data_dte2:=put_data_dte(data_dte2,'Modelopago',get_json('Modelopago',data_json));
                                                        if get_xml('Observacion',data_dte2)='' then
                                                                data_dte2:=put_data_dte(data_dte2,'Observacion',get_json('Observacion',data_json));
                                                        else
                                                                data_dte2:=put_data_dte(data_dte2,'Observacion',(get_xml('Observacion',data_dte2)||' - '||get_json('Observacion',data_json)));
                                                        end if;
                                                        xml2:=logapp(xml2,'[procesa_oc_12812] data_dte2='||data_dte2);
                                                        update de_emitidos set data_dte=data_dte2 where codigo_txel=de_cod_txel;
                                                end if;
                                                --update de_emitidos set data_dte=put_data_dte(put_data_dte(put_data_dte(put_data_dte(put_data_dte(data_dte,'Estado',get_json('Estado',data_json)),'FechaEstimadaPago',get_json('FechaEstimadaPago',data_json)),'FormaPago',get_json('FormaPago',data_json)),'Modelopago',get_json('Modelopago',data_json)),'Observacion',get_json('Observacion',data_json)) where rut_emisor=split_part(get_json('RutEmisor',data_json),'-',1)::integer and tipo_dte=801 and folio=get_json('Folio',aux::json);
                                        end if;
                                        i:=i+1;
                                        aux:=get_json_index(campo1.referencias,i);
                                end loop;
                        end if;
                else
                        xml2:=logapp(xml2,'[procesa_oc_12812] No existe DTE en dte_emitidos RUT_EMISOR='||split_part(get_json('RutEmisor',data_json),'-',1)||' TIPO_DTE='||get_json('TipoDTE',data_json)||' FOLIO='||get_json('Folio',data_json)||' URI='||uri1);
                        xml2:=logapp(xml2,'[procesa_oc_12812] data_json='||data_json::varchar);
                        xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
                        xml2:=sp_procesa_respuesta_cola_motor(xml2);
                        return xml2;
                end if;
        --procesamos cuando es una confirmación de fecha de pago
        elsif get_json('tipo_xml',data_json)='fecha_pago' then
		-- Se modifica MVG 20190514
		/* xml2:=logapp(xml2,'[procesa_oc_12812] dte_emitidos rut_emisor='||split_part(get_json('RutReceptor',data_json),'-',1)||' tipo_dte='||get_json('TipoDTE',data_json)||' folio='||get_json('Folio',data_json)||' URI='||uri1);
                query1:='select * from dte_emitidos where rut_emisor='||split_part(get_json('RutReceptor',data_json),'-',1)||' and tipo_dte='||get_json('TipoDTE',data_json)||' and folio='||get_json('Folio',data_json);*/
		xml2:=logapp(xml2,'[procesa_oc_12812] dte_recibidos rut_receptor='||split_part(get_json('RutEmisor',data_json),'-',1)||' tipo_dte='||get_json('TipoDTE',data_json)||' folio='||get_json('Folio',data_json)||' URI='||uri1);
                --query1:='select * from dte_recibidos where rut_receptor='||split_part(get_json('RutEmisor',data_json),'-',1)||' and tipo_dte='||get_json('TipoDTE',data_json)||' and folio='||get_json('Folio',data_json);
                -- MVG - NBV - Se agrega filtro por rut_emisor -- 20190819
		query1:='select * from dte_recibidos where rut_receptor='||split_part(get_json('RutEmisor',data_json),'-',1)||' and tipo_dte='||get_json('TipoDTE',data_json)||' and folio='||get_json('Folio',data_json)||' and rut_emisor='||split_part(get_json('RutReceptor',data_json),'-',1); 
                xml2:=logapp(xml2,'[procesa_oc_12812] query='||query1);
                execute query1 into campo1;
                if campo1.codigo_txel is not null then
                        xml2:=logapp(xml2,'[procesa_oc_12812] actualizo dte_recibidos codigo_txel='||campo1.codigo_txel::varchar||' URI='||uri1);

                        data_dte1:=put_data_dte(coalesce(campo1.data_dte,''),'Estado',get_json('Estado',data_json));
                        data_dte1:=put_data_dte(data_dte1,'FechaPago',get_json('FechaPago',data_json));
                        data_dte1:=put_data_dte(data_dte1,'FormaPago',get_json('FormaPago',data_json));
                        data_dte1:=put_data_dte(data_dte1,'Modelopago',get_json('Modelopago',data_json));
                        if get_xml('Observacion',data_dte1)='' then
                                data_dte1:=put_data_dte(data_dte1,'Observacion',get_json('Observacion',data_json));
                        else
                                data_dte1:=put_data_dte(data_dte1,'Observacion',(get_xml('Observacion',data_dte1)||' - '||get_json('Observacion',data_json)));
                        end if;

                        --update dte_emitidos set data_dte=data_dte1 where codigo_txel=campo1.codigo_txel;
			update dte_recibidos set data_dte=data_dte1 where codigo_txel=campo1.codigo_txel;
                        if not found then
                                xml2:=logapp(xml2,'[procesa_oc_12812] FALLA al actualizar data_dte codigo_txel='||campo1.codigo_txel::varchar||' data_json='||data_json::varchar);
                        end if;
                        --busco todas las referencias (oc) del dte para actualizar
                        if campo1.referencias is not null and campo1.referencias::varchar<>'[]' then
                                aux:=get_json_index(campo1.referencias,i);
                                while(aux<>'') loop
                                        if get_json('Tipo',aux::json)='801' then
                                                xml2:=logapp(xml2,'[procesa_oc_12812] Actualizando referencia='||aux||' URI='||uri1);
                                                --select codigo_txel,data_dte into de_cod_txel,data_dte2 from de_emitidos where rut_emisor=split_part(get_json('RutEmisor',data_json),'-',1)::integer and tipo_dte=801 and folio=get_json('Folio',aux::json);
                                                execute 'select codigo_txel,data_dte from de_emitidos where rut_emisor='||split_part(get_json('RutEmisor',data_json),'-',1)||' and tipo_dte=801 and folio='''||get_json('Folio',aux::json)||'''' into de_cod_txel,data_dte2;
                                                --if found then
                                                if de_cod_txel is not null then
                                                        data_dte2:=put_data_dte(coalesce(data_dte2,''),'Estado',get_json('Estado',data_json));
                                                        data_dte2:=put_data_dte(data_dte2,'FechaPago',get_json('FechaPago',data_json));
                                                        data_dte2:=put_data_dte(data_dte2,'FormaPago',get_json('FormaPago',data_json));
                                                        data_dte2:=put_data_dte(data_dte2,'Modelopago',get_json('Modelopago',data_json));
                                                        if get_xml('Observacion',data_dte2)='' then
                                                                data_dte2:=put_data_dte(data_dte2,'Observacion',get_json('Observacion',data_json));
                                                        else
                                                                data_dte2:=put_data_dte(data_dte2,'Observacion',(get_xml('Observacion',data_dte2)||' - '||get_json('Observacion',data_json)));
                                                        end if;
                                                        update de_emitidos set data_dte=data_dte2 where codigo_txel=de_cod_txel;
                                                end if;
                                                --update de_emitidos set data_dte=put_data_dte(put_data_dte(put_data_dte(data_dte,'Estado',get_json('Estado',data_json)),'FechaPago',get_json('FechaPago',data_json)),'Observacion',(case when (data_dte<>'' and data_dte) is not null then get_xml('Observacion',data_dte)||' - '||get_json('Observacion',data_json) else get_json('Observacion',data_json) end)) where rut_emisor=split_part(get_json('RutEmisor',data_json),'-',1)::integer and tipo_dte=801 and folio=get_json('Folio',aux::json);
                                        end if;
                                        i:=i+1;
                                        aux:=get_json_index(campo1.referencias,i);
                                end loop;
                        end if;

                else
                        xml2:=logapp(xml2,'[procesa_oc_12812] No existe DTE en dte_emitidos RUT_EMISOR='||split_part(get_json('RutEmisor',data_json),'-',1)||' TIPO_DTE='||get_json('TipoDTE',data_json)||' FOLIO='||get_json('Folio',data_json)||' URI='||uri1);
                        xml2:=logapp(xml2,'[procesa_oc_12812] data_json='||data_json::varchar);
                        xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
                        xml2:=sp_procesa_respuesta_cola_motor(xml2);
                        return xml2;
                end if;
        else
                xml2:=logapp(xml2,'[procesa_oc_12812] tipo_xml no valido URI='||uri1);
                xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
                xml2:=sp_procesa_respuesta_cola_motor(xml2);
                return xml2;
        end if;

        xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK');
        xml2:=sp_procesa_respuesta_cola_motor(xml2);
        return xml2;
end;
$function$ language plpgsql;
