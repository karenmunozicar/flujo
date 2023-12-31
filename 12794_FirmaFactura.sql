delete from isys_querys_tx where llave='12794';

--Se cambia la base de datos de la 9 a la 8009 para que vaya directo al api web
insert into isys_querys_tx values ('12794',10,8009,1,'select emitir_documento_pivote_v2_12794(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('12794',17,15,1,'select get_folio_gestor_12794(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('12794',20,8009,1,'select emitir_documento_firmado_v2_12794(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('12794',40,1,2,'Servicio de Firma 192.168.3.17',4013,109,106,0,0,50,50);
insert into isys_querys_tx values ('12794',50,9,1,'select emitir_documento_firmado_resp_12794(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);
--Publicamos el DTE
insert into isys_querys_tx values ('12794',65,1,8,'Publica DTE',12704,0,0,0,0,70,70);

--Publicamos inmediatamente
insert into isys_querys_tx values ('12794',67,1,8,'Publica DTE',80103,0,0,0,0,70,70);
--80103=8010 pero va a la Api Web
insert into isys_querys_tx values ('12794',68,1,8,'Publica DTE',8010,0,0,0,0,70,70);

--Validamos la publicacion
insert into isys_querys_tx values ('12794',70,9,1,'select valida_publicacion_dte_12794(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

CREATE or replace FUNCTION response_requests_6000_upper_12794(character varying, character varying, character varying, json)
RETURNS json AS $$
DECLARE
	json2           alias for $4;
	id1		bigint;
	ses1		varchar;
BEGIN
	id1:=get_json('__ID_FORM__',json2)::bigint;
	ses1:=get_json('session_id',json2);
	if $1 in ('1','3') then
		update tmp_json_emitir set estado_emision='OK',cod_respuesta_emision=$1,mensaje_respuesta_emision=$2,respuesta_emision=$3 where sesion=ses1 and id=id1;
	else
		update tmp_json_emitir set estado_emision='ERROR' where sesion=ses1 and id=id1;
	end if;
	return response_requests_6000_upper($1,$2,$3,$4);
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION response_requests_6000_12794(character varying, character varying, character varying, json)
RETURNS json AS $$
DECLARE
	json2           alias for $4;
	id1		bigint;
	ses1		varchar;
BEGIN
	id1:=get_json('__ID_FORM__',json2)::bigint;
	ses1:=get_json('session_id',json2);
	if $1 in ('1','3') then
		update tmp_json_emitir set estado_emision='OK',cod_respuesta_emision=$1,mensaje_respuesta_emision=$2,respuesta_emision=$3 where sesion=ses1 and id=id1;
	else
		update tmp_json_emitir set estado_emision='ERROR' where sesion=ses1 and id=id1;
	end if;
	return response_requests_6000($1,$2,$3,$4);
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION emitir_documento_pivote_v2_12794(json)
RETURNS json AS $$
DECLARE
    json2               alias for $1;
    rut1                varchar;
        stMaestro       maestro_clientes%ROWTYPE;
        stCont          contribuyentes%ROWTYPE;
        stRutFirma      record;
        tipo1           varchar;
        j1              json;
        request1                varchar;
        tipo_dte1               varchar;
        folio1                  varchar;
        stCorrelativo   id_temporal_gestor_folios%ROWTYPE;
        sesion1 varchar;
	rut_usuario1    integer;
	json_par1	json;
	json_out1	json;
	json_in1	json;
	campo	record;
	json3	json;
	rutr1	varchar;
	aux1	varchar;
	aux2	varchar;
	idGestorFolio1	bigint;
	f1	varchar;
BEGIN
        rut1:=get_json('rutCliente',json2);

        json2:=put_json(json2,'RUT_USUARIO=',get_json('rutUsuario',json2));
        json2:=put_json(json2,'__SECUENCIAOK__','0');

        BEGIN
                j1:=get_json('formEmitirdocumento',json2)::json;
        EXCEPTION WHEN OTHERS THEN
                json2:=response_requests_6000_12794('2', 'ERROR en Formulario', '', json2);
                return json2;
        end;
        tipo_dte1:=get_json('tipoDTE',j1);
        sesion1:=get_json('session_id',json2);
	rut_usuario1:=get_json('rutUsuario',json2)::integer;
	json2:=logjson(json2,'TIPO_DOCUMENTO '||get_json('TipoDTE',j1));

	--PROXY TBK
        if get_json('__TBK__', json2) = 'SI' then
		f1:=get_json('FECHA_INGRESO_COLA_ORIGINAL',json2);
		if strpos(get_json('XML_FLAGS',json2),'Fecha de Emision supera periodo tributario')>0 and f1<>'' then
			f1:=to_char(f1::timestamp,'YYYY-MM-DD');
			json2:=logjson(json2,'Reemplazamos la Fecha de Emision por la fecha de ingreso a la cola '||get_json('FchEmis',j1)||' '||f1);
			j1:=put_json(j1,'FchEmis',f1);
			json2:=put_json(json2,'formEmitirdocumento',j1::varchar);
		end if;
		json2:=logjson(json2,'TBK_PROXY FOLIO_EMITIDO='||get_json('FOLIO_EMITIDO',json2)||' __ID_GESTOR_FOLIOS__='||get_json('__ID_GESTOR_FOLIOS__',json2)||' __ID_DTE__='||get_json('__ID_DTE__',json2));
		if is_number(get_json('FOLIO_EMITIDO',json2)) and is_number(get_json('__ID_GESTOR_FOLIOS__',json2)) and is_number(get_json('__ID_DTE__',json2)) then
                        --Insertamos el id temporal gestor
                        aux1:=get_json('__ID_GESTOR_FOLIOS__',json2);
                        if exists(select 1 from id_temporal_gestor_folios where id=aux1::bigint) is False then
				json2:=logjson(json2,'TBK_PROXY Insertamos en id_temporal_gestor_folios xq viene desde las colas y ya entregamos el FOLIO(TED)');
                                insert into id_temporal_gestor_folios(sesion,rut,tipo_dte,id,fecha_ingreso,rut_usuario,folio,estado) values(get_json('session_id',json2),rut1,tipo_dte1,aux1::bigint,now(),rut_usuario1,get_json('FOLIO_EMITIDO',json2)::bigint,3);
                        end if;
                else
			--Hacemos Proxy
			rutr1:=split_part(replace(get_json('RUTRecep',j1),'.',''),'-',1);
			aux1:=right(sesion1,28);
			json2:=logjson(json2,'TBK_PROXY Tipo='||tipo_dte1||' RutEmisor='||rut1||' Receptor='||rutr1||' parametro5='||aux1);
			if sesion1='__PRUEBAS_AUTOMATIZADAS_TBK__' then
				json3:=put_json('{}','TED','TED_DE_PRUEBA');
				json3:=put_json(json3,'FOLIO','999');
				json3:=put_json(json3,'ID_GESTOR_FOLIO','-1');
				json2:=logjson(json2,'TBK_PROXY PRUEBA');
				json2:=response_requests_6000_upper_12794('4', 'Prueba TBK, respondemos en duro',json3::varchar,json2);
				return json2;
			end if;
			BEGIN
				select * into campo from dte_boletas_diarias where rut_emisor=rut1::integer and tipo_dte=tipo_dte1::integer and rut_receptor=rutr1::integer and parametro5=aux1;
				if found then
					json3:=put_json('{}','TED',get_xml('TED_TBK',campo.data_dte));
					json3:=put_json(json3,'FOLIO',campo.folio::varchar);
					json3:=put_json(json3,'ID_GESTOR_FOLIO','-1');
					json2:=logjson(json2,'TBK_PROXY Found');
					json2:=response_requests_6000_upper_12794('4', 'El DTE ya fue emitido correctamente',json3::varchar,json2);
					return json2;
				else
					json2:=logjson(json2,'TBK_PROXY Not Found');
				end if;
			EXCEPTION WHEN OTHERS THEN
				json2:=logjson(json2,'TBK_PROXY Exception');
			END;
		end if;
	end if;

        if(tipo_dte1<>'801') then
	--Si tengo un folio o intento de folio en id_temporal_gestor_folios para la misma sesion en estado=1, significa que fallo el anterior (borrado o emision),  por ende usamos este id para la transaccion
		aux2:=get_json('session_id',json2);
		select * into stCorrelativo from id_temporal_gestor_folios where rut=rut1 and tipo_dte=tipo_dte1 and estado in (1,3) and sesion=aux2 order by id limit 1 for update;
		if found then
			--FAY-DAO si encontramos el folio en estado 1, significa que algo termino mal o aun esta en proceso (reintento del FrameWork)
			json2:=logjson(json2,'REUSO_FOLIO:');
			--Si el folio es no numerico, entonces lo reusamos porque no fue emitido
			if is_number(stCorrelativo.folio) is false or stCorrelativo.folio is null then
				--Le marco la fecha de actualizacion y le paso el correlativo a la misma sesion para no reusar un folio
				update id_temporal_gestor_folios set fecha_actualizacion=now() where id=stCorrelativo.id;
				json2:=logjson(json2,'REUSO_FOLIO: Reuso ID de CAFstCorrelativo.id='||stCorrelativo.id::varchar||' Folio no numerico');
			else
				--Buscamos si el folio esta emitido
				select * into campo from dte_emitidos where rut_emisor=stCorrelativo.rut::integer and tipo_dte=stCorrelativo.tipo_dte::integer and folio=stCorrelativo.folio::bigint;
				if not found then
					--Le marco la fecha de actualizacion y le paso el correlativo a la misma sesion para no reusar un folio
					update id_temporal_gestor_folios set fecha_actualizacion=now() where id=stCorrelativo.id;
					json2:=logjson(json2,'REUSO_FOLIO: Reuso ID de CAFstCorrelativo.id='||stCorrelativo.id::varchar||' Folio no emitido');
				else
					--Verificamos si ya fue enviado al SII
					if campo.estado_sii in ('PROCESADO_POR_EL_SII','ACEPTADO_POR_EL_SII','ENVIADO_AL_SII','ACEPTADO_CON_REPAROS_POR_EL_SII','RECHAZADO_POR_EL_SII') then
						--Si esta aprobado o algo por el SII, debo verificar si es el mismo dte que se esta emitiendo ahora
						update id_temporal_gestor_folios set fecha_actualizacion=now() where id=stCorrelativo.id;
						delete from id_temporal_gestor_folios where id=stCorrelativo.id;
						json2:=logjson(json2,'REUSO_FOLIO: ID de CAFstCorrelativo.id='||stCorrelativo.id::varchar||' ya esta procesado por el SII, se borra');
						if (campo.monto_total=get_json('MntTotal',j1)::bigint and campo.fecha_emision=get_json('FchEmis',j1) and campo.rut_receptor=split_part(get_json('RUTRecep',j1),'-',1)::integer) then
							--Si es el mismo, lo borro
							json3:='{}';
							json2:=logjson(json2,'REUSO_FOLIO: ID de CAFstCorrelativo.id='||stCorrelativo.id::varchar||' es el mismo que se emite, se responde OK a Pantalla');
							--Contestamos que el DTE ya esta emitido correctamente
                                        		if get_json('dispositivo',j1)='movil' then
								json2:=logjson(json2,'REUSO_FOLIO: MOVIL-'||get_json('rutCliente',json2));
                		                                json2:=response_requests_6000_upper_12794('1', '*El DTE fue emitido correctamente, para visualizarlo aca click <a target=''_blank'' href=''http://almacen.acepta.com/ca4webv3/PdfViewMedia?url='||campo.uri||'''>aquí</a>',json3::varchar,json2);
                                		        else
                                               			 json2:=response_requests_6000_upper_12794('1', '*El DTE fue emitido correctamente, para visualizarlo aca click <a target=''_blank'' href='''||campo.uri||'&sesion='||get_json('session_id',json2)||'''>aquí</a>',json3::varchar,json2);
		                                        end if;
							return json2;
						else
							--Si no es igual el DTE que se emite ahora, le entregamos otro folio y borramos este id de la tabla
        						if get_json('__TBK__', json2) = 'SI' then
								insert into id_temporal_gestor_folios(sesion,rut,tipo_dte,id,rut_usuario,fecha_ingreso,estado,fecha_actualizacion) values (get_json('session_id',json2),rut1,tipo_dte1,nextval('correlativo_emitir_dte'),rut_usuario1,now(),3,now()) returning id into stCorrelativo.id;
							else
								insert into id_temporal_gestor_folios(sesion,rut,tipo_dte,id,rut_usuario,fecha_ingreso,estado,fecha_actualizacion) values (get_json('session_id',json2),rut1,tipo_dte1,nextval('correlativo_emitir_dte'),rut_usuario1,now(),1,now()) returning id into stCorrelativo.id;
							end if;
							json2:=logjson(json2,'REUSO_FOLIO: Inserta ID de CAF stCorrelativo.id='||stCorrelativo.id::varchar||' no es el mismo que se emite');
						end if;
					else --campo.estado_sii
						--Le marco la fecha de actualizacion y le paso el correlativo a la misma sesion para no reusar un folio
						update id_temporal_gestor_folios set fecha_actualizacion=now() where id=stCorrelativo.id;
						json2:=logjson(json2,'REUSO_FOLIO: Reuso ID de CAFstCorrelativo.id='||stCorrelativo.id::varchar);
					end if;
				end if;
			end if;
		else
			--Busco si existe un id de gestor, La Emision Anterior Fallo
			select * into stCorrelativo from id_temporal_gestor_folios where rut=rut1 and tipo_dte=tipo_dte1 and estado=0 order by id limit 1 for update;
			if found then
				--Si existe lo tomo para este DTE
				update id_temporal_gestor_folios set sesion=get_json('session_id',json2),estado=1,fecha_actualizacion=now() where id=stCorrelativo.id;
				json2:=logjson(json2,'Encuentra ID de CAF stCorrelativo.id='||stCorrelativo.id::varchar);
			else
				insert into id_temporal_gestor_folios(sesion,rut,tipo_dte,id,rut_usuario,fecha_ingreso,estado,fecha_actualizacion) values (get_json('session_id',json2),rut1,tipo_dte1,nextval('correlativo_emitir_dte'),rut_usuario1,now(),1,now()) returning id into stCorrelativo.id;
				json2:=logjson(json2,'Inserta ID de CAF stCorrelativo.id='||stCorrelativo.id::varchar);
			end if;
		end if;
	else
                stCorrelativo.id:=-1;
	end if;

        json2:=put_json(json2,'TipoDTE',tipo_dte1);
        json2:=put_json(json2,'idGestorFolios',stCorrelativo.id::varchar);
	idGestorFolio1:=stCorrelativo.id;

        -- FGE - 20200529 - Asigna __ID_FORM__ el correlativo para la validacion si es TBK.
        if get_json('__TBK__', json2) = 'SI' then
                json2:=put_json(json2, '__ID_FORM__', stCorrelativo.id::varchar);
        end if;

	--VALIDACIONES_CLIENTE
        select * into stMaestro from maestro_clientes where rut_emisor=rut1::integer;
        if not found then
                json2:=logjson(json2,'Cliente '||rut1::varchar||' no esta en maestro_clientes');
                json2:=response_requests_6000_12794('2', 'Cliente '||rut1::varchar||' no esta en maestro_clientes', '', json2);
                --Libero el Folio
                update id_temporal_gestor_folios set estado=0 where id=idGestorFolio1 and estado=1;
                return json2;
        end if;
        --IOS Pruebas
        if get_json('rutUsuario',json2)='16813717' then
                update id_temporal_gestor_folios set estado=0 where id=idGestorFolio1 and estado=1;
                return response_requests_6000_12794('2', 'Sin permisos para emitir en produccion','',json2);
        end if;
        --DAO 20190712 Para clientes Windte que ocupan solo un certificado
        if stMaestro.rut_emision_dtes is not null and stMaestro.rut_emision_dtes<>'' and get_json('APP',json2)<>'emitir_de' then
                select * into stRutFirma from rut_firma_clave where rut_emisor=rut1::integer and rut_firmante=stMaestro.rut_emision_dtes;
                if found then
                        --DAO 20191122 si viene clave la validamos contra la clave del portal
                        --Solo validamos clave del usuario cuando firma por Movil
                        if get_json('pass',json2)<>'__CLAVE_CENTRALIZADA_ESC__' and get_json('tipo_tx',json2) not in ('crear_factura10k','emitir_factura_masiva_csv','emitir_factura_masiva','crea_documento_desde_url') and get_json('apk',json2)<>'DTE_MOBILE_REPRO' then
                                aux1:=get_json('rutUsuario',json2);
                                if (exists(select 1 from user_10k where rut_usuario=aux1 and get_json(get_json('host_canal',json2),clave_json)=md5(get_json('pass',json2))) is false) then
                                        --Validamos si la clave coincide con la del certificado
                                        if corrige_pass(decode(stRutFirma.clave,'hex')::text)<>get_json('pass',json2) then
                                                json2:=response_requests_6000_12794('2', 'Clave Invalida..','',json2);
                                                --Libero el Folio
                                                update id_temporal_gestor_folios set estado=0 where id=idGestorFolio1 and estado=1;
                                                return json2;
                                        end if;
                                end if;
                        end if;
                        json2:=put_json(json2,'rut_firma',stMaestro.rut_emision_dtes);
                        json2:=put_json(json2,'pass',corrige_pass(decode(stRutFirma.clave,'hex')::text));
                        --FAY 2019-09-12 para evitar soporte si tiene rut de firma centralizado el mensaje de clave incorrecta es diferente.
                        json2:=put_json(json2,'flag_rut_firma_centralizado','SI');
                        json2:=logjson(json2,'Rut con Firma Centralizada Rut='||stMaestro.rut_emision_dtes);
                else
                        json2:=logjson(json2,'Error Rut con Firma Centralizada Rut='||stMaestro.rut_emision_dtes||' no registrado en rut_firma_clave');
                        --DAO 20201008 Si es TBK, debe tener SI o SI Firma Centralizada
                        if get_json('__TBK__', json2) = 'SI' then
                                update id_temporal_gestor_folios set estado=0 where id=idGestorFolio1 and estado=1;
                                return response_requests_6000_12794('2', 'Cliente TBK sin firma centralizada.', '', json2);
                        end if;
                end if;
        else
                --DAO 20201008 Si es TBK, debe tener SI o SI Firma Centralizada
                if get_json('__TBK__', json2) = 'SI' then
                        update id_temporal_gestor_folios set estado=0 where id=idGestorFolio1 and estado=1;
                        return response_requests_6000_12794('2', 'Cliente TBK sin firma centralizada', '', json2);
                end if;
        end if;
        json2:=put_json(json2,'NombreEmisor',substring(stMaestro.razon_social,1,100));
        json2:=put_json(json2,'DominioEmisor',stMaestro.dominio);
        json2:=put_json(json2,'Acteco',stMaestro.acteco_principal);
        json2:=put_json(json2,'GiroEmis',substring(stMaestro.giro,1,80));
        json2:=put_json(json2,'DirOrigen',substring(stMaestro.direccion,1,60));
        json2:=put_json(json2,'CmnaOrigen',substring(stMaestro.comuna_membrete,1,20));
        json2:=put_json(json2,'CiudadOrigen',substring(stMaestro.ciudad,1,20));
        json2:=put_json(json2,'RznSoc',substring(stMaestro.razon_social,1,100));
        json2:=put_json(json2,'GiroEmisor',substring(stMaestro.giro,1,80));
        json2:=put_json(json2,'RznSocEmisor',substring(stMaestro.razon_social,1,90));
        json2:=put_json(json2,'RAZON_SOCIAL_MAESTRO',stMaestro.razon_social);
	json2:=put_json(json2,'siiZona',stMaestro.zona_sii);
        json2:=put_json(json2,'XSL_MAESTRO',stMaestro.xsl_dte);
        json2:=put_json(json2,'URI_IN','http://'||stMaestro.dominio||to_char(now(),'YYMM')||'.acepta.com/v01/'||lpad('_'||replace(getipserver('eth0'),'.','')||'_'||to_char(now(),'DDHH24MISSMI')||'_'||nextval('correlativo_uri')::varchar||'_',40,'0')||'?k='||md5(coalesce(stMaestro.dominio,'X')));
        
	if(get_json('__FLAG_EMISION_TIENDA__',json2)<>'SI') then
                --chequeamos si tiene logo y publicidad en la tabla maestro_cliente
                if length(trim(stMaestro.dte_publicidad_logo))>0 then
                        json2:=put_json(json2,'Logo_Maestro',stMaestro.dte_publicidad_logo);
                end if;
                if length(trim(stMaestro.dte_publicidad_horizontal))>0 then
                        json2:=put_json(json2,'Publicidad_Horizontal_Maestro',stMaestro.dte_publicidad_horizontal);
                end if;
                if length(trim(stMaestro.dte_publicidad_vertical))>0 then
                        json2:=put_json(json2,'Publicidad_Vertical_Maestro',stMaestro.dte_publicidad_vertical);
                end if;
        end if;

        --Leo los datos del contribuyente
        select * into stCont from contribuyentes where rut_emisor=rut1::integer;
        if not found then
                json2:=logjson(json2,'Cliente '||rut1::varchar||' no esta en tabla de contribuyentes');
                json2:=response_requests_6000_12794('2', 'Cliente '||rut1::varchar||' no esta en contribuyentes', '', json2);
                --Libero el Folio
                update id_temporal_gestor_folios set estado=0 where id=idGestorFolio1 and estado=1;
                return json2;
        end if;
        json2:=put_json(json2,'NumRes_SII',stCont.nro_resolucion::varchar);
        json2:=put_json(json2,'FechRes_SII',substring(stCont.fecha_resolucion,9,2)||'-'||substring(stCont.fecha_resolucion,6,2)||'-'||substring(stCont.fecha_resolucion,1,4));
	if tipo_dte1='801' then
        	json2:=put_json(json2,'__SECUENCIAOK__','20');
	else
        	json2:=put_json(json2,'__SECUENCIAOK__','17');
	end if;

        RETURN json2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION emitir_documento_firmado_v2_12794(json)
RETURNS json AS $$
DECLARE
    json1                       alias for $1;
    json2                       json;
    rut1                varchar;
    pass1               varchar;
    pass2               varchar;
    sesid1              varchar;
    select1             varchar;
    menu1               RECORD;
    def1               RECORD;
     status1    varchar;
        tx1     integer;
        cola1   integer;
        nombre_tabla1   varchar;
         categoria1      varchar;
        uri1    varchar;
        emision1        varchar;
        dte1            varchar;
        --stPatron        patron_dte_10k%ROWTYPE;
        html1           varchar;
        file1           varchar;
        sts         integer;
        caf1            varchar;
        rsask1          varchar;
        rsask_p1        varchar;
        ted1            varchar;
        firma_ted1              varchar;
        json3                   json;
        folio1          varchar;
        id_tx1          varchar;
        rut_emisor1     varchar;
        data_firma1     varchar;
        id1             varchar;
        patron_dte1     varchar;
         producto1       varchar;
         fecha1       varchar;
        idGestorFolio1  bigint;
        json_in1        json;
        resp1   varchar;
        json_par1       json;
        json_out1       json;
        resp_db         json;
        razon_rec1      varchar;
        aux     varchar;
        aux1    varchar;
        -- NBV_201705 801
        tipo_dte1       varchar;
        -- NBV 20170803
        guiaExport      varchar;
        OtraMoneda      varchar;

        mail_mandato1   varchar;
        camporec        record;

        -- NBV 20180227
        campo_etiquetas record;
        campo_etiquetas1        record;
        eti1            varchar;
        -- NBV 20180307
        campo_parametros        record;
        valorParametro1         varchar;
        param1                  varchar;

        --EOP 20200116
        dte2            varchar;
        aux2 varchar;
        count integer;
        i integer;
        form_detalle varchar;
        form_documento varchar;
        texto_qbli TEXT;
        texto   varchar;

        --20200124 Impuesto base faena
        v_codigo_ganado varchar;
        v_retenedor_ganado varchar;

        --20201210 Indicador de servicios
        v_tipo_boleta varchar;
        v_bol_serv_desde varchar;
        v_bol_serv_hasta varchar;
        v_json json;

        --20201120 Subtotales informativos
        v_subtotales_informativos varchar;
        v_subtotal_informativo varchar;
        v_texto_subtotales varchar='';
        v_descripcion_impuesto varchar;
        v_dte_save varchar;
	v_rut_receptor varchar;

        v_resp json;
BEGIN
        json2:=replace(replace(json1::varchar,chr(6),';'),chr(5),chr(39))::json;

        rut1:=get_json_upper('rutCliente',json2);
        rut_emisor1:=rut1 || '-' || modulo11(rut1);
        idGestorFolio1:=get_json('idGestorFolios',json2)::bigint;
        json2:=put_json(json2,'ROL_USUARIO_EMISION',get_json('rol_usuario',json2));

        tipo_dte1:=get_json('TipoDTE',json2);
        json2:=logjson(json2,'TIPO_DOCUMENTO '||tipo_dte1);
	--Si no es OC, validamos la respuesta del Gestor
        if(tipo_dte1<>'801') then
		BEGIN
                	resp_db:=decode_hex(get_json('RESPUESTA_GESTOR_FOLIOS',json2))::json;
			resp1:=get_json('WF_RESPUESTA',resp_db);
			resp1:=hex_2_ascii2(encode(resp1::bytea,'hex'));
		EXCEPTION WHEN OTHERS THEN
                        json2:=response_requests_6000_12794('2', 'CAF: Falla obtencion.', '', json2);
                        --Libero el Folio
                        update id_temporal_gestor_folios set estado=0 where id=idGestorFolio1 and estado=1;
                        return json2;
		END;
                if (get_json('GF_CODIGO_RESPUESTA',resp_db)<>'1') then
                        json2:=response_requests_6000_12794('2', 'CAF: '||get_json('GF_MENSAJE_RESPUESTA',resp_db), '', json2);
                        --Libero el Folio
                        update id_temporal_gestor_folios set estado=0 where id=idGestorFolio1 and estado=1;
                        return json2;
                end if;
                if(resp1='') then
                        json2:=response_requests_6000_12794('2', 'CAF: Falla obtencion', '', json2);
                        --Libero el Folio
                        update id_temporal_gestor_folios set estado=0 where id=idGestorFolio1 and estado=1;
                        return json2;
                end if;
		json2:=logjson(json2,'resp1='||resp1);
                caf1:=replace(replace('<CAF version="1.0">' || split_part(split_part(resp1,'<CAF version="1.0">',2),'</CAF>',1) || '</CAF>',chr(10),''),chr(13),'');
                rsask1:=replace(replace(replace(split_part(split_part(resp1,'-----BEGIN RSA PRIVATE KEY-----',2),'-----END RSA PRIVATE KEY-----',1),chr(10),''),chr(12),''),chr(13),'');
                rsask_p1:=replace(replace(replace(split_part(split_part(resp1,'-----BEGIN PUBLIC KEY-----',2),'-----END PUBLIC KEY-----',1),chr(10),''),chr(12),''),chr(13),'');
                folio1:=split_part(split_part(resp1,'<folioInicial>',2),'</folioInicial>',1);
                --Guardo el folio en el id_temporal_gestor_folios
                update id_temporal_gestor_folios set folio=folio1 where id=idGestorFolio1;
                if not found then
                        json2:=logjson(json2,'Falla en actualizacion de id_temporal_gestor_folios con idGestorFolio1='||idGestorFolio1::varchar);
                        json2:=response_requests_6000_12794('2', 'Error en la asignacion de folios, Emita Nuevamente', '', json2);
                        return json2;
                end if;
                json2:=logjson(json2,'------RSASK-----'||rsask1);
                json2:=logjson(json2,'------RSASKPUB-----'||rsask_p1);
                --Transforma el DTE entrante
        else
                -- NBV_201705 801
                folio1:=get_json('Folio',get_json_upper('formEmitirdocumento',json2)::json);
        end if;
        -- NBV_201705 801

        uri1:=get_json('URI_IN',json2);

        --Asigna el Folio
        json2:=put_json(json2,'Folio',folio1);
        json2:=put_json(json2,'RUTEmisor',rut_emisor1);
        json2:=put_json(json2,'TipoDTE',get_json('tipoDTE',get_json_upper('formEmitirdocumento',json2)::json));
        json2:=put_json(json2,'Acteco',get_json('Actecos',get_json_upper('formEmitirdocumento',json2)::json));

        json3:='{}';
        if(get_json('XSL_MAESTRO',json2)<>'') then
                json3:=put_json(json3,'XSL',get_json('XSL_MAESTRO',json2));
                if(get_json('tipoDTE',get_json_upper('formEmitirdocumento',json2)::json)='110' or get_json('tipoDTE',get_json_upper('formEmitirdocumento',json2)::json)='111' or get_json('tipoDTE',get_json_upper('formEmitirdocumento',json2)::json)='112') then
                        json3:=put_json(json3,'XSL',replace(get_json('XSL_MAESTRO',json2), '.xsl', '_exp.xsl'));
                end if;
        else
                if(get_json('tipoDTE',get_json_upper('formEmitirdocumento',json2)::json)='110' or get_json('tipoDTE',get_json_upper('formEmitirdocumento',json2)::json)='111' or get_json('tipoDTE',get_json_upper('formEmitirdocumento',json2)::json)='112') then
                        json3:=get_parametros_motor_json(json3,'XSL_GENERICO_EXP');
                        json3:=put_json(json3,'XSL',get_json('__VALOR_PARAM__',json3));
                elsif(tipo_dte1='801') then
                        json3:=put_json(json3,'XSL','https://escritorio.acepta.com/xsl/xsl_oc_html.xsl');
                else
                        json3:=get_parametros_motor_json(json3,'XSL_GENERICO');
                        json3:=put_json(json3,'XSL',get_json('__VALOR_PARAM__',json3));
                end if;
                --json3:=put_json(json3,'XSL','https://escritorio.acepta.com/xsl/xsl_generico.xsl');
        end if;

        --RME 20200508 XSl para el servicio de tbk
        if (get_json('__TBK__', json2) = 'SI') or (get_json('flag_tbk', json2) = 'SI') then
                --Solo para boletas
                if(get_json('tipoDTE',get_json_upper('formEmitirdocumento',json2)::json)='39') or (get_json('tipoDTE',get_json_upper('formEmitirdocumento',json2)::json)='41') then
                        json3:=put_json(json3,'XSL','https://escritorio.acepta.com/xsl/dte_boleta-transbank.xsl');
                        json2:=logjson(json2,'Se modifica XSL a transbank.xsl');
                end if;
        end if;

        json3:=put_json(json3,'Folio',folio1);
        json3:=put_json(json3,'URI_IN',uri1);
        json3:=put_json(json3,'RUTEmisor',rut_emisor1);
        json3:=put_json(json3,'NombreEmisor',get_json('NombreEmisor',json2));
        json3:=put_json(json3,'DominioEmisor',get_json('DominioEmisor',json2));
        json3:=put_json(json3,'Acteco',get_json('Acteco',json2));
        json3:=put_json(json3,'GiroEmis',get_json('GiroEmis',json2));
        json3:=put_json(json3,'DirOrigen',get_json('DirOrigen',json2));
        json3:=put_json(json3,'CmnaOrigen',get_json('CmnaOrigen',json2));
        json3:=put_json(json3,'CiudadOrigen',get_json('CiudadOrigen',json2));
        json3:=put_json(json3,'TipoDTE',get_json('tipoDTE',get_json_upper('formEmitirdocumento',json2)::json));
        json3:=put_json(json3,'TipoDE',tipo_dte1);
        json3:=put_json(json3,'CredEC',replace(get_json('CredEspContru',get_json('formEmitirdocumento',json2)::json),'.',''));
        json3:=put_json(json3,'Acteco',get_json('Actecos',get_json_upper('formEmitirdocumento',json2)::json));
        json3:=put_json(json3,'RznSoc',get_json('RznSoc',json2));

        -- FGE - 20200513 - Giro y Razon Social Emisor para boleta
        json3:=put_json(json3,'GiroEmisor',get_json('GiroEmisor',json2));
        json3:=put_json(json3,'RznSocEmisor',get_json('RznSocEmisor',json2));

        IF (get_json('tipoDTE',get_json_upper('formEmitirdocumento',json2)::json) in ('39', '41')) THEN
	    --DAO 20201231
	    v_rut_receptor = get_json('RUTRecep',get_json_upper('formEmitirdocumento',json2)::json);
            IF v_rut_receptor = '' OR v_rut_receptor = '0' THEN
            	json3:=put_json(json3, 'RUTRecep', '66666666-6');
            ELSE
            	json3:=put_json(json3,'RUTRecep',TRANSLATE(REPLACE(v_rut_receptor, '.', ''), 'k', 'K'));
            END IF;
            v_json := json_remove_json(get_json_upper('formEmitirdocumento', json2)::TEXT, '{"GiroRecep":""}');
            v_json := json_remove_json(v_json::TEXT, '{"FchVenc":""}');
            json3:=put_json(json3,'RznSocEmisor',substring(get_json('RAZON_SOCIAL_MAESTRO',json2),1,90));
            json3:=put_json(json3, 'FORMEMITIRDOCUMENTO', v_json::varchar);
        ELSE
	    --DAO 20201231
            json3:=put_json(json3,'RUTRecep',replace(get_json('RUTRecep',get_json_upper('formEmitirdocumento',json2)::json),'.',''));
            json3:=put_json(json3,'GiroRecep',substring(get_json('GiroRecep',get_json_upper('formEmitirdocumento',json2)::json),1,40));
            json3:=put_json(json3, 'FORMEMITIRDOCUMENTO', get_json_upper('formEmitirdocumento', json2));
        END IF;

        --Verificamos si hay que enviar mandatos
        mail_mandato1:='';
        if get_json('CorreoRecep',get_json_upper('formEmitirdocumento',json2)::json)<>'' then
                json2:=logjson(json2,'Se incluye Mandato en XML');
                mail_mandato1:=get_json('CorreoRecep',get_json_upper('formEmitirdocumento',json2)::json);
        end if;
        aux:=split_part(get_json('RUTRecep',json3),'-',1);
        select * into camporec from rut_emision_10k where rut_emisor=rut1::integer and rut_cliente=aux::integer and flag_mandato and coalesce(mail_contacto,'')<>'';
        if found then
                if mail_mandato1='' then
                        mail_mandato1:=camporec.mail_contacto;
                else
                        mail_mandato1:=mail_mandato1||','||camporec.mail_contacto;
                end if;
        end if;

        if mail_mandato1<>'' then
                json3:=put_json(json3,'MAIL_MANDATO',mail_mandato1);
                json3:=put_json(json3,'MAIL_EMISOR','noreply@acepta.com');
                json3:=put_json(json3,'SUBJECT_MANDATO','Documento de '||get_json('RAZON_SOCIAL_MAESTRO',json2)||' Folio='||folio1::varchar||' Emision el '||get_json('FchEmis',get_json_upper('formEmitirdocumento',json2)::json));
        end if;

        json3:=put_json(json3,'siiZona',get_json('siiZona',json2));

        if(get_json('__FLAG_EMISION_TIENDA__',json2)<>'SI') then
                --chequeamos si tiene logo y publicidad en la tabla maestro_cliente
                if length(trim(get_json('Logo_Maestro',json2)))>0 then
                        json3:=put_json(json3,'Logo',get_json('Logo_Maestro',json2));
                end if;
                if length(trim(get_json('Publicidad_Horizontal_Maestro',json2)))>0 then
                        json3:=put_json(json3,'Publicidad_Horizontal',get_json('Publicidad_Horizontal_Maestro',json2));
                end if;
                if length(trim(get_json('Publicidad_Vertical_Maestro',json2)))>0 then
                        json3:=put_json(json3,'Publicidad_Vertical',get_json('Publicidad_Vertical_Maestro',json2));
                end if;
        end if;

        --Para ACEPTA no tomo los datos del maestro
        if(get_json('rutCliente',json2)='96919050') then
                json3:=put_json(json3,'Logo','https://escritorio.acepta.com/img_factura/96919050/LOGO_FATURA10k.jpg');
                json3:=put_json(json3,'Publicidad_Horizontal','');
                json3:=put_json(json3,'Publicidad_Vertical','');
        end if;

        json3:=put_json(json3,'RutFirma',get_json_upper('rut_firma',json2)||'-'||modulo11(get_json_upper('rut_firma',json2)));
        json3:=put_json(json3,'NumRes_SII',get_json('NumRes_SII',json2));
        json3:=put_json(json3,'FechRes_SII',get_json('FechRes_SII',json2));

        --Tipo servicio boleta
        IF (get_json('tipoDTE', get_json('formEmitirdocumento',json2)::json) in ('39', '41')) and (get_json('boletaTiposerv', get_json('formEmitirdocumento', json2)::json) = '') THEN
            return response_requests_6000('2', 'Falta  el tipo de servicio de la boleta, es obligatorio.', '', json2);
        ELSE
            v_tipo_boleta := get_json('boletaTiposerv', get_json('formEmitirdocumento', json2)::json);
        END IF;

        IF (get_json('tipoDTE', get_json('formEmitirdocumento',json2)::json) in ('39', '41')) and (v_tipo_boleta in ('3', '4')) THEN
            json3 := put_json(json3, 'IndServicio', v_tipo_boleta);
        END IF;

        IF (get_json('tipoDTE', get_json('formEmitirdocumento',json2)::json) in ('39', '41')) and (v_tipo_boleta in ('1', '2')) THEN
            v_bol_serv_desde := get_json('boletaPeriodoServDesde', get_json('formEmitirdocumento', json2)::json);
            v_bol_serv_hasta := get_json('boletaPeriodoServHasta', get_json('formEmitirdocumento', json2)::json);
            json3 := put_json(json3, 'IndServicio', v_tipo_boleta);
            json3 := put_json(json3, 'PeriodoDesde', v_bol_serv_desde);
            json3 := put_json(json3, 'PeriodoHasta', v_bol_serv_hasta);
        END IF;

        --Para el evento de la traza
        json2:=put_json(json2,'RUT_EMISOR',split_part(rut_emisor1,'-',1));
        json2:=put_json(json2,'RUT_RECEPTOR',split_part(get_json('RUTRecep',json3),'-',1));
        json2:=put_json(json2,'FECHA_EMISION',get_json('FchEmis',get_json_upper('formEmitirdocumento',json2)::json));
        json2:=put_json(json2,'REFERENCIAS_ADJUNTAS',get_json('jsonReferenciasAdjuntas',get_json_upper('formEmitirdocumento',json2)::json));
        json2:=put_json(json2,'FOLIO',get_json('Folio',json2));
        json2:=put_json(json2,'TIPO_DTE',get_json('TipoDTE',json3));

        json2:=bitacora10k(json2,'EMITIR','Get CAF Folio='||get_json('Folio',json3)||' RutEmisor='||get_json('RUTEmisor',json3)||' Tipo='||get_json('TipoDTE',json3));

        id1:='F'||get_json('Folio',json3)||'T'||get_json('TipoDTE',json3);
        if (get_json('TipoDTE',json2)='43') then
               producto1:=coalesce(get_json('NmbItem',json_field_comillas(get_json('jsonDetalleLiqFactura',get_json_upper('formEmitirdocumento',json2)::json)::varchar,'1')::json),'');
        else
               producto1:=coalesce(get_json('NmbItem',json_field_comillas(get_json('jsonDetalle',get_json_upper('formEmitirdocumento',json2)::json)::varchar,'1')::json),'');
        end if;

        ---- kms 2015-11-11 reviso que el producto no tenga caracteres invalidos para el servicio, no caracteres especiales ni acentos.
        --producto1:= regexp_replace(producto1,'[^a-zA-Z0-9" "_-]','','g');

        producto1:=escape_xml_characters_simple(substring(producto1,1,40));
        razon_rec1:=escape_xml_characters_simple(substring(get_json('RznSocRecep',get_json_upper('formEmitirdocumento',json2)::json),1,40));

        fecha1:=to_char(now(),'YYYY-MM-DD')||'T'||to_char(now(),'HH24:MI:SS');

        -- NBV_201705 801
        ted1:='';
        if(tipo_dte1<>'801') then
                ted1:='<DD><RE>' || rut1 || '-' || modulo11(rut1) || '</RE><TD>' || get_json('TipoDTE',json2) || '</TD><F>' || get_json('Folio',json2) || '</F><FE>' || get_json('FchEmis',get_json_upper('formEmitirdocumento',json2)::json) || '</FE><RR>'||get_json('RUTRecep',json3)||'</RR><RSR>'||razon_rec1||'</RSR><MNT>'||get_json('MntTotal',get_json_upper('formEmitirdocumento',json2)::json)||'</MNT><IT1>'||producto1||'</IT1>' || caf1 || '<TSTED>'||fecha1||'</TSTED></DD>';

                begin
                        firma_ted1:=sign_rsa_ted(ted1,rsask1,rsask_p1);
                EXCEPTION WHEN OTHERS THEN
                        perform logfile ('FALLA select sign_rsa_ted('''||ted1||''','''||rsask1||''','''||rsask_p1||'''); SOL='||get_json('id_solicitud',json2));
                        -- Si falla en firmar el ted reutilizaremos el folio
                        update id_temporal_gestor_folios set estado=0 where id=idGestorFolio1 and estado=1;
                        json2:=response_requests_6000_12794('2', 'Falla Firma CAF, Reintente Por Favor','', json2);
                        return json2;
                end;
                ted1:='<TED version="1.0">'||ted1||'<FRMT algoritmo="SHA1withRSA">'||firma_ted1||'</FRMT></TED>'||chr(10)||'<TmstFirma>'||fecha1||'</TmstFirma>';
                -- FGE - 20200409 - Devuelve solamente el TED - MS de TBK
                if get_json('__SOLO_TED__', json2) = 'SI' then
                    update id_temporal_gestor_folios set estado=3 where id=idGestorFolio1 and estado=1;
                    json2:=put_json(json2,'__SECUENCIAOK__','0');
                    ted1:=split_part(ted1,'<TmstFirma>',1);
                    json2:=response_requests_6000_12794('1', 'Devuelve Solo TED', put_json(put_json(put_json('{}', 'TED', str2latin12base64(ted1)), 'FOLIO', get_json('Folio',json2)), 'ID_GESTOR_FOLIO', idGestorFolio1::varchar)::varchar, json2);
                    return json2;
                end if;
                if (get_json('__TBK__', json2) = 'SI') then
                        --Metemos el TED en el json para poder guardarlo en dte_boletas_diarias para el proxy
                        json2:=put_json(json2,'TED_TBK',str2latin12base64(split_part(ted1,'<TmstFirma>',1)));
                end if;
        end if;
        -- NBV_201705 801

        patron_dte1:=pg_read_file('./patron_dte_10k/patron_'||get_json('TipoDTE',json3)||'.xml');
        if (patron_dte1='' or patron_dte1 is null) then
                json2:=response_requests_6000_12794('2', 'Falla Insercion no existe patron de DTE','', json2);
                --Libero el Folio
                update id_temporal_gestor_folios set estado=0 where id=idGestorFolio1 and estado=1;
                return json2;
        end if;

        --json3:=escape_xml_characters(json3::varchar)::json;
        json3:=escape_xml_characters(replace(json3::varchar,chr(5),chr(39)))::json;

        -- NBV 20180227 --> Se buscan etiquetas personalizadas por cliente
        select * from datos_dte_adicionales_rut where rut_emisor=rut1::bigint and tag_final='__PAR_ADICIONAL_EMISION__' into campo_etiquetas;
        if found then
                eti1:='';
                for campo_etiquetas1 in execute 'select * from datos_dte_adicionales_rut where rut_emisor='||rut1::bigint||' and tag_final=''__PAR_ADICIONAL_EMISION__'' order by orden' loop
                        if (get_json(campo_etiquetas1.tag_inicio,get_json_upper('formEmitirdocumento',json2)::json) <> '') then
                                eti1:=eti1||'<DatosAdjuntos>'||chr(10)||'<NombreDA>'||campo_etiquetas1.tag_inicio::varchar||'</NombreDA>'||chr(10)||'<ValorDA>'||campo_etiquetas1.parametro||'</ValorDA>'||chr(10)||'</DatosAdjuntos>'||chr(10)||'<DatosAdjuntos>'||chr(10)||'<NombreDA>'||campo_etiquetas1.parametro||'</NombreDA>'||chr(10)||'<ValorDA>'||get_json(campo_etiquetas1.tag_inicio,get_json_upper('formEmitirdocumento',json2)::json)||'</ValorDA>'||chr(10)||'</DatosAdjuntos>';
                        end if;
                end loop;
                json3:=put_json(json3,'ETIQUETAS_ADICIONALES',eti1);
        end if;

        -- FGE - 20200513 - Campos adicionales en el caso de TBK
        if get_json('__TBK__', json2) = 'SI' then
	    json3:=put_json(json3, 'ETIQUETAS_ADICIONALES', get_json('ETIQUETAS_ADICIONALES', json3) || replace(get_json('XML_Adicionales', get_json_upper('formEmitirdocumento',json2)::json),chr(6),';'));
        end if;


        -- ETIQUETAS_ADICIONALES
        -- NBV 20180307
        select * from filtros_rut where rut_emisor=rut1::bigint and canal='EMITIDOS' and alias_dnd='__PAR_ADICIONAL_EMISION__' into campo_parametros;
        if found then
                param1:='';
                for campo_parametros in execute 'select * from filtros_rut where rut_emisor='||rut1::bigint||' and canal=''EMITIDOS'' and alias_dnd=''__PAR_ADICIONAL_EMISION__'' order by split_part(parametro,''PARAMETRO'',2) ' loop
                        if (get_json(campo_parametros.parametro,get_json_upper('formEmitirdocumento',json2)::json)<>'') then
                                valorParametro1:=get_json(campo_parametros.parametro,get_json_upper('formEmitirdocumento',json2)::json)::varchar;
                                param1:=param1||'<DatosAdjuntos>'||chr(10)||campo_parametros.tag_inicio||valorParametro1||campo_parametros.tag_final||chr(10)||'</DatosAdjuntos>';
                                json2:=logjson(json2,'PARAMETRO_ADICIONAL_EMISOR_CAMPO=>'||param1::varchar);
                        end if;
                end loop;
                json3:=put_json(json3,'PARAMETROS_ADICIONALES',param1);
        end if;
        -- PARAMETROS_ADICIONALES

        if (get_json('TipoDTE',json2) in ('52','110','111','112')) then
                if (get_json('CdgTraslado',get_json_upper('formEmitirdocumento',json2)::json)<>'') then
                        -- si CdgTraslado es <> '' se agrega tags GuiaExport
                        guiaExport:='<GuiaExport><CdgTraslado>'||get_json('CdgTraslado',get_json_upper('formEmitirdocumento',json2)::json)||'</CdgTraslado></GuiaExport>';
                        json3:=put_json(json3,'GuiaExport',guiaExport);
                end if;
                if( get_json('TipoDTE',json2) in ('110','111','112') and get_json('TpoMoneda2',get_json_upper('formEmitirdocumento',json2)::json)<>'') then
                        -- Cuando TpoMoneda es <> '' agrego tags OtraMoneda
                        OtraMoneda:='<OtraMoneda><TpoMoneda>'||get_json('TpoMoneda2',get_json_upper('formEmitirdocumento',json2)::json)||'</TpoMoneda><TpoCambio>'||get_json('TpoCambio',get_json_upper('formEmitirdocumento',json2)::json)||'</TpoCambio><MntExeOtrMnda>'||get_json('MntExeOtrMnda',get_json_upper('formEmitirdocumento',json2)::json)||'</MntExeOtrMnda><MntTotOtrMnda>'||get_json('MntTotOtrMnda',get_json_upper('formEmitirdocumento',json2)::json)||'</MntTotOtrMnda></OtraMoneda>';
                        json3:=put_json(json3,'OtraMoneda',OtraMoneda);
                end if;
        end if;

        --DAO 20191101
        json3:=put_json(json3,'RUT_USUARIO_EMISOR',get_json_upper('rutUsuario',json2)||'-'||modulo11(get_json_upper('rutUsuario',json2)));

        texto='';
        --INICIO EOP NUEVO XML

          form_documento:=get_json('FORMEMITIRDOCUMENTO', json3);
          form_detalle:=get_json('jsonDetalle', form_documento::json);
          i=1;
          aux2:=get_json_index(form_detalle::json, i);
          json2:=logjson(json2, 'AUX2= '||aux2);
          while aux2 <> '' loop
                --raise notice '1.- % %',i,aux2;
                --CONCATENAR
            json2:=logjson(json2, 'JSON DETALLE 2 = '||aux2);

            texto_qbli='';
            count = 0;
            loop
              if count = 0 then
                if(get_json('TpoCodigo', aux2::json)::varchar !='') then
		  --texto_qbli=texto_qbli||'<CdgItem><TpoCodigo>'||get_json('TpoCodigo', aux2::json)::varchar||'</TpoCodigo><VlrCodigo>'||get_json('VlrCodigo', aux2::json)::varchar||'</VlrCodigo></CdgItem>';
		  --DAO 20201231
		  texto_qbli=texto_qbli||'<CdgItem><TpoCodigo>'||LEFT(get_json('TpoCodigo', aux2::json)::varchar, 10)||'</TpoCodigo><VlrCodigo>'||LEFT(get_json('VlrCodigo', aux2::json)::varchar, 35)||'</VlrCodigo></CdgItem>';
                end if;
              else
                if(get_json('TpoCodigo'||count, aux2::json)::varchar !='') then
                  --texto_qbli=texto_qbli||'<CdgItem><TpoCodigo>'||get_json('TpoCodigo'||count, aux2::json)::varchar||'</TpoCodigo><VlrCodigo>'||get_json('VlrCodigo'||count, aux2::json)::varchar||'</VlrCodigo></CdgItem>';
		  --DAO 20201231
		  texto_qbli=texto_qbli||'<CdgItem><TpoCodigo>'||LEFT(get_json('TpoCodigo'||count, aux2::json)::varchar, 10)||'</TpoCodigo><VlrCodigo>'||LEFT(get_json('VlrCodigo'||count, aux2::json)::varchar, 35)||'</VlrCodigo></CdgItem>';
                end if;
              end if;
              count = count + 1;
              exit when count=6;
            end loop;


            v_codigo_ganado:='';
            v_retenedor_ganado:='';
            if get_json('CodImpAdic', aux2::json) = '17011' then
                v_codigo_ganado:='<CdgItem><TpoCodigo>CPCS</TpoCodigo><VlrCodigo>1701</VlrCodigo></CdgItem>';
                v_retenedor_ganado:='<IndAgente>R</IndAgente><MntBaseFaena>'||get_json('MntBaseFaena', aux2::json)::varchar||'</MntBaseFaena><PrcConsFinal>'||get_json('PrcConsFinal', aux2::json)::varchar||'</PrcConsFinal>';
            end if;

            if get_json('CodImpAdic', aux2::json) = '18' and get_json('MntBaseFaena', aux2::json)<>'' and get_json('MntBaseFaena', aux2::json)<>'undefined' then
                --v_codigo_ganado:='<CdgItem><TpoCodigo>INT1</TpoCodigo><VlrCodigo>CE-E</VlrCodigo></CdgItem>';
                v_retenedor_ganado:='<IndAgente>R</IndAgente><MntBaseFaena>'||get_json('MntBaseFaena', aux2::json)::varchar||'</MntBaseFaena><PrcConsFinal>'||get_json('PrcConsFinal', aux2::json)::varchar||'</PrcConsFinal>';
            end if;

            texto=texto||'<Detalle><NroLinDet>'||get_json('NroLinDet', aux2::json)::varchar||'</NroLinDet>'||texto_qbli||'<IndExe>'||get_json('IndExe', aux2::json)::varchar||'</IndExe><Retenedor>'||(case when (get_json('CodImpAdic', aux2::json) = '17011' or get_json('CodImpAdic', aux2::json) = '18') then v_retenedor_ganado else get_json('Retenedor', aux2::json) end)::varchar||'</Retenedor><RUTMandante>'||get_json('RUTMandante', aux2::json)::varchar||'</RUTMandante><NmbItem>'||get_json('NmbItem', aux2::json)::varchar||'</NmbItem><DscItem>'||get_json('DscItem', aux2::json)::varchar||'</DscItem><QtyItem>'||get_json('QtyItem', aux2::json)::varchar||'</QtyItem><UnmdItem>'||LEFT(get_json('UnmdItem', aux2::json)::varchar, 4)||'</UnmdItem><PrcItem>'||get_json('PrcItem', aux2::json)::varchar||'</PrcItem><DescuentoMonto>'||get_json('DescuentoMonto', aux2::json)::varchar||'</DescuentoMonto><CodImpAdic>'||(case when get_json('CodImpAdic', aux2::json)='17011' then '17' else get_json('CodImpAdic', aux2::json) end)::varchar||'</CodImpAdic><MontoItem>'||get_json('MontoItem', aux2::json)::varchar||'</MontoItem></Detalle>';
            i=i+1;
            aux2:=get_json_index(form_detalle::json, i);
          end loop;

          --Subtotales informativos
          if get_json('TipoDTE', json2) in ('39', '41') then
              if (get_json('jsonSubtotalesInformativosXml', form_documento::json) != '') then
                  i = 0;
                  v_subtotales_informativos := get_json_index(get_json('jsonSubtotalesInformativosXml', form_documento::json)::json, i);
                  while v_subtotales_informativos <> '' loop
                      if get_json('glosaSti', v_subtotales_informativos::json) != '' then
                          --perform logfile('EOP V subtotal informativo: '||v_subtotales_informativos);
                          SELECT descripcion INTO v_descripcion_impuesto FROM impuesto_detalle_10k WHERE codigo=get_json('glosaSti', v_subtotales_informativos::json)::integer;
                          v_texto_subtotales := v_texto_subtotales ||chr(10)||chr(9)||'<SubTotInfo>'||chr(10)||chr(9)||chr(9)||'<NroSTI>'||get_json('NroSTI', v_subtotales_informativos::json)||'</NroSTI>'||chr(10)||chr(9)||chr(9)||'<GlosaSTI>'||LEFT(TRIM(INITCAP(SUBSTRING(v_descripcion_impuesto, 5))), 80)||'</GlosaSTI>'||chr(10)||chr(9)||chr(9)||'<SubTotAdicSTI>'||get_json('subTotAdic', v_subtotales_informativos::json)||'</SubTotAdicSTI>'||chr(10)||chr(9)||'</SubTotInfo>';
                      end if;
                      i = i + 1;
                      v_subtotales_informativos := get_json_index(get_json('jsonSubtotalesInformativosXml', form_documento::json)::json, i);
                  end loop;
              end if;
              --perform logfile('EOP V XML Subtotales: '||v_texto_subtotales);
              patron_dte1:=replace(patron_dte1,chr(36)||chr(36)||chr(36)||'SUBTOTALES_INFORMATIVOS'||chr(36)||chr(36)||chr(36),v_texto_subtotales);
          end if;

          json2:=logjson(json2,'MVG IndTraslado='||get_json('IndTraslado',get_json('formEmitirdocumento',json2)::json));
          if get_json('IndTraslado',get_json('formEmitirdocumento',json2)::json) in ('6','5') then
                json2:=logjson(json2,'MVG entro en IndTraslado');
                --texto:=regexp_replace(texto,'<PrcItem>[0-9]*</PrcItem>','','g');
                --DAO-MVG 20200225 para que no genere aceptado con reparos
                texto:=regexp_replace(texto,'<PrcItem>0</PrcItem>','','g');
                json2:=logjson(json2,'MVG texto sin PrcItem= '||texto);
          end if;

        json2:=logjson(json2, 'TEXTO DETALLE = '||texto::varchar);
        patron_dte1:=replace(patron_dte1,chr(36)||chr(36)||chr(36)||'DETALLE_PRODUCTOS'||chr(36)||chr(36)||chr(36),texto);

	json2:=logjson(json2,'JSON3='||json3::varchar);
        dte1:=remplaza_tags_json_c(json3,patron_dte1);
        dte1:=replace(dte1,'#-#DscRcgGlobal#-#','');
        dte1:=limpia_tags(dte1);

        if get_json('TipoDTE', json2) in ('39', '41') then
            v_resp := limpia_tags_referencias(dte1);
            if get_json('status', v_resp) = '200' then
                dte1 := get_json('xml', v_resp);
            end if;
        end if;

        --json2:=logjson(json2,'TED='||ted1);
        dte1:=replace(dte1,'REEMPLAZA_TED_TAG',ted1);

        if get_json('rutUsuario', json2) = '17522200' then
            v_dte_save := ordenar_xml_prueba(dte1);
            if v_dte_save != '' then
                dte1 := v_dte_save;
            end if;
        end if;

        --perform logfile('DAO dte1='||replace(str2latin12base64(dte1),chr(10),''));
        json2:=put_json(json2,'__SECUENCIAOK__','40');
        data_firma1:=replace('{"documento":"'||str2latin12base64(dte1)||'","nodoId":"'||id1||'","rutEmpresa":"'||get_json_upper('rut_firma',json2)||'-'||modulo11(get_json_upper('rut_firma',json2))||'","entidad":"SII","rutFirmante":"'||get_json_upper('rut_firma',json2)||'-'||modulo11(get_json_upper('rut_firma',json2))||'","codigoAcceso":"'||replace(get_json_upper('pass',json2),chr(92),chr(92)||chr(92))||'"}',chr(10),'');

        json2:=get_parametros_motor_json(json2,get_parametro_firmador(get_json_upper('rut_firma',json2)||'-'||modulo11(get_json_upper('rut_firma',json2))));
        json2:=put_json(json2,'INPUT_FIRMADOR','POST '||get_json_upper('PARAMETRO_RUTA',json2)||' HTTP/1.1'||chr(10)||'User-Agent: curl/7.26.0'||chr(10)||'Host: '||get_json_upper('__IP_CONEXION_CLIENTE__',json2)||chr(10)||'Accept: '||chr(42)||'/'||chr(42)||chr(10)||'Content-Type: application/json; charset=ISO-8859-1'||chr(10)||'Content-Length: '||length(data_firma1)::varchar||chr(10)||chr(10)||data_firma1);
        RETURN json2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION valida_publicacion_dte_12794(json) RETURNS json AS $$
declare
        json1   alias for $1;
        json2   json;
        json3   json;
        rut1    varchar;
        stContribuyente contribuyentes%ROWTYPE;
        mail1   varchar;
        xml2    varchar;
        status1 varchar;
        data1   varchar;
	xml3    varchar;
	idGestorFolio1 bigint;
	idForm1 bigint;
	msg1	varchar;
	mensaje_error1	varchar;
	json_ref1	json;
	xml4	varchar;
	i 	integer;
	aux1	varchar;
	-- NBV_201705 801
        json_oc_resp    json;
	id_sol1	bigint;
	tipo_dte1	varchar;
begin
        json2:=json1;
        json2:=put_json(json2,'__SECUENCIAOK__','0');

        /*if (get_json_upper('__PUBLICADO_OK__',json2)<>'SI') then
                  json2:=response_requests_6000_upper_12794('2', 'Falla Publicacion del DTE', '',json2);
                  return json2;
        end if;
        */


	idGestorFolio1:=get_json('idGestorFolios',json2)::bigint;
	idForm1:=get_json('__ID_FORM__',json2)::bigint;
	rut1:=get_json('rutCliente',json2);

	--if (get_json('__FLAG_PUB_10K__',json2)='SI' and get_json('rutCliente',json2)='96919050') then
	json2:=logjson(json2,'__FLAG_PUB_10K__='||get_json('__FLAG_PUB_10K__',json2)||' '||get_json('RESPUESTA',json2));
	if (get_json('__FLAG_PUB_10K__',json2)='SI') then
		status1:=get_json('RESPUESTA',json2);
		mensaje_error1:=get_json('__MENSAJE_10K__',json2);
		if length(mensaje_error1)=0 then
			mensaje_error1:='Falla Emision de DTE..';
		end if;
	else
		-- NBV_201705 801
                if(get_json('TipoDTE',json2)<>'801') then
			xml2:='';
			data1:=get_json_upper('INPUT',json2);
			xml2:=put_campo(xml2,'INPUT',data1);
			xml2:=put_campo(xml2,'CONTENT_LENGTH',(length(data1)/2)::varchar);
			xml2:=put_campo(xml2,'SCRIPT_NAME','/ca4/ca4dte');
			xml2:=put_campo(xml2,'TX','8010');
			xml2:=put_campo(xml2,'DTE_MANUAL','SI');
	
			--Antes de grabar vamos a validar el schema
			xml3:=get_hash_dte(xml2,get_json('TipoDTE',json2));
			if (get_campo('__BASURA_CON_URI__',xml3)='SI') then
				--json2:=response_requests_6000_upper_12794('2', 'Falla Validacion de Esquema en el DTE '||get_json_upper('URI_IN',json2),'',json2);
				json3:='{}';
				json3:=put_json(json3,'id_solicitud',get_json_upper('id_solicitud',json2));
				json2:=response_requests_6000_upper_12794('2', get_campo('COMENTARIO_TRAZA',xml3),json3::varchar,json2);
				json2:=bitacora10k(json2,'EMITIR',get_campo('COMENTARIO_TRAZA',xml3));
				--Libero el Folio
				update id_temporal_gestor_folios set estado=0 where id=idGestorFolio1 and estado=1;
				return json2;
			end if;

			status1:=sp_inserta_data(xml2);
			json2:=logjson(json2,'Status sp_inserta_data='||status1);
			mensaje_error1:='Falla Emision de DTE.';
		end if;
	end if;

        if (strpos(status1,'200 OK')>0) then
		--Si salio todo OK y el documento tiene Referencias adjuntas ... las agregamos
		json2:=graba_tx_apk(json2);
                if(get_json('REFERENCIAS_ADJUNTAS',json2)<>'') then
                        BEGIN
                                json_ref1:=get_json('REFERENCIAS_ADJUNTAS',json2)::json;
                        EXCEPTION WHEN OTHERS THEN
                                json_ref1:=null;
                        END;
                        xml4:='';
                        xml4:=put_campo(xml4,'FECHA_EMISION',get_json('FECHA_EMISION',json2));
                        xml4:=put_campo(xml4,'RUT_EMISOR',get_json('RUT_EMISOR',json2));
                        xml4:=put_campo(xml4,'RUT_OWNER',get_json('RUT_EMISOR',json2));
                        xml4:=put_campo(xml4,'FECHA_EVENTO',now()::varchar);
                        xml4:=put_campo(xml4,'RUT_RECEPTOR',get_json('RUT_RECEPTOR',json2));
                        xml4:=put_campo(xml4,'FOLIO',get_json_upper('FOLIO',json2));
                        xml4:=put_campo(xml4,'TIPO_DTE',get_json('TipoDTE',json2));
                        xml4:=put_campo(xml4,'CANAL','EMITIDOS');
                        xml4:=put_campo(xml4,'URI_IN',get_json_upper('URI_IN',json2));

                        if(json_ref1 is not null) then
                                xml4:='';
                                xml4:=put_campo(xml4,'FECHA_EMISION',get_json('FECHA_EMISION',json2));
                                xml4:=put_campo(xml4,'RUT_EMISOR',get_json('RUT_EMISOR',json2));
                                xml4:=put_campo(xml4,'RUT_OWNER',get_json('RUT_EMISOR',json2));
                                xml4:=put_campo(xml4,'FECHA_EVENTO',now()::varchar);
                                xml4:=put_campo(xml4,'RUT_RECEPTOR',get_json('RUT_RECEPTOR',json2));
                                xml4:=put_campo(xml4,'FOLIO',get_json_upper('FOLIO',json2));
                                xml4:=put_campo(xml4,'TIPO_DTE',get_json('TipoDTE',json2));
                                xml4:=put_campo(xml4,'CANAL','EMITIDOS');
                                xml4:=put_campo(xml4,'URI_IN',get_json_upper('URI_IN',json2));

                                i:=1;
                                aux1:=get_json_index(json_ref1,i);
                                while (aux1<>'') loop
                                        xml4:=put_campo(xml4,'COMENTARIO2',get_json('NombreRef',aux1::json));
                                        xml4:=put_campo(xml4,'COMENTARIO_TRAZA','Tipo Documento:'||get_json('TipoRef',aux1::json));
                                        xml4:=put_campo(xml4,'URL_GET',get_json('URIRef',aux1::json));
                                        xml4 := graba_bitacora(xml4,'ADJ');
                                        i:=i+1;
                                        aux1:=get_json_index(json_ref1,i);
                                end loop;
                        end if;
                end if;

		json3:='{}';
		json3:=put_json(json3,'URL_DOC',get_json_upper('URI_IN',json2));
		json3:=put_json(json3,'FOLIO',get_json_upper('FOLIO',json2));
                json3:=put_json(json3,'id_solicitud',get_json_upper('id_solicitud',json2));
		json3:=put_json(json3,'URL_REDIRECT',(select remplaza_tags_6000(href,json2) from menu_info_10k where id2='emitidos'));
		--if (get_json('__FLAG_PUB_10K__',json2)='SI' and get_json('rutCliente',json2)='96919050') then
		if (get_json('__FLAG_PUB_10K__',json2)='SI') then
			json2:=put_json(json2,'FOLIO_JSON',encode(('{"FOLIO":"'||get_json('Folio',json2)||'","TIPO_DTE":"'||get_json('TipoDTE',json2)||'"}')::bytea,'hex')::varchar);
			json2:=logjson(json2,'rol_usuario='||get_json('rol_usuario',json2));
			if(check_funcionalidad_6000(json2,'NO_REDIRECT_EMITIDOS') or get_json('rol_usuario',json2)='Emitir' or get_json('ROL_USUARIO_EMISION',json2)='Emitir') then
				json2:=logjson(json2,'redirect emitir_v3');
				json3:=put_json(json3,'URL_RESPUESTA',(select remplaza_tags_6000(href,json2) from menu_info_10k where id2='emitir_v3'));
			else	
				json2:=logjson(json2,'redirect buscar');
				json3:=put_json(json3,'URL_RESPUESTA',(select remplaza_tags_6000(href,json2) from menu_info_10k where id2='buscarNEW_emitidos_folio'));
			end if;
			if(get_json('__FLAG_RESPUESTA_NO_HTTP__',json2)='SI') then
                		--Borramos el correlativo temporal
                		delete from id_temporal_gestor_folios where id=idGestorFolio1;
				--DAO_20170621
                                --Si viene ID_SOLICITUD y viene el TAG de FACTURA_ACEPTA Actualizamos el estado
                                if(is_number(get_json_upper('id_solicitud',json2)) and get_json('__FACTURA_ACEPTA__',json2)='SI') then
                                        id_sol1:=get_json_upper('id_solicitud',json2)::bigint;
                                        update facturacion_cliente set uri=get_json_upper('URI_IN',json2),folio=get_json_upper('FOLIO',json2)::bigint, estado='EMITIDO' where id=id_sol1;
				elsif(is_number(get_json_upper('id_solicitud',json2))) then
                                        id_sol1:=get_json_upper('id_solicitud',json2)::bigint;
					update solicitudes_productos set num_factura=get_json_upper('FOLIO',json2),url_factura=get_json_upper('URI_IN',json2) where id_solicitud=id_sol1;
				elsif (is_number(get_json('id_rendicion',json2))) then
                                        id_sol1:=get_json_upper('id_rendicion',json2)::bigint;
					update icar_rendiciones set folio_factura=get_json_upper('FOLIO',json2),url_factura=get_json_upper('URI_IN',json2) where id=id_sol1;
                                end if;
				if get_json('__FLAG_RESPUESTA_PANTALLA__',json2) != 'SI' then
                                        json2:=response_requests_6000_upper_12794('3', 'OK',json3::varchar,json2);
                                        return json2;
                                else
                                        json2:=response_requests_6000_12794('1', 'Exitoso',get_json_upper('URI_IN',json2), json2);
                                        return json2;
                                end if;
				--json2:=response_requests_6000_upper_12794('3', 'OK',json3::varchar,json2);
                                --return json2;
			else
				-- NBV_201705 801
                                if(get_json('TipoDE',json2)='801') then
                                        tipo_dte1:=get_json('TipoDE',json2);
                                else
                                        tipo_dte1:=get_json('TipoDTE',json2);
                                end if;
				-- NBV_201705 801
                                if(tipo_dte1<>'801') then
					--  FIX 20180212 NBV
					BEGIN
                                        if get_json('dispositivo',get_json_upper('formEmitirdocumento',json2)::json)='movil' then
						json2:=logjson(json2,'MOVIL-'||get_json('rutCliente',json2));
                                                json2:=response_requests_6000_upper_12794('3', 'El DTE fue emitido correctamente, para visualizarlo aca click <a target=''_blank'' href=''http://almacen.acepta.com/ca4webv3/PdfViewMedia?url='||get_json_upper('URI_IN',json2)||'&sesion='||get_json('session_id',json2)||'''>aquí</a>',json3::varchar,json2);
                                        else
                                                json2:=response_requests_6000_upper_12794('3', 'El DTE fue emitido correctamente, para visualizarlo aca click <a target=''_blank'' href='''||get_json_upper('URI_IN',json2)||'&sesion='||get_json('session_id',json2)||'''>aquí</a>',json3::varchar,json2);
                                        end if;
					EXCEPTION WHEN OTHERS THEN
                                                json2:=response_requests_6000_upper_12794('3', 'El DTE fue emitido correctamente, para visualizarlo aca click <a target=''_blank'' href='''||get_json_upper('URI_IN',json2)||'&sesion='||get_json('session_id',json2)||'''>aquí</a>',json3::varchar,json2);
					END;
	                		--json2:=response_requests_6000_upper_12794('3', 'El DTE fue emitido correctamente, para visualizarlo aca click <a target="_blank" href="'||get_json_upper('URI_IN',json2)||'">aquí</a>',json3::varchar,json2);
                                else
                                        -- NBV_201705 801
                                        json_oc_resp:=update_estado_documento_no_tributario_6000(json2);
                                        json2:=logjson(json2,'RESPUESTA_UPDATE_801=>'||json_oc_resp::varchar);
                                        json3:=put_json(json3,'URL_RESPUESTA',(select remplaza_tags_6000(href,json2) from menu_info_10k where id2='buscarOC_emitidos_folio'));
                                        json2:=response_requests_6000_upper_12794('3', 'El Documento fue emitido correctamente, para visualizarlo aca click <a target=''_blank'' href='''||get_json_upper('URI_IN',json2)||'''>aquí</a>',json3::varchar,json2);
                                end if;
                                -- NBV_201705 801
			end if;
		else
                	json2:=response_requests_6000_upper_12794('1', 'DTE Firmado OK',json3::varchar,json2);
		end if;

                --Borramos el correlativo temporal
                delete from id_temporal_gestor_folios where id=idGestorFolio1;
		--Elimino el Form del Temporal
		--DAO-FAY 20201002 No se borra mas, porque lo utilizaremos de proxy
		/*if get_json('flagBorrador', json2)='1' or get_json('flagBorrador', json2)='' then
			delete from tmp_json_emitir where id=idForm1;
		end if;*/
		--Descuento Saldo
		if (get_json('tipo_plan_mc',json2)='PLAN10K') then
	                msg1:=update_saldo_10k(rut1::integer,get_json('TIPO_DTE',json2)::integer);
		end if;

                json2:=bitacora10k(json2,'EMITIR','Publicacion OK');
        else
		if(is_number(get_json_upper('id_solicitud',json2)) and get_json('__FACTURA_ACEPTA__',json2)='SI') then
			id_sol1:=get_json_upper('id_solicitud',json2)::bigint;
			update facturacion_cliente set estado=replace(replace(mensaje_error1,chr(39),''),chr(10),' ') where id=id_sol1;
		elsif(is_number(get_json_upper('id_solicitud',json2))) then
			id_sol1:=get_json_upper('id_solicitud',json2)::bigint;
			update solicitudes_productos set mensaje_estado_factura=replace(replace(mensaje_error1,chr(39),''),chr(10),' '),estado_factura='FALLA_EMISION' where id_solicitud=id_sol1;
		end if;
                --Libero el Folio
                update id_temporal_gestor_folios set estado=0 where id=idGestorFolio1  and estado=1;
               	json2:=response_requests_6000_upper_12794('2', mensaje_error1||' Reintente.',get_json_upper('URI_IN',json2),json2);
		json2:=logjson(json2,'MENSAJE_XML_FLAGS='||replace(mensaje_error1,chr(39),''));
		json2:=put_json(json2,'MENSAJE_XML_FLAGS',replace(mensaje_error1,chr(39),''));
                json2:=bitacora10k(json2,'EMITIR',mensaje_error1);
        end if;
        return json2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION emitir_documento_firmado_resp_12794(json) RETURNS json AS $$
DECLARE
    json1                alias for $1;
    json2                json;
    resp1               varchar;
    uri1        varchar;
    data1       varchar;
        aux1    varchar;
        json_resp1      varchar;
        xml2    varchar;
	idGestorFolio1  bigint;
BEGIN
        json2:=json1;

        json2:=put_json(json2,'__SECUENCIAOK__','0');
        json2:=respuesta_no_chunked_json(json2);
        resp1:=decode(get_json_upper('RESPUESTA_HEX',json2),'hex');
        json_resp1:=split_part(resp1,'\012\012',2);
        json2:=put_json(json2,'RESPUESTA_HEX','');
        json2:=put_json(json2,'INPUT_FIRMADOR','');
	idGestorFolio1:=get_json('idGestorFolios',json2)::bigint;

       if (strpos(resp1,'HTTP/1.1 200 ')>0) then
                aux1:=json_get('documentoFirmado',json_resp1);
                if (length(aux1)>0) then
                        --Obtengo el documento para enviarlo al EDTE
                        data1:=base642hex(aux1);
                        --Armo la URI segun el dominio del dte
                        data1:=replace(data1,encode('__REMPLAZA_URI__','hex')::varchar,encode(get_json_upper('URI_IN',json2)::bytea,'hex')::varchar);
                        --Analizamos el DTE con el XSD del servicio
                        --xml2:='';
                        --xml2:=put_campo(xml2,'URI_IN',get_json_upper('URI_IN',json2));
                        --xml2:=put_campo(xml2,'INPUT',data1);
--                      json2:=logjson(json2,'XML2=' || xml2);
                        --xml2:=get_hash_dte(xml2,get_json_upper('TipoDTE',json2));
--                      json2:=logjson(json2,'COMENTARIO_TRAZA='||get_campo('COMENTARIO_TRAZA',xml2) || ' xml2=' || xml2 || ' tipodte=' || get_json_upper('TipoDTE',json2));
                        json2:=put_json(json2,'INPUT',data1);
                        json2:=put_json(json2,'CONTENT_LENGTH',(length(data1)/2)::varchar);
                        --json2:=put_json(json2,'__SECUENCIAOK__','65');
                        insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json_upper('rut_firma',json2),'OK','','DTE',get_json_upper('URI_IN',json2));
                        json2:=bitacora10k(json2,'EMITIR','Firma OK');
			json2:=logjson(json2,'Firma OK');
			json2:=put_json(json2,'__FLAG_PUB_10K__','SI');
			json2:=put_json(json2,'FECHA_INGRESO_COLA',now()::varchar);
			/*
		        --if (get_json('rutCliente',json2)='96919050' and get_json('__FLAG_PUB_10K__',json2)='SI') then
		        if (get_json('__FLAG_PUB_10K__',json2)='SI') then
                                json2:=put_json(json2,'__SECUENCIAOK__','67');
                                json2:=put_json(json2,'RESPUESTA','');
				return json2;
                        end if;
			*/
			--Se procesa por el 8010
                        json2:=put_json(json2,'SCRIPT_NAME','/ca4/ca4dte');
                        json2:=put_json(json2,'__SECUENCIAOK__','67');
			--Si viene por las colas lo mandamos al 8010 normal (No api web)
			if is_number(get_json('__ID_DTE__',json2)) then
				--20210226 limpiamos el formEmitirdocumento que ya cumplio su proposito
				json2:=put_json(json2,'formEmitirdocumento','');
                        	json2:=put_json(json2,'__SECUENCIAOK__','68');
			end if;
                        json2:=put_json(json2,'RESPUESTA','');
			return json2;

                else
			--Libero el Folio excepto que sea de TBK y q este en las colas estado=3
			json2:=logjson(json2,'Falla Firma');
			update id_temporal_gestor_folios set estado=0 where id=idGestorFolio1 and estado=1;
			resp1:=json_get('ERROR',json_resp1);
			   if (length(resp1)=0) then
				resp1:='Servicio Firma Electronica no responde.<br>Reintente más tarde.';
			   end if;
                        json2:=response_requests_6000_upper_12794('2', resp1, '',json2);
			json2:=put_json(json2,'MENSAJE_XML_FLAGS',resp1);
                        insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json_upper('rut_firma',json2),'FALLA',resp1,'DTE',get_json_upper('URI_IN',json2));
                        json2:=bitacora10k(json2,'EMITIR','Firma Falla');
                end if;
       elsif (strpos(resp1,'HTTP/1.1 500 ')>0) then
		   --Libero el Folio excepto que sea de TBK y q este en las colas estado=3
		   json2:=logjson(json2,'Falla Firma error 500 '||resp1);
                   update id_temporal_gestor_folios set estado=0 where id=idGestorFolio1 and estado=1;
                   resp1:=json_get('ERROR',json_resp1);
		   if (length(resp1)=0) then
			resp1:='Servicio de Validación de Firma Electronica no responde.<br>Reintente más tarde.';
		   end if;
		   --FAY-DAO 2019-09-12 si el error de de clave invalida y tiene rut firma centralizado....
		   if (get_json('flag_rut_firma_centralizado',json2)='SI' and strpos(resp1,'Password incorrecto')>0) then
			resp1:='Esta empresa tiene configurado Rut Emision Centralizado y la clave configurada para el rut '||get_json('rut_firma',json2)||'-'||modulo11(get_json('rut_firma',json2))||' esta incorrecta. Revise la <a target=''_blank'' href='''||(select remplaza_tags_6000(href,json2)||'%26abre_acordeon=ac22_rut_sii%26' as href from menu_info_10k where id2='configDinamico')||'''>Configuracion</a>';
			json2:=put_json(json2,'MENSAJE_XML_FLAGS','Esta empresa tiene configurado Rut Emision Centralizado y la clave configurada para el rut '||get_json('rut_firma',json2)||'-'||modulo11(get_json('rut_firma',json2))||' esta incorrecta');
                   	json2:=response_requests_6000_upper_12794('2', resp1, '',json2);
		   else
                   	json2:=response_requests_6000_upper_12794('2', resp1, '',json2);
		   	json2:=put_json(json2,'MENSAJE_XML_FLAGS',resp1||' rut '||get_json('rut_firma',json2)||'-'||modulo11(get_json('rut_firma',json2))||' ');
		   end if;
                   --resp1:='Error: '||split_part(split_part(resp1,'<faultstring>com.acepta.custodiafirma.servicios.ServicioFirmaXMLException:',2),'</faultstring>',1);
                   insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json_upper('rut_firma',json2),'FALLA',resp1,'DTE',get_json_upper('URI_IN',json2));
                   json2:=bitacora10k(json2,'EMITIR','Firma Falla');
        else
	  	   --Libero el Folio
		   json2:=logjson(json2,'Falla Firma error XXX');
                   update id_temporal_gestor_folios set estado=0 where id=idGestorFolio1 and estado=1;
                   json2:=response_requests_6000_upper_12794('2', 'Servicio de Firma no responde', '',json2);
                   insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json_upper('rut_firma',json2),'FALLA','Servicio de Firma no responde','DTE',get_json_upper('URI_IN',json2));
                   json2:=bitacora10k(json2,'EMITIR','Firma Falla');

        end if;
        return json2;

END;
$$ LANGUAGE plpgsql;

