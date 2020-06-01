CREATE or replace FUNCTION emitir_documento_firmado_12794(json)
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
        stMaestro       maestro_clientes%ROWTYPE;
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
        stCont          contribuyentes%ROWTYPE;
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

        -- NBV 20180212
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
        stRutFirma      record;

        --EOP 20200116
        dte2            varchar;
        aux2 varchar;
        count integer;
        i integer;
        form_detalle varchar;
        form_documento varchar;
        texto_qbli TEXT;
        texto   varchar;

        --EOP 20200124
        v_codigo_ganado varchar;
        v_retenedor_ganado varchar;

BEGIN
        json2:=json1;

        json2:=emitir_documento_pivote_12794(json2);

        rut1:=get_json_upper('rutCliente',json2);
        rut_emisor1:=rut1 || '-' || modulo11(rut1);
        idGestorFolio1:=get_json('idGestorFolios',json2)::bigint;

        select * into stMaestro from maestro_clientes where rut_emisor=rut1::integer;
        if not found then
                json2:=logjson(json2,'Cliente '||rut1::varchar||' no esta en maestro_clientes');
                json2:=response_requests_6000('2', 'Cliente '||rut1::varchar||' no esta en maestro_clientes', '', json2);
                --Libero el Folio
                update id_temporal_gestor_folios set estado=0 where id=idGestorFolio1;
                return json2;
        end if;
        --DAO 20190712 Para clientes Windte que ocupan solo un certificado
        if stMaestro.rut_emision_dtes is not null and stMaestro.rut_emision_dtes<>'' then
                select * into stRutFirma from rut_firma_clave where rut_emisor=rut1::integer and rut_firmante=stMaestro.rut_emision_dtes;
                if found then
                        --DAO 20191122 si viene clave la validamos contra la clave del portal
                        if get_json('pass',json2)<>'__CLAVE_CENTRALIZADA_ESC__' then
                                aux1:=get_json('rutUsuario',json2);
                                if (exists(select 1 from user_10k where rut_usuario=aux1 and get_json(get_json('host_canal',json2),clave_json)=md5(get_json('pass',json2))) is false) then
                                        json2:=response_requests_6000('2', 'Clave Invalida','',json2);
                                        --Libero el Folio
                                        update id_temporal_gestor_folios set estado=0 where id=idGestorFolio1;
                                        return json2;
                                end if;
                        end if;
                        json2:=put_json(json2,'rut_firma',stMaestro.rut_emision_dtes);
                        json2:=put_json(json2,'pass',corrige_pass(decode(stRutFirma.clave,'hex')::text));

                end if;
        end if;

        --Leo los datos del contribuyente
        select * into stCont from contribuyentes where rut_emisor=rut1::integer;
        if not found then
                json2:=logjson(json2,'Cliente '||rut1::varchar||' no esta en tabla de contribuyentes');
                json2:=response_requests_6000('2', 'Cliente '||rut1::varchar||' no esta en contribuyentes', '', json2);
                --Libero el Folio
                update id_temporal_gestor_folios set estado=0 where id=idGestorFolio1;
                return json2;
        end if;

        --DAO,FAY 2015-12-02 Chequeo el codigo de respuesta de retorna el servicio que devuelve el CAF. Si es distinto a 1 es un error y devuelvo el mensaje.
        /*if(strpos(get_json('RESPUESTA',json2),'<codigoRespuesta>1</codigoRespuesta>')=0)then
                json2:=response_requests_6000('2','CAF: '|| split_part(split_part(get_json('RESPUESTA',json2),'<documentoRespuesta>',2),'</documentoRespuesta>',1), '', json2);
                --Libero el Folio
                update id_temporal_gestor_folios set estado=0 where id=idGestorFolio1;
                return json2;
        end if;
        */

        -- NBV_201705 801
        if(length(get_json('TipoDE',json2))>0) then
                tipo_dte1:=get_json('TipoDE',json2);
        else
                tipo_dte1:=get_json('TipoDTE',json2);
        end if;

        -- NBV_201705 801
        if(tipo_dte1<>'801') then
                --Vamos a pedir folios al Gestor de Folios
                json_par1:=get_parametros_motor_json('{}','BASE_GESTOR_FOLIOS');
                json_in1:=json2;
                json_in1:=put_json(json_in1,'GF_ID_TRX',idGestorFolio1::varchar);
                json_in1:=put_json(json_in1,'GF_RUT_EMPRESA',rut_emisor1);
                json_in1:=put_json(json_in1,'GF_CODIGO_POS','EMISION'||split_part(rut_emisor1::varchar,'-',1));
                json_in1:=put_json(json_in1,'GF_USUARIO',get_json('rutUsuario',json2));
                json_in1:=put_json(json_in1,'GF_TIPO_DOC',get_json('TipoDTE',json2));
                json_in1:=put_json(json_in1,'GF_FORMATO','CAF');
                json_in1:=put_json(json_in1,'GF_CANAL','ESCRITORIO');
                json2:=logjson(json2,'Solicito CAF json_in1='||json_in1::varchar);
                json_out1:=query_db_json(get_json('__IP_CONEXION_CLIENTE__',json_par1),get_json('__IP_PORT_CLIENTE__',json_par1)::integer,'select web.gf_asigna_subrango_pos('||quote_literal(json_in1)||'::json)');
                resp_db:=get_json('gf_asigna_subrango_pos',json_out1);
                resp1:=get_json('WF_RESPUESTA',resp_db);
                resp1:=hex_2_ascii2(encode(resp1::bytea,'hex'));
                json2:=logjson(json2,'Respuesta CAF '||resp1);
                --perform logfile('DAO resp1='||resp1 ||' json_out1='||resp_db::varchar);
                if (get_json('GF_CODIGO_RESPUESTA',resp_db)<>'1') then
                        json2:=response_requests_6000('2', 'CAF: '||get_json('GF_MENSAJE_RESPUESTA',resp_db), '', json2);
                        --Libero el Folio
                        update id_temporal_gestor_folios set estado=0 where id=idGestorFolio1;
                        return json2;
                end if;
                if(resp1='') then
                        json2:=response_requests_6000('2', 'CAF: Falla obtencion', '', json2);
                        --Libero el Folio
                        update id_temporal_gestor_folios set estado=0 where id=idGestorFolio1;
                        return json2;
                end if;

        --        caf1:=replace('<CAF version="1.0">' || split_part(split_part(get_json('RESPUESTA',json2),'<CAF version="1.0">',2),'</CAF>',1) || '</CAF>',chr(10),'');

                --json2:=logjson(json2,'------CAF-----'||get_json('RESPUESTA',json2));
                caf1:=replace(replace('<CAF version="1.0">' || split_part(split_part(resp1,'<CAF version="1.0">',2),'</CAF>',1) || '</CAF>',chr(10),''),chr(13),'');

                --rsask1:=replace(replace(replace(split_part(split_part(resp1,'-----BEGIN RSA PRIVATE KEY-----'||chr(10),2),chr(10)||'-----END RSA PRIVATE KEY-----',1),chr(10),''),chr(12),''),chr(13),'');
                rsask1:=replace(replace(replace(split_part(split_part(resp1,'-----BEGIN RSA PRIVATE KEY-----',2),'-----END RSA PRIVATE KEY-----',1),chr(10),''),chr(12),''),chr(13),'');

                --rsask_p1:=replace(replace(replace(split_part(split_part(resp1,'-----BEGIN PUBLIC KEY-----'||chr(10),2),chr(10)||'-----END PUBLIC KEY-----',1),chr(10),''),chr(12),''),chr(13),'');
                rsask_p1:=replace(replace(replace(split_part(split_part(resp1,'-----BEGIN PUBLIC KEY-----',2),'-----END PUBLIC KEY-----',1),chr(10),''),chr(12),''),chr(13),'');

                folio1:=split_part(split_part(resp1,'<folioInicial>',2),'</folioInicial>',1);

                --Guardo el folio en el id_temporal_gestor_folios
                update id_temporal_gestor_folios set folio=folio1 where id=idGestorFolio1;
                if not found then
                        json2:=logjson(json2,'Falla en actualizacion de id_temporal_gestor_folios con idGestorFolio1='||idGestorFolio1::varchar);
                        json2:=response_requests_6000('2', 'Error en la asignacion de folios, Emita Nuevamente', '', json2);
                        --Libero el Folio
                        update id_temporal_gestor_folios set estado=0 where id=idGestorFolio1;
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

        uri1:='http://'||stMaestro.dominio||to_char(now(),'YYMM')||'.acepta.com/v01/'||lpad('_'||replace(getipserver('eth0'),'.','')||'_'||to_char(now(),'DDHH24MISSMI')||'_'||nextval('correlativo_uri')::varchar||'_',40,'0')||'?k='||md5(coalesce(stMaestro.dominio,'X'));
        json2:=put_json(json2,'URI_IN',uri1);

        --Asigna el Folio
        json2:=put_json(json2,'Folio',folio1);
        json2:=put_json(json2,'RUTEmisor',rut_emisor1);
        json2:=put_json(json2,'NombreEmisor',substring(stMaestro.razon_social,1,100));
        json2:=put_json(json2,'GiroEmis',substring(stMaestro.giro,1,80));

        -- NBV 20170621
        if(tipo_dte1<>'801') then
                json2:=put_json(json2,'TipoDTE',get_json('tipoDTE',get_json_upper('formEmitirdocumento',json2)::json));
        else
                json2:=put_json(json2,'TipoDE',get_json('tipoDTE',get_json_upper('formEmitirdocumento',json2)::json));
        end if;
        -- NBV 20170621
        json2:=put_json(json2,'Acteco',get_json('Actecos',get_json_upper('formEmitirdocumento',json2)::json));
--      json2:=put_json(json2,'TipoDTE',get_json_upper('tipoDTE',get_json_upper('formEmitirdocumento',json2)));
        json2:=put_json(json2,'RznSoc',substring(stMaestro.razon_social,1,100));

        json3:='{}';
        if(stMaestro.xsl_dte is not null)then
                json3:=put_json(json3,'XSL',stMaestro.xsl_dte);
        else
                if(get_json('tipoDTE',get_json_upper('formEmitirdocumento',json2)::json)='110' or get_json('tipoDTE',get_json_upper('formEmitirdocumento',json2)::json)='111' or get_json('tipoDTE',get_json_upper('formEmitirdocumento',json2)::json)='112') then
                        json3:=get_parametros_motor_json(json3,'XSL_GENERICO_EXP');
                        json3:=put_json(json3,'XSL',get_json('__VALOR_PARAM__',json3));
                elsif(tipo_dte1='801') then
                        json3:=put_json(json3,'XSL','https://escritorio_cert.acepta.com/xsl/xsl_oc_html.xsl');
                        --json3:=put_json(json3,'XSL','https://escritorio_cert.acepta.com/xsl/xsl_generico_oc.xsl');
                else
                        json3:=get_parametros_motor_json(json3,'XSL_GENERICO');
                        json3:=put_json(json3,'XSL',get_json('__VALOR_PARAM__',json3));
                end if;
                --json3:=put_json(json3,'XSL','https://escritorio.acepta.com/xsl/xsl_generico.xsl');
        end if;
        if get_json('rutUsuario',json2)='17597643' and get_json('CorreoRecep',get_json_upper('formEmitirdocumento',json2)::json)<>'' then
                json3:=put_json(json3,'MAIL_RECEPTOR',get_json('CorreoRecep',get_json_upper('formEmitirdocumento',json2)::json));
                json3:=put_json(json3,'ASUNTO_MAIL','Documento de '||stMaestro.razon_social||' Folio='||folio1::varchar||' Emitido el '||to_char(now(),'YYYY-MM-DD'));
        end if;
        json3:=put_json(json3,'Folio',folio1);
        json3:=put_json(json3,'URI_IN',uri1);
        json3:=put_json(json3,'RUTEmisor',rut_emisor1);
        json3:=put_json(json3,'NombreEmisor',substring(stMaestro.razon_social,1,100));
        json3:=put_json(json3,'DominioEmisor',stMaestro.dominio);
        json3:=put_json(json3,'Acteco',stMaestro.acteco_principal);
        json3:=put_json(json3,'GiroEmis',substring(stMaestro.giro,1,80));
        json3:=put_json(json3,'DirOrigen',substring(stMaestro.direccion,1,60));
        json3:=put_json(json3,'CmnaOrigen',substring(stMaestro.comuna_membrete,1,20));
        json3:=put_json(json3,'CiudadOrigen',substring(stMaestro.ciudad,1,20));
        if(tipo_dte1<>'801') then
                json3:=put_json(json3,'TipoDTE',get_json('tipoDTE',get_json_upper('formEmitirdocumento',json2)::json));
        else
                json3:=put_json(json3,'TipoDE',get_json('tipoDTE',get_json_upper('formEmitirdocumento',json2)::json));
        end if;
        json3:=put_json(json3,'CredEC',replace(get_json('CredEspContru',get_json('formEmitirdocumento',json2)::json),'.',''));
        json3:=put_json(json3,'RUTRecep',replace(get_json('RUTRecep',get_json_upper('formEmitirdocumento',json2)::json),'.',''));
        json3:=put_json(json3,'Acteco',get_json('Actecos',get_json_upper('formEmitirdocumento',json2)::json));
        json3:=put_json(json3,'GiroRecep',substring(get_json('GiroRecep',get_json_upper('formEmitirdocumento',json2)::json),1,40));
        json3:=put_json(json3,'RznSoc',substring(stMaestro.razon_social,1,90));
        json3:=put_json(json3,'FORMEMITIRDOCUMENTO',get_json_upper('formEmitirdocumento',json2));
        /*json3:=put_json(json3,'Logo','https://escritorio_qa.acepta.com:8443/Logos/acepta_logo.png');
        json3:=put_json(json3,'Publicidad_Vertical','https://escritorio_qa.acepta.com:8443/Logos/acepta_logo.png');
        json3:=put_json(json3,'Publicidad_Horizontal','https://escritorio_qa.acepta.com:8443/Logos/acepta_logo.png');*/

        -- NBV 20180208 --> Se comenta !!!!
        --Si viene sucursal sacamos la zona del SII del settings
        /*aux:=get_json('CdgSIISucur',get_json_upper('formEmitirdocumento',json2)::json);
        if(aux<>'') then
                select coalesce(parametro9,'') into aux1 from settings_10k where codigo_settings=2 and empresa=rut1 and parametro1=aux;
                json3:=put_json(json3,'siiZona',aux1);
        end if;*/
        -- NBV 20180208 --> Nuevo !!!
        json3:=put_json(json3,'siiZona',stMaestro.zona_sii);
        -- NBV 20180208 --> Nuevo !!!


        -- NBV 20180212 --> Mandato , Tag Contacto
        --json3:=put_json(json3,'Contacto',get_json('Contacto',get_json_upper('formEmitirdocumento',json2)::varchar)::varchar);
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
                        if mail_mandato1 <> camporec.mail_contacto then
                                mail_mandato1:=mail_mandato1||','||camporec.mail_contacto;
                        else
                                mail_mandato1:=camporec.mail_contacto;
                        end if;
                end if;
        end if;

        -- FIX 20180308
        --if mail_mandato1<>'' and camporec.flag_mandato=true then
        if mail_mandato1<>'' then
                --json3:=put_json(json3,'Contacto',mail_mandato1);
                json3:=put_json(json3,'MAIL_MANDATO',mail_mandato1);
                json3:=put_json(json3,'MAIL_EMISOR','noreply@acepta.com');
                json3:=put_json(json3,'SUBJECT_MANDATO','Documento de '||stMaestro.razon_social||' Folio='||folio1::varchar||' Emision el '||get_json('FchEmis',get_json_upper('formEmitirdocumento',json2)::json));
        end if;
        -- NBV 20180212 --> Mandato , Tag Contacto

        if(get_json('__FLAG_EMISION_TIENDA__',json2)<>'SI') then
                --chequeamos si tiene logo y publicidad en la tabla maestro_cliente
                if length(trim(stMaestro.dte_publicidad_logo))>0 then
                        json3:=put_json(json3,'Logo',stMaestro.dte_publicidad_logo);
                end if;
                if length(trim(stMaestro.dte_publicidad_horizontal))>0 then
                        json3:=put_json(json3,'Publicidad_Horizontal',stMaestro.dte_publicidad_horizontal);
                end if;
                if length(trim(stMaestro.dte_publicidad_vertical))>0 then
                        json3:=put_json(json3,'Publicidad_Vertical',stMaestro.dte_publicidad_vertical);
                end if;
        end if;

        --Para ACEPTA no tomo los datos del maestro
        /*if(get_json('rutCliente',json2)='96919050') then
                json3:=put_json(json3,'Logo','https://escritorio.acepta.com/img_factura/96919050/LOGO_FATURA10k.jpg');
                json3:=put_json(json3,'Publicidad_Horizontal','');
                json3:=put_json(json3,'Publicidad_Vertical','');
        end if;*/

        --json3:=put_json(json3,'Logo','https://escritorio.acepta.com/Settings/img_factura/'||split_part(rut_emisor1,'-',1)||'/'||split_part(rut_emisor1,'-',1)||'_logo.jpg');
        --json3:=put_json(json3,'Publicidad_Vertical','https://escritorio.acepta.com/Settings/img_factura/'||split_part(rut_emisor1,'-',1)||'/'||split_part(rut_emisor1,'-',1)||'_publicidad_vertical.jpg');
        --json3:=put_json(json3,'Publicidad_Horizontal','https://escritorio.acepta.com/Settings/img_factura/'||split_part(rut_emisor1,'-',1)||'/'||split_part(rut_emisor1,'-',1)||'_publicidad_horizontal.jpg');

        json3:=put_json(json3,'RutFirma',get_json_upper('rut_firma',json2)||'-'||modulo11(get_json_upper('rut_firma',json2)));

        json3:=put_json(json3,'NumRes_SII',stCont.nro_resolucion::varchar);
        json3:=put_json(json3,'FechRes_SII',substring(stCont.fecha_resolucion,9,2)||'-'||substring(stCont.fecha_resolucion,6,2)||'-'||substring(stCont.fecha_resolucion,1,4));
        --Para el evento de la traza
        json2:=put_json(json2,'RUT_EMISOR',split_part(rut_emisor1,'-',1));
        json2:=put_json(json2,'RUT_RECEPTOR',split_part(get_json('RUTRecep',json3),'-',1));
        json2:=put_json(json2,'FECHA_EMISION',get_json('FchEmis',get_json_upper('formEmitirdocumento',json2)::json));
        json2:=put_json(json2,'REFERENCIAS_ADJUNTAS',get_json('jsonReferenciasAdjuntas',get_json_upper('formEmitirdocumento',json2)::json));
        json2:=put_json(json2,'FOLIO',get_json('Folio',json2));
        if(tipo_dte1<>'801') then
                json2:=put_json(json2,'TIPO_DTE',get_json('TipoDTE',json3));
        else
                json2:=put_json(json2,'TIPO_DTE',get_json('TipoDE',json3));
        end if;

        json2:=bitacora10k(json2,'EMITIR','Get CAF Folio='||get_json('Folio',json3)||' RutEmisor='||get_json('RUTEmisor',json3)||' Tipo='||get_json('TipoDTE',json3));

        if(tipo_dte1<>'801') then
                id1:='F'||get_json('Folio',json3)||'T'||get_json('TipoDTE',json3);
        else
                id1:='F'||get_json('Folio',json3)||'T'||get_json('TipoDE',json3);
        end if;
        if (get_json('TipoDTE',json2)='43') then
               producto1:=coalesce(get_json('NmbItem',json_field_comillas(get_json('jsonDetalleLiqFactura',get_json_upper('formEmitirdocumento',json2)::json)::varchar,'1')::json),'');
        else
               producto1:=coalesce(get_json('NmbItem',json_field_comillas(get_json('jsonDetalle',get_json_upper('formEmitirdocumento',json2)::json)::varchar,'1')::json),'');
        end if;

        ---- kms 2015-11-11 reviso que el producto no tenga caracteres invalidos para el servicio, no caracteres especiales ni acentos.
        --producto1:= regexp_replace(producto1,'[^a-zA-Z0-9" "_-]','','g');

        producto1:=escape_xml_characters_simple(substring(producto1,1,40));
        razon_rec1:=escape_xml_characters_simple(substring(get_json('RznSocRecep',get_json_upper('formEmitirdocumento',json2)::json),1,40));

        fecha1:=to_char(now(),'YYYY-MM-DD')||'T'||to_char(now(),'HH24:MM:SS');

        -- NBV_201705 801
        ted1:='';
        if(tipo_dte1<>'801') then
                --ted1:='<DD><RE>' || rut1 || '-' || modulo11(rut1) || '</RE><TD>' || get_json('TipoDTE',json2) || '</TD><F>' || get_json('Folio',json2) || '</F><FE>' || get_json('FchEmis',get_json_upper('formEmitirdocumento',json2)::json) || '</FE><RR>'||get_json('RUTRecep',json3)||'</RR><RSR>'||escape_xml_characters(substring(get_json('RznSocRecep',get_json_upper('formEmitirdocumento',json2)::json),1,40))||'</RSR><MNT>'||get_json('MntTotal',get_json_upper('formEmitirdocumento',json2)::json)||'</MNT><IT1>'||escape_xml_characters(substring(producto1,1,40))||'</IT1>' || caf1 || '<TSTED>'||fecha1||'</TSTED></DD>';
                ted1:='<DD><RE>' || rut1 || '-' || modulo11(rut1) || '</RE><TD>' || get_json('TipoDTE',json2) || '</TD><F>' || get_json('Folio',json2) || '</F><FE>' || get_json('FchEmis',get_json_upper('formEmitirdocumento',json2)::json) || '</FE><RR>'||get_json('RUTRecep',json3)||'</RR><RSR>'||razon_rec1||'</RSR><MNT>'||get_json('MntTotal',get_json_upper('formEmitirdocumento',json2)::json)||'</MNT><IT1>'||producto1||'</IT1>' || caf1 || '<TSTED>'||fecha1||'</TSTED></DD>';

                begin
                        firma_ted1:=sign_rsa_ted(ted1,rsask1,rsask_p1);
                EXCEPTION WHEN OTHERS THEN
                        perform logfile ('FALLA ted='||ted1||' rsask1='||rsask1||' rsask_p1='||rsask_p1);
                        update id_temporal_gestor_folios set estado=0 where id=idGestorFolio1;
                        json2:=response_requests_6000('2', 'Falla Firma CAF, Reintente Por Favor','', json2);
                        return json2;
                end;
                ted1:='<TED version="1.0">'||ted1||'<FRMT algoritmo="SHA1withRSA">'||firma_ted1||'</FRMT></TED>'||chr(10)||'<TmstFirma>'||fecha1||'</TmstFirma>';
        end if;
        -- NBV_201705 801

        if(tipo_dte1<>'801') then
                patron_dte1:=pg_read_file('./patron_dte_10k/patron_'||get_json('TipoDTE',json3)||'.xml');
        else
                -- NBV 20170621
                patron_dte1:=pg_read_file('./patron_dte_10k/patron_'||get_json('TipoDE',json3)||'.xml');
                --patron_dte1:=pg_read_file('./patron_dte_10k/patron_801.xml');
        end if;
        if (patron_dte1='' or patron_dte1 is null) then
                json2:=response_requests_6000('2', 'Falla Insercion no existe patron de DTE','', json2);
                --Libero el Folio
                update id_temporal_gestor_folios set estado=0 where id=idGestorFolio1;
                return json2;
        end if;

        json3:=escape_xml_characters(json3::varchar)::json;

        -- NBV 20180227 --> Se buscan etiquetas personalizadas por cliente
        select * from datos_dte_adicionales_rut where rut_emisor=rut1::bigint  and tag_final='__PAR_ADICIONAL_EMISION__' into campo_etiquetas;
        if found then
                eti1:='';
                for campo_etiquetas1 in execute 'select * from datos_dte_adicionales_rut where rut_emisor='||rut1::bigint||' and tag_final=''__PAR_ADICIONAL_EMISION__'' order by orden' loop
                        if (get_json(campo_etiquetas1.tag_inicio,get_json_upper('formEmitirdocumento',json2)::json) <> '') then
                                --eti1:=eti1||campo_etiquetas1.tag_inicio||get_json(campo_etiquetas1.parametro,get_json_upper('formEmitirdocumento',json2)::json)||campo_etiquetas1.tag_final;
                                eti1:=eti1||'<DatosAdjuntos>'||chr(10)||'<NombreDA>'||campo_etiquetas1.tag_inicio::varchar||'</NombreDA>'||chr(10)||'<ValorDA>'||campo_etiquetas1.parametro||'</ValorDA>'||chr(10)||'</DatosAdjuntos>'||chr(10)||'<DatosAdjuntos>'||chr(10)||'<NombreDA>'||campo_etiquetas1.parametro||'</NombreDA>'||chr(10)||'<ValorDA>'||get_json(campo_etiquetas1.tag_inicio,get_json_upper('formEmitirdocumento',json2)::json)||'</ValorDA>'||chr(10)||'</DatosAdjuntos>';
                        end if;
                end loop;
                json3:=put_json(json3,'ETIQUETAS_ADICIONALES',eti1);
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

        -- NBV 20170803
        -- Valido Tags de exportacion en guias de despacho
        if (get_json('TipoDTE',json2) in ('52','110','111','112')) then
                if (get_json('CdgTraslado',get_json_upper('formEmitirdocumento',json2)::json)<>'') then
                        -- si CdgTraslado es <> '' se agrega tags GuiaExport
                        guiaExport:='<GuiaExport><CdgTraslado>'||get_json('CdgTraslado',get_json_upper('formEmitirdocumento',json2)::json)||'</CdgTraslado></GuiaExport>';
                        json3:=put_json(json3,'GuiaExport',guiaExport);
                end if;
                if(get_json('TpoMoneda2',get_json_upper('formEmitirdocumento',json2)::json)<>'') then
                        -- Cuando TpoMoneda2 es <> '' agrego tags OtraMoneda
                        OtraMoneda:='<OtraMoneda><TpoMoneda>'||get_json('TpoMoneda2',get_json_upper('formEmitirdocumento',json2)::json)||'</TpoMoneda><TpoCambio>'||get_json('TpoCambio',get_json_upper('formEmitirdocumento',json2)::json)||'</TpoCambio><MntExeOtrMnda>'||get_json('MntExeOtrMnda',get_json_upper('formEmitirdocumento',json2)::json)||'</MntExeOtrMnda><MntTotOtrMnda>'||get_json('MntTotOtrMnda',get_json_upper('formEmitirdocumento',json2)::json)||'</MntTotOtrMnda></OtraMoneda>';
                        patron_dte1:=replace(patron_dte1,chr(36)||chr(36)||chr(36)||'OtraMoneda'||chr(36)||chr(36)||chr(36),OtraMoneda);
                        --json3:=put_json(json3,'OtraMoneda',OtraMoneda);
                end if;
        end if;


        --DAO 20191101
        json3:=put_json(json3,'RUT_USUARIO_EMISOR',get_json_upper('rutUsuario',json2)||'-'||modulo11(get_json_upper('rutUsuario',json2)));
        --json2:=logjson(json2,'JSON_PARA_XML=' ||json3::varchar);

        texto='';
        --INICIO EOP NUEVO XML
        --if get_json('rutUsuario',json2) in ('17597643','17871406','17522200') then
          json2:=logjson(json2,'JSON_PARA_XML=' ||json3::varchar);
          form_documento:=get_json('FORMEMITIRDOCUMENTO', json3);
          form_detalle:=get_json('jsonDetalle', form_documento::json);
          raise notice '%',form_detalle;
          i=1;
          aux2:=get_json_index(form_detalle::json, i);
          json2:=logjson(json2, 'AUX2= '||aux2);
          while aux2 <> '' loop
                raise notice '1.- % %',i,aux2;
                --CONCATENAR
            json2:=logjson(json2, 'JSON DETALLE 2 = '||aux2);

            texto_qbli='';
            count = 0;
            loop
              if count = 0 then
                if(get_json('TpoCodigo', aux2::json)::varchar !='') then
                  texto_qbli=texto_qbli||'<CdgItem><TpoCodigo>'||get_json('TpoCodigo', aux2::json)::varchar||'</TpoCodigo><VlrCodigo>'||get_json('VlrCodigo', aux2::json)::varchar||'</VlrCodigo></CdgItem>';
                end if;
              else
                if(get_json('TpoCodigo'||count, aux2::json)::varchar !='') then
                  texto_qbli=texto_qbli||'<CdgItem><TpoCodigo>'||get_json('TpoCodigo'||count, aux2::json)::varchar||'</TpoCodigo><VlrCodigo>'||get_json('VlrCodigo'||count, aux2::json)::varchar||'</VlrCodigo></CdgItem>';
                end if;
              end if;
              count = count + 1;
              exit when count=6;
            end loop;

            if get_json('CodImpAdic', aux2::json) = '17011' then
                v_codigo_ganado:='<CdgItem><TpoCodigo>CPCS</TpoCodigo><VlrCodigo>1701</VlrCodigo></CdgItem>';
                v_retenedor_ganado:='<IndAgente>R</IndAgente><MntBaseFaena>'||get_json('MntBaseFaena', aux2::json)::varchar||'</MntBaseFaena><PrcConsFinal>'||get_json('PrcConsFinal', aux2::json)::varchar||'</PrcConsFinal>';
            end if;

            if get_json('CodImpAdic', aux2::json) = '18' then
                --v_codigo_ganado:='<CdgItem><TpoCodigo>INT1</TpoCodigo><VlrCodigo>CE-E</VlrCodigo></CdgItem>';
                v_retenedor_ganado:='<IndAgente>R</IndAgente><MntBaseFaena>'||get_json('MntBaseFaena', aux2::json)::varchar||'</MntBaseFaena><PrcConsFinal>'||get_json('PrcConsFinal', aux2::json)::varchar||'</PrcConsFinal>';
            end if;

                raise notice '2.- % %',i,aux2;
            texto=texto||'<Detalle><NroLinDet>'||get_json('NroLinDet', aux2::json)::varchar||'</NroLinDet>'||texto_qbli||'<IndExe>'||get_json('IndExe', aux2::json)::varchar||'</IndExe><Retenedor>'||(case when (get_json('CodImpAdic', aux2::json) = '17011' or get_json('CodImpAdic', aux2::json) = '18') then v_retenedor_ganado else get_json('Retenedor', aux2::json) end)::varchar||'</Retenedor><RUTMandante>'||get_json('RUTMandante', aux2::json)::varchar||'</RUTMandante><NmbItem>'||get_json('NmbItem', aux2::json)::varchar||'</NmbItem><DscItem>'||get_json('DscItem', aux2::json)::varchar||'</DscItem><QtyItem>'||get_json('QtyItem', aux2::json)::varchar||'</QtyItem><UnmdItem>'||get_json('UnmdItem', aux2::json)::varchar||'</UnmdItem><PrcItem>'||get_json('PrcItem', aux2::json)::varchar||'</PrcItem><DescuentoMonto>'||get_json('DescuentoMonto', aux2::json)::varchar||'</DescuentoMonto><CodImpAdic>'||(case when get_json('CodImpAdic', aux2::json)='17011' then '17' else get_json('CodImpAdic', aux2::json) end)::varchar||'</CodImpAdic><MontoItem>'||get_json('MontoItem', aux2::json)::varchar||'</MontoItem></Detalle>';
            --texto=texto||'<Detalle><NroLinDet>'||get_json('NroLinDet', aux2::json)::varchar||'</NroLinDet>'||texto_qbli||'<IndExe>'||get_json('IndExe', aux2::json)::varchar||'</IndExe><Retenedor>'||get_json('Retenedor', aux2::json)::varchar||'</Retenedor><RUTMandante>'||get_json('RUTMandante', aux2::json)::varchar||'</RUTMandante><NmbItem>'||get_json('NmbItem', aux2::json)::varchar||'</NmbItem><DscItem>'||get_json('DscItem', aux2::json)::varchar||'</DscItem><QtyItem>'||get_json('QtyItem', aux2::json)::varchar||'</QtyItem><UnmdItem>'||get_json('UnmdItem', aux2::json)::varchar||'</UnmdItem><PrcItem>'||get_json('PrcItem', aux2::json)::varchar||'</PrcItem><DescuentoMonto>'||get_json('DescuentoMonto', aux2::json)::varchar||'</DescuentoMonto><CodImpAdic>'||get_json('CodImpAdic', aux2::json)::varchar||'</CodImpAdic><MontoItem>'||get_json('MontoItem', aux2::json)::varchar||'</MontoItem></Detalle>';
            i=i+1;
            aux2:=get_json_index(form_detalle::json, i);
          end loop;


          json2:=logjson(json2,'MVG IndTraslado='||get_json('IndTraslado',get_json('formEmitirdocumento',json2)::json));
          if get_json('IndTraslado',get_json('formEmitirdocumento',json2)::json) in ('6','5') then
                json2:=logjson(json2,'MVG entro en IndTraslado');
                texto:=regexp_replace(texto,'<PrcItem>[0-9]*</PrcItem>','','g');
                json2:=logjson(json2,'MVG texto sin PrcItem= '||texto);
          end if;

          raise notice '%',texto;
          json2:=logjson(json2, 'TEXTO DETALLE = '||texto::varchar);
          patron_dte1:=replace(patron_dte1,chr(36)||chr(36)||chr(36)||'DETALLE_PRODUCTOS'||chr(36)||chr(36)||chr(36),texto);
          dte1:=remplaza_tags_json_c(json3, patron_dte1);
          dte1:=replace(dte1,'#-#DscRcgGlobal#-#','');
          dte1:=limpia_tags(dte1);
          dte1:=replace(dte1,'REEMPLAZA_TED_TAG',ted1);
          json2:=logjson(json2, '------DTE2-XML------'||dte1);
          json2:=put_json(json2, '__SECUENCIAOK__', '40');
          data_firma1:=replace('{"documento":"'||str2latin12base64(dte1)||'","nodoId":"'||id1||'","rutEmpresa":"'||get_json_upper('rut_firma',json2)||'-'||modulo11(get_json_upper('rut_firma',json2))||'","entidad":"SII","rutFirmante":"'||get_json_upper('rut_firma',json2)||'-'||modulo11(get_json_upper('rut_firma',json2))||'","codigoAcceso":"'||replace(get_json_upper('pass',json2),chr(92),chr(92)||chr(92))||'"}',chr(10),'');
          json2:=get_parametros_motor_json(json2,get_parametro_firmador(get_json_upper('rut_firma',json2)||'-'||modulo11(get_json_upper('rut_firma',json2))));
          json2:=put_json(json2,'INPUT_FIRMADOR','POST '||get_json_upper('PARAMETRO_RUTA',json2)||' HTTP/1.1'||chr(10)||'User-Agent: curl/7.26.0'||chr(10)||'Host: '||get_json_upper('__IP_CONEXION_CLIENTE__',json2)||chr(10)||'Accept: '||chr(42)||'/'||chr(42)||chr(10)||'Content-Type: application/json; charset=ISO-8859-1'||chr(10)||'Content-Length: '||length(data_firma1)::varchar||chr(10)||chr(10)||data_firma1);
          RETURN json2;
        --end if;
        --FIN EOP NUEVO XML

        /*dte1:=remplaza_tags_json_c(json3,patron_dte1);
        dte1:=replace(dte1,'#-#DscRcgGlobal#-#','');
        dte1:=limpia_tags(dte1);

        dte1:=replace(dte1,'REEMPLAZA_TED_TAG',ted1);

        --perform logfile('DAO dte1='||replace(str2latin12base64(dte1),chr(10),''));
        json2:=put_json(json2,'__SECUENCIAOK__','40');
        --data_firma1:=replace('{"documento":"'||encode(dte1::bytea,'base64')::varchar||'","nodoId":"'||id1||'","rutEmpresa":"'||get_json_upper('rut_firma',json2)||'-'||modulo11(get_json_upper('rut_firma',json2))||'","entidad":"SII","rutFirmante":"'||get_json_upper('rut_firma',json2)||'-'||modulo11(get_json_upper('rut_firma',json2))||'","codigoAcceso":"'||replace(get_json_upper('pass',json2),chr(92),chr(92)||chr(92))||'"}',chr(10),'');
        perform logfile('DTE1='||dte1::varchar);
        data_firma1:=replace('{"documento":"'||str2latin12base64(dte1)||'","nodoId":"'||id1||'","rutEmpresa":"'||get_json_upper('rut_firma',json2)||'-'||modulo11(get_json_upper('rut_firma',json2))||'","entidad":"SII","rutFirmante":"'||get_json_upper('rut_firma',json2)||'-'||modulo11(get_json_upper('rut_firma',json2))||'","codigoAcceso":"'||replace(get_json_upper('pass',json2),chr(92),chr(92)||chr(92))||'"}',chr(10),'');



        --json2:=get_parametros_motor_json(json2,'FIRMADOR');
        json2:=get_parametros_motor_json(json2,get_parametro_firmador(get_json_upper('rut_firma',json2)||'-'||modulo11(get_json_upper('rut_firma',json2))));
        json2:=put_json(json2,'INPUT_FIRMADOR','POST '||get_json_upper('PARAMETRO_RUTA',json2)||' HTTP/1.1'||chr(10)||'User-Agent: curl/7.26.0'||chr(10)||'Host: '||get_json_upper('__IP_CONEXION_CLIENTE__',json2)||chr(10)||'Accept: '||chr(42)||'/'||chr(42)||chr(10)||'Content-Type: application/json; charset=ISO-8859-1'||chr(10)||'Content-Length: '||length(data_firma1)::varchar||chr(10)||chr(10)||data_firma1);
        --json2:=put_json(json2,'__IP_CONEXION_CLIENTE__','192.168.3.17');
        --json2:=put_json(json2,'__IP_PORT_CLIENTE__','80');
        RETURN json2;*/
END;
$$ LANGUAGE plpgsql;

