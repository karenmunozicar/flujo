--Publica documento
delete from isys_querys_tx where llave='13794';

insert into isys_querys_tx values ('13794',30,9,1,'select emitir_documento_firmado_13794(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('13794',40,1,2,'Servicio de Firma 192.168.3.17',4013,109,106,0,0,50,50);

insert into isys_querys_tx values ('13794',50,9,1,'select emitir_documento_firmado_resp_13794(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

--Publicamos el DTE
insert into isys_querys_tx values ('13794',65,1,8,'Publica DTE',12704,0,0,0,0,70,70);

--Publicamos inmediatamente
--insert into isys_querys_tx values ('13794',67,1,8,'Publica DTE',8010,0,0,0,0,70,70);
insert into isys_querys_tx values ('13794',67,1,8,'Publica DTE',80103,0,0,0,0,70,70);

--Validamos la publicacion
insert into isys_querys_tx values ('13794',70,9,1,'select valida_publicacion_dte_13794(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

CREATE or replace FUNCTION emitir_documento_pivote_13794(json)
RETURNS json AS $$
DECLARE
    json2               alias for $1;
    rut1                varchar;
        stMaestro       maestro_clientes%ROWTYPE;
        tipo1           varchar;
        j1              xml;
        request1                varchar;
        tipo_dte1               varchar;
        folio1                  varchar;
        stCorrelativo   id_temporal_gestor_folios%ROWTYPE;
        sesion1 varchar;
        rut_usuario1    integer;
        json_par1       json;
        json_out1       json;
        json_in1        json;
	rutr1   varchar;
        aux1    varchar;
        campo   record;
        json3   json;
BEGIN

        json2:=put_json(json2,'RUT_USUARIO=',get_json('rutUsuario',json2));

        BEGIN
                j1:=lower(get_json('XML_DTE',json2)::xml::varchar);
        EXCEPTION WHEN OTHERS THEN
                json2:=put_json(json2,'__SECUENCIAOK__','0');
                json2:=response_requests_6000('2', 'ERROR en DTE', '', json2);
                return json2;
        end;
        --rut1:=get_json('rutCliente',json2);
        rut1:=split_part(get_xml_hex1('RUTEmisor',j1::varchar),'-',1);
	json2:=put_json(json2,'rutCliente',rut1);
        tipo_dte1:=get_xml_hex1('TipoDTE',j1::varchar);
        rut_usuario1:=get_json('rutUsuario',json2)::integer;
	
	rutr1:=split_part(replace(get_xml_hex1('RUTRecep',j1::varchar),'.',''),'-',1);
        aux1:=get_json('ID_UNICO_TX',json2);

        if aux1<>'' then
                json2:=logjson(json2,'Revisamos Proxy '||rut1||' '||tipo_dte1||' '||rutr1||' '||aux1);
                if tipo_dte1 in ('39','41') then
                        select * into campo from dte_boletas_diarias where rut_emisor=rut1::integer and tipo_dte=tipo_dte1::integer and rut_receptor=rutr1::integer and parametro5=aux1;
                else
                        execute 'select * from dte_emitidos_'||to_char(now(),'YYMM')||' where dia='||to_char(now(),'YYYYMMDD')||' and rut_emisor='||rut1||' and tipo_dte='||tipo_dte1||' and rut_receptor='||rutr1||' and parametro5='''||aux1||''' ' into campo;
                end if;
                if campo.codigo_txel is not null then
                        json2:=logjson(json2,'Responde Proxy');
                        json3:='{}';
                        json3:=put_json(json3,'URL_DOC',campo.uri);
                        json3:=put_json(json3,'FOLIO',campo.folio::varchar);
                        json3:=put_json(json3,'URL_REDIRECT',(select remplaza_tags_6000(href,json2) from menu_info_10k where id2='emitidos'));
                        json3:=put_json(json3,'FOLIO_JSON',encode(('{"FOLIO":"'||campo.folio::varchar||'","TIPO_DTE":"'||campo.tipo_dte::varchar||'"}')::bytea,'hex')::varchar);
                        json2:=response_requests_6000_upper('3', 'OK',json3::varchar,json2);
                        json2:=put_json(json2,'RESPONDE_PROXY','SI');
                        return json2;
                end if;
        end if;

        --Si tengo un folio o intento de folio en id_temporal_gestor_folios para la misma sesion en estado=1, significa que fallo el anterior (borrado o emision),  por ende usamos este id para la transaccion
        --Busco si existe un id de gestor
        select * into stCorrelativo from id_temporal_gestor_folios where rut=rut1 and tipo_dte=tipo_dte1 and estado=0 order by id limit 1 for update;
        if found then
                --Si existe lo tomo para este DTE
                update id_temporal_gestor_folios set sesion=get_json('session_id',json2),estado=1,fecha_actualizacion=now() where id=stCorrelativo.id;
                json2:=logjson(json2,'Encuentra ID de CAF stCorrelativo.id='||stCorrelativo.id::varchar);
        else
                insert into id_temporal_gestor_folios(sesion,rut,tipo_dte,id,rut_usuario,fecha_ingreso,estado,fecha_actualizacion) values (get_json('session_id',json2),rut1,tipo_dte1,nextval('correlativo_emitir_dte'),rut_usuario1,now(),1,now()) returning id into stCorrelativo.id;
                json2:=logjson(json2,'Inserta ID de CAF stCorrelativo.id='||stCorrelativo.id::varchar);
        end if;

        json2:=put_json(json2,'TipoDTE',tipo_dte1);
        json2:=put_json(json2,'idGestorFolios',stCorrelativo.id::varchar);

        RETURN json2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION emitir_documento_firmado_13794(json)
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
        tipo_dte1       varchar;
        xml_dte1        varchar;
        caf_aux1        varchar;
        stCafFirma      record;
	stRutFirma	record;
BEGIN
        json2:=json1;

        json2:=emitir_documento_pivote_13794(json2);

        rut1:=get_json_upper('rutCliente',json2);
        rut_emisor1:=rut1 || '-' || modulo11(rut1);
        idGestorFolio1:=get_json('idGestorFolios',json2)::bigint;
        tipo_dte1:=get_json('TipoDTE',json2);

        xml_dte1:=lower(get_json('XML_DTE',json2));
        --Agregamos Tag DTE en caso de que no venga
        if(strpos(xml_dte1,encode('<DTE','hex')::varchar)=0 and strpos(xml_dte1,encode('</DTE>','hex')::varchar)=0) then
                --perform logfile('1DAO_DTE_13794_1='||coalesce(xml_dte1,'NULO6'));
                xml_dte1:=encode('<DTE xmlns="http://www.sii.cl/SiiDte" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" version="1.0">','hex')::varchar||xml_dte1||encode('</DTE>','hex')::varchar;
                --perform logfile('1DAO_DTE_13794_2='||coalesce(xml_dte1,'NULO5'));
        else
		-- NBV 20180422 Se separa por documentos segun formato XML
                if tipo_dte1 not in ('43','110','111','112') then
                        xml_dte1:=encode('<DTE xmlns="http://www.sii.cl/SiiDte" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" version="1.0"><Documento','hex')||split_part(xml_dte1,encode('<Documento','hex'),2);
                elsif(tipo_dte1='43') then
                        xml_dte1:=encode('<DTE xmlns="http://www.sii.cl/SiiDte" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" version="1.0"><Liquidacion','hex')||split_part(xml_dte1,encode('<Liquidacion','hex'),2);
                else
                        xml_dte1:=encode('<DTE xmlns="http://www.sii.cl/SiiDte" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" version="1.0"><Exportaciones','hex')||split_part(xml_dte1,encode('<Exportaciones','hex'),2);
                end if;
--              xml_dte1:=encode('<DTE xmlns="http://www.sii.cl/SiiDte" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" version="1.0"><Documento','hex')||split_part(xml_dte1,encode('<Documento','hex'),2);
        end if;
        folio1:=get_xml_hex1('Folio',xml_dte1);

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

	if get_json('FLAG_PRE_VISUALIZACION',json2)<>'SI' then
          if(folio1<>'') then
                --Subio folio para firma masiva...Folios no estan en el gestor
                --Chequeo si tiene CAF para este folio
                select * into stCafFirma from caf_firma_xml where empresa=rut1::integer and folio1::integer>=folio_init and folio1::integer<=folio_fin;
                if not found then
                        json2:=response_requests_6000('2', 'Empresa no tiene CAF cargado para el folio='||folio1, '', json2);
                        return json2;
                end if;
                caf_aux1:=hex_2_ascii(encode(stCafFirma.caf,'hex'));
                caf1:=replace('<CAF version="1.0">' || split_part(split_part(caf_aux1,'<CAF version="1.0">',2),'</CAF>',1) || '</CAF>',chr(10),'');
                rsask1:=replace(replace(split_part(split_part(caf_aux1,'-----BEGIN RSA PRIVATE KEY-----'||chr(10),2),chr(10)||'-----END RSA PRIVATE KEY-----',1),chr(10),''),chr(12),'');
                rsask_p1:=replace(replace(split_part(split_part(caf_aux1,'-----BEGIN PUBLIC KEY-----'||chr(10),2),chr(10)||'-----END PUBLIC KEY-----',1),chr(10),''),chr(12),'');

          else
                --Vamos a pedir folios al Gestor de Folios
                json_par1:=get_parametros_motor_json('{}','BASE_GESTOR_FOLIOS');
                json_in1:=json2;
                json_in1:=put_json(json_in1,'GF_ID_TRX',idGestorFolio1::varchar);
                json_in1:=put_json(json_in1,'GF_RUT_EMPRESA',rut_emisor1);
                json_in1:=put_json(json_in1,'GF_CODIGO_POS','EMISION'||split_part(rut_emisor1::varchar,'-',1));
                json_in1:=put_json(json_in1,'GF_USUARIO',get_json('rutUsuario',json2));
                json_in1:=put_json(json_in1,'GF_TIPO_DOC',tipo_dte1);
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
			if(get_json_upper('__EMISION_LOTE__',json2)='SI') then
                        	json2:=response_requests_6000_upper('2', 'CAF: '||get_json('GF_MENSAJE_RESPUESTA',resp_db),get_json_upper('URI_IN',json2)||'_SOLICITUD='||get_json_upper('id_solicitud',json2),json2);
			else
                        	json2:=response_requests_6000('2', 'CAF: '||get_json('GF_MENSAJE_RESPUESTA',resp_db), '', json2);
			end if;
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

                caf1:=replace(replace('<CAF version="1.0">' || split_part(split_part(resp1,'<CAF version="1.0">',2),'</CAF>',1) || '</CAF>',chr(10),''),chr(13),'');

                rsask1:=replace(replace(replace(split_part(split_part(resp1,'-----BEGIN RSA PRIVATE KEY-----'||chr(10),2),chr(10)||'-----END RSA PRIVATE KEY-----',1),chr(10),''),chr(12),''),chr(13),'');

                rsask_p1:=replace(replace(replace(split_part(split_part(resp1,'-----BEGIN PUBLIC KEY-----'||chr(10),2),chr(10)||'-----END PUBLIC KEY-----',1),chr(10),''),chr(12),''),chr(13),'');

                folio1:=split_part(split_part(resp1,'<folioInicial>',2),'</folioInicial>',1);
          end if;
	

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
          uri1:='http://'||stMaestro.dominio||to_char(now(),'YYMM')||'.acepta.com/v01/'||lpad('_'||replace(getipserver('eth0'),'.','')||'_'||to_char(now(),'DDHH24MISSMI')||'_'||nextval('correlativo_uri')::varchar||'_',40,'0')||'?k='||md5(coalesce(stMaestro.dominio,'X'));
          json2:=put_json(json2,'URI_IN',uri1);

	else
          folio1:='POR_ASIGNAR';
        end if; --FLAG_PRE_VISUALIZACION<>'SI'

        json3:='{}';
        json3:=put_json(json3,'DominioEmisor',stMaestro.dominio);
        if(stMaestro.xsl_dte is not null)then
                json3:=put_json(json3,'XSL',stMaestro.xsl_dte);
        else
                if(tipo_dte1 in ('110','111','112')) then
                        json3:=get_parametros_motor_json(json3,'XSL_GENERICO_EXP');
                        json3:=put_json(json3,'XSL',get_json('__VALOR_PARAM__',json3));
                else
                        json3:=get_parametros_motor_json(json3,'XSL_GENERICO');
                        json3:=put_json(json3,'XSL',get_json('__VALOR_PARAM__',json3));
                end if;
        end if;

        --Si viene sucursal sacamos la zona del SII del settings
        aux:=get_xml_hex1('CdgSIISucur',xml_dte1);
        if(aux<>'') then
                select coalesce(parametro9,'') into aux1 from settings_10k where codigo_settings=2 and empresa=rut1 and parametro1=aux;
                json3:=put_json(json3,'siiZona',aux1);
        end if;

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

        --Para Sobre Custodium
        json3:=put_json(json3,'RUTEmisor',rut_emisor1);
        json3:=put_json(json3,'RutFirma',get_json_upper('rut_firma',json2)||'-'||modulo11(get_json_upper('rut_firma',json2)));
        json3:=put_json(json3,'Folio',folio1);
        json3:=put_json(json3,'FchEmis',get_xml_hex1('FchEmis',xml_dte1));
        json3:=put_json(json3,'RUTRecep',get_xml_hex1('RUTRecep',xml_dte1));
        json3:=put_json(json3,'RznSocRecep',get_xml_hex1('RznSocRecep',xml_dte1));
        json3:=put_json(json3,'DirRecep',get_xml_hex1('DirRecep',xml_dte1));
        json3:=put_json(json3,'NombreEmisor',substring(stMaestro.razon_social,1,100));
        json3:=put_json(json3,'SUCURSAL',get_xml_hex1('CdgSIISucur',xml_dte1));
        json3:=put_json(json3,'URI_IN',uri1);
        --

        --Para el evento de la traza
        json2:=put_json(json2,'RUT_EMISOR',split_part(rut_emisor1,'-',1));
        json2:=put_json(json2,'RUT_RECEPTOR',split_part(get_xml_hex1('RUTRecep',xml_dte1),'-',1));
        json2:=put_json(json2,'FECHA_EMISION',get_xml_hex1('FchEmis',xml_dte1));
        json2:=put_json(json2,'FOLIO',folio1);
        json2:=put_json(json2,'TIPO_DTE',tipo_dte1);
        --

        json2:=bitacora10k(json2,'EMITIR','Get CAF Folio='||get_json('Folio',json3)||' RutEmisor='||get_json('RUTEmisor',json3)||' Tipo='||get_json('TipoDTE',json3));

        id1:='F'||folio1||'T'||tipo_dte1;
        if (get_json('TipoDTE',json2)='43') then
                --REVISAR--
                producto1:=decode(split_part(split_part(split_part(xml_dte1,encode('<NroLinDet>1</NroLinDet>','hex'),2),encode('<NmbItem>','hex'),2),encode('</NmbItem>','hex'),1),'hex');
        else
                producto1:=decode(split_part(split_part(split_part(xml_dte1,encode('<NroLinDet>1</NroLinDet>','hex'),2),encode('<NmbItem>','hex'),2),encode('</NmbItem>','hex'),1),'hex');
        end if;

        producto1:=escape_xml_characters_simple(substring(producto1,1,40));
       -- razon_rec1:=escape_xml_characters_simple(substring(get_xml_hex1('RznSocRecep',xml_dte1),1,40));
	razon_rec1:=escape_xml_characters_simple(substring(hex_2_ascii2(get_xml_hex('527a6e536f635265636570',xml_dte1)),1,35));

        fecha1:=to_char(now(),'YYYY-MM-DD')||'T'||to_char(now(),'HH24:MI:SS');

	if get_json('FLAG_PRE_VISUALIZACION',json2)<>'SI' then
          ted1:='<DD><RE>' || rut1 || '-' || modulo11(rut1) || '</RE><TD>' || tipo_dte1 || '</TD><F>' || folio1 || '</F><FE>' || get_xml_hex1('FchEmis',xml_dte1) || '</FE><RR>'||get_xml_hex1('RUTRecep',xml_dte1)||'</RR><RSR>'||razon_rec1||'</RSR><MNT>'||get_xml_hex1('MntTotal',xml_dte1)||'</MNT><IT1>'||producto1||'</IT1>' || caf1 || '<TSTED>'||fecha1||'</TSTED></DD>';

          begin
                firma_ted1:=sign_rsa_ted(ted1,rsask1,rsask_p1);
          EXCEPTION WHEN OTHERS THEN
                perform logfile ('FALLA ted='||ted1||' rsask1='||rsask1||' rsask_p1='||rsask_p1);
                update id_temporal_gestor_folios set estado=0 where id=idGestorFolio1;
                json2:=response_requests_6000('2', 'Falla Firma CAF, Reintente Por Favor','', json2);
                return json2;
          end;
          ted1:='<TED version="1.0">'||ted1||'<FRMT algoritmo="SHA1withRSA">'||firma_ted1||'</FRMT></TED>'||chr(10)||'<TmstFirma>'||fecha1||'</TmstFirma>';
	else
          ted1:='';
        end if; --FLAG_PRE_VISUALIZACION<>'SI'

        patron_dte1:=pg_read_file('./patron_dte_10k/patron_dte_sin_custodium.xml');
        if (patron_dte1='' or patron_dte1 is null) then
                json2:=response_requests_6000('2', 'Falla Insercion no existe patron de DTE','', json2);
                --Libero el Folio
                update id_temporal_gestor_folios set estado=0 where id=idGestorFolio1;
                return json2;
        end if;

        --Agregamos TED y TmstFirma en el DTE
	if tipo_dte1 not in ('43','110','111','112') then
                xml_dte1:=replace(xml_dte1,encode('</Documento>','hex'),encode(ted1::bytea,'hex')||encode('</Documento>','hex'));
        elsif(tipo_dte1='43') then
                xml_dte1:=replace(xml_dte1,encode('</Liquidacion>','hex'),encode(ted1::bytea,'hex')||encode('</Liquidacion>','hex'));
        else
                xml_dte1:=replace(xml_dte1,encode('</Exportaciones>','hex'),encode(ted1::bytea,'hex')||encode('</Exportaciones>','hex'));
        end if;

        --xml_dte1:=replace(xml_dte1,encode('</Documento>','hex'),encode(ted1::bytea,'hex')||encode('</Documento>','hex'));
        --Agregamos FOLIO en el DTE
        if(strpos(xml_dte1,encode('<Folio/>','hex')::varchar)>0) then
                xml_dte1:=replace(xml_dte1,encode('<Folio/>','hex')::varchar,encode('<Folio>','hex')||encode(folio1::varchar::bytea,'hex')||encode('</Folio>','hex'));
        else
                xml_dte1:=split_part(xml_dte1,encode('<Folio>','hex'),1)||encode('<Folio>','hex')||encode(folio1::varchar::bytea,'hex')||encode('</Folio>','hex')||split_part(xml_dte1,encode('</Folio>','hex'),2);
        end if;
        --Agregamos ID en el DTE
 --       xml_dte1:=split_part(xml_dte1,encode('<Documento','hex'),1)||encode(('<Documento ID="'||id1||'"><Encabezado>')::bytea,'hex')||split_part(xml_dte1,encode('<Encabezado>','hex'),2);
	if tipo_dte1 not in ('43','110','111','112') then
                xml_dte1:=split_part(xml_dte1,encode('<Documento','hex'),1)||encode(('<Documento ID="'||id1||'"><Encabezado>')::bytea,'hex')||split_part(xml_dte1,encode('<Encabezado>','hex'),2);
        elsif(tipo_dte1='43') then
                xml_dte1:=split_part(xml_dte1,encode('<Liquidacion','hex'),1)||encode(('<Liquidacion ID="'||id1||'"><Encabezado>')::bytea,'hex')||split_part(xml_dte1,encode('<Encabezado>','hex'),2);
        else
                xml_dte1:=split_part(xml_dte1,encode('<Exportaciones','hex'),1)||encode(('<Exportaciones ID="'||id1||'"><Encabezado>')::bytea,'hex')||split_part(xml_dte1,encode('<Encabezado>','hex'),2);
        end if;

        --perform logfile('DAO_DTE_13794_3='||coalesce(xml_dte1,'NULO3'));

        json3:=escape_xml_characters(json3::varchar)::json;
        dte1:=remplaza_tags_json_c(json3,patron_dte1);
        dte1:=limpia_tags(dte1);
        --perform logfile('DAO_DTE_13794_4='||coalesce(dte1,'NULO2'));

        xml_dte1:=replace(encode(dte1::bytea,'hex'),encode('#|#|#|#DTE#|#|#|#','hex'),xml_dte1);
        --perform logfile('DAO_DTE_13794_5='||coalesce(xml_dte1,'NULO1'));

        --perform logfile('DAO_DTE_13794_6='||id1||'   '||get_json_upper('rut_firma',json2)||'   '||coalesce(hex2ascii2base64(xml_dte1),'NULO'));
	if get_json('FLAG_PRE_VISUALIZACION',json2)='SI' then
                html1:=xml_2_html2(xml_dte1,get_json('XSL',json3));
                json2:=response_requests_6000('1', 'OK',put_json('{}','HTML',str2latin12hex(html1))::varchar, json2);
                return json2;
        end if;

        json2:=put_json(json2,'__SECUENCIAOK__','40');
        --data_firma1:=replace('{"documento":"'||hex2ascii2base64(xml_dte1)||'","nodoId":"'||id1||'","rutEmpresa":"'||get_json_upper('rut_firma',json2)||'-'||modulo11(get_json_upper('rut_firma',json2))||'","entidad":"SII","rutFirmante":"'||get_json_upper('rut_firma',json2)||'-'||modulo11(get_json_upper('rut_firma',json2))||'","codigoAcceso":"'||replace(get_json_upper('pass',json2),chr(92),chr(92)||chr(92))||'"}',chr(10),'');
        --data_firma1:=replace('{"documento":"'||str2latin12base64(decode_hex(xml_dte1))||'","nodoId":"'||id1||'","rutEmpresa":"'||get_json_upper('rut_firma',json2)||'-'||modulo11(get_json_upper('rut_firma',json2))||'","entidad":"SII","rutFirmante":"'||get_json_upper('rut_firma',json2)||'-'||modulo11(get_json_upper('rut_firma',json2))||'","codigoAcceso":"'||replace(get_json_upper('pass',json2),chr(92),chr(92)||chr(92))||'"}',chr(10),'');
        --data_firma1:=replace('{"documento":"'||str2latin12base64(decode_hex_latin1(xml_dte1))||'","nodoId":"'||id1||'","rutEmpresa":"'||get_json_upper('rut_firma',json2)||'-'||modulo11(get_json_upper('rut_firma',json2))||'","entidad":"SII","rutFirmante":"'||get_json_upper('rut_firma',json2)||'-'||modulo11(get_json_upper('rut_firma',json2))||'","codigoAcceso":"'||replace(get_json_upper('pass',json2),chr(92),chr(92)||chr(92))||'"}',chr(10),'');
        data_firma1:=replace('{"documento":"'||hex2latin12base64(xml_dte1)||'","nodoId":"'||id1||'","rutEmpresa":"'||get_json_upper('rut_firma',json2)||'-'||modulo11(get_json_upper('rut_firma',json2))||'","entidad":"SII","rutFirmante":"'||get_json_upper('rut_firma',json2)||'-'||modulo11(get_json_upper('rut_firma',json2))||'","codigoAcceso":"'||replace(get_json_upper('pass',json2),chr(92),chr(92)||chr(92))||'"}',chr(10),'');
        --perform logfile('DAO_DTE_13794_7 '||coalesce(data_firma1::varchar,'NULO7'));

        --json2:=get_parametros_motor_json(json2,'FIRMADOR');
	json2:=get_parametros_motor_json(json2,get_parametro_firmador(get_json_upper('rut_firma',json2)||'-'||modulo11(get_json_upper('rut_firma',json2))));
        json2:=put_json(json2,'INPUT_FIRMADOR','POST '||get_json_upper('PARAMETRO_RUTA',json2)||' HTTP/1.1'||chr(10)||'User-Agent: curl/7.26.0'||chr(10)||'Host: '||get_json_upper('__IP_CONEXION_CLIENTE__',json2)||chr(10)||'Accept: '||chr(42)||'/'||chr(42)||chr(10)||'Content-Type: application/json; charset=ISO-8859-1'||chr(10)||'Content-Length: '||length(data_firma1)::varchar||chr(10)||chr(10)||data_firma1);
        json2:=put_json(json2,'XML_DTE','');
        json2:=put_json(json2,'REQUEST_URI','');
        json2:=put_json(json2,'QUERY_STRING','');
        RETURN json2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION valida_publicacion_dte_13794(json) RETURNS json AS $$
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
        msg1    varchar;
        mensaje_error1  varchar;
begin
        json2:=json1;
        json2:=put_json(json2,'__SECUENCIAOK__','0');

        /*if (get_json_upper('__PUBLICADO_OK__',json2)<>'SI') then
                  json2:=response_requests_6000_upper('2', 'Falla Publicacion del DTE', '',json2);
                  return json2;
        end if;
        */


        idGestorFolio1:=get_json('idGestorFolios',json2)::bigint;
        rut1:=get_json('rutCliente',json2);

        --if (get_json('__FLAG_PUB_10K__',json2)='SI' and get_json('rutCliente',json2)='96919050') then
        if (get_json('__FLAG_PUB_10K__',json2)='SI') then
                status1:=get_json('RESPUESTA',json2);
                mensaje_error1:=replace(get_json('__MENSAJE_10K__',json2),'''','');
                if length(mensaje_error1)=0 then
                        mensaje_error1:='Falla Emision de DTE.';
                end if;
        else
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
                        --json2:=response_requests_6000_upper('2', 'Falla Validacion de Esquema en el DTE '||get_json_upper('URI_IN',json2),'',json2);
                        json2:=response_requests_6000_upper('2', get_campo('COMENTARIO_TRAZA',xml3),'',json2);
                        json2:=bitacora10k(json2,'EMITIR',get_campo('COMENTARIO_TRAZA',xml3));
                        --Libero el Folio
                        update id_temporal_gestor_folios set estado=0 where id=idGestorFolio1;
                        return json2;
                end if;

                status1:=sp_inserta_data(xml2);
                json2:=logjson(json2,'Status sp_inserta_data='||status1);
                mensaje_error1:='Falla Emision de DTE.';
        end if;

        if (strpos(status1,'200 OK')>0) then
                json3:='{}';
                json3:=put_json(json3,'URL_DOC',get_json_upper('URI_IN',json2));
                json3:=put_json(json3,'FOLIO',get_json_upper('FOLIO',json2));
                json3:=put_json(json3,'id_solicitud',get_json_upper('id_solicitud',json2));
                json3:=put_json(json3,'URL_REDIRECT',(select remplaza_tags_6000(href,json2) from menu_info_10k where id2='emitidos'));
                --if (get_json('__FLAG_PUB_10K__',json2)='SI' and get_json('rutCliente',json2)='96919050') then
                if (get_json('__FLAG_PUB_10K__',json2)='SI') then
                        json2:=put_json(json2,'FOLIO_JSON',encode(('{"FOLIO":"'||get_json('Folio',json2)||'","TIPO_DTE":"'||get_json('TipoDTE',json2)||'"}')::bytea,'hex')::varchar);
                        json3:=put_json(json3,'URL_RESPUESTA',(select remplaza_tags_6000(href,json2) from menu_info_10k where id2='buscarNEW_emitidos_folio'));
                        if(get_json('__FLAG_RESPUESTA_NO_HTTP__',json2)='SI') then
                                --Borramos el correlativo temporal
                                delete from id_temporal_gestor_folios where id=idGestorFolio1;
                                json2:=response_requests_6000_upper('3', 'OK',json3::varchar,json2);
                                return json2;
                        else
                                json2:=response_requests_6000_upper('3', 'El DTE fue emitido correctamente, para visualizarlo aca click <a target="_blank" href="'||get_json_upper('URI_IN',json2)||'">aquí</a>',json3::varchar,json2);
                        end if;
                else
                        json2:=response_requests_6000_upper('1', 'DTE Firmado OK',json3::varchar,json2);
                end if;

                --Borramos el correlativo temporal
                delete from id_temporal_gestor_folios where id=idGestorFolio1;
                --Elimino el Form del Temporal
                --Descuento Saldo
                --if (get_json('tipo_plan_mc',json2)='PLAN10K') then
                        msg1:=update_saldo_10k(rut1::integer,get_json('TIPO_DTE',json2)::integer);
                --end if;

                json2:=bitacora10k(json2,'EMITIR','Publicacion OK');
        else
                --Libero el Folio
                update id_temporal_gestor_folios set estado=0 where id=idGestorFolio1;
		if(get_json_upper('__EMISION_LOTE__',json2)='SI') then
                        json2:=response_requests_6000_upper('2', mensaje_error1||' Reintente.',get_json_upper('URI_IN',json2)||'_SOLICITUD='||get_json_upper('id_solicitud',json2),json2);
                else
                        json2:=response_requests_6000_upper('2', mensaje_error1||' Reintente.',get_json_upper('URI_IN',json2),json2);
                end if;
                --json2:=response_requests_6000_upper('2', mensaje_error1||' Reintente.',get_json_upper('URI_IN',json2),json2);
                json2:=bitacora10k(json2,'EMITIR',mensaje_error1);
        end if;
        return json2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION emitir_documento_firmado_resp_13794(json) RETURNS json AS $$
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
        json2:=put_json(json2,'XML_DTE','');
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
                        /*
                        --if (get_json('rutCliente',json2)='96919050' and get_json('__FLAG_PUB_10K__',json2)='SI') then
                        if (get_json('__FLAG_PUB_10K__',json2)='SI') then
                                json2:=put_json(json2,'__SECUENCIAOK__','67');
                                json2:=put_json(json2,'RESPUESTA','');
                                return json2;
                        end if;
                        */
                        --Se procesa por el 8010
                        --perform logfile('DAO_DTE_13794_8'||data1);
			if get_json('ID_UNICO_TX',json2)<>'' then
                                json2:=logjson(json2,'Seteamos Parametro5 para hacer proxy');
                                json2:=put_json(json2,'PARAMETRO5',get_json('ID_UNICO_TX',json2));
                        end if;
			json2:=put_json(json2,'FECHA_INGRESO_COLA',now()::varchar);
                        json2:=put_json(json2,'__SECUENCIAOK__','67');
                        json2:=put_json(json2,'RESPUESTA','');
                        return json2;

                else
                        --Libero el Folio
                        json2:=logjson(json2,'Falla Firma');
                        update id_temporal_gestor_folios set estado=0 where id=idGestorFolio1;
                        resp1:=json_get('ERROR',json_resp1);
                           if (length(resp1)=0) then
                                resp1:='Servicio Firma Electronica no responde.<br>Reintente más tarde.';
                           end if;
			if(get_json_upper('__EMISION_LOTE__',json2)='SI') then
                        	json2:=response_requests_6000_upper('2', resp1,get_json_upper('URI_IN',json2)||'_SOLICITUD='||get_json_upper('id_solicitud',json2),json2);
			else
                        	json2:=response_requests_6000_upper('2', resp1, '',json2);
			end if;
                        insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json_upper('rut_firma',json2),'FALLA',resp1,'DTE',get_json_upper('URI_IN',json2));
                        json2:=bitacora10k(json2,'EMITIR','Firma Falla');
                end if;
       elsif (strpos(resp1,'HTTP/1.1 500 ')>0) then
                        json2:=logjson(json2,'FIRMADOR '||resp1);
                   --Libero el Folio
                   json2:=logjson(json2,'Falla Firma error 500');
                   update id_temporal_gestor_folios set estado=0 where id=idGestorFolio1;
                   resp1:=json_get('ERROR',json_resp1);
                   if (length(resp1)=0) then
                        resp1:='Servicio de Validación de Firma Electronica no responde.<br>Reintente más tarde.';
                   end if;
                   --resp1:='Error: '||split_part(split_part(resp1,'<faultstring>com.acepta.custodiafirma.servicios.ServicioFirmaXMLException:',2),'</faultstring>',1);
		   if(get_json_upper('__EMISION_LOTE__',json2)='SI') then
                       	json2:=response_requests_6000_upper('2', resp1,get_json_upper('URI_IN',json2)||'_SOLICITUD='||get_json_upper('id_solicitud',json2),json2);
		   else
                   	json2:=response_requests_6000_upper('2', resp1, '',json2);
		   end if;
                   insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json_upper('rut_firma',json2),'FALLA',resp1,'DTE',get_json_upper('URI_IN',json2));
                   json2:=bitacora10k(json2,'EMITIR','Firma Falla');
        else
                   --Libero el Folio
                   json2:=logjson(json2,'Falla Firma error XXX');
                   update id_temporal_gestor_folios set estado=0 where id=idGestorFolio1;
		   if(get_json_upper('__EMISION_LOTE__',json2)='SI') then
                       	json2:=response_requests_6000_upper('2','Servicio de Firma no responde',get_json_upper('URI_IN',json2)||'_SOLICITUD='||get_json_upper('id_solicitud',json2),json2);
		   else
                        json2:=response_requests_6000_upper('2', 'Servicio de Firma no responde', '',json2);
		   end if;
                   insert into log_firma_10k_hsm (fecha,rut_firma,estado,respuesta,doc_firmado,uri) values (now(),get_json_upper('rut_firma',json2),'FALLA','Servicio de Firma no responde','DTE',get_json_upper('URI_IN',json2));
                   json2:=bitacora10k(json2,'EMITIR','Firma Falla');

        end if;
        return json2;

END;
$$ LANGUAGE plpgsql;


