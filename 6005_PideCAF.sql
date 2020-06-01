delete from isys_querys_tx where llave='6005';

insert into isys_querys_tx values ('6005',100,1,1,'select verifica_config_sii_6005(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

--Llama Servicio Generico Gestor de Folios
insert into isys_querys_tx values ('6005',110,1,2,'Servicio HTTP Generico',4013,100,101,0,0,120,120);

--Valida Respuesta de gestor de folio
insert into isys_querys_tx values ('6005',120,1,1,'select valida_gestor_folio_6005(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);
--Llama Servicio Generico Gestor de Folios para Inicializar
insert into isys_querys_tx values ('6005',130,1,2,'Servicio HTTP Generico',4013,100,101,0,0,140,140);

--Valida Resp inicializacoin Gestor
insert into isys_querys_tx values ('6005',140,1,1,'select valida_gestor_folio_inicializacion_6005(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

CREATE OR REPLACE FUNCTION verifica_config_sii_6005(json) RETURNS json
AS $$
DECLARE
        json1               alias for $1;
        json2               json;
        request1            varchar;
        rut_emisor1         varchar;
        rut_caf1            varchar;
        v_id_solicitud        varchar;
        stEmpresa             record;
        afecta                varchar;
        exenta                varchar;
        tipoDTE               integer;
BEGIN
        json2:=json1;
        v_id_solicitud:=get_json('id_solicitud',json2);
	json2:=logjson(json2,'v_id_solicitud='||v_id_solicitud);

        if(length(v_id_solicitud)>0) then
                select * from empresa_certificacion_datos where id_solicitud = v_id_solicitud::bigint into stEmpresa;
                if found then
                    rut_caf1:=split_part(stEmpresa.rutempresa,'-',1);
			json2:=get_parametros_motor_json(json2,'rut_caf1='||rut_caf1);
                    rut_emisor1:=rut_caf1;
			json2:=get_parametros_motor_json(json2,'rut_emisor1='||rut_emisor1);
                        select setbasico, exento into afecta, exenta from tipo_documento_certificar where rutempresa=stEmpresa.rutempresa;
                        if found then
                            if(afecta='S') then
                                tipoDTE:=33;
				json2:=put_json(json2,'tipoDTE','33');
                            else
                                tipoDTE:=34;
				json2:=put_json(json2,'tipoDTE','34');
                            end if;
                        else
                              json2:=logjson(json2,'NO SE ENCUENTRAN TIPOS DE DTE PARA LA EMPRESA =>'||stEmpresa.rutempresa);
                              json2:=response_requests_6000('2', 'No existen datos', '', json2);
                              return json2;
                        end if;
                else
                        json2:=logjson(json2,'NO SE ENCUENTRAN TIPOS DE DTE PARA LA EMPRESA =>'||stEmpresa.rutempresa);
                        json2:=response_requests_6000('2', 'No existen datos', '', json2);
                        return json2;
                end if;
        end if;

	json2:=put_json(json2,'rutCAF',rut_caf1);
	json2:=put_json(json2,'rutCliente',rut_caf1);

        json2:=get_parametros_motor_json(json2,'VERIFICA_CLIENTE_SII');
        request1:='<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:emis="http://emision.legacy.servicioscaf.acepta.com/"><soapenv:Header/><soapenv:Body><emis:consultaFolioEmision><rutEmisor>'||rut_emisor1||'-'||modulo11(rut_emisor1)||'</rutEmisor><tipoDte>'||tipoDTE||'</tipoDte></emis:consultaFolioEmision></soapenv:Body></soapenv:Envelope>';
	json2:=logjson(json2,'REQUEST VERIFICA_CONFIG'||request1);
        json2:=put_json(json2,'INPUT','POST '||get_json('PARAMETRO_RUTA',json2)||' HTTP/1.1'||chr(10)||'User-Agent: curl/7.26.0'||chr(10)||'Host: '||get_json('__IP_CONEXION_CLIENTE__',json2)||chr(10)||'Accept: '||chr(42)||'/'||chr(42)||chr(10)||'Content-Type: text/xml; charset=UTF-8'||chr(10)||'SOAPAction: ""'||chr(10)||'Accept-Encoding: deflate'||chr(10)||'Content-Length: '||length(request1)::varchar||chr(10)||chr(10)||request1);

        json2:=put_json(json2,'__SECUENCIAOK__','110');
        RETURN json2;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION valida_gestor_folio_inicializacion_6005(json) RETURNS json AS $$
DECLARE
    json1                alias for $1;
    json2                json;
    codigo1             varchar;
    msg1                varchar;
    resp1               varchar;
    uri1        varchar;
    resp2       varchar;
BEGIN
        json2:=json1;

        resp1:=get_json('RESPUESTA',json2);
        json2:=put_json(json2,'RESPUESTA','');
        json2:=logjson(json2,'RESPUESTA=' || resp1);
        codigo1:=split_part(split_part(resp1,'<codigoRespuesta>',2),'</codigoRespuesta>',1);
        msg1:=split_part(split_part(resp1,'<mensaje>',2),'</mensaje>',1);

        if (codigo1 = '1') then
                json2:=logjson(json2,'Inicializacion OK');
                json2:=put_json(json2,'__SECUENCIAOK__','200');
		json2:=put_json(json2,'tarea','solicita_folios');
                return wf_avanza_solicitud1(json2);
        else
                json2:=logjson(json2,'Falla Inicializacion');
                json2:=logjson(json2,resp1);
                json2:=put_json(json2,'__SECUENCIAOK__','0');
                json2:=response_requests_6000('2',msg1,'',json2);
                return json2;
        end if;
END;
$$ LANGUAGE plpgsql;



CREATE or replace FUNCTION valida_gestor_folio_6005(json) RETURNS json AS $$
declare
        json1   alias for $1;
        json2   json;
    codigo1             varchar;
    msg1                varchar;
    resp1               varchar;
    uri1        varchar;
    resp2       varchar;
BEGIN
        json2:=json1;
        resp1:=get_json('RESPUESTA',json2);
        json2:=put_json(json2,'RESPUESTA','');
        json2:=logjson(json2,'RESPUESTA=' || resp1);
        codigo1:=split_part(split_part(resp1,'<codigoRespuesta>',2),'</codigoRespuesta>',1);
        msg1:=split_part(split_part(resp1,'<mensaje>',2),'</mensaje>',1);
        json2:=put_json(json2,'__SECUENCIAOK__','0');
        json2:=logjson(json2,'RESPUESTA='||resp1);

        if (codigo1 in ('6','9','10','11','12','15')) then
                json2:=logjson(json2,'Falla Gestor Folios '||msg1);
                json2:=response_requests_6000('2',msg1,'',json2);
                return json2;
        elsif (codigo1 = '3') then
                --Vamos a emitir el doc
                json2:=logjson(json2,'Gestor Folios OK');
                json2:=put_json(json2,'__SECUENCIAOK__','200');
		json2:=put_json(json2,'tarea','solicita_folios');
                return wf_avanza_solicitud1(json2);
                --return json2;
        elsif(codigo1 = '20') then
                --json2:=response_requests_6000('-1','Usuario debe Inicializar','',json2);
                json2:=logjson(json2,'Usuario debe Inicializar');
                json2:=put_json(json2,'numfolio','1');
                json2:=inicializa_caf_6000(json2);
                json2:=put_json(json2,'__SECUENCIAOK__','130');
                return json2;
        else
                json2:=logjson(json2,'Error Conexion SII en Gestor de Folios');
                json2:=response_requests_6000('2','Error Conexion SII en Gestor de Folios','',json2);
                return json2;
        end if;
END;
$$ LANGUAGE plpgsql;



