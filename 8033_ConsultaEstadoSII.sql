delete from isys_querys_tx where llave='8033';

--Proceso el DTE REcibido
insert into isys_querys_tx values ('8033',20,19,1,'select arma_llamada_sii_8033(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('8033',30,1,2,'Servicio de SII 172.16.14.88',4013,100,101,0,0,40,40);
insert into isys_querys_tx values ('8033',40,1,1,'select verifica_resp_sii_8033(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
--Genera Solicitud de CRT
insert into isys_querys_tx values ('8033',50,19,1,'select genera_solicitud_crt_8031(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,1000);

insert into isys_querys_tx values ('8033',1000,19,1,'select sp_procesa_respuesta_cola_motor88_json(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,0,0);


CREATE or replace FUNCTION verifica_resp_sii_8033(varchar) RETURNS varchar AS $$
DECLARE
        xml1        alias for $1;
        xml2    varchar;
        --json_par1       json;
        json_aux        json;
        json_in         json;
        json_script1    json;
        resp_est        varchar;
        resp_cod        varchar;
        glosa_es        varchar;
        glosa_er        varchar;
        output1         varchar;
        fecha1          varchar;
        cola1   varchar;
        v_nombre_tabla  VARCHAR;
        rut1    varchar;
        aux1    varchar;
	json_out	json;
	j4	json;
BEGIN
	xml2:=xml1;

        xml2 := put_campo(xml2,'__SECUENCIAOK__','1000');
	output1:=get_campo('RESPUESTA',xml2);
	xml2:=logapp(xml2,'SII json='||replace(output1,chr(10),''));
	if(strpos(output1,'HTTP/1.0 200')=0 and strpos(output1,'HTTP/1.1 200')=0) then
		xml2:=logapp(xml2,'Falla Respuesta del SII '||output1);
                xml2:=put_campo(xml2,'RESPUESTA','Status: 400 NK');
		--perform libera_ipport_sii(get_campo('IPPORT_SII',xml2),'FALLA');
                return xml2;
        end if;
	
	--Si no es un json, reintentamos
        begin
                json_out:=split_part(output1,chr(10)||chr(10),2)::json;
                j4:=get_first_key_json(get_first_key_json(json_out::varchar));
        exception when others then
                xml2:=logapp(xml2,'Respuesta SII no es un json' );
                xml2:=put_campo(xml2,'RESPUESTA','Status: 400 NK');
		--perform libera_ipport_sii(get_campo('IPPORT_SII',xml2),'FALLA');
                return xml2;
        end;
        
	xml2:=put_campo(xml2,'INPUT','');
	--perform libera_ipport_sii(get_campo('IPPORT_SII',xml2),'OK');
        resp_est:=get_json('ESTADO',j4);
        resp_cod:=get_json('ERR_CODE',j4);
        glosa_es:=get_json('GLOSA_ESTADO',j4);
        glosa_er:=get_json('GLOSA_ERR',j4);
        if(resp_cod<>'') then
                --resp_est:='FALLA';
                if(resp_est='DOK') then
                        xml2:=logapp(xml2,'DTE Documento Recibido por el SII. Datos Coinciden con los Registrados. ');
                        xml2:=put_campo(xml2,'COMENTARIO_TRAZA','Glosa: '||glosa_es||' ('||resp_est||')');
                        xml2:=put_campo(xml2,'EVENTO','ASI');
                        xml2:=actualiza_estado_dte(xml2);
                        xml2:=graba_bitacora(xml2,'ASI');
			xml2:=put_campo(xml2,'RESPUESTA','');
			xml2:=put_campo(xml2,'CERTIFICADO_X509','');
			xml2:=put_campo(xml2,'XML3','');
                        xml2 := put_campo(xml2,'__SECUENCIAOK__','50');
                        return xml2;
		--Se asume que si un DTE esta con una nota de credito en el SII, esta recibido, por ende aprobado
                elsif(resp_est in ('MMC','ANC','MMD','TMC','AND','TMD')) then
                        xml2:=logapp(xml2,'DTE Documento Recibido por el SII pero con Errores.');
                        xml2:=put_campo(xml2,'COMENTARIO_TRAZA','Glosa: DTE Documento Recibido por el SII.'||chr(10)||glosa_er||' ('||resp_cod||')');
                        xml2:=put_campo(xml2,'EVENTO','ASI');
                        xml2:=actualiza_estado_dte(xml2);
                        xml2:=graba_bitacora(xml2,'ASI');
                        --xml2 := put_campo(xml2,'RESPUESTA','Status: 200 OK');
			--FAY 2018-07-19 Se envia el CRT
			xml2:=put_campo(xml2,'RESPUESTA','');
			xml2:=put_campo(xml2,'CERTIFICADO_X509','');
			xml2:=put_campo(xml2,'XML3','');
                        xml2 := put_campo(xml2,'__SECUENCIAOK__','50');
                        return xml2;
                elsif(resp_est in ('DNK')) then
                        xml2:=logapp(xml2,'DTE Documento Recibido por el SII pero Datos NO Coinciden con los registrados. ');
                        xml2:=put_campo(xml2,'COMENTARIO_TRAZA','Glosa: '||glosa_es||' ('||resp_est||')');
                        xml2:=put_campo(xml2,'EVENTO','RSI');
                        xml2:=actualiza_estado_dte(xml2);
                        xml2:=graba_bitacora(xml2,'RSI');
                        xml2 := put_campo(xml2,'RESPUESTA','Status: 200 OK');
                        return xml2;
		--Se crea un nuevo estado
		elsif(resp_est in ('FAN')) then
                        xml2:=logapp(xml2,'DTE Documento Anulado por el SII');
                        xml2:=put_campo(xml2,'COMENTARIO_TRAZA','Glosa: '||glosa_es||' ('||resp_est||')');
                        xml2:=put_campo(xml2,'EVENTO','RFAN');
                        xml2:=actualiza_estado_dte(xml2);
                        xml2:=graba_bitacora(xml2,'FAN');
                        xml2 := put_campo(xml2,'RESPUESTA','Status: 200 OK');
                        return xml2;

                elsif(resp_est in ('FAU','NA')) then
			--Aun no llega al sii, le damos tiempo
			if(now()-get_campo('FECHA_INGRESO_COLA',xml2)::timestamp>interval '2 days') then
                       		xml2 := put_campo(xml2,'RESPUESTA','Status: 200 OK');
				xml2:=logapp(xml2,'DTE por 2 dias, se borra de las cola');
				xml2:=put_campo(xml2,'EVENTO','RSI');
				resp_est:='RSI';
                        	xml2:=actualiza_estado_dte(xml2);
       		        else
                       		xml2 := put_campo(xml2,'RESPUESTA','Status: 444 NK');
				xml2:=put_campo(xml2,'EVENTO',resp_est);
				--Vuelvo a poner la direccion porque el motor la borra
	                end if;
			xml2:=put_campo(xml2,'COMENTARIO_TRAZA','Glosa: '||glosa_er||' ('||resp_cod||'-'||resp_est||')');
			xml2:=graba_bitacora(xml2,resp_est);
                else
                       xml2 := put_campo(xml2,'RESPUESTA','Status: 400 NK');
                       xml2 := logapp(xml2,'Falla Consulta SII');
                       return xml2;
                end if;
        else
                --Lo graba en la cola para procesamiento posterior
                xml2 := logapp(xml2,'Falla Consulta SII');
                xml2 := put_campo(xml2,'RESPUESTA','Status: 400 NK');
                return xml2;
        end if;
        return xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION arma_llamada_sii_8033(varchar) RETURNS varchar AS $$
DECLARE
        xml1        alias for $1;
        xml2    varchar;
	--json_par1	json;
	json_aux	json;
	json_in		json;
	json_script1	json;
	resp_est	varchar;
	resp_cod	varchar;	
	glosa_es	varchar;
	glosa_er	varchar;
	output1		varchar;
	fecha1		varchar;
	cola1	varchar;
	v_nombre_tabla	VARCHAR;
	rut1	varchar;
	aux1	varchar;
	port            varchar;
BEGIN
    xml2:=xml1;
	
    begin	
	fecha1:=to_char(get_campo('FECHA_EMISION',xml2)::timestamp,'DD-MM-YYYY');
    exception when others then
	--Si la fecha viene mal, se poner por defecto
       xml2:=logapp(xml2,'Fecha de Emision Invalida '||get_campo('FECHA_EMISION',xml2));
       xml2:=put_campo(xml2,'RESPUESTA','Status: 444 NK');
       xml2 := put_campo(xml2,'__SECUENCIAOK__','1000');
       return xml2;
    end;
	
    /*
     port:=get_ipport_sii();
     --Si no hay puertos libres ...
     if (port='') then
	--Si no hay puertos libres...
       xml2:=logapp(xml2,'No hay puertos libres');
       xml2:=put_campo(xml2,'RESPUESTA','Status: 400 NK');
       xml2 := put_campo(xml2,'__SECUENCIAOK__','1000');
       return xml2;
     end if;
    */

     json_in:='{"RutCompania":"'||get_campo('RUT_EMISOR',xml2)||'","DvCompania":"'||modulo11(get_campo('RUT_EMISOR',xml2))||'","RutReceptor":"'||get_campo('RUT_RECEPTOR',xml2)||'","DvReceptor":"'||modulo11(get_campo('RUT_RECEPTOR',xml2))||'","TipoDte":"'||get_campo('TIPO_DTE',xml2)||'","FolioDte":"'||get_campo('FOLIO',xml2)||'","FechaEmisionDte":"'||fecha1||'","URI":"'||get_campo('URI_IN',xml2)||'","RUT_OWNER":"'||get_campo('RUT_RECEPTOR',xml2)||'"}';
    json_in:=put_json(json_in,'MontoDte',get_campo('MONTO_TOTAL',xml2));

    xml2:=logapp(xml2,'SII json='||json_in::varchar);
    xml2 := put_campo(xml2,'__SECUENCIAOK__','30');

    --xml2:=put_campo(xml2,'__IP_CONEXION_CLIENTE__',split_part(port,':',1));
    --xml2:=put_campo(xml2,'__IP_PORT_CLIENTE__',split_part(port,':',2));
    --xml2:=put_campo(xml2,'IPPORT_SII',port);
    xml2:=put_campo(xml2,'__IP_CONEXION_CLIENTE__','interno.acepta.com');
    xml2:=put_campo(xml2,'__IP_PORT_CLIENTE__','8080');
    --xml2:=put_campo(xml2,'IP_PORT_CLIENTE',split_part(port,':',2));
    --xml2:=put_campo(xml2,'IP_CONEXION_CLIENTE',split_part(port,':',1));
    xml2:=put_campo(xml2,'IP_PORT_CLIENTE','interno.acepta.com');
    xml2:=put_campo(xml2,'IP_CONEXION_CLIENTE','8080');

    xml2:=put_campo(xml2,'INPUT','POST /sii/estado_dte HTTP/1.1'||chr(10)||'User-Agent: curl/7.26.0'||chr(10)||'Host: '||get_campo('__IP_CONEXION_CLIENTE__',xml2)||':'||get_campo('__IP_PORT_CLIENTE__',xml2)||chr(10)||'Accept: '||chr(42)||'/'||chr(42)||chr(10)||'Content-Type: application/json; charset=ISO-8859-1'||chr(10)||'Content-Length: '||length(json_in::varchar)::varchar||chr(10)||chr(10)||json_in::varchar);
    xml2:=put_campo(xml2,'RESPUESTA','');
    return xml2;
END;
$$ LANGUAGE plpgsql;

