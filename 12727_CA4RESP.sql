delete from isys_querys_tx where llave='12727';

-- Prepara llamada al AML
insert into isys_querys_tx values ('12727',20,1,1,'select proc_procesa_input_12727(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0); 

--Llamada al Flujo de Publicacion
--Llamada al AML directo SGCI
--insert into isys_querys_tx values ('12727',25,1,8,'Llamada Publica DTE',12704,0,0,0,0,30,30);
insert into isys_querys_tx values ('12727',30,1,2,'Llamada directo al AML',14000,102,101,0,0,40,40);
insert into isys_querys_tx values ('12727',31,1,2,'Llamada directo al AML',14001,102,101,0,0,40,40);
insert into isys_querys_tx values ('12727',32,1,2,'Llamada directo al AML',14002,102,101,0,0,40,40);
insert into isys_querys_tx values ('12727',33,1,2,'Llamada directo al AML',14003,102,101,0,0,40,40);
insert into isys_querys_tx values ('12727',34,1,2,'Llamada directo al AML',14004,102,101,0,0,40,40);
insert into isys_querys_tx values ('12727',35,1,2,'Llamada directo al AML',14005,102,101,0,0,40,40);
insert into isys_querys_tx values ('12727',36,1,2,'Llamada directo al AML',14006,102,101,0,0,40,40);
insert into isys_querys_tx values ('12727',37,1,2,'Llamada directo al AML',14007,102,101,0,0,40,40);
insert into isys_querys_tx values ('12727',38,1,2,'Llamada directo al AML',14008,102,101,0,0,40,40);
insert into isys_querys_tx values ('12727',39,1,2,'Llamada directo al AML CGE',14009,102,101,0,0,40,40);

--Respuesta del AML
insert into isys_querys_tx values ('12727',40,1,1,'select proc_procesa_respuesta_12727(''$$__XMLCOMPLETO__$$'') as __xml__ ',0,0,0,1,1,-1,0);


CREATE or replace FUNCTION proc_procesa_input_12727(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
        xml2    varchar;
    data1       varchar;
    file1       varchar;
    sts         integer;
    host1       varchar;
    url1        varchar;
    respuesta1  varchar;
    resp1       varchar;
    status1     varchar;
    input1      varchar;
    rut_emisor1 varchar;
    rut_receptor1 varchar;
    tipo_dte1   varchar;
    folio1      varchar;
    monto1      varchar;
    fecha_emi1  varchar;
    stRec       dte_recibidos%ROWTYPE;
    mail1       varchar;
    stMail      windte.wdte_op_clientes%ROWTYPE;
    md5_input1	varchar;  
    stRechazado	dte_arm_rechazados%ROWTYPE;

    stSecuencia secuencia_aml%ROWTYPE;
    falla1	integer;
BEGIN
    xml2:=xml1;
    --Si es un nagios, ignoro el procesamiento
    if (get_campo('REQUEST_METHOD',xml2)='GET') then
        if ((strpos(get_campo('HTTP_USER_AGENT',xml2),'check_http')>0) or (length(get_campo('QUERY_STRING',xml2))=0)) then
                xml2 := logapp(xml2,'Nagios Check o GET sin datos, se ignora');
                --Se Responde OK
                xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK'||chr(10)||
                'Content-type: text/html; charset=iso-8859-1'||chr(10)||
                'Content-length: 0'||chr(10)||
                'Vary: Accept-Encoding'||chr(10)||chr(10));
                xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
		xml2 := sp_procesa_respuesta_cola_motor(xml2);
                return xml2;
        end if;
    end if;

    --Parseo datos
   input1:=decode(get_campo('INPUT',xml2),'hex');
   xml2 := put_campo(xml2,'TIPO_DTE',get_xml('TipoDoc',input1));
   tipo_dte1:=get_campo('TIPO_DTE',xml2);
   if (tipo_dte1='') then
   	xml2 := put_campo(xml2,'TIPO_DTE',get_xml('TipoDTE',input1));
	tipo_dte1:=get_campo('TIPO_DTE',xml2);
   end if;
	
   xml2 := put_campo(xml2,'FOLIO',get_xml('Folio',input1));
   xml2 := put_campo(xml2,'RUT_EMISOR',split_part(get_xml('RUTEmisor',input1),'-',1));
   xml2 := put_campo(xml2,'RUT_RECEPTOR',split_part(get_xml('RUTRecep',input1),'-',1));
   xml2 := put_campo(xml2,'FECHA_EMISION',get_xml('FchEmis',input1));
   xml2 := put_campo(xml2,'MONTO_TOTAL',get_xml('MntTotal',input1));
   xml2 := put_campo(xml2,'URI_IN',split_part(split_part(input1,'filename="',2),'"',1));


   --xml2 := logapp(xml2,'DATA CA4RESP='||replace(input1,'\012',chr(10)));

   rut_emisor1:=get_campo('RUT_EMISOR',xml2);
   rut_receptor1:=get_campo('RUT_RECEPTOR',xml2);
   folio1:=get_campo('FOLIO',xml2);
   monto1:=get_campo('MONTO_TOTAL',xml2);
   fecha_emi1:=get_campo('FECHA_EMISION',xml2);

	

   xml2:=logapp(xml2,'CA4RESP rut_emisor1='||rut_emisor1||' tipo_dte1='||tipo_dte1||' folio1='||folio1||' monto1='||monto1||' fecha_emi1='||fecha_emi1||' URI='||get_campo('URI_IN',xml2));
   xml2:=logapp(xml2,'CA4RESP '||input1);
   
   --Si el RUT_EMISOR no es numerico borre el DTE
   if (is_number(rut_emisor1) is false) then
		xml2:=logapp(xml2,'CA4RESP: Se borra DTE, rut_emisor no numerico');
                xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK'||chr(10)||
                'Content-type: text/html; charset=iso-8859-1'||chr(10)||
                'Content-length: 0'||chr(10)||
                'Vary: Accept-Encoding'||chr(10)||chr(10));
                xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
                xml2 := sp_procesa_respuesta_cola_motor(xml2);
		return xml2;
   end if;

    --servicio directo al AML
    --raise notice 'arma scgi';
    xml2:=arma_scgi(xml2);
    if (strpos(get_campo('URI_IN',xml2),'http://cencosud')>0) then
                xml2 := logapp(xml2,'Filtro Cencosud');
                xml2 := put_campo(xml2,'__SECUENCIAOK__','37');
                return xml2;
   end if;
   if (strpos(get_campo('URI_IN',xml2),'http://windte')>0) then
                xml2 := logapp(xml2,'Filtro Windte');
                xml2 := put_campo(xml2,'__SECUENCIAOK__','34');
                return xml2;
   end if;
   --Si es CGE cambio motor.fcgi por ca4dte
   if (get_campo('SCRIPT_NAME',xml2)='/motor/motor.fcgi') then
        xml2 := put_campo(xml2,'SCRIPT_NAME','/ca4/ca4dte');
        xml2 := put_campo(xml2,'SERVER_NAME','cge-pub.acepta.com');
        xml2 := put_campo(xml2,'SCRIPT_URL','/ca4/ca4dte');
        xml2 := put_campo(xml2,'SCRIPT_URI','http://cge-pub.acepta.com/ca4/ca4dte');
        xml2 := put_campo(xml2,'REQUEST_URI','/ca4/ca4dte');

   end if;


    --Determino a que AML tengo que ir
   host1=get_campo('SERVER_NAME',xml2);
   url1:=get_campo('SCRIPT_NAME',xml2);
   xml2 := logapp(xml2,'Server '||host1||' Url '||url1);
   select * into stSecuencia from secuencia_aml where server_name=host1 and script_name=url1;
   if found then
                xml2 := put_campo(xml2,'__SECUENCIAOK__',stSecuencia.secuencia);
   else
                xml2 := logapp(xml2,'FALLA SERVER_NAME='||host1||' SCRIPT_NAME='||url1||' no definido');
                --Vamos a publicar el DTE
                --xml2 := put_campo(xml2,'__SECUENCIAOK__','1000');
                xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
                xml2 := sp_procesa_respuesta_cola_motor(xml2);
   end if;

   --raise notice 'exit ';
   return xml2;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION proc_procesa_respuesta_12727(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2        varchar;
    data1       varchar;
    resp1       varchar;
    sts         integer;
    texto_resp1 varchar;
    respuesta1  varchar;
    status1     varchar;
    md5_input1  varchar;
    stRec       dte_recibidos%ROWTYPE;
BEGIN
    xml2:=xml1;
    data1:=get_campo('INPUT',xml2);
    xml2 := put_campo(xml2,'__SECUENCIAOK__','0');

    --Limpio el INPUT para el LOG
    resp1:= get_campo('RESPUESTA',xml2);  
    xml2:=logapp(xml2,'CA4RESP: Texto Respuesta AML='||resp1);
    --Si viene este texto entonces AML responde OK
    texto_resp1 := 'URL(True): '||get_campo('URI_IN',xml2);

    --Verifico si me fue bien con el AML
    --Debe contestar un OK y debe venir la URI que se envio a la entrada
    --if strpos(resp1,'200 OK')>0 then
    if (strpos(resp1,'200 OK')>0 and strpos(resp1,texto_resp1)>0) then
        xml2 := logapp(xml2,'CA4RESP: FOLIO='||get_campo('FOLIO',xml2)||' RUT_EMISOR='||get_campo('RUT_EMISOR',xml2)||' TIPO_DTE='||get_campo('TIPO_DTE',xml2)||' Respuesta OK Aml');
        --Un documento reprocesado, que se envia al AML, se puede borrar de la cola de procesamiento
        if (get_campo('_REPROCESO_',xml2)='SI') then
                xml2 := put_campo(xml2,'_ESTADO_REPROCESO_','OK');
        end if;

        --Saco los datos que requiero de la respuesta
        xml2 := put_campo(xml2,'URI',get_tag_http(resp1,'URL(True): '));
    else

        xml2 := logapp(xml2,'CA4RESP: FOLIO='||get_campo('FOLIO',xml2)||' RUT_EMISOR='||get_campo('RUT_EMISOR',xml2)||' TIPO_DTE='||get_campo('TIPO_DTE',xml2)||' Falla Respuesta AML');
        xml2 := put_campo(xml2,'ESTADO','ERROR_AML');
        xml2 := graba_bitacora(xml2,'ERROR_AML');
        xml2 := logapp(xml2,resp1);
    end if;

    --TODO hacer un control cuando falle el update
    --xml2 := put_campo(xml2,'INPUT','');
    --respuesta1:=split_part(resp1,chr(10)||chr(10),2);
    if (strpos(resp1,'200 OK')>0) then
        status1:='Status: 200 OK'||chr(10)||
                 'Content-type: text/html'||chr(10)||
                 'Content-Location: '||get_campo('URI_IN',xml2)||chr(10)||
                 'Content-length: '||length(texto_resp1)||chr(10);
        xml2 := logapp(xml2,'CA4RESP: Respuesta Servicio 200 OK URI'||get_campo('URI_IN',xml1));
	xml2 := logapp(xml2,'CA4RESP: Respuesta'||status1||chr(10)||texto_resp1);
        xml2:=put_campo(xml2,'RESPUESTA',status1||chr(10)||texto_resp1);
    else
        status1:='Status: 400 Rechazado'||chr(10)||'Content-type: text/html'||chr(10)||'Content-length: '||length(respuesta1)||chr(10);
        xml2 := logapp(xml2,'Respuesta Servicio 400 Rechazado (12727) URI'||get_campo('URI_IN',xml2));
    end if;

    xml2 := put_campo(xml2,'__SOCKET_RESPONSE__','RESPUESTA');
    xml2 := sp_procesa_respuesta_cola_motor(xml2);
    --Respondo lo que viene
    RETURN xml2;
END;
$$ LANGUAGE plpgsql;


