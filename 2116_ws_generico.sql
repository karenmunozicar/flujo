delete from isys_querys_tx where llave='2116';

--Va por el API Base_PG_WEB puerto 8009
insert into isys_querys_tx values ('2116',10,9,1,'select ws_generico_2116(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('2116',20,1,10,'$$SCRIPT$$',0,0,0,1,1,30,30);

insert into isys_querys_tx values ('2116',30,1,1,'select ws_generico_resp_script_2116(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

CREATE or replace FUNCTION ws_generico_2116(varchar) RETURNS varchar AS $$
DECLARE
    xml1		alias for $1;
    xml2		varchar;
    data1		varchar;
    file1		varchar;
    sts			integer;
    header1		varchar;
    url1		varchar;
    host1		varchar;
    rut_emisor1 	varchar;
    query1		varchar;
    resp_xml1		varchar;
    tipo_tx1 		varchar;
    exists_select1  	varchar;
    estado_select1  	varchar;
    tipo_resp1		varchar;
    estado1		varchar;
    input1		varchar;
    json1		varchar;
    pos1		integer;
    pos12		integer;
    stSec		define_secuencia_ws_generico%ROWTYPE;
    file_wsdl1	varchar;
BEGIN 
    xml2:=xml1;
    xml2:=get_parametros(xml2);
    xml2:=put_campo(xml2,'__SECUENCIAOK__','0');
    --xml2:=logapp(xml2,'XML21= '||xml2);  

    --Contestamos el wsld
    if (get_campo('REQUEST_METHOD',xml2)='GET' and strpos(get_campo('REQUEST_URI',xml2),'wsdl')>0) then
		xml2:=logapp(xml2,'WSDL : Request Uri-->'||get_campo('REQUEST_URI',xml2));
                SELECT pg_read_file('.'||get_campo('REQUEST_URI',xml2)) into file_wsdl1;
                xml2:=logapp(xml2,'Responde WSDL para '||get_campo('REQUEST_URI',xml2));
                xml2:=put_campo(xml2,'RESPUESTA','Status: 200 OK'||chr(10)||'Content-type: text/html'||chr(10)||'Content-length: '||length(file_wsdl1)||chr(10)||chr(10)||file_wsdl1);
                return xml2;
    end if;

  
    input1:=decode(get_campo('INPUT',xml2),'hex');
    xml2:=logapp(xml2,'INPUT= '||input1);
    pos1:=position('<tem:IN>' in input1)+8;
    pos12:=position('</tem:IN>' in input1)-1;
    if(pos12-pos1+1>0) then 
    	json1:=substring(input1 from pos1 for (pos12-pos1+1));
--	json1:=b64_to_string(json1);
    	xml2:=json_to_xml(json1,xml2); 
    end if;
    
--    xml2:=logapp(xml2,'INPUT= '||input1);
    xml2:=logapp(xml2,'JSON= '||json1);
--    xml2:=logapp(xml2,'XML21= '||xml2);
--    xml2:=logapp(xml2,'XML22= '||json_to_xml(json1,xml2));

    tipo_tx1:=get_campo('tipo_tx',xml2);
    tipo_resp1:=get_campo('tipo_resp',xml2);
    if(tipo_resp1 <> 'xml' and tipo_resp1 <> 'json' and tipo_resp1 <> '') then
	xml2:=response_requests_2116('400', '', 'Tipo de Respuesta No Valido', xml2);
	return xml2;
    elsif(tipo_resp1='') then
	xml2:=put_campo(xml2,'tipo_resp','json');
    end if;

    xml2:=logapp(xml2,'TX  = '||tipo_tx1);
    xml2:=logapp(xml2,'RUT = '||get_campo('rut_emisor',xml2));

    select * into stSec from define_secuencia_ws_generico where tipo_tx=tipo_tx1;
    if not found then
    	xml2:=put_campo(xml2,'ERROR_LOG','No existe transaccion = '||tipo_tx1);
        xml2:=put_campo(xml2,'STATUS_HTTP','400 Peticion Incorrecta');
        xml2:=put_campo(xml2,'ERRORCOD','400');
        xml2:=put_campo(xml2,'ERRORMSG','Servicio '||tipo_tx1||'No Habilitado');
        xml2:=logapp(xml2,'No existe transaccion = '||tipo_tx1);

	xml2:=response_requests_2116('400', '', get_campo('ERROR_LOG',xml2), xml2);	
        return xml2;
    end if;

    --EJECUTA FUNCION INPUT
    xml2:=logapp(xml2,'Ejecuta='||stSec.funcion_input);
    if length(stSec.funcion_input)>0 then
	EXECUTE 'SELECT ' || stSec.funcion_input || '(' || chr(39) || xml2 || chr(39) || ')' into xml2;
    end if;

    --Si necesita llamar un scrip lo ejecuta
    if (get_campo('LLAMA_SCRIPT',xml2)='SI') then
        xml2:=put_campo(xml2,'__SECUENCIAOK__','20');
    end if;

    return xml2;
    
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION ws_generico_resp_script_2116(varchar) RETURNS varchar AS $$
DECLARE
    xml1                alias for $1;
    xml2        varchar;
    funcion1    varchar;
begin
    --EJECUTA FUNCION output
    xml2:=xml1;
    xml2:=put_campo(xml2,'__SECUENCIAOK__','0');
    funcion1:=get_campo('FUNCION_RESPUESTA',xml2);
    xml2:=logapp(xml2,'Ejecuta Respuesta='||funcion1);
    if length(funcion1)>0 then
        EXECUTE 'SELECT ' || funcion1 || '(' || chr(39) || xml2 || chr(39) || ')' into xml2;
    end if;
    return xml2;
END;
$$ LANGUAGE plpgsql;

