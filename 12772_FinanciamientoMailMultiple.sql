--Publica documento
delete from isys_querys_tx where llave='12772';

insert into isys_querys_tx values ('12772',10,1,8,'LLAMADA AL FLUJO 12773',12773,0,0,1,1,30,30);

insert into isys_querys_tx values ('12772',20,19,1,'select get_next_financiador_12772(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,30,30);

insert into isys_querys_tx values ('12772',30,19,1,'select analiza_resp_12772(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

CREATE or replace FUNCTION pivote_financiamiento_12772(json) RETURNS json
AS $$
DECLARE
	json1	alias for $1;
	json2	json;
	json3   json;
        sts2    varchar;
        query1  varchar;
        rut1    varchar;
        tipo1   varchar;
        folio1  varchar;
        campo   financiamiento_solicitudes%ROWTYPE;
	campo1	record;
	rut_aux1	bigint;
	date1	date;
	campo_cd record;
	rut_usu1	varchar;
	anomes1	varchar;
	uri_cd1	varchar;
BEGIN
	json2:=json1;
	json2:=put_json(json2,'LLAMA_FLUJO','SI');
        rut1:=split_part(replace(get_json('EMISOR',json2),'.',''),'-',1);
        tipo1:=get_json('TIPO',json2);
        folio1:=replace(get_json('FOLIO',json2),'.','');
	rut_usu1:=get_json('rutUsuario',json2);
	anomes1:=(to_char(now(),'YYYY')||'-'||to_char(now(),'MM')::integer::varchar);

	json2:=logjson(json2,'Entra a pivote_financiamiento_12772');
	if (get_json('check_tyc',json2)<>'on') then
		return response_requests_6000('2','Debe aceptar los Términos y Condiciones.','',json2);
	end if;

	BEGIN
		date1:=get_json('fecha_pago',json2)::date;
	EXCEPTION WHEN OTHERS THEN
		return response_requests_6000('2','Por favor ingrese correctamente la fecha de pago.','',json2);
	END;

	json2:=logjson(json2,'Entra a pivote_financiamiento_12772 1');
        --Verificamos si no existe una cotizacion enviada
        select *  into campo from financiamiento_solicitudes where rut_emisor=rut1::integer and tipo_dte=tipo1::integer and folio=folio1::bigint;
        if found then
                if campo.estado='SOLICITADO' then
                        return response_requests_6000('2','Ya existe una solicitud pendiente de financiamiento','',json2);
                end if;
        end if;

	--Verificamos si tiene la carpeta tributaria del mes
	select * into campo_cd from cd_lista_carpeta_legal where categoria='CARPETA_TRIBUTARIA' and rut=rut1::integer and nro_cliente=rut_usu1 and subcategoria=anomes1;
	if not found then
		uri_cd1:=coalesce((select remplaza_tags_6000(href,json2) from menu_info_10k where id2='casilla_legal'),'');
		--return response_requests_6000('2','La Empresa aún no tiene su carpeta legal del mes actual en nuestros registros '||anomes1,'{"URL_RESPUESTA_BLANK":"'||uri_cd1||'"}',json2);
		return response_requests_6000('2','Estimado Cliente,'||chr(10)||'La empresa no tiene su carpeta tributaria eléctronica del mes actual en nuestros registros.'||chr(10)||'Esta información es necesaria para que el financista pueda realizar la evaluación correspondiente.'||chr(10)||'Le pedimos que la actualice en su Casilla Legal para continuar con la cotización. ','{"URL_RESPUESTA_BLANK":"'||uri_cd1||'"}',json2);
	end if;	

	lista_fin:='[]';
	for campo1 in select * from financiamiento_financiadores where flag_prueba ='NO' and codigo is not null loop
		lista_fin:=put_json_list(lista_fin,campo1.codigo);
	end loop;	
	i:=0;
	json2:=put_json(json2,'CONTADOR_FIN',i::varchar);
	json2:=put_json(json2,'JSON_FIN',lista_fin::varchar);
	json2:=put_json(json2,'TOTAL_FIN',count_array_json(lista_fin)::varchar);
	json2:=put_json(json2,'financiador',get_json_index(lista_fin,i));

	json2:=put_json(json2,'__SECUENCIAOK__','12772');
	json2:=logjson(json2,'Entra a pivote_financiamiento_12772 4');
	return json2;
END;
$$
LANGUAGE plpgsql;

CREATE or replace FUNCTION get_next_financiador_12772(json) RETURNS json
AS $$
DECLARE
        json1   alias for $1;
        json2   json;
	jfin	json;
	i	integer;
BEGIN
        json2:=json1;
	jfin:=get_json('JSON_FIN',json2);
	i:=get_json('CONTADOR_FIN',json2)::integer;
	if i+1>=get_json('TOTAL_FIN',json2)::integer then
		json2:=put_json(json2,'__SECUENCIAOK__','0');
                return response_requests_6000('1','Solicitud Enviada OK','{"NO_REFRESH":"SI"}',json2);
	end if;

	json2:=put_json(json2,'CONTADOR_FIN',(i+1)::varchar);
	json2:=put_json(json2,'financiador',get_json_index(jfin,i+1));
	json2:=put_json(json2,'__SECUENCIAOK__','10');
	
	return json2;
END;
$$
LANGUAGE plpgsql;


CREATE or replace FUNCTION analiza_resp_12772(json) RETURNS json
AS $$
DECLARE
        json1   alias for $1;
        json2   json;
BEGIN
        json2:=json1;
	json2:=put_json(json2,'__SECUENCIAOK__','20');
	return json2;
END;
$$
LANGUAGE plpgsql;

