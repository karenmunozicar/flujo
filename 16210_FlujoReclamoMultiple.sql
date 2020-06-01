--Consulta Estado Reclamo SII
delete from isys_querys_tx where llave='16210';

insert into isys_querys_tx values ('16210',10,1,1,'select evaluar_ejecucion_16210(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('16210',12767,1,8,'Llamada ARM JSON',12767,0,0,0,0,20,20);
insert into isys_querys_tx values ('16210',12796,1,8,'Llamada NAR JSON',12796,0,0,0,0,20,20);
insert into isys_querys_tx values ('16210',16201,1,8,'Llamada Reclamo',16201,0,0,0,0,20,20); 

insert into isys_querys_tx values ('16210',20,1,1,'select flujo_control_16210(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('16210',1000,19,1,'select sp_procesa_respuesta_cola_motor88_json(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,0,0);


CREATE or replace FUNCTION define_secuencia_16210(json,varchar) RETURNS json AS $$
DECLARE
        json1                   alias for $1;
        json2                   json;
	accion1			alias for $2;
BEGIN
	json2:=json1;
	if (get_json('__COLA_MOTOR__',json2)<>'') then
		if (accion1='BORRAR') then
			json2:=put_json(json2,'RESPUESTA','Status: 200 OK');
		else
			json2:=put_json(json2,'RESPUESTA','Status: 400 NK');
		end if;
		json2:=put_json(json2,'__SECUENCIAOK__','1000');
	else
		json2:=put_json(json2,'__SECUENCIAOK__','0');
	end if;
	return json2;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION flujo_control_16210(json) RETURNS json AS $$
DECLARE
	json1                   alias for $1;
	json2                   json; 

	json_resp	json;
	cod_resp	varchar;
	msg_resp	varchar;
	linea1		varchar;
	json3		json;
BEGIN
	json2:=json1;
	json2:=logjson(json2,'Pasa por flujo_control_16210');
	BEGIN
		json_resp:=split_part(get_json('RESPUESTA',json2),chr(10)||chr(10),2)::json;
		cod_resp:=get_json('CODIGO_RESPUESTA',json_resp);	
		msg_resp:=get_json('MENSAJE_RESPUESTA',json_resp);
		--FAY-DAO 2018-11-12 Para el ARM y NAR cuando no tiene certificado en el HSM se asume que no se puede enviar
		--Pero la respuesta es exitosa para que no queden encolados enviando NAR para siempre
		if (cod_resp='2' and (strpos(msg_resp,'No existen certificados para el rut y password especificados')>0 or strpos(msg_resp,'Acceso denegado. Password incorrecto')>0)) then
			cod_resp:='1';
		end if;
		json2:=logjson(json2,'CODIGO='||cod_resp||' Mensaje='||msg_resp);
		json2:=put_json(json2,'MENSAJE_XML_FLAGS','CODIGO='||cod_resp||' Mensaje='||msg_resp);
	EXCEPTION WHEN OTHERS THEN
		json2:=logjson(json2,'Falla json respuesta '||get_json('RESPUESTA',json2));
		msg_resp:='Falla Acción';
		cod_resp:='3';
	END;
	linea1:=get_json('--NUM--',json2);
	--11EstadoReclamo
	--43EstadoMercaderia
	--RED #C90E0E
	--GREEN #58A858
	
	json2:=put_json(json2,'MENSAJE_16210',get_json('MENSAJE_16210',json2)||'['||msg_resp||'] ');
	
	--Si falla el reclamo no seguimos...
	if(get_json('FLAG_RECLAMO',json2)='SI') then
		json2:=logjson(json2,'Linea de grilla '||linea1||' Evento='||get_json('EVENTO_RECLAMO',json2));
		if is_number(linea1) then
			--Para que cuadre en la grilla
			--Seteamos el color para pintar inmediatamente el cuadro
        		if (get_json('EVENTO_RECLAMO',json2) in ('ACD','RCD')) then
				json3:='{"COLORES": [ {"ID":"'||linea1||'EstadoReclamo","COLOR":"'||case when get_json('__COLOR__',json2)='RED' then '#C90E0E' when get_json('__COLOR__',json2)='GREEN' then '#58A858' else '' end ||'","TITLE":"'||msg_resp||'"}]}';
			elsif (get_json('EVENTO_RECLAMO',json2) in ('ERM','RFT','RFP','ERG')) then
				json2:=logjson(json2,linea1);
				json2:=logjson(json2,msg_resp);
				json3:='{"COLORES": [ {"ID":"'||linea1||'EstadoMercaderia","COLOR":"'||case when get_json('__COLOR__',json2)='RED' then '#C90E0E' when get_json('__COLOR__',json2)='GREEN' then '#58A858' else '' end ||'","TITLE":"'||msg_resp||'"}]}';
			end if;
			json2:=logjson(json2,'Colores '||json3::varchar);
			json2:=put_json(json2,'COLORES',json3::varchar);
		end if;
		if (cod_resp='2') then
			--Si falla el Reclamo, se borra y no hay NAR
			json2:=response_requests_6000(cod_resp,get_json('MENSAJE_16210',json2),get_json('COLORES',json2)::varchar,json2);
			json2:=define_secuencia_16210(json2,'BORRAR');
			return json2;
		--Si fallo
		elsif (cod_resp='3') then
			json2:=response_requests_6000('2',get_json('MENSAJE_16210',json2),get_json('COLORES',json2)::varchar,json2);
			json2:=define_secuencia_16210(json2,'FALLA');
			return json2;
		end if;
		json2:=put_json(json2,'FLAG_RECLAMO','NO');
		json2:=put_json(json2,'RESPUESTA','');
		--Solo salgo si el reclamo esta OK
	end if;
	
	--Acumulamos respuestas
	if (get_json('FLAG_NAR',json2)='SI') then
		json2:=put_json(json2,'FLAG_NAR','NO');
		json2:=put_json(json2,'__SECUENCIAOK__','12796');
		json2:=put_json(json2,'MENSAJE_16210',get_json('MENSAJE_16210',json2)||' -- NAR:');
		json2:=logjson(json2,'Ejecuta NAR');
		return json2;
	end if;
	
	if (get_json('FLAG_ARM',json2)='SI') then
                json2:=put_json(json2,'FLAG_ARM','NO');
                json2:=put_json(json2,'__SECUENCIAOK__','12767');
		json2:=put_json(json2,'MENSAJE_16210',get_json('MENSAJE_16210',json2)||' -- ARM:');
		json2:=logjson(json2,'Ejecuta ARM');
                return json2;
        end if;
	
	json2:=logjson(json2,'Responde Total');
	--Si fue exitoso
	if (cod_resp='1') then
		json2:=response_requests_6000(cod_resp,get_json('MENSAJE_16210',json2),get_json('COLORES',json2)::varchar,json2);
		json2:=define_secuencia_16210(json2,'BORRAR');
	elsif (cod_resp in ('2','3')) then
		--Esta combinacion se borra
		msg_resp:=lower(get_json('MENSAJE_16210',json2));
		if (strpos(msg_resp,'dte ya esta reclamado')>0 and strpos(msg_resp,'dte no encontrado')>0) then
			json2:=response_requests_6000(cod_resp,get_json('MENSAJE_16210',json2),get_json('COLORES',json2)::varchar,json2);
			json2:=define_secuencia_16210(json2,'BORRAR');
			return json2;
		end if;
		json2:=response_requests_6000('2',get_json('MENSAJE_16210',json2),get_json('COLORES',json2)::varchar,json2);
		json2:=define_secuencia_16210(json2,'FALLA');
	end if;
	return json2;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION evaluar_ejecucion_16210(json) RETURNS json AS $$
DECLARE
        json1                   alias for $1;
        json2                   json;
        v_codTxel               integer;
        v_accion                varchar;
        v_tx                    varchar;
        campo                   record;

        json_reclamo            json;
        v_condicion             varchar;

        chk_sii1        varchar;
        chk_emi1        varchar;
	flag_nar	boolean;
	flag_arm	boolean;
	flag_reclamo	boolean;
BEGIN
        json2:=json1;
        v_accion:=get_json('accion_arm',json2);

	--Flag para que los flujos 16201 no borren
	 json2:=put_json(json2,'__FLAG_16210__','NO_BORRAR');

        json2:=put_json(json2,'__SECUENCIAOK__','0');
        if(v_accion='') then
                v_accion:=get_json('tipoAceptacion',json2);
        end if;

        json2:=put_json(json2,'RUT_EMISOR',trim(replace(split_part(get_json_upper('EMISOR',json2),'-',1),'.','')));
        json2:=put_json(json2,'TIPO_DTE',trim(get_json('TIPO',json2)));
        json2:=put_json(json2,'FOLIO',ltrim(replace(trim(get_json('FOLIO',json2)),'.',''),'0'));
        json2:=put_json(json2,'EVENTO_RECLAMO',v_accion);

        chk_sii1:=get_json('rbtn_sii',json2);
        chk_emi1:=get_json('rbtn_emi',json2);
	if(get_json('checkACD',json2)='') then
		json2:=define_secuencia_16210(json2,'REINTENTE');
		return response_requests_6000('2','Debe aceptar las condiciones.','',json2);
	end if;
	flag_reclamo:=false;
	flag_nar:=false;
	flag_arm:=false;
	--Solo SII
	--Si marcan solo SII
	if(chk_sii1='on' and chk_emi1<>'on') then
		if(trim(get_json('TIPO',json2)) not in ('33','34','43')) then
			json2:=response_requests_6000('2','Para este tipo de documento no aplica esta accion','',json2);
			json2:=define_secuencia_16210(json2,'BORRAR');
			return json2;
		else
			flag_reclamo:=true;
		end if;
	--Solo Emisor
	elsif(chk_sii1<>'on' and chk_emi1='on') then
		if(v_accion in ('ACD','RCD')) then
			flag_nar:=true;
		elsif(v_accion='ERM') then
			flag_arm:=true;
		else
			json2:=response_requests_6000('2','Acción no aplica','',json2);
			json2:=define_secuencia_16210(json2,'BORRAR');
			return json2;
		end if;
	elsif(chk_sii1='on' and chk_emi1='on') then
		if(trim(get_json('TIPO',json2)) in ('33','34','43')) then
			flag_reclamo:=true;
		else
			json2:=put_json(json2,'MENSAJE_16210','SII: [Para este DTE no aplica notificación al SII] ');
		end if;
		if(v_accion in ('ACD','RCD')) then
			flag_nar:=true;
		elsif(v_accion='ERM') then
			flag_arm:=true;
		end if;
	else
		json2:=define_secuencia_16210(json2,'BORRAR');
		return response_requests_6000('2','Por favor refresque Pantalla CTRL+R','',json2);
	end if;
	if(flag_reclamo is false and flag_nar is false and flag_arm is false) then
		json2:=response_requests_6000('2','Acción no aplica.','',json2);
		json2:=define_secuencia_16210(json2,'BORRAR');
		return json2;
	end if;


	if(flag_reclamo) then
		json2:=put_json(json2,'FLAG_RECLAMO','SI');
	else
		json2:=put_json(json2,'FLAG_RECLAMO','NO');
	end if;
	if flag_nar then
		if(v_accion in ('ACD','RCD')) then
			if(v_accion='ACD') then
				json2:=put_json(json2,'estadoDte','0');
			else
				json2:=put_json(json2,'estadoDte','2');
			end if;
		end if;
		json2:=put_json(json2,'FLAG_NAR','SI');
	else
		json2:=put_json(json2,'FLAG_NAR','NO');
	end if;
	if flag_arm then
		json2:=put_json(json2,'recinto',get_json('glosaEstado',json2));
		json2:=put_json(json2,'FLAG_ARM','SI');
	else
		json2:=put_json(json2,'FLAG_ARM','NO');
	end if;
	json2:=logjson(json2,'FLAG_ARM='||flag_arm::varchar||' FLAG_NAR='||flag_nar::varchar||' FLAG_RECLAMO='||flag_reclamo::varchar);
	json2:=put_json(json2,'flag_tx_buscar','SI');
	json2:=put_json(json2,'rutDte',trim(replace(split_part(get_json('EMISOR',json2),'-',1),'.','')));
	json2:=put_json(json2,'tipoDte',get_json('TIPO',json2));
	json2:=put_json(json2,'folioDte',replace(get_json('FOLIO',json2),'.',''));

	json2:=put_json(json2,'__FLAG_INFORMA_SII__',get_json('FLAG_RECLAMO',json2));
	--Vamos por parte
	if flag_reclamo then
		json2:=put_json(json2,'MENSAJE_16210','SII: ');
		json2:=put_json(json2,'__SECUENCIAOK__','16201');
		json2:=put_json(json2,'__FLAG_PUB_10K__','SI');
		json2:=logjson(json2,'Ejecuta Reclamo');
		return json2;
	end if;
		
	
	if flag_nar then
		json2:=put_json(json2,'__SECUENCIAOK__','12796');
		json2:=put_json(json2,'__FLAG_PUB_10K__','SI');
		if get_json('MENSAJE_16210',json2)='' then
			json2:=put_json(json2,'MENSAJE_16210','NAR: ');
		else	
			json2:=put_json(json2,'MENSAJE_16210',get_json('MENSAJE_16210',json2)||' -- NAR:');
		end if;
		json2:=put_json(json2,'FLAG_NAR','NO');		
		json2:=logjson(json2,'Ejecuta NAR');
		return json2;
	end if;	

	if flag_arm then
		json2:=put_json(json2,'__SECUENCIAOK__','12767');
		json2:=put_json(json2,'__FLAG_PUB_10K__','SI');
		if get_json('MENSAJE_16210',json2)='' then
			json2:=put_json(json2,'MENSAJE_16210','ARM: ');
		else
			json2:=put_json(json2,'MENSAJE_16210',get_json('MENSAJE_16210',json2)||' -- ARM:');
		end if;
		json2:=put_json(json2,'FLAG_ARM','NO');
		json2:=logjson(json2,'Ejecuta ARM');
		return json2;
	end if;	
	json2:=define_secuencia_16210(json2,'BORRAR');
	return response_requests_6000('2','Acción no permitida','',json2);
END;
$$ LANGUAGE plpgsql;


