delete from isys_querys_tx where llave='12750';
--Obtiene el DTE Original con la entrada URI_IN
--insert into isys_querys_tx values ('12750',5,1,8,'GET XML desde Almacen',12705,0,0,1,1,10,10);
insert into isys_querys_tx values ('12750',10,1,1,'select proc_procesa_envio_estados_erp_12750(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

--WS COPEC 
--insert into isys_querys_tx values ('12750',12,1,2,'COPEC',231312,100,101,0,0,100,100);

--WS Generico por Nombre, Se envia HEX y se responde ASCII
insert into isys_querys_tx values ('12750',50,1,2,'GENERICO',4013,103,101,0,0,100,100);

--Generico Llamada de SCRIPT
insert into isys_querys_tx values ('12750',90,1,10,'$$SCRIPT$$',0,0,0,1,1,100,100);

insert into isys_querys_tx values ('12750',100,1,1,'select proc_procesa_respuesta_estados_erp_12750(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

--DROP FUNCTION proc_procesa_envio_estados_erp_12718(varchar);
CREATE or replace FUNCTION proc_procesa_envio_estados_erp_12750(varchar) RETURNS varchar AS $$
DECLARE
    	xml1		alias for $1;
        xml2    	varchar;
	stDND		traza.rut_dnd%ROWTYPE;
	t_traza1	varchar;
	rut1		varchar;
	evento1		varchar;
	data1		varchar;
	uri1		varchar;
	reintentos1	integer;
	fecha1		varchar;
	id1		bigint;
	id2		varchar;
	flag_reproceso  varchar;
	xml3		varchar;
BEGIN
	xml2:=xml1;
	--Viene desde el shell.
	rut1:=get_campo('RUT',xml2);
	evento1:=get_campo('EVENTO',xml2);
	uri1:=get_campo('URI',xml2);
	data1:=get_campo('DATA',xml2);
	id2:=get_campo('__ID_DTE__',xml2);	
	xml2:=put_campo(xml2,'__SECUENCIAOK__','0');
	if (is_number(id2)) then
		id1:=-1;
	else
		id1:=get_campo('ID',xml2)::bigint;
	end if;

	xml2 := logapp(xml2,'12750_Recibe_Eventos_ERP '||rut1||' - '||evento1||' - '||uri1);

	--Si no viene evento.
	if (length(data1)=0) then 
		xml2:=logapp(xml2,'No viene tag DATA desde el Shell');
		if (is_number(id2)) then
			 delete from cola_motor_eventos where id=id2::bigint;
		end if;
		return xml2;
	end if;

	--Leo procedimiento de entrada y salida desde rut_dnd
    	SELECT * into stDND from traza.rut_dnd where rut=rut1;
    	if found then
        	xml2:=put_campo(xml2,'FUNCION_OUT',stDND.sp_out_eventos_erp);
  	else
        	xml2:=logapp(xml2,'Rut no definido en tabla traza.rut_dnd RUT='||rut1);
        	return xml2;
    	end if;
	
	--Verifico si tiene fecha_despacho_erp el evento	
	/*t_traza1:=get_tabla_traza(uri1);
	xml2:=logapp(xml2,'12750_Tabla.Traza='||t_traza1);
	
	-- traza.traza_1408
	EXECUTE 'select coalesce(fecha_despacho_erp::varchar,''SIN_FECHA'') from '||t_traza1||' where  uri=$1 and evento=$2' using uri1,evento1 into fecha1;
	*/
	--Si es distinto, es porque tiene fecha y no se procesa.
	-- 2014-08-29 <> SIN_FECHA
	
	/*select 1 into flag_reproceso from eventos_x_enviar_reprocesos where uri=uri1;
	if not found then
		if (fecha1<>'SIN_FECHA') then
			xml2:=put_campo(xml2,'__SECUENCIAOK__','0');
			xml2:=logapp(xml2,'Evento ya marcado con envio a ERP');
			DELETE from eventos_x_enviar_erp where id=id1;
			if (is_number(id2)) then
				 delete from cola_motor_cuadratura where id=id2::bigint;
			end if;
			return xml2;
		end if;
	end if;
	*/
   	--Ejecuto funcion que envia un Evento por WebService directo al ERP del cliente
	--Verificamos si tiene funcion de sp_in_eventos
	if length(stDnd.sp_in_eventos_erp) > 0 then
		xml2:=logapp(xml2,'Ejecuta InEventos '||stDnd.sp_in_eventos_erp);
		EXECUTE 'SELECT ' || stDND.sp_in_eventos_erp || '(' || quote_literal(xml2) || ')' into xml2;

		--Si tiene secuencia OK=0 entonces aumento reintentos
		if (get_campo('__SECUENCIAOK__',xml2)='0') then
			update cola_motor_eventos set reintentos=reintentos+1 where id=id2::bigint;
   			xml2:=logapp(xml2,'Aumenta Reintentos por secuencia 0');
		end if;
	else
		xml2:=put_campo(xml2,'__SECUENCIAOK__','0');
		--Si no tiene funcion aumento reintentos
		if (is_number(id2)) then
			update cola_motor_eventos set reintentos=reintentos+1 where id=id2::bigint;
   			xml2:=logapp(xml2,'ERROR Configuracion: No Hay funcion para envio Eventos Rut:' || rut1);
		end if;
	end if;


   	return xml2;
END;
$$ LANGUAGE plpgsql;

--DROP FUNCTION proc_procesa_respuesta_estados_erp_12718(varchar);
CREATE or replace FUNCTION proc_procesa_respuesta_estados_erp_12750(varchar) RETURNS varchar AS $$
DECLARE
    	xml1    	alias for $1;
    	xml2		varchar;
	stEventosErp	eventos_x_enviar_erp%ROWTYPE; 
	rut1		varchar;
	evento1		varchar;
	uri1		varchar;
	
	funcion1	varchar;
	t_traza1	varchar;
	fecha1		timestamp;
	id1		bigint;	
	id2		varchar;
	comentario_evento_erp1 varchar;
	reintentos1	integer;
	stMail		aviso_mail%ROWTYPE;
	flag_reproceso  varchar;
	xml3	varchar;
	json1	json;
BEGIN
	xml2:=xml1;
	fecha1:=to_char(clock_timestamp(),'YYYY-MM-DD HH24MISS');
	rut1:=get_campo('RUT',xml2);
        evento1:=get_campo('EVENTO',xml2);
        uri1:=get_campo('URI_IN',xml2);
	id2:=get_campo('__ID_DTE__',xml2);
	if (is_number(id2)) then
		id1:=-1;
	else
		id1:=get_campo('ID',xml2)::bigint;
	end if;
        --data1:=get_campo('DATA',xml2);
    	
	--Procesamos la respuesta la respuesta
    	funcion1:=get_campo('FUNCION_OUT',xml2);
	--Verificamos si existe funcion de salida
	if length(funcion1) = 0 then
		if (is_number(id2)) then
			update cola_motor_eventos set reintentos=reintentos+1 where id=id2::bigint;
		end if;
		return xml2;
	end if;
	xml2:=logapp(xml2,'Ejecuta.Funcion.Out '||funcion1);
   	EXECUTE 'SELECT ' || funcion1 || '(' || quote_literal(xml2) || ')' into xml2;

	--xml2:=logapp(xml2,'Respuesta Evento ERP');
	--return xml2; 
	t_traza1:=get_tabla_traza(uri1);
	comentario_evento_erp1:=get_campo('COMENTARIO_EVENTO_ERP',xml2);
	xml3:=parametros_traza(t_traza1);
    	
	--Verificamos si fue exitoso el envio
    	xml2:=logapp(xml2,'Evento.ID='||id1||' Evento'||evento1||' Rut'||rut1);
	select 1 into flag_reproceso from eventos_x_enviar_reprocesos where uri=uri1;
        if found then
                comentario_evento_erp1:=get_campo('COMENTARIO_EVENTO_ERP',xml2) || chr(10) || 'Reprocesado  ' || to_char(clock_timestamp(),'YYYY-MM-DD HH24:MI:SS');
                xml2:=logapp(xml2,'Reproceso');
        end if;


xml2:=logapp(xml2,'RMERMERME --> '||get_campo('ENVIO_EVENTO_ERP',xml2) || t_traza1 || ' ' || uri1);


	if (get_campo('ENVIO_EVENTO_ERP',xml2)='OK') then
		--Tenemos que marcar todas las trazas que encontremos (99520000)
		--xml2:=logapp(xml2,'Tablas_de_Traza='||t_traza1||' Rut='||rut1);
		--RME y JSE 20160427 Se agrega fecha de ultimo intento en comentario traza
		comentario_evento_erp1:= 'Envio Exitoso EventoERP' || chr(10) || ' Hora Envio ' || to_char(now(),'yyyy-mm-dd HH24:mi:ss');
		
		if get_campo('TRAZA_REMOTO',xml3)='SI' then
			json1:=query_db_json(get_campo('__IP_CONEXION_CLIENTE__',xml3),get_campo('__IP_PORT_CLIENTE__',xml3)::integer,'update '||t_traza1||' set fecha_despacho_erp=now(),comentario_erp='||quote_literal(comentario_evento_erp1)||' where uri='''||uri1||''' and evento='''||evento1||'''');
			if get_json('STATUS',json1)<>'SIN_DATA' then
   				xml2:=logapp(xml2,'Falla Update traza remota');
				RAISE EXCEPTION 'Falla Update traza remota ' USING ERRCODE = 20000;
			end if;
		else
			EXECUTE 'update '||t_traza1||' set fecha_despacho_erp=now(),comentario_erp='||quote_literal(comentario_evento_erp1)||' where uri=$1 and evento=$2' using uri1,evento1;
		end if;
		
		INSERT 	INTO eventos_x_enviar_erp_historico (fecha,rut,data,evento,uri,reintentos,id,fecha_insercion) SELECT fecha,rut,data,evento,uri,reintentos,id,now() FROM eventos_x_enviar_erp WHERE id=id1;
		xml2:=logapp(xml2,'RespEventoERP. Recibe OK Cliente RUT='||rut1);
		DELETE from eventos_x_enviar_erp where id=id1;
		if (is_number(id2)) then
			--Inserto eventos en el historico
			INSERT  INTO eventos_x_enviar_erp_historico (fecha,rut,data,evento,uri,reintentos,id,fecha_insercion) select fecha,rut_emisor::integer,data,get_campo('EVENTO',xml2),uri,reintentos,id,now() from cola_motor_eventos where id=id2::bigint;

xml2:=logapp(xml2,'EVENTO ERP Id a borrar-->'||id2);

			 delete from cola_motor_eventos where id=id2::bigint;
  			 xml2:=logapp(xml2,'Borra Mensaje ERP Cola');
		end if;
    	else
		--RME y JSE 20160427 Se agrega fecha de ultimo intento en comentario traza
		comentario_evento_erp1:='Falla Envio Evento' || chr(10) || 'Ultimo reintento ' || to_char(now(),'yyyy-mm-dd HH24:mi:ss');
		EXECUTE 'update '||t_traza1||' set comentario_erp='||quote_literal(comentario_evento_erp1)||' where uri=$1 and evento=$2' using uri1,evento1;
		--Si no tengo respueta OK, Aumenta reintentos
		xml2:=logapp(xml2,'RespEventoERP. Rechaza Cliente RUT='||rut1||'.Se suben reintentos ID='||id1::varchar);
		UPDATE eventos_x_enviar_erp SET reintentos = reintentos + 1 where id=id1 RETURNING reintentos into reintentos1;
		if (is_number(id2)) then
			update cola_motor_eventos set reintentos=reintentos+1 where id=id2::bigint;
   			xml2:=logapp(xml2,'Aumenta Reintentos Mensaje ERP Cola');
		end if;
		if (reintentos1>=8) then
			if (is_number(id2)) then
				INSERT INTO eventos_x_enviar_erp_rechazos SELECT fecha::timestamp,rut1,get_campo('DATA',xml2),evento1,uri1,reintentos1,id2::bigint from cola_motor_eventos where id=id2::bigint;
				--Si ya envie mail no lo hago nuevamente
				select * into stMail from aviso_mail where rut=rut1 and strpos(asunto,'FALLA Servicio Automatico Envio Eventos ERP')>0;
				if not found then
					--Manda mail
					insert into aviso_mail (fecha,rut,mail,asunto,mensaje,mail_bcc,categoria) values (now(),rut1,'fernando.arancibia@acepta.com,ruben.munoz@acepta.com,ingrid.leyton@acepta.com,sistemas@acepta.com','FALLA Servicio Automatico Envio Eventos ERP  '||to_char(now(),'YYYY/MM/DD HH24:MI:SS'),'Evento Rechazado 8 veces por ERP.'||chr(10)||'__________________________________________________________'||chr(10)||'__________________________________________________________'||chr(10)||get_campo('__ID_COLA__',xml2)||';'||rut1||';'||evento1||';'||get_campo('URI',xml2)||';'||reintentos1::varchar||';'||get_campo('DATA',xml2)||chr(10),'','FALLA_ERP');
					 delete from cola_motor_eventos where id=id2::bigint;
					 delete from eventos_x_enviar_erp where id=id2::bigint;
  			 	         xml2:=logapp(xml2,'Borra Mensaje ERP Cola y eventos_x_enviar_erp');
				end if;
			end if;
		end if;
    	end if; 
    return xml2;
END;
$$ LANGUAGE plpgsql;


