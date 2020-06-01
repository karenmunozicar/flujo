delete from isys_querys_tx where llave='6007';
insert into isys_querys_tx values ('6007',10,9,1,'select telefonica_6007(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);
--Llamada a Script Generico
insert into isys_querys_tx values ('6007',20,1,10,'$$SCRIPT$$',0,0,0,1,1,0,0);
--Llama Servicio Generico
insert into isys_querys_tx values ('6007',30,1,2,'Servicio HTTP Generico',4013,100,101,0,0,30,30);

CREATE or REPLACE  FUNCTION telefonica_6007(json) RETURNS json AS $$
DECLARE
    
    json1               alias for $1;
    json2               json;
    pg_context          TEXT;
    rut_empaux		varchar;
    rut_repaux		varchar;
    rut_emp1		varchar;
    razon1		varchar;
    giro1		varchar;
    direccion1		varchar;
    comuna1		varchar;
    ciudad1		varchar;
    rut_rep1		varchar;
    nombre_rep1		varchar;                      	
    email_rep1		varchar;
    tel_rep1		varchar;
    tipo1		varchar;
    producto_origen1    varchar;
    desc_producto1	varchar;
    origen1 		varchar;      
    id_producto1	varchar;
    id_transaccion1	varchar;
    datosUsuario	RECORD;
    v_codigo_descuento  varchar;
    datosFlujo          json;
    idsolicitudf	varchar;
    md5hash1	        varchar;
    SecuenciaNextVal    integer;
    correlativo		integer;
    datosFlujo2     	json;
    cod_producto1	varchar;
    fin_ciclo1		varchar;
    accion1		varchar;
    tipoCodigo1		varchar;
    areaFonoModificado1 varchar;
    err_msg	    	varchar:='';
    json_contenedor	json;
    json_resp1		json;
    respuesta1          varchar;
    estado1             varchar;
    cod_accion1		integer;	   
    id_prospecto_telefonica1 integer;
    flag_contribuyente1       integer;
    dataContribuyente    RECORD;
    uri_logo1		varchar:='http://escritorio_cert.acepta.com/include/img/telefonica_sidebar_logo.png';
BEGIN

     json2:=json1;
     json2:=put_json(json2,'__SECUENCIAOK__','0');
     json2:=put_json(json2,'__FLUJO_ENTRADA__','6007');
     json2:=logjson(json2,'JSON 6007 INPUT='||json2);
     json_contenedor:='{}';  
     json_resp1:='{}';  

     -- obtencion de datos
     id_transaccion1:=get_json('idNotify',json2);
     tipo1:=get_json('tipo',json2);
     accion1:=get_json('accion', json2);
     tipoCodigo1:=get_json('tipoCodigo', json2);   
     rut_empaux:=get_json('rut',json2);
     razon1:=get_json('nombre',json2);
     giro1:=get_json('giro',json2);
     fin_ciclo1:=get_json('fin_ciclo', json2);
     direccion1:=get_json('direccion',json2);
     comuna1:=get_json('comuna',json2);
     ciudad1:=get_json('ciudad',json2);
     rut_repaux:=get_json('rutRL',json2);
     nombre_rep1:=get_json('nombreRL',json2);                      	
     email_rep1:=get_json('emailCliente',json2);
     tel_rep1:=get_json('fonoContacto',json2);   
     producto_origen1:=get_json('producto',json2);
     cod_producto1:=get_json('codigoProductoTercero',json2);
     desc_producto1:=get_json('descProducto',json2);	
     origen1:=get_json('origen', json2);
     areaFonoModificado1:=get_json('areaFonoModificado',json2);

     -- validacion de datos comunes para todos los eventos

     if (length(accion1)=0) then
            err_msg:='El campo accion no existente';
            RAISE EXCEPTION SQLSTATE '77777' using message = err_msg;
     elsif((accion1!='alta') and (accion1!='modificacion') and (accion1!='baja') and (accion1!='corte') and (accion1!='reposicion')) then
            err_msg:='El campo accion tiene un valor no esperado'; 
            RAISE EXCEPTION SQLSTATE '77777' using message = err_msg;
   	 end if; 
       
     if (length(id_transaccion1)=0) then
            err_msg:='El campo idNotify no existente';
            RAISE EXCEPTION SQLSTATE '77777' using message = err_msg;
     end if;
     
     if (length(tipo1)=0) then
            err_msg:='El campo tipo no existente';
            RAISE EXCEPTION SQLSTATE '77777' using message = err_msg;
     end if;

     if (length(producto_origen1)=0) then
             err_msg:='El campo producto no existente';
             RAISE EXCEPTION SQLSTATE '77777' using message = err_msg;
         elsif(is_number(producto_origen1) is false) then
             err_msg:='El valor para el campo producto no numerico';
             RAISE EXCEPTION SQLSTATE '77777' using message = err_msg;
     end if;
 
     if (length(cod_producto1)=0) then
            err_msg:='El campo codigoProductoTercero no existente';
            RAISE EXCEPTION SQLSTATE '77777' using message = err_msg;
     	elsif(exists(select 1 from planes_10k where cod_plan = cod_producto1) is false) then
           	err_msg:='El valor del campo codigoProductoTercero no es un plan valido';
            RAISE EXCEPTION SQLSTATE '77777' using message = err_msg;
     end if;

     if (length(origen1)=0) then
            err_msg:='El campo origen no existente';
            RAISE EXCEPTION SQLSTATE '77777' using message = err_msg;
     end if;

    -- validacion de datos para la accion modificacion

    if(accion1='modificacion') then
		if (length(areaFonoModificado1)=0) then
            err_msg:='El campo areaFonoModificacion no existente';
            RAISE EXCEPTION SQLSTATE '77777' using message = err_msg;
        end if;
    end if;

    -- validacion de datos para la accion alta

    if(accion1='alta') then
	
       if (length(razon1)=0) then
    		err_msg:='El campo nombre no existente';
            RAISE EXCEPTION SQLSTATE '77777' using message = err_msg;
       end if; 

       if (length(giro1)=0) then
	   	    err_msg:='El campo giro no existente';
            RAISE EXCEPTION SQLSTATE '77777' using message = err_msg;
	   end if; 

       if (length(direccion1)=0) then
    		err_msg:='El campo direccion no existente';
            RAISE EXCEPTION SQLSTATE '77777' using message = err_msg;           
       end if; 

       if (length(comuna1)=0) then
	   	   err_msg:='El campo comuna no existente';
           RAISE EXCEPTION SQLSTATE '77777' using message = err_msg;
	   end if; 

       if (length(ciudad1)=0) then
    		err_msg:='El campo ciudad no existente';
            RAISE EXCEPTION SQLSTATE '77777' using message = err_msg;
       end if; 

       if (length(nombre_rep1)=0) then
    		err_msg:='El campo nombreRL no existente';
            RAISE EXCEPTION SQLSTATE '77777' using message = err_msg;
       end if; 

       if (length(tel_rep1)=0) then
    		err_msg:='El campo fonoContacto no existente';
            RAISE EXCEPTION SQLSTATE '77777' using message = err_msg;
       end if; 

 	   if (length(tipoCodigo1)=0) then
            err_msg:='El campo tipoCodigo no existente';
            RAISE EXCEPTION SQLSTATE '77777' using message = err_msg;
       end if;
           
      if (length(fin_ciclo1)=0) then
            err_msg:='El campo fin_ciclo no existente';
            RAISE EXCEPTION SQLSTATE '77777' using message = err_msg;
        elsif(is_number(fin_ciclo1) is false) then
           	err_msg:='El valor para el campo fin_ciclo es invalido';
            RAISE EXCEPTION SQLSTATE '77777' using message = err_msg;
       end if;

       if (length(email_rep1)=0) then
        	err_msg:='El campo emailCliente no existente';
            RAISE EXCEPTION SQLSTATE '77777' using message = err_msg;
	    elsif(valida_email(email_rep1) is false) then
		    err_msg:='El campo emailCliente es invalido';
            RAISE EXCEPTION SQLSTATE '77777' using message = err_msg;
       end if; 

      if (length(rut_repaux)=0) then
    		err_msg:='El campo rutRL no existente';
            RAISE EXCEPTION SQLSTATE '77777' using message = err_msg;
      else  
	    	 rut_rep1:= substring(replace(rut_repaux, '-', ''),1, (length(replace(rut_repaux, '-', ''))-1));
		     if(modulo11(rut_rep1) != substring(rut_repaux,length(rut_repaux), (length(rut_repaux)-1))) then
    			err_msg:='El valor para el campo rutRL no es valido';
                RAISE EXCEPTION SQLSTATE '77777' using message = err_msg;
	         end if;
      end if;

      if (length(rut_empaux)=0) then
    		err_msg:='El campo rut no existente';
            RAISE EXCEPTION SQLSTATE '77777' using message = err_msg;
        else 
	   	    rut_emp1:= substring(replace(rut_empaux, '-', ''),1, (length(replace(rut_empaux, '-', ''))-1));
		    if(modulo11(rut_emp1) != substring(rut_empaux,length(rut_empaux), (length(rut_empaux)-1))) then
    			err_msg:='El valor para el campo rut no valido';
                RAISE EXCEPTION SQLSTATE '77777' using message = err_msg;
	   	    end if;      
     end if;

     md5hash1:= md5(rut_rep1||rut_emp1||to_char(now(), 'YYYYMMDD'));

  end if;
   
  -- acciones

  -- crear nuevo
  if((accion1 = 'alta') and ( exists (select 1 from prospecto_telefonica where rut_empresa = rut_emp1) is false)) then
         
         -- creacion/actualizacion de usuario 
         SELECT aplicaciones INTO datosUsuario FROM user_10k WHERE rut_usuario = rut_rep1;
         if(datosUsuario.aplicaciones is null) then 
            estado1 := '1';
            INSERT INTO user_10k(rut_usuario, password, nombre, mail, fono, flag_cambioclave, aplicaciones, fecha_creacion) VALUES (rut_rep1, md5(rut_rep1), nombre_rep1, email_rep1, tel_rep1, 'SI', 'MARKETPLACE', NOW());
            if not found then
                    err_msg:='error cofigurando usario sobre '||id_transaccion1;
                    RAISE EXCEPTION SQLSTATE '77777' using message = err_msg;
            end if;
         elsif(datosUsuario.aplicaciones <> 'MARKETPLACE') then
                    UPDATE user_10k SET aplicaciones = datosUsuario.aplicaciones||'MARKETPLACE' WHERE rut_usuario = rut_rep1;
                    if not found then
                        err_msg:='error cofigurando usario sobre '||id_transaccion1;
                        RAISE EXCEPTION SQLSTATE '77777' using message = err_msg;
                    end if;
                estado1 := '2';
        end if;            

        if(exists(select 1 from menu_10k where rut_usuario=rut_rep1 and aplicacion = 'MARKETPLACE') is false) then       
            -- creacion del usuario en el menu10k
            INSERT INTO menu_10k (rut_usuario, perfil, empresa, def, aplicacion, fecha_ingreso) VALUES (rut_rep1, 'MarketPlace', '1','SI', 'MARKETPLACE', NOW());
            if not found then
                 err_msg:='error cofigurando menu sobre '||id_transaccion1;
                 RAISE EXCEPTION SQLSTATE '77777' using message = err_msg;
             end if;
        end if;

        INSERT INTO prospecto_telefonica (rut_empresa, razon_social, giro, direccion, comuna, ciudad, rut_rep, nombre_rep, email_rep, telefono_rep, producto_origen, cod_producto, id_transaccion, pwd_inicial, areafonomodificado, tipo, fin_ciclo,evento, tipo_codigo, estado_usuario, datos_peticion, origen, desc_producto ) VALUES (rut_emp1,razon1, giro1, direccion1,comuna1, ciudad1, rut_rep1, nombre_rep1, email_rep1, tel_rep1::numeric, producto_origen1, cod_producto1, id_transaccion1, rut_rep1, areafonomodificado1 ,tipo1, fin_ciclo1::smallint, accion1, tipoCodigo1, estado1, json2, origen1, desc_producto1 ) returning id into id_prospecto_telefonica1;
        if not found then
              err_msg:='error creando prospecto sobre '||id_transaccion1;
              RAISE EXCEPTION SQLSTATE '77777' using message = err_msg;
        end if;

       secuenciaNextVal = nextval('solicitudes_productos_id_transbank_seq'::regclass);
       json2:=logjson(json2,'Secuencia creada '||secuenciaNextVal::varchar);
       correlativo:= nextval('solicitudes_productos_correlativo_seq'::regclass);
       json2:=logjson(json2,'Correlativo creado '||correlativo::varchar);

       -- Se crea solicitud
       datosFlujo:=wf_crea_solicitud(cast('{
                                                  "rutCliente":"'||rut_emp1||'",
                                                  "rutUsuario":"'||rut_rep1||'",
                                                  "perfil":"MarketPlace",
                                                  "decision":"INICIO",
                                                  "aplicacion":"MARKETPLACE"
                                                }' as json));
	raise notice 'datosFlujo-------------------------> % ',datosFlujo;
       json2:=logjson(json2,'6007 alta respuesta datosFlujo '||datosFlujo::text);
    
       if (get_json('WF_CODIGO_RESPUESTA',datosFlujo)::integer = 1) then
           idsolicitudf:=get_json('id_solicitud',datosFlujo);
           -- creo codigo de convenio
	   -- asigno convenio de telefonica (90635000) a la empresa
           v_codigo_descuento:=crear_num_convenio(90635000,rut_emp1::integer);
           json2:=logjson(json2,'6007 alta convenio creado'||v_codigo_descuento::varchar);
           -- capturo identificador de plan segun su codigo
           id_producto1:=(SELECT id_producto FROM planes_10k WHERE cod_plan=cod_producto1 );

           select * from contribuyentes where rut_emisor = rut_emp1::integer into dataContribuyente;
           if found then 
               if(dataContribuyente.email <> 'FacturacionMIPYME@sii.cl') then
                    flag_contribuyente1:=1;
                  else
                    flag_contribuyente1:=0;
               end if; 
            else
                    flag_contribuyente1:=0;   
           end if;

           INSERT INTO empresa_certificacion_datos (id_solicitud, rutempresa, rut_representante,fecha_solicitud, flag_contribuyente) VALUES (idsolicitudf::bigint, (rut_emp1||'-'||modulo11(rut_emp1)), rut_rep1, now(), flag_contribuyente1);
           if not found then
                err_msg:='error insertando empresa_certificacion_datos sobre '||id_solicitud;
                RAISE EXCEPTION SQLSTATE '77777' using message = err_msg;
           end if;

           INSERT INTO solicitudes_productos (id_solicitud, rutEmpresa, rutUsuario ,id_transbank, correlativo, aplicacion,perfil,fecha_solicitud,fecha_ultimoestado, nom_raz_soc_fact, rut_fact, direccion_fact, comuna_fact, ciudad_fact, giro_fact, tel_fact, mail_fact, producto,estado,familia_prod,estado_pago,codigo_descuento, md5hash,host_canal,id_envio_form_cert) VALUES (idsolicitudf, (rut_emp1||'-'||modulo11(rut_emp1)), rut_rep1,secuenciaNextVal::integer,correlativo,'MARKETPLACE','MarketPlace',NOW(),NOW(),razon1,rut_emp1,direccion1,comuna1,ciudad1,giro1,tel_rep1,email_rep1,id_producto1,'Compra Por Convenio','Plan','PAGADO',v_codigo_descuento, md5hash1,'telefonica.acepta.com','');
           if not found then
                err_msg:='error cofigurando producto sobre '||id_transaccion1;
                RAISE EXCEPTION SQLSTATE '77777' using message = err_msg;
           end if;
	   -- avanza tarea a estado ok_pago_ok
           datosFlujo2:=wf_avanza_solicitud1(cast('{"id_solicitud":"'||idsolicitudf||'", "rutUsuario":"'||rut_rep1||'", "OBSERVACION":"Creacion prospecto telefonica'||rut_emp1||'", "tarea":"crear_solicitud_plan","cod_respuesta":"1"}' as json));
           json2:=logjson(json2,'6007 alata respuesta wf_avanza_solicitud1='||datosFlujo2::text); 
           -- actualizo codigo de solicitud en prospecto_telefonica
           UPDATE prospecto_telefonica SET id_solicitud_proc_cert = idsolicitudf::bigint WHERE id=id_prospecto_telefonica1;
           if not found then
                err_msg:='error actualizando prospecto sobre '||id_transaccion1;
                RAISE EXCEPTION SQLSTATE '77777' using message = err_msg;
            end if;
	    
	    insert into css_empresas (rutcliente,id_paleta_colores, header_superior, header_inferior, nombre_empresa_color, sidebar_normal_logo_uri,boton_color_fondo) VALUES (rut_emp1,'paleta_telefonica','#003245','#003245','#FF6633',uri_logo1,'#003245');
	    if not found then
                err_msg:='error creando paleta de colores  sobre '||rut_emp1;
                RAISE EXCEPTION SQLSTATE '77777' using message = err_msg;
            end if;	    

            json_resp1:=put_json(json_resp1,'CODIGO_RESPUESTA','1');
            json_resp1:=put_json(json_resp1,'MENSAJE_RESPUESTA','Peticion accion alta (nuevo prospecto) sobre '||id_transaccion1||'  Recibida OK');
            json_contenedor:=put_json(json_contenedor,'RESPUESTA',json_resp1);
            respuesta1:='Status: 200 OK' ||chr(10)||'Content-type: json'||chr(10)||'Content-Length: '||octet_length(json_contenedor::varchar)::varchar||chr(10)||chr(10)||json_contenedor;
            json2:=put_json(json2,'RESPUESTA',respuesta1);
            return json2;
        else
            err_msg:='la peticion accion alta (nuevo prospecto) sobre '||id_transaccion1||' no ha sido procesada';                    
            RAISE EXCEPTION SQLSTATE '77777' using message = err_msg;            
        end if;
    end if;

    if((accion1 = 'alta') and ( exists (select 1 from prospecto_telefonica where id_transaccion=id_transaccion1 and estado_operar = '6') is true)) then
     update prospecto_telefonica set evento = accion1, estado_operar='0' where id_transaccion=id_transaccion1 and estado_operar = '6';
     if found then
        json_resp1:=put_json(json_resp1,'CODIGO_RESPUESTA','1');
        json_resp1:=put_json(json_resp1,'MENSAJE_RESPUESTA','Peticion de accion alta (reinicio de proceso de certificacion) sobre '||id_transaccion1||'  Recibida OK');
        json_contenedor:=put_json(json_contenedor,'RESPUESTA',json_resp1);
        respuesta1:='Status: 200 OK' ||chr(10)||'Content-type: json'||chr(10)||'Content-Length: '||octet_length(json_contenedor::varchar)::varchar||chr(10)||chr(10)||json_contenedor;
        json2:=put_json(json2,'RESPUESTA',respuesta1);
        return json2;
      else
        err_msg:=' la accion alta (reinicio de proceso de certificacion) sobre '||id_transaccion1||' no produjo ningun resultado';
        RAISE EXCEPTION SQLSTATE '77777' using message = err_msg;            
     end if;
   end if;

   if((accion1 = 'alta') and ( exists (select 1 from prospecto_telefonica where rut_empresa = rut_emp1 and (estado_operar='0' or estado_operar='1')) is true)) then
       err_msg:='Peticion de accion alta sobre '||id_transaccion1||' ya fue procesada';
       RAISE EXCEPTION SQLSTATE '77777' using message = err_msg;            
   end if;

    if((accion1 = 'modificacion') and ( exists (select 1 from prospecto_telefonica where id_transaccion=id_transaccion1 and estado_operar = '1') is true)) then
         update prospecto_telefonica set cod_producto = cod_producto1, tipo = tipo1, evento=accion1, areafonomodificado=areaFonoModificado1, origen=origen1, producto_origen = producto_origen1, desc_producto = desc_producto1, estado_operar='3' where id_transaccion = id_transaccion1 and estado_operar = '1';
         if found then         
            json_resp1:=put_json(json_resp1,'CODIGO_RESPUESTA','1');
            json_resp1:=put_json(json_resp1,'MENSAJE_RESPUESTA','Peticion de accion modificacion sobre '||id_transaccion1||' Recibida OK');
            json_contenedor:=put_json(json_contenedor,'RESPUESTA',json_resp1);
            respuesta1:='Status: 200 OK' ||chr(10)||'Content-type: json'||chr(10)||'Content-Length: '||octet_length(json_contenedor::varchar)::varchar||chr(10)||chr(10)||json_contenedor;
            json2:=put_json(json2,'RESPUESTA',respuesta1);
            return json2;
          else
               err_msg:='la peticion de accion modificacion sobre '||id_transaccion1||' no produjo ningun resultado ';
               RAISE EXCEPTION SQLSTATE '77777' using message = err_msg;            
         end if;
   end if;

if( ( (accion1 = 'baja') or (accion1 = 'corte') or (accion1 = 'reposicion') ) and ( exists (select 1 from prospecto_telefonica where id_transaccion=id_transaccion1 and estado_operar = '1') is true)) then
     if(accion1 = 'baja') then   
            cod_accion1:= 5;   
        elsif(accion1 = 'corte') then
            cod_accion1:= 4;
        elsif(accion1 = 'reposicion') then
            cod_accion1:= 2;
     end if;
     update prospecto_telefonica set evento = accion1, estado_operar=cod_accion1 where id_transaccion=id_transaccion1 and estado_operar = '1';
     if found then
               json_resp1:=put_json(json_resp1,'CODIGO_RESPUESTA','1');
               json_resp1:=put_json(json_resp1,'MENSAJE_RESPUESTA','Peticion de accion '||accion1||' sobre '||id_transaccion1||' Recibida OK');
               json_contenedor:=put_json(json_contenedor,'RESPUESTA',json_resp1);
               respuesta1:='Status: 200 OK' ||chr(10)||'Content-type: json'||chr(10)||'Content-Length: '||octet_length(json_contenedor::varchar)::varchar||chr(10)||chr(10)||json_contenedor;
               json2:=put_json(json2,'RESPUESTA',respuesta1);                  
               return json2;
      else
               err_msg:=' la peticion de accion '|| accion1 ||' sobre '||id_transaccion1||' no produjo ningun resultado';
               RAISE EXCEPTION SQLSTATE '77777' using message = err_msg;            
       end if;
  end if;

  EXCEPTION
        WHEN SQLSTATE '77777' THEN
            json2:=json1;
            json2:=logjson(json2,err_msg);
            json_resp1:=put_json(json_resp1,'CODIGO_RESPUESTA','2');
            json_resp1:=put_json(json_resp1,'MENSAJE_RESPUESTA',err_msg);
            json_contenedor:=put_json(json_contenedor,'RESPUESTA',json_resp1);
            respuesta1:='Status: 200 OK' ||chr(10)||'Content-type: json'||chr(10)||'Content-Length: '||octet_length(json_contenedor::varchar)::varchar||chr(10)||chr(10)||json_contenedor;
            json2:=put_json(json2,'RESPUESTA',respuesta1);               
            return json2;
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS pg_context = PG_EXCEPTION_CONTEXT;
                    raise notice 'Falló la orden SQL: %. El error fue: %, contexto: %',SQLSTATE,SQLERRM,pg_context;
                    err_msg:='Falló la orden SQL: '||SQLSTATE||'. El error fue: '||SQLERRM;
                    json2:=logjson(json2,err_msg);
                    json_resp1:=put_json(json_resp1,'CODIGO_RESPUESTA','2');
                    json_resp1:=put_json(json_resp1,'MENSAJE_RESPUESTA',err_msg);
                    json_contenedor:=put_json(json_contenedor,'RESPUESTA',json_resp1);
                    respuesta1:='Status: 200 OK' ||chr(10)||'Content-type: json'||chr(10)||'Content-Length: '||octet_length(json_contenedor::varchar)::varchar||chr(10)||chr(10)||json_contenedor;
                    json2:=put_json(json2,'RESPUESTA',respuesta1);               
                    return json2;
END;
$$ LANGUAGE plpgsql;
