delete from isys_querys_tx where llave='112703';
-- Prepara llamada al AML
--20210119 Para el EDTE vaya por el Api ACT
insert into isys_querys_tx values ('112703',10,8022,1,'select proc_traductor_fcgi_12701(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
--Los eventos que empiecen con ECM_XXXX y el LMA se van a actualizar a el sistema ECM
insert into isys_querys_tx values ('112703',15,30,1,'select actualiza_evento_ecm_12701(''$$__JSONCOMPLETO__$$'') as __JSON__',0,0,0,1,1,-1,0);
--Flujo consulta bitacora
insert into isys_querys_tx values ('112703',70,1,8,'Flujo Consulta Bitacora',7010,0,0,1,1,0,0);
--Actualizacion estado LCE en base LCE
--insert into isys_querys_tx values ('112703',90,16,1,'select lce_actualiza_estado(''$$URI_IN$$'',''$$EVENTO$$'') as __RESPUESTA_LCE__' ,0,0,0,1,1,91,91);
insert into isys_querys_tx values ('112703',90,16,1,'select lce.actualiza_estado_libro_lce(''$$URI_IN$$'',''$$EVENTO$$'') as __RESPUESTA_LCE__' ,0,0,0,1,1,91,91);
insert into isys_querys_tx values ('112703',91,1,1,'select Valida_lce_actualiza_estado(''$$__XMLCOMPLETO__$$'') as __XML__' ,0,0,0,1,1,-1,0);
--Traza CGE
insert into isys_querys_tx values ('112703',390,1,2,'Llamada a Traza CGE',8881,103,101,0,0,410,410);
--Traza Normal
insert into isys_querys_tx values ('112703',400,1,2,'Llamada a Traza',8880,103,101,0,0,410,410);
insert into isys_querys_tx values ('112703',401,1,2,'Llamada a Traza',9880,103,106,0,0,410,410);
insert into isys_querys_tx values ('112703',410,1,1,'select proc_respuesta_traza_12701(''$$__XMLCOMPLETO__$$'') as __XML__',0,0,0,1,1,-1,0);
--Flujo Control Basura
insert into isys_querys_tx values ('112703',500,1,8,'Flujo 9999 Basura',9999,0,0,1,1,100,100);
--Secuencia para WebIECV
insert into isys_querys_tx values ('112703',600,13,1,'select actualiza_estado_libro(''$$__JSONCOMPLETO__$$'') as __JSON__',0,0,0,1,1,610,610);
insert into isys_querys_tx values ('112703',610,1,1,'select procesa_respuesta_webiecv_12701(''$$__XMLCOMPLETO__$$'') as __XML__',0,0,0,1,1,-1,0);
--Parsea respuesta para FastCGI
insert into isys_querys_tx values ('112703',100,1,1,'select proc_respuesta_fcgi_12701(''$$__XMLCOMPLETO__$$'') as __XML_NUEVO__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('112703',700,1,8,'Flujo 7000 Nueva Cuadratura',7000,0,0,1,1,0,0);
insert into isys_querys_tx values ('112703',800,1,1,'select proc_test(''$$__XMLCOMPLETO__$$'') as __XML__',0,0,0,1,1,0,0);

