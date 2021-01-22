delete from isys_querys_tx where llave='80103';

--Primero que hacemos el publicar DTE
insert into isys_querys_tx values ('80103',10,1,8,'Publica DTE',1127043,0,0,0,0,20,20);

-- Prepara llamada al AML
--20201027 Se cambia la secuencia de error a las 1005 para que si falla no envie el mandato
insert into isys_querys_tx values ('80103',20,9,1,'select proc_procesa_input_dte_8010(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,1005);

--Reglas de Validacion Base ACM
insert into isys_querys_tx values ('80103',600,7,1,'select reglas.validacion_lista(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
--Reglas de BASE_1_LOCAL
insert into isys_querys_tx values ('80103',610,1,1,'select reglas.validacion_lista(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

--20201027 Se cambia la secuencia de error a las 1005 para que si falla no envie el mandato
insert into isys_querys_tx values ('80103',28,9,1,'select proc_procesa_input_dte_8010_parte2(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,1005);

--Borra o Actualiza el contenido de la cola API MOTOR
insert into isys_querys_tx values ('80103',1000,9,1,'select sp_procesa_respuesta_cola_motor_original(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,0,0);
--20201027 Secuencia para aumentar reintentos en caso de error
insert into isys_querys_tx values ('80103',1005,19,1,'select sp_procesa_respuesta_cola_motor(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,0,0);
--Ejecuta la respuesta para borrar y envia el mandato en el caso de las boletas
insert into isys_querys_tx values ('80103',1010,19,1,'select proc_verifica_fin_dte88_8010(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,0,0);

--Llamada al Flujo de Publicacion
insert into isys_querys_tx values ('80103',100,1,8,'Llamada Publica DTE',112704,0,0,0,0,1000,1000);

--Llamada al Flujo de Escritura Directa al EDTE para Mandatos
insert into isys_querys_tx values ('80103',110,1,8,'Llamada Publica Mandato  EDTE',12771,0,0,0,0,120,120);
--Llamada a Grabar en Respaldo NINA
--insert into isys_querys_tx values ('80103',115,1,8,'Llamada Publica Respaldo',12713,0,0,0,0,120,120);
--Llamada al Flujo de Escritura Directa al EDTE
insert into isys_querys_tx values ('80103',120,1,8,'Llamada Publica EDTE',12702,0,0,0,0,40,40);

--Para envio de mandatos de boletas
insert into isys_querys_tx values ('80103',1600,1,8,'Llamada Publica Mandato  EDTE',12771,0,0,0,0,40,40);
--Envia Boletas con mandato
insert into isys_querys_tx values ('80103',1610,1,8,'Llamada Publica Mandato  EDTE',12771,0,0,0,0,1010,1010);

--EnvioBoletaSII-
insert into isys_querys_tx values ('80103',1620,19,1,'select encola_envio_boleta_sii_8010(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,1010);
insert into isys_querys_tx values ('80103',1630,1,2,'Llamada MS EnvioBoletaSII',4013,300,101,0,0,1631,1631);
insert into isys_querys_tx values ('80103',1631,1,1,'select valida_respuesta_envio_boleta_sii_8010(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,1010);
insert into isys_querys_tx values ('80103',1632,19,1,'select valida_respuesta_envio_boleta_sii_error_8010(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,1010);

--Llamada al AML por el Apache
--insert into isys_querys_tx values ('80103',30,1,2,'Llamada al AML',4001,100,101,0,0,40,40);
--Llamada al AML directo SGCI
insert into isys_querys_tx values ('80103',30,1,2,'Llamada directo al AML',14000,102,101,0,0,40,40);
insert into isys_querys_tx values ('80103',31,1,2,'Llamada directo al AML',14001,102,101,0,0,40,40);
insert into isys_querys_tx values ('80103',32,1,2,'Llamada directo al AML',14002,102,101,0,0,40,40);
insert into isys_querys_tx values ('80103',33,1,2,'Llamada directo al AML',14003,102,101,0,0,40,40);
insert into isys_querys_tx values ('80103',34,1,2,'Llamada directo al AML',14004,102,101,0,0,40,40);
insert into isys_querys_tx values ('80103',35,1,2,'Llamada directo al AML',14005,102,101,0,0,40,40);
insert into isys_querys_tx values ('80103',36,1,2,'Llamada directo al AML',14006,102,101,0,0,40,40);
insert into isys_querys_tx values ('80103',37,1,2,'Llamada directo al AML',14007,102,101,0,0,40,40);
insert into isys_querys_tx values ('80103',38,1,2,'Llamada directo al AML',14008,102,101,0,0,40,40);
--AML CGE
insert into isys_querys_tx values ('80103',39,1,2,'Llamada directo al AML CGE',14009,102,101,0,0,40,40);
--Respuesta del AML
insert into isys_querys_tx values ('80103',40,9,1,'select proc_procesa_respuesta_dte_8010(''$$__XMLCOMPLETO__$$'') as __xml__ ',0,0,0,1,1,-1,0);

--Flujo de Mandato de Boletas y DTE
--Va directo al 1000 para verificar si le fue bien en el flujo y se borra de la cola
insert into isys_querys_tx values ('80103',150,1,8,'Flujo Mandato Boletas y DTEs',12712,0,0,0,0,1000,1000);

--Flujo de Mensajes Basura con y sin URI
insert into isys_querys_tx values ('80103',500,1,8,'Flujo 9999 Basura',9999,0,0,1,1,1010,1010);
insert into isys_querys_tx values ('80103',510,1,8,'Flujo 9999 Basura',9999,0,0,1,1,1010,1010);


--Lee tabla webdte.boletas de base 
--insert into isys_querys_tx values ('80103',200,3,1,'select * from webdte.boletas where rut=split_part(''$$RUT_EMISOR$$'',''-'',1) and periodo=(substring(''$$FECHA_EMISION$$'',1,4)||substring(''$$FECHA_EMISION$$'',6,2))::numeric order by periodo asc',0,0,0,1,1,250,250);

--Acumula Boletas webiecv
insert into isys_querys_tx values ('80103',200,3,1,'select proc_graba_webiecv_boletas_8010(''$$__XMLCOMPLETO__$$'') as respuesta_boleta',0,0,0,1,1,210,210);
--insert into isys_querys_tx values ('80103',210,1,1,'select proc_procesa_respuesta_boleta_8010(''$$__XMLCOMPLETO__$$'') as __xml__ ',0,0,0,1,1,-1,0);


--Flujo de validacion de certificados X509
insert into isys_querys_tx values ('80103',250,1,8,'Flujo Validacion Certificado x509',12729,0,0,1,1,0,0);

--Flujo de los CA4ARM
insert into isys_querys_tx values ('80103',1500,1,8,'Flujo ARM',12718,0,0,1,1,0,0);
--insert into isys_querys_tx values ('80103',1501,1,8,'Flujo ARM',112718,0,0,1,1,0,0);
--Flujo de los CA4RESP
--insert into isys_querys_tx values ('80103',1510,1,8,'Flujo RESP',12727,0,0,1,1,0,0);
insert into isys_querys_tx values ('80103',1510,1,8,'Flujo RESP',12779,0,0,1,1,0,0);
--insert into isys_querys_tx values ('80103',1511,1,8,'Flujo RESP',112779,0,0,1,1,0,0);
--Flujo de los CA4AEC
--insert into isys_querys_tx values ('80103',1520,1,8,'Flujo AEC',12728,0,0,1,1,0,0);
insert into isys_querys_tx values ('80103',1520,1,8,'Flujo AEC',12786,0,0,1,1,0,0);
--Flujo de los CA4REC Recibidos
insert into isys_querys_tx values ('80103',1530,1,8,'Flujo Recibidos',12703,0,0,1,1,0,0);

--Flujo de los CA4SAR Reclamos Recibidos
insert into isys_querys_tx values ('80103',1540,1,8,'Flujo Reclamos Recibidos',16200,0,0,1,1,1010,1010);

--Flujo de los CA4LIB, igual que el de WEBIECV
insert into isys_querys_tx values ('80103',700,1,8,'Flujo CA4LIB',12788,0,0,1,1,0,0);
--Flujo de los CA4RCF, igual que el de WEBIECV
insert into isys_querys_tx values ('80103',710,1,8,'Flujo CA4RCF',12802,0,0,1,1,0,0);

