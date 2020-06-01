--Publica documento
delete from isys_querys_tx where llave='112702';
insert into isys_querys_tx values ('112702',10,1,8,'Llamada Publica EDTE',12702,0,0,0,0,20,20);
insert into isys_querys_tx values ('112702',20,1,1,'select sp_procesa_respuesta_cola_motor_original(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,0,0);

