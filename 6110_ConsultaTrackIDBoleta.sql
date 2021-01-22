delete from isys_querys_tx where llave='6110';

insert into isys_querys_tx values ('6110',2,9,16,'LOG_JSON',0,0,0,1,1,5,5);
insert into isys_querys_tx values ('6110',5,19,1,'select control_flujo_80101(''$$__JSONCOMPLETO__["__PROC_ACTIVOS__","TX","REQUEST_URI","__ARGV__","__CATEGORIA_COLA__","__FLUJO_ACTUAL__"]$$''::json) as __json__',0,0,0,1,1,-1,10);

insert into isys_querys_tx values ('6110',10,19,1,'select consulta_trackid_boleta_6001_colas(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

--Llamada a un MicroServicio sonde la respuesta va a la base de colas
insert into isys_querys_tx values ('6110',23,1,2,'Microservicios 127.0.0.1',4013,300,101,0,0,26,26);
insert into isys_querys_tx values ('6110',22,1,2,'Microservicios 127.0.0.1',4013,300,101,0,0,26,26);

insert into isys_querys_tx values ('6110',26,9,16,'LOG_JSON',0,0,0,1,1,27,27);

insert into isys_querys_tx values ('6110',27,19,1,'select consulta_trackid_boleta_6001_resp_colas(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);


