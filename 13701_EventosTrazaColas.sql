delete from isys_querys_tx where llave='13701';

--Se llama a la traza normal
insert into isys_querys_tx values ('13701',10,1,8,'Traza 12701',12701,0,0,1,1,20,20);

insert into isys_querys_tx values ('13701',20,19,1,'select sp_procesa_respuesta_cola_motor88_json(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,0,0);
