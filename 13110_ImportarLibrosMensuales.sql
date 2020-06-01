delete from isys_querys_tx where llave='13110';

insert into isys_querys_tx values ('13110',10,13,1,'select iecv.imp_data_dump_to_file(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('13110',20,13,1,'select iecv.imp_validar_formato_tipo_dato(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,60);

insert into isys_querys_tx values ('13110',30,13,1,'select iecv.imp_validar_negocio(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,70,60);

insert into isys_querys_tx values ('13110',31,13,1,'select iecv.imp_validar_obligatoriedad(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,70,60);

insert into isys_querys_tx values ('13110',32,13,1,'select iecv.imp_validar_fechas(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,70,60);

insert into isys_querys_tx values ('13110',33,13,1,'select iecv.imp_validar_cuadratura(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,70,60);

insert into isys_querys_tx values ('13110',60,13,1,'select iecv.imp_generar_archivo_error(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('13110',70,13,1,'select iecv.imp_finalizar_procedimiento(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,60);

