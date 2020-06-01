delete from isys_querys_tx where llave='25106';

insert into isys_querys_tx values ('25106','10',44,1,'select arma_querys_25106(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

--Cuenta Redshift
insert into isys_querys_tx values ('25106','20',47,1,'$$QUERY_RS$$',0,0,0,9,1,30,30);
insert into isys_querys_tx values ('25106','30',44,1,'select revisa_resultado_count_rs_25106(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

--Armo Querys para sacar los codigos de la primera pagina
insert into isys_querys_tx values ('25106','60',44,1,'select arma_querys_coddocumento_25106(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

--CodDocumento Redshift
insert into isys_querys_tx values ('25106','70',47,1,'$$QUERY_RS$$',0,0,0,9,1,80,80);
insert into isys_querys_tx values ('25106','80',44,1,'select revisa_resultado_query_rs_25106(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

--Arma Query Final
insert into isys_querys_tx values ('25106','110',44,1,'select arma_query_final_25106(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);

--Base Normal
insert into isys_querys_tx values ('25106','120',44,1,'$$QUERY_DATA$$',0,0,0,9,1,130,130);
insert into isys_querys_tx values ('25106','130',44,1,'select arma_respuesta_25106(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,-1,0);


