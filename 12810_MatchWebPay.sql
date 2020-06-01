delete from isys_querys_tx where llave='12810';

--Va a la base de webpay
insert into isys_querys_tx values ('12810',10,21,1,'select array_agg(sql) as pagos from (SELECT estadocompra,ordencompra from compra where idsesion=''$$ID$$''::integer) sql',0,0,0,1,1,20,20);

insert into isys_querys_tx values ('12810',20,9,1,'select match_transbank_session_6000_v2(''$$__JSONCOMPLETO__$$''::json) as __json__',0,0,0,1,1,0,0);



