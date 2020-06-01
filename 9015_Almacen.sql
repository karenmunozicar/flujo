delete from isys_querys_tx where llave='9015';

-- Prepara llamada al AML
insert into isys_querys_tx values ('9015',20,1,4,'$$ALMACEN$$',0,100,0,1,1,30,30);
--Borra un campo
insert into isys_querys_tx values ('9015',30,1,15,'INPUT',0,0,0,0,0,0,0);
