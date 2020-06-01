delete from isys_querys_tx where llave='8016';

-- Prepara llamada al EDTE
insert into isys_querys_tx values ('8016',20,1,4,'$$ALMACEN$$',0,104,0,1,1,30,30);
insert into isys_querys_tx values ('8016',30,1,10,'$$SCRIPT_EDTE$$',0,104,0,1,1,0,0);
