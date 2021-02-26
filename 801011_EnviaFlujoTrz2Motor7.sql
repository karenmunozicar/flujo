delete from isys_querys_tx where llave='801011';

--Para forzar el REQUEST_URI
insert into isys_querys_tx values ('801011',5,1,14,'{"f":"INSERTA_JSON","p1":{"REQUEST_URI":"/request_uri/tx_dinamica","SCGI_REQUEST_URI":"/request_uri/tx_dinamica"}}',0,0,0,0,0,10,10);

--Primero que hacemos el publicar DTE
insert into isys_querys_tx values ('801011',10,1,19,'Envia a Procesar a Procesador TrazaAws',1801011,0,0,0,0,15,15);

--Log especifico
insert into isys_querys_tx values ('801011',15,1,16,'["TX","__COLA_MOTOR__","__ID_DTE__","__STS_ERROR_SOCKET__","__TIME_TX__","STATUS","__PROC_ACTIVOS__","CATEGORIA","SUB_CATEGORIA"]',0,0,0,1,1,0,0);

