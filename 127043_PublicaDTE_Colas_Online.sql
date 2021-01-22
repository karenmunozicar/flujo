--Publica documento
delete from isys_querys_tx where llave='1127043';

--Consultamos en la base de traza si el DTE ya esta publicado
insert into isys_querys_tx values ('1127043',10,9,1,'select proc_consulta_publicacion_112704(''__FLUJO_ACTUAL__[]=$$__FLUJO_ACTUAL__$$###REQUEST_METHOD[]=$$REQUEST_METHOD$$###HTTP_USER_AGENT[]=$$HTTP_USER_AGENT$$###QUERY_STRING[]=$$$QUERY_STRING$$###URI_IN[]=$$URI_IN$$###RUT_EMISOR[]=$$RUT_EMISOR$$###TIPO_DTE[]=$$TIPO_DTE$$###FOLIO[]=$$FOLIO$$###MONTO_TOTAL[]=$$MONTO_TOTAL$$###SCRIPT_NAME[]=$$SCRIPT_NAME$$###CONTENIDO[]=$$CONTENIDO$$###XML_FLAGS[]=$$XML_FLAGS$$###__FLAG_PUB_10K__[]=$$__FLAG_PUB_10K__$$###'') as __xml__',0,0,0,1,1,-1,0);

--20200110 proxy en Amazon
--Traza 2014
insert into isys_querys_tx values ('1127043',2014,38,1,'select proxy_traza_amazon_112704(''__FLUJO_ACTUAL__[]=$$__FLUJO_ACTUAL__$$###URI_IN[]=$$URI_IN$$###TABLA_TRAZA[]=$$TABLA_TRAZA$$###'') as __xml__',0,0,0,1,1,-1,0);
--Traza 2015
insert into isys_querys_tx values ('1127043',2015,37,1,'select proxy_traza_amazon_112704(''__FLUJO_ACTUAL__[]=$$__FLUJO_ACTUAL__$$###URI_IN[]=$$URI_IN$$###TABLA_TRAZA[]=$$TABLA_TRAZA$$###'') as __xml__',0,0,0,1,1,-1,0);
--Traza 2016
insert into isys_querys_tx values ('1127043',2016,36,1,'select proxy_traza_amazon_112704(''__FLUJO_ACTUAL__[]=$$__FLUJO_ACTUAL__$$###URI_IN[]=$$URI_IN$$###TABLA_TRAZA[]=$$TABLA_TRAZA$$###'') as __xml__',0,0,0,1,1,-1,0);
--Traza 2017
insert into isys_querys_tx values ('1127043',2017,33,1,'select proxy_traza_amazon_112704(''__FLUJO_ACTUAL__[]=$$__FLUJO_ACTUAL__$$###URI_IN[]=$$URI_IN$$###TABLA_TRAZA[]=$$TABLA_TRAZA$$###'') as __xml__',0,0,0,1,1,-1,0);
--Traza 2018
insert into isys_querys_tx values ('1127043',2018,46,1,'select proxy_traza_amazon_112704(''__FLUJO_ACTUAL__[]=$$__FLUJO_ACTUAL__$$###URI_IN[]=$$URI_IN$$###TABLA_TRAZA[]=$$TABLA_TRAZA$$###'') as __xml__',0,0,0,1,1,-1,0);
--Traza 2019
insert into isys_querys_tx values ('1127043',2019,49,1,'select proxy_traza_amazon_112704(''__FLUJO_ACTUAL__[]=$$__FLUJO_ACTUAL__$$###URI_IN[]=$$URI_IN$$###TABLA_TRAZA[]=$$TABLA_TRAZA$$###'') as __xml__',0,0,0,1,1,-1,0);
--Traza 2020
insert into isys_querys_tx values ('1127043',2020,50,1,'select proxy_traza_amazon_112704(''__FLUJO_ACTUAL__[]=$$__FLUJO_ACTUAL__$$###URI_IN[]=$$URI_IN$$###TABLA_TRAZA[]=$$TABLA_TRAZA$$###'') as __xml__',0,0,0,1,1,-1,0);

insert into isys_querys_tx values ('1127043',20,9,1,'select proc_consulta_publicacion_112704_2(''__FLUJO_ACTUAL__[]=$$__FLUJO_ACTUAL__$$###REQUEST_METHOD[]=$$REQUEST_METHOD$$###HTTP_USER_AGENT[]=$$HTTP_USER_AGENT$$###QUERY_STRING[]=$$$QUERY_STRING$$###URI_IN[]=$$URI_IN$$###RUT_EMISOR[]=$$RUT_EMISOR$$###TIPO_DTE[]=$$TIPO_DTE$$###FOLIO[]=$$FOLIO$$###MONTO_TOTAL[]=$$MONTO_TOTAL$$###SCRIPT_NAME[]=$$SCRIPT_NAME$$###CONTENIDO[]=$$CONTENIDO$$###XML_FLAGS[]=$$XML_FLAGS$$###__FLAG_PUB_10K__[]=$$__FLAG_PUB_10K__$$###'') as __xml__',0,0,0,1,1,-1,0);

--Sacamos el XML del DTE Emitido en caso de ser necesario
--insert into isys_querys_tx values ('1127043',20,1,8,'GET XML desde Almacen',12705,0,0,1,1,40,40);
--Ejecuta la Pre-Emision en Controller
insert into isys_querys_tx values ('1127043',30,9,1,'select pre_emision_controller_112704(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('1127043',35,1,8, 'Firma XML flujo 13795',13795,0,0,1,1,0,0);

--Llamamos a Escribir Directo
insert into isys_querys_tx values ('1127043',40,19,1,'select proc_prepara_graba_directo_almacen_colas_112704(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

--Llamada al flujo 8015 idioma motor (Tipo 7= Respuesta Motor)
insert into isys_querys_tx values ('1127043',56,1,2,'Llamada a Escribir en Almacen',9017,104,200,0,0,66,66);
--Se eejecuta en la base de colas
insert into isys_querys_tx values ('1127043',66,19,1,'select proc_respuesta_almacen_112704_3(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
--Se eejecuta en la base principal
insert into isys_querys_tx values ('1127043',70,9,1,'select graba_estado_publicacion_traza_112704(''__FLUJO_ACTUAL__[]=$$__FLUJO_ACTUAL__$$###$$XML3$$'') as __xml__',0,0,0,1,1,-1,0);

