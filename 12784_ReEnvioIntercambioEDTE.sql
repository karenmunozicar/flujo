--Publica documento
delete from isys_querys_tx where llave='12784';

--Llamamos a Escribir Directo
insert into isys_querys_tx values ('12784',40,1,1,'select proc_prepara_grabacion_edte_12784(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('12784',50,1,3,'Llamada a Escribir en EDTE',8016,0,0,0,0,60,60);
insert into isys_querys_tx values ('12784',60,1,1,'select proc_respuesta_edte_12784(''$$__XMLCOMPLETO__$$'') as __xml__',0,0,0,1,1,-1,0);

CREATE or replace FUNCTION proc_prepara_grabacion_edte_12784(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
        xml2    varchar;
    data1       varchar;
    file1       varchar;
    sts         integer;
    host1       varchar;
    header1     varchar;
   largo1	integer;
    pos_final1 integer;	
    pos_inicial1 integer;
    dominio1 varchar;
fecha1	varchar;
directorio1 varchar;
tabla_traza1	varchar;
uri1	varchar;
stTraza	traza.traza%ROWTYPE;
	id1	varchar;
    
BEGIN
    xml2:=xml1; 

    xml2:=put_campo(xml2,'__SECUENCIAOK__','0');

    uri1:=get_campo('URI_IN',xml2);
    --20150224 FAY Si no viene URI no se puede publicar
    if (length(uri1)=0) then
	xml2 := logapp(xml2,'No viene URI_IN, no se puede publicar');
        xml2 := put_campo(xml2,'__EDTE_REENVIO_INTER_OK__','NO');
	return xml2;	
    end if;


    --xml2:=put_context(xml2,'CONTEXTO_ALMACEN');
    xml2 := put_campo(xml2,'TX','8016'); 

    file1:=replace(replace(replace(replace(uri1,':','%3A'),'/','%2F'),'?','%3F'),'=','%3D');
    xml2:=put_campo(xml2,'ALMACEN','/opt/acepta/enviodte/work/inter/dte/escribiendo_motor/'||file1);
    xml2:=logapp(xml2,'REENVIO INTER: '||get_campo('ALMACEN',xml2));

    xml2:=put_campo(xml2,'SCRIPT_EDTE','/bin/mv /opt/acepta/enviodte/work/inter/dte/escribiendo_motor/'||file1||' /opt/acepta/enviodte/work/inter/dte/pendiente/'||file1);
    --xml2:=put_campo(xml2,'SCRIPT_EDTE','echo 1');

    xml2:=logapp(xml2,'REENVIO INTER Script:'||get_campo('SCRIPT_EDTE',xml2));
   --Si es CGE
   xml2:=put_campo(xml2,'RUT_CGE',get_campo('RUT_EMISOR',xml2));
   xml2:=verifica_evento_cge(xml2);
   if (get_campo('EVENTO_CGE',xml2)='SI') then
        xml2:=put_campo(xml2,'__IP_CONEXION_CLIENTE__','172.16.10.181');
        --xml2 := put_campo(xml2,'__SECUENCIAOK__','35');
   else
        xml2:=put_campo(xml2,'__IP_CONEXION_CLIENTE__',get_campo('__IP_CONEXION_CLIENTE__',get_parametros_motor('','EDTE')));
        --xml2 := put_campo(xml2,'__SECUENCIAOK__','30');
   end if;
    xml2:=logapp(xml2,'REENVIO INTER: Envia Data Directo '||get_campo('__IP_CONEXION_CLIENTE__',xml2)||' '||uri1);
    xml2:=put_campo(xml2,'__SECUENCIAOK__','50');
    xml2 := put_campo(xml2,'_STS_FILE_','');
    return xml2;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION proc_respuesta_edte_12784(varchar) RETURNS varchar AS $$
DECLARE
    xml1        alias for $1;
    xml2        varchar;
        data1   varchar;
	sts1	varchar;
	publicado1	varchar;
	xml3	varchar;
	cola1  bigint;
	nombre_tabla1   varchar;
	        uri1    varchar;
        rut1    varchar;
	        tx1     varchar;
	id1	varchar;


BEGIN
        --Cambio la respuesta de cuadratura por la respuesta original
        xml2:=xml1;
       	xml2 := put_campo(xml2,'__EDTE_REENVIO_INTER_OK__','NO');
	sts1:=get_campo('_STS_FILE_',xml2);
	if (sts1='FILE_YA_EXISTE') then
		xml2 := logapp(xml2,'REENVIO INTER:File ya existe en EDTE');	
        	xml2 := put_campo(xml2,'__EDTE_REENVIO_INTER_OK__','SI');
	elsif (sts1='OK') then
                xml2 := logapp(xml2,'EDTE:OK Escritos '||get_campo('_STS_FILE_BYTES_WRITTEN_',xml2)||' ContentLength:'||get_campo('CONTENT_LENGTH',xml2)||' Largo Data:'||get_campo('LEN_INPUT_CUSTODIUM',xml2));
        	xml2 := put_campo(xml2,'__EDTE_REENVIO_INTER_OK__','SI');
	else
                xml2 := logapp(xml2,'REENVIO INTER:Falla EDTE Directo '||get_campo('_STS_FILE_',xml2));
        	xml2 := put_campo(xml2,'__EDTE_REENVIO_INTER_OK__','NO');
        end if;
        xml2 := put_campo(xml2,'__SECUENCIAOK__','0');
        xml2 := put_campo(xml2,'_STS_FILE_','');

        return xml2;
END;
$$ LANGUAGE plpgsql;

