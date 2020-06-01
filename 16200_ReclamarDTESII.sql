--Reclamar DTE SII
delete from isys_querys_tx where llave='16200';

insert into isys_querys_tx values ('16200',10,1,1,'select reclamar_sii_16200(''$$__JSONCOMPLETO__$$'') as __json__',0,0,0,1,1,-1,0);
insert into isys_querys_tx values ('16200',20,1,8,'Flujo Reclamo',16201,0,0,1,1,0,0);

CREATE or replace FUNCTION reclamar_sii_16200(json) RETURNS json AS $$
DECLARE
        json1   alias for $1;
        json2   json;

	v_data	varchar;
BEGIN
        json2:=json1;

        json2:=logjson(json2,'URI_RECLAMO='||get_json('URI_IN',json2));
        --Reclamo a ser enviado
        v_data:=get_json('INPUT',json2);
	
	json2:=put_json(json2,'RUT_EMISOR',split_part(get_xml_hex1('RUTEmisor',v_data),'-',1));
	json2:=put_json(json2,'FOLIO',get_xml_hex1('Folio',v_data));
	json2:=put_json(json2,'TIPO_DTE',get_xml_hex1('TipoDTE',v_data));
	json2:=put_json(json2,'RUT_RECEPTOR',split_part(get_xml_hex1('RUTRecep',v_data),'-',1));
	
	--2019-05-07 FAY-DAO se agregan fecha emision y monto para agregar al dte_pendientes en caso de que se acepte el reclamo
	json2:=put_json(json2,'FECHA_EMISION',get_xml_hex1('FchEmis',v_data));
	json2:=put_json(json2,'MONTO_TOTAL',get_xml_hex1('MntTotal',v_data));

	if(is_number(get_xml_hex1('EstadoDTE',v_data))) then
                if(get_xml_hex1('EstadoDTE',v_data)='0') then
                        json2:=put_json(json2,'EVENTO_RECLAMO','ACD');
                elsif(get_xml_hex1('EstadoDTE',v_data)='1') then
                        json2:=put_json(json2,'EVENTO_RECLAMO','RCD');
                elsif(get_xml_hex1('EstadoDTE',v_data)='2') then
                        json2:=put_json(json2,'EVENTO_RECLAMO','ERM');
                elsif(get_xml_hex1('EstadoDTE',v_data)='3') then
                        json2:=put_json(json2,'EVENTO_RECLAMO','RFP');
                elsif(get_xml_hex1('EstadoDTE',v_data)='4') then
                        json2:=put_json(json2,'EVENTO_RECLAMO','RFT');
                end if;
        else
                json2:=put_json(json2,'EVENTO_RECLAMO',get_xml_hex1('EstadoDTE',v_data));
        end if;

	--json2:=put_json(json2,'EVENTO_RECLAMO',get_xml_hex1('EstadoDTE',v_data));
	json2:=put_json(json2,'__SECUENCIAOK__','20');

        return json2;
END;
$$ LANGUAGE plpgsql;

