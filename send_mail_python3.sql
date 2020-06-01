CREATE or REPLACE FUNCTION send_mail_python3(data text)
returns varchar
as $$
import smtplib
import cjson
import plpy
import requests
import urllib2

from email.MIMEMultipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.MIMEBase import MIMEBase
from email import Encoders
from email.mime.text import MIMEText
from lxml import etree
import StringIO
import cStringIO
import PyPDF2
import re
import os
import time
import datetime

pid=os.getpid()
def LOG(data):
        ts = time.time()
        llave="/var/log/postgresql/logapp/"+datetime.datetime.fromtimestamp(ts).strftime('%Y%m%d%H')+".LOG"
        f=open(llave,"ab+")
        f.write(datetime.datetime.fromtimestamp(ts).strftime('%Y%m%d %H:%M:%S')+" PID="+str(pid)+" "+data+"\n")
        f.close()


jout={}
j1=cjson.decode(data)

LOG(data)

uri=j1["uri"].replace("\/","/")
#Si viene con flag_data_xml=SI, viene el XML
if 'flag_data_xml' not in j1:
	#Buscamos el DTE en el almacen
	uri_depot=uri.replace("/v01/","/depot/")
	#Traigo el xml del almacen
	plpy.notice(uri_depot)
	xml_almacen=requests.get(uri_depot)
	if xml_almacen.status_code!=200:
		jout["status"]="FALLA"
		jout["mensaje"]="Falla leer Xml del almacen"
		return cjson.encode(jout)
	data_xml=xml_almacen.content
else:
	#si viene como parametro, lo convertimos desde hex
	data_xml=j1["INPUT_CUSTODIUM"].decode("hex")

msg = MIMEMultipart()
if 'subject_hex' not in j1:
	msg['Subject'] = j1["subject"]
else:
	msg['Subject'] = j1["subject_hex"].decode("hex")
msg['From'] = j1["from"]
#msg['To'] = j1["to"].replace(' ',',')
msg['To'] = j1["to"].replace(" ","; ")

LOG(j1["to"].replace(" ","; "))
LOG(j1["subject_hex"].decode("hex"))
LOG(j1["RAZON_SOCIAL_RECEPTOR"])

if 'RAZON_SOCIAL_RECEPTOR' in j1:
	index1 = data_xml.find('</RUTReceptor>')
	if index1>0:
		data_xml=data_xml[0:index1+14] + '<RznSocRecep>' +j1["RAZON_SOCIAL_RECEPTOR"] +'</RznSocRecep>' + data_xml[index1+14:]	
	else:
		data_xml=data_xml[0:index1+14] + '<RznSocRecep>Receptor Manual</RznSocRecep>' + data_xml[index1+14:]

LOG(data_xml)

if 'msg_id' in j1:
	msg['Message-ID'] = j1["msg_id"]
if 'return_path' in j1:
	msg['Return-Path'] = j1["return_path"]
	#Si viene return_path setea como sender para el servidor de correo
	sender=j1["return_path"]
else:
	sender=j1["from"]
if 'tipo_envio' not in j1:
	part1 = MIMEText(data_xml, 'plain')	
else:
	#Hace un match del xsl con el xml para enviar el mail
	if j1["tipo_envio"]=="XSL":
		if 'file_xsl' in j1:
			xsl1=j1["file_xsl"].replace("\/","/")
			try:
				xslRoot = etree.fromstring(open(xsl1).read())
			except:
				#Buscamos xsl por defecto
				plpy.notice("Uso xsl por defecto")
				xsl1="/opt/acepta/motor/xsl/windte/xsl/mail.xsl"
				try:
					xslRoot = etree.fromstring(open(xsl1).read())	
				except:
					jout["status"]="OK"
					jout["mensaje"]="OK: Falla Abrir xsl "+xsl1
					return cjson.encode(jout)
			
			transform = etree.XSLT(xslRoot)
			xmlRoot = etree.fromstring(StringIO.StringIO(data_xml).read())
			transRoot = transform(xmlRoot)
			#jout["html"]=etree.tostring(transRoot).encode('hex')
			#part1 = MIMEText(etree.tostring(transRoot),'html')
			html1=etree.tostring(transRoot)
                        #Si viene flag_de_lectura
                        if 'evento_lma' in j1:
				aux=html1
                                #Buscamos si viene cuadraturav2
                                pos1=aux.find("cuadraturav2")
                                if pos1>0:
                                        #Remplazamos cuadraturav2
                                        #plpy.notice("cuadraturav2"+str(pos1))
                                        #Sacamos donde empieza <img donde viene cuadratura en la ultima ocurrencia
                                        for m in re.finditer('<img', aux[0:pos1]):
                                                x1=m.start()
                                        #plpy.notice("x1 "+str(x1))
                                        if x1>0:
                                                #la primera parte
                                                html1=aux[0:x1]
                                                #plpy.notice(html1)
                                                #Buscamos el final, > la primera ocurrencia
                                                fin1=aux[pos1:].find(">")
                                                #plpy.notice("fin1="+str(fin1))
                                                if fin1>0:
                                                        fin1=fin1+pos1+1
                                                        html1=html1+aux[fin1:]
                                #Buscamos si viene traza LMA antiguo
                                pos1=aux.find("traza")
                                if pos1>0:
                                        #Remplazamos traza
                                        #plpy.notice("traza"+str(pos1))
                                        #Sacamos donde empieza <img donde viene cuadratura en la ultima ocurrencia
                                        for m in re.finditer('<img', aux[0:pos1]):
                                                x1=m.start()
                                        #plpy.notice("x1 "+str(x1))
                                        if x1>0:
                                                #la primera parte
                                                html1=aux[0:x1]
                                                #plpy.notice(html1)
                                                #Buscamos el final, > la primera ocurrencia
                                                fin1=aux[pos1:].find(">")
                                                #plpy.notice("fin1="+str(fin1))
                                                if fin1>0:
                                                        fin1=fin1+pos1+1
                                                        html1=html1+aux[fin1:]
	                        #Se agrega el evento siempre, html1 viene sin eventos
                                jout["html"]=html1.encode('hex')
                                html1=html1.replace('</title>','</title><img style="display: none;" src="'+j1["evento_lma"].replace("\/","/")+'"/>')
                                #Solo como debug para ver lo que envie por correo
                                jout["html2"]=html1.encode('hex')
                        part1 = MIMEText(html1,'html')
		else:
			part1 = MIMEText(data_xml, 'plain')
	elif j1["tipo_envio"]=="HTML":
		part1 = MIMEText(j1["content_html"].decode("hex"), 'html')
	else:
		part1 = MIMEText(data_xml, 'plain')

msg.attach(part1)

if 'adjunta_xml' in j1:
	if j1["adjunta_xml"]=="SI":
		part1 = MIMEText(data_xml, 'xml')
		part1.add_header('Content-Disposition', 'attachment; filename="'+j1["nombre_xml"]+'.xml"')
		msg.attach(part1)

if 'adjunta_pdf' in j1:
	if j1["adjunta_pdf"]=="SI":
		#Saco el PDF
		uri_pdf="http://almacen.acepta.com/ca4webv3/PdfViewMedia?url="+uri
		try:
			pdf=requests.get(uri_pdf)
		except:
			jout["status"]="FALLA"
			jout["mensaje"]="Falla leer PDF del almacen"
			return cjson.encode(jout)
		if pdf.status_code!=200:
			jout["status"]="FALLA"
			jout["mensaje"]="Falla status PDF del almacen"
			return cjson.encode(jout)

		#Verifico que venga un PDF
		if pdf.content[0:5].find('PDF')<0:
			jout["status"]="FALLA"
			jout["mensaje"]="Falla PDF, no existe xslfo para el dominio"
			return cjson.encode(jout)
		elif 'clave_pdf' in j1:
			f=cStringIO.StringIO(pdf.content)
			output = PyPDF2.PdfFileWriter()
			input_stream = PyPDF2.PdfFileReader(f)
			for i in range(0, input_stream.getNumPages()):
				 output.addPage(input_stream.getPage(i))
			output.encrypt("Acepta S.A.",j1["clave_pdf"],use_128bit=True)
			#Abro archivo en memoria
			outputStream=cStringIO.StringIO()
			output.write(outputStream)
		else:
			outputStream=cStringIO.StringIO()
			outputStream.write(pdf.content)
			
		#part1 = MIMEApplication(pdf.content, "pdf")
		part1 = MIMEApplication(outputStream.getvalue(), "pdf")
		part1.add_header('Content-Disposition', 'attachment; filename="'+j1["nombre_pdf"]+'.pdf"')
		msg.attach(part1)
		outputStream.close()
			
		
if 'ip_envio' in j1:
	try:
		server = smtplib.SMTP(j1["ip_envio"])
	except Exception, e:
		jout["status"]="FALLA"
		jout["mensaje"]=str(e)
		return cjson.encode(jout)
else:
	server = smtplib.SMTP("localhost")

try:
	server.sendmail(sender,msg["To"].split(';'), msg.as_string(),rcpt_options=['NOTIFY=success,failure'])
except Exception, e:
	jout["status"]="FALLA"
	jout["mensaje"]=str(e)
	return cjson.encode(jout)
	

#Si viene evento_ema, lo enviamos a la traza
if 'evento_ema' in j1:
	try:
		url_traza=j1["url_traza"].replace("\/","/")
		evento_ema=j1["evento_ema"].replace("\/","/")
		urllib2.urlopen(url_traza,evento_ema).read()
	except urllib2.HTTPError:
		jout["status"]="FALLA"
		jout["mensaje"]="Falla Enviar Evento del Mandato"
		return cjson.encode(jout)
	
jout["status"]="OK"
jout["mensaje"]="OK: Mail Enviado"
return cjson.encode(jout)
$$ language plpythonu;
