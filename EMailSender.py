
# Open a plain text file for reading.  For this example, assume that
# the text file contains only ASCII characters.
#fp = open(textfile, 'rb')
# Create a text/plain message
def sendMail(lDesc,addr,sensor,id_agent,value,datetime,unit,symbol):
	# Import smtplib for the actual sending function
	import smtplib

	# Import the email modules we'll need
	from email.mime.text import MIMEText
	from email.mime.multipart import MIMEMultipart


	msg = MIMEMultipart('alternative')
	#msg = MIMEText("<center><h1>Alert detected from idEdison="+str(id_agent)+"</h1></center>\nSensor:"+str(sensor)+"\nLocation description: "+text+"\n(more information in alert reports)")
	#fp.close()
	html = """\
	<html>
  	<head>
		<img src="https://upload.wikimedia.org/wikipedia/commons/thumb/c/ca/Escudo-UNAM-escalable.svg/250px-Escudo-UNAM-escalable.svg.png" height="50" width="50"/>
		<img src="http://www.lanase.unam.mx/images/logo-main4.png" height="50" width="50"/>
		<center>
		<h1>Alert detected from Edison """+ str(id_agent) + """</h1>
		</center>
  	</head>
  	<body>
	<hr style="border-width: 2px">
  	<table style="border:0px">
  <tr>
    <td><h4>Detection date and time</h4></td>
    <td>"""+datetime+"""</td>
  </tr>
  <tr>
    <td><h4>Sensor</h4></td>
    <td>"""+str(sensor)+"""</td>
  </tr>
  <tr>
    <td><h4>Detected value</h4></td>
    <td>"""+str(value)+symbol+""" ("""+unit+""")</td>
  </tr>
  <tr>
    <td><h4>Location description</h4></td>
    <td>"""+lDesc+"""</td>
  </tr>
  <tr>
    <td><h4>Address</h4></td>
    <td>"""+addr+"""</td>
  </tr>
</table>
<hr>
<center>
(More information in Alerts Reports)
</center>
  	</body>

	</html>
	"""
	# me == the sender's email address
	# you == the recipient's email address
	msg['Subject'] = 'Alert from idEdison='+str(id_agent)+' Sensor:'+str(sensor)
	msg['From'] = 'Email direction'
	msg['To'] = 'Another Email direction'
	#print(msg.as_string)
	# Send the message via our own SMTP server, but don't include the
	# envelope header.
	part = MIMEText(html, 'html')
	msg.attach(part)

	server = smtplib.SMTP('smtp.gmail.com',587)
	#server.connect('smtp.gmail.com',587)
	server.ehlo()
	server.starttls()
	server.ehlo()
	server.login('Email direction', 'passwordToLog')
	server.sendmail(msg['From'], msg['To'], msg.as_string())
	server.quit()
	print("Email sent")