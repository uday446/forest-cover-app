
import os
from smtplib import SMTP

class mail:
    def __init__(self):
        self.port = 587
        self.to = "ujadeja96@gmail.com"
        self.host = "smtp.gmail.com"
        self.username = os.environ.get("from")
        self.password = os.environ.get("pass")
        #self.fr_om =
        #self.to =

    def send_mail(self,error):
        CON = SMTP(self.host, self.port)
        CON.ehlo()
        CON.starttls()
        CON.login(self.username, self.password)
        msg = "Subject : Error In Forest-Cover-Classification-App\n" + error
        CON.sendmail(self.username, self.to, msg)
        CON.quit()
        print("Got here")

