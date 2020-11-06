using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net.Mail;
using System.Configuration;


namespace csvtosql
{
    class Emailer
    {
        public string from_email;
        public string to_email;
        public string cc_email;
        public string bc_email;
        public string smtpserver;
        public bool IS_SMTP_READY
        {
            get { return ((from_email != null) && (from_email != string.Empty) 
                    && (to_email != null) && (to_email != string.Empty) && (smtpserver != null)); }
        }
  
        public bool SendSMTPMessage(string subject, string message)
        {
            if (!IS_SMTP_READY)
                return false;
            MailMessage mail = new MailMessage();
            SmtpClient SmtpServer = new SmtpClient();
            List<string> emails = to_email.Split(';').ToList();
            foreach (string s in emails)
                if ((s != string.Empty) && (s.ToCharArray().Contains('@')))
                    mail.To.Add(s);
            //to_email.Split(';').ToList().ForEach(e => mail.To.Add(e));
            mail.From = new MailAddress(from_email);
            mail.Subject = subject;
            mail.IsBodyHtml = true;
            mail.Body = message;
            SmtpServer.Host = smtpserver;
            SmtpServer.Port = 25;
            SmtpServer.DeliveryMethod = SmtpDeliveryMethod.Network;
            try
            {

                SmtpServer.Send(mail);
                return true;
            }
            catch (Exception ex)
            {
                if (ex.InnerException != null)                  
                    Console.WriteLine("Failed to send email:\n" + ex.InnerException);
                return false;
            }

            finally
            {
                mail.Dispose();
                //mail = null;
                //SmtpServer = null;
            }
        }
        public bool SendSMTPMessage(string from_email, string to_email, string smtpserver, string subject, string message)
        {
            MailMessage mail = new MailMessage();
            SmtpClient SmtpServer = new SmtpClient();
            mail.To.Add(to_email);
            mail.CC.Add(cc_email);
            mail.Bcc.Add(bc_email);
            mail.From = new MailAddress(from_email);// "mail@domain.com");
            mail.Subject = subject;
            mail.IsBodyHtml = true;
            mail.Body = message;
            SmtpServer.Host = smtpserver;// "smtpserver";
            SmtpServer.Port = 25;
            SmtpServer.DeliveryMethod = System.Net.Mail.SmtpDeliveryMethod.Network;
            try 
            {

                SmtpServer.Send(mail);
                return true;
            }
            catch (Exception ex) 
            { 
                if (ex.InnerException != null)
                    Console.WriteLine("Exception Inner:   " + ex.InnerException);
                return false;
            }
        }  
    }
}
