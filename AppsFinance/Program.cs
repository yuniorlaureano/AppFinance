using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using AppsFinance.Entity;
using AppsFinance.Data;
using System.Net.Mail;

namespace AppsFinance
{
    public class Program
    {
        static void Main(string[] args)
        {
            //Prueba();

            #region tareas comunes
            string[] procedures = { "current_link", "due_30", "due_60", "due_90", "due_120", "out_120", "invoice_future" };
            string[] tranvencetype = { "10CUR", "30DUE", "60DUE", "90DUE", "9D120", "9O120", "9DFUT" };

            Console.WriteLine("Cleaning lighter tables");
            new MigrateData(Country.PRI, ENVVAR.PRI).DeleteDueAging();

            Task[] datamovers = new Task[procedures.Length];

            for (int i = 0; i < procedures.Length; i++)
            {
                datamovers[i] = new Task((ii) =>
                {

                    Console.WriteLine("***************************************************************************");
                    Console.WriteLine("***Process: " + procedures[(int)ii] + " started                   **************************");
                    Console.WriteLine("---------------------------------------------------------------------------");

                    MigrateData dataMigrator = new MigrateData(Country.PRI, ENVVAR.PRI);
                    MigrateData dataInsertor = new MigrateData(Country.PRI, ENVVAR.PRI);

                    dataInsertor.InsertStatement(
                        statements: dataMigrator.GetStatement(procedures[(int)ii]),
                        procedure: "pkg_appsfinance.sp_add_cviewer_detail",
                        tranvencetype: tranvencetype[(int)ii]
                        );

                    Console.WriteLine("***************************************************************************");
                    Console.WriteLine("***Process: " + procedures[(int)ii] + " ended                     **************************");
                    Console.WriteLine("---------------------------------------------------------------------------");
                }, i);

                datamovers[i].Start();
            }
            #endregion
                        
            try
            {
                //tareas comunes
                Task.WaitAll(datamovers);

                Console.WriteLine("Cleaning heavy tables");
                new MigrateData(Country.PRI, ENVVAR.PRI).DeleteCustCollectorPayment();
                //hevier tasks
                Task custResumeTask = new Task(() =>
                {
                    Console.WriteLine("***************************************************************************");
                    Console.WriteLine("***Process: custResumeTask started               **************************");
                    Console.WriteLine("---------------------------------------------------------------------------");

                    MigrateData dataMigrator = new MigrateData(Country.PRI, ENVVAR.PRI);
                    MigrateData dataInsertor = new MigrateData(Country.PRI, ENVVAR.PRI);
                    dataInsertor.InsertCviewerCustResumeStatement(dataMigrator.GetCviewerCustResumeStatement());

                    Console.WriteLine("***************************************************************************");
                    Console.WriteLine("***Process: custResumeTask, ended                **************************");
                    Console.WriteLine("---------------------------------------------------------------------------");
                });

                Task collectorTask = new Task(() =>
                {
                    Console.WriteLine("***************************************************************************");
                    Console.WriteLine("***Process: collectorTask, started               **************************");
                    Console.WriteLine("---------------------------------------------------------------------------");

                    MigrateData dataMigrator = new MigrateData(Country.PRI, ENVVAR.PRI);
                    MigrateData dataInsertor = new MigrateData(Country.PRI, ENVVAR.PRI);
                    dataInsertor.InsertCviewerCollectorStatement(dataMigrator.GetCviewerCollectorStatement());

                    Console.WriteLine("***************************************************************************");
                    Console.WriteLine("***Process: collectorTask, ended                 **************************");
                    Console.WriteLine("---------------------------------------------------------------------------");

                });

                Task paymentTask = new Task(() =>
                {
                    Console.WriteLine("***************************************************************************");
                    Console.WriteLine("***Process: paymentTask, started                 **************************");
                    Console.WriteLine("---------------------------------------------------------------------------");

                    MigrateData dataMigrator = new MigrateData(Country.PRI, ENVVAR.PRI);
                    MigrateData dataInsertor = new MigrateData(Country.PRI, ENVVAR.PRI);
                    dataInsertor.InsertCviewerPaymentStatement(dataMigrator.GetCviewerPaymentDetailStatement());

                    Console.WriteLine("***************************************************************************");
                    Console.WriteLine("***Process: paymentTask, ended                   **************************");
                    Console.WriteLine("---------------------------------------------------------------------------");

                });

                custResumeTask.Start();
                collectorTask.Start();
                paymentTask.Start();
                Task.WaitAll(custResumeTask, collectorTask, paymentTask);

                Console.WriteLine("All completed...................................");
                SendNotification("SUCCESS: (PR) APPFINANCE", "Transactions Successfully Completed.");
            }
            catch (AggregateException except)
            {
                foreach (Exception inner in except.InnerExceptions)
                {
                    Console.WriteLine("Exception type {0}", inner.GetType());
                }

                SendNotification("ERROR: (PR) APPFINANCE", except.Message);
            }
            catch(Exception except)
            {
                SendNotification("ERROR: (PR) APPFINANCE", except.Message);
            }
        }

        public static void SendNotification(string subject, string body) 
        {

            MailMessage message = new MailMessage();
            message.From = new MailAddress("Desarrollo/sistemas@paginasamarillas.com.do");
            message.To.Add(new MailAddress("y.laureano@caribemedia.com.do"));
            message.To.Add(new MailAddress("n.gutierrez@caribemedia.com.do"));
            message.Subject = subject;
            message.BodyEncoding = System.Text.Encoding.UTF8;
            message.Priority = MailPriority.Normal;
            message.Body = body;

            SmtpClient server = new SmtpClient("172.27.136.11", 25);
            server.UseDefaultCredentials = true;
            server.DeliveryMethod = SmtpDeliveryMethod.Network;
            server.EnableSsl = false;
            server.Send(message);
        }

        public static void Prueba() {

            Console.WriteLine("Inicio de transaccion");
            MigrateData dataMigrator = new MigrateData(Country.PRI, ENVVAR.PRI);

            foreach (string item in dataMigrator.GetStatement("out_120"))
            {
                Console.WriteLine(item);
            }
        }
    }
}
