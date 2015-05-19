using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Tamir.SharpSsh;

namespace LogManager
{
    public class SSHManager
    {
        public string IP { get; set; }
        public string User { get; set; }
        public string Password { get; set; }

        public SSHManager(string IP, string User, string Pass) 
        { this.IP = IP; this.User = User; this.Password = Pass; }

        public void RunCommand(string cmd)
        {
            SshExec ssh = new SshExec(this.IP, this.User, this.Password);
            ssh.Connect();
            string result = ssh.RunCommand(cmd);
            Console.WriteLine(result);
            ssh.Close();
        }

        public void Put(string fromPath, string toPath)
        {
            SshTransferProtocolBase sftpBase = new Sftp(this.IP, this.User,this.Password);
            Console.WriteLine("Trying to Open Connection...");
            sftpBase.Connect();
            Console.WriteLine("Connected Successfully !");
            try
            {
                sftpBase.Put(fromPath, toPath);
                Console.WriteLine("File transfer succeeded");
            }
            catch (Exception ex)
            {
                Console.WriteLine("{0}",ex);
            }
            Console.WriteLine("Trying to Close Connection...");
            sftpBase.Close();
            Console.WriteLine("Disconnected Successfully !"); 
        }

        public void Get(string fromPath, string toPath)
        {
            SshTransferProtocolBase sftpBase = new Sftp(this.IP, this.User, this.Password);
            Console.WriteLine("Trying to Open Connection...");
                sftpBase.Connect();
            Console.WriteLine("Connected Successfully !");
            sftpBase.Get(fromPath, toPath);
            Console.WriteLine("Trying to Close Connection...");
            sftpBase.Close();
            Console.WriteLine("Disconnected Successfully !"); 
        }
    }
}
