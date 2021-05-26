using System;
using System.Data;
using System.Data.SqlClient;
using Microsoft.SqlServer.Server;

public partial class Triggers
{
    public static void InsertAuthorTrigger()
    {
        SqlPipe sqlP = SqlContext.Pipe;
        sqlP.Send("Wpisano imiê do bazy danych.");
    }
}

