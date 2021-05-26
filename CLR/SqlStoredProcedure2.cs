using System;
using Microsoft.SqlServer.Server;

public class Przyklad
{
    [SqlProcedure]
    public static void Przyk(out string przyklad1)
    {
        SqlContext.Pipe.Send("Tu bêdzie przyk³ad." + Environment.NewLine);
        przyklad1 = "To jest przyk³ad.";
    }
}

