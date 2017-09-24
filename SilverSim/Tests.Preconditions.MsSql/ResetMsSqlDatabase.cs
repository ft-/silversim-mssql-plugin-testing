// SilverSim is distributed under the terms of the
// GNU Affero General Public License v3 with
// the following clarification and special exception.

// Linking this library statically or dynamically with other modules is
// making a combined work based on this library. Thus, the terms and
// conditions of the GNU Affero General Public License cover the whole
// combination.

// As a special exception, the copyright holders of this library give you
// permission to link this library with independent modules to produce an
// executable, regardless of the license terms of these independent
// modules, and to copy and distribute the resulting executable under
// terms of your choice, provided that you also meet, for each linked
// independent module, the terms and conditions of the license of that
// module. An independent module is a module which is not derived from
// or based on this library. If you modify this library, you may extend
// this exception to your version of the library, but you are not
// obligated to do so. If you do not wish to do so, delete this
// exception statement from your version.

using log4net;
using Nini.Config;
using SilverSim.Main.Common;
using SilverSim.ServiceInterfaces.Database;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace SilverSim.Tests.Preconditions.MsSql
{
    [PluginName("ResetDatabase")]
    public sealed class ResetMsSqlDatabase : IPlugin, IDBServiceInterface
    {
        private static readonly ILog m_Log = LogManager.GetLogger("MSSQL DATABASE RESET");
        private readonly string m_ConnectionString;
        private readonly string m_InitialCatalog;

        public ResetMsSqlDatabase(IConfig config)
        {
            var sb = new SqlConnectionStringBuilder();

            if (!(config.Contains("Server") && config.Contains("Database")))
            {
                if (!config.Contains("Server"))
                {
                    m_Log.FatalFormat("[MSSQL CONFIG]: Parameter 'Server' missing in [{0}]", config.Name);
                }
                if (!config.Contains("Database"))
                {
                    m_Log.FatalFormat("[MSSQL CONFIG]: Parameter 'Database' missing in [{0}]", config.Name);
                }
                throw new ConfigurationLoader.ConfigurationErrorException();
            }

            sb.DataSource = config.GetString("Server");
            if (config.Contains("Username"))
            {
                sb.UserID = config.GetString("Username");
            }
            if (config.Contains("Password"))
            {
                sb.Password = config.GetString("Password");
            }
            m_InitialCatalog = config.GetString("Database");
            sb.InitialCatalog = m_InitialCatalog;
            m_ConnectionString = sb.ToString();
        }

        public void Startup(ConfigurationLoader loader)
        {
        }

        public void VerifyConnection()
        {
            var tables = new List<string>();

            using (var connection = new SqlConnection(m_ConnectionString))
            {
                connection.Open();
                m_Log.Info("Executing reset database");
                using (var cmd = new SqlCommand("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_CATALOG=@catalog", connection))
                {
                    cmd.Parameters.AddWithValue("@catalog", m_InitialCatalog);
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            tables.Add((string)reader.GetValue(0));
                        }
                    }
                }

                m_Log.InfoFormat("Deleting {0} tables", tables.Count);
                foreach (string table in tables)
                {
                    m_Log.InfoFormat("Deleting table {0}", table);
                    using (var cmd = new SqlCommand(string.Format("DROP TABLE {0}", table), connection))
                    {
                        cmd.ExecuteNonQuery();
                    }
                }
            }
        }

        public void ProcessMigrations()
        {
        }
    }
}
