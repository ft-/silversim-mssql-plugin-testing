using log4net;
using Nini.Config;
using SilverSim.Database.MsSql._Migration;
using SilverSim.Main.Common;
using SilverSim.ServiceInterfaces.Database;
using SilverSim.ServiceInterfaces.Experience;
using SilverSim.Types;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SilverSim.Database.MsSql.Experience
{
    [Description("MsSql ExperienceName Backend")]
    [PluginName("ExperienceNames")]
    public sealed class MsSqlExperienceNameService : ExperienceNameServiceInterface, IDBServiceInterface, IPlugin
    {
        private readonly string m_ConnectionString;
        private static readonly ILog m_Log = LogManager.GetLogger("MSSQL EXPERIENCE NAMES SERVICE");

        #region Constructor
        public MsSqlExperienceNameService(IConfig ownSection)
        {
            m_ConnectionString = MsSqlUtilities.BuildConnectionString(ownSection, m_Log);
        }

        public void Startup(ConfigurationLoader loader)
        {
            /* intentionally left empty */
        }
        #endregion

        #region Accessors
        public override bool TryGetValue(UUID experienceID, out UEI uei)
        {
            using (var connection = new SqlConnection(m_ConnectionString))
            {
                connection.Open();

                using (var cmd = new SqlCommand("SELECT TOP(1) * FROM experiencenames WHERE ExperienceID = @experienceid", connection))
                {
                    cmd.Parameters.AddParameter("@experienceid", experienceID);
                    using (SqlDataReader dbReader = cmd.ExecuteReader())
                    {
                        if (dbReader.Read())
                        {
                            uei = ToUEI(dbReader);
                            return true;
                        }
                    }
                }
            }
            uei = default(UEI);
            return false;
        }

        private static UEI ToUEI(SqlDataReader dbReader) =>
            new UEI(dbReader.GetUUID("ExperienceID"), (string)dbReader["ExperienceName"], dbReader.GetUri("HomeURI"))
            {
                AuthorizationToken = dbReader.GetBytesOrNull("AuthorizationData")
            };

        public override List<UEI> GetExperiencesByName(string experienceName, int limit)
        {
            var groups = new List<UEI>();
            using (var connection = new SqlConnection(m_ConnectionString))
            {
                connection.Open();

                using (var cmd = new SqlCommand("SELECT * FROM experiencenames WHERE ExperienceName = @experienceName LIMIT @limit", connection))
                {
                    cmd.Parameters.AddParameter("@experienceName", experienceName);
                    cmd.Parameters.AddParameter("@limit", limit);
                    using (SqlDataReader dbReader = cmd.ExecuteReader())
                    {
                        while (dbReader.Read())
                        {
                            groups.Add(ToUEI(dbReader));
                        }
                    }
                }
            }
            return groups;
        }

        public override void Store(UEI experience)
        {
            using (var connection = new SqlConnection(m_ConnectionString))
            {
                connection.Open();

                Dictionary<string, object> vars = new Dictionary<string, object>
                {
                    { "ExperienceID", experience.ID },
                    { "HomeURI", experience.HomeURI },
                    { "ExperienceName", experience.ExperienceName }
                };
                if (experience.AuthorizationToken != null)
                {
                    vars.Add("AuthorizationData", experience.AuthorizationToken);
                }
                connection.ReplaceInto("experiencenames", vars, new string[] { "ExperienceID" });
            }
        }
        #endregion

        public void VerifyConnection()
        {
            using (var connection = new SqlConnection(m_ConnectionString))
            {
                connection.Open();
            }
        }

        public void ProcessMigrations()
        {
            using (var connection = new SqlConnection(m_ConnectionString))
            {
                connection.Open();
                connection.MigrateTables(Migrations, m_Log);
            }
        }

        private static readonly IMigrationElement[] Migrations = new IMigrationElement[]
        {
            new SqlTable("experiencenames"),
            new AddColumn<UUID>("ExperienceID") { IsNullAllowed = false, Default = UUID.Zero },
            new AddColumn<string>("HomeURI") { Cardinality = 255, IsNullAllowed = false, Default = string.Empty },
            new AddColumn<string>("ExperienceName") { Cardinality = 255, IsNullAllowed = false, Default = string.Empty },
            new AddColumn<byte[]>("AuthorizationData"),
            new PrimaryKeyInfo("ExperienceID"),
        };
    }
}
