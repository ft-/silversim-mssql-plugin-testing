﻿// SilverSim is distributed under the terms of the
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
using SilverSim.Database.MsSql._Migration;
using SilverSim.Main.Common;
using SilverSim.ServiceInterfaces.Account;
using SilverSim.ServiceInterfaces.Database;
using SilverSim.Types;
using SilverSim.Types.Account;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlClient;

namespace SilverSim.Database.MsSql.UserAccounts
{
    [Description("MsSql UserAccount Backend")]
    [PluginName("UserAccounts")]
    public sealed class MsSqlUserAccountService : UserAccountServiceInterface, IDBServiceInterface, IPlugin, IUserAccountSerialNoInterface
    {
        private readonly string m_ConnectionString;
        private Uri m_HomeURI;
        private static readonly ILog m_Log = LogManager.GetLogger("MSSQL USERACCOUNT SERVICE");

        #region Constructor
        public MsSqlUserAccountService(IConfig ownSection)
        {
            m_ConnectionString = MsSqlUtilities.BuildConnectionString(ownSection, m_Log);
        }

        public void Startup(ConfigurationLoader loader)
        {
            m_HomeURI = new Uri(loader.HomeURI);
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

            ulong serno;
            if (!TryGetSerialNumber(out serno))
            {
                using (var connection = new SqlConnection(m_ConnectionString))
                {
                    connection.Open();

                    using (var cmd = new SqlCommand("SELECT COUNT(ID) FROM useraccounts", connection))
                    {
                        using (SqlDataReader reader = cmd.ExecuteReader())
                        {
                            if (!reader.Read())
                            {
                                throw new ConfigurationLoader.ConfigurationErrorException("Failed to read number of accounts");
                            }
                            serno = (ulong)reader.GetInt32(0);
                        }
                    }

                    var vals = new Dictionary<string, object>
                    {
                        { "SerialNumber", serno }
                    };
                    connection.InsertInto("useraccounts_serial", vals);
                }
            }
        }

        private static readonly IMigrationElement[] Migrations = new IMigrationElement[]
        {
            new SqlTable("useraccounts"),
            new AddColumn<UUID>("ID") { IsNullAllowed = false, Default = UUID.Zero },
            new AddColumn<UUID>("ScopeID") { IsNullAllowed = false, Default = UUID.Zero },
            new AddColumn<string>("FirstName") { Cardinality = 31, IsNullAllowed = false, Default = string.Empty },
            new AddColumn<string>("LastName") { Cardinality = 31, IsNullAllowed = false, Default = string.Empty },
            new AddColumn<string>("Email") { Cardinality = 255, IsNullAllowed = false, Default = string.Empty },
            new AddColumn<Date>("Created") { IsNullAllowed = false, Default = Date.UnixTimeToDateTime(0) },
            new AddColumn<int>("UserLevel") { IsNullAllowed = false, Default = 0 },
            new AddColumn<int>("UserFlags") { IsNullAllowed = false, Default = 0 },
            new AddColumn<string>("UserTitle") { Cardinality = 64, IsNullAllowed = false, Default = string.Empty },
            new PrimaryKeyInfo("ID"),
            new NamedKeyInfo("Email", "Email"),
            new NamedKeyInfo("Name", "FirstName", "LastName") { IsUnique = true },
            new NamedKeyInfo("FirstName", "FirstName"),
            new NamedKeyInfo("LastName", "LastName"),
            new TableRevision(2),
            new ChangeColumn<uint>("UserFlags") { IsNullAllowed = false, Default = (uint)0 },
            new TableRevision(3),
            new AddColumn<bool>("IsEverLoggedIn") { IsNullAllowed = false, Default = false },

            new SqlTable("useraccounts_serial"),
            new AddColumn<ulong>("SerialNumber") { IsNullAllowed = false, Default = (ulong)0 }
        };

        public override bool ContainsKey(UUID scopeID, UUID accountID)
        {
            using (var connection = new SqlConnection(m_ConnectionString))
            {
                connection.Open();
                if (scopeID != UUID.Zero)
                {
                    using (var cmd = new SqlCommand("SELECT TOP(1) ID FROM useraccounts WHERE ScopeID = @scopeid AND ID = @id", connection))
                    {
                        cmd.Parameters.AddParameter("@scopeid", scopeID);
                        cmd.Parameters.AddParameter("@id", accountID);
                        using (SqlDataReader reader = cmd.ExecuteReader())
                        {
                            if (reader.Read())
                            {
                                return true;
                            }
                        }
                    }
                }
                else
                {
                    using (var cmd = new SqlCommand("SELECT TOP(1) ID FROM useraccounts WHERE ID = @id", connection))
                    {
                        cmd.Parameters.AddParameter("@id", accountID);
                        using (SqlDataReader reader = cmd.ExecuteReader())
                        {
                            if (reader.Read())
                            {
                                return true;
                            }
                        }
                    }
                }
            }

            return false;
        }

        public override bool TryGetValue(UUID scopeID, UUID accountID, out UserAccount account)
        {
            using (var connection = new SqlConnection(m_ConnectionString))
            {
                connection.Open();
                if (scopeID != UUID.Zero)
                {
                    using (var cmd = new SqlCommand("SELECT TOP(1) * FROM useraccounts WHERE ScopeID = @scopeid AND ID = @id", connection))
                    {
                        cmd.Parameters.AddParameter("@scopeid", scopeID);
                        cmd.Parameters.AddParameter("@id", accountID);
                        using (SqlDataReader reader = cmd.ExecuteReader())
                        {
                            if (reader.Read())
                            {
                                account = reader.ToUserAccount(m_HomeURI);
                                return true;
                            }
                        }
                    }
                }
                else
                {
                    using (var cmd = new SqlCommand("SELECT TOP(1) * FROM useraccounts WHERE ID = @id", connection))
                    {
                        cmd.Parameters.AddParameter("@id", accountID);
                        using (SqlDataReader reader = cmd.ExecuteReader())
                        {
                            if (reader.Read())
                            {
                                account = reader.ToUserAccount(m_HomeURI);
                                return true;
                            }
                        }
                    }
                }
            }

            account = default(UserAccount);
            return false;
        }

        public override UserAccount this[UUID scopeID, UUID accountID]
        {
            get
            {
                UserAccount account;
                if (!TryGetValue(scopeID, accountID, out account))
                {
                    throw new UserAccountNotFoundException();
                }
                return account;
            }
        }

        public override bool ContainsKey(UUID scopeID, string email)
        {
            using (var connection = new SqlConnection(m_ConnectionString))
            {
                connection.Open();
                using (var cmd = new SqlCommand("SELECT TOP(1) ScopeID FROM useraccounts WHERE ScopeID = @scopeid AND Email = @email", connection))
                {
                    cmd.Parameters.AddParameter("@scopeid", scopeID);
                    cmd.Parameters.AddParameter("@email", email);
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            return true;
                        }
                    }
                }
            }

            return false;
        }

        public override bool TryGetValue(UUID scopeID, string email, out UserAccount account)
        {
            using (var connection = new SqlConnection(m_ConnectionString))
            {
                connection.Open();
                using (var cmd = new SqlCommand("SELECT TOP(1) * FROM useraccounts WHERE ScopeID = @scopeid AND Email = @email", connection))
                {
                    cmd.Parameters.AddParameter("@scopeid", scopeID);
                    cmd.Parameters.AddParameter("@email", email);
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            account = reader.ToUserAccount(m_HomeURI);
                            return true;
                        }
                    }
                }
            }

            account = default(UserAccount);
            return false;
        }

        public override UserAccount this[UUID scopeID, string email]
        {
            get
            {
                UserAccount account;
                if (!TryGetValue(scopeID, email, out account))
                {
                    throw new UserAccountNotFoundException();
                }
                return account;
            }
        }

        public override bool ContainsKey(UUID scopeID, string firstName, string lastName)
        {
            using (var connection = new SqlConnection(m_ConnectionString))
            {
                connection.Open();
                if (scopeID != UUID.Zero)
                {
                    using (var cmd = new SqlCommand("SELECT TOP(1) ScopeID FROM useraccounts WHERE ScopeID = @scopeid AND FirstName = @firstname AND LastName = @lastname", connection))
                    {
                        cmd.Parameters.AddParameter("@scopeid", scopeID);
                        cmd.Parameters.AddParameter("@firstname", firstName);
                        cmd.Parameters.AddParameter("@lastname", lastName);
                        using (SqlDataReader reader = cmd.ExecuteReader())
                        {
                            if (reader.Read())
                            {
                                return true;
                            }
                        }
                    }
                }
                else
                {
                    using (var cmd = new SqlCommand("SELECT TOP(1) ScopeID FROM useraccounts WHERE FirstName = @firstname AND LastName = @lastname", connection))
                    {
                        cmd.Parameters.AddParameter("@firstname", firstName);
                        cmd.Parameters.AddParameter("@lastname", lastName);
                        using (SqlDataReader reader = cmd.ExecuteReader())
                        {
                            if (reader.Read())
                            {
                                return true;
                            }
                        }
                    }
                }
            }

            return false;
        }

        public override bool TryGetValue(UUID scopeID, string firstName, string lastName, out UserAccount account)
        {
            using (var connection = new SqlConnection(m_ConnectionString))
            {
                connection.Open();
                if (scopeID != UUID.Zero)
                {
                    using (var cmd = new SqlCommand("SELECT TOP(1) * FROM useraccounts WHERE ScopeID = @scopeid AND FirstName = @firstname AND LastName = @lastname", connection))
                    {
                        cmd.Parameters.AddParameter("@scopeid", scopeID);
                        cmd.Parameters.AddParameter("@firstname", firstName);
                        cmd.Parameters.AddParameter("@lastname", lastName);
                        using (SqlDataReader reader = cmd.ExecuteReader())
                        {
                            if (reader.Read())
                            {
                                account = reader.ToUserAccount(m_HomeURI);
                                return true;
                            }
                        }
                    }
                }
                else
                {
                    using (var cmd = new SqlCommand("SELECT TOP(1) * FROM useraccounts WHERE FirstName = @firstname AND LastName = @lastname", connection))
                    {
                        cmd.Parameters.AddParameter("@firstname", firstName);
                        cmd.Parameters.AddParameter("@lastname", lastName);
                        using (SqlDataReader reader = cmd.ExecuteReader())
                        {
                            if (reader.Read())
                            {
                                account = reader.ToUserAccount(m_HomeURI);
                                return true;
                            }
                        }
                    }
                }
            }

            account = default(UserAccount);
            return false;
        }

        public override UserAccount this[UUID scopeID, string firstName, string lastName]
        {
            get
            {
                UserAccount account;
                if (!TryGetValue(scopeID, firstName, lastName, out account))
                {
                    throw new UserAccountNotFoundException();
                }
                return account;
            }
        }

        public override List<UserAccount> GetAccounts(UUID scopeID, string query)
        {
            string[] words = query.Split(new char[] { ' ' }, 2);
            var accounts = new List<UserAccount>();
            if (query.Trim().Length == 0)
            {
                using (var connection = new SqlConnection(m_ConnectionString))
                {
                    connection.Open();
                    using (var cmd = new SqlCommand("SELECT * FROM useraccounts WHERE (ScopeID = @ScopeID or ScopeID = '00000000-0000-0000-0000-000000000000')", connection))
                    {
                        cmd.Parameters.AddParameter("@ScopeID", scopeID);
                        using (SqlDataReader dbreader = cmd.ExecuteReader())
                        {
                            while (dbreader.Read())
                            {
                                accounts.Add(dbreader.ToUserAccount(m_HomeURI));
                            }
                        }
                    }
                }
                return accounts;
            }

            using (var connection = new SqlConnection(m_ConnectionString))
            {
                connection.Open();
                string cmdstr = "select * from useraccounts where (ScopeID = @ScopeID or ScopeID = '00000000-0000-0000-0000-000000000000') and (FirstName LIKE @word0 or LastName LIKE @word0)";
                if (words.Length == 2)
                {
                    cmdstr = "select * from useraccounts where (ScopeID = @ScopeID or ScopeID = '00000000-0000-0000-0000-000000000000') and (FirstName LIKE @word0 or LastName LIKE @word1)";
                }
                using (var cmd = new SqlCommand(cmdstr, connection))
                {
                    cmd.Parameters.AddParameter("@ScopeID", scopeID);
                    for (int i = 0; i < words.Length; ++i)
                    {
                        cmd.Parameters.AddParameter("@word" + i.ToString(), words[i]);
                    }
                    using (SqlDataReader dbreader = cmd.ExecuteReader())
                    {
                        while (dbreader.Read())
                        {
                            accounts.Add(dbreader.ToUserAccount(m_HomeURI));
                        }
                    }
                }
            }
            return accounts;
        }

        public override void Add(UserAccount userAccount)
        {
            var data = new Dictionary<string, object>
            {
                ["ID"] = userAccount.Principal.ID,
                ["ScopeID"] = userAccount.ScopeID,
                ["FirstName"] = userAccount.Principal.FirstName,
                ["LastName"] = userAccount.Principal.LastName,
                ["Email"] = userAccount.Email,
                ["Created"] = userAccount.Created,
                ["UserLevel"] = userAccount.UserLevel,
                ["UserFlags"] = userAccount.UserFlags,
                ["UserTitle"] = userAccount.UserTitle,
                ["IsEverLoggedIn"] = userAccount.IsEverLoggedIn ? 1 : 0
            };
            using (var connection = new SqlConnection(m_ConnectionString))
            {
                connection.Open();
                connection.InsideTransaction((transaction) =>
                {
                    connection.InsertInto("useraccounts", data, transaction);
                    using (var cmd = new SqlCommand("UPDATE useraccounts_serial SET SerialNumber = SerialNumber + 1", connection)
                    {
                        Transaction = transaction
                    })
                    {
                        cmd.ExecuteNonQuery();
                    }
                });
            }
        }

        private bool TryGetSerialNumber(out ulong serialno)
        {
            using (var connection = new SqlConnection(m_ConnectionString))
            {
                connection.Open();
                using (var cmd = new SqlCommand("SELECT TOP(1) SerialNumber FROM useraccounts_serial", connection))
                {
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            serialno = (ulong)(long)reader["SerialNumber"];
                            return true;
                        }
                    }
                }
            }
            serialno = 0;
            return false;
        }

        public ulong SerialNumber
        {
            get
            {
                ulong serno;
                if (!TryGetSerialNumber(out serno))
                {
                    throw new InvalidOperationException("Serial number access failed");
                }
                return serno;
            }
        }

        public override void Update(UserAccount userAccount)
        {
            var data = new Dictionary<string, object>
            {
                ["FirstName"] = userAccount.Principal.FirstName,
                ["LastName"] = userAccount.Principal.LastName,
                ["Email"] = userAccount.Email,
                ["Created"] = userAccount.Created,
                ["UserLevel"] = userAccount.UserLevel,
                ["UserFlags"] = userAccount.UserFlags,
                ["UserTitle"] = userAccount.UserTitle,
                ["IsEverLoggedIn"] = userAccount.IsEverLoggedIn ? 1 : 0
            };
            var w = new Dictionary<string, object>
            {
                ["ScopeID"] = userAccount.ScopeID,
                ["ID"] = userAccount.Principal.ID
            };
            using (var connection = new SqlConnection(m_ConnectionString))
            {
                connection.Open();
                connection.UpdateSet("useraccounts", data, w);
            }
        }

        public override void Remove(UUID scopeID, UUID accountID)
        {
            using (var connection = new SqlConnection(m_ConnectionString))
            {
                connection.Open();
                using (var cmd = new SqlCommand("DELETE FROM useraccounts WHERE ID = @id AND ScopeID = @scopeid", connection))
                {
                    cmd.Parameters.AddParameter("@id", accountID);
                    cmd.Parameters.AddParameter("@scopeid", scopeID);
                    if (cmd.ExecuteNonQuery() < 1)
                    {
                        throw new KeyNotFoundException();
                    }
                }
            }
        }

        public List<UUI> AccountList
        {
            get
            {
                var list = new List<UUI>();

                using (var conn = new SqlConnection(m_ConnectionString))
                {
                    conn.Open();
                    using (var cmd = new SqlCommand("SELECT ID, FirstName, LastName FROM useraccounts", conn))
                    {
                        using (SqlDataReader reader = cmd.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                list.Add(new UUI
                                {
                                    ID = reader.GetUUID("ID"),
                                    FirstName = (string)reader["FirstName"],
                                    LastName = (string)reader["LastName"]
                                });
                            }
                        }
                    }
                }

                return list;
            }
        }
        public override void SetEverLoggedIn(UUID scopeID, UUID accountID)
        {
            using (var connection = new SqlConnection(m_ConnectionString))
            {
                connection.Open();
                using (var cmd = new SqlCommand("UPDATE useraccounts SET IsEverLoggedIn=1 WHERE ID = @id AND ScopeID = @scopeid", connection))
                {
                    cmd.Parameters.AddParameter("@id", accountID);
                    cmd.Parameters.AddParameter("@scopeid", scopeID);
                    if (cmd.ExecuteNonQuery() < 1)
                    {
                        throw new KeyNotFoundException();
                    }
                }
            }
        }
    }
}
