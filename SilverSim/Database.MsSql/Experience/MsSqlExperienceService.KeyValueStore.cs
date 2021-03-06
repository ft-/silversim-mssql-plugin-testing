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

using SilverSim.ServiceInterfaces.Experience;
using SilverSim.Types;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace SilverSim.Database.MsSql.Experience
{
    public sealed partial class MsSqlExperienceService : IExperienceKeyValueInterface
    {
        void IExperienceKeyValueInterface.Add(UEI experienceID, string key, string value)
        {
            var vals = new Dictionary<string, object>
            {
                ["ExperienceID"] = experienceID.ID,
                ["Key"] = key,
                ["Value"] = value
            };
            using (var conn = new SqlConnection(m_ConnectionString))
            {
                conn.Open();

                conn.InsertInto("experiencekeyvalues", vals);
            }
        }

        bool IExperienceKeyValueInterface.GetDatasize(UEI experienceID, out int used, out int quota)
        {
            used = 0;
            quota = -1;
            using (var conn = new SqlConnection(m_ConnectionString))
            {
                conn.Open();
                using (var cmd = new SqlCommand("SELECT `Value` FROM experiencekeyvalues WHERE ExperienceID = @experienceid", conn))
                {
                    cmd.Parameters.AddParameter("@experienceid", experienceID.ID);
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            used += ((string)reader["Value"]).Length;
                        }
                    }
                }
            }
            return true;
        }

        List<string> IExperienceKeyValueInterface.GetKeys(UEI experienceID)
        {
            var result = new List<string>();
            using (var conn = new SqlConnection(m_ConnectionString))
            {
                conn.Open();
                using (var cmd = new SqlCommand("SELECT [Key] FROM experiencekeyvalues WHERE ExperienceID = @experienceid", conn))
                {
                    cmd.Parameters.AddParameter("@experienceid", experienceID.ID);
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            result.Add((string)reader["Key"]);
                        }
                    }
                }
            }
            return result;
        }

        bool IExperienceKeyValueInterface.Remove(UEI experienceID, string key)
        {
            using (var conn = new SqlConnection(m_ConnectionString))
            {
                conn.Open();
                using (var cmd = new SqlCommand("DELETE FROM experiencekeyvalues WHERE ExperienceID = @experienceid AND [Key] = @key", conn))
                {
                    cmd.Parameters.AddParameter("@experienceid", experienceID.ID);
                    cmd.Parameters.AddParameter("@key", key);
                    return cmd.ExecuteNonQuery() > 0;
                }
            }
        }

        void IExperienceKeyValueInterface.Store(UEI experienceID, string key, string value)
        {
            var vals = new Dictionary<string, object>
            {
                ["ExperienceID"] = experienceID.ID,
                ["Key"] = key,
                ["Value"] = value
            };
            using (var conn = new SqlConnection(m_ConnectionString))
            {
                conn.Open();
                conn.ReplaceInto("experiencekeyvalues", vals, new string[] { "ExperienceID", "Key" });
            }
        }

        bool IExperienceKeyValueInterface.StoreOnlyIfEqualOrig(UEI experienceID, string key, string value, string orig_value)
        {
            using (var conn = new SqlConnection(m_ConnectionString))
            {
                conn.Open();
                return conn.InsideTransaction<bool>((transaction) =>
                {
                    using (var cmd = new SqlCommand("SELECT TOP(1) [Value] FROM experiencekeyvalues WHERE ExperienceID = @experienceid AND [Key] = @key", conn))
                    {
                        cmd.Transaction = transaction;
                        cmd.Parameters.AddParameter("@experienceid", experienceID.ID);
                        cmd.Parameters.AddParameter("@key", key);
                        using (SqlDataReader reader = cmd.ExecuteReader())
                        {
                            if (reader.Read() && (string)reader["Value"] != orig_value)
                            {
                                return false;
                            }
                        }
                    }

                    var vals = new Dictionary<string, object>
                    {
                        ["ExperienceID"] = experienceID.ID,
                        ["Key"] = key,
                        ["Value"] = value
                    };
                    conn.ReplaceInto("experiencekeyvalues", vals, new string[] { "ExperienceID", "Key" }, transaction);

                    return true;
                });
            }
        }

        bool IExperienceKeyValueInterface.TryGetValue(UEI experienceID, string key, out string val)
        {
            using (var conn = new SqlConnection(m_ConnectionString))
            {
                conn.Open();
                using (var cmd = new SqlCommand("SELECT TOP(1) [Value] FROM experiencekeyvalues WHERE ExperienceID = @experienceid AND [Key] = @key", conn))
                {
                    cmd.Parameters.AddParameter("@experienceid", experienceID.ID);
                    cmd.Parameters.AddParameter("@key", key);
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            val = (string)reader["Value"];
                            return true;
                        }
                    }
                }
            }
            val = default(string);
            return false;
        }
    }
}
