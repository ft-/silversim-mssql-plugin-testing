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

using SilverSim.ServiceInterfaces.Estate;
using SilverSim.Types;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace SilverSim.Database.MsSql.Estate
{
    public sealed partial class MsSqlEstateService : IEstateGroupsServiceInterface, IEstateGroupsServiceListAccessInterface
    {
        List<UGI> IEstateGroupsServiceListAccessInterface.this[uint estateID]
        {
            get
            {
                var estategroups = new List<UGI>();
                using (var conn = new SqlConnection(m_ConnectionString))
                {
                    conn.Open();
                    using (var cmd = new SqlCommand("SELECT GroupID FROM estate_groups WHERE EstateID = @estateid", conn))
                    {
                        cmd.Parameters.AddParameter("@estateid", estateID);
                        using (SqlDataReader reader = cmd.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                var ugi = new UGI
                                {
                                    ID = reader.GetUUID("GroupID")
                                };
                                estategroups.Add(ugi);
                            }
                        }
                    }
                }
                return estategroups;
            }
        }

        bool IEstateGroupsServiceInterface.this[uint estateID, UGI group]
        {
            get
            {
                using (var conn = new SqlConnection(m_ConnectionString))
                {
                    conn.Open();
                    using (var cmd = new SqlCommand("SELECT TOP(1) GroupID FROM estate_groups WHERE EstateID = @estateid AND GroupID = @groupid", conn))
                    {
                        cmd.Parameters.AddParameter("@estateid", estateID);
                        cmd.Parameters.AddParameter("@groupid", group.ID);
                        using (SqlDataReader reader = cmd.ExecuteReader())
                        {
                            return reader.Read();
                        }
                    }
                }
            }
            set
            {
                using (var conn = new SqlConnection(m_ConnectionString))
                {
                    conn.Open();
                    if (value)
                    {
                        var vals = new Dictionary<string, object>
                        {
                            ["EstateID"] = estateID,
                            ["GroupID"] = group.ID
                        };
                        conn.ReplaceInto("estate_groups", vals, new string[] { "EstateID", "GroupID" });
                    }
                    else
                    {
                        using (var cmd = new SqlCommand("DELETE FROM estate_groups WHERE EstateID = @estateid AND GroupID = @groupid", conn))
                        {
                            cmd.Parameters.AddParameter("@estateid", estateID);
                            cmd.Parameters.AddParameter("@groupid", group.ID);
                            if (cmd.ExecuteNonQuery() < 1)
                            {
                                throw new EstateUpdateFailedException();
                            }
                        }
                    }
                }
            }
        }

        IEstateGroupsServiceListAccessInterface IEstateGroupsServiceInterface.All => this;
    }
}
