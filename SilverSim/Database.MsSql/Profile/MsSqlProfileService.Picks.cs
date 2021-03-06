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

using SilverSim.ServiceInterfaces.Profile;
using SilverSim.Types;
using SilverSim.Types.Profile;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace SilverSim.Database.MsSql.Profile
{
    public sealed partial class MsSqlProfileService : IPicksInterface
    {
        Dictionary<UUID, string> IPicksInterface.GetPicks(UGUI user)
        {
            var res = new Dictionary<UUID, string>();
            using (var conn = new SqlConnection(m_ConnectionString))
            {
                conn.Open();
                using (var cmd = new SqlCommand("SELECT pickuuid, [name] FROM userpicks WHERE creatoruuid = @uuid", conn))
                {
                    cmd.Parameters.AddParameter("@uuid", user.ID);
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            res.Add(reader.GetUUID("pickuuid"), (string)reader["name"]);
                        }
                        return res;
                    }
                }
            }
        }

        bool IPicksInterface.ContainsKey(UGUI user, UUID id)
        {
            using (var conn = new SqlConnection(m_ConnectionString))
            {
                conn.Open();
                using (var cmd = new SqlCommand("SELECT TOP(1) pickuuid FROM userpicks WHERE pickuuid = @uuid", conn))
                {
                    cmd.Parameters.AddParameter("@uuid", id);
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

        bool IPicksInterface.TryGetValue(UGUI user, UUID id, out ProfilePick pick)
        {
            using (var conn = new SqlConnection(m_ConnectionString))
            {
                conn.Open();
                using (var cmd = new SqlCommand("SELECT TOP(1) * FROM userpicks WHERE pickuuid = @uuid", conn))
                {
                    cmd.Parameters.AddParameter("@uuid", id);
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            pick = new ProfilePick();
                            pick.Creator.ID = reader.GetUUID("creatoruuid");
                            pick.Description = (string)reader["description"];
                            pick.Enabled = (bool)reader["enabled"];
                            pick.Name = (string)reader["name"];
                            pick.OriginalName = (string)reader["originalname"];
                            pick.ParcelID = reader.GetParcelID("parceluuid");
                            pick.PickID = reader.GetUUID("pickuuid");
                            pick.SimName = (string)reader["simname"];
                            pick.SnapshotID = reader.GetUUID("snapshotuuid");
                            pick.SortOrder = (int)reader["sortorder"];
                            pick.TopPick = (bool)reader["toppick"];
                            pick.GlobalPosition = reader.GetVector3("posglobal");
                            pick.ParcelName = (string)reader["parcelname"];
                            return true;
                        }
                    }
                }
            }

            pick = default(ProfilePick);
            return false;
        }

        void IPicksInterface.Update(ProfilePick value)
        {
            var replaceVals = new Dictionary<string, object>
            {
                ["pickuuid"] = value.PickID,
                ["creatoruuid"] = value.Creator.ID,
                ["toppick"] = value.TopPick,
                ["parceluuid"] = value.ParcelID,
                ["name"] = value.Name,
                ["description"] = value.Description,
                ["snapshotuuid"] = value.SnapshotID,
                ["parcelname"] = value.ParcelName,
                ["originalname"] = value.OriginalName,
                ["simname"] = value.SimName,
                ["posglobal"] = value.GlobalPosition,
                ["sortorder"] = value.SortOrder,
                ["enabled"] = value.Enabled
            };
            using (var conn = new SqlConnection(m_ConnectionString))
            {
                conn.Open();
                conn.ReplaceInto("userpicks", replaceVals, new string[] { "pickuuid" });
            }
        }

        void IPicksInterface.Delete(UUID id)
        {
            using (var conn = new SqlConnection(m_ConnectionString))
            {
                conn.Open();
                using (var cmd = new SqlCommand("DELETE FROM userpicks WHERE pickuuid = @pickuuid", conn))
                {
                    cmd.Parameters.AddParameter("@pickuuid", id);
                    if (1 > cmd.ExecuteNonQuery())
                    {
                        throw new InvalidOperationException();
                    }
                }
            }
        }
    }
}
