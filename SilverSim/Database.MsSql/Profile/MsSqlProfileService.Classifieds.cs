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
    public sealed partial class MsSqlProfileService : IClassifiedsInterface
    {
        Dictionary<UUID, string> IClassifiedsInterface.GetClassifieds(UGUI user)
        {
            var res = new Dictionary<UUID, string>();
            using (var conn = new SqlConnection(m_ConnectionString))
            {
                conn.Open();
                using (var cmd = new SqlCommand("SELECT classifieduuid, [name] FROM classifieds WHERE creatoruuid = @uuid", conn))
                {
                    cmd.Parameters.AddParameter("@uuid", user.ID);
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            res.Add(reader.GetUUID("classifieduuid"), (string)reader["name"]);
                        }
                        return res;
                    }
                }
            }
        }

        bool IClassifiedsInterface.TryGetValue(UGUI user, UUID id, out ProfileClassified classified)
        {
            using (var conn = new SqlConnection(m_ConnectionString))
            {
                conn.Open();
                using (var cmd = new SqlCommand("SELECT TOP(1) * FROM classifieds WHERE classifieduuid = @uuid", conn))
                {
                    cmd.Parameters.AddParameter("@uuid", id);
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            classified = new ProfileClassified
                            {
                                ClassifiedID = reader.GetUUID("classifieduuid"),
                                Category = (int)reader["category"],
                                CreationDate = reader.GetDate("creationdate"),
                                Creator = new UGUI(reader.GetUUID("creatoruuid")),
                                Description = (string)reader["description"],
                                ExpirationDate = reader.GetDate("expirationdate"),
                                Flags = (byte)(int)reader["classifiedflags"],
                                GlobalPos = reader.GetVector3("posglobal"),
                                Name = (string)reader["name"],
                                ParcelID = reader.GetParcelID("parceluuid"),
                                ParcelName = (string)reader["parcelname"],
                                ParentEstate = (int)reader["parentestate"],
                                Price = (int)reader["priceforlisting"],
                                SimName = (string)reader["simname"],
                                SnapshotID = reader.GetUUID("snapshotuuid")
                            };
                            return true;
                        }
                    }
                }
            }
            classified = default(ProfileClassified);
            return false;
        }

        bool IClassifiedsInterface.ContainsKey(UGUI user, UUID id)
        {
            using (var conn = new SqlConnection(m_ConnectionString))
            {
                conn.Open();
                using (var cmd = new SqlCommand("SELECT TOP(1) classifieduuid FROM classifieds WHERE classifieduuid = @uuid", conn))
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

        void IClassifiedsInterface.Update(ProfileClassified c)
        {
            var replaceVals = new Dictionary<string, object>
            {
                ["classifieduuid"] = c.ClassifiedID,
                ["creatoruuid"] = c.Creator.ID,
                ["creationdate"] = c.CreationDate,
                ["expirationdate"] = c.ExpirationDate,
                ["category"] = c.Category,
                ["name"] = c.Name,
                ["description"] = c.Description,
                ["parceluuid"] = c.ParcelID,
                ["parentestate"] = c.ParentEstate,
                ["snapshotuuid"] = c.SnapshotID,
                ["simname"] = c.SimName,
                ["posglobal"] = c.GlobalPos,
                ["parcelname"] = c.ParcelName,
                ["classifiedflags"] = c.Flags,
                ["priceforlisting"] = c.Price
            };
            using (var conn = new SqlConnection())
            {
                conn.Open();
                conn.ReplaceInto("classifieds", replaceVals, new string[] { "classifieduuid" });
            }
        }

        void IClassifiedsInterface.Delete(UUID id)
        {
            using (var conn = new SqlConnection(m_ConnectionString))
            {
                conn.Open();
                using (var cmd = new SqlCommand("DELETE FROM classifieds WHERE classifieduuid = @classifieduuid", conn))
                {
                    cmd.Parameters.AddParameter("@classifieduuid", id);
                    if (1 > cmd.ExecuteNonQuery())
                    {
                        throw new InvalidOperationException();
                    }
                }
            }
        }
    }
}
