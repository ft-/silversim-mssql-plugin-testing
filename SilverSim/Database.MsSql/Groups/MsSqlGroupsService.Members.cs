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

using SilverSim.ServiceInterfaces.Groups;
using SilverSim.Types;
using SilverSim.Types.Groups;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace SilverSim.Database.MsSql.Groups
{
    public sealed partial class MsSqlGroupsService : IGroupMembersInterface
    {
        List<GroupMember> IGroupMembersInterface.this[UGUI requestingAgent, UGUI principal]
        {
            get
            {
                var members = new List<GroupMember>();
                using (var conn = new SqlConnection(m_ConnectionString))
                {
                    conn.Open();
                    using (var cmd = new SqlCommand("SELECT m.* FROM groupmemberships AS m WHERE m.PrincipalID = @principalid", conn))
                    {
                        cmd.Parameters.AddParameter("@principalid", principal.ID);
                        using (SqlDataReader reader = cmd.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                GroupMember gmem = reader.ToGroupMember();
                                gmem.Group = ResolveName(requestingAgent, gmem.Group);
                                gmem.Principal = ResolveName(gmem.Principal);
                                members.Add(gmem);
                            }
                        }
                    }
                }
                return members;
            }
        }

        List<GroupMember> IGroupMembersInterface.this[UGUI requestingAgent, UGI group]
        {
            get
            {
                var members = new List<GroupMember>();
                using (var conn = new SqlConnection(m_ConnectionString))
                {
                    conn.Open();
                    using (var cmd = new SqlCommand("SELECT m.* FROM groupmemberships AS m WHERE m.GroupID = @groupid", conn))
                    {
                        cmd.Parameters.AddParameter("@groupid", group.ID);
                        using (SqlDataReader reader = cmd.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                GroupMember gmem = reader.ToGroupMember();
                                gmem.Group = ResolveName(requestingAgent, gmem.Group);
                                gmem.Principal = ResolveName(gmem.Principal);
                                members.Add(gmem);
                            }
                        }
                    }
                }
                return members;
            }
        }

        GroupMember IGroupMembersInterface.Add(UGUI requestingAgent, UGI group, UGUI principal, UUID roleID, string accessToken)
        {
            var vals = new Dictionary<string, object>
            {
                ["GroupID"] = group.ID,
                ["PrincipalID"] = principal.ID,
                ["SelectedRoleID"] = roleID,
                ["AccessToken"] = accessToken
            };
            using (var conn = new SqlConnection(m_ConnectionString))
            {
                conn.Open();
                conn.InsertInto("groupmemberships", vals);
            }

            return new GroupMember
            {
                Principal = principal,
                Group = group,
                IsAcceptNotices = true,
                IsListInProfile = true,
                AccessToken = accessToken,
                SelectedRoleID = roleID
            };
        }

        bool IGroupMembersInterface.ContainsKey(UGUI requestingAgent, UGI group, UGUI principal)
        {
            using (var conn = new SqlConnection(m_ConnectionString))
            {
                conn.Open();
                using (var cmd = new SqlCommand("SELECT TOP(1) GroupID FROM groupmemberships WHERE GroupID = @groupid AND PrincipalID = @principalid", conn))
                {
                    cmd.Parameters.AddParameter("@groupid", group.ID);
                    cmd.Parameters.AddParameter("@principalid", principal.ID);
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        return reader.Read();
                    }
                }
            }
        }

        void IGroupMembersInterface.Delete(UGUI requestingAgent, UGI group, UGUI principal)
        {
            using (var conn = new SqlConnection(m_ConnectionString))
            {
                conn.Open();
                using (var cmd = new SqlCommand("DELETE FROM groupmemberships WHERE GroupID = @groupid AND PrincipalID = @principalid", conn))
                {
                    cmd.Parameters.AddParameter("@principalid", principal.ID);
                    cmd.Parameters.AddParameter("@groupid", group.ID);
                    if (cmd.ExecuteNonQuery() < 1)
                    {
                        throw new KeyNotFoundException();
                    }
                }
            }
        }

        void IGroupMembersInterface.SetContribution(UGUI requestingagent, UGI group, UGUI principal, int contribution)
        {
            using (var conn = new SqlConnection(m_ConnectionString))
            {
                conn.Open();
                using (var cmd = new SqlCommand("UPDATE groupmemberships SET Contribution=@contribution WHERE GroupID = @groupid AND PrincipalID = @principalid", conn))
                {
                    cmd.Parameters.AddParameter("@contribution", contribution);
                    cmd.Parameters.AddParameter("@principalid", principal.ID);
                    cmd.Parameters.AddParameter("@groupid", group.ID);
                    if (cmd.ExecuteNonQuery() < 1)
                    {
                        throw new KeyNotFoundException();
                    }
                }
            }
        }

        bool IGroupMembersInterface.TryGetValue(UGUI requestingAgent, UGI group, UGUI principal, out GroupMember gmem)
        {
            gmem = null;
            using (var conn = new SqlConnection(m_ConnectionString))
            {
                conn.Open();
                using (var cmd = new SqlCommand("SELECT TOP(1) * FROM groupmemberships WHERE GroupID = @groupid AND PrincipalID = @principalid", conn))
                {
                    cmd.Parameters.AddParameter("@groupid", group.ID);
                    cmd.Parameters.AddParameter("@principalid", principal.ID);
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            gmem = reader.ToGroupMember();
                            gmem.Group = ResolveName(requestingAgent, gmem.Group);
                            gmem.Principal = ResolveName(gmem.Principal);
                            return true;
                        }
                    }
                }
            }
            return false;
        }

        void IGroupMembersInterface.Update(UGUI requestingagent, UGI group, UGUI principal, bool acceptNotices, bool listInProfile)
        {
            using (var conn = new SqlConnection(m_ConnectionString))
            {
                conn.Open();
                using (var cmd = new SqlCommand("UPDATE groupmemberships SET AcceptNotices=@acceptnotices, ListInProfile=@listinprofile WHERE GroupID = @groupid AND PrincipalID = @principalid", conn))
                {
                    cmd.Parameters.AddParameter("@acceptnotices", acceptNotices);
                    cmd.Parameters.AddParameter("@listinprofile", listInProfile);
                    cmd.Parameters.AddParameter("@groupid", group.ID);
                    cmd.Parameters.AddParameter("@principalid", principal.ID);
                    if (cmd.ExecuteNonQuery() < 1)
                    {
                        throw new KeyNotFoundException();
                    }
                }
            }
        }
    }
}
