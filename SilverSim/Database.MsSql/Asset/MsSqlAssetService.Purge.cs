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

using SilverSim.ServiceInterfaces.Purge;
using SilverSim.ServiceInterfaces.Statistics;
using SilverSim.Threading;
using SilverSim.Types;
using SilverSim.Types.Asset;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading;

namespace SilverSim.Database.MsSql.Asset
{
    public sealed partial class MsSqlAssetService : IAssetPurgeServiceInterface, IQueueStatsAccess
    {
        public void MarkAssetAsUsed(List<UUID> assetIDs)
        {
            string ids = "'" + string.Join("','", assetIDs) + "'";
            string sql = string.Format("UPDATE assetrefs SET access_time=@access_time WHERE id IN ({0})", ids);

            using (var conn = new SqlConnection(m_ConnectionString))
            {
                conn.Open();
                using (var cmd = new SqlCommand(sql, conn))
                {
                    ulong now = Date.GetUnixTime();
                    cmd.Parameters.AddParameter("@access_time", now);
                    cmd.ExecuteNonQuery();
                }
            }
        }

        private int m_ProcessingPurge;
        private string m_PurgeState = "IDLE";

        public long PurgeUnusedAssets()
        {
            long purged;
            try
            {
                using (var conn = new SqlConnection(m_ConnectionString))
                {
                    conn.Open();
                    m_PurgeState = "PURGE_REFS";
                    using (var cmd = new SqlCommand("DELETE FROM assetrefs WHERE usesprocessed = 1 AND access_time < @access_time AND NOT EXISTS (SELECT TOP(1) NULL FROM assetsinuse WHERE usesid = assetrefs.id)", conn))
                    {
                        ulong now = Date.GetUnixTime() - 2 * 24 * 3600;
                        cmd.Parameters.AddParameter("@access_time", now);
                        purged = cmd.ExecuteNonQuery();
                    }
                    m_PurgeState = "PURGE_USES";
                    int removed = 1000;
                    int execres;
                    do
                    {
                        m_ProcessingPurge = removed;
                        using (var cmd = new SqlCommand("DELETE TOP(1) FROM assetsinuse WHERE NOT EXISTS (SELECT TOP(1) NULL FROM assetrefs WHERE assetsinuse.id = assetrefs.id)", conn))
                        {
                            execres = cmd.ExecuteNonQuery();
                        }
                        removed -= execres;
                        Interlocked.Add(ref m_PurgedAssets, execres);
                    } while (removed > 0 && execres > 0);

                    m_PurgeState = "PURGE_DATA";
                    removed = 1000;
                    do
                    {
                        m_ProcessingPurge = removed;
                        using (var cmd = new SqlCommand("DELETE TOP(1) FROM assetdata WHERE NOT EXISTS (SELECT TOP(1) NULL FROM assetrefs WHERE assetdata.hash = assetrefs.hash AND assetdata.assetType = assetrefs.assetType)", conn))
                        {
                            execres = cmd.ExecuteNonQuery();
                        }
                        removed -= execres;
                        Interlocked.Add(ref m_PurgedAssets, execres);
                    } while (removed > 0 && execres > 0);
                }
            }
            finally
            {
                m_PurgeState = "IDLE";
                m_ProcessingPurge = 0;
            }

            return purged;
        }

        private void GenerateAssetInUseEntries(AssetData data)
        {
            List<UUID> references = data.References;
            using (var conn = new SqlConnection(m_ConnectionString))
            {
                conn.Open();
                UUID assetid = data.ID;
                foreach (UUID refid in references)
                {
                    if (refid == assetid || refid == UUID.Zero)
                    {
                        continue;
                    }
                    using (var cmd = new SqlCommand("REPLACE INTO assetsinuse (`id`, `usesid`) VALUES (@id, @usesid)", conn))
                    {
                        cmd.Parameters.AddParameter("@id", assetid);
                        cmd.Parameters.AddParameter("@usesid", refid);
                        cmd.ExecuteNonQuery();
                    }
                }
                using (var cmd = new SqlCommand("UPDATE assetrefs SET usesprocessed = 1 WHERE id = @id", conn))
                {
                    cmd.Parameters.AddParameter("@id", assetid);
                    cmd.ExecuteNonQuery();
                }
            }
        }

        public List<UUID> GetUnprocessedAssets()
        {
            var assets = new List<UUID>();
            using (var conn = new SqlConnection(m_ConnectionString))
            {
                conn.Open();
                using (var cmd = new SqlCommand("SELECT TOP(1000) id FROM assetrefs WHERE usesprocessed = 0", conn))
                {
                    using (SqlDataReader dbReader = cmd.ExecuteReader())
                    {
                        while (dbReader.Read())
                        {
                            assets.Add(dbReader.GetUUID("id"));
                        }
                    }
                }
            }
            return assets;
        }

        private readonly BlockingQueue<UUID> m_AssetProcessQueue = new BlockingQueue<UUID>();
        private int m_ActiveAssetProcessors;
        private int m_Processed;
        private int m_PurgedAssets;

        public void EnqueueAsset(UUID assetid)
        {
            m_AssetProcessQueue.Enqueue(assetid);
            if (m_ActiveAssetProcessors == 0)
            {
                ThreadPool.QueueUserWorkItem(AssetProcessor);
            }
        }

        private void AssetProcessor(object state)
        {
            Interlocked.Increment(ref m_ActiveAssetProcessors);
            while (m_AssetProcessQueue.Count > 0)
            {
                UUID assetid;
                try
                {
                    assetid = m_AssetProcessQueue.Dequeue(1000);
                }
                catch
                {
                    Interlocked.Decrement(ref m_ActiveAssetProcessors);
                    if (m_AssetProcessQueue.Count == 0)
                    {
                        break;
                    }
                    Interlocked.Increment(ref m_ActiveAssetProcessors);
                    continue;
                }

                AssetData asset;
                try
                {
                    asset = this[assetid];
                }
                catch
                {
                    continue;
                }

                try
                {
                    GenerateAssetInUseEntries(asset);
                }
                catch
                {
                    m_AssetProcessQueue.Enqueue(asset.ID);
                }
                Interlocked.Increment(ref m_Processed);
                asset = null; /* ensure cleanup */
            }
        }

        private QueueStat GetProcessorQueueStats()
        {
            int c = m_AssetProcessQueue.Count;
            return new QueueStat(c != 0 ? "PROCESSING" : "IDLE", c, (uint)m_Processed);
        }

        private QueueStat GetPurgeQueueStats()
        {
            int c = m_ProcessingPurge;
            return new QueueStat(m_PurgeState, c, (uint)m_PurgedAssets);
        }

        IList<QueueStatAccessor> IQueueStatsAccess.QueueStats => new List<QueueStatAccessor>
        {
            new QueueStatAccessor("AssetReferences", GetProcessorQueueStats),
            new QueueStatAccessor("AssetPurges", GetPurgeQueueStats)
        };
    }
}
