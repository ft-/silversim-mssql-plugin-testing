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
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using MsSqlMigrationException = SilverSim.Database.MsSql.MsSqlUtilities.MsSqlMigrationException;

namespace SilverSim.Database.MsSql._Migration
{
    public static partial class Migrator
    {
        static void ExecuteStatement(SqlConnection conn, string command, ILog log, SqlTransaction transaction)
        {
            try
            {
                using (var cmd = new SqlCommand(command, conn)
                {
                    Transaction = transaction
                })
                {
                    cmd.ExecuteNonQuery();
                }
            }
            catch
            {
                log.Debug(command);
                throw;
            }
        }

        static void CreateTable(
            this SqlConnection conn,
            SqlTable table,
            PrimaryKeyInfo primaryKey,
            Dictionary<string, IColumnInfo> fields,
            Dictionary<string, NamedKeyInfo> tableKeys,
            uint tableRevision,
            ILog log,
            SqlTransaction transaction)
        {
            var b = new SqlCommandBuilder();
            log.InfoFormat("Creating table '{0}' at revision {1}", table.Name, tableRevision);
            var fieldSqls = new List<string>();
            foreach (IColumnInfo field in fields.Values)
            {
                fieldSqls.Add(field.FieldSql(table.Name));
            }
            if (null != primaryKey)
            {
                fieldSqls.Add(primaryKey.FieldSql());
            }

            string escapedTableName = b.QuoteIdentifier(table.Name);
            var cmd = new StringBuilder("CREATE TABLE " + escapedTableName + " (");
            cmd.Append(string.Join(",", fieldSqls));
            cmd.Append(");");
            foreach (NamedKeyInfo key in tableKeys.Values)
            {
                cmd.Append(key.Sql(table.Name));
            }
            cmd.AppendFormat("EXEC sys.{2} @name=N'table_revision', " +
            "@value = N'{1}', @level0type = N'SCHEMA', @level0name = N'dbo'," +
            "@level1type = N'TABLE', @level1name = N'{0}';", table.Name, tableRevision, "sp_addextendedproperty");
            ExecuteStatement(conn, cmd.ToString(), log, transaction);
        }

        private static void CommentTable(this SqlConnection conn, string tablename, uint revision, ILog log, SqlTransaction transaction)
        {
            ExecuteStatement(conn, string.Format("EXEC sys.{2} @name=N'table_revision', " +
            "@value = N'{1}', @level0type = N'SCHEMA', @level0name = N'dbo'," +
            "@level1type = N'TABLE', @level1name = N'{0}';", tablename, revision, revision == 1 ? "sp_addextendedproperty" : "sp_updateextendedproperty"), log, transaction);
        }

        public static void MigrateTables(this SqlConnection conn, IMigrationElement[] processTable, ILog log)
        {
            var b = new SqlCommandBuilder();
            var tableFields = new Dictionary<string, IColumnInfo>();
            PrimaryKeyInfo primaryKey = null;
            var tableKeys = new Dictionary<string, NamedKeyInfo>();
            SqlTable table = null;
            uint processingTableRevision = 0;
            uint currentAtRevision = 0;
            SqlTransaction insideTransaction = null;
            m_MaxAvailableMigrationRevision = 1;

            if (processTable.Length == 0)
            {
                throw new MsSqlMigrationException("Invalid MsSql migration");
            }

            if (null == processTable[0] as SqlTable)
            {
                throw new MsSqlMigrationException("First entry must be table name");
            }

            bool skipToNext = false;

            foreach (IMigrationElement migration in processTable)
            {
                Type migrationType = migration.GetType();

                if (typeof(SqlTable) == migrationType)
                {
                    skipToNext = false;
                    if (insideTransaction != null)
                    {
                        CommentTable(conn, table.Name, processingTableRevision, log, insideTransaction);
                        insideTransaction.Commit();
                        insideTransaction = null;
                    }

                    if (null != table && 0 != processingTableRevision)
                    {
                        if (currentAtRevision == 0)
                        {
                            conn.CreateTable(
                                table,
                                primaryKey,
                                tableFields,
                                tableKeys,
                                processingTableRevision,
                                log, insideTransaction);
                        }
                        tableFields.Clear();
                        tableKeys.Clear();
                        primaryKey = null;
                    }
                    table = (SqlTable)migration;
                    currentAtRevision = conn.GetTableRevision(table.Name);
                    processingTableRevision = 1;
                    if (currentAtRevision != 0 && m_DeleteTablesBefore)
                    {
                        log.Info($"Dropping table {table.Name}");
                        ExecuteStatement(conn, $"DROP TABLE {table.Name}", log, insideTransaction);
                        currentAtRevision = 0;
                    }
                }
                else if (skipToNext)
                {
                    /* skip processing */
                    if (typeof(TableRevision) == migrationType)
                    {
                        m_MaxAvailableMigrationRevision = Math.Max(m_MaxAvailableMigrationRevision, ((TableRevision)migration).Revision);
                    }
                }
                else if (typeof(TableRevision) == migrationType)
                {
                    if (insideTransaction != null)
                    {
                        CommentTable(conn, table.Name, processingTableRevision, log, insideTransaction);
                        insideTransaction.Commit();
                        insideTransaction = null;
                        if (currentAtRevision != 0)
                        {
                            currentAtRevision = processingTableRevision;
                        }
                    }

                    var rev = (TableRevision)migration;
                    m_MaxAvailableMigrationRevision = Math.Max(m_MaxAvailableMigrationRevision, rev.Revision);
                    if (processingTableRevision == m_StopAtMigrationRevision)
                    {
                        /* advance to next table for testing */
                        skipToNext = true;
                        continue;
                    }

                    if (rev.Revision != processingTableRevision + 1)
                    {
                        throw new MsSqlMigrationException(string.Format("Invalid TableRevision entry. Expected {0}. Got {1}", processingTableRevision + 1, rev.Revision));
                    }

                    processingTableRevision = rev.Revision;

                    if (processingTableRevision - 1 == currentAtRevision && 0 != currentAtRevision)
                    {
                        insideTransaction = conn.BeginTransaction(System.Data.IsolationLevel.Serializable);
                        log.InfoFormat("Migration table '{0}' to revision {1}", table.Name, processingTableRevision);
                    }
                }
                else if (processingTableRevision == 0 || table == null)
                {
                    if (table != null)
                    {
                        throw new MsSqlMigrationException("Unexpected processing element for " + table.Name);
                    }
                    else
                    {
                        throw new MsSqlMigrationException("Unexpected processing element");
                    }
                }
                else
                {
                    Type[] interfaces = migration.GetType().GetInterfaces();

                    if (interfaces.Contains(typeof(IAddColumn)))
                    {
                        var columnInfo = (IAddColumn)migration;
                        if (tableFields.ContainsKey(columnInfo.Name))
                        {
                            throw new ArgumentException("Column " + columnInfo.Name + " was added twice.");
                        }
                        tableFields.Add(columnInfo.Name, columnInfo);
                        if (insideTransaction != null)
                        {
                            ExecuteStatement(conn, columnInfo.Sql(table.Name), log, insideTransaction);
                        }
                    }
                    else if (interfaces.Contains(typeof(IChangeColumn)))
                    {
                        var columnInfo = (IChangeColumn)migration;
                        IColumnInfo oldColumn;
                        if (columnInfo.OldName?.Length != 0)
                        {
                            if (!tableFields.TryGetValue(columnInfo.OldName, out oldColumn))
                            {
                                throw new ArgumentException("Change column for " + columnInfo.Name + " has no preceeding AddColumn for " + columnInfo.OldName);
                            }
                        }
                        else if (!tableFields.TryGetValue(columnInfo.Name, out oldColumn))
                        {
                            throw new ArgumentException("Change column for " + columnInfo.Name + " has no preceeding AddColumn");
                        }
                        if (insideTransaction != null)
                        {
                            ExecuteStatement(conn, columnInfo.Sql(table.Name, oldColumn.FieldType), log, insideTransaction);
                        }
                        if (columnInfo.OldName?.Length != 0)
                        {
                            tableFields.Remove(columnInfo.OldName);
                            if(primaryKey != null)
                            {
                                string[] fields = primaryKey.FieldNames;
                                int n = fields.Length;
                                for (int i = 0; i < n; ++i)
                                {
                                    if (fields[i] == columnInfo.OldName)
                                    {
                                        fields[i] = columnInfo.Name;
                                    }
                                }
                            }
                            foreach (NamedKeyInfo keyinfo in tableKeys.Values)
                            {
                                string[] fields = keyinfo.FieldNames;
                                int n = fields.Length;
                                for (int i = 0; i < n; ++i)
                                {
                                    if (fields[i] == columnInfo.OldName)
                                    {
                                        fields[i] = columnInfo.Name;
                                    }
                                }
                            }
                        }
                        tableFields[columnInfo.Name] = columnInfo;
                    }
                    else if (migrationType == typeof(DropColumn))
                    {
                        var columnInfo = (DropColumn)migration;
                        if (insideTransaction != null)
                        {
                            ExecuteStatement(conn, columnInfo.Sql(table.Name, tableFields[columnInfo.Name].FieldType), log, insideTransaction);
                        }
                        tableFields.Remove(columnInfo.Name);
                    }
                    else if (migrationType == typeof(PrimaryKeyInfo))
                    {
                        if (null != primaryKey && insideTransaction != null)
                        {
                            ExecuteStatement(conn, "DECLARE @SQL VARCHAR(4000);" +
                                "SET @SQL = 'ALTER TABLE " + b.QuoteIdentifier(table.Name) + " DROP CONSTRAINT |ConstraintName| ';" +
                                "SET @SQL = REPLACE(@SQL, '|ConstraintName|', ( SELECT   name " +
                                               "FROM sysobjects " +
                                               "WHERE xtype = 'PK' " +
                                                      " AND parent_obj = OBJECT_ID(" + table.Name.ToMsSqlQuoted() + ")));" +
                                "EXEC(@SQL);", log, insideTransaction);
                        }
                        primaryKey = new PrimaryKeyInfo((PrimaryKeyInfo)migration);
                        if (insideTransaction != null)
                        {
                            ExecuteStatement(conn, primaryKey.Sql(table.Name), log, insideTransaction);
                        }
                    }
                    else if (migrationType == typeof(DropPrimaryKeyinfo))
                    {
                        if (null != primaryKey && insideTransaction != null)
                        {
                            ExecuteStatement(conn, ((DropPrimaryKeyinfo)migration).Sql(table.Name), log, insideTransaction);
                        }
                        primaryKey = null;
                    }
                    else if (migrationType == typeof(NamedKeyInfo))
                    {
                        var namedKey = (NamedKeyInfo)migration;
                        tableKeys.Add(namedKey.Name, new NamedKeyInfo(namedKey));
                        if (insideTransaction != null)
                        {
                            ExecuteStatement(conn, namedKey.Sql(table.Name), log, insideTransaction);
                        }
                    }
                    else if (migrationType == typeof(DropNamedKeyInfo))
                    {
                        var namedKey = (DropNamedKeyInfo)migration;
                        tableKeys.Remove(namedKey.Name);
                        if (insideTransaction != null)
                        {
                            ExecuteStatement(conn, namedKey.Sql(table.Name), log, insideTransaction);
                        }
                    }
                    else
                    {
                        throw new MsSqlMigrationException("Invalid type " + migrationType.FullName + " in migration list");
                    }
                }
            }

            if (insideTransaction != null)
            {
                CommentTable(conn, table.Name, processingTableRevision, log, insideTransaction);
                insideTransaction.Commit();
                if (currentAtRevision != 0)
                {
                    currentAtRevision = processingTableRevision;
                }
                insideTransaction = null;
            }

            if (null != table && 0 != processingTableRevision && currentAtRevision == 0)
            {
                conn.CreateTable(
                    table,
                    primaryKey,
                    tableFields,
                    tableKeys,
                    processingTableRevision,
                    log, insideTransaction);
            }
        }
    }
}
