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
using SilverSim.Main.Common;
using SilverSim.Scene.Types.SceneEnvironment;
using SilverSim.Types;
using SilverSim.Types.StructuredData.Llsd;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

namespace SilverSim.Database.MsSql
{
    public static class MsSqlUtilities
    {
        #region Connection String Creator
        public static string BuildConnectionString(IConfig config, ILog log)
        {
            var sb = new SqlConnectionStringBuilder();

            if (!(config.Contains("Server") && config.Contains("Database")))
            {
                string configName = config.Name;
                if (!config.Contains("Server"))
                {
                    log.FatalFormat("[MSSQL CONFIG]: Parameter 'Server' missing in [{0}]", configName);
                }
                if (!config.Contains("Database"))
                {
                    log.FatalFormat("[MSSQL CONFIG]: Parameter 'Database' missing in [{0}]", configName);
                }
                throw new ConfigurationLoader.ConfigurationErrorException("Incomplete database reference");
            }

            sb.DataSource = config.GetString("Server", "localhost");

            if (config.Contains("Username"))
            {
                sb.UserID = config.GetString("Username");
                sb.IntegratedSecurity = false;
            }
            if (config.Contains("Password"))
            {
                sb.Password = config.GetString("Password");
                sb.IntegratedSecurity = false;
            }
            sb.InitialCatalog = config.GetString("Database");

            if (config.Contains("MaximumPoolsize"))
            {
                sb.MaxPoolSize = config.GetInt("MaximumPoolsize");
            }

            return sb.ToString();
        }
        #endregion

        #region Exceptions
        [Serializable]
        public class MsSqlInsertException : Exception
        {
            public MsSqlInsertException()
            {
            }

            public MsSqlInsertException(string msg)
                : base(msg)
            {
            }

            protected MsSqlInsertException(SerializationInfo info, StreamingContext context)
                : base(info, context)
            {
            }

            public MsSqlInsertException(string msg, Exception innerException)
                : base(msg, innerException)
            {
            }
        }

        [Serializable]
        public class MsSqlMigrationException : Exception
        {
            public MsSqlMigrationException()
            {
            }

            public MsSqlMigrationException(string msg)
                : base(msg)
            {
            }

            protected MsSqlMigrationException(SerializationInfo info, StreamingContext context)
                : base(info, context)
            {
            }

            public MsSqlMigrationException(string msg, Exception innerException)
                : base(msg, innerException)
            {
            }
        }
        #endregion

        #region Transaction Helper
        public static void InsideTransaction(this SqlConnection connection, Action<SqlTransaction> del)
        {
            InsideTransaction(connection, IsolationLevel.Serializable, del);
        }

        public static void InsideTransaction(this SqlConnection connection, IsolationLevel level, Action<SqlTransaction> del)
        {
            SqlTransaction transaction = connection.BeginTransaction(level);
            try
            {
                del(transaction);
            }
            catch
            {
                transaction.Rollback();
                throw;
            }
            transaction.Commit();
        }
        public static T InsideTransaction<T>(this SqlConnection connection, Func<SqlTransaction, T> del) =>
            InsideTransaction(connection, IsolationLevel.Serializable, del);

        public static T InsideTransaction<T>(this SqlConnection connection, IsolationLevel level, Func<SqlTransaction, T> del)
        {
            T ret;
            SqlTransaction transaction = connection.BeginTransaction(level);
            try
            {
                ret = del(transaction);
            }
            catch
            {
                transaction.Rollback();
                throw;
            }
            transaction.Commit();
            return ret;
        }
        #endregion

        #region Push parameters
        public static void AddParameter(this SqlParameterCollection sqlparam, string key, object value)
        {
            var t = value?.GetType();
            if (t == typeof(Vector3))
            {
                var v = (Vector3)value;
                sqlparam.AddWithValue(key + "X", v.X);
                sqlparam.AddWithValue(key + "Y", v.Y);
                sqlparam.AddWithValue(key + "Z", v.Z);
            }
            else if (t == typeof(GridVector))
            {
                var v = (GridVector)value;
                sqlparam.AddWithValue(key + "X", (int)v.X);
                sqlparam.AddWithValue(key + "Y", (int)v.Y);
            }
            else if (t == typeof(Quaternion))
            {
                var v = (Quaternion)value;
                sqlparam.AddWithValue(key + "X", v.X);
                sqlparam.AddWithValue(key + "Y", v.Y);
                sqlparam.AddWithValue(key + "Z", v.Z);
                sqlparam.AddWithValue(key + "W", v.W);
            }
            else if (t == typeof(Color))
            {
                var v = (Color)value;
                sqlparam.AddWithValue(key + "Red", v.R);
                sqlparam.AddWithValue(key + "Green", v.G);
                sqlparam.AddWithValue(key + "Blue", v.B);
            }
            else if (t == typeof(ColorAlpha))
            {
                var v = (ColorAlpha)value;
                sqlparam.AddWithValue(key + "Red", v.R);
                sqlparam.AddWithValue(key + "Green", v.G);
                sqlparam.AddWithValue(key + "Blue", v.B);
                sqlparam.AddWithValue(key + "Alpha", v.A);
            }
            else if (t == typeof(EnvironmentController.WLVector2))
            {
                var vec = (EnvironmentController.WLVector2)value;
                sqlparam.AddWithValue(key + "X", vec.X);
                sqlparam.AddWithValue(key + "Y", vec.Y);
            }
            else if (t == typeof(EnvironmentController.WLVector4))
            {
                var vec = (EnvironmentController.WLVector4)value;
                sqlparam.AddWithValue(key + "Red", vec.X);
                sqlparam.AddWithValue(key + "Green", vec.Y);
                sqlparam.AddWithValue(key + "Blue", vec.Z);
                sqlparam.AddWithValue(key + "Value", vec.W);
            }
            else if (t == typeof(bool))
            {
                sqlparam.AddWithValue(key, (bool)value);
            }
            else if (t == typeof(UUID))
            {
                sqlparam.AddWithValue(key, (Guid)(UUID)value);
            }
            else if (t == typeof(UGUI) || t == typeof(UGUIWithName) || t == typeof(UGI) || t == typeof(Uri) || t == typeof(UEI))
            {
                sqlparam.AddWithValue(key, value.ToString());
            }
            else if (t == typeof(ParcelID))
            {
                ParcelID parcelid = (ParcelID)value;
                UUID id = new UUID(parcelid.GetBytes(), 0);
                sqlparam.AddWithValue(key, (Guid)id);
            }
            else if (t == typeof(AnArray))
            {
                using (var stream = new MemoryStream())
                {
                    LlsdBinary.Serialize((AnArray)value, stream);
                    sqlparam.AddWithValue(key, stream.ToArray());
                }
            }
            else if (t == typeof(Date))
            {
                sqlparam.AddWithValue(key, ((Date)value).AsLong);
            }
            else if (t == typeof(ulong))
            {
                sqlparam.AddWithValue(key, (long)(ulong)value);
            }
            else if (t == typeof(uint))
            {
                sqlparam.AddWithValue(key, (int)(uint)value);
            }
            else if (t == typeof(ushort))
            {
                sqlparam.AddWithValue(key, (short)(ushort)value);
            }
            else if (t == typeof(byte))
            {
                sqlparam.AddWithValue(key, (short)(byte)value);
            }
            else if (t == typeof(sbyte))
            {
                sqlparam.AddWithValue(key, (short)(sbyte)value);
            }
            else if (t.IsEnum)
            {
                Type utype = t.GetEnumUnderlyingType();
                if (utype == typeof(byte) || utype == typeof(sbyte) || utype == typeof(ushort))
                {
                    utype = typeof(short);
                }
                else if (utype == typeof(uint))
                {
                    utype = typeof(int);
                }
                else if (utype == typeof(ulong))
                {
                    utype = typeof(long);
                }

                sqlparam.AddWithValue(key, Convert.ChangeType(value, utype));
            }
            else
            {
                sqlparam.AddWithValue(key, value);
            }
        }

        private static void AddParameters(this SqlParameterCollection sqlparam, Dictionary<string, object> vals)
        {
            foreach (KeyValuePair<string, object> kvp in vals)
            {
                if (kvp.Value != null)
                {
                    AddParameter(sqlparam, "@v_" + kvp.Key, kvp.Value);
                }
            }
        }
        #endregion

        #region REPLACE INTO style helper
        public static void ReplaceInto(this SqlConnection connection, string tablename, Dictionary<string, object> vals, string[] keyfields, SqlTransaction transaction = null)
        {
            var q = new List<string>();
            foreach (KeyValuePair<string, object> kvp in vals)
            {
                object value = kvp.Value;

                var t = value?.GetType();
                string key = kvp.Key;

                if (t == typeof(Vector3))
                {
                    q.Add(key + "X");
                    q.Add(key + "Y");
                    q.Add(key + "Z");
                }
                else if (t == typeof(GridVector) || t == typeof(EnvironmentController.WLVector2))
                {
                    q.Add(key + "X");
                    q.Add(key + "Y");
                }
                else if (t == typeof(Quaternion))
                {
                    q.Add(key + "X");
                    q.Add(key + "Y");
                    q.Add(key + "Z");
                    q.Add(key + "W");
                }
                else if (t == typeof(Color))
                {
                    q.Add(key + "Red");
                    q.Add(key + "Green");
                    q.Add(key + "Blue");
                }
                else if (t == typeof(EnvironmentController.WLVector4))
                {
                    q.Add(key + "Red");
                    q.Add(key + "Green");
                    q.Add(key + "Blue");
                    q.Add(key + "Value");
                }
                else if (t == typeof(ColorAlpha))
                {
                    q.Add(key + "Red");
                    q.Add(key + "Green");
                    q.Add(key + "Blue");
                    q.Add(key + "Alpha");
                }
                else if (value == null)
                {
                    /* skip */
                }
                else
                {
                    q.Add(key);
                }
            }

            var cb = new SqlCommandBuilder();

            var q1 = new StringBuilder();
            string quotedTableName = cb.QuoteIdentifier(tablename);
            var insertIntoParams = new StringBuilder();
            var insertIntoFields = new StringBuilder();
            var updateParams = new StringBuilder();
            var whereParams = new StringBuilder();

            foreach (string p in q)
            {
                string quotedFieldName = cb.QuoteIdentifier(p);
                if (insertIntoParams.Length != 0)
                {
                    insertIntoParams.Append(",");
                    insertIntoFields.Append(",");
                }
                insertIntoParams.Append("@v_");
                insertIntoParams.Append(p);
                insertIntoFields.Append(quotedFieldName);


                if (keyfields.Contains(p))
                {
                    if (whereParams.Length != 0)
                    {
                        whereParams.Append(" AND ");
                    }
                    whereParams.Append(quotedFieldName);
                    whereParams.Append(" = ");
                    whereParams.Append("@v_");
                    whereParams.Append(p);
                }
                else
                {
                    if (updateParams.Length != 0)
                    {
                        updateParams.Append(",");
                    }
                    updateParams.Append(quotedFieldName);
                    updateParams.Append("=");
                    updateParams.Append("@v_");
                    updateParams.Append(p);
                }
            }

            if (updateParams.Length != 0)
            {
                q1.Append("UPDATE ");
                q1.Append(quotedTableName);
                q1.Append(" SET ");
                q1.Append(updateParams);
                q1.Append(" WHERE ");
                q1.Append(whereParams);
            }

            q1.Append("; INSERT INTO ");
            q1.Append(quotedTableName);
            q1.Append(" (");
            q1.Append(insertIntoFields);
            q1.Append(") SELECT ");
            q1.Append(insertIntoParams);
            q1.Append(" WHERE NOT EXISTS (SELECT NULL FROM ");
            q1.Append(quotedTableName);
            q1.Append(" WHERE ");
            q1.Append(whereParams);
            q1.Append(");");

            using (var command = new SqlCommand(q1.ToString(), connection))
            {
                command.Transaction = transaction;
                AddParameters(command.Parameters, vals);
                if (command.ExecuteNonQuery() < 1)
                {
                    throw new MsSqlInsertException();
                }
            }
        }
        #endregion

        #region Common INSERT INTO helper
        public static void InsertInto(this SqlConnection connection, string tablename, Dictionary<string, object> vals, SqlTransaction transaction = null)
        {
            var q = new List<string>();
            foreach (KeyValuePair<string, object> kvp in vals)
            {
                object value = kvp.Value;

                var t = value?.GetType();
                string key = kvp.Key;

                if (t == typeof(Vector3))
                {
                    q.Add(key + "X");
                    q.Add(key + "Y");
                    q.Add(key + "Z");
                }
                else if (t == typeof(GridVector) || t == typeof(EnvironmentController.WLVector2))
                {
                    q.Add(key + "X");
                    q.Add(key + "Y");
                }
                else if (t == typeof(Quaternion))
                {
                    q.Add(key + "X");
                    q.Add(key + "Y");
                    q.Add(key + "Z");
                    q.Add(key + "W");
                }
                else if (t == typeof(Color))
                {
                    q.Add(key + "Red");
                    q.Add(key + "Green");
                    q.Add(key + "Blue");
                }
                else if (t == typeof(EnvironmentController.WLVector4))
                {
                    q.Add(key + "Red");
                    q.Add(key + "Green");
                    q.Add(key + "Blue");
                    q.Add(key + "Value");
                }
                else if (t == typeof(ColorAlpha))
                {
                    q.Add(key + "Red");
                    q.Add(key + "Green");
                    q.Add(key + "Blue");
                    q.Add(key + "Alpha");
                }
                else if (value == null)
                {
                    /* skip */
                }
                else
                {
                    q.Add(key);
                }
            }

            var cb = new SqlCommandBuilder();
            var q1 = new StringBuilder();
            var q2 = new StringBuilder();
            q1.Append("INSERT INTO ");
            q1.Append(cb.QuoteIdentifier(tablename));
            q1.Append(" (");
            q2.Append(") VALUES (");
            bool first = true;
            foreach (string p in q)
            {
                if (!first)
                {
                    q1.Append(",");
                    q2.Append(",");
                }
                first = false;
                q1.Append(cb.QuoteIdentifier(p));
                q2.Append("@v_");
                q2.Append(p);
            }
            q1.Append(q2);
            q1.Append(")");
            using (var command = new SqlCommand(q1.ToString(), connection))
            {
                command.Transaction = transaction;
                AddParameters(command.Parameters, vals);
                if (command.ExecuteNonQuery() < 1)
                {
                    throw new MsSqlInsertException();
                }
            }
        }
        #endregion

        #region UPDATE SET helper
        private static List<string> UpdateSetFromVals(Dictionary<string, object> vals, SqlCommandBuilder b)
        {
            var updates = new List<string>();

            foreach (KeyValuePair<string, object> kvp in vals)
            {
                object value = kvp.Value;
                var t = value?.GetType();
                string key = kvp.Key;

                if (t == typeof(Vector3))
                {
                    updates.Add(b.QuoteIdentifier(key + "X") + " = @v_" + key + "X");
                    updates.Add(b.QuoteIdentifier(key + "Y") + " = @v_" + key + "Y");
                    updates.Add(b.QuoteIdentifier(key + "Z") + " = @v_" + key + "Z");
                }
                else if (t == typeof(GridVector) || t == typeof(EnvironmentController.WLVector2))
                {
                    updates.Add(b.QuoteIdentifier(key + "X") + " = @v_" + key + "X");
                    updates.Add(b.QuoteIdentifier(key + "Y") + " = @v_" + key + "Y");
                }
                else if (t == typeof(Quaternion))
                {
                    updates.Add(b.QuoteIdentifier(key + "X") + " = @v_" + key + "X");
                    updates.Add(b.QuoteIdentifier(key + "Y") + " = @v_" + key + "Y");
                    updates.Add(b.QuoteIdentifier(key + "Z") + " = @v_" + key + "Z");
                    updates.Add(b.QuoteIdentifier(key + "W") + " = @v_" + key + "W");
                }
                else if (t == typeof(Color))
                {
                    updates.Add(b.QuoteIdentifier(key + "Red") + " = @v_" + key + "Red");
                    updates.Add(b.QuoteIdentifier(key + "Green") + " = @v_" + key + "Green");
                    updates.Add(b.QuoteIdentifier(key + "Blue") + " = @v_" + key + "Blue");
                }
                else if (t == typeof(EnvironmentController.WLVector4))
                {
                    updates.Add(b.QuoteIdentifier(key + "Red") + " = @v_" + key + "Red");
                    updates.Add(b.QuoteIdentifier(key + "Green") + " = @v_" + key + "Green");
                    updates.Add(b.QuoteIdentifier(key + "Blue") + " = @v_" + key + "Blue");
                    updates.Add(b.QuoteIdentifier(key + "Value") + " = @v_" + key + "Value");
                }
                else if (t == typeof(ColorAlpha))
                {
                    updates.Add(b.QuoteIdentifier(key + "Red") + " = @v_" + key + "Red");
                    updates.Add(b.QuoteIdentifier(key + "Green") + " = @v_" + key + "Green");
                    updates.Add(b.QuoteIdentifier(key + "Blue") + " = @v_" + key + "Blue");
                    updates.Add(b.QuoteIdentifier(key + "Alpha") + " = @v_" + key + "Alpha");
                }
                else if (value == null)
                {
                    /* skip */
                }
                else
                {
                    updates.Add(b.QuoteIdentifier(key) + " = @v_" + key);
                }
            }
            return updates;
        }

        public static void UpdateSet(this SqlConnection connection, string tablename, Dictionary<string, object> vals, string where, SqlTransaction transaction = null)
        {
            SqlCommandBuilder b = new SqlCommandBuilder();
            string q1 = "UPDATE " + tablename + " SET ";

            q1 += string.Join(",", UpdateSetFromVals(vals, b));

            using (var command = new SqlCommand(q1 + " WHERE " + where, connection))
            {
                command.Transaction = transaction;
                AddParameters(command.Parameters, vals);
                if (command.ExecuteNonQuery() < 1)
                {
                    throw new MsSqlInsertException();
                }
            }
        }

        public static void UpdateSet(this SqlConnection connection, string tablename, Dictionary<string, object> vals, Dictionary<string, object> where)
        {
            SqlCommandBuilder b = new SqlCommandBuilder();
            string q1 = "UPDATE " + tablename + " SET ";

            q1 += string.Join(",", UpdateSetFromVals(vals, b));

            var wherestr = new StringBuilder();
            foreach (KeyValuePair<string, object> w in where)
            {
                if (wherestr.Length != 0)
                {
                    wherestr.Append(" AND ");
                }
                wherestr.AppendFormat("{0} = @w_{1}", b.QuoteIdentifier(w.Key), w.Key);
            }

            using (var command = new SqlCommand(q1 + " WHERE " + wherestr, connection))
            {
                AddParameters(command.Parameters, vals);
                foreach (KeyValuePair<string, object> w in where)
                {
                    command.Parameters.AddParameter("@w_" + w.Key, w.Value);
                }
                if (command.ExecuteNonQuery() < 1)
                {
                    throw new MsSqlInsertException();
                }
            }
        }
        #endregion

        #region Data parsers
        public static EnvironmentController.WLVector4 GetWLVector4(this SqlDataReader dbReader, string prefix) =>
            new EnvironmentController.WLVector4(
                (double)dbReader[prefix + "Red"],
                (double)dbReader[prefix + "Green"],
                (double)dbReader[prefix + "Blue"],
                (double)dbReader[prefix + "Value"]);

        public static T GetEnum<T>(this SqlDataReader dbreader, string prefix)
        {
            var enumType = typeof(T).GetEnumUnderlyingType();
            object v = dbreader[prefix];
            Type dbType = v.GetType();
            if (enumType == typeof(ulong) || enumType == typeof(long))
            {
                dbType = typeof(long);
            }
            else if (enumType == typeof(ushort) || enumType == typeof(byte) || enumType == typeof(uint) ||
                enumType == typeof(short) || enumType == typeof(sbyte))
            {
                dbType = typeof(int);
            }
            return (T)Convert.ChangeType(Convert.ChangeType(v, dbType), enumType);
        }

        public static ParcelID GetParcelID(this SqlDataReader dbReader, string prefix)
        {
            UUID id = dbReader.GetUUID(prefix);
            return new ParcelID(id.GetBytes(), 0);
        }

        public static UUID GetUUID(this SqlDataReader dbReader, string prefix)
        {
            object v = dbReader[prefix];
            var t = v?.GetType();
            if (t == typeof(Guid))
            {
                return new UUID((Guid)v);
            }

            if (t == typeof(string))
            {
                return new UUID((string)v);
            }

            throw new InvalidCastException("GetUUID could not convert value for " + prefix);
        }

        public static UGUIWithName GetUGUIWithName(this SqlDataReader dbReader, string prefix)
        {
            object v = dbReader[prefix];
            var t = v?.GetType();
            if (t == typeof(Guid))
            {
                return new UGUIWithName((Guid)v);
            }

            if (t == typeof(string))
            {
                return new UGUIWithName((string)v);
            }

            throw new InvalidCastException("GetUUI could not convert value for " + prefix);
        }

        public static UGUI GetUGUI(this SqlDataReader dbReader, string prefix)
        {
            object v = dbReader[prefix];
            var t = v?.GetType();
            if (t == typeof(Guid))
            {
                return new UGUI((Guid)v);
            }

            if (t == typeof(string))
            {
                return new UGUI((string)v);
            }

            throw new InvalidCastException("GetUGUI could not convert value for " + prefix);
        }

        public static UGI GetUGI(this SqlDataReader dbReader, string prefix)
        {
            object v = dbReader[prefix];
            var t = v?.GetType();
            if (t == typeof(Guid))
            {
                return new UGI((Guid)v);
            }

            if (t == typeof(string))
            {
                return new UGI((string)v);
            }

            throw new InvalidCastException("GetUGI could not convert value for " + prefix);
        }

        public static UEI GetUEI(this SqlDataReader dbReader, string prefix)
        {
            object v = dbReader[prefix];
            var t = v?.GetType();
            if (t == typeof(Guid))
            {
                return new UEI((Guid)v);
            }

            if (t == typeof(string))
            {
                return new UEI((string)v);
            }

            throw new InvalidCastException("GetUEI could not convert value for " + prefix);
        }

        public static Date GetDate(this SqlDataReader dbReader, string prefix)
        {
            ulong v;
            if (!ulong.TryParse(dbReader[prefix].ToString(), out v))
            {
                throw new InvalidCastException("GetDate could not convert value for " + prefix);
            }
            return Date.UnixTimeToDateTime(v);
        }

        public static EnvironmentController.WLVector2 GetWLVector2(this SqlDataReader dbReader, string prefix) =>
            new EnvironmentController.WLVector2(
                (double)dbReader[prefix + "X"],
                (double)dbReader[prefix + "Y"]);

        public static Vector3 GetVector3(this SqlDataReader dbReader, string prefix) =>
            new Vector3(
                (double)dbReader[prefix + "X"],
                (double)dbReader[prefix + "Y"],
                (double)dbReader[prefix + "Z"]);

        public static Quaternion GetQuaternion(this SqlDataReader dbReader, string prefix) =>
            new Quaternion(
                (double)dbReader[prefix + "X"],
                (double)dbReader[prefix + "Y"],
                (double)dbReader[prefix + "Z"],
                (double)dbReader[prefix + "W"]);

        public static Color GetColor(this SqlDataReader dbReader, string prefix) =>
            new Color(
                (double)dbReader[prefix + "Red"],
                (double)dbReader[prefix + "Green"],
                (double)dbReader[prefix + "Blue"]);

        public static ColorAlpha GetColorAlpha(this SqlDataReader dbReader, string prefix) =>
            new ColorAlpha(
                (double)dbReader[prefix + "Red"],
                (double)dbReader[prefix + "Green"],
                (double)dbReader[prefix + "Blue"],
                (double)dbReader[prefix + "Alpha"]);

        public static byte[] GetBytes(this SqlDataReader dbReader, string prefix)
        {
            object o = dbReader[prefix];
            var t = o?.GetType();
            if (t == typeof(DBNull))
            {
                return new byte[0];
            }
            return (byte[])o;
        }

        public static byte[] GetBytesOrNull(this SqlDataReader dbReader, string prefix)
        {
            object o = dbReader[prefix];
            var t = o?.GetType();
            if (t == typeof(DBNull))
            {
                return null;
            }
            return (byte[])o;
        }

        public static Uri GetUri(this SqlDataReader dbReader, string prefix)
        {
            object o = dbReader[prefix];
            var t = o?.GetType();
            if (t == typeof(DBNull))
            {
                return null;
            }
            var s = (string)o;
            if (s.Length == 0)
            {
                return null;
            }
            return new Uri(s);
        }

        public static GridVector GetGridVector(this SqlDataReader dbReader, string prefix) =>
            new GridVector((uint)(int)dbReader[prefix + "X"], (uint)(int)dbReader[prefix + "Y"]);
        #endregion

        #region Migrations helper
        public static uint GetTableRevision(this SqlConnection connection, string name)
        {
            using (var cmd = new SqlCommand("SELECT cast(value as varchar(255)) AS value FROM sys.extended_properties where major_id = OBJECT_ID(@name) AND name = N'table_revision';", connection))
            {
                cmd.Parameters.AddWithValue("@name", "dbo." + name);
                using (SqlDataReader dbReader = cmd.ExecuteReader())
                {
                    if (dbReader.Read())
                    {
                        uint u;
                        if (!uint.TryParse((string)dbReader["value"], out u))
                        {
                            throw new InvalidDataException("description is not a parseable number");
                        }
                        return u;
                    }
                }
            }
            return 0;
        }
        #endregion

        public static string ToMsSqlQuoted(this string unquoted)
        {
            var sb = new StringBuilder();
            sb.Append("'");
            foreach(char c in unquoted)
            {
                if(c== '\'')
                {
                    sb.Append("''");
                }
                else
                {
                    sb.Append(c);
                }
            }
            sb.Append("'");
            return sb.ToString();
        }
    }
}
