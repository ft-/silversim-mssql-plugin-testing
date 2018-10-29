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

using SilverSim.Scene.Types.SceneEnvironment;
using SilverSim.Types;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Globalization;

namespace SilverSim.Database.MsSql._Migration
{
    public interface IMigrationElement
    {
        string Sql(string tableName);
    }

    public class SqlTable : IMigrationElement
    {
        public string Name { get; }

        public SqlTable(string name)
        {
            Name = name;
        }

        public string Sql(string tableName)
        {
            throw new NotSupportedException();
        }
    }

    public class PrimaryKeyInfo : IMigrationElement
    {
        public string[] FieldNames { get; }

        public PrimaryKeyInfo(params string[] fieldNames)
        {
            FieldNames = fieldNames;
        }

        public PrimaryKeyInfo(PrimaryKeyInfo src)
        {
            FieldNames = new string[src.FieldNames.Length];
            for (int i = 0; i < src.FieldNames.Length; ++i)
            {
                FieldNames[i] = src.FieldNames[i];
            }
        }

        public string FieldSql()
        {
            SqlCommandBuilder b = new SqlCommandBuilder();
            var fieldNames = new List<string>();
            foreach (string fName in FieldNames)
            {
                fieldNames.Add(b.QuoteIdentifier(fName));
            }
            return "PRIMARY KEY(" + string.Join(",", fieldNames) + ")";
        }

        public string Sql(string tableName) => "ALTER TABLE " + tableName + " ADD " + FieldSql() + ";";
    }

    public class DropPrimaryKeyinfo : IMigrationElement
    {
        public string Sql(string tableName)
        {
            SqlCommandBuilder b = new SqlCommandBuilder();

            return "DECLARE @SQL VARCHAR(4000);" +
            "SET @SQL = 'ALTER TABLE " + b.QuoteIdentifier(tableName) + " DROP CONSTRAINT |ConstraintName| ';" +
            "SET @SQL = REPLACE(@SQL, '|ConstraintName|', ( SELECT   name " +
                                               "FROM sysobjects " +
                                               "WHERE xtype = 'PK' " +
                                                      " AND parent_obj = OBJECT_ID(" + tableName.ToMsSqlQuoted() + ")));" +
            "EXEC(@SQL);";
        }
    }

    public class NamedKeyInfo : IMigrationElement
    {
        public bool IsUnique { get; set; }
        public string Name { get; }
        public string[] FieldNames { get; }

        public NamedKeyInfo(string name, params string[] fieldNames)
        {
            Name = name;
            FieldNames = fieldNames;
        }

        public NamedKeyInfo(NamedKeyInfo src)
        {
            IsUnique = src.IsUnique;
            Name = src.Name;
            FieldNames = new string[src.FieldNames.Length];
            for (int i = 0; i < src.FieldNames.Length; ++i)
            {
                FieldNames[i] = src.FieldNames[i];
            }
        }

        private string FieldSql()
        {
            var b = new SqlCommandBuilder();
            var fieldNames = new List<string>();
            foreach (string fName in FieldNames)
            {
                fieldNames.Add(b.QuoteIdentifier(fName));
            }
            return "(" + string.Join(",", fieldNames) + ")";
        }

        public string Sql(string tableName)
        {
            var b = new SqlCommandBuilder();
            return "CREATE " + (IsUnique ? "UNIQUE " : "") + "INDEX " + b.QuoteIdentifier(tableName + "_" + Name) + " ON " + b.QuoteIdentifier(tableName) + " " + FieldSql() + ";";
        }
    }

    public class DropNamedKeyInfo : IMigrationElement
    {
        public string Name { get; }

        public DropNamedKeyInfo(string name)
        {
            Name = name;
        }

        public string Sql(string tableName) => "DROP " + new SqlCommandBuilder().QuoteIdentifier(tableName + "_" + Name) + ";";
    }

    #region Table fields
    public interface IColumnInfo
    {
        string Name { get; }
        Type FieldType { get; }
        uint Cardinality { get; }
        bool IsNullAllowed { get; }
        bool IsLong { get; }
        bool IsFixed { get; }
        object Default { get; }
        string FieldSql(string tableName);
    }

    public interface IAddColumn : IColumnInfo
    {
        string Sql(string tableName);
    }

    internal static class ColumnGenerator
    {
        public static Dictionary<string, string> DropDefault(this IColumnInfo colInfo, string tableName)
        {
            string baseDefaultName = $"DF_{tableName}_{colInfo.Name}";
            var result = new Dictionary<string, string>();
            var t = new SqlCommandBuilder();
            Type f = colInfo.FieldType;
            string cmdgen = "ALTER TABLE {0} DROP CONSTRAINT {1};";

            if (f == typeof(Vector3))
            {
                result.Add(colInfo.Name + "X", string.Format(cmdgen, t.QuoteIdentifier(tableName), t.QuoteIdentifier(baseDefaultName + "X")));
                result.Add(colInfo.Name + "Y", string.Format(cmdgen, t.QuoteIdentifier(tableName), t.QuoteIdentifier(baseDefaultName + "Y")));
                result.Add(colInfo.Name + "Z", string.Format(cmdgen, t.QuoteIdentifier(tableName), t.QuoteIdentifier(baseDefaultName + "Z")));
                return result;
            }
            else if (f == typeof(GridVector))
            {
                result.Add(colInfo.Name + "X", string.Format(cmdgen, t.QuoteIdentifier(tableName), t.QuoteIdentifier(baseDefaultName + "X")));
                result.Add(colInfo.Name + "Y", string.Format(cmdgen, t.QuoteIdentifier(tableName), t.QuoteIdentifier(baseDefaultName + "Y")));
                return result;
            }
            else if (f == typeof(Vector4))
            {
                result.Add(colInfo.Name + "X", string.Format(cmdgen, t.QuoteIdentifier(tableName), t.QuoteIdentifier(baseDefaultName + "X")));
                result.Add(colInfo.Name + "Y", string.Format(cmdgen, t.QuoteIdentifier(tableName), t.QuoteIdentifier(baseDefaultName + "Y")));
                result.Add(colInfo.Name + "Z", string.Format(cmdgen, t.QuoteIdentifier(tableName), t.QuoteIdentifier(baseDefaultName + "Z")));
                result.Add(colInfo.Name + "W", string.Format(cmdgen, t.QuoteIdentifier(tableName), t.QuoteIdentifier(baseDefaultName + "W")));
                return result;
            }
            else if (f == typeof(Quaternion))
            {
                result.Add(colInfo.Name + "X", string.Format(cmdgen, t.QuoteIdentifier(tableName), t.QuoteIdentifier(baseDefaultName + "X")));
                result.Add(colInfo.Name + "Y", string.Format(cmdgen, t.QuoteIdentifier(tableName), t.QuoteIdentifier(baseDefaultName + "Y")));
                result.Add(colInfo.Name + "Z", string.Format(cmdgen, t.QuoteIdentifier(tableName), t.QuoteIdentifier(baseDefaultName + "Z")));
                result.Add(colInfo.Name + "W", string.Format(cmdgen, t.QuoteIdentifier(tableName), t.QuoteIdentifier(baseDefaultName + "W")));
                return result;
            }
            else if (f == typeof(EnvironmentController.WLVector2))
            {
                result.Add(colInfo.Name + "X", string.Format(cmdgen, t.QuoteIdentifier(tableName), t.QuoteIdentifier(baseDefaultName + "X")));
                result.Add(colInfo.Name + "Y", string.Format(cmdgen, t.QuoteIdentifier(tableName), t.QuoteIdentifier(baseDefaultName + "Y")));
                return result;
            }
            else if (f == typeof(EnvironmentController.WLVector4))
            {
                result.Add(colInfo.Name + "Red", string.Format(cmdgen, t.QuoteIdentifier(tableName), t.QuoteIdentifier(baseDefaultName + "Red")));
                result.Add(colInfo.Name + "Green", string.Format(cmdgen, t.QuoteIdentifier(tableName), t.QuoteIdentifier(baseDefaultName + "Green")));
                result.Add(colInfo.Name + "Blue", string.Format(cmdgen, t.QuoteIdentifier(tableName), t.QuoteIdentifier(baseDefaultName + "Blue")));
                result.Add(colInfo.Name + "Value", string.Format(cmdgen, t.QuoteIdentifier(tableName), t.QuoteIdentifier(baseDefaultName + "Value")));
                return result;
            }
            else if (f == typeof(Color))
            {
                result.Add(colInfo.Name + "Red", string.Format(cmdgen, t.QuoteIdentifier(tableName), t.QuoteIdentifier(baseDefaultName + "Red")));
                result.Add(colInfo.Name + "Green", string.Format(cmdgen, t.QuoteIdentifier(tableName), t.QuoteIdentifier(baseDefaultName + "Green")));
                result.Add(colInfo.Name + "Blue", string.Format(cmdgen, t.QuoteIdentifier(tableName), t.QuoteIdentifier(baseDefaultName + "Blue")));
                return result;
            }
            else if (f == typeof(ColorAlpha))
            {
                result.Add(colInfo.Name + "Red", string.Format(cmdgen, t.QuoteIdentifier(tableName), t.QuoteIdentifier(baseDefaultName + "Red")));
                result.Add(colInfo.Name + "Green", string.Format(cmdgen, t.QuoteIdentifier(tableName), t.QuoteIdentifier(baseDefaultName + "Green")));
                result.Add(colInfo.Name + "Blue", string.Format(cmdgen, t.QuoteIdentifier(tableName), t.QuoteIdentifier(baseDefaultName + "Blue")));
                result.Add(colInfo.Name + "Alpha", string.Format(cmdgen, t.QuoteIdentifier(tableName), t.QuoteIdentifier(baseDefaultName + "Alpha")));
                return result;
            }
            else
            {
                result.Add(colInfo.Name, string.Format(cmdgen, t.QuoteIdentifier(tableName), t.QuoteIdentifier(baseDefaultName)));
                return result;
            }
        }

        public static Dictionary<string, string> AddDefault(this IColumnInfo colInfo, string tableName)
        {
            string baseDefaultName = $"DF_{tableName}_{colInfo.Name}";
            var result = new Dictionary<string, string>();
            var t = new SqlCommandBuilder();
            Type f = colInfo.FieldType;
            if (f == typeof(Vector3))
            {
                if (colInfo.Default != null && !colInfo.IsNullAllowed)
                {
                    if (colInfo.Default.GetType() != f)
                    {
                        throw new ArgumentException("Default is not a Vector3 for field " + colInfo.Name);
                    }

                    var v = (Vector3)colInfo.Default;
                    result.Add(colInfo.Name + "X", string.Format(CultureInfo.InvariantCulture, "CONSTRAINT {2} DEFAULT '{0}' FOR {1}", v.X, t.QuoteIdentifier(colInfo.Name + "X"), t.QuoteIdentifier(baseDefaultName + "X")));
                    result.Add(colInfo.Name + "Y", string.Format(CultureInfo.InvariantCulture, "CONSTRAINT {2} DEFAULT '{0}' FOR {1}", v.Y, t.QuoteIdentifier(colInfo.Name + "Y"), t.QuoteIdentifier(baseDefaultName + "Y")));
                    result.Add(colInfo.Name + "Z", string.Format(CultureInfo.InvariantCulture, "CONSTRAINT {2} DEFAULT '{0}' FOR {1}", v.Z, t.QuoteIdentifier(colInfo.Name + "Z"), t.QuoteIdentifier(baseDefaultName + "Z")));
                }
                return result;
            }
            else if (f == typeof(GridVector))
            {
                if (colInfo.Default != null && !colInfo.IsNullAllowed)
                {
                    if (colInfo.Default.GetType() != f)
                    {
                        throw new ArgumentException("Default is not a GridVector for field " + colInfo.Name);
                    }

                    var v = (GridVector)colInfo.Default;
                    result.Add(colInfo.Name + "X", string.Format(CultureInfo.InvariantCulture, "CONSTRAINT {2} DEFAULT '{0}' FOR {1}", v.X, t.QuoteIdentifier(colInfo.Name + "X"), t.QuoteIdentifier(baseDefaultName + "X")));
                    result.Add(colInfo.Name + "Y", string.Format(CultureInfo.InvariantCulture, "CONSTRAINT {2} DEFAULT '{0}' FOR {1}", v.Y, t.QuoteIdentifier(colInfo.Name + "Y"), t.QuoteIdentifier(baseDefaultName + "Y")));
                }
                return result;
            }
            else if (f == typeof(Vector4))
            {
                if (colInfo.Default != null && !colInfo.IsNullAllowed)
                {
                    if (colInfo.Default.GetType() != f)
                    {
                        throw new ArgumentException("Default is not a Vector4 for field " + colInfo.Name);
                    }

                    var v = (Vector4)colInfo.Default;
                    result.Add(colInfo.Name + "X", string.Format(CultureInfo.InvariantCulture, "CONSTRAINT {2} DEFAULT '{0}' FOR {1}", v.X, t.QuoteIdentifier(colInfo.Name + "X"), t.QuoteIdentifier(baseDefaultName + "X")));
                    result.Add(colInfo.Name + "Y", string.Format(CultureInfo.InvariantCulture, "CONSTRAINT {2} DEFAULT '{0}' FOR {1}", v.Y, t.QuoteIdentifier(colInfo.Name + "Y"), t.QuoteIdentifier(baseDefaultName + "Y")));
                    result.Add(colInfo.Name + "Z", string.Format(CultureInfo.InvariantCulture, "CONSTRAINT {2} DEFAULT '{0}' FOR {1}", v.Z, t.QuoteIdentifier(colInfo.Name + "Z"), t.QuoteIdentifier(baseDefaultName + "Z")));
                    result.Add(colInfo.Name + "W", string.Format(CultureInfo.InvariantCulture, "CONSTRAINT {2} DEFAULT '{0}' FOR {1}", v.W, t.QuoteIdentifier(colInfo.Name + "W"), t.QuoteIdentifier(baseDefaultName + "W")));
                }
                return result;
            }
            else if (f == typeof(Quaternion))
            {
                if (colInfo.Default != null && !colInfo.IsNullAllowed)
                {
                    if (colInfo.Default.GetType() != f)
                    {
                        throw new ArgumentException("Default is not a Quaternion for " + colInfo.Name);
                    }

                    var v = (Quaternion)colInfo.Default;
                    result.Add(colInfo.Name + "X", string.Format(CultureInfo.InvariantCulture, "CONSTRAINT {2} DEFAULT '{0}' FOR {1}", v.X, t.QuoteIdentifier(colInfo.Name + "X"), t.QuoteIdentifier(baseDefaultName + "X")));
                    result.Add(colInfo.Name + "Y", string.Format(CultureInfo.InvariantCulture, "CONSTRAINT {2} DEFAULT '{0}' FOR {1}", v.Y, t.QuoteIdentifier(colInfo.Name + "Y"), t.QuoteIdentifier(baseDefaultName + "Y")));
                    result.Add(colInfo.Name + "Z", string.Format(CultureInfo.InvariantCulture, "CONSTRAINT {2} DEFAULT '{0}' FOR {1}", v.Z, t.QuoteIdentifier(colInfo.Name + "Z"), t.QuoteIdentifier(baseDefaultName + "Z")));
                    result.Add(colInfo.Name + "W", string.Format(CultureInfo.InvariantCulture, "CONSTRAINT {2} DEFAULT '{0}' FOR {1}", v.W, t.QuoteIdentifier(colInfo.Name + "W"), t.QuoteIdentifier(baseDefaultName + "W")));
                }
                return result;
            }
            else if (f == typeof(EnvironmentController.WLVector2))
            {
                if (colInfo.Default != null && !colInfo.IsNullAllowed)
                {
                    if (colInfo.Default.GetType() != f)
                    {
                        throw new ArgumentException("Default is not a EnvironmentController.WLVector2 for field " + colInfo.Name);
                    }

                    var v = (EnvironmentController.WLVector2)colInfo.Default;
                    result.Add(colInfo.Name + "X", string.Format("CONSTRAINT {2} DEFAULT '{0}' FOR {1}", v.X, t.QuoteIdentifier(colInfo.Name + "X"), t.QuoteIdentifier(baseDefaultName + "X")));
                    result.Add(colInfo.Name + "Y", string.Format("CONSTRAINT {2} DEFAULT '{0}' FOR {1}", v.Y, t.QuoteIdentifier(colInfo.Name + "Y"), t.QuoteIdentifier(baseDefaultName + "Y")));
                }
                return result;
            }
            else if (f == typeof(EnvironmentController.WLVector4))
            {
                if (colInfo.Default != null && !colInfo.IsNullAllowed)
                {
                    if (colInfo.Default.GetType() != f)
                    {
                        throw new ArgumentException("Default is not a EnvironmentController.WLVector4 for field " + colInfo.Name);
                    }

                    var v = (EnvironmentController.WLVector4)colInfo.Default;
                    result.Add(colInfo.Name + "Red", string.Format("CONSTRAINT {2} DEFAULT '{0}' FOR {1}", v.X, t.QuoteIdentifier(colInfo.Name + "Red"), t.QuoteIdentifier(baseDefaultName + "Red")));
                    result.Add(colInfo.Name + "Green", string.Format("CONSTRAINT {2} DEFAULT '{0}' FOR {1}", v.Y, t.QuoteIdentifier(colInfo.Name + "Green"), t.QuoteIdentifier(baseDefaultName + "Green")));
                    result.Add(colInfo.Name + "Blue", string.Format("CONSTRAINT {2} DEFAULT '{0}' FOR {1}", v.Z, t.QuoteIdentifier(colInfo.Name + "Blue"), t.QuoteIdentifier(baseDefaultName + "Blue")));
                    result.Add(colInfo.Name + "Value", string.Format("CONSTRAINT {2} DEFAULT '{0}' FOR {1}", v.W, t.QuoteIdentifier(colInfo.Name + "Value"), t.QuoteIdentifier(baseDefaultName + "Value")));
                }
                return result;
            }
            else if (f == typeof(Color))
            {
                if (colInfo.Default != null && !colInfo.IsNullAllowed)
                {
                    if (colInfo.Default.GetType() != f)
                    {
                        throw new ArgumentException("Default is not a Color for field " + colInfo.Name);
                    }

                    var v = (Color)colInfo.Default;
                    result.Add(colInfo.Name + "Red", string.Format("CONSTRAINT {2} DEFAULT '{0}' FOR {1}", v.R, t.QuoteIdentifier(colInfo.Name + "Red"), t.QuoteIdentifier(baseDefaultName + "Red")));
                    result.Add(colInfo.Name + "Green", string.Format("CONSTRAINT {2} DEFAULT '{0}' FOR {1}", v.G, t.QuoteIdentifier(colInfo.Name + "Green"), t.QuoteIdentifier(baseDefaultName + "Green")));
                    result.Add(colInfo.Name + "Blue", string.Format("CONSTRAINT {2} DEFAULT '{0}' FOR {1}", v.B, t.QuoteIdentifier(colInfo.Name + "Blue"), t.QuoteIdentifier(baseDefaultName + "Blue")));
                }
                return result;
            }
            else if (f == typeof(ColorAlpha))
            {
                if (colInfo.Default != null && !colInfo.IsNullAllowed)
                {
                    if (colInfo.Default.GetType() != f)
                    {
                        throw new ArgumentException("Default is not a ColorAlpha for field " + colInfo.Name);
                    }

                    var v = (ColorAlpha)colInfo.Default;
                    result.Add(colInfo.Name + "Red", string.Format("CONSTRAINT {2} DEFAULT '{0}' FOR {1}", v.R, t.QuoteIdentifier(colInfo.Name + "Red"), t.QuoteIdentifier(baseDefaultName + "Red")));
                    result.Add(colInfo.Name + "Green", string.Format("CONSTRAINT {2} DEFAULT '{0}' FOR {1}", v.G, t.QuoteIdentifier(colInfo.Name + "Green"), t.QuoteIdentifier(baseDefaultName + "Green")));
                    result.Add(colInfo.Name + "Blue", string.Format("CONSTRAINT {2} DEFAULT '{0}' FOR {1}", v.B, t.QuoteIdentifier(colInfo.Name + "Blue"), t.QuoteIdentifier(baseDefaultName + "Blue")));
                    result.Add(colInfo.Name + "Alpha", string.Format("CONSTRAINT {2} DEFAULT '{0}' FOR {1}", v.A, t.QuoteIdentifier(colInfo.Name + "Alpha"), t.QuoteIdentifier(baseDefaultName + "Alpha")));
                }
                return result;
            }

            if (colInfo.Default != null && !colInfo.IsNullAllowed)
            {
                if (colInfo.Default.GetType() != colInfo.FieldType &&
                    !(colInfo.Default.GetType() == typeof(UUID) &&
                    colInfo.FieldType == typeof(UGUI)) &&
                    !(colInfo.Default.GetType() == typeof(UUID) &&
                    colInfo.FieldType == typeof(UGUIWithName)) &&
                    !(colInfo.Default.GetType() == typeof(UUID) &&
                    colInfo.FieldType == typeof(UGI)))
                {
                    throw new ArgumentOutOfRangeException("Default does not match expected type in field " + colInfo.Name + " target type=" + colInfo.FieldType.FullName + " defaultType=" + colInfo.Default.GetType().FullName);
                }

                object def = colInfo.Default;
                if (typeof(bool) == f)
                {
                    def = ((bool)def) ? 1 : 0;
                }
                else if (typeof(Date) == f)
                {
                    def = ((Date)def).AsULong;
                }
                else if (typeof(ParcelID) == f)
                {
                    def = (Guid)new UUID(((ParcelID)def).GetBytes(), 0);
                }
                else if (f.IsEnum)
                {
                    def = Convert.ChangeType(def, f.GetEnumUnderlyingType());
                }
                result.Add(colInfo.Name, string.Format("CONSTRAINT {2} DEFAULT {0} FOR {1}", def.ToString().ToMsSqlQuoted(), t.QuoteIdentifier(colInfo.Name), t.QuoteIdentifier(baseDefaultName)));
            }

            return result;
        }

        public static Dictionary<string, string> ColumnSql(this IColumnInfo colInfo, string tableName, bool useDefault = true)
        {
            var t = new SqlCommandBuilder();
            var result = new Dictionary<string, string>();
            string notNull = colInfo.IsNullAllowed ? string.Empty : "NOT NULL ";
            string typeSql;
            Type f = colInfo.FieldType;
            string baseDefaultName = $"DF_{tableName}_{colInfo.Name}";
            if (f == typeof(string))
            {
                typeSql = (colInfo.Cardinality == 0) ?
                    (colInfo.IsLong ? "nvarchar(max)" : "nvarchar(4000)") :
                    (colInfo.IsFixed ? "NCHAR" : "NVARCHAR") + "(" + colInfo.Cardinality.ToString() + ")";
            }
            else if (f == typeof(UGUI) || f == typeof(UGUIWithName) || f == typeof(UGI))
            {
                typeSql = "NVARCHAR(255)";
            }
            else if (f == typeof(UUID) || f == typeof(ParcelID))
            {
                typeSql = "uniqueidentifier";
            }
            else if (f == typeof(double))
            {
                typeSql = "float(53)";
            }
            else if (f.IsEnum)
            {
                Type enumType = f.GetEnumUnderlyingType();
                if (enumType == typeof(ulong) || enumType == typeof(long))
                {
                    typeSql = "bigint";
                }
                else if (enumType == typeof(byte) || enumType == typeof(ushort) || enumType == typeof(sbyte) || enumType == typeof(short))
                {
                    typeSql = "smallint";
                }
                else if (enumType == typeof(uint))
                {
                    typeSql = "integer";
                }
                else
                {
                    typeSql = "integer";
                }
            }
            else if (f == typeof(int) || f == typeof(uint))
            {
                typeSql = "integer";
            }
            else if (f == typeof(short) || f == typeof(ushort) || f == typeof(byte) || f == typeof(sbyte))
            {
                typeSql = "smallint";
            }
            else if (f == typeof(bool))
            {
                typeSql = "bit";
            }
            else if (f == typeof(long) || f == typeof(ulong) || f == typeof(Date))
            {
                typeSql = "bigint";
            }
            else if (f == typeof(Vector3))
            {
                if (colInfo.Default != null && !colInfo.IsNullAllowed && useDefault)
                {
                    if (colInfo.Default.GetType() != f)
                    {
                        throw new ArgumentException("Default is not a Vector3 for field " + colInfo.Name);
                    }

                    var v = (Vector3)colInfo.Default;
                    result.Add(colInfo.Name + "X", string.Format(CultureInfo.InvariantCulture, "float(53) {0} CONSTRAINT {2} DEFAULT '{1}'", notNull, v.X, t.QuoteIdentifier(baseDefaultName + "X")));
                    result.Add(colInfo.Name + "Y", string.Format(CultureInfo.InvariantCulture, "float(53) {0} CONSTRAINT {2} DEFAULT '{1}'", notNull, v.Y, t.QuoteIdentifier(baseDefaultName + "Y")));
                    result.Add(colInfo.Name + "Z", string.Format(CultureInfo.InvariantCulture, "float(53) {0} CONSTRAINT {2} DEFAULT '{1}'", notNull, v.Z, t.QuoteIdentifier(baseDefaultName + "Z")));
                }
                else
                {
                    result.Add(colInfo.Name + "X", "float(53) " + notNull);
                    result.Add(colInfo.Name + "Y", "float(53) " + notNull);
                    result.Add(colInfo.Name + "Z", "float(53) " + notNull);
                }
                return result;
            }
            else if (f == typeof(GridVector))
            {
                if (colInfo.Default != null && !colInfo.IsNullAllowed && useDefault)
                {
                    if (colInfo.Default.GetType() != f)
                    {
                        throw new ArgumentException("Default is not a GridVector for field " + colInfo.Name);
                    }

                    var v = (GridVector)colInfo.Default;
                    result.Add(colInfo.Name + "X", string.Format(CultureInfo.InvariantCulture, "integer {0} CONSTRAINT {2} DEFAULT '{1}'", notNull, v.X, t.QuoteIdentifier(baseDefaultName + "X")));
                    result.Add(colInfo.Name + "Y", string.Format(CultureInfo.InvariantCulture, "integer {0} CONSTRAINT {2} DEFAULT '{1}'", notNull, v.Y, t.QuoteIdentifier(baseDefaultName + "Y")));
                }
                else
                {
                    result.Add(colInfo.Name + "X", "integer " + notNull);
                    result.Add(colInfo.Name + "Y", "integer " + notNull);
                }
                return result;
            }
            else if (f == typeof(Vector4))
            {
                if (colInfo.Default != null && !colInfo.IsNullAllowed && useDefault)
                {
                    if (colInfo.Default.GetType() != f)
                    {
                        throw new ArgumentException("Default is not a Vector4 for field " + colInfo.Name);
                    }

                    var v = (Vector4)colInfo.Default;
                    result.Add(colInfo.Name + "X", string.Format(CultureInfo.InvariantCulture, "float(53) {0} CONSTRAINT {2} DEFAULT '{1}'", notNull, v.X, t.QuoteIdentifier(baseDefaultName + "X")));
                    result.Add(colInfo.Name + "Y", string.Format(CultureInfo.InvariantCulture, "float(53) {0} CONSTRAINT {2} DEFAULT '{1}'", notNull, v.Y, t.QuoteIdentifier(baseDefaultName + "Y")));
                    result.Add(colInfo.Name + "Z", string.Format(CultureInfo.InvariantCulture, "float(53) {0} CONSTRAINT {2} DEFAULT '{1}'", notNull, v.Z, t.QuoteIdentifier(baseDefaultName + "Z")));
                    result.Add(colInfo.Name + "W", string.Format(CultureInfo.InvariantCulture, "float(53) {0} CONSTRAINT {2} DEFAULT '{1}'", notNull, v.W, t.QuoteIdentifier(baseDefaultName + "W")));
                }
                else
                {
                    result.Add(colInfo.Name + "X", "float(53) " + notNull);
                    result.Add(colInfo.Name + "Y", "float(53) " + notNull);
                    result.Add(colInfo.Name + "Z", "float(53) " + notNull);
                    result.Add(colInfo.Name + "W", "float(53) " + notNull);
                }
                return result;
            }
            else if (f == typeof(Quaternion))
            {
                if (colInfo.Default != null && !colInfo.IsNullAllowed && useDefault)
                {
                    if (colInfo.Default.GetType() != f)
                    {
                        throw new ArgumentException("Default is not a Quaternion for " + colInfo.Name);
                    }

                    var v = (Quaternion)colInfo.Default;
                    result.Add(colInfo.Name + "X", string.Format(CultureInfo.InvariantCulture, "float(53) {0} CONSTRAINT {2} DEFAULT '{1}'", notNull, v.X, t.QuoteIdentifier(baseDefaultName + "X")));
                    result.Add(colInfo.Name + "Y", string.Format(CultureInfo.InvariantCulture, "float(53) {0} CONSTRAINT {2} DEFAULT '{1}'", notNull, v.Y, t.QuoteIdentifier(baseDefaultName + "Y")));
                    result.Add(colInfo.Name + "Z", string.Format(CultureInfo.InvariantCulture, "float(53) {0} CONSTRAINT {2} DEFAULT '{1}'", notNull, v.Z, t.QuoteIdentifier(baseDefaultName + "Z")));
                    result.Add(colInfo.Name + "W", string.Format(CultureInfo.InvariantCulture, "float(53) {0} CONSTRAINT {2} DEFAULT '{1}'", notNull, v.W, t.QuoteIdentifier(baseDefaultName + "W")));
                }
                else
                {
                    result.Add(colInfo.Name + "X", "float(53) " + notNull);
                    result.Add(colInfo.Name + "Y", "float(53) " + notNull);
                    result.Add(colInfo.Name + "Z", "float(53) " + notNull);
                    result.Add(colInfo.Name + "W", "float(53) " + notNull);
                }
                return result;
            }
            else if (f == typeof(EnvironmentController.WLVector2))
            {
                if (colInfo.Default != null && !colInfo.IsNullAllowed && useDefault)
                {
                    if (colInfo.Default.GetType() != f)
                    {
                        throw new ArgumentException("Default is not a EnvironmentController.WLVector2 for field " + colInfo.Name);
                    }

                    var v = (EnvironmentController.WLVector2)colInfo.Default;
                    result.Add(colInfo.Name + "X", string.Format(CultureInfo.InvariantCulture, "float(53) {0} CONSTRAINT {2} DEFAULT '{1}'", notNull, v.X, t.QuoteIdentifier(baseDefaultName + "X")));
                    result.Add(colInfo.Name + "Y", string.Format(CultureInfo.InvariantCulture, "float(53) {0} CONSTRAINT {2} DEFAULT '{1}'", notNull, v.Y, t.QuoteIdentifier(baseDefaultName + "Y")));
                }
                else
                {
                    result.Add(colInfo.Name + "X", "float(53) " + notNull);
                    result.Add(colInfo.Name + "Y", "float(53) " + notNull);
                }
                return result;
            }
            else if (f == typeof(EnvironmentController.WLVector4))
            {
                if (colInfo.Default != null && !colInfo.IsNullAllowed && useDefault)
                {
                    if (colInfo.Default.GetType() != f)
                    {
                        throw new ArgumentException("Default is not a EnvironmentController.WLVector4 for field " + colInfo.Name);
                    }

                    var v = (EnvironmentController.WLVector4)colInfo.Default;
                    result.Add(colInfo.Name + "Red", string.Format(CultureInfo.InvariantCulture, "float(53) {0} CONSTRAINT {2} DEFAULT '{1}'", notNull, v.X, t.QuoteIdentifier(baseDefaultName + "Red")));
                    result.Add(colInfo.Name + "Green", string.Format(CultureInfo.InvariantCulture, "float(53) {0} CONSTRAINT {2} DEFAULT '{1}'", notNull, v.Y, t.QuoteIdentifier(baseDefaultName + "Green")));
                    result.Add(colInfo.Name + "Blue", string.Format(CultureInfo.InvariantCulture, "float(53) {0} CONSTRAINT {2} DEFAULT '{1}'", notNull, v.Z, t.QuoteIdentifier(baseDefaultName + "Blue")));
                    result.Add(colInfo.Name + "Value", string.Format(CultureInfo.InvariantCulture, "float(53) {0} CONSTRAINT {2} DEFAULT '{1}'", notNull, v.W, t.QuoteIdentifier(baseDefaultName + "Value")));
                }
                else
                {
                    result.Add(colInfo.Name + "Red", "float(53) " + notNull);
                    result.Add(colInfo.Name + "Green", "float(53) " + notNull);
                    result.Add(colInfo.Name + "Blue", "float(53) " + notNull);
                    result.Add(colInfo.Name + "Value", "float(53) " + notNull);
                }
                return result;
            }
            else if (f == typeof(Color))
            {
                if (colInfo.Default != null && !colInfo.IsNullAllowed && useDefault)
                {
                    if (colInfo.Default.GetType() != f)
                    {
                        throw new ArgumentException("Default is not a Color for field " + colInfo.Name);
                    }

                    var v = (Color)colInfo.Default;
                    result.Add(colInfo.Name + "Red", string.Format(CultureInfo.InvariantCulture, "float(53) {0} CONSTRAINT {2} DEFAULT '{1}'", notNull, v.R, t.QuoteIdentifier(baseDefaultName + "Red")));
                    result.Add(colInfo.Name + "Green", string.Format(CultureInfo.InvariantCulture, "float(53) {0} CONSTRAINT {2} DEFAULT '{1}'", notNull, v.G, t.QuoteIdentifier(baseDefaultName + "Green")));
                    result.Add(colInfo.Name + "Blue", string.Format(CultureInfo.InvariantCulture, "float(53) {0} CONSTRAINT {2} DEFAULT '{1}'", notNull, v.B, t.QuoteIdentifier(baseDefaultName + "Blue")));
                }
                else
                {
                    result.Add(colInfo.Name + "Red", "float(53) " + notNull);
                    result.Add(colInfo.Name + "Green", "float(53) " + notNull);
                    result.Add(colInfo.Name + "Blue", "float(53) " + notNull);
                }
                return result;
            }
            else if (f == typeof(ColorAlpha))
            {
                if (colInfo.Default != null && !colInfo.IsNullAllowed && useDefault)
                {
                    if (colInfo.Default.GetType() != f)
                    {
                        throw new ArgumentException("Default is not a ColorAlpha for field " + colInfo.Name);
                    }

                    var v = (ColorAlpha)colInfo.Default;
                    result.Add(colInfo.Name + "Red", string.Format(CultureInfo.InvariantCulture, "float(53) {0} CONSTRAINT {2} DEFAULT '{1}'", notNull, v.R, t.QuoteIdentifier(baseDefaultName + "Red")));
                    result.Add(colInfo.Name + "Green", string.Format(CultureInfo.InvariantCulture, "float(53) {0} CONSTRAINT {2} DEFAULT '{1}'", notNull, v.G, t.QuoteIdentifier(baseDefaultName + "Green")));
                    result.Add(colInfo.Name + "Blue", string.Format(CultureInfo.InvariantCulture, "float(53) {0} CONSTRAINT {2} DEFAULT '{1}'", notNull, v.B, t.QuoteIdentifier(baseDefaultName + "Blue")));
                    result.Add(colInfo.Name + "Alpha", string.Format(CultureInfo.InvariantCulture, "float(53) {0} CONSTRAINT {2} DEFAULT '{1}'", notNull, v.A, t.QuoteIdentifier(baseDefaultName + "Alpha")));
                }
                else
                {
                    result.Add(colInfo.Name + "Red", "float(53) " + notNull);
                    result.Add(colInfo.Name + "Green", "float(53) " + notNull);
                    result.Add(colInfo.Name + "Blue", "float(53) " + notNull);
                    result.Add(colInfo.Name + "Alpha", "float(53) " + notNull);
                }
                return result;
            }
            else if (f == typeof(byte[]))
            {
                if (colInfo.IsLong)
                {
                    typeSql = "varbinary(max)";
                }
                else if(colInfo.Cardinality == 0)
                {
                    typeSql = "varbinary(8000)";
                }
                else
                {
                    typeSql = string.Format("{0}({1})", colInfo.IsFixed ? "binary" : "varbinary", colInfo.Cardinality);
                }
            }
            else
            {
                throw new ArgumentOutOfRangeException("FieldType " + f.FullName + " is not supported in field " + colInfo.Name);
            }

            if (colInfo.Default != null && !colInfo.IsNullAllowed && useDefault)
            {
                if (colInfo.Default.GetType() != colInfo.FieldType &&
                    !(colInfo.Default.GetType() == typeof(UUID) &&
                    colInfo.FieldType == typeof(UGUI)) &&
                    !(colInfo.Default.GetType() == typeof(UUID) &&
                    colInfo.FieldType == typeof(UGUIWithName)) &&
                    !(colInfo.Default.GetType() == typeof(UUID) &&
                    colInfo.FieldType == typeof(UGI)))
                {
                    throw new ArgumentOutOfRangeException("Default does not match expected type in field " + colInfo.Name + " target type=" + colInfo.FieldType.FullName + " defaultType=" + colInfo.Default.GetType().FullName);
                }

                object def = colInfo.Default;
                if (typeof(bool) == f)
                {
                    def = ((bool)def) ? 1 : 0;
                }
                else if (typeof(Date) == f)
                {
                    def = ((Date)def).AsULong;
                }
                else if (typeof(ParcelID) == f)
                {
                    def = (Guid)new UUID(((ParcelID)def).GetBytes(), 0);
                }
                else if (f.IsEnum)
                {
                    def = Convert.ChangeType(def, f.GetEnumUnderlyingType());
                }
                result.Add(colInfo.Name, string.Format(CultureInfo.InvariantCulture, "{0} {1} CONSTRAINT {3} DEFAULT {2}",
                    typeSql,
                    notNull,
                    def.ToString().ToMsSqlQuoted(),
                    t.QuoteIdentifier(baseDefaultName)));
            }
            else
            {
                result.Add(colInfo.Name, typeSql + " " + notNull);
            }
            return result;
        }
    }

    public class AddColumn<T> : IMigrationElement, IAddColumn
    {
        public string Name { get; }

        public Type FieldType => typeof(T);

        public uint Cardinality { get; set; }

        public bool IsNullAllowed { get; set; }
        public bool IsLong { get; set; }
        public bool IsFixed { get; set; }

        public object Default { get; set; }

        public AddColumn(string name)
        {
            Name = name;
            IsLong = false;
            IsNullAllowed = true;
            Default = default(T);
        }

        public string FieldSql(string tableName)
        {
            var parts = new List<string>();
            SqlCommandBuilder b = new SqlCommandBuilder();
            foreach (KeyValuePair<string, string> kvp in this.ColumnSql(tableName))
            {
                parts.Add(b.QuoteIdentifier(kvp.Key) + " " + kvp.Value);
            }
            return string.Join(",", parts);
        }

        private string AlterFieldSql(string tableName)
        {
            var parts = new List<string>();
            SqlCommandBuilder b = new SqlCommandBuilder();
            foreach (KeyValuePair<string, string> kvp in this.ColumnSql(tableName, false))
            {
                parts.Add(b.QuoteIdentifier(kvp.Key) + " " + kvp.Value);
            }
            return string.Join(", ", parts);
        }

        public string Sql(string tableName)
        {
            SqlCommandBuilder t = new SqlCommandBuilder();
            Dictionary<string, string> addDefaults = this.AddDefault(tableName);
            List<string> addDefault = new List<string>();
            foreach(string val in addDefaults.Values)
            {
                addDefault.Add($"ALTER TABLE {t.QuoteIdentifier(tableName)} ADD {val};");
            }
            return string.Format("ALTER TABLE {0} ADD {1};", t.QuoteIdentifier(tableName), AlterFieldSql(tableName)) + string.Join("", addDefault);
        }
    }

    public class DropColumn : IMigrationElement
    {
        public string Name { get; private set; }
        public DropColumn(string name)
        {
            Name = name;
        }

        public string Sql(string tableName)
        {
            throw new NotSupportedException();
        }

        public string Sql(string tableName, Type formerType)
        {
            var fieldNames = new string[] { Name };

            if (formerType == typeof(Vector3))
            {
                fieldNames = new string[]
                {
                    Name + "X", Name + "Y", Name + "Z"
                };
            }
            else if (formerType == typeof(GridVector))
            {
                fieldNames = new string[]
                {
                    Name + "X", Name + "Y"
                };
            }
            else if (formerType == typeof(Vector4) || formerType == typeof(Quaternion))
            {
                fieldNames = new string[]
                {
                    Name + "X", Name + "Y", Name + "Z", Name + "W"
                };
            }
            else if (formerType == typeof(EnvironmentController.WLVector4))
            {
                fieldNames = new string[]
                {
                    Name + "Red", Name + "Green", Name + "Blue", Name + "Value"
                };
            }
            else if (formerType == typeof(Color))
            {
                fieldNames = new string[]
                {
                    Name + "Red", Name + "Green", Name + "Blue"
                };
            }
            else if (formerType == typeof(ColorAlpha))
            {
                fieldNames = new string[]
                {
                    Name + "Red", Name + "Green", Name + "Blue", Name + "Alpha"
                };
            }

            SqlCommandBuilder b = new SqlCommandBuilder();
            List<string> dropDefaults = new List<string>();
            for (int i = 0; i < fieldNames.Length; ++i)
            {
                dropDefaults.Add($"ALTER TABLE {tableName} DROP {b.QuoteIdentifier("DF_" + tableName + "_" + fieldNames[i])};");
                fieldNames[i] = b.QuoteIdentifier(fieldNames[i]);
            }
            return string.Join("", dropDefaults) + $"ALTER TABLE {b.QuoteIdentifier(tableName)} DROP COLUMN {string.Join(",", fieldNames)};";
        }
    }

    public interface IChangeColumn : IColumnInfo
    {
        string Sql(string tableName, Type formerType);
        string OldName { get; }
    }

    class FormerFieldInfo : IColumnInfo
    {
        readonly IColumnInfo m_ColumnInfo;
        public FormerFieldInfo(IColumnInfo columnInfo, Type oldFieldType)
        {
            FieldType = oldFieldType;
            m_ColumnInfo = columnInfo;
        }

        public uint Cardinality { get { return 0; } }
        public object Default { get { return null; } }
        public Type FieldType { get; }
        public bool IsNullAllowed { get { return true; } }
        public bool IsLong { get { return m_ColumnInfo.IsLong; } }
        public bool IsFixed { get { return m_ColumnInfo.IsFixed; } }

        public string Name { get { return m_ColumnInfo.Name; } }
        public string FieldSql(string tableName)
        {
            throw new NotSupportedException();
        }
    }

    public class ChangeColumn<T> : IMigrationElement, IChangeColumn
    {
        public string Name { get; }
        public string OldName { get; set; }
        public Type FieldType => typeof(T);

        public bool IsNullAllowed { get; set; }
        public bool IsLong { get; set; }
        public bool IsFixed { get; set; }
        public uint Cardinality { get; set; }
        public bool FixedLength { get; set; }
        public object Default { get; set; }

        public ChangeColumn(string name)
        {
            Name = name;
            OldName = name;
        }

        public string FieldSql(string tableName)
        {
            var parts = new List<string>();
            SqlCommandBuilder b = new SqlCommandBuilder();
            foreach (KeyValuePair<string, string> kvp in this.ColumnSql(tableName))
            {
                parts.Add(b.QuoteIdentifier(kvp.Key) + " " + kvp.Value);
            }
            return string.Join(",", parts);
        }

        public string Sql(string tableName)
        {
            throw new NotSupportedException();
        }

        public string Sql(string tableName, Type formerType)
        {
            var oldField = new FormerFieldInfo(this, formerType);
            List<string> oldFields;
            Dictionary<string, string> newFields;
            Dictionary<string, string> newFieldDefaults;
            Dictionary<string, string> newFieldDropDefaults;
            var sqlDefaults = new List<string>();

            oldFields = new List<string>(oldField.ColumnSql(tableName).Keys);
            newFields = this.ColumnSql(tableName, false);
            newFieldDropDefaults = this.DropDefault(tableName);
            newFieldDefaults = this.AddDefault(tableName);

            var sqlParts = new List<string>();
            var sqlDrops = new List<string>();
            SqlCommandBuilder b = new SqlCommandBuilder();

            /* remove anything that is not needed anymore */
            foreach (string fieldName in oldFields)
            {
                if (!newFields.ContainsKey(fieldName))
                {
                    sqlDrops.Add("ALTER TABLE " + b.QuoteIdentifier(tableName) + " DROP COLUMN " + b.QuoteIdentifier(fieldName) + ";");
                }
            }

            string sqlRename = string.Empty;
            foreach (KeyValuePair<string, string> kvp in newFields)
            {
                string sqlPart;
                if (oldFields.Contains(kvp.Key))
                {
                    string oldName = OldName + kvp.Key.Substring(Name.Length);
                    if (oldName != kvp.Key)
                    {
                        sqlRename += $"EXEC sp_rename @objname='{tableName + "." + oldName}', @newname='{kvp.Key}',@objtype='COLUMN';";
                    }
                    sqlPart = "ALTER COLUMN " + b.QuoteIdentifier(kvp.Key);
                }
                else
                {
                    newFieldDropDefaults.Remove(kvp.Key);
                    sqlPart = "ADD " + b.QuoteIdentifier(kvp.Key);
                }
                sqlPart += " " + kvp.Value;
                sqlParts.Add(sqlPart);
                string def;
                if (newFieldDefaults.TryGetValue(kvp.Key, out def))
                {
                    sqlDefaults.Add("ALTER TABLE " + b.QuoteIdentifier(tableName) + " ADD " + def + ";");
                }
            }

            return string.Join("", sqlDrops) + sqlRename + string.Join("", newFieldDropDefaults.Values) + "ALTER TABLE " + b.QuoteIdentifier(tableName) + " " + string.Join(",", sqlParts) + ";" + string.Join("", sqlDefaults);
        }
    }
    #endregion

    public class TableRevision : IMigrationElement
    {
        public uint Revision { get; }

        public TableRevision(uint revision)
        {
            Revision = revision;
        }

        public string Sql(string tableName) => string.Format("EXEC sys.sp_addextendedproperty @name=N'table_revision', " +
            "@value = N'{1}', @level0type = N'SCHEMA', @level0name = N'dbo'," +
            "@level1type = N'TABLE', @level1name = N'{0}'", tableName, Revision);
    }

    public class SqlStatement : IMigrationElement
    {
        public string Statement { get; }

        public SqlStatement(string statement)
        {
            Statement = statement;
        }

        public string Sql(string tableName) => Statement;
    }
}
