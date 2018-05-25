using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;

namespace DBInsertSpeedTests.Services
{
    [AttributeUsage(AttributeTargets.Property)]
    public class PrimaryKeyAttribute : Attribute { }

    public class SqlSerializer<T>
    {
        private string Name { get; }
        private IEnumerable<PropertyInfo> Properties { get; }

        public SqlSerializer(bool copyIdentifier = false, string name = null)
        {
            var type = typeof(T);
            //Name = name ?? type.Name + "s";
            Name = name ?? type.Name;

            Properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p =>
                    (p.PropertyType.IsValueType ||
                     p.PropertyType == typeof(string) ||
                     p.PropertyType.IsEnum) &&
                     p.CanRead &&
                    (!Attribute.IsDefined(p, typeof(PrimaryKeyAttribute)) || copyIdentifier)
                ).ToList();
        }

        public string SerializeInsert(IEnumerable<T> data)
        {
            var command = new StringBuilder();
            foreach (var d in data)
            {
                var cols = string.Empty;
                var vals = string.Empty;

                foreach (var prop in Properties)
                {
                    string val;
                    if (prop.PropertyType == typeof(string) || prop.PropertyType == typeof(DateTime))
                    {
                        val = prop.GetValue(d) == null ? "NULL" : $"'{prop.GetValue(d).ToString().Replace("\'", "\'\'")}'";
                    }
                    else if (prop.PropertyType == typeof(bool))
                    {
                        val = $"{((bool)prop.GetValue(d) ? "1" : "0")}";
                    }
                    else if (prop.PropertyType == typeof(bool?))
                    {
                        if ((bool?)prop.GetValue(d) == true) val = "1";
                        else if ((bool?)prop.GetValue(d) == false) val = "0";
                        else val = "NULL";
                    }
                    else if (prop.PropertyType.IsEnum)
                    {
                        val = $"{(int)prop.GetValue(d)}";
                    }
                    else
                    {
                        val = $"{prop.GetValue(d) ?? "NULL"}";
                    }

                    cols += $"{(cols != string.Empty ? "," : "")}{prop.Name}";
                    vals += $"{(vals != string.Empty ? "," : "")}{val}";
                }

                command.Append($"INSERT INTO {Name} ({cols}) VALUES ({vals});\n");
            }
            return command.ToString();
        }
    }

    internal class SqlMapper<T>
    {
        private readonly T _model;
        private readonly PropertyInfo[] _modelProperties;

        public SqlMapper(T model)
        {
            _model = model;
            _modelProperties = model.GetType().GetProperties();
        }

        public T Map(IDataRecord data, IEnumerable<string> columnsToMap)
        {
            foreach (var property in columnsToMap)
            {
                var modelProperty = _modelProperties.FirstOrDefault(p => p.Name == property);
                if (modelProperty == null) continue;
                if (!modelProperty.CanWrite) continue;

                if (data[property] is DBNull)
                    modelProperty.SetValue(_model, null);
                else
                    modelProperty.SetValue(_model, data[property]);
            }

            return _model;
        }

        public T Map(object data)
        {
            var columnsToMap = data.GetType().GetProperties().Where(p => p.CanRead);

            foreach (var property in columnsToMap)
            {
                var modelProperty = _modelProperties.FirstOrDefault(p => p.Name == property.Name);
                if (modelProperty == null) continue;
                if (!modelProperty.CanWrite) continue;

                modelProperty.SetValue(_model, property.GetValue(data));
            }

            return _model;
        }
    }

    public class SqlDataQuery<T> : SqlQuery where T : new()
    {
        public SqlDataQuery(DataTableReader reader)
            : base(reader) { }

        public IEnumerable<T> GetAll()
        {
            var resultSchema = GetResultColumns(Reader).ToList();
            var list = new List<T>();
            while (Reader.Read())
            {
                list.Add(new SqlMapper<T>(new T()).Map(Reader, resultSchema));
            }
            return list;
        }

        private static IEnumerable<string> GetResultColumns(IDataRecord reader)
        {
            return Enumerable.Range(0, reader.FieldCount).Select(reader.GetName);
        }
    }

    public class SqlQuery
    {
        protected readonly DataTableReader Reader;

        public SqlQuery(DataTableReader reader)
        {
            Reader = reader;
        }
    }
}
