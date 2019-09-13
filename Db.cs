/*
 * Db.cs - Database table mapper class.
 * (c) 2013  Osvaldo Souza
 * 
*/

using System;
using System.ComponentModel;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.OleDb;
using System.Collections.Generic;
using System.Dynamic;
using Microsoft.CSharp.RuntimeBinder;

namespace Z1.Db
{
    public class Db
    {

        public const int FETCH_READER = 0;
        public const int FETCH_ARRAY = 1;
        public const int FETCH_OBJECT = 2;

        private static Db _instance = null;
        private static OleDbConnection _con = null;

        private static bool _debug = false;

        private static int _fetchMode = Db.FETCH_OBJECT;

        public Db()
        {
        }

        public static Db GetInstance()
        {

            if (_instance == null)
                _instance = new Db();
            return _instance;

        }

        public string _buildSelect(DbMapper mapper)
        {
            int i;
            string _query = "SELECT ";

            if(mapper._limit!=null)
                _query += "TOP " + mapper._limit + " ";

            if (mapper._fields.Length > 0)
            {
                for (i = 0; i < mapper._fields.Length; i++)
                    _query += i < 1 ? mapper._fields[i] : ", " + mapper._fields[i];
                //_query += " ";
            }
            else
            {
                _query += "*";
            }

            /* For offset */

            if(mapper._offset.Length>1) {

                _query += ", ROW_NUMBER() OVER (";

                if (mapper._order.Length > 0 )
                {
                    for (i = 0; i < mapper._order.Length; i++)
                        _query += i < 1 ? "ORDER BY " + mapper._order[i] : ", " + mapper._order[i];
                } else {
                    _query += "ORDER BY " + mapper.Primary;
                }

                _query += ") AS _ROW_NUMBER ";

            } else {
                _query += " ";
            }

            if (mapper._from.Length > 0)
            {
                for (i = 0; i < mapper._from.Length; i++)
                    _query += i < 1 ? "FROM " + mapper._from[i] : ", " + mapper._from[i];
                _query += " ";
            }
            else
            {
                _query += "FROM " + mapper.Name + " ";
            }

            if (mapper._joinInner.Length > 0)
            {
                for (i = 0; i < mapper._joinInner.Length; i++)
                    _query += "INNER JOIN " + mapper._joinInner[i][0] + " ON " + mapper._joinInner[i][1] + " ";
            }

            if (mapper._joinLeft.Length > 0)
            {
                for (i = 0; i < mapper._joinLeft.Length; i++)
                    _query += "LEFT JOIN " + mapper._joinLeft[i][0] + " ON " + mapper._joinLeft[i][1] + " ";
            }

            if (mapper._where.Length > 0)
            {
                for (i = 0; i < mapper._where.Length; i++)
                {
                    if (mapper._where[i] != null)
                        _query += i < 1 ? "WHERE " + mapper._where[i][0] + (i < (mapper._where.Length-1)? " " + mapper._where[i][1] : "") : " " + mapper._where[i][0] + (i < (mapper._where.Length-1) ? " " + mapper._where[i][1] : "");
                }
                _query += " ";
            }

            if (mapper._group.Length > 0)
            {
                for (i = 0; i < mapper._group.Length; i++)
                    _query += i < 1 ? "GROUP BY " + mapper._group[i] : ", " + mapper._group[i];
                _query += " ";
                if (mapper._having != null)
                {
                    _query += "HAVING " + mapper._having;
                }
            }

            if (mapper._order.Length > 0 && mapper._offset.Length < 2)
            {
                for (i = 0; i < mapper._order.Length; i++)
                    _query += i < 1 ? "ORDER BY " + mapper._order[i] : ", " + mapper._order[i];
                _query += " ";
            }

            /*
             * Generates container query for offset results
            */

            if (mapper._offset.Length > 1)
            {

                string _container = "SELECT ";
                string _field;

                if (mapper._fields.Length > 0)
                {
                    for (i = 0; i < mapper._fields.Length; i++)
                    {
                        _field = mapper._fields[i];
                        if (_field.IndexOf(".") > -1)
                            _field = _field.Substring(_field.IndexOf(".") + 1);
                        _container += i < 1 ? _field : ", " + _field;
                    }
                }
                else
                {
                    _container += "*";
                }

                _container += " FROM ( " + _query + " ) AS RESULT_TABLE WHERE RESULT_TABLE._ROW_NUMBER BETWEEN " + mapper._offset[0].ToString() + " AND " + mapper._offset[1].ToString() + ";";

                return _container;

            }

            _query += ";";

            return _query;

        }

        private string _buildInsert(DbMapper mapper, string[,] values)
        {
            int i, len = values.Length/2;
            string _fields = "", _values = "";
            string _query = "INSERT INTO " + mapper.Name;

            if (len > 0)
            {
                for (i = 0; i < len; i++)
                {
                    _fields += values[i, 0] + (i < len - 1 ? ", " : "");
                    if (values[i, 1] == null)
                    {
                        values[i, 1] = "NULL";
                    }
                    else
                    {
                        if (values[i, 1].IndexOf("'") > -1)
                            values[i, 1] = values[i, 1].Replace("'", "\\'");
                        if (mapper._metaInfo[values[i, 0]].ContainsKey("QUOTE"))
                            values[i, 1] = "'" + values[i, 1] + "'";
                    }
                    _values += values[i, 1] + (i < len - 1 ? ", " : "");
                }
            }

            _query += " (" + _fields + ") VALUES (" + _values + ");";

            return _query;

        }

        private string _buildUpdate(DbMapper mapper, string[,] values, string where = null)
        {

            int i, len = values.Length / 2;
            string _query = "UPDATE " + mapper.Name + " SET ";

            if (len > 0)
            {
                for (i = 0; i < len; i++)
                {
                    if (values[i, 1] == null)
                    {
                        values[i, 1] = "NULL";
                    }
                    else
                    {
                        if (values[i, 1].IndexOf("'") > -1)
                            values[i, 1] = values[i, 1].Replace("'", "\\'");
                        if (mapper._metaInfo[values[i, 0]].ContainsKey("QUOTE"))
                            values[i, 1] = "'" + values[i, 1] + "'";
                    }
                    _query += values[i, 0] + " = " + values[i, 1] + (i < len - 1 ? ", " : "");
                }
            }

            if(where!=null)
                _query += " WHERE " + where;

            _query += ";";

            return _query;

        }

        private string _buildDelete(DbMapper mapper, string where = null)
        {
            string _query = "DELETE FROM " + mapper.Name;

            if (where != null)
                _query += " WHERE " + where;

            _query += ";";

            return _query;

        }

        public void getMetaInfo(DbMapper mapper)
        {

            //if (mapper._metaInfo.Count > 0) return;

            OleDbDataReader dr;

            dr = this.execute("EXEC sp_columns '" + mapper.Name + "';");

            if (dr != null)
            {

                int i = 0;
                string columnType;

                mapper._metaInfo = new Dictionary<string, Dictionary<string, string>>();

                while (dr.Read())
                {
                    columnType = dr["TYPE_NAME"].ToString();

                    Dictionary<string, string> metaData = new Dictionary<string, string>();
                    metaData["TYPE"] = columnType;

                    if (columnType.IndexOf("int") < 0 && columnType != "bit" && columnType != "float")
                        metaData["QUOTE"] = "1";

                    mapper._metaInfo[dr["COLUMN_NAME"].ToString()] = metaData;

                    i++;
                }

            }

        }

        public dynamic execute(string query)
        {

            OleDbDataReader oleDbDataReader;

            try
            {

                if (_debug == true)
                {
                    System.Windows.Forms.MessageBox.Show(query);
                    return null;
                }

                OleDbCommand cmd = new OleDbCommand(query, Db._con);
                cmd.CommandType = CommandType.Text;
                if (Db._con.State == ConnectionState.Closed)
                {
                    Db._con.Open();
                }
                oleDbDataReader = cmd.ExecuteReader();
            }
            catch (Exception exception)
            {
                throw exception;
            }

            return oleDbDataReader;

        }

        public void executeNonQuery(string query)
        {

            try
            {

                if (_debug == true)
                {
                    System.Windows.Forms.MessageBox.Show(query);
                    return;
                }

                OleDbCommand cmd = new OleDbCommand(query, Db._con);
                cmd.CommandType = CommandType.Text;
                if (Db._con.State == ConnectionState.Closed)
                {
                    Db._con.Open();
                }
                cmd.ExecuteNonQuery();
            }
            catch (Exception exception)
            {
                throw exception;
            }

        }

        public dynamic fetchAll(DbMapper mapper)
        {
            int len;
            dynamic[] rowset = new dynamic[0];
            OleDbDataReader dr;

            if (Db._fetchMode == Db.FETCH_READER)
            {
                return this.execute(this._buildSelect(mapper));
            }
            else
            {

                dr = this.execute(this._buildSelect(mapper));

                if (_debug == false)
                {

                    while (dr.Read())
                    {

                        string[,] values = new string[dr.VisibleFieldCount, 2];

                        for (int i = 0; i < dr.VisibleFieldCount; i++)
                        {
                            values[i, 0] = dr.GetName(i);
                            values[i, 1] = dr[dr.GetName(i)].ToString();
                        }

                        if (Db._fetchMode == Db.FETCH_OBJECT)
                        {

                            DbMapperRecord record = null;

                            if (mapper.Record != null)
                                record = (DbMapperRecord)System.Activator.CreateInstance(Type.GetType(mapper.Record));
                            else
                                record = new DbMapperRecord(mapper);

                            record.Hydrate(values);

                            len = rowset.Length;
                            Array.Resize<dynamic>(ref rowset, (len + 1));
                            rowset[len] = record;
                        }
                        else
                        {
                            len = rowset.Length;
                            Array.Resize<dynamic>(ref rowset, (len + 1));
                            rowset[len] = values;
                        }

                    }

                    return rowset;

                }

                return dr;

            }

        }

        public dynamic fetchRow(DbMapper mapper)
        {

            OleDbDataReader dr;

            dr = this.execute(this._buildSelect(mapper));

            if (_debug == false)
            {

                dr.Read();

                if (Db._fetchMode == Db.FETCH_OBJECT ||
                    Db._fetchMode == Db.FETCH_ARRAY)
                {
                    string[,] values = new string[dr.VisibleFieldCount, 2];

                    for (int i = 0; i < dr.VisibleFieldCount; i++)
                    {
                        values[i, 0] = dr.GetName(i);
                        values[i, 1] = dr[dr.GetName(i)].ToString();
                    }

                    if (Db._fetchMode == Db.FETCH_OBJECT)
                    {

                        DbMapperRecord record = null;

                        if (mapper.Record != null)
                            record = (DbMapperRecord)System.Activator.CreateInstance(Type.GetType(mapper.Record));
                        else
                            record = new DbMapperRecord(mapper);

                        record.Hydrate(values);
                        return record;
                    }
                    else
                    {
                        return values;
                    }

                }

            }

            return dr;

        }

        public void insert(DbMapper mapper, string[,] values)
        {

            if (mapper._metaInfo == null)
                this.getMetaInfo(mapper);

            this.executeNonQuery(this._buildInsert(mapper, values));

        }

        public void update(DbMapper mapper, string[,] values, string where)
        {

            if (mapper._metaInfo == null)
                this.getMetaInfo(mapper);

            this.executeNonQuery(this._buildUpdate(mapper, values, where));

        }

        public void delete(DbMapper mapper, string where)
        {
            this.executeNonQuery(this._buildDelete(mapper, where));
        }

        public void connect(string config = null)
        {

            if(config==null)
                config = ConfigurationManager.ConnectionStrings["DB"].ToString();

            try
            {
                Db._con = new OleDbConnection(config);
            }
            catch (Exception exception)
            {
                throw exception;
            }

        }

        public void disconnect()
        {
            if (Db._con != null)
            {
                Db._con.Close();
                Db._con = null;
            }
        }

        public static void setDebug(bool debug)
        {
            Db._debug = debug;
        }

        public static void setFetchMode(int mode)
        {
            Db._fetchMode = mode;
        }

        public static int getFetchMode()
        {
            return Db._fetchMode;
        }

    }
}